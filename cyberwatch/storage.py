"""Transactional local reference backend; SQLite is NOT presented as Iceberg."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .schema import fingerprint, utcnow, validate


class Store:
    def __init__(self, path="var/cyberwatch.db"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS bronze (
                    event_id TEXT PRIMARY KEY, received_at TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS silver (
                    event_id TEXT PRIMARY KEY REFERENCES bronze(event_id), event_time TEXT NOT NULL,
                    ingested_at TEXT NOT NULL, proto TEXT NOT NULL, service TEXT NOT NULL,
                    source TEXT NOT NULL, is_synthetic INTEGER NOT NULL,
                    time_is_simulated INTEGER NOT NULL, label INTEGER, attack_cat TEXT,
                    predicted_label INTEGER NOT NULL, score REAL NOT NULL, engine TEXT NOT NULL,
                    reason TEXT NOT NULL, src_ip TEXT, dst_ip TEXT, sbytes REAL NOT NULL,
                    dbytes REAL NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS silver_time ON silver(event_time);
                CREATE INDEX IF NOT EXISTS silver_alerts ON silver(predicted_label, event_time);
                CREATE TABLE IF NOT EXISTS quarantine (
                    event_id TEXT PRIMARY KEY, received_at TEXT NOT NULL, reason TEXT NOT NULL, payload TEXT NOT NULL);
            """)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def ingest(self, events, detector):
        stats = dict(received=0, inserted=0, duplicates=0, rejected=0, alerts=0)
        with self.connect() as db:
            for raw in events:
                stats["received"] += 1
                now = utcnow()
                payload = json.dumps(raw, sort_keys=True, default=str)
                raw_id = raw.get("event_id") if isinstance(raw, dict) else None
                event_id = (
                    raw_id if isinstance(raw_id, str) and raw_id else fingerprint(raw)
                )
                cursor = db.execute(
                    "INSERT OR IGNORE INTO bronze VALUES (?,?,?)",
                    (event_id, now, payload),
                )
                if cursor.rowcount == 0:
                    stats["duplicates"] += 1
                    continue
                try:
                    event = validate(raw)
                except (ValueError, TypeError) as exc:
                    db.execute(
                        "INSERT OR REPLACE INTO quarantine VALUES (?,?,?,?)",
                        (event_id, now, str(exc), payload),
                    )
                    stats["rejected"] += 1
                    continue
                prediction = detector.predict(event)
                db.execute(
                    "INSERT INTO silver VALUES (" + ",".join(["?"] * 19) + ")",
                    (
                        event_id,
                        event["event_time"],
                        now,
                        event["proto"],
                        event["service"],
                        event["source"],
                        int(event["is_synthetic"]),
                        int(event.get("time_is_simulated", True)),
                        event.get("label"),
                        event.get("attack_cat"),
                        prediction["predicted_label"],
                        prediction["score"],
                        prediction["engine"],
                        prediction["reason"],
                        event.get("src_ip"),
                        event.get("dst_ip"),
                        event["sbytes"],
                        event["dbytes"],
                        json.dumps(event, sort_keys=True),
                    ),
                )
                stats["inserted"] += 1
                stats["alerts"] += prediction["predicted_label"]
        return stats

    def dashboard(self, proto=None, limit=100):
        where, args = (" WHERE proto = ?", [proto]) if proto else ("", [])
        with self.connect() as db:
            summary = dict(
                db.execute(
                    """SELECT COUNT(*) AS total_events,
                COALESCE(SUM(predicted_label),0) AS alert_count,
                COALESCE(SUM(is_synthetic),0) AS synthetic_count,
                COALESCE(SUM(time_is_simulated),0) AS simulated_time_count,
                COALESCE(SUM(sbytes+dbytes),0) AS total_bytes,
                COUNT(DISTINCT proto) AS protocol_count, MAX(ingested_at) AS last_ingested,
                MIN(event_time) AS first_event, MAX(event_time) AS last_event
                FROM silver"""
                    + where,
                    args,
                ).fetchone()
            )
            summary["rejected_count"] = db.execute(
                "SELECT COUNT(*) FROM quarantine"
            ).fetchone()[0]
            summary["bronze_count"] = db.execute(
                "SELECT COUNT(*) FROM bronze"
            ).fetchone()[0]
            summary["alert_rate"] = summary["alert_count"] / max(
                summary["total_events"], 1
            )
            timeline = [
                dict(row)
                for row in db.execute(
                    """SELECT substr(event_time,1,16) AS minute,
                COUNT(*) AS events, SUM(predicted_label) AS alerts FROM silver"""
                    + where
                    + " GROUP BY minute ORDER BY minute DESC LIMIT 120",
                    args,
                )
            ]
            timeline.reverse()
            protocols = [
                dict(row)
                for row in db.execute(
                    """SELECT proto, COUNT(*) AS events,
                SUM(predicted_label) AS alerts FROM silver"""
                    + where
                    + " GROUP BY proto ORDER BY events DESC",
                    args,
                )
            ]
            alerts = [
                dict(row)
                for row in db.execute(
                    """SELECT event_id,event_time,proto,service,source,
                src_ip,dst_ip,score,engine,reason FROM silver WHERE predicted_label=1"""
                    + (" AND proto=?" if proto else "")
                    + " ORDER BY event_time DESC LIMIT ?",
                    args + [limit],
                )
            ]
            engines = [
                row[0]
                for row in db.execute(
                    "SELECT DISTINCT engine FROM silver" + where, args
                )
            ]
        return dict(
            summary=summary,
            timeline=timeline,
            protocols=protocols,
            alerts=alerts,
            engines=engines,
            backend="sqlite",
            mode="local reference",
            generated_at=utcnow(),
        )
