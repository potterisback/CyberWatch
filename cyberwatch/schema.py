"""One data contract shared by local replay, Kafka and Spark.

Ground-truth labels are retained for evaluation ONLY. They are not model inputs.
The official train/test CSVs have no addresses or original event timestamps;
we leave addresses null and explicitly flag their replay timestamps as simulated.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

NUMERIC = (
    "dur",
    "spkts",
    "dpkts",
    "sbytes",
    "dbytes",
    "rate",
    "sttl",
    "dttl",
    "sload",
    "dload",
    "sloss",
    "dloss",
    "smean",
    "dmean",
    "synack",
    "ackdat",
    "ct_srv_src",
    "ct_dst_ltm",
    "ct_src_dport_ltm",
    "ct_dst_sport_ltm",
    "ct_state_ttl",
)
CATEGORIES = {
    "proto": ("tcp", "udp", "icmp", "other"),
    "service": ("http", "dns", "ftp", "smtp", "ssh", "-", "other"),
    "state": ("FIN", "CON", "INT", "REQ", "other"),
}
FEATURE_NAMES = list(NUMERIC) + [
    f"{key}={v}" for key, values in CATEGORIES.items() for v in values
]
FULL_COLUMNS = (
    "srcip sport dstip dsport proto state dur sbytes dbytes sttl dttl sloss dloss "
    "service sload dload spkts dpkts swin dwin stcpb dtcpb smean dmean trans_depth "
    "res_bdy_len sjit djit stime ltime sintpkt dintpkt tcprtt synack ackdat "
    "is_sm_ips_ports ct_state_ttl ct_flw_http_mthd is_ftp_login ct_ftp_cmd ct_srv_src "
    "ct_srv_dst ct_dst_ltm ct_src_ltm ct_src_dport_ltm ct_dst_sport_ltm ct_dst_src_ltm "
    "attack_cat label"
).split()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:32]


def number(value, name: str) -> float:
    try:
        n = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{name}: expected a finite number") from exc
    if not math.isfinite(n) or n < 0:
        raise ValueError(f"{name}: expected a non-negative finite number")
    return n


def validate(event: dict) -> dict:
    """Reject malformed events rather than silently inventing feature values."""
    e = dict(event)
    for key in ("event_id", "event_time", "source", "proto", "service", "state"):
        if not e.get(key) or not isinstance(e[key], str):
            raise ValueError(f"{key}: missing or invalid")
    ts = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
    if ts.tzinfo is None:
        raise ValueError("event_time: timezone required")
    e["event_time"] = ts.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    for key in NUMERIC:
        e[key] = number(e.get(key), key)
    if e.get("label") not in (None, 0, 1):
        raise ValueError("label: expected 0, 1, or null")
    if not isinstance(e.get("is_synthetic"), bool):
        raise ValueError("is_synthetic: boolean required")
    return e


def feature_vector(event: dict) -> list[float]:
    result = [math.log1p(float(event[name])) for name in NUMERIC]
    for key, choices in CATEGORIES.items():
        raw = event.get(key)
        value = raw if raw in choices else "other"
        result.extend(float(value == choice) for choice in choices)
    return result


def csv_events(
    path: str | Path,
    split="unassigned",
    limit: int | None = None,
    start_time: str | None = None,
):
    """Read official headered train/test CSVs or the four headerless full CSVs.

    CSV rows are streamed; no full-dataset pandas materialization is needed.
    The producer calls this with the current start time for simulated replay.
    """
    path = Path(path)
    start = (
        datetime.fromisoformat(start_time)
        if start_time
        else datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    with path.open(newline="", encoding="utf-8-sig") as stream:
        first = stream.readline()
        stream.seek(0)
        has_header = "attack_cat" in first.lower() and "label" in first.lower()
        reader = csv.DictReader(stream, fieldnames=None if has_header else FULL_COLUMNS)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            row = {
                str(k).strip().lower(): v.strip() if isinstance(v, str) else v
                for k, v in row.items()
                if k is not None
            }
            # Old full files use uppercase Sload/Dload and "ct_ftp_cmd" with blanks.
            original_time = row.get("stime")
            try:
                event_time = (
                    datetime.fromtimestamp(float(original_time), timezone.utc)
                    if original_time
                    else start + timedelta(milliseconds=i * 100)
                )
                source = "UNSW-NB15"
                event = {
                    "event_id": fingerprint({"file": path.name, "row": i, "data": row}),
                    "event_time": event_time.isoformat(timespec="milliseconds"),
                    "source": source,
                    "split": split,
                    "source_file": path.name,
                    "src_ip": row.get("srcip"),
                    "dst_ip": row.get("dstip"),
                    "src_port": row.get("sport"),
                    "dst_port": row.get("dsport"),
                    "proto": row.get("proto", "other"),
                    "service": row.get("service") or "-",
                    "state": row.get("state", "other"),
                    "is_synthetic": False,
                    "time_is_simulated": not bool(original_time),
                    "label": int(row["label"])
                    if row.get("label") not in (None, "")
                    else None,
                    "attack_cat": row.get("attack_cat") or "Normal",
                }
                for name in NUMERIC:
                    if name == "rate" and name not in row:
                        duration = number(row.get("dur"), "dur")
                        event[name] = (
                            number(row.get("spkts"), "spkts")
                            + number(row.get("dpkts"), "dpkts")
                        ) / max(duration, 0.001)
                    else:
                        event[name] = number(row.get(name), name)
                yield validate(event)
            except (ValueError, TypeError, OverflowError) as exc:
                raise ValueError(f"{path.name}, data row {i + 1}: {exc}") from exc


def read_events(path: str | Path, split="unassigned", limit=None, start_time=None):
    path = Path(path)
    if path.suffix.lower() in (".jsonl", ".ndjson"):
        with path.open(encoding="utf-8") as stream:
            for i, line in enumerate(stream):
                if limit is not None and i >= limit:
                    break
                if line.strip():
                    event = json.loads(line)
                    if split != "unassigned":
                        event["split"] = split
                    yield event
    else:
        yield from csv_events(path, split, limit, start_time)
