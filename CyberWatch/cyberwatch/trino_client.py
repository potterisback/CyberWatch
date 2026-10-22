"""Small read-only client for the Trino HTTP statement API."""

import json
from urllib.request import Request, urlopen
from urllib.parse import urlsplit

from .schema import utcnow


class TrinoStore:
    def __init__(self, url="http://localhost:8080"):
        self.url = url.rstrip("/")

    def query(self, sql):
        request = Request(
            self.url + "/v1/statement",
            data=sql.encode(),
            headers={
                "X-Trino-User": "cyberwatch",
                "X-Trino-Source": "cyberwatch-dashboard",
            },
        )
        rows, names = [], []
        for _ in range(300):
            with urlopen(request, timeout=30) as response:
                page = json.load(response)
            if "error" in page:
                raise RuntimeError(page["error"]["message"])
            if "columns" in page:
                names = [col["name"] for col in page["columns"]]
            rows.extend(dict(zip(names, values)) for values in page.get("data", []))
            if not page.get("nextUri"):
                return rows
            if urlsplit(page["nextUri"]).netloc != urlsplit(self.url).netloc:
                raise RuntimeError("Unexpected Trino pagination host")
            request = Request(page["nextUri"], headers={"X-Trino-User": "cyberwatch"})
        raise TimeoutError("Trino query exceeded pagination limit")

    def dashboard(self, proto=None, limit=100):
        # Validate the one selectable dimension before inserting it in static SQL.
        if proto and (
            len(proto) > 20 or not all(c.isalnum() or c in "_-" for c in proto)
        ):
            raise ValueError("Invalid protocol")
        where = f" WHERE proto='{proto}'" if proto else ""
        summary = self.query(
            "SELECT count(*) total_events, coalesce(sum(predicted_label),0) alert_count, "
            "coalesce(sum(CASE WHEN is_synthetic THEN 1 ELSE 0 END),0) synthetic_count, "
            "coalesce(sum(CASE WHEN time_is_simulated THEN 1 ELSE 0 END),0) simulated_time_count, "
            "coalesce(sum(sbytes+dbytes),0) total_bytes, count(DISTINCT proto) protocol_count, "
            "max(ingested_at) last_ingested, min(event_time) first_event, max(event_time) last_event "
            "FROM lake.silver.events" + where
        )[0]
        for table, field in [
            ("quarantine", "rejected_count"),
            ("bronze", "bronze_count"),
        ]:
            summary[field] = self.query(f"SELECT count(*) n FROM lake.{table}.events")[
                0
            ]["n"]
        summary["alert_rate"] = summary["alert_count"] / max(summary["total_events"], 1)
        timeline = self.query(
            "SELECT minute, sum(events) events, sum(alerts) alerts FROM lake.gold.minute_stats"
            + where
            + " GROUP BY minute ORDER BY minute DESC LIMIT 120"
        )[::-1]
        protocols = self.query(
            "SELECT proto, events, alerts FROM lake.gold.protocol_stats"
            + where
            + " ORDER BY events DESC"
        )
        alerts = self.query(
            "SELECT event_id,event_time,proto,service,source,src_ip,dst_ip,score,engine,reason "
            "FROM lake.gold.alerts"
            + where
            + f" ORDER BY event_time DESC LIMIT {min(int(limit), 500)}"
        )
        engines = [
            r["engine"]
            for r in self.query(
                "SELECT DISTINCT engine FROM lake.silver.events" + where
            )
        ]
        return dict(
            summary=summary,
            timeline=timeline,
            protocols=protocols,
            alerts=alerts,
            engines=engines,
            backend="trino",
            mode="Kafka / Spark / Iceberg",
            generated_at=utcnow(),
        )
