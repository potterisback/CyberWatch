-- Run against Trino's lake catalog. These queries never read labels for detection.
SELECT proto, events, alerts, round(100.0 * alerts / NULLIF(events,0),2) AS alert_percent
FROM lake.gold.protocol_stats ORDER BY events DESC;

SELECT minute, SUM(events) AS connections, SUM(alerts) AS suspicious_connections
FROM lake.gold.minute_stats GROUP BY minute ORDER BY minute;

SELECT event_id, event_time, proto, service, score, reason, engine
FROM lake.gold.alerts ORDER BY event_time DESC LIMIT 100;

-- Audit persistent table history: snapshot IDs must change after successful commits.
SELECT committed_at, snapshot_id, operation
FROM lake.silver."events$snapshots" ORDER BY committed_at DESC;

-- Data quality and lineage.
SELECT source, is_synthetic, time_is_simulated, engine, COUNT(*) AS records
FROM lake.silver.events GROUP BY source,is_synthetic,time_is_simulated,engine;
SELECT reason, COUNT(*) AS rejected FROM lake.quarantine.events GROUP BY reason;
