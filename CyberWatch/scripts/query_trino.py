#!/usr/bin/env python3
"""Run the supplied read-only analytics queries against the Docker stack."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cyberwatch.trino_client import TrinoStore

parser = argparse.ArgumentParser()
parser.add_argument("--url", default="http://localhost:8080")
args = parser.parse_args()
store = TrinoStore(args.url)
for title, sql in [
    (
        "Protocol statistics",
        "SELECT * FROM lake.gold.protocol_stats ORDER BY events DESC",
    ),
    (
        "Recent alerts",
        "SELECT event_id,event_time,proto,score,reason FROM lake.gold.alerts ORDER BY event_time DESC LIMIT 10",
    ),
    (
        "Iceberg snapshots",
        'SELECT snapshot_id,committed_at,operation FROM lake.silver."events$snapshots" ORDER BY committed_at DESC LIMIT 5',
    ),
]:
    print(title)
    print(json.dumps(store.query(sql), indent=2, default=str))
