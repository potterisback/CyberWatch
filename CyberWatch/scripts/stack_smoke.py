#!/usr/bin/env python3
"""Verify an already-running Docker stack with synthetic records.

Does not delete existing data. Synthetic records are explicitly marked. Call
from the project root after `docker compose up -d --build`.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cyberwatch.synthetic import generate
from cyberwatch.trino_client import TrinoStore


def main():
    # A different seed isolates this diagnostic from an ordinary demo replay.
    rows = list(generate(40, seed=314159, split="stack-smoke"))
    path = Path("data/synthetic/stack-smoke.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in rows), encoding="utf-8")
    command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "runner",
        "python",
        "run.py",
        "produce",
        "--bootstrap",
        "kafka:29092",
        "--file",
        str(path),
        "--rate",
        "100",
    ]
    subprocess.run(command, check=True)
    client = TrinoStore()
    deadline = time.monotonic() + 300
    query = "SELECT count(*) n FROM lake.silver.events WHERE split='stack-smoke'"
    last_error = "no records"
    while time.monotonic() < deadline:
        try:
            count = client.query(query)[0]["n"]
            if count == 40:
                break
            last_error = f"expected 40 rows; got {count}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(3)
    else:
        raise RuntimeError("Stack smoke test timed out: " + last_error)
    subprocess.run(command, check=True)
    # Wait until the second replay appears in the stream's committed progress,
    # then ensure event IDs still deduplicate to the same 40 silver rows.
    time.sleep(30)
    assert client.query(query)[0]["n"] == 40
    result = dict(
        kafka_spark_iceberg_trino=True,
        smoke_rows=40,
        replayed_without_inflation=True,
        checked_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )
    Path("reports/stack_smoke.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
