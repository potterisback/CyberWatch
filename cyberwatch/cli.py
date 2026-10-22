from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter

from .detection import Detector
from .schema import read_events
from .storage import Store
from .synthetic import generate


def emit(value):
    print(json.dumps(value, indent=2, default=str), flush=True)


def ingest_file(path, db, model=None, batch_size=500, limit=None):
    store, detector = Store(db), Detector(model)
    totals = dict(received=0, inserted=0, duplicates=0, rejected=0, alerts=0)
    start = perf_counter()
    batch = []
    for event in read_events(path, limit=limit):
        batch.append(event)
        if len(batch) >= batch_size:
            stats = store.ingest(batch, detector)
            totals = {k: totals[k] + stats[k] for k in totals}
            batch.clear()
    if batch:
        stats = store.ingest(batch, detector)
        totals = {k: totals[k] + stats[k] for k in totals}
    elapsed = perf_counter() - start
    return dict(
        totals,
        elapsed_seconds=round(elapsed, 3),
        records_per_second=round(totals["received"] / max(elapsed, 1e-9), 2),
        backend="sqlite",
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="CyberWatch: local demo and distributed lakehouse helpers"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser(
        "demo",
        help="Generate clearly labeled synthetic traffic and start the dashboard",
    )
    p.add_argument("--count", type=int, default=2500)
    p.add_argument("--db", default="var/demo.db")
    p.add_argument("--no-server", action="store_true")
    p.add_argument("--port", type=int, default=8000)
    p = sub.add_parser("serve", help="Start the dashboard")
    p.add_argument("--db", default="var/cyberwatch.db")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--backend", choices=["sqlite", "trino"], default="sqlite")
    p.add_argument("--trino-url", default="http://localhost:8080")
    p = sub.add_parser(
        "generate", help="Write reproducible synthetic JSONL (not benchmark data)"
    )
    p.add_argument("--out", default="data/synthetic/demo.jsonl")
    p.add_argument("--count", type=int, default=2500)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--split", default="demo")
    p = sub.add_parser(
        "download", help="Download checksum-pinned UNSW train/test splits from a mirror"
    )
    p.add_argument(
        "--mirror",
        action="store_true",
        help="Explicitly choose the documented public mirror",
    )
    p.add_argument("--out", default="data/raw")
    p = sub.add_parser(
        "prepare", help="Normalize an official CSV into the canonical JSONL contract"
    )
    p.add_argument("--file", required=True)
    p.add_argument("--out", required=True)
    p.add_argument(
        "--split",
        choices=["training", "testing", "replay", "unassigned"],
        default="unassigned",
    )
    p.add_argument("--limit", type=int)
    p = sub.add_parser(
        "ingest", help="Run local reference ingestion with deduplication and quarantine"
    )
    p.add_argument("--file", required=True)
    p.add_argument("--db", default="var/cyberwatch.db")
    p.add_argument("--model")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--limit", type=int)
    p = sub.add_parser(
        "train", help="Train a forest and evaluate on a distinct held-out split"
    )
    p.add_argument("--train", default="data/raw/train.csv")
    p.add_argument("--test", default="data/raw/test.csv")
    p.add_argument("--model", default="artifacts/forest.json")
    p.add_argument("--report", default="reports/evaluation.json")
    p.add_argument("--limit", type=int)
    p.add_argument("--trees", type=int, default=64)
    p = sub.add_parser("produce", help="Replay CSV/JSONL records into Kafka")
    p.add_argument("--file", required=True)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="cyberwatch.flows")
    p.add_argument("--rate", type=float, default=500)
    p.add_argument("--limit", type=int)
    p = sub.add_parser(
        "benchmark", help="Compare one local transaction with local micro-batches"
    )
    p.add_argument("--file", required=True)
    p.add_argument("--limit", type=int, default=5000)
    p.add_argument("--batch-size", type=int, default=250)
    p.add_argument("--out", default="reports/local_benchmark.json")
    args = parser.parse_args(argv)
    for key in ("count", "limit", "batch_size", "trees"):
        value = getattr(args, key, None)
        if value is not None and value <= 0:
            parser.error(f"--{key.replace('_', '-')} must be positive")
    try:
        if args.command == "demo":
            start = (
                datetime.now(timezone.utc) - timedelta(seconds=args.count * 0.35)
            ).isoformat()
            emit(
                Store(args.db).ingest(
                    generate(args.count, start_time=start), Detector()
                )
            )
            if not args.no_server:
                from .server import serve

                serve(args.db, port=args.port)
        elif args.command == "serve":
            from .server import serve

            serve(args.db, args.host, args.port, args.backend, args.trino_url)
        elif args.command in ("generate", "prepare"):
            output = Path(args.out)
            output.parent.mkdir(parents=True, exist_ok=True)
            rows = (
                generate(args.count, args.seed, args.split)
                if args.command == "generate"
                else read_events(args.file, args.split, args.limit)
            )
            count = 0
            temporary = output.with_suffix(output.suffix + ".partial")
            try:
                with temporary.open("w", encoding="utf-8") as stream:
                    for event in rows:
                        stream.write(json.dumps(event, allow_nan=False) + "\n")
                        count += 1
                temporary.replace(output)
            finally:
                temporary.unlink(missing_ok=True)
            emit(dict(rows=count, output=str(output)))
        elif args.command == "download":
            if not args.mirror:
                parser.error(
                    "Use --mirror to choose the checksum-pinned mirror, or manually download from the official UNSW page. See docs/DATA.md."
                )
            from .datasets import download

            emit(download(args.out))
        elif args.command == "ingest":
            emit(
                ingest_file(args.file, args.db, args.model, args.batch_size, args.limit)
            )
        elif args.command == "train":
            from .ml import train

            emit(
                train(
                    args.train,
                    args.test,
                    args.model,
                    args.report,
                    args.limit,
                    args.trees,
                )
            )
        elif args.command == "produce":
            from .producer import produce

            emit(produce(args.file, args.bootstrap, args.topic, args.rate, args.limit))
        elif args.command == "benchmark":
            rows = list(read_events(args.file, limit=args.limit))
            results = []
            with tempfile.TemporaryDirectory() as directory:
                for name, size in [
                    ("single_batch", len(rows)),
                    ("micro_batches", args.batch_size),
                ]:
                    if not rows:
                        raise ValueError("No benchmark rows")
                    store, detector = (
                        Store(Path(directory) / (name + ".db")),
                        Detector(),
                    )
                    start = perf_counter()
                    for offset in range(0, len(rows), size):
                        store.ingest(rows[offset : offset + size], detector)
                    elapsed = perf_counter() - start
                    snapshot = store.dashboard()
                    results.append(
                        dict(
                            mode=name,
                            rows=len(rows),
                            elapsed_seconds=elapsed,
                            records_per_second=len(rows) / elapsed,
                            alert_count=snapshot["summary"]["alert_count"],
                        )
                    )
            report = dict(
                backend="SQLite local reference (not Spark or Kafka)",
                results=results,
                same_alert_count=results[0]["alert_count"] == results[1]["alert_count"],
                note="One measured local run per mode, excluding CSV decoding; not a distributed scalability claim.",
            )
            Path(args.out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
            emit(report)
    except (ValueError, OSError, ImportError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0
