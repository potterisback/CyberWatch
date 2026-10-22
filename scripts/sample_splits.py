#!/usr/bin/env python3
"""Reproduce the real-data reservoir samples used for native Spark ML validation."""

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cyberwatch.schema import csv_events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train-size", type=int, default=3000)
    parser.add_argument("--test-size", type=int, default=1500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if min(args.train_size, args.test_size) <= 0:
        parser.error("sample sizes must be positive")
    for part, count in [("train", args.train_size), ("test", args.test_size)]:
        rng, sample = random.Random(args.seed), []
        split = "training" if part == "train" else "testing"
        for index, event in enumerate(csv_events(f"data/raw/{part}.csv", split)):
            if index < count:
                sample.append(event)
            else:
                replacement = rng.randint(0, index)
                if replacement < count:
                    sample[replacement] = event
        path = Path(f"data/prepared/ml-validation-{part}.jsonl")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(json.dumps(e) for e in sample), encoding="utf-8")
        print(
            json.dumps(
                dict(
                    file=str(path),
                    rows=len(sample),
                    seed=args.seed,
                    method="reservoir sampling; separate source splits preserved",
                )
            )
        )


if __name__ == "__main__":
    main()
