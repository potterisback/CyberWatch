# Execution and operations

Run commands from the repository root. The local demo needs Python 3.10–3.12.
Docker commands use the bundled Python 3.11 / Java 17 / Spark 3.5.7 image.

## Offline commands

```bash
python run.py demo
python run.py demo --no-server --db var/separate-demo.db --count 4000
python run.py generate --out data/synthetic/demo.jsonl --count 5000
python run.py ingest --file data/synthetic/demo.jsonl --db var/reference.db
python run.py serve --db var/reference.db --port 8002
python run.py benchmark --file data/synthetic/demo.jsonl --limit 5000
```

The local benchmark compares one SQLite transaction with smaller SQLite
transactions over the same predecoded rows. It verifies output consistency and
reports measured throughput; it is **not** a Spark, Kafka or cluster benchmark.

## Prepare real data and retrain the serving model

```bash
python run.py download --mirror
python -m pip install -r requirements-ml.txt
python run.py train
python run.py prepare --file data/raw/train.csv --out data/prepared/train.jsonl --split training
python run.py prepare --file data/raw/test.csv --out data/prepared/test.jsonl --split testing
```

Prepared JSONL can be large. Raw and prepared data are ignored by Git. `--limit`
is available for smoke tests and selects the **first N rows**, not a random or
representative sample; report that limitation if using it for evaluation.

## Full stack startup

```bash
docker compose up -d --build
docker compose run --rm runner python run.py download --mirror
docker compose run --rm runner python run.py produce --bootstrap kafka:29092 --file data/raw/test.csv --rate 300
docker compose logs --tail 80 stream
```

Three Kafka partitions demonstrate parallel input partitioning on a single
broker. This local deployment is not a highly available cluster. A dedicated
Spark worker cluster can be selected through `--master` when available; the
Compose setup uses `local[2]` inside the Spark service for accessible coursework.

Services bind host ports only to 127.0.0.1. S3 credentials are clearly marked
local-development defaults, not personal credentials. Do not expose this
unauthenticated educational stack directly to the internet.

| Service | Local URL |
|---|---|
| Dashboard | http://localhost:8000 |
| Trino | http://localhost:8080 |
| Iceberg REST catalog config | http://localhost:8181/v1/config |
| RustFS S3 endpoint / console | http://localhost:9000 / http://localhost:9001 |
| Kafka client bootstrap | localhost:9092 |

Inside Docker, producer clients must use **kafka:29092**, not localhost:9092.

## Actual Spark batch backfill

After preparing JSONL, stop the stream before doing a teaching/demo backfill
into its same tables. Concurrent independent writers are outside this demo's
supported operating contract.

```bash
docker compose stop stream
docker compose run --rm runner spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,org.apache.iceberg:iceberg-aws-bundle:1.9.2 jobs/spark_job.py batch --catalog rest --file data/prepared/test.jsonl --model artifacts/forest.json --report reports/rest_batch.json
docker compose start stream
```

If a source filename/row identity already exists, first-write-wins insertion
keeps its previous prediction. Retraining does not retroactively rescore old
records. Use a separate warehouse for model comparisons or explicitly rebuild
the derived silver/gold tables under a controlled migration.

## Optional native Spark ML training

The serving forest JSON was trained with scikit-learn. A **separate real Spark
ML implementation** is provided and can be trained on the prepared splits:

```bash
docker compose run --rm runner python run.py prepare --file data/raw/train.csv --out data/prepared/train.jsonl --split training
docker compose run --rm runner python run.py prepare --file data/raw/test.csv --out data/prepared/test.jsonl --split testing
docker compose run --rm runner spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,org.apache.iceberg:iceberg-aws-bundle:1.9.2 jobs/spark_job.py train --catalog rest
```

It saves `artifacts/spark_model/` and `reports/spark_evaluation.json`. A scoring
job can select it with `--spark-model artifacts/spark_model` instead of
`--model artifacts/forest.json`. The two forests use different implementations
and configurations; their metrics must not be conflated. The dashboard's
Model & data panel explicitly names its saved evaluation engine.

The included Spark evaluation is a **sample validation run**, separate from
the complete scikit-learn evaluation. To recreate its deterministic 3,000 / 1,500
record reservoir samples (rather than taking the first rows):

```bash
python scripts/sample_splits.py
```

Then use `--train data/prepared/ml-validation-train.jsonl --test
data/prepared/ml-validation-test.jsonl` with the native Spark training command.
The held-out Spark sample F1 was about 0.8623 after removing exact feature
duplicates. Sampling and different forest settings prevent a direct accuracy
comparison with the complete serving-model run.

## Native Spark/Iceberg verification without Docker

Install Java 17 and optional dependencies. In a **bash** terminal:

```bash
python -m pip install -r requirements-bigdata.txt
PYTHONPATH=. SPARK_LOCAL_IP=127.0.0.1 PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2 pyspark-shell" python tests/spark_integration.py
```

In PowerShell, set the same variables before running the script:

```powershell
$env:PYTHONPATH = "."
$env:SPARK_LOCAL_IP = "127.0.0.1"
$env:PYSPARK_SUBMIT_ARGS = "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2 pyspark-shell"
python tests/spark_integration.py
```

This uses a temporary **HadoopCatalog** and real local Parquet/Iceberg files;
it is distinct from the REST/S3 catalog used by Docker and Trino. Native Spark
on Windows may require additional Hadoop/winutils setup; Docker avoids that.

## Recovery semantics

- Kafka is a durable log with acknowledged delivery; default retention still
  applies. Spark deliberately uses `failOnDataLoss=true` rather than silently
  skipping expired offsets.
- A checkpoint records the source offsets and query state. Reuse the same
  checkpoint to resume the same query. Do not reuse it for a different topic
  or source type.
- Bronze, silver and quarantine writes use **MERGE on stable event_id**. A
  micro-batch retried after a partial failure can finish without inflating
  these tables. Each table commit is atomic, but the whole micro-batch is not
  a single transaction across all tables.
- Gold marts are rebuilt from accepted silver. The example favors clarity
  over incremental aggregation performance; marts can briefly have different
  snapshot times during refresh. The dashboard uses event time, not arrival
  time, for its chart.
- The 5% quality gate blocks publication above the configured rejected
  fraction. Inspect `lake.quarantine.events` and logs before changing the gate.
- Iceberg catalog metadata, object files and Kafka logs persist in named
  volumes; Spark checkpoints live under `var/checkpoints` on the host. Keep
  these as a matched set. Deleting Kafka/catalog data while retaining a
  checkpoint is not a supported recovery procedure.

## Queries and end-to-end check

```bash
python scripts/query_trino.py
python scripts/stack_smoke.py
```

See `sql/analytics.sql` for protocol statistics, alert inspection, provenance
and Iceberg snapshot history. The smoke test checks row delivery and repeat
replay against Trino; it writes a report only after passing.

## Common startup issues

- **Port 8000 busy:** stop the local demo (Ctrl+C) before running Compose.
- **Docker daemon unavailable:** start Docker Desktop and wait until ready.
- **Out of memory:** increase Docker memory or use the lightweight mode first.
- **Missing tables:** send records and wait for the first successful Spark
  micro-batch; inspect `docker compose logs stream` for quality or connector errors.
- **Maven download problem:** rebuild with working internet. Connector versions
  are pinned and cached in the application image, not downloaded from user code.
- **Dataset checksum failure:** do not bypass it blindly. Use the official
  source or review an intentional data-version change and record new provenance.
