# CyberWatch — Implementation and Evaluation Report

**Real-Time Network Intrusion Detection and Security Analytics**  
Harry Lebon-Barré · HUST · Big Data Storage and Processing  
Implementation and validation: 22 October 2026

## 1. Problem and objectives

Security analysis requires reliable collection, processing and historical
inspection of network activity. A classifier alone is insufficient if events
are duplicated, malformed, lost during a restart or disconnected from their
source. CyberWatch therefore combines an ingestion pipeline with validation,
binary intrusion detection, persistent analytics tables and an operator-facing
dashboard.

The project objectives are to replay a public network dataset as a stream,
demonstrate the storage and processing technologies covered in the course,
compare repeatable ingestion paths, and evaluate detection on a held-out
dataset. The application observes completed network-flow records. It does not
capture packets from a real network, launch attacks, block connections or claim
production-grade intrusion prevention.

## 2. Data and preparation

UNSW-NB15 is the selected academic dataset. The originating university describes
2,540,044 records with normal traffic and nine attack categories. This project
uses the classification subsets for measured training and evaluation, while its
CSV reader also supports the four headerless full-dataset files.

The included measured run uses a 175,341-row training partition and an
82,332-row evaluation partition. Downloads are explicit, pinned to a public
mirror commit and checked with SHA-256. The mirror has reversed train/test
filenames relative to the sizes published by UNSW; the downloader normalizes
the roles and records the mapping. Hashes pin the chosen mirror artifact and
are not independent university authentication. `docs/DATA.md` documents the
source and provides the bibliography requested by the dataset authors.

Each record is converted to a shared event contract. It includes a stable
identifier, timestamp, source provenance, protocol/service/state and 21 numeric
measurements. Missing or non-finite detector inputs are rejected rather than
quietly filled with synthetic values. Ground-truth labels and attack categories
remain available for evaluation but are excluded from the detector inputs.

The published classification CSVs lack original timestamps and IP addresses.
The pipeline marks replay timestamps as simulated and leaves addresses null.
Consequently, charts show a controlled replay timeline, not the original
chronology of the captured attacks. A separate deterministic synthetic generator
is used for offline demonstrations and integration tests. Its records are
explicitly labeled and do not contribute to the reported real-data accuracy.

## 3. Architecture and implementation

### Distributed execution path

The producer reads CSV or canonical JSONL records and sends keyed events into
a three-partition Kafka topic. Producer delivery is acknowledged; failed sends
raise an error. Spark Structured Streaming consumes the topic and processes
micro-batches through a shared transformation function also used by batch jobs.

The lakehouse comprises four logical namespaces:

| Layer | Responsibility |
|---|---|
| Bronze | Original payload, receipt time, Kafka topic/partition/offset |
| Quarantine | Invalid records and a validation failure reason |
| Silver | Validated events, provenance and detector output |
| Gold | Per-minute traffic, protocol statistics and suspicious-event mart |

These are genuine Iceberg tables, with Parquet data files. Silver is partitioned
by event day. The Docker configuration uses S3-compatible RustFS storage and an
Iceberg REST catalog backed by persistent JDBC/SQLite metadata. Spark and Trino
use the same catalog and object files. Trino exposes SQL analytics to the
read-only dashboard API.

The example deliberately runs a single Kafka broker and Spark `local[2]`
inside its application service, making it usable on a student workstation.
The engines support distributed execution, but this deployment is not a
multi-node throughput or high-availability experiment. A separate Spark master
can be selected through the job configuration.

### Reliability and data quality

Stable identifiers make bronze, silver and quarantine insertion retry-safe via
Iceberg MERGE operations. Replaying a canonical source preserves identifiers
and therefore does not increase accepted-row counts. Spark checkpoints retain
the stream's consumed offsets. A retry can complete the remaining table writes
after a partial failure.

This is **per-table idempotency**, not a transaction spanning the entire
lakehouse. Gold marts are refreshed from silver after a successful quality
gate. The default gate blocks publication if more than 5% of the current
micro-batch fails validation. Bronze and quarantine still preserve the evidence
of that failure. Gold refreshes are separate commits and can briefly have
different snapshot times.

The integration tests exercise the real mechanisms, including checkpoint
restart. They detected a session-boundary issue in Spark foreachBatch: temporary
views must be registered and queried in the micro-batch DataFrame's own
SparkSession. The final implementation consistently uses that session.

### Local reference path

An alternative standard-library implementation stores the same event contract
in SQLite. It provides transactional ingestion, deduplication, quarantine,
baseline detection and the same dashboard interface. It runs without Docker
and supports offline review. The dashboard labels this backend explicitly;
SQLite is not presented as a Kafka, Spark or Iceberg substitute.

## 4. Detection and evaluation methodology

The transparent baseline flags a high packet-rate/packet-count combination or
high repeated-connection indicators. These rules are illustrative and their
fixed risk values are not calibrated probabilities. They are not used to
produce the measured random-forest results below.

The included serving model is a seeded scikit-learn random forest with 64 trees,
maximum depth 14, minimum leaf size 3 and balanced-subsample weighting. Numeric
features use `log1p`; categorical features use fixed one-hot vocabularies with
an `other` category. No label, attack category, address, filename, timestamp,
record ID or split name is included in the feature vector.

To reduce exact-copy leakage, training records whose engineered feature vector
also appears in the evaluation file are removed before fitting. This removes
27,807 training rows, leaving 147,534. It does not establish independence between
attack campaigns or capture environments; that remains a limitation of the
original data and split design.

The decision threshold is fixed at 0.5. Evaluation labels are not used to tune
the threshold or hyperparameters. The trained trees are exported as JSON, and
portable inference is checked against scikit-learn on 200 held-out records.
A separate native Spark ML implementation trains, persists and loads a Spark
PipelineModel; its reports are distinct from the portable serving model.

## 5. Measured results

The included complete evaluation on 82,332 held-out records produced:

| Metric | Value |
|---|---:|
| Precision | 0.873794 |
| Recall | 0.944785 |
| F1 | 0.907904 |
| Balanced accuracy | 0.888798 |
| ROC AUC | 0.979343 |
| Average precision | 0.985181 |

| Actual / Predicted | Normal | Attack |
|---|---:|---:|
| Normal | 30,814 | 6,186 |
| Attack | 2,503 | 42,829 |

The model detects many labeled attacks, but the 6,186 false positives are an
important practical limitation. A high aggregate AUC does not remove the
operational cost of false alarms. The dominant `sttl` and `ct_state_ttl`
features may partly reflect characteristics of the capture environment.

The authoritative run evidence is `reports/evaluation.json`: exact metrics,
confusion matrix, sample counts, input hashes, seed, software versions and
portable-inference parity. The training run took approximately 22.8 seconds
in the Work runtime, including data reading, feature handling, fitting,
evaluation and model export. This timing is environment-specific.

A separate local ingestion benchmark over 5,000 predecoded real records found
consistent alert counts between a single transaction and micro-batches.
`reports/local_benchmark.json` contains the actual timings. Those timings
describe the SQLite reference and must not be presented as distributed Spark
throughput. The first-N subset and untuned rules are not an accuracy study.

## 6. Verification and reproducibility

The core automated suite covers the canonical schema, original/full CSV parsing,
non-finite values, label independence, replay deduplication, atomic local rollback,
batch/micro-batch parity, protocol filters, HTTP routes and model export parity.

Native integration checks use an actual Spark JVM, the Iceberg runtime and
real temporary table snapshots. A file-source Structured Streaming test checks
checkpoint restart and a fail-closed quality gate. A separate native Kafka
integration uses an actual broker, all three partitions, acknowledged replay,
the portable forest and Spark writes into Iceberg; repeated delivery must keep
60 unique silver records. These tests are distinct from Docker orchestration.

The dashboard was exercised in headless Chromium at desktop and mobile widths,
including tabs, protocol filtering, alert search and the evaluation panel.
It uses no remote chart or font dependency. The core and Spark/Iceberg tests
are also configured in GitHub Actions for repository validation.

The Docker/REST/S3/Trino stack is implemented but could not be executed as a
Compose deployment inside Work because no Docker executable was available.
`scripts/stack_smoke.py` is provided for the target machine. `docs/VALIDATION.md`
records exactly what was run and avoids treating that remaining check as passed.

## 7. Limitations and extensions

The full 2.54-million-record collection was not processed in the included
evaluation. Historical laboratory data, possible capture artifacts, original
split dependence and simulated replay timestamps limit generalization claims.
The classifier makes a binary decision rather than attributing attack families.

Gold tables are fully refreshed after each accepted micro-batch for clarity.
At larger scale, incremental aggregates, file compaction, retention management,
model/version lineage and sustained-load testing would be appropriate next
steps. A production deployment would also require resilient brokers, distributed
workers, access controls and independent validation on current traffic.

## 8. Reproduction entry points

1. `python run.py demo` for an offline end-to-end demonstration.
2. `python run.py download --mirror` and `python run.py train` for the measured
   serving model, after installing `requirements-ml.txt`.
3. `docker compose up -d --build` and the replay commands in the README for
   distributed execution.
4. `python -m unittest discover -s tests -p "test_*.py" -v` for the core suite.
5. The runbook and `tests/spark_integration.py` for native Iceberg verification.
6. `docs/DEMARRER_ET_GITHUB_FR.md` for extraction, Windows setup and GitHub push.
