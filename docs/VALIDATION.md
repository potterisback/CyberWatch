# Execution evidence

Validated in the Work runtime on 22 October 2026. Generated JSON reports contain
the actual execution results. No Docker/Trino pass is implied by native tests.

| Check | Evidence / result |
|---|---|
| Offline demo | 2,500 generated synthetic records; 581 baseline rule alerts |
| Core automated suite | 12 unittest cases passed, including optional model training/export |
| Full real-data serving-model run | 175,341 source training rows; 147,534 after cross-split feature deduplication; 82,332 held-out rows |
| Portable inference parity | Maximum absolute probability difference about 1.2e-15 against scikit-learn on 200 held-out records |
| Real-data local inference | 5,000 CSV records processed with the trained model |
| Actual Spark/Iceberg batch | 600 synthetic records written to real Iceberg tables |
| Native Iceberg integration | Batch, duplicate replay, file-source Structured Streaming, checkpoint restart and quality gate passed; see `reports/spark_integration.json` |
| Actual Kafka integration | Real Kafka 3.9.1 broker → Spark 3.5.7 → local Iceberg; 60 unique records across three partitions; repeat replay deduplicated; portable forest scores matched local inference |
| Real-data Spark inference | First 1,000 evaluation records ingested and scored; see `reports/spark_real_sample.json` |
| Native Spark model persistence | Saved Spark PipelineModel reloaded and scored 1,000 real evaluation records; see `reports/spark_native_score_sample.json` |
| Native Spark ML | Actual training and held-out evaluation on reservoir samples: 3,000 source training rows, 2,820 after feature deduplication, 1,500 evaluation rows; sample F1 about 0.8623. See `reports/spark_evaluation.json` |
| Browser behavior | Desktop 1440px and mobile 390px; no page/console errors; filtering, alert search, tabs, metrics rendering and no mobile page overflow |
| Dependency images | Pinned Kafka 3.9.1, Iceberg REST fixture 1.9.2, RustFS 1.0.0 and Trino 475 manifests resolved during preparation |
| Docker Compose / REST-S3 / Trino | Configured, **not executed in Work**; Docker is unavailable. Run `python scripts/stack_smoke.py` after starting the stack |

## Native runtime

- Python 3.12.14 and OpenJDK 17.
- scikit-learn 1.7.2 for the included forest; dependencies installed into an
  isolated validation environment.
- Spark 3.5.7 with Iceberg Spark runtime 1.9.2 / Scala 2.12.
- Kafka 3.9.1 from the Apache binary distribution; Python kafka-python 2.2.15.
- Actual Kafka native test used a temporary broker and HadoopCatalog, rather
  than claiming that the Docker REST catalog or object store was exercised.
- Browser checks used Playwright 1.56.1 / Chromium 141, with locally generated
  demonstration data.

## CI package-import correction — 22 October 2026

A supplied GitHub Actions screenshot shows `ModuleNotFoundError: No module
named 'cyberwatch'` while importing `tests/test_core.py`. All 12 tests pass
with the complete original ZIP in Work, so the screenshot alone does not
identify whether the remote source folder is missing, misnamed or outside
Python's import path. The remote repository contents have not been inspected.

Both workflow jobs now check that `pyproject.toml` and the lowercase
`cyberwatch/` source package are present at the repository root. They install
the project with its declared optional dependencies before running tests,
and verify imports using Python's isolated mode (`-I`). A missing package
produces an actionable source-layout error.

Validation of this correction in Work:

- A fresh virtual environment installed the project with
  `python -m pip install -e ".[ml]"` (Python 3.12.14, scikit-learn 1.7.2).
- The installed package imports outside the source directory with `-I`.
- All 12 unit tests pass with `-I` and no `PYTHONPATH` override.
- The offline demo command and source compilation complete successfully.
- The Big Data environment installs `.[bigdata]` and imports the Spark
  pipeline outside the source directory with `-I`.
- The source-layout check rejects an empty directory with its intended error.

The updated workflow has not been run on GitHub in this session. The full
Spark integration was not repeated for this workflow-only change; its earlier
execution evidence is listed above. Python 3.10 and 3.11 remain in the CI
matrix, but were not available for local reruns of this correction.

## Reading the reports correctly

`evaluation.json` is the complete real-data **scikit-learn** evaluation. It is
distinct from optional native Spark ML results. `local_benchmark.json` measures
SQLite ingestion, not a distributed benchmark. `spark_smoke.json` and
`spark_integration.json` are functionality checks, predominantly with synthetic
records. `kafka_integration.json` verifies an actual broker, Spark and Iceberg,
and is not a claim about Trino or the S3-backed deployment.

Integration tests use temporary warehouses and do not include those generated
databases, checkpoints or raw records in the ZIP or Git repository. The saved
JSON forest and the measured reports are included for review.
