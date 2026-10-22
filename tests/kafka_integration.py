"""Actual Kafka -> Spark -> local Iceberg test; requires a running Kafka broker.

Run with the Spark Kafka + Iceberg connectors on the classpath. This test uses
a unique topic and temporary warehouse, leaving application tables untouched.
"""

import argparse
import json
import tempfile
import uuid
from pathlib import Path

from pyspark.sql import functions as F
from cyberwatch.detection import Detector
from cyberwatch.producer import produce
from cyberwatch.spark_pipeline import session, process
from cyberwatch.synthetic import generate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()
    result = {"backend": "actual Kafka and Spark; local HadoopCatalog Iceberg"}
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "events.jsonl"
        rows = list(generate(60, seed=2410, split="kafka-integration"))
        path.write_text("\n".join(json.dumps(e) for e in rows), encoding="utf-8")
        topic = "cyberwatch.integration." + uuid.uuid4().hex[:12]
        delivery = produce(path, args.bootstrap, topic, rate=10000)
        assert delivery["acknowledged"] == 60
        spark = session(str(Path(d) / "warehouse"))
        try:
            for run in range(2):
                if run:
                    produce(path, args.bootstrap, topic, rate=10000)
                source = (
                    spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", args.bootstrap)
                    .option("subscribe", topic)
                    .option("startingOffsets", "earliest")
                    .load()
                    .select(
                        F.col("value").cast("string").alias("raw_json"),
                        F.col("topic").alias("kafka_topic"),
                        F.col("partition").alias("kafka_partition"),
                        F.col("offset").alias("kafka_offset"),
                    )
                )

                def consume(frame, batch_id):
                    process(spark, frame, model_path="artifacts/forest.json")

                query = (
                    source.writeStream.foreachBatch(consume)
                    .option("checkpointLocation", str(Path(d) / "checkpoint"))
                    .trigger(availableNow=True)
                    .start()
                )
                query.awaitTermination()
                assert spark.table("lake.silver.events").count() == 60
            actual = {
                r["event_id"]: r["score"]
                for r in spark.table("lake.silver.events")
                .select("event_id", "score")
                .collect()
            }
            detector = Detector("artifacts/forest.json")
            error = max(
                abs(actual[e["event_id"]] - detector.predict(e)["score"]) for e in rows
            )
            assert error < 1e-7
            result.update(
                acknowledged=delivery["acknowledged"],
                silver_unique_rows=60,
                kafka_replay_deduplicated=True,
                portable_forest_parity_error=error,
                kafka_partitions=spark.table("lake.bronze.events")
                .select("kafka_partition")
                .distinct()
                .count(),
            )
        finally:
            spark.stop()
    Path("reports/kafka_integration.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
