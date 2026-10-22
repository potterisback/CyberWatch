#!/usr/bin/env python3
"""Submit with spark-submit; use --help for local and REST catalog options."""

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cyberwatch.spark_pipeline import session, process, text_source, train_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["batch", "stream", "train"])
    parser.add_argument(
        "--catalog",
        choices=["hadoop", "rest"],
        default=os.getenv("CYBERWATCH_CATALOG", "hadoop"),
    )
    parser.add_argument("--warehouse", default="var/iceberg")
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[2]"))
    parser.add_argument("--file")
    parser.add_argument("--model", help="Portable forest JSON; optional")
    parser.add_argument("--spark-model", help="Saved Spark PipelineModel; optional")
    parser.add_argument("--report", default="reports/spark_batch.json")
    parser.add_argument("--source", choices=["kafka", "files"], default="kafka")
    parser.add_argument("--input-dir", default="data/stream")
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default="cyberwatch.flows")
    parser.add_argument("--checkpoint", default="var/checkpoints/main")
    parser.add_argument(
        "--available-now", action="store_true", help="Drain existing data then stop"
    )
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--max-reject-ratio", type=float, default=0.05)
    parser.add_argument("--train", default="data/prepared/train.jsonl")
    parser.add_argument("--test", default="data/prepared/test.jsonl")
    parser.add_argument("--model-out", default="artifacts/spark_model")
    args = parser.parse_args()
    if not 0 <= args.max_reject_ratio <= 1:
        parser.error("--max-reject-ratio must be between 0 and 1")
    if args.model and args.spark_model:
        parser.error("Choose one model format")
    spark = session(args.warehouse, args.catalog, args.master)
    try:
        if args.mode == "train":
            report = train_spark(
                spark,
                args.train,
                args.test,
                args.model_out,
                "reports/spark_evaluation.json",
            )
        elif args.mode == "batch":
            if not args.file:
                parser.error("--file is required for batch mode")
            report = process(
                spark,
                text_source(spark.read.text(args.file)),
                args.model,
                args.spark_model,
                args.max_reject_ratio,
            )
        else:
            if args.source == "files":
                raw = text_source(
                    spark.readStream.format("text")
                    .schema("value STRING")
                    .option("maxFilesPerTrigger", 1)
                    .load(args.input_dir)
                )
            else:
                from pyspark.sql import functions as F

                raw = (
                    spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", args.bootstrap)
                    .option("subscribe", args.topic)
                    .option("startingOffsets", "earliest")
                    .option("maxOffsetsPerTrigger", 5000)
                    .option("failOnDataLoss", "true")
                    .load()
                    .select(
                        F.col("value").cast("string").alias("raw_json"),
                        F.col("topic").alias("kafka_topic"),
                        F.col("partition").alias("kafka_partition"),
                        F.col("offset").alias("kafka_offset"),
                    )
                )

            def consume(frame, batch_id):
                result = process(
                    spark, frame, args.model, args.spark_model, args.max_reject_ratio
                )
                result["micro_batch_id"] = batch_id
                Path(args.report).parent.mkdir(parents=True, exist_ok=True)
                Path(args.report).write_text(
                    json.dumps(result, indent=2), encoding="utf-8"
                )
                print(json.dumps(result), flush=True)

            writer = raw.writeStream.foreachBatch(consume).option(
                "checkpointLocation", args.checkpoint
            )
            query = (
                writer.trigger(availableNow=True)
                if args.available_now
                else writer.trigger(processingTime=args.trigger)
            ).start()
            query.awaitTermination()
            return
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        if args.mode != "train":
            Path(args.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
