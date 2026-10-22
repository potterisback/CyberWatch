"""Actual Spark batch/stream processing into actual Iceberg tables.

Two catalog configurations: local HadoopCatalog for reproducible JVM tests,
and REST + S3 for multi-engine Docker execution. Per-table MERGE is idempotent;
the three layers are separate commits, not a cross-table transaction.
"""

from __future__ import annotations

import json
import os
from functools import reduce
from pathlib import Path
from time import perf_counter

from .schema import CATEGORIES, FEATURE_NAMES, NUMERIC, utcnow


def session(warehouse="var/iceberg", catalog="hadoop", master="local[2]"):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("CyberWatch")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.cache-enabled", "false")
    )
    if catalog == "rest":
        builder = (
            builder.config("spark.sql.catalog.lake.type", "rest")
            .config(
                "spark.sql.catalog.lake.uri",
                os.getenv("ICEBERG_REST_URI", "http://catalog:8181"),
            )
            .config(
                "spark.sql.catalog.lake.warehouse",
                os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            )
            .config(
                "spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
            )
            .config(
                "spark.sql.catalog.lake.s3.endpoint",
                os.getenv("S3_ENDPOINT", "http://object-store:9000"),
            )
            .config("spark.sql.catalog.lake.s3.path-style-access", "true")
            .config(
                "spark.sql.catalog.lake.s3.access-key-id",
                os.getenv("AWS_ACCESS_KEY_ID", "cyberwatch"),
            )
            .config(
                "spark.sql.catalog.lake.s3.secret-access-key",
                os.getenv("AWS_SECRET_ACCESS_KEY", "cyberwatch-local-only-2026"),
            )
        )
    else:
        builder = builder.config("spark.sql.catalog.lake.type", "hadoop").config(
            "spark.sql.catalog.lake.warehouse", Path(warehouse).resolve().as_uri()
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for name in ("bronze", "silver", "gold", "quarantine"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{name}")
    return spark


def event_schema():
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        BooleanType,
        IntegerType,
    )

    strings = [
        "event_id",
        "event_time",
        "source",
        "split",
        "source_file",
        "proto",
        "service",
        "state",
        "src_ip",
        "dst_ip",
        "src_port",
        "dst_port",
        "attack_cat",
    ]
    return StructType(
        [StructField(k, StringType(), True) for k in strings]
        + [StructField(k, DoubleType(), True) for k in NUMERIC]
        + [
            StructField("label", IntegerType(), True),
            StructField("is_synthetic", BooleanType(), True),
            StructField("time_is_simulated", BooleanType(), True),
        ]
    )


def merge_append(spark, frame, table, partition_time=False):
    """Retry-safe insertion keyed by event_id. First accepted payload wins."""
    if not spark.catalog.tableExists(table):
        writer = (
            frame.limit(0)
            .writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
        )
        if partition_time:
            from pyspark.sql.functions import days

            writer = writer.partitionedBy(days("event_time"))
        writer.create()
    temp = "incoming_" + table.replace(".", "_")
    frame.dropDuplicates(["event_id"]).createOrReplaceTempView(temp)
    spark.sql(
        f"MERGE INTO {table} target USING {temp} incoming ON target.event_id=incoming.event_id "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def score(frame, model_path=None, spark_model=None):
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        DoubleType,
        IntegerType,
        StringType,
    )

    if spark_model:
        from pyspark.ml import PipelineModel
        from pyspark.ml.functions import vector_to_array

        scored = PipelineModel.load(spark_model).transform(engineer(frame))
        return (
            scored.withColumn("score", vector_to_array("probability")[1])
            .withColumn("predicted_label", F.col("prediction").cast("int"))
            .withColumn("engine", F.lit("spark_random_forest"))
            .withColumn("reason", F.lit("Spark random forest classification"))
            .drop(
                "features",
                "rawPrediction",
                "probability",
                "prediction",
                *[f"f{i}" for i in range(len(FEATURE_NAMES))],
            )
        )
    if model_path:
        from .detection import Detector

        detector = Detector(model_path)
        result_type = StructType(
            [
                StructField("score", DoubleType()),
                StructField("predicted_label", IntegerType()),
                StructField("engine", StringType()),
                StructField("reason", StringType()),
            ]
        )

        def predict(row):
            result = detector.predict(row.asDict())
            return (
                result["score"],
                result["predicted_label"],
                result["engine"],
                result["reason"],
            )

        predict_udf = F.udf(predict, result_type)
        return (
            frame.withColumn("_prediction", predict_udf(F.struct(*frame.columns)))
            .select("*", "_prediction.*")
            .drop("_prediction")
        )
    dos = (F.col("rate") > 1500) & (F.col("spkts") > 100)
    recon = (F.col("ct_dst_ltm") > 35) & (F.col("ct_src_dport_ltm") > 20)
    return (
        frame.withColumn("score", F.when(dos, 0.95).when(recon, 0.85).otherwise(0.10))
        .withColumn("predicted_label", F.when(dos | recon, 1).otherwise(0))
        .withColumn("engine", F.lit("rules"))
        .withColumn(
            "reason",
            F.when(dos, "High packet rate and packet count")
            .when(recon, "Repeated destination / source-port activity")
            .otherwise("No baseline rule matched"),
        )
    )


def refresh_gold(spark):
    from pyspark.sql import functions as F

    silver = spark.table("lake.silver.events")
    minute = silver.groupBy(
        F.date_format("event_time", "yyyy-MM-dd'T'HH:mm").alias("minute"), "proto"
    ).agg(F.count("*").alias("events"), F.sum("predicted_label").alias("alerts"))
    protocol = silver.groupBy("proto").agg(
        F.count("*").alias("events"), F.sum("predicted_label").alias("alerts")
    )
    for name, frame in [
        ("minute_stats", minute),
        ("protocol_stats", protocol),
        ("alerts", silver.filter(F.col("predicted_label") == 1)),
    ]:
        table = f"lake.gold.{name}"
        writer = (
            frame.writeTo(table).using("iceberg").tableProperty("format-version", "2")
        )
        if spark.catalog.tableExists(table):
            writer.overwrite(F.lit(True))
        else:
            writer.create()


def process(spark, raw, model_path=None, spark_model=None, max_reject_ratio=0.05):
    from pyspark.sql import functions as F

    # foreachBatch uses a separate query session. Register temporary views and
    # execute MERGE in that same session, otherwise a replay can see stale views.
    spark = raw.sparkSession
    started = perf_counter()
    # Keep original payload and Kafka provenance in bronze before parsing.
    bronze = raw.select(
        F.coalesce(
            F.get_json_object("raw_json", "$.event_id"), F.sha2("raw_json", 256)
        ).alias("event_id"),
        F.current_timestamp().alias("received_at"),
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    )
    bronze = bronze.persist()
    received = bronze.count()
    try:
        merge_append(spark, bronze, "lake.bronze.events")
        parsed = bronze.select(
            F.from_json("raw_json", event_schema()).alias("e"),
            "raw_json",
            F.col("event_id").alias("bronze_id"),
            "received_at",
        ).select("e.*", "raw_json", "bronze_id", "received_at")
        parsed = parsed.withColumn("event_time", F.to_timestamp("event_time"))
        required = [
            F.col(k).isNotNull() & (F.length(F.trim(F.col(k))) > 0)
            for k in ("event_id", "source", "proto", "service", "state")
        ]
        checks = required + [
            F.col("event_time").isNotNull(),
            F.col("is_synthetic").isNotNull(),
            F.col("label").isNull() | F.col("label").isin(0, 1),
        ]
        for name in NUMERIC:
            checks.append(
                F.col(name).isNotNull()
                & ~F.isnan(name)
                & (F.col(name) >= 0)
                & (F.col(name) < float("inf"))
            )
        parsed = parsed.withColumn(
            "_valid", F.coalesce(reduce(lambda a, b: a & b, checks), F.lit(False))
        ).persist()
        try:
            invalid = parsed.filter(~F.col("_valid")).select(
                F.col("bronze_id").alias("event_id"),
                "received_at",
                "raw_json",
                F.lit("Schema or value constraint failed").alias("reason"),
            )
            rejected = invalid.count()
            merge_append(spark, invalid, "lake.quarantine.events")
            if received and rejected / received > max_reject_ratio:
                raise ValueError(
                    f"Quality gate failed: {rejected}/{received} rejected; silver/gold publication blocked"
                )
            valid = (
                parsed.filter("_valid")
                .drop("_valid", "raw_json", "bronze_id")
                .withColumnRenamed("received_at", "ingested_at")
            )
            scored = score(valid, model_path, spark_model)
            merge_append(spark, scored, "lake.silver.events", partition_time=True)
            refresh_gold(spark)
            accepted = spark.table("lake.silver.events").count()
            return dict(
                received=received,
                rejected=rejected,
                silver_total=accepted,
                elapsed_seconds=round(perf_counter() - started, 3),
                backend="spark-iceberg",
                generated_at=utcnow(),
            )
        finally:
            parsed.unpersist()
    finally:
        bronze.unpersist()


def text_source(frame):
    from pyspark.sql import functions as F

    return frame.select(
        F.col("value").alias("raw_json"),
        F.lit(None).cast("string").alias("kafka_topic"),
        F.lit(None).cast("int").alias("kafka_partition"),
        F.lit(None).cast("long").alias("kafka_offset"),
    )


def engineer(frame):
    from pyspark.sql import functions as F

    expressions = [F.log1p(F.col(name)).cast("double") for name in NUMERIC]
    for key, choices in CATEGORIES.items():
        value = F.when(F.col(key).isin(*choices), F.col(key)).otherwise("other")
        expressions.extend((value == choice).cast("double") for choice in choices)
    return frame.select(
        "*", *[expr.alias(f"f{i}") for i, expr in enumerate(expressions)]
    )


def train_spark(
    spark,
    train_path,
    test_path,
    model_path="artifacts/spark_model",
    report_path="reports/spark_evaluation.json",
):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F
    from .ml import file_hash

    if Path(train_path).resolve() == Path(test_path).resolve() or file_hash(
        train_path
    ) == file_hash(test_path):
        raise ValueError("Use distinct training and evaluation files")
    columns = [f"f{i}" for i in range(len(FEATURE_NAMES))]
    training = engineer(spark.read.schema(event_schema()).json(train_path))
    testing = engineer(spark.read.schema(event_schema()).json(test_path)).cache()
    training = training.join(
        testing.select(*columns).distinct(), on=columns, how="left_anti"
    ).cache()
    try:
        for data in (training, testing):
            labels = {r[0] for r in data.select("label").distinct().collect()}
            if labels != {0, 1}:
                raise ValueError("Both files must contain normal and attack labels")
        assembler = VectorAssembler(
            inputCols=columns, outputCol="features", handleInvalid="error"
        )
        forest = RandomForestClassifier(
            featuresCol="features", labelCol="label", numTrees=40, maxDepth=10, seed=42
        )
        model = Pipeline(stages=[assembler, forest]).fit(training)
        model.write().overwrite().save(model_path)
        predictions = model.transform(testing).cache()
        try:
            counts = {
                (int(r["label"]), int(r["prediction"])): r["count"]
                for r in predictions.groupBy("label", "prediction").count().collect()
            }
            tn, fp, fn, tp = (
                counts.get(k, 0) for k in [(0, 0), (0, 1), (1, 0), (1, 1)]
            )
            precision = tp / max(tp + fp, 1)
            recall = tp / max(tp + fn, 1)
            report = dict(
                dataset="UNSW-NB15"
                if not testing.filter("is_synthetic").take(1)
                else "SYNTHETIC ONLY",
                engine="Spark ML RandomForestClassifier",
                generated_at=utcnow(),
                seed=42,
                training_rows=training.count(),
                testing_rows=testing.count(),
                threshold=0.5,
                precision=precision,
                recall=recall,
                f1=2 * precision * recall / max(precision + recall, 1e-9),
                roc_auc=BinaryClassificationEvaluator(
                    metricName="areaUnderROC"
                ).evaluate(predictions),
                pr_auc=BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(
                    predictions
                ),
                confusion_matrix=[[tn, fp], [fn, tp]],
                feature_names=FEATURE_NAMES,
                training_sha256=file_hash(train_path),
                testing_sha256=file_hash(test_path),
                feature_importance=sorted(
                    [
                        dict(feature=n, importance=float(v))
                        for n, v in zip(
                            FEATURE_NAMES, model.stages[-1].featureImportances
                        )
                    ],
                    key=lambda r: r["importance"],
                    reverse=True,
                )[:12],
            )
            Path(report_path).parent.mkdir(parents=True, exist_ok=True)
            Path(report_path).write_text(json.dumps(report, indent=2), encoding="utf-8")
            return report
        finally:
            predictions.unpersist()
    finally:
        training.unpersist()
        testing.unpersist()
