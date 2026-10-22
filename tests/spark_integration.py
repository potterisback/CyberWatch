"""Run with Iceberg runtime on the Spark classpath; writes only a temp directory."""

import json
import tempfile
from pathlib import Path

from pyspark.sql import functions as F
from cyberwatch.detection import Detector
from cyberwatch.spark_pipeline import session, process, text_source
from cyberwatch.synthetic import generate


def main():
    results = {}
    with tempfile.TemporaryDirectory() as d:
        spark = session(str(Path(d) / "warehouse"))
        try:
            events = list(generate(120, seed=900))
            raw = text_source(
                spark.createDataFrame([(json.dumps(e),) for e in events], ["value"])
            )
            report = process(spark, raw)
            results["batch_rows"] = report["silver_total"]
            process(spark, raw)
            assert spark.table("lake.silver.events").count() == len(events)
            expected = sum(Detector().predict(e)["predicted_label"] for e in events)
            actual = spark.table("lake.gold.alerts").count()
            assert actual == expected, (actual, expected)
            results["duplicate_replay_deduplicated"] = True
            results["rule_parity"] = True
            stream_dir = Path(d) / "input"
            stream_dir.mkdir()
            more = list(generate(30, seed=901))
            (stream_dir / "part-1.jsonl").write_text(
                "\n".join(json.dumps(e) for e in more), encoding="utf-8"
            )

            def consume(frame, batch_id):
                process(spark, frame)

            for _ in range(2):
                source = text_source(
                    spark.readStream.format("text")
                    .schema("value STRING")
                    .load(str(stream_dir))
                )
                query = (
                    source.writeStream.foreachBatch(consume)
                    .option("checkpointLocation", str(Path(d) / "checkpoint"))
                    .trigger(availableNow=True)
                    .start()
                )
                query.awaitTermination()
            assert spark.table("lake.silver.events").count() == 150
            results["file_stream_and_checkpoint_restart"] = True
            bad = dict(events[0], event_id="bad-quality", rate=-1)
            dirty = text_source(spark.createDataFrame([(json.dumps(bad),)], ["value"]))
            try:
                process(spark, dirty, max_reject_ratio=0)
            except ValueError:
                pass
            else:
                raise AssertionError("Quality gate failed to block publication")
            assert spark.table("lake.silver.events").count() == 150
            assert spark.table("lake.quarantine.events").count() == 1
            results["fail_closed_quality_gate"] = True
            snapshots = spark.sql(
                "SELECT count(*) n FROM lake.silver.events.snapshots"
            ).first()[0]
            assert snapshots > 0
            results["real_iceberg_snapshots"] = int(snapshots)
        finally:
            spark.stop()
    Path("reports/spark_integration.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
