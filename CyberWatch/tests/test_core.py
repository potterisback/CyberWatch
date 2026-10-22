import csv
import json
import math
import tempfile
import threading
import unittest
from copy import deepcopy
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen
from urllib.error import HTTPError

from cyberwatch.detection import Detector
from cyberwatch.schema import (
    CATEGORIES,
    FEATURE_NAMES,
    FULL_COLUMNS,
    NUMERIC,
    csv_events,
    feature_vector,
    validate,
)
from cyberwatch.server import handler
from cyberwatch.storage import Store
from cyberwatch.synthetic import generate


class ContractTests(unittest.TestCase):
    def test_deterministic_events_and_features(self):
        a, b = list(generate(30)), list(generate(30))
        self.assertEqual(a, b)
        self.assertEqual(len(set(e["event_id"] for e in a)), 30)
        self.assertEqual(len(feature_vector(a[0])), len(FEATURE_NAMES))
        self.assertTrue(all(math.isfinite(v) for v in feature_vector(a[0])))

    def test_predictions_do_not_read_ground_truth_or_ids(self):
        detector = Detector()
        for event in generate(100):
            other = dict(
                event,
                label=1 - event["label"],
                attack_cat="Changed",
                event_id="changed",
                split="testing",
            )
            self.assertEqual(detector.predict(event), detector.predict(other))

    def test_reject_bad_numbers_timestamps_and_labels(self):
        event = next(generate(1))
        for name, value in [
            ("rate", float("nan")),
            ("sbytes", -1),
            ("dur", float("inf")),
            ("label", 2),
            ("event_time", "2026-01-01T10:00:00"),
            ("proto", ""),
        ]:
            with self.subTest(name=name), self.assertRaises((ValueError, TypeError)):
                validate(dict(event, **{name: value}))

    def test_official_headered_split_has_no_invented_addresses(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "split.csv"
            row = {k: 1 for k in NUMERIC}
            row.update(
                id=1,
                proto="tcp",
                service="http",
                state="FIN",
                label=0,
                attack_cat="Normal",
            )
            with p.open("w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=list(row))
                w.writeheader()
                w.writerow(row)
            event = next(csv_events(p))
            self.assertIsNone(event["src_ip"])
            self.assertTrue(event["time_is_simulated"])
            self.assertFalse(event["is_synthetic"])

    def test_full_headerless_csv_preserves_original_time(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "UNSW-NB15_1.csv"
            row = {k: "1" for k in FULL_COLUMNS}
            row.update(
                srcip="192.0.2.4",
                dstip="198.51.100.8",
                proto="tcp",
                service="http",
                state="FIN",
                stime="1421927414",
                ltime="1421927415",
                attack_cat="Normal",
                label="0",
            )
            with p.open("w", newline="") as f:
                csv.writer(f).writerow([row[k] for k in FULL_COLUMNS])
            event = next(csv_events(p))
            self.assertFalse(event["time_is_simulated"])
            self.assertEqual(event["src_ip"], "192.0.2.4")
            self.assertTrue(event["event_time"].startswith("2015-"))


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "test.db")

    def tearDown(self):
        self.temp.cleanup()

    def test_quarantine_and_replay_do_not_inflate_counts(self):
        rows = list(generate(50))
        bad = dict(rows[0], event_id="invalid", rate=-1)
        first = self.store.ingest(rows + [bad], Detector())
        second = self.store.ingest(rows + [bad], Detector())
        self.assertEqual(first["inserted"], 50)
        self.assertEqual(first["rejected"], 1)
        self.assertEqual(second["duplicates"], 51)
        data = self.store.dashboard()
        self.assertEqual(data["summary"]["total_events"], 50)
        self.assertEqual(data["summary"]["bronze_count"], 51)
        self.assertEqual(data["summary"]["rejected_count"], 1)
        self.assertEqual(sum(r["events"] for r in data["timeline"]), 50)

    def test_nonfinite_and_nonobject_records_are_quarantined(self):
        bad = dict(next(generate(1)), rate=float("nan"))
        malformed_id = dict(next(generate(1)), event_id={"not": "a string"})
        result = self.store.ingest([bad, None, malformed_id], Detector())
        self.assertEqual(result["rejected"], 3)

    def test_batch_and_microbatch_have_same_results(self):
        rows = list(generate(120))
        self.store.ingest(rows, Detector())
        other = Store(Path(self.temp.name) / "micro.db")
        for i in range(0, len(rows), 13):
            other.ingest(rows[i : i + 13], Detector())
        a, b = self.store.dashboard(), other.dashboard()
        for key in ("total_events", "alert_count", "total_bytes"):
            self.assertEqual(a["summary"][key], b["summary"][key])
        self.assertEqual(a["protocols"], b["protocols"])

    def test_failed_batch_rolls_back_atomically(self):
        class BrokenDetector:
            calls = 0

            def predict(self, event):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("worker failure")
                return Detector().predict(event)

        with self.assertRaises(RuntimeError):
            self.store.ingest(list(generate(5)), BrokenDetector())
        self.assertEqual(self.store.dashboard()["summary"]["bronze_count"], 0)

    def test_protocol_filter_is_parameterized(self):
        self.store.ingest(list(generate(100)), Detector())
        self.assertEqual(
            self.store.dashboard("tcp' OR 1=1 --")["summary"]["total_events"], 0
        )
        self.assertGreater(self.store.dashboard("tcp")["summary"]["total_events"], 0)

    def test_http_endpoints_and_static_whitelist(self):
        self.store.ingest(list(generate(20)), Detector())
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), handler(self.store, "does-not-exist.json")
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            with urlopen(base + "/api/dashboard") as r:
                self.assertEqual(json.load(r)["summary"]["total_events"], 20)
            with urlopen(base + "/") as r:
                self.assertIn(b"CYBERWATCH", r.read())
                self.assertIn("default-src", r.headers["Content-Security-Policy"])
            with self.assertRaises(HTTPError) as exc:
                urlopen(base + "/../run.py")
            self.assertEqual(exc.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ModelTests(unittest.TestCase):
    def test_exported_model_and_label_independence(self):
        try:
            import sklearn
        except ImportError:
            self.skipTest("optional scikit-learn dependency not installed")
        from cyberwatch.ml import train

        with tempfile.TemporaryDirectory() as d:
            paths = [
                Path(d) / name
                for name in ["train.jsonl", "test.jsonl", "model.json", "report.json"]
            ]
            for path, seed, split in [
                (paths[0], 1, "training"),
                (paths[1], 2, "testing"),
            ]:
                path.write_text(
                    "\n".join(json.dumps(e) for e in generate(300, seed, split)),
                    encoding="utf-8",
                )
            report = train(paths[0], paths[1], paths[2], paths[3], trees=8)
            self.assertEqual(report["dataset"], "SYNTHETIC ONLY")
            self.assertLess(report["portable_max_error"], 1e-7)
            detector = Detector(paths[2])
            e = next(generate(1))
            self.assertEqual(
                detector.predict(e),
                detector.predict(dict(e, label=1 - e["label"], attack_cat="changed")),
            )
            with self.assertRaises(ValueError):
                train(paths[0], paths[0], paths[2], paths[3])


if __name__ == "__main__":
    unittest.main()
