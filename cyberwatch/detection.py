"""Transparent baseline plus dependency-free inference for exported forests."""

import json
import struct
from pathlib import Path

from .schema import FEATURE_NAMES, feature_vector


class Detector:
    def __init__(self, model_path=None, threshold=0.5):
        if not 0 < threshold < 1:
            raise ValueError("threshold must be between zero and one")
        self.threshold = threshold
        self.model = None
        if model_path:
            self.model = json.loads(Path(model_path).read_text(encoding="utf-8"))
            if self.model.get("feature_names") != FEATURE_NAMES:
                raise ValueError(
                    "Model feature contract does not match this application"
                )
            if self.model.get("format") != "cyberwatch-forest-v1":
                raise ValueError("Unsupported model format")

    def predict(self, event):
        # Neither branch reads label, attack_cat, event_id, addresses, or split.
        if self.model:
            x = [
                struct.unpack("f", struct.pack("f", v))[0]
                for v in feature_vector(event)
            ]
            probabilities = []
            for tree in self.model["trees"]:
                node = 0
                while tree["left"][node] != -1:
                    node = (
                        tree["left"][node]
                        if x[tree["feature"][node]] <= tree["threshold"][node]
                        else tree["right"][node]
                    )
                probabilities.append(tree["probability"][node])
            score = sum(probabilities) / len(probabilities)
            return dict(
                score=score,
                predicted_label=int(score >= self.threshold),
                engine="random_forest",
                reason="Random forest probability above threshold"
                if score >= self.threshold
                else "Random forest probability below threshold",
            )
        if event["rate"] > 1500 and event["spkts"] > 100:
            return dict(
                score=0.95,
                predicted_label=1,
                engine="rules",
                reason="High packet rate and packet count",
            )
        if event["ct_dst_ltm"] > 35 and event["ct_src_dport_ltm"] > 20:
            return dict(
                score=0.85,
                predicted_label=1,
                engine="rules",
                reason="Repeated destination / source-port activity",
            )
        return dict(
            score=0.10,
            predicted_label=0,
            engine="rules",
            reason="No baseline rule matched",
        )
