"""Train on one file, evaluate once on a distinct held-out file, export JSON.

Only training requires scikit-learn. Inference uses a portable tree format,
with a probability parity check against the fitted scikit-learn estimator.
"""

import hashlib
import json
import platform
from pathlib import Path
from time import perf_counter

from .detection import Detector
from .schema import FEATURE_NAMES, feature_vector, read_events, utcnow, validate


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def train(
    train_path,
    test_path,
    model_path="artifacts/forest.json",
    report_path="reports/evaluation.json",
    limit=None,
    trees=64,
    seed=42,
):
    import numpy as np
    import sklearn
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import (
        average_precision_score,
        balanced_accuracy_score,
        confusion_matrix,
        precision_recall_fscore_support,
        roc_auc_score,
    )

    if Path(train_path).resolve() == Path(test_path).resolve() or file_hash(
        train_path
    ) == file_hash(test_path):
        raise ValueError("Training and evaluation must use different files")
    start = perf_counter()
    training = [validate(e) for e in read_events(train_path, "training", limit)]
    testing = [validate(e) for e in read_events(test_path, "testing", limit)]
    if not training or not testing:
        raise ValueError("Both files must contain records")
    if {e["label"] for e in training} != {0, 1} or {e["label"] for e in testing} != {
        0,
        1,
    }:
        raise ValueError("Both files must include labeled normal and attack examples")
    # Remove exact feature duplicates across splits to make leakage visible and
    # avoid training on exact copies of evaluation examples. IDs are NOT features.
    from .schema import fingerprint

    test_keys = {fingerprint(feature_vector(e)) for e in testing}
    original_training_rows = len(training)
    training = [e for e in training if fingerprint(feature_vector(e)) not in test_keys]
    if {e["label"] for e in training} != {0, 1}:
        raise ValueError(
            "Insufficient training classes after cross-split deduplication"
        )
    x_train = np.asarray([feature_vector(e) for e in training], dtype=np.float32)
    x_test = np.asarray([feature_vector(e) for e in testing], dtype=np.float32)
    y_train = np.asarray([e["label"] for e in training])
    y_test = np.asarray([e["label"] for e in testing])
    model = RandomForestClassifier(
        n_estimators=trees,
        max_depth=14,
        min_samples_leaf=3,
        class_weight="balanced_subsample",
        n_jobs=2,
        random_state=seed,
    )
    model.fit(x_train, y_train)
    probs = model.predict_proba(x_test)[:, 1]
    predicted = (probs >= 0.5).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, predicted, average="binary", zero_division=0
    )
    portable = dict(
        format="cyberwatch-forest-v1",
        feature_names=FEATURE_NAMES,
        threshold=0.5,
        trained_at=utcnow(),
        seed=seed,
        trees=[],
    )
    for estimator in model.estimators_:
        t = estimator.tree_
        positive = []
        for values in t.value[:, 0, :]:
            positive.append(float(values[1] / sum(values)))
        portable["trees"].append(
            dict(
                left=t.children_left.tolist(),
                right=t.children_right.tolist(),
                feature=t.feature.tolist(),
                threshold=t.threshold.tolist(),
                probability=positive,
            )
        )
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(model_path).write_text(
        json.dumps(portable, separators=(",", ":")), encoding="utf-8"
    )
    detector = Detector(model_path)
    # sklearn converts features to float32 at predict time; the portable path does
    # the same before a threshold comparison (see detection.py).
    portable_probs = [detector.predict(e)["score"] for e in testing[:200]]
    parity_error = float(np.max(np.abs(probs[:200] - portable_probs)))
    if parity_error > 1e-7:
        raise RuntimeError(
            f"Portable model differs from sklearn: max error={parity_error}"
        )
    report = dict(
        dataset="UNSW-NB15"
        if not any(e["is_synthetic"] for e in testing)
        else "SYNTHETIC ONLY",
        engine="scikit-learn RandomForestClassifier",
        generated_at=utcnow(),
        python=platform.python_version(),
        sklearn=sklearn.__version__,
        seed=seed,
        threshold=0.5,
        training_rows=len(training),
        original_training_rows=original_training_rows,
        cross_split_duplicates_removed=original_training_rows - len(training),
        testing_rows=len(testing),
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        balanced_accuracy=float(balanced_accuracy_score(y_test, predicted)),
        roc_auc=float(roc_auc_score(y_test, probs)),
        pr_auc=float(average_precision_score(y_test, probs)),
        pr_auc_definition="average_precision_score",
        confusion_matrix=confusion_matrix(y_test, predicted, labels=[0, 1]).tolist(),
        confusion_matrix_order="rows=true [normal, attack], columns=predicted [normal, attack]",
        training_file=str(Path(train_path).name),
        testing_file=str(Path(test_path).name),
        training_sha256=file_hash(train_path),
        testing_sha256=file_hash(test_path),
        feature_names=FEATURE_NAMES,
        feature_importance=sorted(
            [
                dict(feature=n, importance=float(v))
                for n, v in zip(FEATURE_NAMES, model.feature_importances_)
            ],
            key=lambda r: r["importance"],
            reverse=True,
        )[:12],
        portable_max_error=parity_error,
        elapsed_seconds=round(perf_counter() - start, 3),
        limitations=[
            "Historical laboratory data, not live enterprise traffic.",
            "Accuracy on this split is not proof of operational effectiveness.",
            "Replay timestamps do not recreate original attack chronology.",
            "Threshold fixed at 0.5; no tuning on the held-out evaluation set.",
        ],
    )
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
