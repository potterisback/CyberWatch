"""Explicit, checksum-pinned download from a public mirror.

The original university page currently redirects dataset downloads to sign-in.
The mirror's split filenames are reversed relative to the sizes published by
UNSW. We normalize their roles here and record that choice in provenance.json.
"""

import csv
import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen

from .schema import utcnow

COMMIT = "161024104f19eea98b5c9aba0499a8fcb867ba1d"
ORIGIN = "https://research.unsw.edu.au/projects/unsw-nb15-dataset"
BASE = f"https://media.githubusercontent.com/media/jamshaid120/UNSW_NB15-Complete-dataset/{COMMIT}"
FILES = {
    "train.csv": (
        "UNSW_NB15_testing-set.csv",
        "bec7dd5ec88dc2a0ccc7a07879d338395ed7421750f675fd0339e07dfe0648fa",
        175341,
    ),
    "test.csv": (
        "UNSW_NB15_training-set.csv",
        "734fe6642edf758f7c94d7d9149426b49d202fe8e7bf0bef47392489c3c0a559",
        82332,
    ),
}


def download(destination="data/raw"):
    directory = Path(destination)
    directory.mkdir(parents=True, exist_ok=True)
    results = []
    for local_name, (remote_name, expected, expected_rows) in FILES.items():
        path = directory / local_name
        url = f"{BASE}/{remote_name}"
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == expected:
            print(f"Verified existing {path}", flush=True)
        else:
            print(
                f"Downloading {local_name} from checksum-pinned mirror...", flush=True
            )
            temporary = path.with_suffix(".download")
            try:
                digest = hashlib.sha256()
                with urlopen(
                    Request(url, headers={"User-Agent": "CyberWatch-Academic/1.0"}),
                    timeout=90,
                ) as response:
                    with temporary.open("wb") as target:
                        while block := response.read(1024 * 1024):
                            digest.update(block)
                            target.write(block)
                if digest.hexdigest() != expected:
                    raise ValueError(
                        f"Checksum mismatch for {remote_name}; refusing changed data"
                    )
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
        with path.open(newline="", encoding="utf-8-sig") as stream:
            rows = csv.DictReader(stream)
            if not {"label", "attack_cat", "proto", "dur"}.issubset(
                rows.fieldnames or []
            ):
                raise ValueError(f"Invalid dataset header: {path}")
            count = sum(1 for _ in rows)
        if count != expected_rows:
            raise ValueError(f"Unexpected row count for {path}: {count}")
        results.append(
            dict(
                path=str(path),
                mirror_filename=remote_name,
                rows=count,
                sha256=expected,
                download_url=url,
            )
        )
    manifest = dict(
        dataset="UNSW-NB15",
        downloaded_at=utcnow(),
        official_source=ORIGIN,
        mirror_commit=COMMIT,
        files=results,
        note="Mirror filenames use reversed split naming. train.csv is the 175341-row split; test.csv is the 82332-row split. Checksums pin mirror bytes, not independent authentication by UNSW.",
    )
    (directory / "provenance.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest
