"""Deterministic illustrative traffic, deliberately NOT an UNSW-NB15 sample."""

import random
from datetime import datetime, timedelta, timezone

from .schema import NUMERIC, fingerprint, validate


def generate(count=2500, seed=42, split="demo", start_time=None):
    rng = random.Random(seed)
    start = (
        datetime.fromisoformat(start_time)
        if start_time
        else datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    for i in range(count):
        malicious = rng.random() < 0.24
        dos = malicious and rng.random() < 0.55
        values = {name: float(rng.randint(0, 8)) for name in NUMERIC}
        values.update(
            dur=round(rng.uniform(0.01, 9), 5),
            spkts=float(rng.randint(150, 500) if dos else rng.randint(2, 70)),
            dpkts=float(rng.randint(0, 50)),
            sbytes=float(rng.randint(60, 16000)),
            dbytes=float(rng.randint(0, 34000)),
            sttl=float(rng.choice([31, 62, 64, 254])),
            dttl=float(rng.choice([0, 60, 64, 252])),
            ct_dst_ltm=float(
                rng.randint(40, 100) if malicious and not dos else rng.randint(0, 12)
            ),
            ct_src_dport_ltm=float(
                rng.randint(25, 75) if malicious and not dos else rng.randint(0, 6)
            ),
            rate=rng.uniform(1600, 10000) if dos else rng.uniform(2, 500),
            synack=rng.uniform(0, 0.2),
            ackdat=rng.uniform(0, 0.1),
        )
        event = dict(
            values,
            event_id=fingerprint({"synthetic": seed, "row": i, "split": split}),
            event_time=(start + timedelta(seconds=i * 0.35)).isoformat(
                timespec="milliseconds"
            ),
            source="Synthetic demonstration",
            split=split,
            source_file="generated",
            src_ip=f"192.0.2.{rng.randint(1, 45)}",
            dst_ip=f"198.51.100.{rng.randint(1, 10)}",
            src_port=str(rng.randint(1024, 65535)),
            dst_port=str(rng.choice([53, 80, 443])),
            proto=rng.choice(["tcp", "tcp", "udp", "icmp"]),
            service=rng.choice(["http", "dns", "-", "ssh"]),
            state=rng.choice(["FIN", "CON", "INT"]),
            label=int(malicious),
            attack_cat="DoS" if dos else "Reconnaissance" if malicious else "Normal",
            is_synthetic=True,
            time_is_simulated=True,
        )
        yield validate(event)
