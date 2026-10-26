"""Replay completed flow records into Kafka, with delivery acknowledgements."""

import json
import sys
import time

from .schema import read_events, utcnow


def produce(
    path, bootstrap="localhost:9092", topic="cyberwatch.flows", rate=500, limit=None
):
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    if rate <= 0:
        raise ValueError("rate must be positive")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="cyberwatch-admin")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=3, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        retries=5,
        enable_idempotence=True,
        key_serializer=lambda s: s.encode(),
        value_serializer=lambda o: json.dumps(o, allow_nan=False).encode(),
    )
    started = time.perf_counter()
    pending, count = [], 0
    tty = sys.stderr.isatty()
    last_report = started
    total = limit

    def report(final=False):
        elapsed = time.perf_counter() - started
        actual = count / elapsed if elapsed > 0 else 0.0
        of = f"/{total}" if total else ""
        line = (
            f"[produce] {topic} sent={count}{of} "
            f"rate={actual:,.0f}/s (target {rate:g}/s) t={elapsed:5.1f}s"
        )
        if tty:
            print("\r" + line + "\033[K", end="\n" if final else "", file=sys.stderr, flush=True)
        else:
            print(line, file=sys.stderr, flush=True)

    try:
        for event in read_events(path, "replay", limit, start_time=utcnow()):
            pending.append(producer.send(topic, key=event["event_id"], value=event))
            count += 1
            if len(pending) >= 500:
                for future in pending:
                    future.get(timeout=60)
                pending.clear()
            delay = count / rate - (time.perf_counter() - started)
            if delay > 0:
                time.sleep(delay)
            now = time.perf_counter()
            if now - last_report >= (0.2 if tty else 1.0):
                report()
                last_report = now
        for future in pending:
            future.get(timeout=60)
        producer.flush(timeout=60)
        report(final=True)
    finally:
        producer.close(timeout=30)
    return dict(
        acknowledged=count,
        topic=topic,
        elapsed_seconds=round(time.perf_counter() - started, 3),
        source="historical dataset replay; no packets are transmitted to target systems",
    )
