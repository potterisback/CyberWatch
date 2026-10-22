"""Replay completed flow records into Kafka, with delivery acknowledgements."""

import json
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
        for future in pending:
            future.get(timeout=60)
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=30)
    return dict(
        acknowledged=count,
        topic=topic,
        elapsed_seconds=round(time.perf_counter() - started, 3),
        source="historical dataset replay; no packets are transmitted to target systems",
    )
