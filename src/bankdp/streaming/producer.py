from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd

from bankdp.streaming.events import SCHEMA_HEADER, encode

log = logging.getLogger(__name__)


def _file_date(path: Path) -> date:
    stamp = path.name.split("_")[-1].split(".")[0]
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))


def replay_rows(landing: Path, start: date, end: date) -> Iterator[dict]:
    """Transaction rows from the daily extracts, in delivery order, as the core system would have emitted them."""
    files = [
        f for f in sorted((landing / "transactions").glob("transactions_*.csv.gz")) if start <= _file_date(f) <= end
    ]
    if not files:
        raise SystemExit(f"no transaction files between {start} and {end} in {landing}")
    for f in files:
        frame = pd.read_csv(f, dtype=str, keep_default_na=False)
        yield from frame.to_dict("records")


def wait_for_broker(admin, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception as exc:  # broker still starting
            if time.monotonic() > deadline:
                raise SystemExit(f"Kafka broker not reachable: {exc}") from exc
            log.info("waiting for Kafka broker")
            time.sleep(2)


def ensure_topic(bootstrap: str, topic: str, partitions: int, fresh: bool = False, wait_s: float = 60) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    wait_for_broker(admin, wait_s)
    if fresh and topic in admin.list_topics(timeout=10).topics:
        admin.delete_topics([topic])[topic].result()
        while topic in admin.list_topics(timeout=10).topics:
            time.sleep(0.5)
        log.info("deleted topic %s", topic)
    if topic in admin.list_topics(timeout=10).topics:
        return
    admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic].result()
    log.info("created topic %s with %d partitions", topic, partitions)


def produce(
    bootstrap: str,
    topic: str,
    rows: Iterator[dict],
    rate: float = 0,
    corrupt_every: int = 0,
    limit: int | None = None,
    idempotent: bool = True,
) -> int:
    """Publish rows to Kafka. rate is messages per second (0 for as fast as possible)."""
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": idempotent,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )
    failures: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    sent = 0
    started = time.monotonic()
    for row in rows:
        if limit is not None and sent >= limit:
            break
        key, value = encode(row)
        if corrupt_every and sent and sent % corrupt_every == 0:
            value = value[: len(value) // 2]  # truncated payload, as a faulty upstream would send
        while True:
            try:
                producer.produce(topic, key=key, value=value, headers=[SCHEMA_HEADER], on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        sent += 1
        producer.poll(0)
        if rate:
            ahead = sent / rate - (time.monotonic() - started)
            if ahead > 0:
                time.sleep(ahead)
        if sent % 10_000 == 0:
            log.info("produced %d messages (%.0f per second)", sent, sent / (time.monotonic() - started))

    remaining = producer.flush(60)
    if remaining or failures:
        raise SystemExit(f"{len(failures)} deliveries failed, {remaining} unflushed; first error: {failures[:1]}")
    log.info("produced %d messages to %s in %.1fs", sent, topic, time.monotonic() - started)
    return sent
