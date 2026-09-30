from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field
from typing import Protocol

import pandas as pd

from bankdp.schema import STREAM_COLUMNS
from bankdp.streaming.events import decode

log = logging.getLogger(__name__)

OFFSET_BEGINNING = -2  # librdkafka's logical offset for "start of partition"


class Sink(Protocol):
    def last_offsets(self, topic: str) -> dict[int, int]: ...

    def write(self, frame: pd.DataFrame) -> None: ...

    def close(self) -> None: ...


@dataclass
class IngestStats:
    batches: int = 0
    rows: int = 0
    bad_messages: int = 0
    per_partition: dict[int, int] = field(default_factory=dict)


class StreamIngestor:
    """
    Lands Kafka messages in the warehouse in micro batches, exactly once.

    The sink table is the source of truth for progress: on every partition assignment the consumer
    seeks to the highest offset already landed plus one. A crash between the warehouse write and the
    Kafka offset commit therefore never duplicates or skips a message. Kafka commits are still made
    so consumer lag shows up in the usual monitoring.
    """

    def __init__(
        self,
        consumer,
        sink: Sink,
        topic: str,
        batch_size: int = 5000,
        max_wait_s: float = 5.0,
        commit_offsets: bool = True,
    ):
        self.consumer = consumer
        self.sink = sink
        self.topic = topic
        self.batch_size = batch_size
        self.max_wait_s = max_wait_s
        self.commit_offsets = commit_offsets
        self.buffer: list = []
        self.batch_started: float | None = None
        self.stats = IngestStats()
        self._stop = False

    def on_assign(self, consumer, partitions) -> None:
        landed = self.sink.last_offsets(self.topic)
        for p in partitions:
            p.offset = landed[p.partition] + 1 if p.partition in landed else OFFSET_BEGINNING
        log.info("assigned %s", {p.partition: p.offset for p in partitions})
        consumer.assign(partitions)
        if self.commit_offsets:
            # bring the group's committed offsets in line with the table, so lag reports what is really landed
            self._commit({p.partition: p.offset for p in partitions if p.offset >= 0})

    def on_revoke(self, consumer, partitions) -> None:
        # land what we hold before another consumer takes these partitions and seeks from the table
        self.flush()
        consumer.unassign()

    def on_lost(self, consumer, partitions) -> None:
        # the group already moved on without us; writing now could duplicate what the new owner lands
        self.buffer.clear()
        self.batch_started = None
        consumer.unassign()

    def stop(self, *_args) -> None:
        self._stop = True

    def flush(self) -> None:
        if not self.buffer:
            return
        records = [decode(m.topic(), m.partition(), m.offset(), _timestamp_ms(m), m.value()) for m in self.buffer]
        frame = pd.DataFrame([r.as_row() for r in records], columns=STREAM_COLUMNS)
        self.sink.write(frame)

        latest: dict[int, int] = {}
        for r in records:
            latest[r.partition] = max(latest.get(r.partition, -1), r.offset)
            self.stats.per_partition[r.partition] = self.stats.per_partition.get(r.partition, 0) + 1
        bad = sum(r.parse_error is not None for r in records)
        self.stats.batches += 1
        self.stats.rows += len(records)
        self.stats.bad_messages += bad
        log.info("landed batch %d: %d rows (%d unparseable), offsets %s", self.stats.batches, len(records), bad, latest)

        if self.commit_offsets:
            self._commit({p: o + 1 for p, o in latest.items()})
        self.buffer.clear()
        self.batch_started = None

    def _commit(self, next_offsets: dict[int, int]) -> None:
        if not next_offsets:
            return
        from confluent_kafka import TopicPartition

        self.consumer.commit(
            offsets=[TopicPartition(self.topic, p, o) for p, o in next_offsets.items()], asynchronous=False
        )

    def run(self, idle_exit_s: float | None = None, max_batches: int | None = None) -> IngestStats:
        self.consumer.subscribe([self.topic], on_assign=self.on_assign, on_revoke=self.on_revoke, on_lost=self.on_lost)
        last_message = time.monotonic()
        try:
            while not self._stop:
                wanted = max(1, min(1000, self.batch_size - len(self.buffer)))
                messages = self.consumer.consume(num_messages=wanted, timeout=0.5)
                now = time.monotonic()
                for m in messages:
                    if m.error():
                        if m.error().fatal():
                            raise RuntimeError(f"fatal Kafka error: {m.error()}")
                        log.warning("Kafka error: %s", m.error())
                        continue
                    if self.batch_started is None:
                        self.batch_started = now
                    self.buffer.append(m)
                if messages:
                    last_message = now
                due = self.batch_started is not None and now - self.batch_started >= self.max_wait_s
                if len(self.buffer) >= self.batch_size or (self.buffer and due):
                    self.flush()
                    if max_batches is not None and self.stats.batches >= max_batches:
                        # stop as if the process died: whatever is buffered is never landed
                        self.buffer.clear()
                        break
                if idle_exit_s is not None and now - last_message >= idle_exit_s:
                    log.info("no messages for %.0fs, stopping", idle_exit_s)
                    break
            self.flush()
        finally:
            self.consumer.close()
            self.sink.close()
        log.info(
            "landed %d rows in %d batches (%d unparseable)",
            self.stats.rows,
            self.stats.batches,
            self.stats.bad_messages,
        )
        return self.stats


def _timestamp_ms(message) -> int | None:
    kind, value = message.timestamp()
    return value if kind != 0 else None  # 0 = TIMESTAMP_NOT_AVAILABLE


def kafka_consumer(bootstrap: str, group_id: str):
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45_000,
        }
    )


def run_consumer(
    bootstrap: str,
    group_id: str,
    topic: str,
    sink: Sink,
    batch_size: int,
    max_wait_s: float,
    idle_exit_s: float | None,
    max_batches: int | None = None,
    commit_offsets: bool = True,
) -> IngestStats:
    ingestor = StreamIngestor(kafka_consumer(bootstrap, group_id), sink, topic, batch_size, max_wait_s, commit_offsets)
    signal.signal(signal.SIGINT, ingestor.stop)
    signal.signal(signal.SIGTERM, ingestor.stop)
    return ingestor.run(idle_exit_s=idle_exit_s, max_batches=max_batches)
