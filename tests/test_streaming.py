import json

import duckdb
import pytest

pytest.importorskip("confluent_kafka")

from confluent_kafka import TopicPartition  # noqa: E402

from bankdp.streaming.consumer import StreamIngestor  # noqa: E402
from bankdp.streaming.events import decode, encode  # noqa: E402
from bankdp.streaming.sinks import DuckDBSink  # noqa: E402

TOPIC = "bank.transactions"


class FakeMessage:
    def __init__(self, partition, offset, value):
        self._p, self._o, self._v = partition, offset, value

    def topic(self):
        return TOPIC

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def timestamp(self):
        return (1, 1_760_000_000_000 + self._o)

    def error(self):
        return None


class FakeConsumer:
    """Enough of confluent_kafka.Consumer to drive StreamIngestor against an in-memory topic."""

    def __init__(self, log: dict[int, list[bytes]]):
        self.log = log
        self.position: dict[int, int] = {}
        self.committed: dict[int, int] = {}
        self.callbacks = {}

    def subscribe(self, topics, on_assign, on_revoke, on_lost):
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}
        on_assign(self, [TopicPartition(TOPIC, p) for p in self.log])

    def assign(self, partitions):
        self.position = {p.partition: max(p.offset, 0) for p in partitions}

    def unassign(self):
        self.position = {}

    def consume(self, num_messages, timeout):
        out = []
        for p in sorted(self.position):
            while len(out) < num_messages and self.position[p] < len(self.log[p]):
                o = self.position[p]
                out.append(FakeMessage(p, o, self.log[p][o]))
                self.position[p] += 1
        return out

    def commit(self, offsets, asynchronous):
        for tp in offsets:
            self.committed[tp.partition] = tp.offset

    def close(self):
        if self.position:
            self.callbacks["revoke"](self, [TopicPartition(TOPIC, p) for p in self.position])


def make_log(per_partition=350, partitions=3, corrupt=()):
    log = {}
    for p in range(partitions):
        values = []
        for i in range(per_partition):
            _, value = encode({"txn_id": f"T{p}-{i}", "account_id": f"A{p}", "amount": "10.00", "device_id": ""})
            values.append(value[:10] if (p, i) in corrupt else value)
        log[p] = values
    return log


def landed(db):
    con = duckdb.connect(str(db))
    rows = con.execute(
        "select _kafka_partition, count(*), count(distinct _kafka_offset), min(_kafka_offset), max(_kafka_offset) "
        "from raw.transactions_stream group by 1 order by 1"
    ).fetchall()
    con.close()
    return rows


def test_encode_keys_by_account_and_nulls_empty_fields():
    key, value = encode({"txn_id": "T1", "account_id": "A7", "amount": "12.50", "device_id": ""})
    payload = json.loads(value)
    assert key == b"A7"
    assert payload["amount"] == "12.50" and payload["device_id"] is None


def test_decode_keeps_bad_messages_instead_of_failing():
    record = decode(TOPIC, 0, 5, None, b'{"txn_id": "T1", "amou')
    assert record.parse_error and record.raw_value.startswith('{"txn_id"')
    assert record.fields["txn_id"] is None
    assert decode(TOPIC, 0, 6, None, None).parse_error == "empty message"


def test_crash_before_kafka_commit_neither_duplicates_nor_skips(tmp_path):
    db = tmp_path / "bank.duckdb"
    log = make_log()

    first = FakeConsumer(log)
    StreamIngestor(first, DuckDBSink(db), TOPIC, batch_size=200, max_wait_s=60, commit_offsets=False).run(
        idle_exit_s=0.05, max_batches=2
    )
    assert first.committed == {}
    assert sum(r[1] for r in landed(db)) == 400

    second = FakeConsumer(log)  # Kafka would restart this group from the beginning; the table says otherwise
    StreamIngestor(second, DuckDBSink(db), TOPIC, batch_size=200, max_wait_s=60).run(idle_exit_s=0.05)
    assert landed(db) == [(p, 350, 350, 0, 349) for p in range(3)]
    assert second.committed == {0: 350, 1: 350, 2: 350}


def test_revoked_partitions_are_landed_before_release(tmp_path):
    db = tmp_path / "bank.duckdb"
    log = make_log(per_partition=50, partitions=2)
    consumer = FakeConsumer(log)
    ingestor = StreamIngestor(consumer, DuckDBSink(db), TOPIC, batch_size=1000, max_wait_s=60)
    consumer.subscribe([TOPIC], ingestor.on_assign, ingestor.on_revoke, ingestor.on_lost)
    ingestor.buffer.extend(consumer.consume(30, 0))
    ingestor.on_revoke(consumer, [])
    assert sum(r[1] for r in landed(db)) == 30

    StreamIngestor(FakeConsumer(log), DuckDBSink(db), TOPIC, batch_size=1000, max_wait_s=60).run(idle_exit_s=0.05)
    assert landed(db) == [(0, 50, 50, 0, 49), (1, 50, 50, 0, 49)]


def test_lost_partitions_drop_the_buffer(tmp_path):
    db = tmp_path / "bank.duckdb"
    consumer = FakeConsumer(make_log(per_partition=20, partitions=1))
    ingestor = StreamIngestor(consumer, DuckDBSink(db), TOPIC, batch_size=1000, max_wait_s=60)
    consumer.subscribe([TOPIC], ingestor.on_assign, ingestor.on_revoke, ingestor.on_lost)
    ingestor.buffer.extend(consumer.consume(20, 0))
    ingestor.on_lost(consumer, [])
    ingestor.flush()
    assert landed(db) == []


def test_unparseable_messages_are_landed_with_their_error(tmp_path):
    db = tmp_path / "bank.duckdb"
    log = make_log(per_partition=10, partitions=1, corrupt={(0, 3)})
    StreamIngestor(FakeConsumer(log), DuckDBSink(db), TOPIC, batch_size=100, max_wait_s=60).run(idle_exit_s=0.05)
    con = duckdb.connect(str(db))
    bad = con.execute(
        "select _kafka_offset, txn_id from raw.transactions_stream where _parse_error is not null"
    ).fetchall()
    total = con.execute("select count(*) from raw.transactions_stream").fetchone()[0]
    assert bad == [(3, None)] and total == 10
