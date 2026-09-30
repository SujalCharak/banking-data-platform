from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from bankdp.schema import STREAM_TABLE


@dataclass
class PartitionCheck:
    partition: int
    low: int
    high: int
    landed: int
    distinct: int
    min_offset: int | None
    max_offset: int | None

    @property
    def expected(self) -> int:
        return self.high - self.low

    @property
    def passed(self) -> bool:
        if self.expected == 0:
            return self.landed == 0
        return (
            self.landed == self.distinct == self.expected
            and self.min_offset == self.low
            and self.max_offset == self.high - 1
        )


@dataclass
class TopicCheck:
    topic: str
    partitions: list[PartitionCheck] = field(default_factory=list)
    unparseable: int = 0

    @property
    def passed(self) -> bool:
        return bool(self.partitions) and all(p.passed for p in self.partitions)

    def to_markdown(self) -> str:
        lines = [
            f"Topic `{self.topic}`: {'PASS' if self.passed else 'FAIL'}, "
            f"{self.unparseable} unparseable messages landed",
            "",
            "| Partition | Messages in topic | Rows landed | Distinct offsets | Result |",
            "|---:|---:|---:|---:|---|",
        ]
        for p in self.partitions:
            lines.append(
                f"| {p.partition} | {p.expected:,} | {p.landed:,} | {p.distinct:,} | {'PASS' if p.passed else 'FAIL'} |"
            )
        return "\n".join(lines)


def _topic_watermarks(bootstrap: str, topic: str) -> dict[int, tuple[int, int]]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "bank-stream-check", "enable.auto.commit": False})
    try:
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        if meta.error is not None:
            raise SystemExit(f"topic {topic}: {meta.error}")
        return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in meta.partitions}
    finally:
        consumer.close()


def _landed(sink: str, topic: str, db_path: Path) -> tuple[dict[int, tuple[int, int, int, int]], int]:
    sql = (
        "select _kafka_partition, count(*), count(distinct _kafka_offset), min(_kafka_offset), max(_kafka_offset), "
        "count(_parse_error) from {table} where _kafka_topic = {param} group by 1"
    )
    if sink == "duckdb":
        import duckdb

        con = duckdb.connect(str(db_path))
        rows = con.execute(sql.format(table=f"raw.{STREAM_TABLE}", param="?"), [topic]).fetchall()
        con.close()
    else:
        from bankdp.snowflake_io import connect

        con = connect(role="BANK_LOADER", warehouse="BANK_LOAD_WH", query_tag="bank_platform_stream")
        cur = con.cursor()
        cur.execute(sql.format(table="RAW.BANK.TRANSACTIONS_STREAM", param="%s"), (topic,))
        rows = cur.fetchall()
        con.close()
    landed = {int(r[0]): (int(r[1]), int(r[2]), int(r[3]), int(r[4])) for r in rows}
    return landed, sum(int(r[5]) for r in rows)


def check_sink_matches_topic(bootstrap: str, topic: str, sink: str, db_path: Path) -> TopicCheck:
    """Every offset in the topic must be landed exactly once. Assumes the topic has not expired any messages."""
    watermarks = _topic_watermarks(bootstrap, topic)
    landed, unparseable = _landed(sink, topic, db_path)
    report = TopicCheck(topic, unparseable=unparseable)
    for partition in sorted(set(watermarks) | set(landed)):
        low, high = watermarks.get(partition, (0, 0))
        count, distinct, lo, hi = landed.get(partition, (0, 0, None, None))
        report.partitions.append(PartitionCheck(partition, low, high, count, distinct, lo, hi))
    return report
