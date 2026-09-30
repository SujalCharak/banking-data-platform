from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from bankdp.schema import STREAM_BUSINESS_COLUMNS

TOPIC = "bank.transactions"
SCHEMA_HEADER = ("schema", b"bank.transaction.v1")


def encode(row: dict[str, Any]) -> tuple[bytes | None, bytes]:
    """Kafka key and value for one transaction. Keyed by account so each account's events stay in order."""
    payload = {c: (row.get(c) or None) for c in STREAM_BUSINESS_COLUMNS}
    key = payload["account_id"].encode() if payload["account_id"] else None
    return key, json.dumps(payload, separators=(",", ":")).encode()


@dataclass
class LandedRecord:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    fields: dict[str, str | None]
    raw_value: str | None = None
    parse_error: str | None = None

    def as_row(self) -> dict[str, Any]:
        return {
            **self.fields,
            "_kafka_topic": self.topic,
            "_kafka_partition": self.partition,
            "_kafka_offset": self.offset,
            "_kafka_timestamp_ms": self.timestamp_ms,
            "_raw_value": self.raw_value,
            "_parse_error": self.parse_error,
        }


def decode(topic: str, partition: int, offset: int, timestamp_ms: int | None, value: bytes | None) -> LandedRecord:
    """Never raises: a message that isn't a valid event is landed with its raw value and the reason."""
    empty = dict.fromkeys(STREAM_BUSINESS_COLUMNS)
    text = None
    try:
        if value is None:
            raise ValueError("empty message")
        text = value.decode("utf-8")
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("payload is not a JSON object")
        fields = {c: None if payload.get(c) is None else str(payload[c]) for c in STREAM_BUSINESS_COLUMNS}
        return LandedRecord(topic, partition, offset, timestamp_ms, fields)
    except (UnicodeDecodeError, ValueError) as exc:
        raw = text if text is not None else (value or b"").decode("utf-8", errors="replace")
        return LandedRecord(topic, partition, offset, timestamp_ms, empty, raw_value=raw[:4000], parse_error=str(exc))
