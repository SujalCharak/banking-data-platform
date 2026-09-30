RAW_COLUMNS = {
    "customers": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "birth_date",
        "country_code",
        "segment",
        "risk_rating",
        "kyc_status",
        "created_at",
        "updated_at",
    ],
    "accounts": [
        "account_id",
        "customer_id",
        "iban",
        "account_type",
        "currency",
        "status",
        "opened_date",
        "closed_date",
        "opening_balance",
        "updated_at",
    ],
    "merchants": ["merchant_id", "merchant_name", "mcc", "country_code"],
    "fx_rates": ["rate_date", "currency", "rate_to_eur", "source"],
    "transactions": [
        "txn_id",
        "account_id",
        "txn_ts",
        "amount",
        "currency",
        "direction",
        "txn_type",
        "status",
        "channel",
        "merchant_id",
        "counterparty_iban",
        "original_txn_id",
        "description",
        "device_id",
    ],
}

METADATA_COLUMNS = ["_source_file", "_source_row", "_loaded_at"]

# Columns landed from Kafka. Business fields stay text, like the file path; the rest locate each message exactly.
STREAM_TABLE = "transactions_stream"
STREAM_BUSINESS_COLUMNS = RAW_COLUMNS["transactions"]
STREAM_KAFKA_COLUMNS = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp_ms"]
STREAM_ERROR_COLUMNS = ["_raw_value", "_parse_error"]
STREAM_COLUMNS = STREAM_BUSINESS_COLUMNS + STREAM_KAFKA_COLUMNS + STREAM_ERROR_COLUMNS
