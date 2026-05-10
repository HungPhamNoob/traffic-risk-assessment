#!/usr/bin/env python3
"""
US Accident Replay Producer – RAW Parallel Version (GCS Native)

Mục tiêu:
    - Đọc CSV US Accidents từ GCS bucket (gs://) hoặc local file.
    - Gửi mỗi dòng thành 1 JSON raw vào Kafka.
    - Không làm feature engineering.
    - Hỗ trợ chạy 3 producers song song mà KHÔNG gửi trùng dữ liệu.

Cách chia dữ liệu:
    TOTAL_PRODUCERS=3

    Producer 0: row_index % 3 == 0
    Producer 1: row_index % 3 == 1
    Producer 2: row_index % 3 == 2

Input:
    gs://big-data-group-4-bronze/process/us_pipeline_from_2020.csv
    (hoặc local path)

Output:
    Kafka topic: traffic.us.raw (1 partition, 3 replicas)
"""

import csv
import json
import logging
import os
import sys
import time
from typing import Any, Dict

from dotenv import load_dotenv
from confluent_kafka import Producer

# ============================================================
# Load .env nếu có
# ============================================================
load_dotenv()

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=os.getenv("STREAMING_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("us-replay-producer-raw")


# ============================================================
# Helpers đọc ENV an toàn
# ============================================================
def get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid int env %s=%r. Using default=%s", name, value, default)
        return default


def get_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning(
            "Invalid float env %s=%r. Using default=%s", name, value, default
        )
        return default


def get_str_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()


# ============================================================
# Config
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = get_str_env(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = get_str_env(
    "KAFKA_TOPIC_US",
    get_str_env("KAFKA_TOPIC_RAW", "traffic.us.raw"),
)
DATA_FILE_PATH = get_str_env(
    "US_PIPELINE_REPLAY_PATH",
    "data/process/us_pipeline_from_2020.csv",
)
STREAM_MAX_RECORDS = get_int_env("STREAM_MAX_RECORDS", 0)
STREAM_THROTTLE_SECONDS = get_float_env("STREAM_THROTTLE_SECONDS", 0.0)
PRODUCER_CLIENT_ID = get_str_env("PRODUCER_CLIENT_ID", "us-replay-producer-raw")
PRODUCER_FLUSH_EVERY_N_RECORDS = get_int_env("PRODUCER_FLUSH_EVERY_N_RECORDS", 5000)

TOTAL_PRODUCERS = get_int_env(
    "TOTAL_PRODUCERS",
    get_int_env("PRODUCER_PARTITION", 1),
)
PRODUCER_INDEX = get_int_env(
    "PRODUCER_INDEX",
    get_int_env("PRODUCER_PARTITION_INDEX", 0),
)

PRODUCER_MAX_BUFFER_MESSAGES = get_int_env("PRODUCER_MAX_BUFFER_MESSAGES", 100000)
PRODUCER_LINGER_MS = get_int_env("PRODUCER_LINGER_MS", 50)
PRODUCER_BATCH_NUM_MESSAGES = get_int_env("PRODUCER_BATCH_NUM_MESSAGES", 10000)
PRODUCER_COMPRESSION_TYPE = get_str_env("PRODUCER_COMPRESSION_TYPE", "lz4")
PRODUCER_QUEUE_BACKOFF_SECONDS = get_float_env("PRODUCER_QUEUE_BACKOFF_SECONDS", 0.5)


# ============================================================
# File opener – hỗ trợ cả GCS và local
# ============================================================
def open_file(path: str, mode: str = "r"):
    """
    Mở file từ GCS (gs://) hoặc local.

    Dùng gcsfs cho GCS, open() cho local.
    """
    if path.startswith("gs://"):
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        logger.info("Reading from GCS: %s", path)
        # gcsfs.open() cần path không có "gs://"
        bucket_path = path.replace("gs://", "", 1)
        return fs.open(bucket_path, mode=mode, encoding="utf-8")
    else:
        logger.info("Reading local file: %s", path)
        return open(path, mode=mode, encoding="utf-8", newline="")


# ============================================================
# Validate config
# ============================================================
def validate_config() -> None:
    if TOTAL_PRODUCERS <= 0:
        raise ValueError("TOTAL_PRODUCERS must be >= 1")
    if PRODUCER_INDEX < 0 or PRODUCER_INDEX >= TOTAL_PRODUCERS:
        raise ValueError(
            f"PRODUCER_INDEX must be in range [0, {TOTAL_PRODUCERS - 1}], "
            f"got PRODUCER_INDEX={PRODUCER_INDEX}"
        )
    if PRODUCER_FLUSH_EVERY_N_RECORDS <= 0:
        raise ValueError("PRODUCER_FLUSH_EVERY_N_RECORDS must be > 0")
    if STREAM_MAX_RECORDS < 0:
        raise ValueError("STREAM_MAX_RECORDS must be >= 0")
    if STREAM_THROTTLE_SECONDS < 0:
        raise ValueError("STREAM_THROTTLE_SECONDS must be >= 0")
    if PRODUCER_QUEUE_BACKOFF_SECONDS < 0:
        raise ValueError("PRODUCER_QUEUE_BACKOFF_SECONDS must be >= 0")


# ============================================================
# Kafka producer config
# ============================================================
def build_producer_config() -> Dict[str, Any]:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": f"{PRODUCER_CLIENT_ID}-{PRODUCER_INDEX}",
        "queue.buffering.max.messages": PRODUCER_MAX_BUFFER_MESSAGES,
        "linger.ms": PRODUCER_LINGER_MS,
        "batch.num.messages": PRODUCER_BATCH_NUM_MESSAGES,
        "compression.type": PRODUCER_COMPRESSION_TYPE,
        "acks": "all",
        "retries": 10,
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
    }


# ============================================================
# Delivery callback
# ============================================================
delivery_success_count = 0
delivery_failed_count = 0


def delivery_report(error: Any, message: Any) -> None:
    global delivery_success_count, delivery_failed_count
    if error is not None:
        delivery_failed_count += 1
        logger.error("Kafka delivery failed: %s", error)
        return
    delivery_success_count += 1


# ============================================================
# Core logic
# ============================================================
def should_send_row(row_index: int) -> bool:
    return row_index % TOTAL_PRODUCERS == PRODUCER_INDEX


def produce_with_backpressure(producer, topic, key, value) -> None:
    while True:
        try:
            producer.produce(
                topic=topic, key=key, value=value, callback=delivery_report
            )
            producer.poll(0)
            return
        except BufferError:
            logger.warning("Buffer full, backoff %.3fs", PRODUCER_QUEUE_BACKOFF_SECONDS)
            producer.poll(1.0)
            if PRODUCER_QUEUE_BACKOFF_SECONDS > 0:
                time.sleep(PRODUCER_QUEUE_BACKOFF_SECONDS)


def print_startup_log() -> None:
    logger.info("=" * 80)
    logger.info("US Accident Replay Producer – RAW Parallel (GCS Native)")
    logger.info("Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", KAFKA_TOPIC)
    logger.info("CSV:   %s", DATA_FILE_PATH)
    logger.info("Producer: %s/%s", PRODUCER_INDEX + 1, TOTAL_PRODUCERS)
    logger.info("=" * 80)


def main() -> None:
    validate_config()
    print_startup_log()

    producer = Producer(build_producer_config())
    scanned_rows = 0
    skipped_rows = 0
    sent_rows = 0
    start_time = time.time()

    try:
        with open_file(DATA_FILE_PATH) as f:
            reader = csv.DictReader(f)
            logger.info("CSV columns: %s", reader.fieldnames)

            for row_index, row in enumerate(reader):
                scanned_rows += 1

                if not should_send_row(row_index):
                    skipped_rows += 1
                    continue

                key = row.get("ID") or f"row-{row_index}"
                value = json.dumps(row, ensure_ascii=False)

                produce_with_backpressure(producer, KAFKA_TOPIC, key, value)
                sent_rows += 1

                if sent_rows % 1000 == 0:
                    elapsed = max(time.time() - start_time, 1e-6)
                    logger.info(
                        "producer=%s sent=%s scanned=%s skipped=%s rate=%.0f rows/s",
                        PRODUCER_INDEX,
                        f"{sent_rows:,}",
                        f"{scanned_rows:,}",
                        f"{skipped_rows:,}",
                        sent_rows / elapsed,
                    )

                if sent_rows % PRODUCER_FLUSH_EVERY_N_RECORDS == 0:
                    producer.flush()

                if STREAM_THROTTLE_SECONDS > 0:
                    time.sleep(STREAM_THROTTLE_SECONDS)

                if STREAM_MAX_RECORDS > 0 and sent_rows >= STREAM_MAX_RECORDS:
                    break

    except KeyboardInterrupt:
        logger.warning("Interrupted")
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        raise
    finally:
        producer.flush()
        elapsed = max(time.time() - start_time, 1e-6)
        logger.info("=" * 80)
        logger.info(
            "Done. Sent: %s | Skipped: %s | Rate: %.0f rows/s",
            f"{sent_rows:,}",
            f"{skipped_rows:,}",
            sent_rows / elapsed,
        )
        logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Fatal producer error: %s", exc)
        sys.exit(1)
