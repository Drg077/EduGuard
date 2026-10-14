"""
EduGuard Data Ingestion Script
Reads official UCI dataset records and publishes them through Apache Kafka,
populating HDFS and operational database, then triggers the prediction pipeline.
"""

import argparse
import logging

import pandas as pd

from src.config import DATASET_PATH
from src.kafka.consumer import get_kafka_consumer
from src.kafka.producer import get_kafka_producer
from src.kafka.schemas import StudentEventPayload
from src.scheduler.prediction_job import run_prediction_pipeline
from src.storage.database import count_students, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("eduguard.seed")


def _native(value):
    """Convert numpy/pandas scalars to JSON-safe Python types."""
    if hasattr(value, "item"):
        return value.item()
    return value


def ingest_uci_data(limit: int = 150):
    init_db()
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset not found at {DATASET_PATH}")

    logger.info("Loading UCI dataset from %s...", DATASET_PATH)
    df = pd.read_csv(DATASET_PATH)

    records_to_ingest = df.head(limit)
    logger.info("Ingesting %d real student records via Apache Kafka...", len(records_to_ingest))

    producer = get_kafka_producer()
    consumer = get_kafka_consumer()
    published_count = 0
    processed_count = 0

    for idx, row in records_to_ingest.iterrows():
        student_id = f"STU-{1000 + idx}"
        row_dict = row.to_dict()
        row_dict.pop("Target", None)
        features = {k: _native(v) for k, v in row_dict.items()}

        try:
            # Build the same validated payload that Kafka receives
            event = StudentEventPayload(
                student_id=student_id,
                features=features,
                source="UCI_OFFICIAL_DATASET",
            )
            event_dict = event.model_dump()

            producer.publish_student_record(
                student_id=student_id,
                features=features,
                source="UCI_OFFICIAL_DATASET",
                event_id=event.event_id,
            )
            published_count += 1

            # Persist through the consumer pipeline (SQLite + HDFS) immediately
            # so the dashboard is populated even if Kafka drain races the background listener.
            res = consumer.process_record(event_dict)
            if res.get("status") in ("PROCESSED", "SKIPPED_DUPLICATE"):
                processed_count += 1
        except Exception as e:
            logger.error("Failed to ingest %s: %s", student_id, e)

    producer.flush()
    logger.info(
        "Published %d records to Kafka; processed %d into HDFS and SQLite.",
        published_count,
        processed_count,
    )

    total_in_db = count_students()
    logger.info("Total students now in operational database: %d", total_in_db)

    logger.info("Triggering initial prediction pipeline run...")
    run_res = run_prediction_pipeline(triggered_by="INITIAL_SEED")
    logger.info(
        "Initial prediction run complete: %d successful predictions.",
        run_res["successful_predictions"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest UCI student records into EduGuard")
    parser.add_argument("--limit", type=int, default=120, help="Number of records to ingest")
    args = parser.parse_args()
    ingest_uci_data(limit=args.limit)
