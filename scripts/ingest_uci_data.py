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
from src.scheduler.prediction_job import run_prediction_pipeline
from src.storage.database import count_students, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("eduguard.seed")


def ingest_uci_data(limit: int = 150):
    init_db()
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset not found at {DATASET_PATH}")

    logger.info("Loading UCI dataset from %s...", DATASET_PATH)
    df = pd.read_csv(DATASET_PATH)

    records_to_ingest = df.head(limit)
    logger.info("Ingesting %d real student records via Apache Kafka...", len(records_to_ingest))

    producer = get_kafka_producer()
    published_count = 0

    for idx, row in records_to_ingest.iterrows():
        student_id = f"STU-{1000 + idx}"
        row_dict = row.to_dict()
        if "Target" in row_dict:
            del row_dict["Target"]

        producer.publish_student_record(
            student_id=student_id,
            features=row_dict,
            source="UCI_OFFICIAL_DATASET",
        )
        published_count += 1

    producer.flush()
    logger.info("Published %d records to Kafka topic. Now consuming...", published_count)

    consumer = get_kafka_consumer()
    results = consumer.drain_records(max_records=limit + 50, timeout_ms=5000)
    logger.info("Consumer processed %d events into HDFS and SQLite.", len(results))

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
