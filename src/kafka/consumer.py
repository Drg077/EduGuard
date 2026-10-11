"""
EduGuard Apache Kafka Consumer Module
Consumes student records from Kafka, applies validation, prevents duplicates,
and persists student snapshots into SQLite and Hadoop HDFS.
"""

import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.config import (
    HDFS_INCOMING_DIR,
    HDFS_PROCESSED_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
)
from src.kafka.schemas import StudentEventPayload
from src.storage.database import (
    is_event_processed,
    record_processed_event,
    upsert_student,
)
from src.storage.hdfs_client import get_hdfs_client

logger = logging.getLogger("eduguard.kafka.consumer")


class EduGuardKafkaConsumer:
    """
    Kafka Consumer that consumes student records from the eduguard.student-data topic.
    Persists validated records into operational database and Hadoop HDFS.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = KAFKA_CONSUMER_GROUP,
        topic: str = KAFKA_TOPIC,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def get_consumer(self, timeout_ms: int = 5000) -> KafkaConsumer:
        """Initializes and returns a configured KafkaConsumer."""
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        return self._consumer

    def process_record(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates, deduplicates, and processes a single student data event.
        Writes to SQLite and Hadoop HDFS.
        """
        # Step 1: Validate Schema
        try:
            event = StudentEventPayload(**raw_event)
        except Exception as e:
            logger.error("Malformed Kafka message rejected: %s", e)
            return {"status": "REJECTED", "error": f"Schema validation error: {str(e)}"}

        event_id = event.event_id
        student_id = event.student_id

        # Step 2: Idempotency Check
        if is_event_processed(event_id):
            logger.info("Event %s for student %s already processed. Skipping duplicate.", event_id, student_id)
            return {"status": "SKIPPED_DUPLICATE", "event_id": event_id, "student_id": student_id}

        try:
            # Step 3: Persist to operational SQLite database
            upsert_student(
                student_id=student_id,
                features=event.features,
                source=event.source,
            )

            # Step 4: Persist to Hadoop HDFS
            hdfs = get_hdfs_client()
            if hdfs.check_health()["reachable"]:
                # 4a. Write incoming event
                incoming_path = f"{HDFS_INCOMING_DIR}/{event_id}.json"
                hdfs.write_json(incoming_path, event.model_dump(), overwrite=True, atomic=True)

                # 4b. Write updated student processed profile snapshot
                student_snapshot = {
                    "student_id": student_id,
                    "features": event.features,
                    "last_updated": event.event_timestamp,
                    "event_id": event_id,
                    "source": event.source,
                }
                processed_path = f"{HDFS_PROCESSED_DIR}/{student_id}.json"
                hdfs.write_json(processed_path, student_snapshot, overwrite=True, atomic=True)

            # Step 5: Mark event as processed in SQLite for idempotency
            record_processed_event(event_id, student_id, event.event_type, "PROCESSED")

            logger.info("Successfully processed Kafka event %s for student %s", event_id, student_id)
            return {"status": "PROCESSED", "event_id": event_id, "student_id": student_id}

        except Exception as e:
            logger.error("Error processing event %s: %s", event_id, e)
            record_processed_event(event_id, student_id, event.event_type, "FAILED")
            return {"status": "FAILED", "event_id": event_id, "error": str(e)}

    def drain_records(self, max_records: int = 100, timeout_ms: int = 3000) -> List[Dict[str, Any]]:
        """
        Pulls and processes available messages up to max_records or timeout.
        Returns list of processing results.
        """
        consumer = self.get_consumer(timeout_ms=timeout_ms)
        results = []
        count = 0
        try:
            for message in consumer:
                res = self.process_record(message.value)
                results.append(res)
                count += 1
                if count >= max_records:
                    break
        except Exception as e:
            logger.debug("Consumer finished polling: %s", e)
        return results

    def start_background_listener(self) -> None:
        """Starts background consumption thread."""
        if self._running:
            return

        self._running = True

        def _loop():
            logger.info("EduGuard Kafka background consumer started.")
            while self._running:
                try:
                    self.drain_records(max_records=50, timeout_ms=2000)
                    time.sleep(1)
                except Exception as e:
                    logger.warning("Kafka consumer loop error: %s", e)
                    time.sleep(2)
            logger.info("EduGuard Kafka background consumer stopped.")

        self._thread = threading.Thread(target=_loop, daemon=True, name="KafkaConsumerThread")
        self._thread.start()

    def stop(self) -> None:
        """Stops background consumer."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None


_DEFAULT_CONSUMER = None


def get_kafka_consumer() -> EduGuardKafkaConsumer:
    """Returns singleton Kafka consumer instance."""
    global _DEFAULT_CONSUMER
    if _DEFAULT_CONSUMER is None:
        _DEFAULT_CONSUMER = EduGuardKafkaConsumer()
    return _DEFAULT_CONSUMER
