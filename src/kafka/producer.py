"""
EduGuard Apache Kafka Producer Module
Publishes validated student data events to the eduguard.student-data topic.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CLIENT_ID, KAFKA_TOPIC
from src.kafka.schemas import StudentEventPayload

logger = logging.getLogger("eduguard.kafka.producer")


class EduGuardKafkaProducer:
    """Wrapper around KafkaProducer with schema validation and error handling."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        client_id: str = KAFKA_CLIENT_ID,
        topic: str = KAFKA_TOPIC,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.topic = topic
        self._producer: Optional[KafkaProducer] = None

    def get_producer(self) -> KafkaProducer:
        """Lazily initializes and returns the KafkaProducer instance."""
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
                request_timeout_ms=10000,
                acks="all",
            )
        return self._producer

    def check_health(self) -> Dict[str, Any]:
        """Checks if Kafka broker is reachable."""
        try:
            prod = self.get_producer()
            # partitions_for returns None or a set of partition IDs
            partitions = prod.partitions_for(self.topic)
            return {
                "status": "HEALTHY",
                "reachable": True,
                "bootstrap_servers": self.bootstrap_servers,
                "topic": self.topic,
                "partitions": list(partitions) if partitions else [],
            }
        except Exception as e:
            logger.warning("Kafka health check failed: %s", e)
            return {
                "status": "UNREACHABLE",
                "reachable": False,
                "bootstrap_servers": self.bootstrap_servers,
                "error": str(e),
            }

    def publish_student_record(
        self,
        student_id: str,
        features: Dict[str, Any],
        event_type: str = "STUDENT_RECORD_INGESTED",
        source: str = "API",
        event_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validates event payload and publishes student record to Kafka.
        Returns published event metadata (topic, partition, offset).
        """
        payload_data = {
            "student_id": student_id,
            "event_type": event_type,
            "features": features,
            "source": source,
        }
        if event_id:
            payload_data["event_id"] = event_id

        event = StudentEventPayload(**payload_data)
        event_dict = event.model_dump()

        producer = self.get_producer()
        try:
            future = producer.send(
                self.topic,
                key=student_id,
                value=event_dict,
            )
            record_metadata = future.get(timeout=10)
            logger.info(
                "Published student event %s to %s [partition %d, offset %d]",
                event.event_id,
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
            )
            return {
                "event_id": event.event_id,
                "student_id": student_id,
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "status": "PUBLISHED",
            }
        except KafkaError as e:
            logger.error("Failed to publish student %s to Kafka: %s", student_id, e)
            raise

    def publish_batch(
        self,
        records: List[Dict[str, Any]],
        source: str = "BATCH_INGESTION",
    ) -> List[Dict[str, Any]]:
        """Publishes a batch of student records to Kafka."""
        results = []
        for rec in records:
            student_id = rec.get("student_id")
            features = rec.get("features", rec)
            res = self.publish_student_record(
                student_id=str(student_id),
                features=features,
                source=source,
            )
            results.append(res)
        self.flush()
        return results

    def flush(self) -> None:
        """Flushes buffered records."""
        if self._producer:
            self._producer.flush()

    def close(self) -> None:
        """Closes the Kafka producer."""
        if self._producer:
            self._producer.close()
            self._producer = None


_DEFAULT_PRODUCER = None


def get_kafka_producer() -> EduGuardKafkaProducer:
    """Returns singleton Kafka producer instance."""
    global _DEFAULT_PRODUCER
    if _DEFAULT_PRODUCER is None:
        _DEFAULT_PRODUCER = EduGuardKafkaProducer()
    return _DEFAULT_PRODUCER
