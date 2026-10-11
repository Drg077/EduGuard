"""
Integration tests for Apache Kafka Producer and Consumer in EduGuard.
Verifies message publication, consumption, idempotency, and HDFS persistence.
"""

import time
import uuid
import pytest
from src.config import HDFS_INCOMING_DIR, HDFS_PROCESSED_DIR
from src.kafka.consumer import EduGuardKafkaConsumer
from src.kafka.producer import EduGuardKafkaProducer
from src.storage.database import get_student, is_event_processed
from src.storage.hdfs_client import get_hdfs_client


@pytest.fixture(scope="module")
def kafka_infra():
    producer = EduGuardKafkaProducer()
    health = producer.check_health()
    if not health["reachable"]:
        pytest.skip(f"Kafka broker is not reachable: {health.get('error')}")
    test_group = f"test-kafka-suite-{uuid.uuid4().hex[:8]}"
    consumer = EduGuardKafkaConsumer(group_id=test_group)
    return producer, consumer


def test_kafka_health(kafka_infra):
    producer, _ = kafka_infra
    health = producer.check_health()
    assert health["reachable"] is True
    assert health["status"] == "HEALTHY"


def test_kafka_publish_consume_and_storage(kafka_infra):
    producer, consumer = kafka_infra
    hdfs = get_hdfs_client()

    test_student_id = f"KAFKA-TEST-{uuid.uuid4().hex[:6].upper()}"
    test_event_id = str(uuid.uuid4())
    test_features = {
        "Age at enrollment": 21,
        "Admission grade": 135.0,
        "Curricular units 1st sem (approved)": 5,
        "Curricular units 1st sem (grade)": 14.2,
        "Curricular units 2nd sem (approved)": 5,
        "Curricular units 2nd sem (grade)": 13.8,
        "Tuition fees up to date": 1,
        "Scholarship holder": 0,
        "Debtor": 0,
    }

    # 1. Publish record via Kafka
    pub_res = producer.publish_student_record(
        student_id=test_student_id,
        features=test_features,
        event_id=test_event_id,
        source="TEST_SUITE",
    )
    assert pub_res["status"] == "PUBLISHED"
    assert pub_res["student_id"] == test_student_id

    # 2. Consume from Kafka
    results = consumer.drain_records(max_records=10, timeout_ms=4000)
    assert len(results) >= 1

    # Find our event in consumer results
    matching = [r for r in results if r.get("event_id") == test_event_id]
    assert len(matching) == 1
    assert matching[0]["status"] == "PROCESSED"

    # 3. Verify SQLite DB persistence
    student_in_db = get_student(test_student_id)
    assert student_in_db is not None
    assert student_in_db["student_id"] == test_student_id
    assert student_in_db["features"]["Age at enrollment"] == 21

    # 4. Verify HDFS persistence
    if hdfs.check_health()["reachable"]:
        incoming_hdfs = f"{HDFS_INCOMING_DIR}/{test_event_id}.json"
        processed_hdfs = f"{HDFS_PROCESSED_DIR}/{test_student_id}.json"
        assert hdfs.exists(incoming_hdfs) is True
        assert hdfs.exists(processed_hdfs) is True

        read_data = hdfs.read_json(processed_hdfs)
        assert read_data["student_id"] == test_student_id

    # 5. Verify Idempotency: Duplicate event should be skipped
    dup_res = consumer.process_record({
        "event_id": test_event_id,
        "student_id": test_student_id,
        "event_type": "STUDENT_RECORD_INGESTED",
        "features": test_features,
        "source": "TEST_SUITE",
    })
    assert dup_res["status"] == "SKIPPED_DUPLICATE"


def test_malformed_event_handling(kafka_infra):
    _, consumer = kafka_infra
    bad_record = {
        "event_id": "bad-event",
        # Missing required fields like student_id, features
    }
    res = consumer.process_record(bad_record)
    assert res["status"] == "REJECTED"
    assert "Schema validation error" in res["error"]
