"""
End-to-End Integration Test for EduGuard.
Exercises the entire student data pipeline:
Student Record -> Kafka Producer -> Kafka Consumer -> Validation
-> HDFS Storage -> ML Model Inference -> Risk-Score Generation
-> 0.6 Threshold Evaluation -> SQLite Persistence -> Dashboard API.
"""

import time
import uuid
import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.config import RISK_THRESHOLD
from src.kafka.consumer import EduGuardKafkaConsumer, get_kafka_consumer
from src.kafka.producer import get_kafka_producer
from src.scheduler.prediction_job import run_prediction_pipeline
from src.storage.database import get_student
from src.storage.hdfs_client import get_hdfs_client

client = TestClient(app)


def test_complete_end_to_end_pipeline():
    # Step 1: Create a distinct student record (At-Risk student profile)
    student_id = f"STU-E2E-{uuid.uuid4().hex[:6].upper()}"
    event_id = str(uuid.uuid4())
    student_features = {
        "Curricular units 1st sem (approved)": 0,
        "Curricular units 1st sem (evaluations)": 0,
        "Curricular units 1st sem (grade)": 0.0,
        "Curricular units 2nd sem (approved)": 0,
        "Curricular units 2nd sem (evaluations)": 0,
        "Curricular units 2nd sem (grade)": 0.0,
        "Tuition fees up to date": 0,
        "Age at enrollment": 26,
        "Admission grade": 115.0,
        "Debtor": 1,
        "Scholarship holder": 0,
    }

    # Step 2: Publish to Apache Kafka
    producer = get_kafka_producer()
    pub_res = producer.publish_student_record(
        student_id=student_id,
        features=student_features,
        event_id=event_id,
        source="E2E_TEST_SUITE",
    )
    assert pub_res["status"] == "PUBLISHED"
    assert pub_res["event_id"] == event_id

    # Step 3: Consume from Apache Kafka
    consumer = EduGuardKafkaConsumer(group_id=f"test-e2e-{uuid.uuid4().hex[:8]}")
    consumed = consumer.drain_records(max_records=500, timeout_ms=5000)
    matching = [c for c in consumed if c.get("event_id") == event_id]
    assert len(matching) == 1
    assert matching[0]["status"] == "PROCESSED"

    # Step 4: Verify HDFS Storage of incoming event and processed snapshot
    hdfs = get_hdfs_client()
    if hdfs.check_health()["reachable"]:
        incoming_hdfs_path = f"/eduguard/incoming/{event_id}.json"
        processed_hdfs_path = f"/eduguard/processed/{student_id}.json"
        assert hdfs.exists(incoming_hdfs_path) is True
        assert hdfs.exists(processed_hdfs_path) is True

        processed_hdfs_data = hdfs.read_json(processed_hdfs_path)
        assert processed_hdfs_data["student_id"] == student_id

    # Step 5: Execute Prediction Pipeline (Model inference + Threshold evaluation)
    run_res = run_prediction_pipeline(triggered_by="E2E_TEST")
    assert run_res["status"] in ("COMPLETED", "PARTIAL")

    # Step 6: Query Dashboard API for student details
    detail_res = client.get(f"/api/students/{student_id}")
    assert detail_res.status_code == 200
    student_detail = detail_res.json()
    assert student_detail["student_id"] == student_id
    assert student_detail["latest_prediction"] is not None

    pred = student_detail["latest_prediction"]
    score = pred["risk_score"]
    assert 0.0 <= score <= 1.0

    # Step 7: Verify Threshold Logic
    # Strict rule: score > 0.6
    expected_flag = score > RISK_THRESHOLD
    assert pred["is_flagged"] == expected_flag

    # Step 8: Verify Dashboard Summary reflects the student
    summary_res = client.get("/api/summary")
    assert summary_res.status_code == 200
    summary_data = summary_res.json()
    assert summary_data["total_students"] >= 1
    assert summary_data["students_with_predictions"] >= 1

    # Step 9: Verify Flagged filter if student is flagged
    if expected_flag:
        flagged_res = client.get("/api/students?only_flagged=true")
        assert flagged_res.status_code == 200
        flagged_ids = [s["student_id"] for s in flagged_res.json()["students"]]
        assert student_id in flagged_ids
