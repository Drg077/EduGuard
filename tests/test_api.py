"""
Integration tests for EduGuard FastAPI backend endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from src.api.main import app
from src.storage.database import init_db

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_db():
    init_db()


def test_root_endpoint():
    res = client.get("/")
    assert res.status_code == 200
    assert "EduGuard" in res.text


def test_health_endpoint():
    res = client.get("/api/health")
    assert res.status_code == 200
    data = res.json()
    assert "status" in data
    assert data["backend"] == "HEALTHY"
    assert "kafka" in data
    assert "hdfs" in data
    assert "model" in data
    assert "scheduler" in data


def test_model_info_endpoint():
    res = client.get("/api/model/info")
    assert res.status_code == 200
    data = res.json()
    assert data["threshold"] == 0.6
    assert "metadata" in data
    assert data["metadata"]["algorithm"] == "GradientBoostingClassifier"


def test_predict_single_endpoint():
    # Valid high risk student
    payload = {
        "student_id": "API-TEST-001",
        "features": {
            "Curricular units 1st sem (approved)": 0,
            "Curricular units 1st sem (evaluations)": 0,
            "Curricular units 1st sem (grade)": 0.0,
            "Curricular units 2nd sem (approved)": 0,
            "Curricular units 2nd sem (evaluations)": 0,
            "Curricular units 2nd sem (grade)": 0.0,
            "Tuition fees up to date": 0,
            "Age at enrollment": 32,
            "Debtor": 1,
        },
    }
    res = client.post("/api/predict/single", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["student_id"] == "API-TEST-001"
    assert 0.0 <= data["dropout_risk_score"] <= 1.0
    assert data["is_flagged"] == (data["dropout_risk_score"] > 0.6)
    assert data["risk_category"] in {"Low", "Moderate", "High"}

    # Invalid payload (too few features)
    bad_payload = {"student_id": "BAD", "features": {"Age at enrollment": 20}}
    res_bad = client.post("/api/predict/single", json=bad_payload)
    assert res_bad.status_code == 400


def test_student_ingest_and_retrieval():
    payload = {
        "student_id": "STU-API-INGEST-777",
        "features": {
            "Curricular units 1st sem (approved)": 5,
            "Curricular units 1st sem (grade)": 14.0,
            "Curricular units 2nd sem (approved)": 5,
            "Curricular units 2nd sem (grade)": 13.5,
            "Admission grade": 130.0,
            "Age at enrollment": 20,
            "Tuition fees up to date": 1,
        },
        "source": "TEST_API",
    }
    # Ingest
    res_ingest = client.post("/api/students/ingest", json=payload)
    assert res_ingest.status_code == 200
    assert res_ingest.json()["ingested_count"] == 1

    # Run predictions so student has a record
    res_run = client.post("/api/predictions/run")
    assert res_run.status_code == 200
    assert res_run.json()["run_details"]["status"] == "COMPLETED"

    # Retrieve student detail
    res_detail = client.get("/api/students/STU-API-INGEST-777")
    assert res_detail.status_code == 200
    stu_data = res_detail.json()
    assert stu_data["student_id"] == "STU-API-INGEST-777"
    assert stu_data["latest_prediction"] is not None
    assert "prediction_history" in stu_data

    # Retrieve students list
    res_list = client.get("/api/students?limit=10")
    assert res_list.status_code == 200
    assert res_list.json()["total"] >= 1


def test_summary_and_runs_endpoints():
    res_summary = client.get("/api/summary")
    assert res_summary.status_code == 200
    summary = res_summary.json()
    assert summary["total_students"] >= 1
    assert summary["threshold"] == 0.6

    res_runs = client.get("/api/predictions/runs")
    assert res_runs.status_code == 200
    runs = res_runs.json()
    assert runs["total"] >= 1
