"""
Unit and integration tests for EduGuard Weekly Prediction Scheduler.
"""

import time
import pytest
from src.scheduler.prediction_job import (
    EduGuardScheduler,
    run_prediction_pipeline,
)
from src.storage.database import (
    count_students,
    get_latest_prediction_run,
    get_latest_predictions,
    upsert_student,
)
from src.storage.hdfs_client import get_hdfs_client


def test_scheduler_configuration():
    sched = EduGuardScheduler()
    sched.start()
    info = sched.get_schedule_info()

    assert info["status"] == "RUNNING"
    assert "Every" in info["schedule"]
    assert info["next_run_time"] is not None
    assert info["timezone"] == "UTC"

    sched.shutdown()
    assert sched.get_schedule_info()["status"] == "STOPPED"


def test_prediction_pipeline_execution():
    # Insert two test students: one high risk, one low risk
    high_risk_features = {
        "Curricular units 1st sem (approved)": 0,
        "Curricular units 1st sem (evaluations)": 0,
        "Curricular units 1st sem (grade)": 0.0,
        "Curricular units 2nd sem (approved)": 0,
        "Curricular units 2nd sem (evaluations)": 0,
        "Curricular units 2nd sem (grade)": 0.0,
        "Tuition fees up to date": 0,
        "Age at enrollment": 29,
        "Debtor": 1,
    }
    low_risk_features = {
        "Curricular units 1st sem (approved)": 6,
        "Curricular units 1st sem (evaluations)": 6,
        "Curricular units 1st sem (grade)": 15.0,
        "Curricular units 2nd sem (approved)": 6,
        "Curricular units 2nd sem (evaluations)": 6,
        "Curricular units 2nd sem (grade)": 15.5,
        "Tuition fees up to date": 1,
        "Age at enrollment": 18,
        "Scholarship holder": 1,
        "Debtor": 0,
    }

    upsert_student("TEST-SCHED-HIGH", high_risk_features, source="SCHED_TEST")
    upsert_student("TEST-SCHED-LOW", low_risk_features, source="SCHED_TEST")

    # Run prediction pipeline
    result = run_prediction_pipeline(triggered_by="TEST_RUN")

    assert result["status"] == "COMPLETED"
    assert result["successful_predictions"] >= 2
    assert result["failed_predictions"] == 0

    # Verify latest run in DB
    latest_run = get_latest_prediction_run()
    assert latest_run is not None
    assert latest_run["run_id"] == result["run_id"]
    assert latest_run["status"] == "COMPLETED"

    # Verify predictions in DB
    preds, total = get_latest_predictions(limit=100)
    high_preds = [p for p in preds if p["student_id"] == "TEST-SCHED-HIGH"]
    low_preds = [p for p in preds if p["student_id"] == "TEST-SCHED-LOW"]

    assert len(high_preds) == 1
    assert len(low_preds) == 1

    # Verify strict threshold rule is respected
    assert high_preds[0]["is_flagged"] == (high_preds[0]["risk_score"] > 0.6)
    assert low_preds[0]["is_flagged"] == (low_preds[0]["risk_score"] > 0.6)

    # Verify HDFS batch export if reachable
    hdfs = get_hdfs_client()
    if hdfs.check_health()["reachable"]:
        batch_path = f"/eduguard/predictions/{result['run_id']}.json"
        assert hdfs.exists(batch_path) is True
        batch_json = hdfs.read_json(batch_path)
        assert batch_json["run_id"] == result["run_id"]
