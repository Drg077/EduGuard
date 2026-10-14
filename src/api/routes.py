"""
EduGuard API Routes Module
Provides endpoints for dashboard statistics, students, predictions, Kafka ingestion,
and model inspection.
"""

import logging
import math
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import (
    DashboardSummary,
    HealthStatus,
    IngestBatchRequest,
    IngestResponse,
    IngestStudentRequest,
    PredictionRunTriggerResponse,
    SinglePredictRequest,
    StudentDetailResponse,
    StudentListResponse,
    StudentSummary,
)
from src.config import RISK_THRESHOLD
from src.kafka.consumer import get_kafka_consumer
from src.kafka.producer import get_kafka_producer
from src.ml.pipeline import load_model, predict_dropout_risk
from src.scheduler.prediction_job import get_scheduler, run_prediction_pipeline
from src.storage.database import (
    count_students,
    get_dashboard_summary,
    get_latest_prediction_run,
    get_latest_predictions,
    get_student,
    get_student_prediction_history,
    list_prediction_runs,
    list_students,
)
from src.storage.hdfs_client import get_hdfs_client

logger = logging.getLogger("eduguard.api")
router = APIRouter(prefix="/api")


@router.get("/health", response_model=HealthStatus)
def health_check():
    """Returns comprehensive system health status across all components."""
    # Database check
    try:
        count = count_students()
        db_status = "HEALTHY"
    except Exception as e:
        db_status = f"ERROR: {e}"

    # Kafka check
    producer = get_kafka_producer()
    kafka_health = producer.check_health()

    # HDFS check
    hdfs = get_hdfs_client()
    hdfs_health = hdfs.check_health()

    # Model check
    try:
        _, metadata = load_model()
        model_health = {
            "status": "LOADED",
            "algorithm": metadata.get("algorithm", "GradientBoostingClassifier"),
            "version": metadata.get("version", "1.0.0"),
        }
    except Exception as e:
        model_health = {"status": "UNAVAILABLE", "error": str(e)}

    # Scheduler check
    sched = get_scheduler()
    scheduler_health = sched.get_schedule_info()

    all_healthy = (
        db_status == "HEALTHY"
        and kafka_health.get("reachable", False)
        and hdfs_health.get("reachable", False)
        and model_health.get("status") == "LOADED"
    )

    return HealthStatus(
        status="OPERATIONAL" if all_healthy else "DEGRADED",
        backend="HEALTHY",
        database=db_status,
        kafka=kafka_health,
        hdfs=hdfs_health,
        model=model_health,
        scheduler=scheduler_health,
    )


@router.get("/summary", response_model=DashboardSummary)
def get_summary():
    """Returns aggregated metrics for the advisor dashboard overview."""
    summary_data = get_dashboard_summary()
    return DashboardSummary(**summary_data)


@router.get("/students", response_model=StudentListResponse)
def get_students(
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    search: Optional[str] = None,
    only_flagged: bool = False,
    risk_category: Optional[str] = None,
):
    """
    Retrieves a paginated list of students with their latest risk scores,
    flag status, and prediction details.
    """
    offset = (page - 1) * limit
    predictions, total = get_latest_predictions(
        only_flagged=only_flagged,
        search=search,
        risk_category=risk_category,
        limit=limit,
        offset=offset,
    )

    students_list = [
        StudentSummary(
            student_id=p["student_id"],
            risk_score=p["risk_score"],
            risk_category=p["risk_category"],
            is_flagged=p["is_flagged"],
            predicted_outcome=p["predicted_outcome"],
            probabilities=p["probabilities"],
            created_at=p["created_at"],
            source=p.get("source"),
            student_updated_at=p.get("student_updated_at"),
        )
        for p in predictions
    ]

    total_pages = math.ceil(total / limit) if total > 0 else 1

    return StudentListResponse(
        students=students_list,
        total=total,
        page=page,
        limit=limit,
        total_pages=total_pages,
    )


@router.get("/students/{student_id}", response_model=StudentDetailResponse)
def get_student_details(student_id: str):
    """Retrieves full profile, current features, latest prediction, and history for a student."""
    student = get_student(student_id)
    if not student:
        raise HTTPException(status_code=404, detail=f"Student {student_id} not found")

    history = get_student_prediction_history(student_id)
    latest = history[-1] if history else None

    return StudentDetailResponse(
        student_id=student["student_id"],
        source=student["source"],
        created_at=student["created_at"],
        updated_at=student["updated_at"],
        features=student["features"],
        latest_prediction=latest,
        prediction_history=history,
    )


@router.post("/students/ingest", response_model=IngestResponse)
def ingest_student(req: IngestStudentRequest):
    """
    Ingests a student record via Apache Kafka.
    The message is published to Kafka topic and processed by consumer into HDFS and SQLite.
    """
    producer = get_kafka_producer()
    try:
        pub_res = producer.publish_student_record(
            student_id=req.student_id,
            features=req.features,
            source=req.source,
        )
        producer.flush()

        # Persist immediately so the record is available even if the Kafka
        # consumer group is still electing a coordinator.
        consumer = get_kafka_consumer()
        consumer.process_record(
            {
                "event_id": pub_res["event_id"],
                "student_id": req.student_id,
                "event_type": "STUDENT_RECORD_INGESTED",
                "features": req.features,
                "source": req.source,
            }
        )
        consumer.drain_records(max_records=5, timeout_ms=2000)

        return IngestResponse(
            message=f"Student {req.student_id} ingested successfully via Kafka.",
            ingested_count=1,
            kafka_published=True,
            results=[pub_res],
        )
    except Exception as e:
        logger.error("Failed to ingest student: %s", e)
        raise HTTPException(status_code=500, detail=f"Kafka ingestion error: {str(e)}")


@router.post("/students/ingest-batch", response_model=IngestResponse)
def ingest_batch(req: IngestBatchRequest):
    """Ingests a batch of student records via Kafka."""
    producer = get_kafka_producer()
    results = []
    try:
        for r in req.records:
            res = producer.publish_student_record(
                student_id=r.student_id,
                features=r.features,
                source=r.source,
            )
            results.append(res)
        producer.flush()

        consumer = get_kafka_consumer()
        for r, pub in zip(req.records, results):
            consumer.process_record(
                {
                    "event_id": pub["event_id"],
                    "student_id": r.student_id,
                    "event_type": "STUDENT_RECORD_INGESTED",
                    "features": r.features,
                    "source": r.source,
                }
            )
        consumer.drain_records(max_records=len(req.records) + 5, timeout_ms=3000)

        return IngestResponse(
            message=f"Successfully ingested {len(results)} records via Kafka.",
            ingested_count=len(results),
            kafka_published=True,
            results=results,
        )
    except Exception as e:
        logger.error("Batch ingestion error: %s", e)
        raise HTTPException(status_code=500, detail=f"Kafka batch ingestion error: {str(e)}")


@router.get("/predictions/runs")
def get_prediction_runs(limit: int = 20):
    """Returns execution history and metadata for prediction runs."""
    runs = list_prediction_runs(limit=limit)
    return {"runs": runs, "total": len(runs)}


@router.post("/predictions/run", response_model=PredictionRunTriggerResponse)
def trigger_prediction_run():
    """Manually triggers the weekly prediction pipeline for all students."""
    try:
        result = run_prediction_pipeline(triggered_by="MANUAL_API")
        return PredictionRunTriggerResponse(
            message="Prediction pipeline executed successfully.",
            run_details=result,
        )
    except Exception as e:
        logger.error("Prediction run failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Prediction pipeline error: {str(e)}")


@router.post("/predict/single")
def predict_single(req: SinglePredictRequest):
    """Calculates real-time dropout risk score for provided student features."""
    try:
        pred = predict_dropout_risk(req.features, student_id=req.student_id)
        return pred
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")


@router.get("/model/info")
def get_model_info():
    """Returns detailed model metadata, training metrics, and interpretation guidelines."""
    _, metadata = load_model()
    return {
        "metadata": metadata,
        "threshold": RISK_THRESHOLD,
        "flagging_rule": f"Flagged for advisor follow-up if dropout_risk_score > {RISK_THRESHOLD}",
        "dataset": "UCI Predict Students' Dropout and Academic Success (ID 697)",
    }
