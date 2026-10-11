"""
EduGuard Weekly Automatic Prediction Scheduler and Execution Engine
Orchestrates scheduled batch predictions, records job metadata and prediction history,
and exports results to both SQLite and Hadoop HDFS.
"""

import datetime
import logging
import uuid
from typing import Any, Dict, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import (
    HDFS_PREDICTIONS_DIR,
    SCHEDULE_DAY_OF_WEEK,
    SCHEDULE_HOUR,
    SCHEDULE_MINUTE,
    SCHEDULE_TIMEZONE,
)
from src.ml.pipeline import predict_dropout_risk
from src.storage.database import (
    create_prediction_run,
    finish_prediction_run,
    get_all_student_ids,
    get_student,
    save_prediction,
)
from src.storage.hdfs_client import get_hdfs_client

logger = logging.getLogger("eduguard.scheduler")


def run_prediction_pipeline(
    run_id: Optional[str] = None,
    triggered_by: str = "SCHEDULER",
) -> Dict[str, Any]:
    """
    Executes the batch prediction pipeline across all current student profiles.
    
    1. Creates prediction run record.
    2. Retrieves student profiles from database.
    3. Runs ML inference for each student.
    4. Evaluates risk threshold (strictly > 0.6).
    5. Persists predictions to SQLite history.
    6. Stores batch results in Hadoop HDFS.
    7. Updates run status.
    """
    if not run_id:
        timestamp_slug = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_id = f"RUN_{timestamp_slug}_{uuid.uuid4().hex[:6]}"

    logger.info("Initiating prediction run %s (triggered_by: %s)", run_id, triggered_by)
    create_prediction_run(run_id=run_id, triggered_by=triggered_by)

    student_ids = get_all_student_ids()
    total_records = len(student_ids)

    if total_records == 0:
        logger.warning("No student records found to process in run %s", run_id)
        finish_prediction_run(
            run_id=run_id,
            status="COMPLETED",
            total=0,
            successful=0,
            failed=0,
            error_summary="No student records found to predict.",
        )
        return {
            "run_id": run_id,
            "status": "COMPLETED",
            "total_records": 0,
            "successful_predictions": 0,
            "failed_predictions": 0,
            "message": "No student records available.",
        }

    successful = 0
    failed = 0
    predictions_output: List[Dict[str, Any]] = []
    errors: List[str] = []

    for sid in student_ids:
        try:
            student_data = get_student(sid)
            if not student_data or "features" not in student_data:
                failed += 1
                errors.append(f"Student {sid}: features missing")
                continue

            features = student_data["features"]
            # Generate ML prediction
            pred_res = predict_dropout_risk(features, student_id=sid)

            prediction_id = f"PRED_{run_id}_{sid}"
            save_prediction(
                prediction_id=prediction_id,
                student_id=sid,
                run_id=run_id,
                risk_score=pred_res["dropout_risk_score"],
                risk_category=pred_res["risk_category"],
                is_flagged=pred_res["is_flagged"],
                predicted_outcome=pred_res["predicted_outcome"],
                probabilities=pred_res["probabilities"],
                features_snapshot=features,
                model_version=pred_res["model_version"],
            )

            successful += 1
            predictions_output.append({
                "prediction_id": prediction_id,
                "student_id": sid,
                "risk_score": pred_res["dropout_risk_score"],
                "risk_category": pred_res["risk_category"],
                "is_flagged": pred_res["is_flagged"],
                "predicted_outcome": pred_res["predicted_outcome"],
                "probabilities": pred_res["probabilities"],
                "timestamp": pred_res["timestamp"],
            })

        except Exception as e:
            failed += 1
            err_text = f"Student {sid}: {str(e)}"
            logger.error(err_text)
            errors.append(err_text)

    # Persist run batch to Hadoop HDFS
    try:
        hdfs = get_hdfs_client()
        if hdfs.check_health()["reachable"]:
            hdfs_batch_path = f"{HDFS_PREDICTIONS_DIR}/{run_id}.json"
            batch_payload = {
                "run_id": run_id,
                "triggered_by": triggered_by,
                "executed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "total_records": total_records,
                "successful": successful,
                "failed": failed,
                "predictions": predictions_output,
            }
            hdfs.write_json(hdfs_batch_path, batch_payload, overwrite=True, atomic=True)
            logger.info("Saved prediction run batch to HDFS at %s", hdfs_batch_path)
    except Exception as e:
        logger.warning("Could not persist prediction batch to HDFS: %s", e)

    run_status = "COMPLETED" if failed == 0 else ("PARTIAL" if successful > 0 else "FAILED")
    error_summary = "; ".join(errors[:5]) if errors else None
    if len(errors) > 5:
        error_summary += f" (and {len(errors) - 5} more)"

    finish_prediction_run(
        run_id=run_id,
        status=run_status,
        total=total_records,
        successful=successful,
        failed=failed,
        error_summary=error_summary,
    )

    logger.info(
        "Prediction run %s finished with status %s (%d successful, %d failed)",
        run_id,
        run_status,
        successful,
        failed,
    )

    return {
        "run_id": run_id,
        "status": run_status,
        "total_records": total_records,
        "successful_predictions": successful,
        "failed_predictions": failed,
        "error_summary": error_summary,
        "predictions_sample": predictions_output[:5],
    }


class EduGuardScheduler:
    """
    Manages the recurring weekly prediction schedule using APScheduler.
    """

    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone=SCHEDULE_TIMEZONE)
        self.job_id = "weekly_dropout_prediction_job"
        self._is_started = False

    def start(self) -> None:
        """Starts the scheduler with the configured weekly cron schedule."""
        if self._is_started:
            return

        trigger = CronTrigger(
            day_of_week=SCHEDULE_DAY_OF_WEEK,
            hour=SCHEDULE_HOUR,
            minute=SCHEDULE_MINUTE,
            timezone=SCHEDULE_TIMEZONE,
        )

        self.scheduler.add_job(
            func=run_prediction_pipeline,
            trigger=trigger,
            id=self.job_id,
            name="Weekly Automatic Student Dropout Prediction",
            replace_existing=True,
            kwargs={"triggered_by": "SCHEDULER"},
        )

        self.scheduler.start()
        self._is_started = True
        logger.info(
            "EduGuard weekly scheduler started. Configured for %s at %02d:%02d %s.",
            SCHEDULE_DAY_OF_WEEK,
            SCHEDULE_HOUR,
            SCHEDULE_MINUTE,
            SCHEDULE_TIMEZONE,
        )

    def shutdown(self) -> None:
        """Stops the scheduler gracefully."""
        if self._is_started:
            self.scheduler.shutdown(wait=False)
            self._is_started = False
            logger.info("EduGuard scheduler stopped.")

    def get_schedule_info(self) -> Dict[str, Any]:
        """Returns details on the active recurring schedule."""
        job = self.scheduler.get_job(self.job_id) if self._is_started else None
        next_run = job.next_run_time.isoformat() if (job and job.next_run_time) else None

        return {
            "status": "RUNNING" if self._is_started else "STOPPED",
            "schedule": f"Every {SCHEDULE_DAY_OF_WEEK.capitalize()} at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} {SCHEDULE_TIMEZONE}",
            "day_of_week": SCHEDULE_DAY_OF_WEEK,
            "hour": SCHEDULE_HOUR,
            "minute": SCHEDULE_MINUTE,
            "timezone": SCHEDULE_TIMEZONE,
            "next_run_time": next_run,
        }

    def trigger_manual_run(self) -> Dict[str, Any]:
        """Manually triggers a prediction run for immediate execution."""
        return run_prediction_pipeline(triggered_by="MANUAL")


_DEFAULT_SCHEDULER = None


def get_scheduler() -> EduGuardScheduler:
    """Returns singleton scheduler instance."""
    global _DEFAULT_SCHEDULER
    if _DEFAULT_SCHEDULER is None:
        _DEFAULT_SCHEDULER = EduGuardScheduler()
    return _DEFAULT_SCHEDULER
