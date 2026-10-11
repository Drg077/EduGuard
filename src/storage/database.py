"""
EduGuard Operational Database Module
Manages SQLite database storage for students, predictions, runs, and event idempotency.
"""

import json
import sqlite3
import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.config import DATABASE_FILE, RISK_THRESHOLD, get_risk_category, is_student_flagged

logger = logging.getLogger("eduguard.db")


def get_connection() -> sqlite3.Connection:
    """Creates a connection to SQLite with row-factory and foreign keys enabled."""
    DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DATABASE_FILE), timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    """Initializes operational tables and indexes."""
    with get_connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS students (
            student_id TEXT PRIMARY KEY,
            features_json TEXT NOT NULL,
            source TEXT DEFAULT 'UNKNOWN',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS prediction_runs (
            run_id TEXT PRIMARY KEY,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT NOT NULL, -- 'RUNNING', 'COMPLETED', 'FAILED'
            total_records INTEGER DEFAULT 0,
            successful_predictions INTEGER DEFAULT 0,
            failed_predictions INTEGER DEFAULT 0,
            error_summary TEXT,
            triggered_by TEXT DEFAULT 'SCHEDULER' -- 'SCHEDULER' or 'MANUAL'
        );

        CREATE TABLE IF NOT EXISTS predictions (
            prediction_id TEXT PRIMARY KEY,
            student_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            risk_score REAL NOT NULL,
            is_flagged INTEGER NOT NULL, -- 0 or 1 (strictly risk_score > 0.6)
            risk_category TEXT NOT NULL,
            predicted_outcome TEXT,
            probabilities_json TEXT,
            features_snapshot_json TEXT,
            model_version TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (student_id) REFERENCES students (student_id) ON DELETE CASCADE,
            FOREIGN KEY (run_id) REFERENCES prediction_runs (run_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS kafka_events (
            event_id TEXT PRIMARY KEY,
            student_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            status TEXT NOT NULL -- 'PROCESSED', 'FAILED'
        );

        CREATE INDEX IF NOT EXISTS idx_predictions_student_created ON predictions(student_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_predictions_run ON predictions(run_id);
        CREATE INDEX IF NOT EXISTS idx_predictions_flagged ON predictions(is_flagged);
        """)
    logger.info("Operational database initialized at %s", DATABASE_FILE)


# --- Students DAO ---

def upsert_student(student_id: str, features: Dict[str, Any], source: str = "API") -> None:
    """Inserts or updates a student profile."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    features_str = json.dumps(features)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO students (student_id, features_json, source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(student_id) DO UPDATE SET
                features_json = excluded.features_json,
                source = excluded.source,
                updated_at = excluded.updated_at;
            """,
            (student_id, features_str, source, now, now),
        )


def get_student(student_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a student by ID."""
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM students WHERE student_id = ?", (student_id,)).fetchone()
        if not row:
            return None
        res = dict(row)
        res["features"] = json.loads(res["features_json"])
        del res["features_json"]
        return res


def list_students(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """Lists students with pagination."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT student_id, source, created_at, updated_at FROM students ORDER BY student_id LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]


def get_all_student_ids() -> List[str]:
    """Retrieves all student IDs."""
    with get_connection() as conn:
        rows = conn.execute("SELECT student_id FROM students").fetchall()
        return [r["student_id"] for r in rows]


def count_students() -> int:
    """Returns the total number of students in the database."""
    with get_connection() as conn:
        val = conn.execute("SELECT COUNT(*) FROM students").fetchone()[0]
        return int(val)


# --- Prediction Runs DAO ---

def create_prediction_run(run_id: str, triggered_by: str = "SCHEDULER") -> None:
    """Creates a new prediction run record."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO prediction_runs (run_id, start_time, status, triggered_by)
            VALUES (?, ?, 'RUNNING', ?);
            """,
            (run_id, now, triggered_by),
        )


def finish_prediction_run(
    run_id: str,
    status: str,
    total: int,
    successful: int,
    failed: int,
    error_summary: Optional[str] = None,
) -> None:
    """Updates prediction run upon completion."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE prediction_runs SET
                end_time = ?,
                status = ?,
                total_records = ?,
                successful_predictions = ?,
                failed_predictions = ?,
                error_summary = ?
            WHERE run_id = ?;
            """,
            (now, status, total, successful, failed, error_summary, run_id),
        )


def get_latest_prediction_run() -> Optional[Dict[str, Any]]:
    """Returns the most recent prediction run."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM prediction_runs ORDER BY start_time DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None


def list_prediction_runs(limit: int = 20) -> List[Dict[str, Any]]:
    """Lists recent prediction runs."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM prediction_runs ORDER BY start_time DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# --- Predictions DAO ---

def save_prediction(
    prediction_id: str,
    student_id: str,
    run_id: str,
    risk_score: float,
    risk_category: str,
    is_flagged: bool,
    predicted_outcome: str,
    probabilities: Dict[str, float],
    features_snapshot: Dict[str, Any],
    model_version: str,
) -> None:
    """Saves a prediction record."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO predictions (
                prediction_id, student_id, run_id, risk_score, is_flagged,
                risk_category, predicted_outcome, probabilities_json,
                features_snapshot_json, model_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                prediction_id,
                student_id,
                run_id,
                risk_score,
                1 if is_flagged else 0,
                risk_category,
                predicted_outcome,
                json.dumps(probabilities),
                json.dumps(features_snapshot),
                model_version,
                now,
            ),
        )


def get_latest_predictions(
    only_flagged: bool = False,
    search: Optional[str] = None,
    risk_category: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Returns the latest prediction for each student, joined with student information,
    with filtering, searching, and pagination support.
    """
    with get_connection() as conn:
        # Subquery finds the latest prediction per student
        query = """
        WITH RankedPredictions AS (
            SELECT p.*,
                   ROW_NUMBER() OVER(PARTITION BY p.student_id ORDER BY p.created_at DESC) as rn
            FROM predictions p
        )
        SELECT rp.prediction_id, rp.student_id, rp.run_id, rp.risk_score, rp.is_flagged,
               rp.risk_category, rp.predicted_outcome, rp.probabilities_json,
               rp.features_snapshot_json, rp.model_version, rp.created_at,
               s.source, s.updated_at as student_updated_at
        FROM RankedPredictions rp
        JOIN students s ON rp.student_id = s.student_id
        WHERE rp.rn = 1
        """
        params = []

        if only_flagged:
            query += " AND rp.is_flagged = 1"

        if risk_category and risk_category != "ALL":
            query += " AND rp.risk_category = ?"
            params.append(risk_category)

        if search:
            query += " AND rp.student_id LIKE ?"
            params.append(f"%{search}%")

        # Total count for pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        total = conn.execute(count_query, params).fetchone()[0]

        query += " ORDER BY rp.risk_score DESC, rp.created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["is_flagged"] = bool(d["is_flagged"])
            d["probabilities"] = json.loads(d["probabilities_json"]) if d["probabilities_json"] else {}
            d["features_snapshot"] = json.loads(d["features_snapshot_json"]) if d["features_snapshot_json"] else {}
            del d["probabilities_json"]
            del d["features_snapshot_json"]
            result.append(d)
        return result, int(total)


def get_student_prediction_history(student_id: str) -> List[Dict[str, Any]]:
    """Retrieves all historical predictions for a given student in chronological order."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM predictions
            WHERE student_id = ?
            ORDER BY created_at ASC;
            """,
            (student_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["is_flagged"] = bool(d["is_flagged"])
            d["probabilities"] = json.loads(d["probabilities_json"]) if d["probabilities_json"] else {}
            d["features_snapshot"] = json.loads(d["features_snapshot_json"]) if d["features_snapshot_json"] else {}
            del d["probabilities_json"]
            del d["features_snapshot_json"]
            result.append(d)
        return result


def get_dashboard_summary() -> Dict[str, Any]:
    """
    Computes dashboard summary metrics strictly from actual database records:
    - total students
    - students with predictions
    - flagged count (risk_score > 0.6)
    - unflagged count
    - latest prediction run status
    """
    with get_connection() as conn:
        total_students = conn.execute("SELECT COUNT(*) FROM students").fetchone()[0]

        summary_row = conn.execute("""
        WITH RankedPredictions AS (
            SELECT student_id, risk_score, is_flagged,
                   ROW_NUMBER() OVER(PARTITION BY student_id ORDER BY created_at DESC) as rn
            FROM predictions
        )
        SELECT
            COUNT(*) as with_predictions,
            SUM(CASE WHEN is_flagged = 1 THEN 1 ELSE 0 END) as flagged_count,
            SUM(CASE WHEN is_flagged = 0 THEN 1 ELSE 0 END) as not_flagged_count,
            AVG(risk_score) as avg_risk_score
        FROM RankedPredictions
        WHERE rn = 1;
        """).fetchone()

        with_predictions = summary_row[0] or 0
        flagged_count = summary_row[1] or 0
        not_flagged_count = summary_row[2] or 0
        avg_risk_score = round(summary_row[3] or 0.0, 4)

        latest_run = get_latest_prediction_run()

        return {
            "total_students": int(total_students),
            "students_with_predictions": int(with_predictions),
            "students_flagged": int(flagged_count),
            "students_not_flagged": int(not_flagged_count),
            "average_risk_score": avg_risk_score,
            "threshold": RISK_THRESHOLD,
            "latest_run": latest_run,
        }


# --- Kafka Event Deduplication DAO ---

def is_event_processed(event_id: str) -> bool:
    """Checks if a Kafka event has already been processed."""
    with get_connection() as conn:
        row = conn.execute("SELECT 1 FROM kafka_events WHERE event_id = ?", (event_id,)).fetchone()
        return row is not None


def record_processed_event(event_id: str, student_id: str, event_type: str, status: str = "PROCESSED") -> None:
    """Records an event identifier to ensure idempotent processing."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO kafka_events (event_id, student_id, event_type, processed_at, status)
            VALUES (?, ?, ?, ?, ?);
            """,
            (event_id, student_id, event_type, now, status),
        )
