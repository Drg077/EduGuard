"""
Pydantic Schemas for EduGuard REST API
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class HealthStatus(BaseModel):
    status: str
    backend: str = "HEALTHY"
    database: str
    kafka: Dict[str, Any]
    hdfs: Dict[str, Any]
    model: Dict[str, Any]
    scheduler: Dict[str, Any]


class DashboardSummary(BaseModel):
    total_students: int
    students_with_predictions: int
    students_flagged: int
    students_not_flagged: int
    average_risk_score: float
    threshold: float
    latest_run: Optional[Dict[str, Any]] = None
    risk_distribution: List[int] = Field(default_factory=lambda: [0] * 10)


class StudentSummary(BaseModel):
    student_id: str
    risk_score: Optional[float] = None
    risk_category: Optional[str] = "Unknown"
    is_flagged: bool = False
    predicted_outcome: Optional[str] = None
    probabilities: Optional[Dict[str, float]] = None
    created_at: Optional[str] = None
    source: Optional[str] = None
    student_updated_at: Optional[str] = None


class StudentListResponse(BaseModel):
    students: List[StudentSummary]
    total: int
    page: int
    limit: int
    total_pages: int


class StudentDetailResponse(BaseModel):
    student_id: str
    source: str
    created_at: str
    updated_at: str
    features: Dict[str, Any]
    latest_prediction: Optional[Dict[str, Any]] = None
    prediction_history: List[Dict[str, Any]] = []


class IngestStudentRequest(BaseModel):
    student_id: str
    features: Dict[str, Any]
    source: str = "API_INGESTION"


class IngestBatchRequest(BaseModel):
    records: List[IngestStudentRequest]


class IngestResponse(BaseModel):
    message: str
    ingested_count: int
    kafka_published: bool
    results: List[Dict[str, Any]]


class PredictionRunTriggerResponse(BaseModel):
    message: str
    run_details: Dict[str, Any]


class SinglePredictRequest(BaseModel):
    student_id: Optional[str] = None
    features: Dict[str, Any]
