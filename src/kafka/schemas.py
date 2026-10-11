"""
Kafka Event Schemas for EduGuard
"""

import datetime
import uuid
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class StudentEventPayload(BaseModel):
    """Event payload for student data transfer via Kafka."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    student_id: str
    event_type: str = "STUDENT_RECORD_INGESTED"
    event_timestamp: str = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    features: Dict[str, Any]
    schema_version: str = "1.0.0"
    source: str = "API"
