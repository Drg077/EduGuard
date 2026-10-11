"""
EduGuard Configuration Module
Centralized configuration for thresholds, service endpoints, and schedules.
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Dataset path
DATASET_PATH = DATA_DIR / "student_dropout_academic_success.csv"
MODEL_PATH = MODELS_DIR / "pipeline.joblib"
MODEL_METADATA_PATH = MODELS_DIR / "model_metadata.json"

# Operational Database
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR / 'eduguard.db'}")
DATABASE_FILE = DATA_DIR / "eduguard.db"

# Critical Risk Threshold: STRICTLY greater than 0.6 triggers a flag.
# Boundary specification:
# score = 0.60 -> False (Not flagged)
# score = 0.61 -> True (Flagged)
RISK_THRESHOLD = float(os.getenv("EDUGUARD_RISK_THRESHOLD", "0.6"))

# Risk Categories:
# Low: [0.0, 0.35]
# Moderate: (0.35, 0.60]
# High: (0.60, 1.0] (All flagged students are in High risk)
def get_risk_category(score: float) -> str:
    """Categorizes a dropout risk score for advisor presentation."""
    if score is None:
        return "Unknown"
    if score > RISK_THRESHOLD:
        return "High"
    elif score > 0.35:
        return "Moderate"
    else:
        return "Low"

def is_student_flagged(score: float) -> bool:
    """
    Enforces the mandatory flagging rule:
    IF dropout_risk_score > 0.6: Flag
    ELSE: Do not flag
    Strictly greater than 0.6.
    """
    if score is None:
        return False
    return score > RISK_THRESHOLD


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "eduguard.student-data")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "eduguard-consumer-group")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "eduguard-app")

# Hadoop HDFS configuration
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_HTTPFS_URL = os.getenv("HDFS_HTTPFS_URL", "http://localhost:14000")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_ROOT_DIR = os.getenv("HDFS_ROOT_DIR", "/eduguard")
HDFS_INCOMING_DIR = f"{HDFS_ROOT_DIR}/incoming"
HDFS_PROCESSED_DIR = f"{HDFS_ROOT_DIR}/processed"
HDFS_PREDICTIONS_DIR = f"{HDFS_ROOT_DIR}/predictions"
HDFS_MODEL_DIR = f"{HDFS_ROOT_DIR}/model-data"

# Weekly scheduler configuration
# Configured default: Every Monday at 08:00 UTC
SCHEDULE_DAY_OF_WEEK = os.getenv("PREDICTION_SCHEDULE_DAY", "mon")
SCHEDULE_HOUR = int(os.getenv("PREDICTION_SCHEDULE_HOUR", "8"))
SCHEDULE_MINUTE = int(os.getenv("PREDICTION_SCHEDULE_MINUTE", "0"))
SCHEDULE_TIMEZONE = os.getenv("PREDICTION_SCHEDULE_TIMEZONE", "UTC")

# API Configuration
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
FRONTEND_PORT = int(os.getenv("FRONTEND_PORT", "3000"))
