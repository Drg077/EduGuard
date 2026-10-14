#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " Starting EduGuard: Student Dropout Risk Prediction System"
echo "=========================================================="

# 1. Start Docker containers (Apache Kafka and Hadoop HDFS)
echo "[1/5] Ensuring Apache Kafka and Hadoop HDFS containers are up..."
if command -v docker >/dev/null 2>&1; then
    docker compose up -d
    echo "Containers verified."
else
    echo "Warning: docker command not found. Ensure Kafka and Hadoop are running natively."
fi

# 2. Check virtual environment
if [ -d ".venv" ]; then
    if [ -x ".venv/bin/python" ]; then
        PYTHON=".venv/bin/python"
    else
        PYTHON=".venv/Scripts/python"
    fi
else
    PYTHON="python3"
fi

export PYTHONPATH="$(pwd)"

# 3. Check ML model artifact
if [ ! -f "models/pipeline.joblib" ]; then
    echo "[2/5] Training ML model on official UCI dataset..."
    $PYTHON src/ml/train.py
else
    echo "[2/5] Machine Learning model artifact verified."
fi

# 4. Initialize Database
echo "[3/5] Initializing operational database..."
$PYTHON -c "from src.storage.database import init_db; init_db()"

# 5. Seed sample students if the operational database is empty
echo "[4/5] Seeding student records if the database is empty..."
if $PYTHON -c "from src.storage.database import count_students, init_db; init_db(); raise SystemExit(0 if count_students() == 0 else 1)"; then
    $PYTHON scripts/ingest_uci_data.py --limit 120
else
    echo "Database already contains student records."
fi

# 6. Start Backend & Dashboard Server
echo "[5/5] Starting EduGuard FastAPI Backend & Advisor Dashboard on port 8000..."
echo "Advisor Dashboard available at: http://localhost:8000"
echo "Interactive API Documentation at: http://localhost:8000/docs"
echo "=========================================================="

$PYTHON -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
