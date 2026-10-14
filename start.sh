#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " Starting EduGuard: Student Dropout Risk Prediction System"
echo "=========================================================="

# 1. Start Docker containers (Apache Kafka and Hadoop HDFS)
echo "[1/4] Ensuring Apache Kafka and Hadoop HDFS containers are up..."
if command -v docker >/dev/null 2>&1; then
    docker compose up -d
    echo "Containers verified."
else
    echo "Warning: docker command not found. Ensure Kafka and Hadoop are running natively."
fi

# 2. Check virtual environment
if [ -d ".venv" ]; then
    PYTHON=".venv/bin/python"
else
    PYTHON="python3"
fi

# 3. Check ML model artifact
if [ ! -f "models/pipeline.joblib" ]; then
    echo "[2/4] Training ML model on official UCI dataset..."
    PYTHONPATH=. $PYTHON src/ml/train.py
else
    echo "[2/4] Machine Learning model artifact verified."
fi

# 4. Initialize Database
echo "[3/5] Initializing operational database..."
PYTHONPATH=. $PYTHON -c "from src.storage.database import init_db; init_db()"

# 5. Seed UCI student data when the operational DB is empty
STUDENT_COUNT=$(PYTHONPATH=. $PYTHON -c "from src.storage.database import init_db, count_students; init_db(); print(count_students())")
if [ "$STUDENT_COUNT" = "0" ]; then
    echo "[4/5] Seeding UCI student records via Kafka (empty database detected)..."
    PYTHONPATH=. $PYTHON scripts/ingest_uci_data.py --limit 120 || echo "Warning: seed step failed; start the API and re-run scripts/ingest_uci_data.py"
else
    echo "[4/5] Operational database already has $STUDENT_COUNT students — skipping seed."
fi

# 6. Start Backend & Dashboard Server
echo "[5/5] Starting EduGuard FastAPI Backend & Advisor Dashboard on port 8000..."
echo "Advisor Dashboard available at: http://localhost:8000"
echo "Interactive API Documentation at: http://localhost:8000/docs"
echo "=========================================================="

PYTHONPATH=. $PYTHON -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
