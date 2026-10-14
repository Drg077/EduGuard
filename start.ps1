$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "=========================================================="
Write-Host " Starting EduGuard: Student Dropout Risk Prediction System"
Write-Host "=========================================================="

if (Get-Command docker -ErrorAction SilentlyContinue) {
    Write-Host "[1/5] Starting Apache Kafka and Hadoop HDFS containers..."
    docker compose up -d
} else {
    Write-Host "[1/5] Docker not found. Kafka and HDFS tests will be skipped until Docker is available."
}

if (Test-Path ".\.venv\Scripts\python.exe") {
    $Python = ".\.venv\Scripts\python.exe"
} else {
    Write-Host "Creating Python virtual environment..."
    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        py -3 -m venv .venv
    } else {
        python -m venv .venv
    }
    $Python = ".\.venv\Scripts\python.exe"
    & $Python -m pip install --upgrade pip
    & $Python -m pip install -r requirements.txt
}

$env:PYTHONPATH = (Get-Location).Path

if (-not (Test-Path ".\models\pipeline.joblib")) {
    Write-Host "[2/5] Training ML model on the UCI dataset..."
    & $Python src\ml\train.py
} else {
    Write-Host "[2/5] Machine Learning model artifact verified."
}

Write-Host "[3/5] Initializing operational database..."
& $Python -c "from src.storage.database import init_db; init_db()"

Write-Host "[4/5] Seeding student records if the database is empty..."
& $Python -c "from src.storage.database import count_students, init_db; init_db(); raise SystemExit(0 if count_students() == 0 else 1)"
if ($LASTEXITCODE -eq 0) {
    & $Python scripts\ingest_uci_data.py --limit 120
} else {
    Write-Host "Database already contains student records."
}

Write-Host "[5/5] Starting FastAPI backend and Advisor Dashboard on port 8000..."
Write-Host "Advisor Dashboard:           http://localhost:8000"
Write-Host "Interactive API docs:        http://localhost:8000/docs"
Write-Host "=========================================================="

& $Python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
