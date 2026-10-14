# EduGuard: Student Performance & Dropout Risk Prediction System

EduGuard is an educational data analytics and machine learning system that ingests student academic and demographic data via **Apache Kafka**, stores raw and processed snapshots in **Hadoop HDFS**, trains and executes a **Machine Learning classification pipeline** to predict student dropout risk, applies a strict **risk threshold (> 0.60)** for advisor follow-up flagging, schedules **weekly automated predictions**, and presents real-time data through a responsive **Advisor Dashboard**.

---

## 1. System Architecture

```text
               +---------------------------------------+
               |        Official UCI Dataset           |
               | (ID 697: Predict Students' Dropout)   |
               +---------------------------------------+
                                   |
                                   v
               +---------------------------------------+
               |       Apache Kafka Ingestion          |
               | Topic: eduguard.student-data (3 part) |
               +---------------------------------------+
                                   |
                                   v
               +---------------------------------------+
               |        Kafka Consumer & Engine        |
               |   - Schema validation & Idempotency   |
               |   - Atomic storage to Hadoop HDFS     |
               |   - SQLite Operational Database       |
               +---------------------------------------+
                  |                                 |
                  v                                 v
   +------------------------------+  +------------------------------+
   |         Hadoop HDFS          |  |     ML Inference Engine      |
   | /eduguard/incoming/          |  | GradientBoostingClassifier   |
   | /eduguard/processed/         |  | P(Dropout | X) in [0.0, 1.0] |
   | /eduguard/predictions/       |  | Threshold: strictly > 0.60   |
   +------------------------------+  +------------------------------+
                  |                                 |
                  +---------------+-----------------+
                                  |
                                  v
               +---------------------------------------+
               |     APScheduler Weekly Automation     |
               |       Every Monday at 08:00 UTC       |
               +---------------------------------------+
                                  |
                                  v
               +---------------------------------------+
               |        FastAPI REST API Server        |
               |       http://localhost:8000/api       |
               +---------------------------------------+
                                  |
                                  v
               +---------------------------------------+
               |           Advisor Dashboard           |
               |        http://localhost:8000/         |
               +---------------------------------------+
```

---

## 2. Key Technical Specifications

| Component | Technology | Version / Configuration |
| :--- | :--- | :--- |
| **Operating System** | macOS (Darwin 27.0.1 arm64) | Apple Silicon native runtime |
| **Message Streaming** | Apache Kafka (KRaft mode) | Official `apache/kafka:latest` |
| **Big Data Storage** | Apache Hadoop HDFS | `apache/hadoop:3.3.6` (WebHDFS & RPC) |
| **Machine Learning** | Scikit-Learn | GradientBoostingClassifier (Pipeline) |
| **Dataset** | UCI Repository | "Predict Students' Dropout and Academic Success" (ID 697) |
| **Risk Threshold** | Python backend logic | Strictly `score > 0.60` (0.60 is NOT flagged, 0.61 IS flagged) |
| **Scheduler** | APScheduler | Weekly Cron (`Every Mon at 08:00 UTC`) |
| **Backend API** | FastAPI / Uvicorn | Python 3.14 (Async REST with Lifespan) |
| **Operational DB** | SQLite | `data/eduguard.db` with WAL & Foreign Keys |
| **Advisor Dashboard** | HTML5 / Tailwind CSS / Chart.js | Real-time SPA served at `http://localhost:8000/` |

---

## 3. Machine Learning Model & Risk Score Calculation

### Dataset
- **Source:** UCI Machine Learning Repository (ID 697)
- **Total Records:** 4,424 students
- **Input Features:** 36 variables across curricular performance (1st and 2nd semester approved units, evaluations, grades), socio-economic background (debtor, tuition fees, scholarship), and demographics.
- **Target Classes:** `Graduate` (2,209), `Dropout` (1,421), `Enrolled` (794).

### Preprocessing & Training
- Stratified 80/20 train/test split (3,539 train, 885 held-out test).
- Preprocessing pipeline (`ColumnTransformer`) fits median imputers and `StandardScaler` for continuous features, and most-frequent imputers and `OneHotEncoder` for categorical features on training data only to prevent leakage.
- Classifier: `GradientBoostingClassifier(n_estimators=150, max_depth=4, learning_rate=0.1, random_state=42)`.

### Measured Evaluation Metrics (Held-out Test Set)
- **Accuracy:** `75.25%`
- **Dropout Class Precision:** `79.55%`
- **Dropout Class Recall:** `73.94%`
- **Dropout Class F1-Score:** `76.64%`
- **Brier Score Loss (Dropout Calibration):** `0.1065`

### Dropout Risk Score
The dropout-risk score is calculated directly as the model's calibrated probability of the student outcome being in the `Dropout` class:
$$\text{Dropout Risk Score} = P(\text{Dropout} \mid X) \in [0.0, 1.0]$$

---

## 4. Risk Threshold & Flagging Rule

The system strictly enforces the following decision rule in the backend:
$$\text{IF } \text{dropout\_risk\_score} > 0.60 \implies \text{Flag for Advisor Follow-up}$$
$$\text{ELSE} \implies \text{Do Not Automatically Flag}$$

### Boundary Behavior:
- `score = 0.59` $\to$ **Not Flagged** (Moderate Risk)
- `score = 0.60` $\to$ **Not Flagged** (Moderate Risk)
- `score = 0.61` $\to$ **Flagged** (High Risk)
- `score = 0.80` $\to$ **Flagged** (High Risk)

---

## 5. Apache Kafka Integration

- Kafka cluster runs via Docker Compose with KRaft metadata mode.
- Broker exposed on `localhost:9092`.
- Primary topic: `eduguard.student-data` (3 partitions, replication factor 1).
- **Producer (`src/kafka/producer.py`):** Validates event schema (`event_id`, `student_id`, `event_type`, `event_timestamp`, `features`, `schema_version`, `source`), serializes to JSON, and publishes to Kafka.
- **Consumer (`src/kafka/consumer.py`):** Consumes messages, verifies idempotency against `kafka_events` table (discarding duplicate events), writes raw events to HDFS `/eduguard/incoming/{event_id}.json`, writes processed student profile to HDFS `/eduguard/processed/{student_id}.json`, and updates SQLite.

---

## 6. Hadoop HDFS Integration

- Apache Hadoop 3.3.6 cluster runs via Docker Compose with NameNode (port 9870 WebHDFS, port 9000 RPC), DataNode (port 9864), and HttpFS (port 14000).
- Storage layout:
  - `/eduguard/incoming/` — Raw ingested Kafka event payloads
  - `/eduguard/processed/` — Processed student profile snapshots
  - `/eduguard/predictions/` — Weekly and manual batch prediction outputs
  - `/eduguard/model-data/` — Model artifacts and metadata
- **Python Client (`src/storage/hdfs_client.py`):** Connects to WebHDFS, handles NameNode 307 redirects to DataNode across container boundaries, implements atomic file creation via `.tmp.{uuid}` and WebHDFS RENAME.

---

## 7. Weekly Automatic Predictions

- Configured using **APScheduler** in `src/scheduler/prediction_job.py`.
- **Default Schedule:** Every Monday at 08:00 UTC (`PREDICTION_SCHEDULE_DAY=mon`, `PREDICTION_SCHEDULE_HOUR=8`, `PREDICTION_SCHEDULE_MINUTE=0`, `PREDICTION_SCHEDULE_TIMEZONE=UTC`).
- **Execution Workflow:**
  1. Retrieves all student profiles from operational database.
  2. Applies model inference to generate dropout-risk scores.
  3. Applies strict 0.60 threshold flagging.
  4. Persists predictions to SQLite history (preserving chronological trends).
  5. Stores prediction batch in HDFS at `/eduguard/predictions/{run_id}.json`.
  6. Logs run status, record counts, and timestamps in `prediction_runs` table.
- **Manual Trigger:** Available via endpoint `POST /api/predictions/run` or the "Run Predictions" button on the Advisor Dashboard.

---

## 8. How to Start the Application

### Prerequisites
- Docker & Docker Compose
- Python 3.12+ (or existing `.venv`)

### 1-Command Startup
On Windows (PowerShell):
```powershell
.\start.ps1
```

On macOS / Linux:
```bash
./start.sh
```

### Manual Startup (Windows PowerShell)
1. Start infrastructure containers:
   ```powershell
   docker compose up -d
   ```
2. Create a virtual environment and install dependencies (first run only):
   ```powershell
   py -3 -m venv .venv
   .\.venv\Scripts\python -m pip install -r requirements.txt
   $env:PYTHONPATH = "$PWD"
   ```
3. Train ML model (if `models/pipeline.joblib` is missing):
   ```powershell
   .\.venv\Scripts\python src\ml\train.py
   ```
4. Initialize SQLite and seed UCI student records:
   ```powershell
   .\.venv\Scripts\python -c "from src.storage.database import init_db; init_db()"
   .\.venv\Scripts\python scripts\ingest_uci_data.py --limit 120
   ```
5. Start the FastAPI server and advisor dashboard:
   ```powershell
   .\.venv\Scripts\python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
   ```

On macOS / Linux, use `.venv/bin/python` and `PYTHONPATH=.` in place of the PowerShell commands above.

### Access URLs
- **Advisor Dashboard:** [http://localhost:8000](http://localhost:8000)
- **Interactive API Documentation:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **System Health Endpoint:** [http://localhost:8000/api/health](http://localhost:8000/api/health)
- **Hadoop NameNode Web UI:** [http://localhost:9870](http://localhost:9870)

---

## 9. Running Automated Tests

To run the complete test suite across all 7 test modules:

Windows:
```powershell
$env:PYTHONPATH = "$PWD"
.\.venv\Scripts\python -m pytest tests/ -v
```

macOS / Linux:
```bash
PYTHONPATH=. .venv/bin/pytest tests/ -v
```

Offline unit tests (no Docker):
```powershell
.\.venv\Scripts\python -m pytest tests/test_threshold.py tests/test_ml.py -v
```

Kafka / HDFS / end-to-end tests require `docker compose up -d` first. Those tests skip automatically if the broker or NameNode is unreachable.

### Test Suite Summary (39 tests):
- `tests/test_threshold.py` (17 tests): Validates boundary scores (0.00, 0.30, 0.59, 0.60, 0.61, 0.80, 1.00) and ensures 0.60 is NOT flagged while 0.61 IS flagged.
- `tests/test_ml.py` (7 tests): Validates feature schemas, dataset existence, model loading, prediction score validity, reproducibility, and invalid input rejection.
- `tests/test_hdfs.py` (3 tests): Validates live HDFS cluster health, directory layout, atomic write/read cycle, and JSON serialization.
- `tests/test_kafka.py` (3 tests): Validates live Kafka broker connection, event publishing, consumption, database persistence, idempotency deduplication, and malformed payload rejection.
- `tests/test_scheduler.py` (2 tests): Validates weekly cron trigger configuration and batch prediction execution pipeline.
- `tests/test_api.py` (6 tests): Validates FastAPI REST endpoints (`/health`, `/summary`, `/model/info`, `/predict/single`, `/students`, `/students/{id}`, `/predictions/run`).
- `tests/test_e2e.py` (1 test): Exercises the full end-to-end integration workflow from student creation $\to$ Kafka $\to$ HDFS $\to$ ML inference $\to$ threshold flagging $\to$ persistence $\to$ dashboard API.
