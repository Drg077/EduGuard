"""
EduGuard FastAPI Application Entrypoint
Configures CORS, lifecycle events, scheduler startup, and serves API routes.
"""

from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router as api_router
from src.kafka.consumer import get_kafka_consumer
from src.scheduler.prediction_job import get_scheduler
from src.storage.database import init_db
from src.storage.hdfs_client import get_hdfs_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("eduguard.server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager: starts up background workers and scheduler."""
    logger.info("Starting up EduGuard services...")
    # Initialize operational database
    init_db()

    # Ensure HDFS directories
    try:
        hdfs = get_hdfs_client()
        if hdfs.check_health()["reachable"]:
            hdfs.ensure_directories()
            logger.info("HDFS directories verified.")
    except Exception as e:
        logger.warning("HDFS directory check deferred: %s", e)

    # Start weekly prediction scheduler
    try:
        scheduler = get_scheduler()
        scheduler.start()
        logger.info("Weekly prediction scheduler started.")
    except Exception as e:
        logger.error("Failed to start scheduler: %s", e)

    # Start Kafka consumer background listener
    try:
        consumer = get_kafka_consumer()
        consumer.start_background_listener()
        logger.info("Kafka consumer background listener started.")
    except Exception as e:
        logger.warning("Kafka consumer background listener deferred: %s", e)

    yield

    logger.info("Shutting down EduGuard services...")
    try:
        get_scheduler().shutdown()
    except Exception:
        pass
    try:
        get_kafka_consumer().stop()
    except Exception:
        pass
    logger.info("Shutdown complete.")


app = FastAPI(
    title="EduGuard API",
    description="Student Performance & Dropout Risk Prediction System API",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for frontend dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pathlib import Path
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from src.config import BASE_DIR

FRONTEND_DIR = BASE_DIR / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

app.include_router(api_router)


@app.api_route("/", methods=["GET", "HEAD"])
@app.api_route("/dashboard", methods=["GET", "HEAD"])
def dashboard():
    """Serves the advisor dashboard web application."""
    index_file = FRONTEND_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {
        "project": "EduGuard",
        "description": "Student Performance & Dropout Risk Prediction System",
        "docs_url": "/docs",
        "health_url": "/api/health",
        "status": "ONLINE",
    }


if __name__ == "__main__":
    import uvicorn
    from src.config import API_HOST, API_PORT
    uvicorn.run("src.api.main:app", host=API_HOST, port=API_PORT, reload=False)
