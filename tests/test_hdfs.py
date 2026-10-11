"""
Integration tests for Hadoop HDFS Client.
Verifies real communication with the running Hadoop cluster.
"""

import time
import pytest
from src.storage.hdfs_client import get_hdfs_client


@pytest.fixture(scope="module")
def hdfs():
    client = get_hdfs_client()
    # Check if HDFS is reachable
    health = client.check_health()
    if not health["reachable"]:
        pytest.skip(f"Hadoop HDFS cluster is not reachable: {health.get('message')}")
    client.ensure_directories()
    return client


def test_hdfs_health_and_directories(hdfs):
    health = hdfs.check_health()
    assert health["reachable"] is True
    assert health["status"] == "HEALTHY"

    # Verify standard EduGuard directories
    assert hdfs.exists("/eduguard")
    assert hdfs.exists("/eduguard/incoming")
    assert hdfs.exists("/eduguard/processed")
    assert hdfs.exists("/eduguard/predictions")
    assert hdfs.exists("/eduguard/model-data")


def test_hdfs_write_and_read_cycle(hdfs):
    test_path = f"/eduguard/test_cycle_{int(time.time())}.txt"
    test_content = "EduGuard HDFS Test Payload: Student Performance Verification"

    # Write file
    ok = hdfs.write_file(test_path, test_content, overwrite=True, atomic=True)
    assert ok is True
    assert hdfs.exists(test_path) is True

    # Read file
    content_read = hdfs.read_text(test_path)
    assert content_read == test_content

    # Clean up
    deleted = hdfs.delete(test_path)
    assert deleted is True
    assert hdfs.exists(test_path) is False


def test_hdfs_json_serialization(hdfs):
    test_path = f"/eduguard/test_json_{int(time.time())}.json"
    student_payload = {
        "student_id": "TEST-STU-999",
        "attendance_rate": 0.95,
        "first_sem_grade": 14.5,
        "risk_score": 0.22,
        "is_flagged": False,
    }

    # Write JSON
    ok = hdfs.write_json(test_path, student_payload, overwrite=True, atomic=True)
    assert ok is True

    # Read JSON
    read_payload = hdfs.read_json(test_path)
    assert read_payload == student_payload
    assert read_payload["student_id"] == "TEST-STU-999"

    # Clean up
    hdfs.delete(test_path)
