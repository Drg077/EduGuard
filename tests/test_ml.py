"""
Unit tests for Machine Learning Pipeline, Preprocessing, and Inference.
"""

import json
import pytest
import pandas as pd
from src.config import MODEL_METADATA_PATH, MODEL_PATH, DATASET_PATH
from src.ml.pipeline import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    CONTINUOUS_FEATURES,
    load_model,
    predict_dropout_risk,
    validate_student_features,
)


def test_feature_definitions():
    assert len(ALL_FEATURES) == 36
    assert len(CATEGORICAL_FEATURES) == 18
    assert len(CONTINUOUS_FEATURES) == 18
    assert len(set(ALL_FEATURES)) == 36


def test_dataset_file_exists():
    assert DATASET_PATH.exists(), f"Official dataset not found at {DATASET_PATH}"
    df = pd.read_csv(DATASET_PATH)
    assert len(df) == 4424
    assert "Target" in df.columns
    assert set(df["Target"].unique()) == {"Dropout", "Enrolled", "Graduate"}


def test_model_artifact_and_metadata_exist():
    assert MODEL_PATH.exists(), "Trained model artifact missing"
    assert MODEL_METADATA_PATH.exists(), "Model metadata JSON missing"
    with open(MODEL_METADATA_PATH, "r") as f:
        meta = json.load(f)
    assert meta["algorithm"] == "GradientBoostingClassifier"
    assert "overall" in meta
    assert "dropout_class" in meta
    assert meta["overall"]["accuracy"] > 0.70
    assert meta["dropout_class"]["recall"] > 0.65


def test_model_loading():
    model, metadata = load_model()
    assert model is not None
    assert metadata is not None
    assert set(model.classes_) == {"Dropout", "Enrolled", "Graduate"}


def test_valid_prediction():
    df = pd.read_csv(DATASET_PATH)
    sample = df.iloc[0].drop("Target").to_dict()

    res = predict_dropout_risk(sample, student_id="STU-TEST-001")
    assert res["student_id"] == "STU-TEST-001"
    assert 0.0 <= res["dropout_risk_score"] <= 1.0
    assert res["risk_category"] in {"Low", "Moderate", "High"}
    assert isinstance(res["is_flagged"], bool)
    assert res["is_flagged"] == (res["dropout_risk_score"] > 0.6)
    assert set(res["probabilities"].keys()) == {"Dropout", "Enrolled", "Graduate"}
    assert res["model_version"] == "1.0.0"


def test_prediction_reproducibility():
    df = pd.read_csv(DATASET_PATH)
    sample = df.iloc[5].drop("Target").to_dict()

    res1 = predict_dropout_risk(sample, student_id="REPRO-1")
    res2 = predict_dropout_risk(sample, student_id="REPRO-1")
    assert res1["dropout_risk_score"] == res2["dropout_risk_score"]
    assert res1["probabilities"] == res2["probabilities"]


def test_invalid_input_handling():
    # Empty dictionary
    with pytest.raises(ValueError, match="Input validation error"):
        predict_dropout_risk({}, student_id="BAD-1")

    # String instead of dict
    with pytest.raises(ValueError, match="Input validation error"):
        predict_dropout_risk("not-a-dict", student_id="BAD-2")

    # Non-numeric garbage
    with pytest.raises(ValueError, match="Input validation error"):
        predict_dropout_risk({"Age at enrollment": "not_a_number", "GDP": "abc", "Course": "xyz"})

    # Too few features
    with pytest.raises(ValueError, match="Too few features provided"):
        predict_dropout_risk({"Age at enrollment": 20})
