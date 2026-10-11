"""
EduGuard ML Pipeline and Inference Module
Defines feature schemas, data validation, preprocessing, model inference,
and dropout-risk score generation.
"""

import datetime
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.config import (
    MODEL_METADATA_PATH,
    MODEL_PATH,
    RISK_THRESHOLD,
    get_risk_category,
    is_student_flagged,
)

logger = logging.getLogger("eduguard.ml")

# Official UCI Dataset Feature Definitions
CATEGORICAL_FEATURES = [
    "Marital Status",
    "Application mode",
    "Application order",
    "Course",
    "Daytime/evening attendance",
    "Previous qualification",
    "Nacionality",
    "Mother's qualification",
    "Father's qualification",
    "Mother's occupation",
    "Father's occupation",
    "Displaced",
    "Educational special needs",
    "Debtor",
    "Tuition fees up to date",
    "Gender",
    "Scholarship holder",
    "International",
]

CONTINUOUS_FEATURES = [
    "Previous qualification (grade)",
    "Admission grade",
    "Age at enrollment",
    "Curricular units 1st sem (credited)",
    "Curricular units 1st sem (enrolled)",
    "Curricular units 1st sem (evaluations)",
    "Curricular units 1st sem (approved)",
    "Curricular units 1st sem (grade)",
    "Curricular units 1st sem (without evaluations)",
    "Curricular units 2nd sem (credited)",
    "Curricular units 2nd sem (enrolled)",
    "Curricular units 2nd sem (evaluations)",
    "Curricular units 2nd sem (approved)",
    "Curricular units 2nd sem (grade)",
    "Curricular units 2nd sem (without evaluations)",
    "Unemployment rate",
    "Inflation rate",
    "GDP",
]

ALL_FEATURES = CATEGORICAL_FEATURES + CONTINUOUS_FEATURES
TARGET_COLUMN = "Target"
TARGET_CLASSES = ["Dropout", "Enrolled", "Graduate"]

# Global cache for loaded model and metadata
_LOADED_MODEL = None
_LOADED_METADATA = None


def build_preprocessor() -> ColumnTransformer:
    """
    Constructs a ColumnTransformer that imputes and standardizes continuous features,
    and imputes and one-hot encodes categorical features.
    """
    continuous_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("cont", continuous_transformer, CONTINUOUS_FEATURES),
            ("cat", categorical_transformer, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
    )
    return preprocessor


def load_model(force_reload: bool = False):
    """
    Loads the serialized model artifact and metadata from disk.
    Caches the loaded pipeline in memory for fast inference.
    """
    global _LOADED_MODEL, _LOADED_METADATA
    if _LOADED_MODEL is not None and not force_reload:
        return _LOADED_MODEL, _LOADED_METADATA

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model artifact not found at {MODEL_PATH}. Run training first."
        )

    _LOADED_MODEL = joblib.load(MODEL_PATH)

    if MODEL_METADATA_PATH.exists():
        with open(MODEL_METADATA_PATH, "r") as f:
            _LOADED_METADATA = json.load(f)
    else:
        _LOADED_METADATA = {"version": "1.0.0", "algorithm": "GradientBoostingClassifier"}

    return _LOADED_MODEL, _LOADED_METADATA


def validate_student_features(features: Dict[str, Any]) -> Tuple[bool, Optional[str], pd.DataFrame]:
    """
    Validates that the provided dictionary contains valid student features.
    Converts features into a single-row DataFrame aligned with ALL_FEATURES.
    Missing features are imputed during pipeline execution, but unknown structures are rejected.
    """
    if not isinstance(features, dict):
        return False, "Features must be a dictionary", None

    if not features:
        return False, "Features dictionary cannot be empty", None

    # Map possible alternative key names (e.g., lowercased, underscores) to exact column names
    key_mapping = {col.lower().replace(" ", "_"): col for col in ALL_FEATURES}
    # Also direct case-insensitive map
    key_mapping.update({col.lower(): col for col in ALL_FEATURES})
    key_mapping.update({col: col for col in ALL_FEATURES})

    normalized = {}
    for k, v in features.items():
        if k in key_mapping:
            matched_col = key_mapping[k]
            try:
                if v is not None and v != "":
                    normalized[matched_col] = float(v)
                else:
                    normalized[matched_col] = np.nan
            except (ValueError, TypeError):
                return False, f"Invalid value for feature '{k}': {v} (expected numerical)", None

    # Check minimum feature presence: At least 3 genuine features must be provided
    provided_count = sum(1 for c in ALL_FEATURES if c in normalized and not pd.isna(normalized[c]))
    if provided_count < 3:
        return False, f"Too few features provided ({provided_count}). Provide at least 3 academic/demographic features.", None

    # Create 1-row DataFrame with all required columns in exact order
    row_data = {col: normalized.get(col, np.nan) for col in ALL_FEATURES}
    df = pd.DataFrame([row_data], columns=ALL_FEATURES)
    return True, None, df


def predict_dropout_risk(
    student_features: Dict[str, Any],
    student_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Main reusable prediction function.
    
    Validates inputs, passes through the trained preprocessing and classification model,
    and returns a structured prediction response with:
    - dropout-risk score between 0 and 1
    - risk category (Low, Moderate, High)
    - advisor follow-up flag (STRICTLY > 0.6)
    - class probability breakdown
    - model version & interpretation metadata
    """
    valid, err_msg, df = validate_student_features(student_features)
    if not valid:
        raise ValueError(f"Input validation error: {err_msg}")

    model, metadata = load_model()

    # Model inference
    classes = list(model.classes_)
    if "Dropout" not in classes:
        raise RuntimeError("Model does not contain the 'Dropout' target class.")

    dropout_index = classes.index("Dropout")

    # Get class probabilities
    probabilities = model.predict_proba(df)[0]
    raw_dropout_prob = float(probabilities[dropout_index])

    # Round score to 4 decimal places for precision consistency
    risk_score = round(raw_dropout_prob, 4)

    # Apply strict threshold logic: score > 0.6
    flagged = is_student_flagged(risk_score)
    category = get_risk_category(risk_score)

    prob_dict = {
        cls_name: round(float(prob), 4)
        for cls_name, prob in zip(classes, probabilities)
    }

    predicted_class = str(model.predict(df)[0])

    response = {
        "student_id": student_id or student_features.get("student_id") or "unknown",
        "dropout_risk_score": risk_score,
        "is_flagged": flagged,
        "risk_category": category,
        "predicted_outcome": predicted_class,
        "probabilities": prob_dict,
        "threshold": RISK_THRESHOLD,
        "flagging_rule": f"Flagged if dropout_risk_score > {RISK_THRESHOLD}",
        "interpretation": (
            "Model-estimated probability of the student dropping out (Dropout class) "
            "based on academic performance, enrollment parameters, and demographic attributes. "
            "This is an indicative risk score to support early advisor intervention."
        ),
        "model_version": metadata.get("version", "1.0.0"),
        "model_type": metadata.get("algorithm", "GradientBoostingClassifier"),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    return response
