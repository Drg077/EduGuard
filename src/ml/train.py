"""
EduGuard ML Model Training Module
Trains a GradientBoostingClassifier pipeline on the official UCI dataset,
evaluates on held-out test data, and persists pipeline and metadata.
"""

import datetime
import json
import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from src.config import (
    DATASET_PATH,
    MODEL_METADATA_PATH,
    MODEL_PATH,
    MODELS_DIR,
)
from src.ml.pipeline import (
    ALL_FEATURES,
    TARGET_CLASSES,
    TARGET_COLUMN,
    build_preprocessor,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("eduguard.train")


def train_model(
    dataset_path: Path = DATASET_PATH,
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 150,
    max_depth: int = 4,
    learning_rate: float = 0.1,
) -> dict:
    """
    Executes the training workflow:
    1. Loads dataset
    2. Validates features and target
    3. Splits data (stratified)
    4. Builds preprocessor and classifier pipeline
    5. Fits pipeline only on X_train, y_train
    6. Evaluates on X_test, y_test
    7. Persists artifacts
    """
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found at {dataset_path}")

    logger.info("Loading dataset from %s", dataset_path)
    df = pd.read_csv(dataset_path)

    # Check required columns
    missing_cols = [c for c in ALL_FEATURES if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Dataset is missing required feature columns: {missing_cols}")
    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"Dataset is missing target column: {TARGET_COLUMN}")

    X = df[ALL_FEATURES]
    y = df[TARGET_COLUMN]

    logger.info("Dataset shape: %d records, %d features", len(df), len(ALL_FEATURES))
    target_counts = y.value_counts().to_dict()
    logger.info("Target distribution: %s", target_counts)

    # Stratified split to maintain class balance
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    logger.info("Train set: %d records, Test set: %d records", len(X_train), len(X_test))

    # Construct the full end-to-end pipeline
    preprocessor = build_preprocessor()
    classifier = GradientBoostingClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        random_state=random_state,
    )

    full_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )

    logger.info("Fitting preprocessing and classifier pipeline on training data...")
    full_pipeline.fit(X_train, y_train)

    logger.info("Evaluating model on held-out test data...")
    y_pred = full_pipeline.predict(X_test)
    y_proba = full_pipeline.predict_proba(X_test)
    classes = list(full_pipeline.classes_)

    # Overall metrics
    acc = float(accuracy_score(y_test, y_pred))
    macro_prec = float(precision_score(y_test, y_pred, average="macro"))
    macro_rec = float(recall_score(y_test, y_pred, average="macro"))
    macro_f1 = float(f1_score(y_test, y_pred, average="macro"))

    weighted_prec = float(precision_score(y_test, y_pred, average="weighted"))
    weighted_rec = float(recall_score(y_test, y_pred, average="weighted"))
    weighted_f1 = float(f1_score(y_test, y_pred, average="weighted"))

    # Per-class metrics
    clf_report = classification_report(y_test, y_pred, output_dict=True)
    conf_matrix = confusion_matrix(y_test, y_pred, labels=classes).tolist()

    # Dropout-specific performance (critical educational safety metric)
    dropout_idx = classes.index("Dropout")
    dropout_prec = float(clf_report["Dropout"]["precision"])
    dropout_rec = float(clf_report["Dropout"]["recall"])
    dropout_f1 = float(clf_report["Dropout"]["f1-score"])
    dropout_support = int(clf_report["Dropout"]["support"])

    # Calibration evaluation for Dropout class
    y_test_binary = (y_test == "Dropout").astype(int)
    dropout_probs = y_proba[:, dropout_idx]
    brier_score = float(brier_score_loss(y_test_binary, dropout_probs))

    # Feature importances from GradientBoosting
    gb_model = full_pipeline.named_steps["classifier"]
    encoded_feature_names = full_pipeline.named_steps["preprocessor"].get_feature_names_out()
    feature_importances = gb_model.feature_importances_
    top_indices = np.argsort(feature_importances)[::-1][:15]
    top_features = [
        {"feature": str(encoded_feature_names[i]), "importance": round(float(feature_importances[i]), 4)}
        for i in top_indices
    ]

    metrics = {
        "dataset_name": "Predict Students' Dropout and Academic Success (UCI ID 697)",
        "dataset_records_total": len(df),
        "train_records": len(X_train),
        "test_records": len(X_test),
        "test_split_ratio": test_size,
        "random_seed": random_state,
        "algorithm": "GradientBoostingClassifier",
        "version": "1.0.0",
        "training_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "classes": classes,
        "overall": {
            "accuracy": round(acc, 4),
            "macro_precision": round(macro_prec, 4),
            "macro_recall": round(macro_rec, 4),
            "macro_f1": round(macro_f1, 4),
            "weighted_precision": round(weighted_prec, 4),
            "weighted_recall": round(weighted_rec, 4),
            "weighted_f1": round(weighted_f1, 4),
            "brier_score_loss": round(brier_score, 4),
        },
        "dropout_class": {
            "precision": round(dropout_prec, 4),
            "recall": round(dropout_rec, 4),
            "f1": round(dropout_f1, 4),
            "support": dropout_support,
        },
        "confusion_matrix": {
            "labels": classes,
            "matrix": conf_matrix,
        },
        "classification_report": {
            k: {
                metric: round(val, 4) if isinstance(val, float) else val
                for metric, val in v.items()
            }
            for k, v in clf_report.items()
            if isinstance(v, dict)
        },
        "top_features": top_features,
        "hyperparameters": {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
        },
    }

    # Save trained pipeline and metadata
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Saving trained pipeline to %s", MODEL_PATH)
    joblib.dump(full_pipeline, MODEL_PATH)

    logger.info("Saving model metadata to %s", MODEL_METADATA_PATH)
    with open(MODEL_METADATA_PATH, "w") as f:
        json.dump(metrics, f, indent=2)

    logger.info("Training complete. Accuracy: %.4f, Dropout Recall: %.4f, Dropout Precision: %.4f",
                acc, dropout_rec, dropout_prec)
    return metrics


if __name__ == "__main__":
    metrics = train_model()
    print("\n--- MODEL TRAINING REPORT ---")
    print(json.dumps(metrics["overall"], indent=2))
    print("\n--- DROPOUT CLASS METRICS ---")
    print(json.dumps(metrics["dropout_class"], indent=2))
