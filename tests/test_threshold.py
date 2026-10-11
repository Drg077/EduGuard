"""
Unit tests for EduGuard Risk Threshold Logic and Flagging Rules.
Mandatory rule:
IF dropout_risk_score > 0.6: Flag
ELSE: Do not flag
Strictly greater than 0.6.
"""

import pytest
from src.config import RISK_THRESHOLD, get_risk_category, is_student_flagged


def test_threshold_constant():
    assert RISK_THRESHOLD == 0.6, "Risk threshold must be strictly configured as 0.6"


@pytest.mark.parametrize(
    "score,expected_flag,expected_category",
    [
        (0.00, False, "Low"),
        (0.15, False, "Low"),
        (0.30, False, "Low"),
        (0.35, False, "Low"),
        (0.36, False, "Moderate"),
        (0.50, False, "Moderate"),
        (0.59, False, "Moderate"),
        (0.60, False, "Moderate"),  # Exact boundary: 0.60 must NOT trigger flag
        (0.600000, False, "Moderate"),
        (0.600001, True, "High"),   # Any value strictly > 0.6 triggers flag
        (0.61, True, "High"),       # 0.61 must trigger flag
        (0.70, True, "High"),
        (0.80, True, "High"),       # 0.80 must trigger flag
        (0.95, True, "High"),
        (1.00, True, "High"),
    ],
)
def test_threshold_flagging_boundaries(score, expected_flag, expected_category):
    assert is_student_flagged(score) == expected_flag, (
        f"Score {score} flagged status was {is_student_flagged(score)}, expected {expected_flag}"
    )
    assert get_risk_category(score) == expected_category, (
        f"Score {score} category was {get_risk_category(score)}, expected {expected_category}"
    )


def test_none_score_handling():
    assert is_student_flagged(None) is False
    assert get_risk_category(None) == "Unknown"
