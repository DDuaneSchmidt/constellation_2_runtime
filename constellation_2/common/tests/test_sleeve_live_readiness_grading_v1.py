from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_local_grader() -> object:
    repo_root = Path(__file__).resolve().parents[3]
    module_path = repo_root / "ops" / "tools" / "run_sleeve_live_readiness_v1.py"
    spec = importlib.util.spec_from_file_location("local_sleeve_live_readiness_v1", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module._grade_1_to_7_for_score


_grade_1_to_7_for_score = _load_local_grader()


def test_grade_1_to_7_boundaries_are_deterministic() -> None:
    assert _grade_1_to_7_for_score(-1) == 1
    assert _grade_1_to_7_for_score(0) == 1
    assert _grade_1_to_7_for_score(29) == 1
    assert _grade_1_to_7_for_score(30) == 2
    assert _grade_1_to_7_for_score(50) == 3
    assert _grade_1_to_7_for_score(65) == 4
    assert _grade_1_to_7_for_score(75) == 5
    assert _grade_1_to_7_for_score(85) == 6
    assert _grade_1_to_7_for_score(95) == 7
    assert _grade_1_to_7_for_score(100) == 7
    assert _grade_1_to_7_for_score(200) == 7


def test_grade_1_to_7_matches_live_threshold_expectation() -> None:
    assert _grade_1_to_7_for_score(84) == 5
    assert _grade_1_to_7_for_score(85) == 6
