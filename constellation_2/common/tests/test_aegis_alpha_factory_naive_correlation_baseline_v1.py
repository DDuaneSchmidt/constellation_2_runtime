from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import (
    build_alpha_factory_naive_correlation_baseline_v1,
    write_alpha_factory_naive_correlation_baseline_v1,
)
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1


DAY = "2026-06-03"


def _write_poc(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)


def test_naive_baseline_artifact_generation(tmp_path: Path) -> None:
    _write_poc(tmp_path)
    payload = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_naive_correlation_baseline"
    assert payload["top_relationships"]
    assert payload["top_candidate_relationship_clusters"]
    assert payload["summary"]["relationship_count"] > 0
    paths = write_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_naive_baseline_output_is_deterministic(tmp_path: Path) -> None:
    _write_poc(tmp_path)
    first = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_naive_baseline_required_verdict_fields(tmp_path: Path) -> None:
    _write_poc(tmp_path)
    payload = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["baseline_execution"] == "BASELINE_EXECUTION_VALID"
    assert payload["verdicts"]["poc_vs_baseline"] in {
        "POC_OUTPERFORMS_BASELINE",
        "BASELINE_MATCHES_OR_EXCEEDS_POC",
        "INCONCLUSIVE",
    }
    assert payload["verdicts"]["discovery_advantage"] in {
        "DISCOVERY_ADVANTAGE_PRESENT",
        "DISCOVERY_ADVANTAGE_ABSENT",
        "DISCOVERY_ADVANTAGE_INCONCLUSIVE",
    }
    assert payload["verdicts"]["minimum_next_action"]


def test_naive_baseline_hostile_checks_cover_poc_candidate(tmp_path: Path) -> None:
    _write_poc(tmp_path)
    payload = build_alpha_factory_naive_correlation_baseline_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["poc_candidate_mostly_recoverable_by_naive_correlation"] is True
    assert checks["poc_naming_exceeds_evidence"] is True
    assert checks["real_yield_support_zero_sample_or_insufficient"] is True
    assert checks["gld_to_slv_evidence_alone_explains_candidate"] is True
