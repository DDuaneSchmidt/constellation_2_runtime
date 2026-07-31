from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_poc_review_v1 import build_alpha_factory_poc_review_v1, write_alpha_factory_poc_review_v1
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1


DAY = "2026-06-03"


def test_hostile_review_records_required_verdicts(tmp_path: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY, payload=poc)

    review = build_alpha_factory_poc_review_v1(truth_root=tmp_path, day_utc=DAY)

    assert review["verdicts"] == {
        "poc_execution": "POC_EXECUTION_VALID",
        "discovery_claim": "DISCOVERY_CLAIM_INVALID",
        "lineage": "LINEAGE_COMPLETE",
        "evidence": "EVIDENCE_INSUFFICIENT",
        "benchmark": "BENCHMARK_REQUIRED",
    }
    assert review["minimum_next_benchmark"]["benchmark"] == "naive-correlation baseline"


def test_hostile_review_captures_seeded_candidate_and_limitations(tmp_path: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY, payload=poc)

    review = build_alpha_factory_poc_review_v1(truth_root=tmp_path, day_utc=DAY)

    assert review["candidate_review"]["discovered_vs_seeded_assessment"] == "SEEDED_OR_IMPLICITLY_HARDCODED"
    assert review["candidate_review"]["support_assessment"]["real_yield_support_sample_count"] == 0
    assert review["evidence_review"]["limitation_capture_present"] is True
    assert review["evidence_review"]["directional_zero_sample_ids"]
    assert any(finding["verdict_impact"] == "EVIDENCE_INSUFFICIENT" for finding in review["findings"])


def test_hostile_review_output_is_reproducible_and_writable(tmp_path: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY, payload=poc)

    first = build_alpha_factory_poc_review_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_poc_review_v1(truth_root=tmp_path, day_utc=DAY)
    assert first["content_hash"] == second["content_hash"]

    paths = write_alpha_factory_poc_review_v1(truth_root=tmp_path, day_utc=DAY, payload=first)
    assert Path(paths["json"]).exists()
