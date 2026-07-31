from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import json

from constellation_2.common.atlas_v2_research_os.mechanism_hypothesis_generator import (
    generate_mechanism_hypotheses,
    generate_mechanism_search_1000,
    generate_mechanism_search_small,
)
from constellation_2.common.atlas_v2_research_os.mechanism_search_governance import MechanismSearchGovernanceError
from constellation_2.common.atlas_v2_research_os.mechanism_search_models import MECHANISM_FAMILIES
from constellation_2.common.atlas_v2_research_os.mechanism_search_reports import audit_mechanism_search_report, write_mechanism_search_report


def test_small_mechanism_search_emits_required_fields_and_pipeline(tmp_path: Path) -> None:
    run = generate_mechanism_search_small(tmp_path, created_at="2026-06-05T00:00:00Z")
    assert run["governance_result"]["status"] == "PASS"
    assert run["emitted_count"] == 10
    assert len(run["pipeline_results"]) == 10
    first = run["hypotheses"][0]
    for field in [
        "mechanism",
        "conditions",
        "regime",
        "timeframe",
        "entry_observation_rule",
        "exit_observation_rule",
        "invalidation_rule",
        "complexity_score",
        "source_search_config",
    ]:
        assert field in first
    gate = run["pipeline_results"][0]["paper_trade_candidate_gate"]
    assert gate["human_review_required"] is True


def test_mechanism_search_covers_all_requested_families(tmp_path: Path) -> None:
    run = generate_mechanism_hypotheses(limit=120, root=tmp_path, created_at="2026-06-05T00:00:00Z", replay_limit=0)
    emitted = {row["mechanism"] for row in run["hypotheses"]}
    assert set(MECHANISM_FAMILIES).issubset(emitted)


def test_mechanism_search_1000_is_bounded_and_exact(tmp_path: Path) -> None:
    run = generate_mechanism_search_1000(tmp_path, created_at="2026-06-05T00:00:00Z")
    assert run["emitted_count"] == 1000
    assert len(run["hypotheses"]) == 1000
    assert len(run["pipeline_results"]) == 25
    assert run["governance_result"]["max_hypotheses"] == 1000


def test_mechanism_search_rejects_unbounded_request(tmp_path: Path) -> None:
    try:
        generate_mechanism_hypotheses(limit=1001, root=tmp_path, created_at="2026-06-05T00:00:00Z")
    except MechanismSearchGovernanceError as exc:
        assert "exceeds 1000" in str(exc)
    else:
        raise AssertionError("expected bounded governance failure")


def test_write_mechanism_search_report_outputs_latest_files(tmp_path: Path) -> None:
    run = generate_mechanism_search_small(tmp_path / "store", created_at="2026-06-05T00:00:00Z")
    paths = write_mechanism_search_report(tmp_path / "store", report_root=tmp_path / "mechanism_search", run=run, day="2026-06-05")
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["emitted_count"] == 10
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert audit_mechanism_search_report(tmp_path / "mechanism_search")["mechanism_search_audit_ok"] is True
