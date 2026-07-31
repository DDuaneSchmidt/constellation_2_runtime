from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.session_context import SESSION_CONTEXTS
from constellation_2.common.atlas_v2_research_os.session_context_expansion import (
    build_session_context_expansion_report,
    write_session_context_expansion_report,
)
from constellation_2.common.tests.test_atlas_v2_research_os_candidate_family_discovery import _write_sources

NOW = "2026-06-05T00:00:00Z"


def test_session_context_expansion_reports_required_metrics(tmp_path: Path) -> None:
    _write_sources(tmp_path)

    report = build_session_context_expansion_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=8)
    metrics = report["metrics"]

    assert report["session_contexts"] == list(SESSION_CONTEXTS)
    assert set(metrics["observations_by_session"]) == set(SESSION_CONTEXTS)
    assert set(metrics["claims_by_session"]) == set(SESSION_CONTEXTS)
    assert set(metrics["hypotheses_by_session"]) == set(SESSION_CONTEXTS)
    assert set(metrics["eligible_candidates_by_session"]) == set(SESSION_CONTEXTS)
    assert set(metrics["family_count_by_session"]) == set(SESSION_CONTEXTS)
    assert sum(metrics["observations_by_session"].values()) == 1000
    assert sum(metrics["hypotheses_by_session"].values()) == 8
    assert metrics["best_session_contexts"]
    assert metrics["worst_session_contexts"]
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_session_context_expansion_writes_requested_outputs(tmp_path: Path) -> None:
    _write_sources(tmp_path)

    paths = write_session_context_expansion_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=3)

    assert paths["json"].name == "session_context_expansion_report.json"
    assert paths["summary"].name == "session_context_expansion_summary.md"
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_session_context_expansion_report_v1"
    assert "Session Context Expansion" in paths["latest_summary"].read_text(encoding="utf-8")
