from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.narrative_operational_analytics_v1 import (
    build_narrative_operational_analytics_v1,
    write_narrative_operational_analytics_v1,
)

DAY = "2026-05-22"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _paper_projection(root: Path) -> dict:
    path = root / "reports" / "paper_trade_evaluation_projection_v1" / DAY / "paper_trade_evaluation_projection.v1.json"
    payload = {
        "schema_id": "paper_trade_evaluation_projection",
        "schema_version": "v1",
        "day_utc": DAY,
        "content_hash": "paper-hash",
        "realized_pnl": 0.0,
        "unrealized_pnl": -8.3,
        "total_pnl": -8.3,
        "open_trade_count": 1,
        "closed_trade_count": 0,
        "trade_count": 1,
        "open_trades": [
            {
                "trade_id": "ticket:dow",
                "symbol": "DOW",
                "entry_price": 36.06,
                "current_mark": 36.01,
                "quantity": 166,
                "unrealized_pnl": -8.3,
                "return_pct": -0.14,
                "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
            }
        ],
        "closed_trades": [],
        "sleeve_attribution": [{"id": "C2_MEAN_REVERSION_EQ_V1", "trade_count": 1, "realized_pnl": 0.0, "unrealized_pnl": -8.3, "total_pnl": -8.3}],
    }
    _write_json(path, payload)
    return payload


def _candidate_funnel(root: Path) -> dict:
    audit_path = root / "reports" / "candidate_consumption_audit_v1" / DAY / "candidate_consumption_audit.v1.json"
    queue_path = root / "reports" / "dynamic_certification_queue_v1" / DAY / "dynamic_certification_queue.v1.json"
    universe_path = root / "reports" / "dynamic_certified_universe_v1" / DAY / "dynamic_certified_universe.v1.json"
    _write_json(audit_path, {"schema_id": "candidate_consumption_audit", "content_hash": "audit-hash"})
    _write_json(
        queue_path,
        {
            "schema_id": "dynamic_certification_queue",
            "content_hash": "queue-hash",
            "requested_symbols": ["CRWD"],
            "requested_symbol_count": 1,
            "certification_status": "CERTIFIED",
            "certification_results": [{"symbol": "CRWD", "certification_status": "CERTIFIED", "provider": "TIINGO"}],
        },
    )
    _write_json(universe_path, {"schema_id": "dynamic_certified_universe", "content_hash": "universe-hash", "dynamic_certified_symbols": ["CRWD"]})
    _write_json(root / "reports" / "dynamic_certified_universe_v1" / "2026-05-21" / "dynamic_certified_universe.v1.json", {"dynamic_certified_symbols": []})
    return {
        "raw_candidate_count": 319,
        "promoted_candidate_count": 0,
        "excluded_uncovered_symbol_count": 299,
        "excluded_low_score_count": 18,
        "excluded_policy_count": 2,
        "artifact_paths": {"candidate_consumption_audit_v1": str(audit_path)},
        "trend_5d": [
            {"trading_day": "2026-05-21", "raw_candidate_count": 319, "promoted_candidate_count": 0, "excluded_uncovered_symbol_count": 302},
            {"trading_day": DAY, "raw_candidate_count": 319, "promoted_candidate_count": 0, "excluded_uncovered_symbol_count": 299},
        ],
        "dynamic_certification_queue": {
            "artifact_path": str(queue_path),
            "artifact_content_hash": "queue-hash",
            "requested_symbols": ["CRWD"],
            "certification_results": [{"symbol": "CRWD", "certification_status": "CERTIFIED", "provider": "TIINGO"}],
        },
    }


def test_narrative_statements_require_artifact_evidence(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    payload = build_narrative_operational_analytics_v1(
        truth_root=root,
        day_utc=DAY,
        paper_trade_evaluation_projection=_paper_projection(root),
        candidate_funnel_projection=_candidate_funnel(root),
        generated_at_utc="2026-05-22T22:00:00Z",
    )
    assert payload["narrative_statements"]
    for row in payload["narrative_statements"]:
        assert row["statement"]
        assert row["supporting_metric"]
        assert row["supporting_artifact_path"]
        assert Path(row["supporting_artifact_path"]).exists()
        assert row["confidence"] in {"HIGH", "MEDIUM", "LOW"}
        assert row["evidence_status"] in {"COMPLETE", "PARTIAL", "INSUFFICIENT_DATA"}


def test_insufficient_data_produces_no_unsupported_claims(tmp_path: Path) -> None:
    payload = build_narrative_operational_analytics_v1(truth_root=tmp_path / "truth", day_utc=DAY, generated_at_utc="2026-05-22T22:00:00Z")
    assert payload["narrative_statements"] == []
    assert all(chart["status"] == "INSUFFICIENT_HISTORY" for charts in payload["charts"].values() for chart in charts)


def test_crwd_narrative_links_to_dynamic_certification_artifact(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    payload = build_narrative_operational_analytics_v1(
        truth_root=root,
        day_utc=DAY,
        paper_trade_evaluation_projection=_paper_projection(root),
        candidate_funnel_projection=_candidate_funnel(root),
        generated_at_utc="2026-05-22T22:00:00Z",
    )
    crwd = [row for row in payload["narrative_statements"] if "CRWD" in row["statement"]]
    assert crwd
    assert crwd[0]["category"] == "CERTIFICATION"
    assert "dynamic_certification_queue_v1" in crwd[0]["supporting_artifact_path"]
    assert "reducing uncovered exclusions by 3" in crwd[0]["statement"]


def test_dow_pnl_narrative_links_to_trade_evaluation_artifact(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    payload = build_narrative_operational_analytics_v1(
        truth_root=root,
        day_utc=DAY,
        paper_trade_evaluation_projection=_paper_projection(root),
        candidate_funnel_projection=_candidate_funnel(root),
        generated_at_utc="2026-05-22T22:00:00Z",
    )
    dow = [row for row in payload["narrative_statements"] if row["statement"].startswith("DOW unrealized P&L")]
    assert dow
    assert "current mark is below entry" in dow[0]["statement"]
    assert "paper_trade_evaluation_projection_v1" in dow[0]["supporting_artifact_path"]


def test_candidate_funnel_narrative_matches_counts(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    payload = build_narrative_operational_analytics_v1(
        truth_root=root,
        day_utc=DAY,
        paper_trade_evaluation_projection=_paper_projection(root),
        candidate_funnel_projection=_candidate_funnel(root),
        generated_at_utc="2026-05-22T22:00:00Z",
    )
    statement = next(row for row in payload["narrative_statements"] if row["category"] == "CANDIDATE_FUNNEL")
    assert "299 candidates are outside certified coverage" in statement["statement"]
    assert "18 failed score" in statement["statement"]
    assert "2 failed policy" in statement["statement"]


def test_narrative_artifact_writes_content_addressed_report(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    payload = build_narrative_operational_analytics_v1(
        truth_root=root,
        day_utc=DAY,
        paper_trade_evaluation_projection=_paper_projection(root),
        candidate_funnel_projection=_candidate_funnel(root),
        generated_at_utc="2026-05-22T22:00:00Z",
    )
    paths = write_narrative_operational_analytics_v1(truth_root=root, payload=payload)
    written = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
    assert written["schema_id"] == "narrative_operational_analytics"
    assert written["content_hash"] == payload["content_hash"]
    assert written["broker_submit_transmit_allowed"] is False
    assert written["autonomous_execution_allowed"] is False
