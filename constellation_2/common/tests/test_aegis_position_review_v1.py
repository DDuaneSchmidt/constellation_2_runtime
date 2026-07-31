from __future__ import annotations

from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.position_review_v1 import (
    _brief_for_context,
    _brief_lacks_linked_evidence,
    _internal_label_hits,
    build_position_review_brief_v1,
    build_position_review_context_v1,
    build_position_review_score_v1,
    self_check_position_review_v1,
    write_position_review_brief_v1,
    write_position_review_context_v1,
    write_position_review_score_v1,
)

TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-05-29"


def test_position_review_context_is_deterministic_for_same_inputs() -> None:
    first = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    second = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    assert first["summary"]["context_count"] == 36
    assert [row["input_context_hash"] for row in first["contexts"]] == [row["input_context_hash"] for row in second["contexts"]]


def test_position_review_context_links_thesis_validation_and_regime_evidence() -> None:
    context = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    assert context["summary"]["evidence_count"] > context["summary"]["context_count"] * 10
    assert context["summary"]["sleeve_evidence_count"] >= context["summary"]["context_count"]
    assert context["summary"]["signal_evidence_count"] >= context["summary"]["context_count"]
    assert context["summary"]["validation_evidence_count"] >= context["summary"]["context_count"]
    assert context["summary"]["market_regime_evidence_count"] >= context["summary"]["context_count"]
    for row in context["contexts"]:
        counts = row["evidence_counts"]
        assert counts["signal_evidence_count"] > 0
        assert counts["validation_evidence_count"] > 0
        assert counts["market_regime_evidence_count"] > 0
        evidence = row["included_evidence"]
        assert "signal_evidence" in evidence
        assert "validation_evidence" in evidence
        assert "market_regime_evidence" in evidence
        assert row["position_source_artifact_hashes"]



def test_position_review_score_is_deterministic_and_complete() -> None:
    context = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=context)
    first = build_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    second = build_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    assert first["summary"]["score_count"] == context["summary"]["context_count"]
    assert [row["position_health"] for row in first["scores"]] == [row["position_health"] for row in second["scores"]]
    for row in first["scores"]:
        assert row["position_health"] in {"IMPROVING", "STABLE", "DETERIORATING"}
        assert row["thesis_status"] in {"THESIS_STRENGTHENED", "THESIS_UNCHANGED", "THESIS_WEAKENED"}
        assert row["confidence"] in {"LOW", "MEDIUM", "HIGH"}
        assert row["key_insight_inputs"]
        assert row["most_important_supporting_evidence"]
        assert row["most_important_risk"]
        assert row["most_important_confirmation"]
        assert all(item["priority"] in {"HIGH", "MEDIUM", "LOW"} for item in row["evidence_rankings"])

def test_position_review_brief_references_context_hash_and_has_no_trade_advice_language() -> None:
    context = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=context)
    score = build_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=score)
    brief = build_position_review_brief_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    by_context = {row["context_id"]: row for row in context["contexts"]}
    by_score = {row["context_id"]: row for row in score["scores"]}
    assert brief["summary"]["brief_count"] == context["summary"]["context_count"]
    assert brief["trade_advice_allowed"] is False
    assert brief["broker_execution_allowed"] is False
    assert brief["live_trading_allowed"] is False
    assert brief["autonomous_live_trading_allowed"] is False
    for row in brief["briefs"]:
        assert row["input_context_hash"] == by_context[row["context_id"]]["input_context_hash"]
        assert row["score_id"] == by_score[row["context_id"]]["score_id"]
        assert row["position_health"] in {"IMPROVING", "STABLE", "DETERIORATING"}
        assert row["thesis_status"] in {"THESIS_STRENGTHENED", "THESIS_UNCHANGED", "THESIS_WEAKENED"}
        assert row["key_insight"]
        assert row["ranked_evidence"]
        assert row["most_important_risk"]
        assert row["most_important_confirmation"]
        text = " ".join(str(row.get(key, "")) for key in ["conclusion", "entry_thesis", "thesis_summary", "what_changed_since_entry"]).lower()
        assert "exit now" not in text
        assert "increase size" not in text
        assert "reduce size" not in text
        assert row["entry_thesis"]
        assert row["supporting_evidence"]
        assert row["contradicting_evidence"]
        assert row["validation_evidence"]
        assert row["market_regime_context"]
        assert row["monitoring_points"]
        assert row["data_quality"]
        assert _brief_lacks_linked_evidence(row) is False
        assert _internal_label_hits(row) == []


def test_position_review_translates_internal_labels_to_operator_language() -> None:
    context = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=context)
    score = build_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=score)
    brief = build_position_review_brief_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    qqq = next(row for row in brief["briefs"] if row["symbol"] == "QQQ" and row["validation_evidence"])
    visible = " ".join(str(qqq.get(key, "")) for key in ["entry_thesis", "supporting_evidence", "contradicting_evidence", "validation_evidence", "market_regime_context", "risks", "monitoring_points"])
    assert "PROMOTION_BLOCKED" not in visible
    assert "REVIEW_ONLY" not in visible
    assert "EVIDENCE_NOT_DEMANDED" not in visible
    assert "Supporting evidence confirmed" in visible or "Supporting signal evidence exists" in visible or "Required condition remains satisfied" in visible
    assert "Monitoring signal only" not in visible or "no action implied" in visible
    assert "Additional evidence exists but was not required" in visible or "Market-regime evidence was available" in visible
    assert _internal_label_hits(qqq) == []


def test_position_review_self_check_rejects_price_only_brief() -> None:
    thin = {
        "brief_id": "thin",
        "conclusion": "The context shows entry 1, current mark 2, and paper P&L 3.",
        "key_insight": "",
        "position_health": "",
        "thesis_status": "",
        "entry_thesis": "",
        "supporting_evidence": ["Current mark is available"],
        "contradicting_evidence": [],
        "validation_evidence": [],
        "market_regime_context": [],
        "most_important_risk": "",
        "most_important_confirmation": "",
        "monitoring_points": ["Compare entry with current mark"],
        "data_quality": {},
    }
    assert _brief_lacks_linked_evidence(thin) is True


def test_position_review_self_check_passes_for_generated_artifacts() -> None:
    context = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=context)
    score = build_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_score_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=score)
    brief = build_position_review_brief_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    write_position_review_brief_v1(truth_root=TRUTH_ROOT, day_utc=DAY, payload=brief)
    result = self_check_position_review_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    assert result["ok"] is True
    assert result["position_context_count"] == 36
    assert result["position_brief_count"] == 36
    assert result["forbidden_language_violation_count"] == 0


def test_position_with_missing_data_produces_partial_context() -> None:
    context = build_position_review_context_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    synthetic = dict(context["contexts"][0])
    synthetic["data_quality_status"] = "PARTIAL"
    synthetic["null_reasons"] = {"paper_pnl.unrealized_pnl": "MISSING_FROM_DETERMINISTIC_CONTEXT"}
    brief = _brief_for_context(synthetic, generated_at="2026-05-30T00:00:00Z")
    assert brief["status"] == "PARTIAL"
    assert brief["null_reasons"]
