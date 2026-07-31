from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.signal_evidence_boundary_v1 import (
    SIGNAL_EVIDENCE_PRESENT,
    SIGNAL_EVIDENCE_REJECTED_INTENT,
    build_signal_evidence_boundary_v1,
    render_signal_evidence_boundary_v1,
)

DAY = "2026-05-28"
SESSION = "PAPER-2026-05-28-0950"
NOW = "2026-05-28T14:00:00Z"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _candidate(symbol: str, idx: int) -> dict:
    return {
        "symbol": symbol,
        "candidate_id": f"candidate-{idx}",
        "candidate_contract_id": f"candidate-{idx}",
        "raw_signal_id": f"raw-{symbol.lower()}",
        "paper_session_id": SESSION,
    }


def _seed_base(root: Path) -> None:
    candidates = [_candidate("QQQ", 1), _candidate("SPY", 2), _candidate("AAL", 3), _candidate("AMD", 4)]
    _write(
        root / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {"day_utc": DAY, "paper_session_id": SESSION, "current_session_candidates": candidates},
    )
    _write(
        root / "reports" / "aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json",
        {"signals": [{"symbol": "QQQ", "raw_signal_id": "raw-qqq"}, {"symbol": "SPY", "raw_signal_id": "raw-spy"}]},
    )
    _write(
        root / "reports" / "sleeve_evaluation_kernel_v1" / DAY / "C2_TREND_EQ_PRIMARY_V1" / "sleeve_evaluation.v1.json",
        {
            "exposure_intent_batch": {
                "output_intents": [
                    {"symbol": "QQQ", "raw_signal_id": "raw-qqq"},
                    {"symbol": "SPY", "raw_signal_id": "raw-spy"},
                ]
            },
            "rejected_intents": [
                {"symbol": "AAL", "raw_signal_id": "raw-aal"},
                {"symbol": "AMD", "raw_signal_id": "raw-amd"},
            ],
        },
    )
    _write(
        root / "reports" / "intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {"portfolio_ranking": [{"symbol": "AAL", "raw_signal_id": "raw-aal"}, {"symbol": "AMD", "raw_signal_id": "raw-amd"}], "raw_candidate_intents": []},
    )
    _write(
        root / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json",
        {"paper_session_id": SESSION, "rows": [{"symbol": "AAL", "candidate_id": "candidate-3"}, {"symbol": "AMD", "candidate_id": "candidate-4"}]},
    )


def test_signal_boundary_accounts_for_all_current_session_candidates(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_signal_evidence_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["current_session_candidate_count"] == 4
    assert payload["signal_evidence_graph_signal_count"] == 2
    assert payload["output_intent_count"] == 2
    assert payload["rejected_intent_count"] == 2
    assert payload["signal_evidence_present_count"] == 2
    assert payload["blocked_missing_output_intent_count"] == 2
    assert payload["silent_omission_count"] == 0
    statuses = {row["symbol"]: row["boundary_status"] for row in payload["boundary_rows"]}
    assert statuses == {
        "QQQ": SIGNAL_EVIDENCE_PRESENT,
        "SPY": SIGNAL_EVIDENCE_PRESENT,
        "AAL": SIGNAL_EVIDENCE_REJECTED_INTENT,
        "AMD": SIGNAL_EVIDENCE_REJECTED_INTENT,
    }


def test_signal_boundary_rejected_intent_rows_keep_lineage(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_signal_evidence_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    aal = next(row for row in payload["boundary_rows"] if row["symbol"] == "AAL")

    assert aal["in_signal_evidence_graph"] is False
    assert aal["in_output_intents"] is False
    assert aal["in_rejected_intents"] is True
    assert aal["in_arbitration_ranking"] is True
    assert aal["in_paper_review_queue"] is True
    assert aal["boundary_status"] == SIGNAL_EVIDENCE_REJECTED_INTENT
    assert "absent from final output_intents" in aal["boundary_reason"]


def test_signal_boundary_status_render(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_signal_evidence_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    rendered = render_signal_evidence_boundary_v1(payload)

    assert "Signal evidence boundary for PAPER-2026-05-28-0950:" in rendered
    assert "- current-session candidates: 4" in rendered
    assert "- signal evidence present: 2" in rendered
    assert "- blocked missing output intent: 2" in rendered
