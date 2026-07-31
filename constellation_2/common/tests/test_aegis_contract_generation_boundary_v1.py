from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.contract_generation_boundary_v1 import (
    CONTRACT_BLOCKED_MISSING_MARKET_DATA,
    CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE,
    CONTRACT_INPUT_BOUNDARY_VIOLATION,
    VALID_CONTRACT,
    build_contract_generation_boundary_v1,
    render_contract_generation_boundary_v1,
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
        "paper_session_id": SESSION,
        "raw_signal_id": f"raw-{symbol.lower()}",
        "candidate_lifecycle_state": "GENERATED",
    }


def _seed_base(root: Path, *, include_violation: bool = False) -> None:
    candidates = [_candidate("QQQ", 1), _candidate("AMT", 2), _candidate("BDX", 3)]
    if include_violation:
        candidates.append(_candidate("ARM", 4))
    _write(
        root / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {"day_utc": DAY, "paper_session_id": SESSION, "current_session_candidates": candidates},
    )
    market_rows = [
        {"symbol": "QQQ", "value": "100", "day_utc": DAY, "validation_status": "VALID"},
        {"symbol": "AMT", "value": "188", "day_utc": DAY, "validation_status": "VALID"},
        {"symbol": "BDX", "value": "148", "day_utc": "2026-05-27", "validation_status": "STALE", "reason": "SOURCE_SESSION_NOT_CURRENT:2026-05-27"},
    ]
    if include_violation:
        market_rows.append({"symbol": "ARM", "value": "335", "day_utc": DAY, "validation_status": "VALID"})
    _write(
        root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json",
        {"input_records": market_rows},
    )
    signal_rows = [{"symbol": "QQQ", "raw_signal_id": "raw-qqq"}]
    if include_violation:
        signal_rows.append({"symbol": "ARM", "raw_signal_id": "raw-arm"})
    _write(
        root / "reports" / "aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json",
        {"signals": signal_rows},
    )
    _write(
        root / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {
            "candidate_contracts": [
                {"symbol": "QQQ", "candidate_id": "candidate-1", "candidate_contract_id": "candidate-1", "contract_validation_status": "VALID"}
            ],
            "rejected_raw_signals": [],
        },
    )


def test_contract_boundary_accounts_for_all_current_session_candidates(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_contract_generation_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["current_session_candidate_count"] == 3
    assert payload["market_data_valid_count"] == 2
    assert payload["signal_evidence_graph_signal_count"] == 1
    assert payload["contract_generation_attempted_count"] == 1
    assert payload["valid_contract_count"] == 1
    assert payload["blocked_missing_signal_evidence_count"] == 1
    assert payload["blocked_missing_market_data_count"] == 1
    assert payload["silent_omission_count"] == 0
    statuses = {row["symbol"]: row["boundary_status"] for row in payload["boundary_rows"]}
    assert statuses == {
        "QQQ": VALID_CONTRACT,
        "AMT": CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE,
        "BDX": CONTRACT_BLOCKED_MISSING_MARKET_DATA,
    }


def test_market_data_valid_candidate_missing_from_signal_graph_is_explicitly_blocked(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_contract_generation_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    amt = next(row for row in payload["boundary_rows"] if row["symbol"] == "AMT")

    assert amt["market_data_valid"] is True
    assert amt["in_signal_evidence_graph"] is False
    assert amt["contract_generation_attempted"] is False
    assert amt["contract_present"] is False
    assert amt["boundary_status"] == CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE
    assert "absent from signal_evidence_graph_v1" in amt["boundary_reason"]




def test_contract_boundary_uses_signal_evidence_boundary_reason(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "boundary_rows": [
                {
                    "symbol": "AMT",
                    "candidate_id": "candidate-2",
                    "candidate_contract_id": "candidate-2",
                    "boundary_status": "SIGNAL_EVIDENCE_REJECTED_INTENT",
                    "boundary_reason": "Candidate is present in sleeve rejected_intents/stale_artifacts but absent from final output_intents.",
                }
            ]
        },
    )

    payload = build_contract_generation_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    amt = next(row for row in payload["boundary_rows"] if row["symbol"] == "AMT")

    assert amt["boundary_status"] == CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE
    assert "Signal boundary" in amt["boundary_reason"]
    assert "rejected_intents" in amt["boundary_reason"]

def test_boundary_violation_when_signal_candidate_has_no_contract_result(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_violation=True)

    payload = build_contract_generation_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    arm = next(row for row in payload["boundary_rows"] if row["symbol"] == "ARM")

    assert arm["market_data_valid"] is True
    assert arm["in_signal_evidence_graph"] is True
    assert arm["contract_generation_attempted"] is True
    assert arm["boundary_status"] == CONTRACT_INPUT_BOUNDARY_VIOLATION
    assert payload["contract_input_boundary_violation_count"] == 1
    assert payload["silent_omission_count"] == 0


def test_status_render_prints_market_data_valid_but_uncontracted_symbols(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_contract_generation_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    rendered = render_contract_generation_boundary_v1(payload)

    assert "Contract generation for PAPER-2026-05-28-0950:" in rendered
    assert "- current-session candidates: 3" in rendered
    assert "- valid contracts: 1" in rendered
    assert "- blocked missing signal evidence: 1" in rendered
    assert "- market-data-valid but uncontracted symbols: AMT" in rendered
