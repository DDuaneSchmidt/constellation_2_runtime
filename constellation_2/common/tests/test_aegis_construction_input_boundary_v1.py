from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.construction_input_boundary_v1 import (
    BOUNDARY_VIOLATION_SILENT_OMISSION,
    CONSTRUCTED,
    CONSTRUCTION_BLOCKED_MARKET_DATA,
    CONSTRUCTION_BLOCKED_MISSING_CONTRACT,
    build_construction_input_boundary_v1,
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
        "candidate_lifecycle_state": "GENERATED",
    }


def _seed_base(root: Path, *, include_ready_contract: bool = False, include_construction: bool = True) -> None:
    candidates = [_candidate("QQQ", 1), _candidate("AMT", 2), _candidate("BDX", 3)]
    if include_ready_contract:
        candidates.append(_candidate("ARM", 4))
    _write(
        root / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {"day_utc": DAY, "paper_session_id": SESSION, "current_session_candidates": candidates},
    )
    _write(
        root / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json",
        {"paper_session_id": SESSION, "review_candidates": [candidates[0]]},
    )
    contract_rows = [
        {"symbol": "QQQ", "candidate_id": "candidate-1", "candidate_contract_id": "candidate-1", "direction": "LONG"},
    ]
    if include_ready_contract:
        contract_rows.append({"symbol": "ARM", "candidate_id": "candidate-4", "candidate_contract_id": "candidate-4", "direction": "LONG"})
    _write(
        root / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {"candidate_contracts": contract_rows},
    )
    market_rows = [
        {"symbol": "QQQ", "value": "100", "day_utc": DAY, "validation_status": "VALID"},
        {"symbol": "AMT", "value": "188", "day_utc": DAY, "validation_status": "VALID"},
        {"symbol": "BDX", "value": "148", "day_utc": "2026-05-27", "validation_status": "STALE", "reason": "SOURCE_SESSION_NOT_CURRENT:2026-05-27"},
    ]
    if include_ready_contract:
        market_rows.append({"symbol": "ARM", "value": "335", "day_utc": DAY, "validation_status": "VALID"})
    _write(
        root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json",
        {"input_records": market_rows},
    )
    if include_construction:
        _write(
            root / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json",
            {
                "paper_session_id": SESSION,
                "constructed_paper_trades": [{"symbol": "QQQ", "candidate_id": "candidate-1", "candidate_contract_id": "candidate-1", "construction_status": "CONSTRUCTED"}],
                "skipped_candidates": [],
                "market_data_diagnostics": [{"symbol": "QQQ", "candidate_id": "candidate-1", "status": "PASS"}],
            },
        )


def test_boundary_report_accounts_for_all_current_session_candidates(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_construction_input_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert payload["current_session_candidate_count"] == 3
    assert payload["candidate_review_packet_count"] == 1
    assert payload["candidate_contract_count"] == 1
    assert payload["market_data_valid_count"] == 2
    assert payload["construction_attempted_count"] == 1
    assert payload["constructed_count"] == 1
    assert payload["blocked_missing_contract_count"] == 1
    assert payload["blocked_market_data_count"] == 1
    assert payload["silent_omission_count"] == 0
    statuses = {row["symbol"]: row["boundary_status"] for row in payload["boundary_rows"]}
    assert statuses == {"QQQ": CONSTRUCTED, "AMT": CONSTRUCTION_BLOCKED_MISSING_CONTRACT, "BDX": CONSTRUCTION_BLOCKED_MARKET_DATA}


def test_valid_market_candidate_absent_from_review_packet_is_not_silent_if_missing_contract(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_construction_input_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    amt = next(row for row in payload["boundary_rows"] if row["symbol"] == "AMT")

    assert amt["in_candidate_review_packet"] is False
    assert amt["market_data_valid"] is True
    assert amt["candidate_contract_present"] is False
    assert amt["construction_attempted"] is False
    assert amt["boundary_status"] == CONSTRUCTION_BLOCKED_MISSING_CONTRACT
    assert "no candidate contract" in amt["boundary_reason"]




def test_construction_boundary_uses_contract_generation_boundary_reason(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "reports" / "aegis_contract_generation_boundary_v1" / DAY / "contract_generation_boundary.v1.json",
        {
            "boundary_rows": [
                {
                    "symbol": "AMT",
                    "candidate_id": "candidate-2",
                    "candidate_contract_id": "candidate-2",
                    "boundary_status": "CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE",
                    "boundary_reason": "Candidate has valid market data but is absent from signal_evidence_graph_v1.",
                }
            ]
        },
    )

    payload = build_construction_input_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    amt = next(row for row in payload["boundary_rows"] if row["symbol"] == "AMT")

    assert amt["boundary_status"] == CONSTRUCTION_BLOCKED_MISSING_CONTRACT
    assert "Contract boundary" in amt["boundary_reason"]
    assert "absent from signal_evidence_graph_v1" in amt["boundary_reason"]

def test_stale_market_data_blocks_construction_boundary(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_construction_input_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    bdx = next(row for row in payload["boundary_rows"] if row["symbol"] == "BDX")

    assert bdx["market_data_bound"] is True
    assert bdx["market_data_valid"] is False
    assert bdx["boundary_status"] == CONSTRUCTION_BLOCKED_MARKET_DATA


def test_ready_candidate_without_construction_result_is_boundary_violation(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_ready_contract=True)

    payload = build_construction_input_boundary_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    arm = next(row for row in payload["boundary_rows"] if row["symbol"] == "ARM")

    assert arm["candidate_contract_present"] is True
    assert arm["market_data_valid"] is True
    assert arm["construction_attempted"] is False
    assert arm["boundary_status"] == BOUNDARY_VIOLATION_SILENT_OMISSION
    assert payload["silent_omission_count"] == 1
    assert payload["status"] == BOUNDARY_VIOLATION_SILENT_OMISSION
