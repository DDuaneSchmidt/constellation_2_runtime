from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.candidate_lineage_forensics_v1 import build_candidate_lineage_forensics_v1

DAY = "2026-05-20"
CURRENT = "2026-05-26"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_finds_candidate_by_symbol(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write(root / "reports" / "candidate_lineage_v1" / DAY / "run" / "candidate_lineage.v1.json", {
        "schema_id": "candidate_lineage",
        "lineage_rows": [{"candidate_id": "candidate-dow", "symbol": "DOW", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "raw_intent_id": "raw-dow"}],
    })

    payload = build_candidate_lineage_forensics_v1(truth_root=root, symbol="DOW")

    summary = payload["forensic_summary"]
    assert payload["day_utc"] == DAY
    assert summary["candidate_id"] == "candidate-dow"
    assert summary["raw_signal_id"] == "raw-dow"


def test_distinguishes_governed_vs_legacy_path(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write(root / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json", {
        "candidate_contracts": [{"candidate_id": "candidate-dow", "symbol": "DOW", "raw_signal_id": "raw-dow", "sleeve_id": "TREND", "contract_validation_status": "VALID"}],
    })
    _write(root / "reports" / "aegis_signal_evidence_graph_v1" / DAY / "signal_evidence_graph.v1.json", {
        "signals": [{"symbol": "DOW", "raw_signal_id": "raw-dow"}],
    })
    governed = build_candidate_lineage_forensics_v1(truth_root=root, symbol="DOW", day_utc=DAY)
    assert governed["forensic_summary"]["classification"] == "GOVERNED"

    legacy_root = tmp_path / "legacy"
    _write(legacy_root / "manual_trade_receipts" / DAY / "manual-dow.manual_trade_receipt.v1.json", {"symbol": "DOW", "receipt_id": "manual-dow"})
    legacy = build_candidate_lineage_forensics_v1(truth_root=legacy_root, symbol="DOW", day_utc=DAY)
    assert legacy["forensic_summary"]["classification"] == "LEGACY_OR_PARTIAL"


def test_identifies_missing_lineage_fields(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write(root / "reports" / "candidate_lineage_v1" / DAY / "run" / "candidate_lineage.v1.json", {
        "lineage_rows": [{"candidate_id": "candidate-dow", "symbol": "DOW"}],
    })

    summary = build_candidate_lineage_forensics_v1(truth_root=root, symbol="DOW", day_utc=DAY)["forensic_summary"]

    assert "raw_signal_id" in summary["missing_lineage_fields"]
    assert "candidate_contract_path" in summary["missing_lineage_fields"]
    assert "signal_evidence_graph_path" in summary["missing_lineage_fields"]


def test_does_not_treat_simulated_paper_rehearsal_candidates_as_real(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write(root / "reports" / "manual_execution_receipt_v1" / DAY / "paper_rehearsal_golden_path_v1" / "manual_execution_receipt.v1.json", {
        "symbol": "DOW",
        "candidate_id": "candidate-dow",
        "receipt_type": "SIMULATED_PAPER",
        "verification_status": "VERIFIED",
    })

    summary = build_candidate_lineage_forensics_v1(truth_root=root, symbol="DOW", day_utc=DAY)["forensic_summary"]

    assert summary["classification"] == "SIMULATED_OR_REHEARSAL"
    assert summary["does_not_treat_rehearsal_as_real"] is True


def test_forensics_does_not_alter_policy_gates(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write(root / "reports" / "aegis_candidate_contracts_v1" / CURRENT / "candidate_contracts.v1.json", {
        "candidate_contracts": [{"candidate_id": "candidate-dow", "symbol": "DOW", "raw_signal_id": "raw-dow", "sleeve_id": "TREND", "contract_validation_status": "VALID"}],
    })
    _write(root / "reports" / "aegis_signal_evidence_graph_v1" / CURRENT / "signal_evidence_graph.v1.json", {"signals": [{"symbol": "DOW"}]})

    payload = build_candidate_lineage_forensics_v1(truth_root=root, symbol="DOW", day_utc=CURRENT)

    assert payload["safety"]["policy_gates_modified"] is False
    assert payload["safety"]["trade_advice_allowed"] is False
    assert payload["safety"]["broker_execution_allowed"] is False
