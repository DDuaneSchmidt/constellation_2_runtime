from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.account_economic_state_authority_v1 import account_source_summary_v1
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1
from constellation_2.common.economic_state_authority_v1 import _evaluate_semantics, EconomicContext


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_contract_input_hashes_require_real_source(tmp_path: Path) -> None:
    src = tmp_path / "operator_statement.v1.json"
    _write(src, {"day_utc": "2026-05-20", "cash_total": "300000.00"})
    hashes = contract_input_hashes_for_paths_v1([src])
    assert hashes[f"input:{src.resolve()}"]


def _ctx(tmp_path: Path) -> EconomicContext:
    truth = tmp_path / "truth"
    _write(truth / "reports/aegis_runtime_truth_kernel_v1/2026-05-20/runtime_evaluation.v1.json", {"deterministic_output_hash": "a" * 64})
    return EconomicContext(
        repo_root=Path.cwd(), canonical_truth_root=truth, truth_sleeves_root=tmp_path, execution_truth_root=tmp_path,
        day_utc="2026-05-20", sleeve_id="PRIMARY", environment="PAPER", ib_account="DUO847203",
        operation_type="fresh_paper_entry_v1", context_hash="ctx", global_context_hash="gctx",
        global_context_build_path=tmp_path / "gbuild.json", global_context_package_path=tmp_path / "gpkg.json",
    )


def test_runtime_bound_context_rejects_wrong_runtime_hash(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    status, detail = _evaluate_semantics(
        dependency_id="global_context_package_v1",
        obj={"schema_id":"global_context_package","day_utc":"2026-05-20","validation_status":"VALID","runtime_evaluation_hash":"b" * 64,"sealed":True,"mode":"PAPER","sleeve_id":"PRIMARY","account_id":"DUO847203","operation_type":"fresh_paper_entry_v1","context_hash":"gctx"},
        path=tmp_path / "global.json",
        ctx=ctx,
    )
    assert status == "STALE"
    assert "RUNTIME_EVALUATION_HASH_MISMATCH" in detail


def test_static_account_source_allows_runtime_rebinding_by_economic_package(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    status, detail = _evaluate_semantics(
        dependency_id="cash_ledger_snapshot_v1",
        obj={"schema_id":"C2_CASH_LEDGER_SNAPSHOT_V1","day_utc":"2026-05-20","status":"OK","runtime_evaluation_hash":"b" * 64,"source_type":"STATIC_RISK_BUDGET","snapshot":{"cash_total_cents":1}},
        path=tmp_path / "cash.json",
        ctx=ctx,
    )
    assert status == "PRESENT"
    assert detail == "CASH_LEDGER_OK"


def test_empty_static_paper_positions_validate_without_broker_statement(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    status, detail = _evaluate_semantics(
        dependency_id="positions_snapshot_v5",
        obj={"schema_id":"C2_POSITIONS_SNAPSHOT_V5","day_utc":"2026-05-20","status":"OK","runtime_evaluation_hash":"b" * 64,"source_type":"SIMULATION_LEDGER","items":[],"reason_codes":["BUNDLE_A_CANONICAL_STATE_V5","BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY"],"reconciliation":{"positions_status":"UNKNOWN","broker_statement_present":False}},
        path=tmp_path / "positions.json",
        ctx=ctx,
    )
    assert status == "PRESENT"
    assert detail == "EMPTY_POSITIONS_STATIC_PAPER"


def test_account_source_summary_reports_missing_paths(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write(truth / "reports/aegis_runtime_truth_kernel_v1/2026-05-20/runtime_evaluation.v1.json", {"deterministic_output_hash": "a" * 64})
    result = {"build_obj": {"dependency_results": []}}
    summary = account_source_summary_v1(truth_root=truth, day_utc="2026-05-20", result=result)
    assert summary["cash_source_type"] == "MISSING"
    assert summary["runtime_evaluation_hash"] == "a" * 64
