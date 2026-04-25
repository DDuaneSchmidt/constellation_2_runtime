#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1
from constellation_2.common.advisory.advisory_storage_v1 import (
    execution_intent_path_v1,
    write_immutable_json_v1 as write_advisory_immutable_json_v1,
)
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.household_portfolio_compiler_v1 import trade_action_key_v1
from constellation_2.common.advisory.household_portfolio_storage_v1 import (
    portfolio_authorization_path_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_frozen_decision_input_bundle_v1,
    build_governed_artifact_lineage_v1,
)
from constellation_2.common.execution_build_authority_v1 import _constitutional_dependency_refs, run_execution_build_authority_v1
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.runtime_contract_v1 import resolve_truth_sleeves_root
from constellation_2.common.runtime_base_v1 import advisor_runtime_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

DEFAULT_PROOF_ROOT = Path("/tmp/constellation_2_foundation/final_readiness_proof_v1").resolve()
DEFAULT_DAY = "2026-04-14"
DEFAULT_PRODUCED_UTC = "2026-04-14T14:30:00Z"
DEFAULT_IB_ACCOUNT = "DUO847203"
PHASEC_FIXTURE = (REPO_ROOT / "_smoketest_phasec_2026_04_02").resolve()
FOUNDATION_ROOT = Path("/tmp/constellation_2_foundation").resolve()


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve_capital_authority_allocation_ref(*, truth_root: Path, day: str) -> Dict[str, str]:
    candidate_paths = [
        (
            (resolve_truth_sleeves_root() / "PRIMARY" / "PAPER" / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json")
            .resolve()
        ),
        (truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json").resolve(),
    ]
    for candidate in candidate_paths:
        if not candidate.exists() or not candidate.is_file():
            continue
        candidate_obj = _read_json(candidate)
        validate_against_repo_schema_v1(
            candidate_obj,
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json",
        )
        return {"path": str(candidate), "sha256": _sha256_file(candidate)}
    raise SystemExit(
        "FAIL: READINESS_CAPITAL_AUTHORITY_ALLOCATION_MISSING:"
        + "|".join(str(path) for path in candidate_paths)
    )


def _require_repo_prerequisites(*, proof_root: Path) -> None:
    if not (REPO_ROOT / "constellation_2").exists():
        raise SystemExit(f"FAIL: REPO_LAYOUT_MISSING_CONSTELLATION_2:{REPO_ROOT}")
    if not (REPO_ROOT / "ops" / "tools").exists():
        raise SystemExit(f"FAIL: REPO_LAYOUT_MISSING_OPS_TOOLS:{REPO_ROOT}")
    if not PHASEC_FIXTURE.exists() or not PHASEC_FIXTURE.is_dir():
        raise SystemExit(f"FAIL: REQUIRED_PHASEC_FIXTURE_MISSING:{PHASEC_FIXTURE}")
    fixture_plan = PHASEC_FIXTURE / "equity_order_plan.v2.json"
    if not fixture_plan.exists() or not fixture_plan.is_file():
        raise SystemExit(f"FAIL: REQUIRED_PHASEC_ORDER_PLAN_MISSING:{fixture_plan}")
    if proof_root != FOUNDATION_ROOT and FOUNDATION_ROOT not in proof_root.parents:
        raise SystemExit(f"FAIL: PROOF_ROOT_OUTSIDE_FOUNDATION:{proof_root}")
    try:
        FOUNDATION_ROOT.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"FAIL: FOUNDATION_ROOT_NOT_WRITABLE:{FOUNDATION_ROOT}:{type(exc).__name__}:{exc}") from exc
    advisor_root = advisor_runtime_root()
    try:
        advisor_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"FAIL: ADVISOR_RUNTIME_ROOT_NOT_WRITABLE:{advisor_root}:{type(exc).__name__}:{exc}") from exc


def _hx(char: str) -> str:
    return char * 64


def _proof_paths(proof_root: Path) -> Dict[str, Path]:
    return {
        "proof_root": proof_root.resolve(),
        "truth_root": (proof_root / "truth_sleeves" / "PRIMARY" / "PAPER").resolve(),
        "replay_root": (proof_root / "replay_truth").resolve(),
        "advisor_output_root": advisor_runtime_root(),
    }


def _phasec_plan() -> Dict[str, Any]:
    return _read_json(PHASEC_FIXTURE / "equity_order_plan.v2.json")


def _intent_snapshot_doc(*, day: str, produced_utc: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": str(plan.get("source_intent_id") or f"proof_intent_{day}"),
        "created_at_utc": produced_utc,
        "engine": {
            "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
            "suite": "C2_HYBRID_V1",
            "mode": "PAPER",
        },
        "underlying": {"symbol": str(plan.get("symbol") or "SPY"), "currency": str(plan.get("currency") or "USD")},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.4",
        "expected_holding_days": 3,
        "risk_class": "TREND",
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }


def _seed_submit_prerequisites(*, truth_root: Path, day: str, produced_utc: str, ib_account: str) -> Dict[str, Any]:
    plan = _phasec_plan()
    intent_hash = str(plan.get("intent_hash") or "").strip()
    intent_sha256 = str(plan.get("intent_sha256") or intent_hash).strip()
    if not intent_sha256:
        raise SystemExit("FAIL: PHASEC_FIXTURE_MISSING_INTENT_HASH")
    snapshot = _intent_snapshot_doc(day=day, produced_utc=produced_utc, plan=plan)
    _write_json(truth_root / "intents_v1" / "snapshots" / day / f"{intent_sha256}.exposure_intent.v1.json", snapshot)
    if intent_hash and intent_hash != intent_sha256:
        _write_json(truth_root / "intents_v1" / "snapshots" / day / f"{intent_hash}.exposure_intent.v1.json", snapshot)
    _write_json(
        truth_root / "intents_v1" / "day_rollup" / day / "intents_day_rollup.v1.json",
        {
            "schema_id": "intents_day_rollup.v1",
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"component": "paper_day_readiness_proof_v1", "version": "v1", "git_sha": "proof"},
            "inputs": {
                "market_data_snapshot_hashes": [_hx("1")],
                "market_calendar_hash": _hx("2"),
                "engine_config_hashes": [_hx("3")],
            },
            "engines": [
                {
                    "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
                    "intent_type": "exposure_intent.v1",
                    "intent_hashes": [intent_sha256],
                    "intent_count": 1,
                }
            ],
            "rollup_sha256": _hx("4"),
        },
    )
    _write_json(
        truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "ok": True,
            "day_utc": day,
            "status": "PASS",
            "authoritative": True,
            "points_to": str((truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()),
        },
    )
    _write_json(
        truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json",
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": 1,
            "day_utc": day,
            "mode": "PAPER",
            "produced_utc": produced_utc,
            "included_gates": [],
            "excluded_gates": [],
            "blocking_gates": [],
            "status": "PASS",
            "blocking_class": "NONE",
            "reason_codes": ["AUTHORIZATION_GATES_PASS"],
            "evidence_refs": [],
            "decision_ledger_ref": "proof",
        },
    )
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json",
        {
            "schema_id": "global_kill_switch_state",
            "schema_version": "v1",
            "state": "INACTIVE",
            "allow_entries": True,
            "allow_exits": True,
        },
    )
    _write_json(
        truth_root / "trade_submit_readiness_c2_v1" / "status.json",
        {
            "schema_id": "trade_submit_readiness_c2",
            "schema_version": "v1",
            "ok": True,
            "state": "OK",
            "environment": "PAPER",
            "ib_account": ib_account,
            "provenance": {"truth_root": str(truth_root)},
        },
    )
    _write_json(
        truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_sha256}.authorization.v1.json",
        {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "git_sha": "proof", "module": "run_paper_day_readiness_proof_v1.py"},
            "status": "AUTHORIZED",
            "reason_codes": [],
            "input_manifest": [
                {
                    "type": "intent",
                    "path": str((truth_root / "intents_v1" / "snapshots" / day / f"{intent_sha256}.exposure_intent.v1.json").resolve()),
                    "sha256": intent_sha256,
                    "day_utc": day,
                    "producer": "proof",
                }
            ],
            "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
            "intent_id": str(snapshot["intent_id"]),
            "intent_hash": intent_sha256,
            "authorization": {
                "decision": "AUTHORIZED",
                "authorized_quantity": int(plan.get("qty_shares") or 1),
                "constraints": [],
                "decision_hash": _hx("5"),
            },
        },
    )
    _write_json(
        truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json",
        {
            "schema_id": "capital_authority_allocation",
            "schema_version": "v1",
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"module": "run_paper_day_readiness_proof_v1.py"},
            "input_manifest": [],
            "status": "OK",
        },
    )
    return {
        "plan": plan,
        "intent_hash": intent_hash,
        "intent_sha256": intent_sha256,
        "intent_id": str(snapshot["intent_id"]),
        "engine_id": str(plan.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
        "symbol": str(plan.get("symbol") or "SPY"),
    }


def _build_execution_intent_for_submit_path(
    *,
    day: str,
    produced_utc: str,
    ib_account: str,
    seed: Dict[str, Any],
    execution_intent_id_override: str = "",
    intent_hash_override: str = "",
) -> ExecutionIntentV1:
    plan = dict(seed.get("plan") or {})
    intent_id = str(execution_intent_id_override or seed.get("intent_id") or f"proof_intent_{day}").strip()
    if not intent_id:
        raise SystemExit("FAIL: READINESS_INTENT_ID_MISSING")
    action = str(plan.get("action") or "BUY").strip().upper()
    if action not in {"BUY", "SELL"}:
        raise SystemExit(f"FAIL: READINESS_INTENT_SIDE_INVALID:{action}")
    order_terms_src = dict(plan.get("order_terms") or {})
    order_terms = {
        "order_type": str(order_terms_src.get("order_type") or "LIMIT").strip().upper(),
        "limit_price": order_terms_src.get("limit_price"),
        "time_in_force": str(order_terms_src.get("time_in_force") or "DAY").strip().upper(),
    }
    intent_payload = {
        "schema_id": "execution_intent",
        "schema_version": "v1",
        "record_id": intent_id,
        "execution_intent_id": intent_id,
        "promotion_record_id": f"paper_day_readiness_proof:{intent_id}",
        "household_id": "PAPER_READINESS_PROOF",
        "created_at_utc": produced_utc,
        "effective_at_utc": produced_utc,
        "actor_source": "run_paper_day_readiness_proof_v1",
        "contract_version": "execution_intent_contract_v1",
        "builder_version": "execution_intent_builder_v1",
        "idempotency_key": canonical_hash_for_c2_artifact_v1(
            {
                "intent_id": intent_id,
                "day_utc": day,
                "ib_account": ib_account,
                "plan_id": str(plan.get("plan_id") or ""),
                "intent_sha256": str(seed.get("intent_sha256") or ""),
            }
        ),
        "operation_type": "fresh_paper_entry_v1",
        "day_utc": day,
        "environment": "PAPER",
        "sleeve_id": "PRIMARY",
        "account_id": ib_account,
        "engine_id": str(seed.get("engine_id") or "C2_TREND_EQ_PRIMARY_V1"),
        "instrument": {
            "kind": "EQUITY",
            "symbol": str(seed.get("symbol") or plan.get("symbol") or "SPY"),
            "currency": str(plan.get("currency") or "USD"),
            "ib_conId": None,
            "ib_localSymbol": str(seed.get("symbol") or plan.get("symbol") or "SPY"),
        },
        "side": action,
        "quantity_shares": int(plan.get("qty_shares") or 1),
        "order_terms": order_terms,
        "parent_lineage_refs": [
            f"proof_seed_intent_id:{intent_id}",
        ],
        "source_artifact_refs": [
            f"phasec_fixture_path:{(PHASEC_FIXTURE / 'equity_order_plan.v2.json').resolve()}",
            f"phasec_fixture_intent_sha256:{str(seed.get('intent_sha256') or '')}",
        ],
        "canonical_json_hash": None,
    }
    if str(intent_hash_override).strip():
        intent_payload["idempotency_key"] = str(intent_hash_override).strip()
    intent_payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(
        {**intent_payload, "canonical_json_hash": None}
    )
    return ExecutionIntentV1.from_dict(intent_payload)


def _execution_build_capital_intent_hash_missing(build_obj: Dict[str, Any]) -> bool:
    for row in list(build_obj.get("dependency_results") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("dependency_id") or "").strip() != "capital_authority_allocation_v1":
            continue
        detail = str(row.get("detail") or "").strip()
        return "CAPITAL_AUTHORITY_INTENT_HASH_MISSING" in detail
    return False


def _is_hex_sha256(value: str) -> bool:
    token = str(value or "").strip().lower()
    return len(token) == 64 and all(ch in "0123456789abcdef" for ch in token)


def _load_capital_lineage_intent_hashes_for_day(*, day: str) -> set[str]:
    capauth_path = (
        resolve_truth_sleeves_root()
        / "PRIMARY"
        / "PAPER"
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / day
        / "capital_authority_allocation.v1.json"
    ).resolve()
    if not capauth_path.exists() or not capauth_path.is_file():
        raise SystemExit(f"FAIL: READINESS_CAPITAL_AUTHORITY_ALLOCATION_MISSING:{capauth_path}")
    capauth_obj = _read_json(capauth_path)
    decision_chain = dict(capauth_obj.get("decision_chain") or {})
    hashes: set[str] = set()
    for row in list(decision_chain.get("authorized_trade_intents") or []):
        if not isinstance(row, dict):
            continue
        intent_hash = str(row.get("intent_hash") or row.get("intent_sha256") or "").strip().lower()
        if _is_hex_sha256(intent_hash):
            hashes.add(intent_hash)
    if not hashes:
        raise SystemExit(
            "FAIL: READINESS_CAPITAL_AUTHORITY_INTENT_SET_EMPTY:"
            f"path={capauth_path}:reason=authorized_trade_intents_missing_hashes"
        )
    return hashes


def _extract_package_intent_hash(*, package_obj: Dict[str, Any]) -> str:
    advisory_submission = dict(package_obj.get("advisory_submission") or {})
    field_candidates = [
        str(advisory_submission.get("promotion_idempotency_key") or "").strip(),
        str(advisory_submission.get("intent_sha256") or "").strip(),
        str(package_obj.get("intent_hash") or "").strip(),
        str(package_obj.get("intent_sha256") or "").strip(),
    ]
    for candidate in field_candidates:
        if _is_hex_sha256(candidate):
            return candidate.lower()
    selected_plan_ref = dict(package_obj.get("selected_order_plan_ref") or {})
    selected_plan_path_raw = str(selected_plan_ref.get("path") or "").strip()
    if not selected_plan_path_raw:
        return ""
    selected_plan_path = Path(selected_plan_path_raw).resolve()
    if not selected_plan_path.exists() or not selected_plan_path.is_file():
        return ""
    selected_plan_obj = _read_json(selected_plan_path)
    plan_hashes = [
        str(selected_plan_obj.get("intent_sha256") or "").strip(),
        str(selected_plan_obj.get("intent_hash") or "").strip(),
    ]
    for candidate in plan_hashes:
        if _is_hex_sha256(candidate):
            return candidate.lower()
    return ""


def _first_blocking_dependency_detail(build_obj: Dict[str, Any]) -> str:
    for row in list(build_obj.get("dependency_results") or []):
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").strip().upper()
        detail = str(row.get("detail") or "").strip()
        if status in {"BLOCKED", "FAILED", "MISSING", "INVALID", "REJECTED"} and detail:
            return detail
    for row in list(build_obj.get("dependency_results") or []):
        if not isinstance(row, dict):
            continue
        detail = str(row.get("detail") or "").strip()
        if detail:
            return detail
    return "UNKNOWN_DEPENDENCY_BLOCKER"


def _latest_active_attempt_id_for_day(*, day: str) -> str:
    pointer_path = (
        resolve_truth_sleeves_root()
        / "PRIMARY"
        / "PAPER"
        / "run_ledgers_v1"
        / day
        / "scopes"
        / "PRIMARY__PAPER__GLOBAL"
        / "latest_active_attempt.v1.json"
    ).resolve()
    if not pointer_path.exists() or not pointer_path.is_file():
        raise SystemExit(f"FAIL: READINESS_LATEST_ACTIVE_ATTEMPT_POINTER_MISSING:{pointer_path}")
    pointer_obj = _read_json(pointer_path)
    attempt_id = str(pointer_obj.get("attempt_id") or "").strip()
    if not attempt_id:
        raise SystemExit(f"FAIL: READINESS_LATEST_ACTIVE_ATTEMPT_POINTER_INVALID:{pointer_path}")
    return attempt_id


def _resolve_canonical_phasec_candidate_path_for_day(*, day: str, capital_intent_hashes: set[str]) -> Path:
    active_attempt_id = _latest_active_attempt_id_for_day(day=day)
    attempt_dir = (
        resolve_truth_sleeves_root()
        / "PRIMARY"
        / "PAPER"
        / "phaseC_preflight_v1"
        / day
        / f"attempt_{active_attempt_id}"
    ).resolve()
    if not attempt_dir.exists() or not attempt_dir.is_dir():
        raise SystemExit(f"FAIL: READINESS_ACTIVE_ATTEMPT_DIR_MISSING:{attempt_dir}")
    for intent_hash in sorted(capital_intent_hashes):
        candidate_dir = (attempt_dir / intent_hash).resolve()
        if not candidate_dir.exists() or not candidate_dir.is_dir():
            continue
        if (candidate_dir / "equity_order_plan.v2.json").exists() or (candidate_dir / "equity_order_plan.v1.json").exists():
            return candidate_dir
    raise SystemExit(
        "FAIL: READINESS_ACTIVE_ATTEMPT_CAPITAL_INTENT_MISMATCH:"
        f"attempt_id={active_attempt_id}:attempt_dir={attempt_dir}:capital_intent_hashes={','.join(sorted(capital_intent_hashes))}"
    )


def _resolve_existing_submit_pair_for_day(*, day: str) -> Dict[str, Path]:
    capital_intent_hashes = _load_capital_lineage_intent_hashes_for_day(day=day)
    execution_root = (resolve_truth_sleeves_root() / "PRIMARY" / "PAPER").resolve()
    candidate_path = _resolve_canonical_phasec_candidate_path_for_day(day=day, capital_intent_hashes=capital_intent_hashes)
    build_result = run_execution_build_authority_v1(
        repo_root=REPO_ROOT,
        operation_type="fresh_paper_entry_v1",
        candidate_path=candidate_path,
        materialize=True,
        emit_package=True,
    )
    build_obj = dict(build_result.get("build_obj") or {})
    submission_id = str(build_obj.get("submission_id") or "").strip()
    if not submission_id:
        raise SystemExit(f"FAIL: READINESS_SUBMISSION_ID_MISSING:build_path={build_result.get('build_path')}")
    package_path = Path(str(build_result.get("package_path") or "")).resolve()
    if not str(build_result.get("package_path") or "").strip():
        package_path = (execution_root / "execution_package_v1" / day / submission_id / "execution_package.v1.json").resolve()
    submission_record_path = (
        execution_root / "execution_kernel_v1" / "submission_records" / day / submission_id / "submission_record.v1.json"
    ).resolve()
    return {
        "execution_package_path": package_path,
        "submission_record_path": submission_record_path,
        "execution_build_path": Path(str(build_result["build_path"])).resolve(),
        "submission_id": submission_id,
    }


def _upgrade_legacy_execution_build_v1(
    *,
    build_obj: Dict[str, Any],
    submission_id: str,
    truth_root: Path,
    day: str,
    trade_submit_readiness_override: Path | None = None,
) -> Dict[str, Any]:
    has_constitutional_fields = (
        isinstance(build_obj.get("constitutional_dependency_declaration"), dict)
        and isinstance(build_obj.get("constitutional_lineage"), dict)
        and isinstance(build_obj.get("frozen_decision_input_bundle"), dict)
    )
    if has_constitutional_fields:
        return dict(build_obj)

    upgraded = dict(build_obj)
    dependency_rows = list(upgraded.get("dependency_results") or [])
    results_by_id: Dict[str, Dict[str, Any]] = {}
    for row in dependency_rows:
        if not isinstance(row, dict):
            continue
        dependency_id = str(row.get("dependency_id") or "").strip()
        if dependency_id:
            results_by_id[dependency_id] = dict(row)
    for dependency_id, row in list(results_by_id.items()):
        dep_path_raw = str(row.get("path") or "").strip()
        if not dep_path_raw:
            continue
        dep_path = Path(dep_path_raw).resolve()
        if not dep_path.exists() or not dep_path.is_file():
            continue
        row["path"] = str(dep_path)
        row["sha256"] = _sha256_file(dep_path)
        results_by_id[dependency_id] = row
    upgraded_dependency_rows: list[Dict[str, Any]] = []
    seen_dependency_ids: set[str] = set()
    for row in dependency_rows:
        if not isinstance(row, dict):
            continue
        dependency_id = str(row.get("dependency_id") or "").strip()
        if not dependency_id:
            continue
        upgraded_dependency_rows.append(dict(results_by_id.get(dependency_id) or row))
        seen_dependency_ids.add(dependency_id)
    if "capital_authority_allocation_v1" not in results_by_id:
        capital_authority_ref = _resolve_capital_authority_allocation_ref(truth_root=truth_root, day=day)
        results_by_id["capital_authority_allocation_v1"] = {
            "dependency_id": "capital_authority_allocation_v1",
            "path": str(capital_authority_ref["path"]),
            "sha256": str(capital_authority_ref["sha256"]),
            "status": "PRESENT",
            "required": True,
            "advisory_only": False,
            "post_submit_only": False,
        }
        if "capital_authority_allocation_v1" not in seen_dependency_ids:
            upgraded_dependency_rows.append(dict(results_by_id["capital_authority_allocation_v1"]))
    trade_submit_readiness_path = (
        Path(trade_submit_readiness_override).resolve()
        if trade_submit_readiness_override is not None
        else (truth_root / "trade_submit_readiness_c2_v1" / "status.json").resolve()
    )
    if trade_submit_readiness_path.exists() and trade_submit_readiness_path.is_file():
        readiness_row = dict(results_by_id.get("trade_submit_readiness_c2_v1") or {})
        readiness_row.update(
            {
                "dependency_id": "trade_submit_readiness_c2_v1",
                "path": str(trade_submit_readiness_path),
                "sha256": _sha256_file(trade_submit_readiness_path),
                "status": "PRESENT",
                "required": True,
                "advisory_only": False,
                "post_submit_only": False,
            }
        )
        results_by_id["trade_submit_readiness_c2_v1"] = readiness_row
        replaced = False
        for idx, row in enumerate(upgraded_dependency_rows):
            if str(row.get("dependency_id") or "").strip() == "trade_submit_readiness_c2_v1":
                upgraded_dependency_rows[idx] = dict(readiness_row)
                replaced = True
                break
        if not replaced:
            upgraded_dependency_rows.append(dict(readiness_row))

    build_contract = assert_constitutional_writer_allowed_v1(
        REPO_ROOT,
        "execution_build_v1",
        "constellation_2.common.execution_build_authority_v1",
    )
    required_dependencies = [
        str(dep).strip()
        for dep in (build_contract.get("required_upstream_dependencies") or [])
        if str(dep).strip()
    ]
    constitutional_refs = _constitutional_dependency_refs(
        repo_root=REPO_ROOT,
        results=results_by_id,
        dependency_ids=required_dependencies,
    )
    artifact_class = str(build_contract.get("artifact_class") or "").strip()
    generated_at_utc = str(upgraded.get("generated_utc") or "").strip()
    if not generated_at_utc:
        generated_at_utc = DEFAULT_PRODUCED_UTC
    closure_status = str(upgraded.get("closure_status") or "").strip().upper()
    finality_state = FINALITY_FINALIZED if closure_status == "COMPLETE" else FINALITY_PROVISIONAL

    upgraded["constitutional_dependency_declaration"] = build_artifact_dependency_declaration_v1(
        artifact_type="execution_build_v1",
        artifact_class=artifact_class,
        authority_id="execution_build_v1",
        declared_dependency_artifacts=required_dependencies,
        dependency_refs=constitutional_refs,
    )
    upgraded["constitutional_lineage"] = build_governed_artifact_lineage_v1(
        artifact_type="execution_build_v1",
        artifact_version="v1",
        artifact_class=artifact_class,
        authority_id="execution_build_v1",
        producer_id="constellation_2.common.execution_build_authority_v1",
        generated_at_utc=generated_at_utc,
        effective_at_utc=generated_at_utc,
        finality_state=finality_state,
        input_artifact_refs=constitutional_refs,
        policy_snapshot_refs=[],
        code_version="readiness-proof-legacy-upgrade",
        run_id=submission_id,
    )
    upgraded["frozen_decision_input_bundle"] = build_frozen_decision_input_bundle_v1(
        artifact_type="execution_build_v1",
        authority_id="execution_build_v1",
        generated_at_utc=generated_at_utc,
        effective_at_utc=generated_at_utc,
        input_artifact_refs=constitutional_refs,
        policy_snapshot_refs=[],
        run_id=submission_id,
        reason_codes=["CONSTITUTIONAL_RUNTIME_FROZEN_INPUT_BUNDLE_V1"],
    )
    upgraded["dependency_results"] = upgraded_dependency_rows
    upgraded["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**upgraded, "canonical_json_hash": None})
    validate_against_repo_schema_v1(
        upgraded,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_build.v1.schema.json",
    )
    return upgraded


def _materialize_submit_interface_inputs(
    *,
    truth_root: Path,
    day: str,
    produced_utc: str,
    ib_account: str,
    seed: Dict[str, Any],
) -> Dict[str, Any]:
    pair = _resolve_existing_submit_pair_for_day(day=day)
    build_path = Path(str(pair["execution_build_path"])).resolve()
    if not build_path.exists() or not build_path.is_file():
        raise SystemExit(f"FAIL: READINESS_EXECUTION_BUILD_MISSING:{build_path}")

    legacy_build_obj = _read_json(build_path)
    legacy_closure_status = str(legacy_build_obj.get("closure_status") or "").strip().upper()
    if legacy_closure_status != "COMPLETE":
        raise SystemExit(
            "FAIL: READINESS_EXECUTION_BUILD_NOT_COMPLETE:"
            f"submission_id={pair['submission_id']}:closure_status={legacy_closure_status}:"
            f"detail={_first_blocking_dependency_detail(legacy_build_obj)}"
        )
    execution_package_path = Path(str(pair["execution_package_path"])).resolve()
    if not execution_package_path.exists() or not execution_package_path.is_file():
        raise SystemExit(f"FAIL: READINESS_EXECUTION_PACKAGE_MISSING:{execution_package_path}")
    package_obj = _read_json(execution_package_path)

    local_pair_root = (truth_root / "_readiness_submit_interface_v1" / day / str(pair["submission_id"])).resolve()
    local_pair_root.mkdir(parents=True, exist_ok=True)

    trade_submit_readiness_source_path: Path | None = None
    for row in list(package_obj.get("dependency_refs") or []):
        if not isinstance(row, dict):
            continue
        dependency_id = str(row.get("dependency_id") or "").strip()
        if dependency_id != "trade_submit_readiness_c2_v1":
            continue
        candidate = Path(str(row.get("path") or "")).resolve()
        if candidate.exists() and candidate.is_file():
            trade_submit_readiness_source_path = candidate
            break
    trade_submit_readiness_override_path: Path | None = None
    if trade_submit_readiness_source_path is not None:
        patched_readiness = _read_json(trade_submit_readiness_source_path)
        declaration = dict(patched_readiness.get("constitutional_dependency_declaration") or {})
        dependency_refs = list(declaration.get("dependency_refs") or [])
        refreshed_dependency_refs: list[Dict[str, Any]] = []
        for ref in dependency_refs:
            if not isinstance(ref, dict):
                continue
            refreshed_ref = dict(ref)
            ref_path_raw = str(refreshed_ref.get("path") or "").strip()
            if ref_path_raw:
                ref_path = Path(ref_path_raw).resolve()
                if ref_path.exists() and ref_path.is_file():
                    refreshed_ref["path"] = str(ref_path)
                    refreshed_ref["sha256"] = _sha256_file(ref_path)
            refreshed_dependency_refs.append(refreshed_ref)
        declaration["dependency_refs"] = refreshed_dependency_refs
        patched_readiness["constitutional_dependency_declaration"] = declaration
        lineage = dict(patched_readiness.get("constitutional_lineage") or {})
        lineage["input_artifact_refs"] = [dict(ref) for ref in refreshed_dependency_refs]
        patched_readiness["constitutional_lineage"] = lineage
        trade_submit_readiness_override_path = (local_pair_root / "trade_submit_readiness.status.v1.json").resolve()
        _write_json(trade_submit_readiness_override_path, patched_readiness)
        validate_against_repo_schema_v1(
            patched_readiness,
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json",
        )
    submission_record_path_obj = Path(str(pair["submission_record_path"])).resolve()
    if not submission_record_path_obj.exists() or not submission_record_path_obj.is_file():
        raise SystemExit(f"FAIL: READINESS_SUBMISSION_RECORD_MISSING:{submission_record_path_obj}")
    submission_record_obj = _read_json(submission_record_path_obj)
    upgraded_build_obj = _upgrade_legacy_execution_build_v1(
        build_obj=legacy_build_obj,
        submission_id=str(pair["submission_id"]),
        truth_root=truth_root,
        day=day,
        trade_submit_readiness_override=trade_submit_readiness_override_path,
    )

    local_build_path = (local_pair_root / "execution_build.v1.json").resolve()
    local_package_path = (local_pair_root / "execution_package.v1.json").resolve()
    local_submission_record_path = (local_pair_root / "submission_record.v1.json").resolve()
    _write_json(local_build_path, upgraded_build_obj)
    local_build_sha = _sha256_file(local_build_path)

    package_obj["build_ref"] = {"path": str(local_build_path), "sha256": local_build_sha}
    refreshed_dependency_refs: list[Dict[str, Any]] = []
    seen_dependency_ids: set[str] = set()
    for row in list(package_obj.get("dependency_refs") or []):
        if not isinstance(row, dict):
            continue
        refreshed = dict(row)
        dependency_id = str(refreshed.get("dependency_id") or "").strip()
        if dependency_id:
            seen_dependency_ids.add(dependency_id)
        if dependency_id == "trade_submit_readiness_c2_v1" and trade_submit_readiness_override_path is not None:
            if trade_submit_readiness_override_path.exists() and trade_submit_readiness_override_path.is_file():
                refreshed["path"] = str(trade_submit_readiness_override_path)
                refreshed["sha256"] = _sha256_file(trade_submit_readiness_override_path)
                refreshed_dependency_refs.append(refreshed)
                continue
        if dependency_id == "capital_authority_allocation_v1":
            capital_ref = _resolve_capital_authority_allocation_ref(truth_root=truth_root, day=day)
            refreshed["path"] = str(capital_ref["path"])
            refreshed["sha256"] = str(capital_ref["sha256"])
            refreshed_dependency_refs.append(refreshed)
            continue
        dep_path_raw = str(refreshed.get("path") or "").strip()
        if dep_path_raw:
            dep_path = Path(dep_path_raw).resolve()
            if dep_path.exists() and dep_path.is_file():
                refreshed["path"] = str(dep_path)
                refreshed["sha256"] = _sha256_file(dep_path)
        refreshed_dependency_refs.append(refreshed)
    if "capital_authority_allocation_v1" not in seen_dependency_ids:
        capital_ref = _resolve_capital_authority_allocation_ref(truth_root=truth_root, day=day)
        refreshed_dependency_refs.append(
            {
                "dependency_id": "capital_authority_allocation_v1",
                "owner_ref": "run_capital_authority_allocation_day_v1.py",
                "path": str(capital_ref["path"]),
                "role_class": "TRUTH_OWNER",
                "sha256": str(capital_ref["sha256"]),
                "status": "PRESENT",
            }
        )
    if refreshed_dependency_refs:
        package_obj["dependency_refs"] = refreshed_dependency_refs
    package_obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**package_obj, "canonical_json_hash": None})
    validate_against_repo_schema_v1(
        package_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json",
    )
    _write_json(local_package_path, package_obj)

    submission_record_obj["execution_build_ref"] = {"path": str(local_build_path), "sha256": local_build_sha}
    submission_record_obj["execution_package_ref"] = {
        "path": str(local_package_path),
        "sha256": str(package_obj.get("canonical_json_hash") or ""),
    }
    submission_record_obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(
        {**submission_record_obj, "canonical_json_hash": None}
    )
    validate_against_repo_schema_v1(
        submission_record_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_record.v1.schema.json",
    )
    _write_json(local_submission_record_path, submission_record_obj)

    advisory_submission = dict(package_obj.get("advisory_submission") or {})
    execution_intent_id = str(advisory_submission.get("execution_intent_id") or package_obj.get("intent_id") or "").strip()
    intent_hash = str(advisory_submission.get("promotion_idempotency_key") or "").strip()
    if not execution_intent_id:
        raise SystemExit(f"FAIL: READINESS_EXECUTION_INTENT_ID_MISSING:{execution_package_path}")
    selected_order_plan_ref = dict(package_obj.get("selected_order_plan_ref") or {})
    selected_order_plan_path = Path(str(selected_order_plan_ref.get("path") or "")).resolve()
    if not selected_order_plan_path.exists() or not selected_order_plan_path.is_file():
        raise SystemExit(f"FAIL: READINESS_SELECTED_ORDER_PLAN_MISSING:{selected_order_plan_path}")
    selected_order_plan = _read_json(selected_order_plan_path)
    synthetic_seed = {
        "plan": selected_order_plan,
        "intent_id": execution_intent_id,
        "intent_sha256": str(selected_order_plan.get("intent_sha256") or intent_hash or ""),
        "engine_id": str(selected_order_plan.get("engine_id") or ""),
        "symbol": str(selected_order_plan.get("symbol") or ""),
    }
    execution_intent = _build_execution_intent_for_submit_path(
        day=day,
        produced_utc=produced_utc,
        ib_account=ib_account,
        seed=synthetic_seed,
        execution_intent_id_override=execution_intent_id,
        intent_hash_override=intent_hash,
    )
    _seed_portfolio_authorization_for_submit(
        execution_intent=execution_intent,
        eval_time_utc=produced_utc,
    )
    return {
        "execution_package_path": local_package_path,
        "submission_record_path": local_submission_record_path,
        "submission_id": str(pair["submission_id"]),
    }


def _seed_portfolio_authorization_for_submit(*, execution_intent: ExecutionIntentV1, eval_time_utc: str) -> None:
    output_root = advisor_runtime_root()
    execution_intent_obj = execution_intent.to_dict()
    execution_intent_artifact_path = execution_intent_path_v1(
        output_root,
        execution_intent.household_id,
        execution_intent.execution_intent_id,
    )
    write_advisory_immutable_json_v1(execution_intent_artifact_path, execution_intent_obj)
    validate_against_repo_schema_v1(
        execution_intent_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/ADVISORY/execution_intent.v1.schema.json",
    )
    allowed_action = {
        "action_key": trade_action_key_v1(execution_intent=execution_intent),
        "execution_intent_id": execution_intent.execution_intent_id,
        "account_id": execution_intent.account_id,
        "symbol": str(execution_intent.instrument.get("symbol") or "").strip().upper(),
        "side": str(execution_intent.side or "").strip().upper(),
        "quantity_shares": int(execution_intent.quantity_shares),
        "max_notional_cents": "999999999999",
        "reason_codes": ["AUTH_ACTION_EXPLICITLY_ALLOWED"],
    }
    day = str(execution_intent.day_utc)
    portfolio_authorization_obj = {
        "schema_id": "portfolio_authorization",
        "schema_version": "v1",
        "record_id": execution_intent.execution_intent_id,
        "portfolio_authorization_id": execution_intent.execution_intent_id,
        "household_id": execution_intent.household_id,
        "execution_intent_id": execution_intent.execution_intent_id,
        "snapshot_refs": {
            "compiled_constraints_id": "readiness-proof",
            "allocation_plan_id": "readiness-proof",
            "risk_envelope_id": "readiness-proof",
            "tax_adjudicated_rebalance_id": "readiness-proof",
        },
        "valid_from": f"{day}T00:00:00Z",
        "valid_until": f"{day}T23:59:59Z",
        "authorization_scope": "PAPER_SUBMIT",
        "allowed_actions": [allowed_action],
        "blocked_actions": [],
        "max_incremental_deployment": "1.0",
        "required_prerequisite_actions": [],
        "account_route_permissions": [
            {"account_id": execution_intent.account_id, "route_allowed": True},
        ],
        "emergency_mode": False,
        "reason_codes": ["READINESS_PROOF_AUTH_SEED"],
        "stale_if_older_than_seconds": 86400,
    }
    validate_against_repo_schema_v1(
        portfolio_authorization_obj,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_authorization.v1.schema.json",
    )
    portfolio_authorization_artifact_path = portfolio_authorization_path_v1(
        output_root,
        execution_intent.household_id,
        execution_intent.execution_intent_id,
    )
    write_advisory_immutable_json_v1(portfolio_authorization_artifact_path, portfolio_authorization_obj)


def _call(script: Path, args: list[str], *, env: Dict[str, str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [sys.executable, str(script)] + args,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(REPO_ROOT),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise SystemExit(f"FAIL: TOOL_NONZERO: {script.name}: {detail}")
    return completed


def _seed_post_submit_truth(*, truth_root: Path, day: str, produced_utc: str, submission_dir: Path, seed: Dict[str, Any]) -> Dict[str, Any]:
    broker_submission = _read_json(submission_dir / "broker_submission_record.v2.json")
    submission_id = str(broker_submission.get("submission_id") or submission_dir.name).strip()
    binding_hash = str(broker_submission.get("binding_hash") or "").strip()
    if not submission_id or not binding_hash:
        raise SystemExit("FAIL: SUBMISSION_OUTPUT_INCOMPLETE")
    _write_json(
        truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json",
        {
            "schema_id": "capital_authority_allocation",
            "schema_version": "v1",
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"module": "run_paper_day_readiness_proof_v1.py"},
            "input_manifest": [],
            "status": "OK",
        },
    )
    _write_json(
        truth_root / "execution_stream_v1" / day / f"{_hx('6')}.execution_event_stream_record.v1.json",
        {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "git_sha": "proof", "module": "run_paper_day_readiness_proof_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": seed["engine_id"],
            "source_intent_id": seed["intent_id"],
            "intent_sha256": seed["intent_sha256"],
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "event_type": "EXEC_DETAILS",
            "event_time_utc": produced_utc,
            "observed_at_utc": produced_utc,
            "broker_ids": {"order_id": 101, "perm_id": 202},
            "order_state": {"status": "FILLED", "filled_qty": 1, "remaining_qty": 0, "avg_fill_price": "500.25"},
            "fill": {"fill_qty": 1, "fill_price": "500.25", "commission": "1.00", "currency": "USD"},
            "canonical_json_hash": _hx("7"),
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day / f"{submission_id}.fill_ledger.v1.json",
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "git_sha": "proof", "module": "run_paper_day_readiness_proof_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": seed["engine_id"],
            "source_intent_id": seed["intent_id"],
            "intent_sha256": seed["intent_sha256"],
            "order_qty": 1,
            "filled_qty": 1,
            "remaining_qty": 0,
            "avg_fill_price_weighted": "500.25",
        },
    )
    _write_json(
        truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json",
        {"schema_id": "positions_snapshot", "schema_version": "v2"},
    )
    _write_json(
        truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json",
        {
            "schema_id": "gate_stack_verdict",
            "schema_version": "v1",
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "proof", "module": "run_paper_day_readiness_proof_v1.py", "git_sha": "proof"},
            "status": "FAIL",
            "blocking_class": "CLASS1_SYSTEM_HARD_STOP",
            "reason_codes": [],
            "input_manifest": [],
            "gates": [
                {"gate_id": "feed_attestation_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/feed.json", "artifact_sha256": _hx("8"), "reason_codes": []},
                {"gate_id": "heartbeat_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/hb.json", "artifact_sha256": _hx("9"), "reason_codes": []},
                {"gate_id": "correlation_envelope_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/corr.json", "artifact_sha256": _hx("a"), "reason_codes": []},
                {"gate_id": "replay_certification_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "PASS", "artifact_path": "/tmp/fake/replay.json", "artifact_sha256": _hx("b"), "reason_codes": []},
                {"gate_id": "capital_risk_envelope_v2", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": "/tmp/fake/capital.json", "artifact_sha256": _hx("c"), "reason_codes": ["NAV_MISSING"]},
                {"gate_id": "liquidity_slippage_gate_v1", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": "/tmp/fake/liquidity.json", "artifact_sha256": _hx("d"), "reason_codes": ["LIQPOL_MISSING_NAV"]},
                {"gate_id": "operator_daily_gate_v3", "gate_class": "CLASS1_SYSTEM_HARD_STOP", "required": True, "blocking": True, "status": "FAIL", "artifact_path": "/tmp/fake/operator.json", "artifact_sha256": _hx("e"), "reason_codes": ["MISSING_RECONCILIATION_REPORT_V3"]},
            ],
        },
    )
    return {
        "submission_id": submission_id,
    }


def _write_authority_registry(*, advisor_output_root: Path, day: str, produced_utc: str) -> Path:
    env = metadata_envelope_v1(
        produced_utc=produced_utc,
        day_utc=day,
        mode="PAPER",
        source_artifact_refs=[],
        artifact_family="authority_registry_v1",
    )
    path = advisor_output_root / "PAPER" / "reports" / "authority_registry_v1" / day / "authority_registry.v1.json"
    _write_json(path, build_authority_registry(envelope=env).to_dict())
    return path


def _write_publication_inputs(root: Path, *, day: str, produced_utc: str) -> Dict[str, Path]:
    artifact_path = root / "planning_snapshot.json"
    semantic_path = root / "semantic_reconciliation_report.json"
    _write_json(
        artifact_path,
        {
            "schema_id": "planning_snapshot",
            "authority_class": "fact_authority",
            "support_status": "fully_supported",
            "planning_snapshot_id": "planning-snapshot-1",
        },
    )
    _write_json(
        semantic_path,
        {
            "schema_id": "semantic_reconciliation_report",
            "schema_version": "v1",
            "authority_class": "policy_authority",
            "support_status": "fully_supported",
            "produced_utc": produced_utc,
            "run_id": "semantic-report-1",
            "planning_snapshot_id": "planning-snapshot-1",
            "advisory_packet_id": "advisory-packet-1",
            "day_utc": day,
            "checks": [],
            "overall_status": "pass",
        },
    )
    return {"artifact": artifact_path, "semantic": semantic_path}


def _write_promotion_inputs(root: Path, *, produced_utc: str) -> Dict[str, Path]:
    candidate = PromotionCandidateV1(
        schema_id="promotion_candidate",
        schema_version="v1",
        produced_utc=produced_utc,
        run_id="candidate-run-1",
        candidate_id="candidate1",
        proposal_id="proposal1",
        planning_snapshot_id="planning-snapshot-1",
        decision_plan_id="decision-plan-1",
        candidate_status="candidate",
        candidate_class="withdrawal_candidate",
        source_account="paper-account",
        proposed_amount_cents=1000,
        periodicity="monthly",
        source_artifact_refs=("proposal_id:proposal1",),
        notes=("candidate note",),
    )
    review = PromotionReviewV1(
        schema_id="promotion_review",
        schema_version="v1",
        produced_utc=produced_utc,
        run_id="review-run-1",
        review_id="review1",
        candidate_id="candidate1",
        review_status="review_required",
        reason_codes=("REQUIRES_MANUAL_REVIEW",),
        source_artifact_refs=("candidate_id:candidate1",),
        notes=("review note",),
    )
    manual = PromotionManualReviewV1(
        schema_id="promotion_manual_review",
        schema_version="v1",
        produced_utc=produced_utc,
        run_id="manual-run-1",
        manual_review_id="manual1",
        candidate_id="candidate1",
        review_id="review1",
        manual_review_status="approved_for_future_promotion",
        operator_id="operator1",
        operator_notes="approved after review",
        source_artifact_refs=("review_id:review1",),
        selection_basis="approved_for_future_promotion",
    )
    candidate_path = root / "promotion_candidate.json"
    review_path = root / "promotion_review.json"
    manual_path = root / "promotion_manual_review.json"
    _write_json(candidate_path, candidate.to_dict())
    _write_json(review_path, review.to_dict())
    _write_json(manual_path, manual.to_dict())
    return {"candidate": candidate_path, "review": review_path, "manual": manual_path}


def run_readiness_proof(*, proof_root: Path, day: str, produced_utc: str, ib_account: str) -> Dict[str, str]:
    _require_repo_prerequisites(proof_root=proof_root)
    if not str(proof_root).startswith("/tmp/constellation_2_foundation/"):
        raise SystemExit(f"FAIL: proof_root must remain under /tmp/constellation_2_foundation: {proof_root}")
    paths = _proof_paths(proof_root)
    shutil.rmtree(paths["proof_root"], ignore_errors=True)
    paths["truth_root"].mkdir(parents=True, exist_ok=True)
    for stale_dir in [
        paths["advisor_output_root"] / "PAPER" / "reports" / "authority_registry_v1" / day,
        paths["advisor_output_root"] / "PAPER" / "publication_gate_result_v1" / day,
        paths["advisor_output_root"] / "PAPER" / "promotion_gate_result_v1" / day,
    ]:
        shutil.rmtree(stale_dir, ignore_errors=True)
    paths["advisor_output_root"].mkdir(parents=True, exist_ok=True)
    seed = _seed_submit_prerequisites(
        truth_root=paths["truth_root"],
        day=day,
        produced_utc=produced_utc,
        ib_account=ib_account,
    )
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(paths["truth_root"])
    submit_inputs = _materialize_submit_interface_inputs(
        truth_root=paths["truth_root"],
        day=day,
        produced_utc=produced_utc,
        ib_account=ib_account,
        seed=seed,
    )
    submit_script = REPO_ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py"
    _call(
        submit_script,
        [
            "--eval_time_utc",
            produced_utc,
            "--execution_package_path",
            str(submit_inputs["execution_package_path"]),
            "--submission_record_path",
            str(submit_inputs["submission_record_path"]),
            "--risk_budget",
            str((REPO_ROOT / "constellation_2" / "phaseD" / "inputs" / "sample_risk_budget.v1.json").resolve()),
            "--ib_host",
            "127.0.0.1",
            "--ib_port",
            "4002",
            "--ib_client_id",
            "7",
            "--ib_account",
            ib_account,
            "--dry_run",
            "YES",
            "--submissions_root_override",
            str((paths["truth_root"] / "execution_evidence_v1" / "submissions").resolve()),
        ],
        env=env,
    )
    submissions_day = paths["truth_root"] / "execution_evidence_v1" / "submissions" / day
    submission_dirs = sorted([p for p in submissions_day.iterdir() if p.is_dir()]) if submissions_day.exists() else []
    if len(submission_dirs) != 1:
        raise SystemExit(f"FAIL: expected exactly one submission dir in proof path: {submissions_day}")
    submission_dir = submission_dirs[0]
    if not (submission_dir / "broker_submission_record.v2.json").exists():
        raise SystemExit(f"FAIL: missing broker_submission_record.v2.json: {submission_dir}")
    broker_submission_record = (submission_dir / "broker_submission_record.v2.json").resolve()
    post_submit = _seed_post_submit_truth(
        truth_root=paths["truth_root"],
        day=day,
        produced_utc=produced_utc,
        submission_dir=submission_dir,
        seed=seed,
    )
    plane_produced_utc = f"{day}T00:00:00Z"
    _call(REPO_ROOT / "ops" / "tools" / "run_execution_truth_plane_v1.py", ["--day_utc", day, "--truth_root", str(paths["truth_root"]), "--produced_utc", plane_produced_utc], env=env)
    _call(REPO_ROOT / "ops" / "tools" / "run_gate_authority_plane_v1.py", ["--day_utc", day, "--truth_root", str(paths["truth_root"]), "--produced_utc", plane_produced_utc, "--mode", "PAPER"], env=env)
    _call(REPO_ROOT / "ops" / "tools" / "run_runtime_trace_bundle_v1.py", ["--day_utc", day, "--truth_root", str(paths["truth_root"]), "--submission_id", post_submit["submission_id"]], env=env)
    _call(REPO_ROOT / "ops" / "tools" / "run_runtime_replay_day_v1.py", ["--day_utc", day, "--source_truth_root", str(paths["truth_root"]), "--replay_truth_root", str(paths["replay_root"]), "--submission_id", post_submit["submission_id"]], env=env)
    _write_authority_registry(advisor_output_root=paths["advisor_output_root"], day=day, produced_utc=produced_utc)
    publication_inputs = _write_publication_inputs(paths["proof_root"], day=day, produced_utc=produced_utc)
    _call(
        REPO_ROOT / "ops" / "tools" / "run_publication_gate_v1.py",
        [
            "--artifact_json",
            str(publication_inputs["artifact"]),
            "--semantic_report_json",
            str(publication_inputs["semantic"]),
            "--mode",
            "PAPER",
            "--day_utc",
            day,
            "--produced_utc",
            produced_utc,
            "--output_root",
            str(paths["advisor_output_root"]),
        ],
        env=env,
    )
    promotion_inputs = _write_promotion_inputs(paths["proof_root"], produced_utc=produced_utc)
    _call(
        REPO_ROOT / "ops" / "tools" / "run_promotion_gate_v1.py",
        [
            "--promotion_candidate_json",
            str(promotion_inputs["candidate"]),
            "--promotion_review_json",
            str(promotion_inputs["review"]),
            "--promotion_manual_review_json",
            str(promotion_inputs["manual"]),
            "--mode",
            "PAPER",
            "--day_utc",
            day,
            "--produced_utc",
            produced_utc,
            "--output_root",
            str(paths["advisor_output_root"]),
        ],
        env=env,
    )
    return {
        "proof_root": str(paths["proof_root"]),
        "truth_root": str(paths["truth_root"]),
        "replay_root": str(paths["replay_root"]),
        "advisor_output_root": str(paths["advisor_output_root"]),
        "submission_id": post_submit["submission_id"],
        "broker_submission_record": str(broker_submission_record),
        "execution_truth_gap": str((paths["truth_root"] / "reports" / "execution_completion_gap_report_v1" / day / "execution_completion_gap_report.v1.json").resolve()),
        "authorization_verdict": str((paths["truth_root"] / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()),
        "runtime_trace_bundle": str((paths["truth_root"] / "reports" / "runtime_trace_bundle_v1" / day / f"{post_submit['submission_id']}.runtime_trace_bundle.v1.json").resolve()),
        "replay_manifest": str((paths["replay_root"] / "reports" / "replay_manifest_v1" / day / f"{post_submit['submission_id']}.replay_manifest.v1.json").resolve()),
        "publication_gate_result": str((paths["advisor_output_root"] / "PAPER" / "publication_gate_result_v1" / day / "publication_gate_result.v1.json").resolve()),
        "promotion_gate_result": str((paths["advisor_output_root"] / "PAPER" / "promotion_gate_result_v1" / day / "promotion_gate_result.v1.json").resolve()),
        "authority_registry": str((paths["advisor_output_root"] / "PAPER" / "reports" / "authority_registry_v1" / day / "authority_registry.v1.json").resolve()),
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_paper_day_readiness_proof_v1")
    ap.add_argument("--proof_root", default=str(DEFAULT_PROOF_ROOT))
    ap.add_argument("--day_utc", default=DEFAULT_DAY)
    ap.add_argument("--produced_utc", default=DEFAULT_PRODUCED_UTC)
    ap.add_argument("--ib_account", default=DEFAULT_IB_ACCOUNT)
    args = ap.parse_args()
    result = run_readiness_proof(
        proof_root=Path(str(args.proof_root).strip()).expanduser().resolve(),
        day=str(args.day_utc).strip(),
        produced_utc=str(args.produced_utc).strip(),
        ib_account=str(args.ib_account).strip(),
    )
    print("OK: PAPER_DAY_READINESS_PROOF_V1 " + " ".join(f"{k}={v}" for k, v in sorted(result.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
