from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    CLOSURE_STATE_DEGRADED,
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_frozen_decision_input_bundle_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    build_machine_blocker_envelope_v1,
    get_constitutional_artifact_contract_v1,
    validate_governed_artifact_payload_v1,
    validate_read_model_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.governed_evaluation_v1"
POLICY_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json"

POLICY_SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/evaluation_policy_snapshot.v1.schema.json"
SLEEVE_PERFORMANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_performance_truth.v1.schema.json"
SLEEVE_VALIDITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_validity_state.v1.schema.json"
SLEEVE_EVALUATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_evaluation_state.v1.schema.json"
PORTFOLIO_PERFORMANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_performance_truth.v1.schema.json"
PORTFOLIO_VALIDITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_validity_state.v1.schema.json"
PORTFOLIO_EVALUATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_evaluation_state.v1.schema.json"
SLEEVE_ACTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_governance_action_state.v1.schema.json"
PORTFOLIO_ACTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_governance_action_state.v1.schema.json"
WEEKLY_SCORECARD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EVALUATION/weekly_scorecard_view.v1.schema.json"

ROLE_DIRECTIONAL_EQUITY = "directional_equity"
ROLE_DEFENSIVE = "defensive"
ROLE_NEUTRAL = "neutral_absolute_return"
ROLE_DIVERSIFIER = "diversifier"

VALIDITY_INSUFFICIENT_SAMPLE = "insufficient_sample"
VALIDITY_EXECUTION_CONTAMINATED = "execution_contaminated"
VALIDITY_BENCHMARK_NOT_APPLICABLE = "benchmark_not_applicable"
VALIDITY_BENCHMARK_INVALID = "benchmark_invalid"
VALIDITY_DATA_INCOMPLETE = "data_incomplete"
VALIDITY_PROVISIONAL = "provisional"
VALIDITY_VALID = "valid"

EDGE_POSITIVE_UNVALIDATED = "edge_positive_unvalidated"
EDGE_VALIDATED = "edge_validated"
EDGE_DETERIORATING = "edge_deteriorating"
EDGE_NEGATIVE = "edge_negative"
NOT_ENOUGH_EVIDENCE = "not_enough_evidence"

STABILITY_STABLE = "stable"
STABILITY_UNSTABLE = "unstable"

RISK_WITHIN_POLICY = "risk_within_policy"
RISK_WARNING = "risk_warning"
RISK_BREACH = "risk_breach"


@dataclass(frozen=True)
class SleeveUpstreamV1:
    sleeve_id: str
    fact_ledger_path: Path
    snapshot_path: Path
    execution_truth_root: Path
    slippage_gate_path: Path
    exit_reconciliation_path: Path


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _repo_dirty_status_v1() -> str:
    proc = subprocess.run(["git", "-C", str(REPO_ROOT), "status", "--short"], capture_output=True, text=True, check=False)
    return "DIRTY" if str(proc.stdout or "").strip() else "CLEAN"


def _scorecard_producer_contract_v1(*, day_utc: str, output_path: Path, source_refs: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    stable = {
        "schema_version": "producer_contract.v1",
        "producer_name": WRITER_ID,
        "producer_command": f"materialize_governed_evaluation_day_v1(day_utc={_parse_day(day_utc)})",
        "code_version_git_commit": repo_git_sha_v1(),
        "source_dirty_status": _repo_dirty_status_v1(),
        "input_artifacts": [dict(row) for row in source_refs],
        "output_artifacts": [{"path": str(output_path.resolve())}],
        "schema_versions": {"weekly_scorecard_view_v1": "v1"},
    }
    return {
        **stable,
        "generated_at_utc": _now_utc(),
        "deterministic_fingerprint": hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest(),
    }


def _parse_day(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise ValueError(f"BAD_DAY_UTC:{day!r}")
    return day


def _prev_day(day_utc: str) -> str:
    day_obj = date.fromisoformat(_parse_day(day_utc))
    return (day_obj - timedelta(days=1)).isoformat()


def _decimal(value: Any, *, default: str = "0") -> Decimal:
    text = str(value if value is not None else default).strip()
    if not text:
        text = default
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal(default)


def _decimal_text(value: Decimal, *, places: str = "0.00000000") -> str:
    return format(value.quantize(Decimal(places)), "f")


def _hash_obj(obj: Mapping[str, Any]) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    return read_json_object_v1(path)


def _market_snapshot_close(path: Path) -> Decimal | None:
    if not path.exists() or not path.is_file():
        return None
    payload = _read_json(path)
    raw = payload.get("close")
    if raw is None or str(raw).strip() == "":
        return None
    return _decimal(raw)


def _optional_metric(*, value: str | None, status: str, reason_codes: Sequence[str] | None = None) -> Dict[str, Any]:
    return {
        "value": value,
        "status": status,
        "reason_codes": [str(code).strip() for code in (reason_codes or []) if str(code).strip()],
    }


def _optional_metric_from_decimal(*, value: Decimal | None, reason_codes: Sequence[str] | None = None) -> Dict[str, Any]:
    if value is None:
        return _optional_metric(value=None, status="UNAVAILABLE", reason_codes=reason_codes)
    return _optional_metric(value=_decimal_text(value), status="AVAILABLE", reason_codes=reason_codes)


def _state_realized_net_pnl(state_payload: Mapping[str, Any]) -> Decimal:
    fills = state_payload.get("incorporated_fills") if isinstance(state_payload.get("incorporated_fills"), list) else []
    realized_gross = Decimal("0")
    fees = Decimal("0")
    position_qty = Decimal("0")
    cost_basis = Decimal("0")
    ordered: List[Mapping[str, Any]] = []
    for row in fills:
        if not isinstance(row, dict):
            continue
        ordered.append(row)
    ordered.sort(key=lambda row: str(row.get("observed_utc") or "1970-01-01T00:00:00Z"))
    for fill in ordered:
        side = str(fill.get("side") or "").strip().upper()
        qty = _decimal(fill.get("fill_quantity"))
        if qty <= 0:
            continue
        signed_qty = qty
        if side in {"SELL", "SLD"}:
            signed_qty = Decimal("0") - qty
        elif side not in {"BUY", "BOT"}:
            continue
        price = _decimal(fill.get("fill_price"))
        fees += _decimal(fill.get("commission"))
        if position_qty == 0 or (position_qty > 0 and signed_qty > 0) or (position_qty < 0 and signed_qty < 0):
            total_qty = abs(position_qty) + abs(signed_qty)
            if total_qty > 0:
                weighted_cost = (abs(position_qty) * cost_basis) + (abs(signed_qty) * price)
                cost_basis = weighted_cost / total_qty
            position_qty += signed_qty
            continue
        closing_qty = min(abs(position_qty), abs(signed_qty))
        if position_qty > 0 and signed_qty < 0:
            realized_gross += (price - cost_basis) * closing_qty
        elif position_qty < 0 and signed_qty > 0:
            realized_gross += (cost_basis - price) * closing_qty
        position_qty += signed_qty
        if position_qty == 0:
            cost_basis = Decimal("0")
        elif abs(signed_qty) > closing_qty:
            cost_basis = price
    return realized_gross - fees


def _trade_outcome_truth_from_fact_ledger(fact_ledger: Mapping[str, Any]) -> Dict[str, Any]:
    trade_rows = fact_ledger.get("trade_facts") if isinstance(fact_ledger.get("trade_facts"), list) else []
    net_pnls: List[Decimal] = []
    reason_codes: List[str] = []
    for row in trade_rows:
        if not isinstance(row, dict):
            continue
        if row.get("included_in_metrics") is not True:
            continue
        measurement_class = str(row.get("measurement_class") or "").strip().upper()
        if measurement_class != "NATIVE_ENTRY":
            continue
        state_path = Path(str(row.get("incorporated_state_path") or "")).resolve()
        if not state_path.exists() or not state_path.is_file():
            reason_codes.append("TRADE_OUTCOME_STATE_MISSING")
            continue
        try:
            state_payload = _read_json(state_path)
        except Exception:
            reason_codes.append("TRADE_OUTCOME_STATE_INVALID")
            continue
        net_pnls.append(_state_realized_net_pnl(state_payload))
    winning = [value for value in net_pnls if value > 0]
    losing = [value for value in net_pnls if value < 0]
    flat = [value for value in net_pnls if value == 0]
    trade_count = len(net_pnls)
    avg_win = (sum(winning, Decimal("0")) / Decimal(len(winning))) if winning else None
    avg_loss = (sum(abs(value) for value in losing) / Decimal(len(losing))) if losing else None
    win_rate = (Decimal(len(winning)) / Decimal(trade_count)) if trade_count > 0 else None
    payoff_ratio = (avg_win / avg_loss) if (avg_win is not None and avg_loss is not None and avg_loss > 0) else None
    if trade_count <= 0:
        reason_codes.append("NO_TRADES")
    elif not winning:
        reason_codes.append("NO_WINNING_TRADES")
    elif not losing:
        reason_codes.append("NO_LOSING_TRADES")
    return {
        "status": "ACTIVE" if trade_count > 0 else "NO_TRADES",
        "trade_count": trade_count,
        "winning_trade_count": len(winning),
        "losing_trade_count": len(losing),
        "flat_trade_count": len(flat),
        "win_rate": _optional_metric_from_decimal(value=win_rate, reason_codes=["NO_TRADES"] if trade_count <= 0 else []),
        "avg_win": _optional_metric_from_decimal(
            value=avg_win,
            reason_codes=["NO_WINNING_TRADES"] if not winning else [],
        ),
        "avg_loss": _optional_metric_from_decimal(
            value=avg_loss,
            reason_codes=["NO_LOSING_TRADES"] if not losing else [],
        ),
        "payoff_ratio": _optional_metric_from_decimal(
            value=payoff_ratio,
            reason_codes=["PAYOFF_RATIO_UNAVAILABLE"] if payoff_ratio is None else [],
        ),
        "reason_codes": sorted(set(code for code in reason_codes if code)),
    }


def _registry_finality(artifact_id: str) -> str:
    contract = get_constitutional_artifact_contract_v1(REPO_ROOT, artifact_id)
    return str(contract.get("initial_finality_state") or FINALITY_FINALIZED).strip() or FINALITY_FINALIZED


def _governed_ref(artifact_id: str, path: Path) -> Dict[str, Any]:
    return build_governed_dependency_ref_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        path=str(path.resolve()),
        sha256=sha256_file_v1(path.resolve()),
        finality_state=_registry_finality(artifact_id),
    )


def _plain_ref(artifact_id: str, path: Path) -> Dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "path": str(path.resolve()),
        "sha256": sha256_file_v1(path.resolve()),
    }


def _write_governed(
    *,
    artifact_id: str,
    schema_relpath: str,
    path: Path,
    payload: Dict[str, Any],
) -> SurfaceRefV1:
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        payload=payload,
    )
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=schema_relpath,
        volatile_field_names=("produced_utc",),
    )


def _resolve_execution_truth_root(*, truth_root: Path, execution_sleeve_id: str, mode: str) -> Path:
    registry = _read_json((REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve())
    for row in registry.get("sleeves") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("sleeve_id") or "").strip().upper() != str(execution_sleeve_id).strip().upper():
            continue
        if str(row.get("mode") or "").strip().upper() != str(mode).strip().upper():
            continue
        partition = str(row.get("truth_partition") or "").strip()
        if not partition:
            break
        root = (truth_root.resolve().parent / partition).resolve()
        if root.exists() and root.is_dir():
            return root
    candidate = (truth_root.resolve().parent / "truth_sleeves" / str(execution_sleeve_id).strip().upper() / str(mode).strip().upper()).resolve()
    return candidate


def _latest_sleeve_snapshot_path(*, execution_truth_root: Path, day_utc: str, sleeve_id: str) -> Path:
    base = (
        execution_truth_root
        / "reports"
        / "sleeve_edge_snapshot_v1"
        / _parse_day(day_utc)
        / str(sleeve_id).strip()
    ).resolve()
    candidates = sorted(base.glob("*/sleeve_edge_snapshot.v1.json"))
    if not candidates:
        raise ValueError(f"MISSING_SLEEVE_EDGE_SNAPSHOT:{sleeve_id}:{day_utc}")
    return candidates[-1].resolve()


def _discover_sleeve_ids(*, execution_truth_root: Path, day_utc: str) -> List[str]:
    base = (execution_truth_root / "reports" / "sleeve_edge_snapshot_v1" / _parse_day(day_utc)).resolve()
    discovered: List[str] = []
    if base.exists() and base.is_dir():
        discovered.extend(child.name for child in base.iterdir() if child.is_dir())
    if discovered:
        return sorted(set(item for item in discovered if item))
    policy_path = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
    if not policy_path.exists() or not policy_path.is_file():
        return []
    policy = _read_json(policy_path)
    sleeves = policy.get("sleeves") if isinstance(policy.get("sleeves"), list) else []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip()
        if sleeve_id:
            discovered.append(sleeve_id)
    return sorted(set(discovered))


def _resolve_latest_economic_state_build(*, truth_root: Path, day_utc: str) -> Path:
    root = (truth_root / "reports" / "economic_state_build_v1" / _parse_day(day_utc)).resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"MISSING_ECONOMIC_STATE_BUILD:{day_utc}")
    candidates = sorted(root.glob("*/economic_state_build.v1.json"))
    if not candidates:
        raise ValueError(f"MISSING_ECONOMIC_STATE_BUILD:{day_utc}")
    return candidates[-1].resolve()


def _role_for_sleeve(policy: Mapping[str, Any], sleeve_id: str) -> str:
    assignments = policy.get("sleeve_role_assignments") if isinstance(policy.get("sleeve_role_assignments"), dict) else {}
    role = str(assignments.get(str(sleeve_id).strip()) or "").strip()
    return role or ROLE_NEUTRAL


def _benchmark_policy_for_role(policy: Mapping[str, Any], role: str) -> Dict[str, Any]:
    rows = policy.get("benchmark_role_policy") if isinstance(policy.get("benchmark_role_policy"), dict) else {}
    row = rows.get(role)
    if isinstance(row, dict):
        return dict(row)
    return {"benchmark_mode": "cash_baseline", "benchmark_symbol": "", "benchmark_applicability": "not_applicable"}


def _build_policy_snapshot_payload(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "evaluation_policy_snapshot_v1", WRITER_ID)
    registry_path = (REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve()
    registry_payload = _read_json(registry_path)
    produced_utc = _now_utc()
    snapshot_id = _hash_obj(
        {
            "artifact_id": "evaluation_policy_snapshot_v1",
            "day_utc": _parse_day(day_utc),
            "registry_sha256": sha256_file_v1(registry_path),
        }
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="evaluation_policy_snapshot_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="evaluation_policy_snapshot_v1",
        declared_dependency_artifacts=[],
        dependency_refs=[],
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="evaluation_policy_snapshot_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="evaluation_policy_snapshot_v1",
        producer_id=WRITER_ID,
        generated_at_utc=produced_utc,
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"evaluation_policy_snapshot:{_parse_day(day_utc)}",
    )
    return {
        "schema_id": "C2_EVALUATION_POLICY_SNAPSHOT_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": produced_utc,
        "policy_snapshot_id": snapshot_id,
        "policy_version": str(registry_payload.get("policy_version") or "").strip(),
        "annual_target_return": str(registry_payload.get("annual_target_return") or "").strip(),
        "window_policy": dict(registry_payload.get("window_policy") or {}),
        "evidence_assembly_policy": dict(registry_payload.get("evidence_assembly_policy") or {}),
        "outcome_attribution_policy": dict(registry_payload.get("outcome_attribution_policy") or {}),
        "sleeve_edge_measurement_policy": dict(registry_payload.get("sleeve_edge_measurement_policy") or {}),
        "benchmark_role_policy": dict(registry_payload.get("benchmark_role_policy") or {}),
        "sleeve_role_assignments": dict(registry_payload.get("sleeve_role_assignments") or {}),
        "allocation_governance_policy": dict(registry_payload.get("allocation_governance_policy") or {}),
        "governance_action_policy": dict(registry_payload.get("governance_action_policy") or {}),
        "operative_control_policy": dict(registry_payload.get("operative_control_policy") or {}),
        "source_registry_ref": {
            "path": str(registry_path),
            "sha256": sha256_file_v1(registry_path),
        },
        "reason_codes": [],
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _evaluation_policy_snapshot_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root.resolve()
        / "reports"
        / "evaluation_policy_snapshot_v1"
        / _parse_day(day_utc)
        / "evaluation_policy_snapshot.v1.json"
    ).resolve()


def write_evaluation_policy_snapshot_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    payload = _build_policy_snapshot_payload(truth_root=truth_root, day_utc=day_utc)
    return _write_governed(
        artifact_id="evaluation_policy_snapshot_v1",
        schema_relpath=POLICY_SNAPSHOT_SCHEMA,
        path=_evaluation_policy_snapshot_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
    )


def _build_benchmark_truth(*, canonical_truth_root: Path, execution_truth_root: Path, day_utc: str, role: str, role_policy: Mapping[str, Any]) -> Dict[str, Any]:
    benchmark_mode = str(role_policy.get("benchmark_mode") or "").strip()
    benchmark_symbol = str(role_policy.get("benchmark_symbol") or "").strip().upper()
    applicability = str(role_policy.get("benchmark_applicability") or "").strip()
    if applicability == "not_applicable":
        return {
            "benchmark_role": role,
            "benchmark_mode": benchmark_mode,
            "benchmark_id": "",
            "state": VALIDITY_BENCHMARK_NOT_APPLICABLE,
            "daily_return": _optional_metric(value=None, status="NOT_APPLICABLE"),
            "reason_codes": ["BENCHMARK_NOT_APPLICABLE_FOR_ROLE"],
        }
    if benchmark_mode != "external_symbol" or not benchmark_symbol:
        return {
            "benchmark_role": role,
            "benchmark_mode": benchmark_mode,
            "benchmark_id": "",
            "state": VALIDITY_BENCHMARK_INVALID,
            "daily_return": _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["BENCHMARK_POLICY_UNSUPPORTED"]),
            "reason_codes": ["BENCHMARK_POLICY_UNSUPPORTED"],
        }
    current_paths = [
        execution_truth_root / "market_data_snapshot_v1" / "snapshots" / _parse_day(day_utc) / f"{benchmark_symbol}.market_data_snapshot.v1.json",
        canonical_truth_root / "market_data_snapshot_v1" / "snapshots" / _parse_day(day_utc) / f"{benchmark_symbol}.market_data_snapshot.v1.json",
    ]
    prev_paths = [
        execution_truth_root / "market_data_snapshot_v1" / "snapshots" / _prev_day(day_utc) / f"{benchmark_symbol}.market_data_snapshot.v1.json",
        canonical_truth_root / "market_data_snapshot_v1" / "snapshots" / _prev_day(day_utc) / f"{benchmark_symbol}.market_data_snapshot.v1.json",
    ]
    current_close = next((value for value in (_market_snapshot_close(path.resolve()) for path in current_paths) if value is not None), None)
    prev_close = next((value for value in (_market_snapshot_close(path.resolve()) for path in prev_paths) if value is not None), None)
    if current_close is None or prev_close is None or prev_close <= 0:
        return {
            "benchmark_role": role,
            "benchmark_mode": benchmark_mode,
            "benchmark_id": benchmark_symbol,
            "state": VALIDITY_BENCHMARK_INVALID,
            "daily_return": _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["BENCHMARK_SNAPSHOT_CLOSE_UNAVAILABLE"]),
            "reason_codes": ["BENCHMARK_SNAPSHOT_CLOSE_UNAVAILABLE"],
        }
    benchmark_return = (current_close / prev_close) - Decimal("1")
    return {
        "benchmark_role": role,
        "benchmark_mode": benchmark_mode,
        "benchmark_id": benchmark_symbol,
        "state": "benchmark_valid",
        "daily_return": _optional_metric(value=_decimal_text(benchmark_return), status="OK"),
        "reason_codes": [],
    }


def _resolve_sleeve_upstream(*, truth_root: Path, execution_sleeve_id: str, mode: str, day_utc: str, sleeve_id: str) -> SleeveUpstreamV1:
    execution_truth_root = _resolve_execution_truth_root(
        truth_root=truth_root,
        execution_sleeve_id=execution_sleeve_id,
        mode=mode,
    )
    snapshot_path = _latest_sleeve_snapshot_path(
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
    )
    snapshot = _read_json(snapshot_path)
    fact_ref = snapshot.get("fact_ledger_ref") if isinstance(snapshot.get("fact_ledger_ref"), dict) else {}
    fact_ledger_path = Path(str(fact_ref.get("artifact_path") or "")).resolve()
    slippage_gate_path = (execution_truth_root / "reports" / "liquidity_slippage_gate_v1" / _parse_day(day_utc) / "liquidity_slippage_gate.v1.json").resolve()
    exit_reconciliation_path = (execution_truth_root / "exit_reconciliation_v1" / _parse_day(day_utc) / "exit_reconciliation.v1.json").resolve()
    return SleeveUpstreamV1(
        sleeve_id=sleeve_id,
        fact_ledger_path=fact_ledger_path,
        snapshot_path=snapshot_path,
        execution_truth_root=execution_truth_root,
        slippage_gate_path=slippage_gate_path,
        exit_reconciliation_path=exit_reconciliation_path,
    )


def _sleeve_paths(*, execution_truth_root: Path, day_utc: str, sleeve_id: str) -> Dict[str, Path]:
    base = execution_truth_root / "reports"
    return {
        "performance": (base / "sleeve_performance_truth_v1" / _parse_day(day_utc) / sleeve_id / "sleeve_performance_truth.v1.json").resolve(),
        "validity": (base / "sleeve_validity_state_v1" / _parse_day(day_utc) / sleeve_id / "sleeve_validity_state.v1.json").resolve(),
        "evaluation": (base / "sleeve_evaluation_state_v1" / _parse_day(day_utc) / sleeve_id / "sleeve_evaluation_state.v1.json").resolve(),
        "action": (base / "sleeve_governance_action_state_v1" / _parse_day(day_utc) / sleeve_id / "sleeve_governance_action_state.v1.json").resolve(),
    }


def _portfolio_paths(*, truth_root: Path, day_utc: str) -> Dict[str, Path]:
    base = truth_root.resolve() / "reports"
    return {
        "performance": (base / "portfolio_performance_truth_v1" / _parse_day(day_utc) / "portfolio_performance_truth.v1.json").resolve(),
        "validity": (base / "portfolio_validity_state_v1" / _parse_day(day_utc) / "portfolio_validity_state.v1.json").resolve(),
        "evaluation": (base / "portfolio_evaluation_state_v1" / _parse_day(day_utc) / "portfolio_evaluation_state.v1.json").resolve(),
        "action": (base / "portfolio_governance_action_state_v1" / _parse_day(day_utc) / "portfolio_governance_action_state.v1.json").resolve(),
        "scorecard": (base / "weekly_scorecard_view_v1" / _parse_day(day_utc) / "weekly_scorecard_view.v1.json").resolve(),
    }


def _build_sleeve_performance_payload(
    *,
    truth_root: Path,
    upstream: SleeveUpstreamV1,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    policy_snapshot_ref: Mapping[str, Any],
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_performance_truth_v1", WRITER_ID)
    snapshot = _read_json(upstream.snapshot_path)
    fact_ledger = _read_json(upstream.fact_ledger_path)
    slippage_gate = _read_json(upstream.slippage_gate_path)
    exit_reconciliation = _read_json(upstream.exit_reconciliation_path)
    factual_metrics = snapshot.get("factual_metrics") if isinstance(snapshot.get("factual_metrics"), dict) else {}
    role = _role_for_sleeve(_read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve()), upstream.sleeve_id)
    role_policy = _benchmark_policy_for_role(_read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve()), role)
    benchmark_truth = _build_benchmark_truth(
        canonical_truth_root=truth_root,
        execution_truth_root=upstream.execution_truth_root,
        day_utc=day_utc,
        role=role,
        role_policy=role_policy,
    )
    native_count = int(factual_metrics.get("native_trade_count") or 0)
    adopted_count = int(factual_metrics.get("adopted_trade_count") or 0)
    sample_count = int(factual_metrics.get("sample_count") or 0)
    slippage_status = str(slippage_gate.get("status") or slippage_gate.get("gate_status") or "UNKNOWN").strip().upper()
    execution_completeness = factual_metrics.get("execution_data_completeness") if isinstance(factual_metrics.get("execution_data_completeness"), dict) else {}
    trade_outcome_truth = _trade_outcome_truth_from_fact_ledger(fact_ledger)
    performance_status = (
        "NO_TRADES"
        if sample_count <= 0
        else ("ACTIVE" if int(trade_outcome_truth.get("trade_count") or 0) > 0 else "NOT_ENOUGH_EVIDENCE")
    )
    reason_codes = sorted(
        set(
            [str(code).strip() for code in factual_metrics.get("invalidity_reasons") or [] if str(code).strip()]
            + [str(code).strip() for code in benchmark_truth.get("reason_codes") or [] if str(code).strip()]
            + [str(code).strip() for code in trade_outcome_truth.get("reason_codes") or [] if str(code).strip()]
        )
    )
    dependency_refs = [
        _governed_ref("sleeve_edge_fact_ledger_v1", upstream.fact_ledger_path),
        _governed_ref("sleeve_edge_snapshot_v1", upstream.snapshot_path),
        _governed_ref("liquidity_slippage_gate_v1", upstream.slippage_gate_path),
        _governed_ref("exit_reconciliation_v1", upstream.exit_reconciliation_path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_performance_truth_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_performance_truth_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_performance_truth_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_performance_truth_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_performance_truth:{_parse_day(day_utc)}:{upstream.sleeve_id}",
    )
    return {
        "schema_id": "C2_SLEEVE_PERFORMANCE_TRUTH_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": upstream.sleeve_id,
        "strategy_family": str(snapshot.get("strategy_family") or fact_ledger.get("strategy_family") or "").strip(),
        "performance_status": performance_status,
        "sample_trade_count": sample_count,
        "measurement_window": dict(snapshot.get("metric_window") or {}),
        "realized_pnl_truth": {
            "native_net_pnl": str(factual_metrics.get("native_net_pnl") or "0"),
            "native_gross_pnl": str(factual_metrics.get("native_gross_pnl") or "0"),
            "native_net_expectancy": str(factual_metrics.get("native_net_expectancy") or "0"),
            "native_recent_vs_baseline_drift": str(
                (
                    ((factual_metrics.get("native_recent_vs_baseline_drift") or {}) if isinstance(factual_metrics.get("native_recent_vs_baseline_drift"), dict) else {}).get("value")
                    or "0"
                )
            ),
            "adopted_net_pnl": str(factual_metrics.get("adopted_net_pnl") or "0"),
            "adopted_management_expectancy": str(factual_metrics.get("adopted_management_expectancy") or "0"),
            "fee_drag": str(factual_metrics.get("fee_drag") or "0"),
        },
        "drawdown_truth": {
            "native_realized_drawdown": str(factual_metrics.get("native_realized_drawdown") or "0"),
            "adopted_drawdown": str(factual_metrics.get("adopted_drawdown") or "0"),
        },
        "execution_truth": {
            "slippage_gate_status": slippage_status,
            "measured_slippage_drag": dict(factual_metrics.get("measured_slippage_drag") or {}),
            "execution_data_completeness": execution_completeness,
            "exit_reconciliation_status": str(exit_reconciliation.get("status") or "").strip(),
        },
        "benchmark_truth": benchmark_truth,
        "trade_attribution_truth": {
            "fact_input_hash": str(snapshot.get("fact_input_hash") or fact_ledger.get("fact_input_hash") or "").strip(),
            "native_trade_count": native_count,
            "adopted_trade_count": adopted_count,
            "unknown_attribution_count": int(factual_metrics.get("unknown_attribution_count") or 0),
            "included_trade_count": len(snapshot.get("included_trade_ids") or []),
            "excluded_trade_count": len(snapshot.get("excluded_trade_ids") or []),
        },
        "trade_outcome_truth": trade_outcome_truth,
        "legacy_measurement_refs": [
            _plain_ref("sleeve_edge_fact_ledger_v1", upstream.fact_ledger_path),
            _plain_ref("sleeve_edge_snapshot_v1", upstream.snapshot_path),
        ],
        "reason_codes": reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_sleeve_validity_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    performance_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_validity_state_v1", WRITER_ID)
    performance = performance_ref.payload
    policy = policy_ref.payload
    sample_count = int(performance.get("sample_trade_count") or 0)
    validity_policy = _read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve()).get("validity_policy") or {}
    provisional_min = int(validity_policy.get("provisional_min_sample_count") or 2)
    valid_min = int(validity_policy.get("valid_min_sample_count") or 5)
    benchmark_truth = performance.get("benchmark_truth") if isinstance(performance.get("benchmark_truth"), dict) else {}
    execution_truth = performance.get("execution_truth") if isinstance(performance.get("execution_truth"), dict) else {}
    execution_completeness = execution_truth.get("execution_data_completeness") if isinstance(execution_truth.get("execution_data_completeness"), dict) else {}
    contamination = (
        "execution_contaminated"
        if str(execution_truth.get("slippage_gate_status") or "").upper() not in {"PASS", "OK"}
        or str(execution_completeness.get("state") or "").upper() != "COMPLETE"
        else "execution_clean"
    )
    benchmark_state = str(benchmark_truth.get("state") or VALIDITY_BENCHMARK_INVALID).strip()
    if benchmark_state == "benchmark_valid":
        benchmark_applicability = "benchmark_valid"
    elif benchmark_state == VALIDITY_BENCHMARK_NOT_APPLICABLE:
        benchmark_applicability = VALIDITY_BENCHMARK_NOT_APPLICABLE
    else:
        benchmark_applicability = VALIDITY_BENCHMARK_INVALID
    data_complete = bool(execution_completeness) and bool(
        performance.get("realized_pnl_truth") and performance.get("drawdown_truth")
    )
    data_state = "complete" if data_complete else "incomplete"
    reason_codes: List[str] = []
    if contamination == "execution_contaminated":
        validity_state = VALIDITY_EXECUTION_CONTAMINATED
        reason_codes.append("EXECUTION_CONTAMINATED")
    elif data_state == "incomplete":
        validity_state = VALIDITY_DATA_INCOMPLETE
        reason_codes.append("DATA_INCOMPLETE")
    elif benchmark_applicability == VALIDITY_BENCHMARK_INVALID:
        validity_state = VALIDITY_BENCHMARK_INVALID
        reason_codes.append("BENCHMARK_INVALID")
    elif sample_count < provisional_min:
        validity_state = VALIDITY_INSUFFICIENT_SAMPLE
        reason_codes.append("INSUFFICIENT_SAMPLE")
    elif sample_count < valid_min:
        validity_state = VALIDITY_PROVISIONAL
        reason_codes.append("PROVISIONAL_SAMPLE")
    else:
        validity_state = VALIDITY_VALID
    if benchmark_applicability == VALIDITY_BENCHMARK_NOT_APPLICABLE:
        reason_codes.append("BENCHMARK_NOT_APPLICABLE")
    dependency_refs = [
        _governed_ref("sleeve_performance_truth_v1", performance_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_validity_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_validity_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_validity_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_validity_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_validity:{_parse_day(day_utc)}:{performance['sleeve_id']}",
    )
    return {
        "schema_id": "C2_SLEEVE_VALIDITY_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "execution_sleeve_id": execution_sleeve_id,
        "mode": mode,
        "sleeve_id": str(performance.get("sleeve_id") or "").strip(),
        "window": dict(performance.get("measurement_window") or {}),
        "sample_trade_count": sample_count,
        "validity_state": validity_state,
        "execution_contamination_state": contamination,
        "benchmark_applicability_state": benchmark_applicability,
        "data_completeness_state": data_state,
        "reason_codes": reason_codes,
        "upstream_refs": [dict(row) for row in dependency_refs],
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_sleeve_evaluation_payload(
    *,
    day_utc: str,
    execution_sleeve_id: str,
    mode: str,
    performance_ref: SurfaceRefV1,
    validity_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_evaluation_state_v1", WRITER_ID)
    performance = performance_ref.payload
    validity = validity_ref.payload
    policy = _read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve())
    sleeve_policy = policy.get("sleeve_evaluation_policy") if isinstance(policy.get("sleeve_evaluation_policy"), dict) else {}
    expectancy = _decimal(((performance.get("realized_pnl_truth") or {}) if isinstance(performance.get("realized_pnl_truth"), dict) else {}).get("native_net_expectancy"))
    drift_obj = (((performance.get("legacy_measurement_refs") or []), None))
    drift = _decimal((((((performance.get("realized_pnl_truth") or {}) if isinstance(performance.get("realized_pnl_truth"), dict) else {})).get("native_recent_vs_baseline_drift")) or "0"))
    edge_positive_min = _decimal(sleeve_policy.get("edge_positive_unvalidated_min_expectancy"), default="0.01")
    edge_validated_min = _decimal(sleeve_policy.get("edge_validated_min_expectancy"), default="25.00")
    deteriorating_max = _decimal(sleeve_policy.get("edge_deteriorating_max_drift"), default="-10.00")
    reason_codes: List[str] = []
    if str(validity.get("validity_state") or "") != VALIDITY_VALID:
        edge_state = NOT_ENOUGH_EVIDENCE
        stability_state = NOT_ENOUGH_EVIDENCE
        outcome_state = NOT_ENOUGH_EVIDENCE
        reason_codes.append("NO_VALID_EVIDENCE_FOR_EVALUATION")
    else:
        if expectancy >= edge_validated_min:
            edge_state = EDGE_VALIDATED
        elif expectancy >= edge_positive_min:
            edge_state = EDGE_POSITIVE_UNVALIDATED
        else:
            edge_state = EDGE_NEGATIVE
        if drift <= deteriorating_max:
            stability_state = STABILITY_UNSTABLE
            if edge_state != EDGE_NEGATIVE:
                edge_state = EDGE_DETERIORATING
        else:
            stability_state = STABILITY_STABLE
        if edge_state in {EDGE_VALIDATED, EDGE_POSITIVE_UNVALIDATED, EDGE_DETERIORATING, EDGE_NEGATIVE}:
            outcome_state = edge_state
        else:
            outcome_state = NOT_ENOUGH_EVIDENCE
    dependency_refs = [
        _governed_ref("sleeve_performance_truth_v1", performance_ref.path),
        _governed_ref("sleeve_validity_state_v1", validity_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_evaluation_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_evaluation_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_evaluation_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_evaluation_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_evaluation:{_parse_day(day_utc)}:{performance['sleeve_id']}",
    )
    benchmark_truth = performance.get("benchmark_truth") if isinstance(performance.get("benchmark_truth"), dict) else {}
    trade_outcome = performance.get("trade_outcome_truth") if isinstance(performance.get("trade_outcome_truth"), dict) else {}
    return {
        "schema_id": "C2_SLEEVE_EVALUATION_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "execution_sleeve_id": execution_sleeve_id,
        "mode": mode,
        "sleeve_id": str(performance.get("sleeve_id") or "").strip(),
        "window": dict(performance.get("measurement_window") or {}),
        "expectancy": _decimal_text(expectancy),
        "win_rate": dict(trade_outcome.get("win_rate") or _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"])),
        "payoff_ratio": dict(trade_outcome.get("payoff_ratio") or _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"])),
        "outlier_concentration": _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"]),
        "stability_state": stability_state,
        "edge_state": edge_state,
        "outcome_state": outcome_state,
        "validity_ref": _plain_ref("sleeve_validity_state_v1", validity_ref.path),
        "benchmark_ref": dict(benchmark_truth),
        "upstream_refs": [dict(row) for row in dependency_refs],
        "reason_codes": reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_portfolio_performance_payload(
    *,
    truth_root: Path,
    day_utc: str,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "portfolio_performance_truth_v1", WRITER_ID)
    economic_path = _resolve_latest_economic_state_build(truth_root=truth_root, day_utc=day_utc)
    capital_path = (truth_root / "reports" / "capital_risk_envelope_v2" / _parse_day(day_utc) / "capital_risk_envelope.v2.json").resolve()
    economic = _read_json(economic_path)
    capital = _read_json(capital_path)
    evaluation = economic.get("economic_evaluation") if isinstance(economic.get("economic_evaluation"), dict) else {}
    performance_state = evaluation.get("performance_state") if isinstance(evaluation.get("performance_state"), dict) else {}
    benchmark_state = evaluation.get("benchmark_state") if isinstance(evaluation.get("benchmark_state"), dict) else {}
    portfolio_row = performance_state.get("portfolio") if isinstance(performance_state.get("portfolio"), dict) else {}
    envelope = capital.get("envelope") if isinstance(capital.get("envelope"), dict) else {}
    dependency_refs = [
        _governed_ref("economic_state_build_v1", economic_path),
        _governed_ref("capital_risk_envelope_v2", capital_path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="portfolio_performance_truth_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_performance_truth_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="portfolio_performance_truth_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_performance_truth_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"portfolio_performance_truth:{_parse_day(day_utc)}",
    )
    return {
        "schema_id": "C2_PORTFOLIO_PERFORMANCE_TRUTH_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "portfolio_scope": "CONSTELLATION_PORTFOLIO",
        "performance_status": "ACTIVE" if str(portfolio_row.get("status") or "").strip().upper() in {"OK", "ACTIVE"} else "NOT_ENOUGH_EVIDENCE",
        "portfolio_return": {
            "daily_pnl": int(portfolio_row.get("daily_pnl") or 0),
            "daily_return": portfolio_row.get("daily_return"),
            "status": str(portfolio_row.get("status") or "").strip(),
        },
        "drawdown_truth": {
            "drawdown_pct": envelope.get("drawdown_pct"),
            "drawdown_abs": envelope.get("drawdown_abs"),
            "drawdown_scaling_multiplier": envelope.get("multiplier"),
        },
        "benchmark_truth": dict(benchmark_state),
        "risk_truth": {
            "capital_risk_status": str(capital.get("status") or "").strip(),
            "allowed_capital_at_risk_cents": int(envelope.get("allowed_capital_at_risk_cents") or 0),
            "portfolio_capital_at_risk_cents": int(envelope.get("portfolio_capital_at_risk_cents") or 0),
            "hard_stop_proxy": str(capital.get("status") or "").strip(),
        },
        "reason_codes": [str(code).strip() for code in (benchmark_state.get("reason_codes") or []) if str(code).strip()],
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_portfolio_validity_payload(
    *,
    day_utc: str,
    performance_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "portfolio_validity_state_v1", WRITER_ID)
    performance = performance_ref.payload
    portfolio_return = performance.get("portfolio_return") if isinstance(performance.get("portfolio_return"), dict) else {}
    benchmark_truth = performance.get("benchmark_truth") if isinstance(performance.get("benchmark_truth"), dict) else {}
    daily_return = portfolio_return.get("daily_return")
    if daily_return in (None, ""):
        validity_state = VALIDITY_INSUFFICIENT_SAMPLE
        reason_codes = ["PORTFOLIO_RETURN_NOT_READY"]
        data_state = "incomplete"
    else:
        validity_state = VALIDITY_VALID
        reason_codes = []
        data_state = "complete"
    policy_baseline = benchmark_truth.get("policy_baseline") if isinstance(benchmark_truth.get("policy_baseline"), dict) else {}
    benchmark_applicability = "benchmark_valid" if str(policy_baseline.get("status") or "").strip() in {"OK", "GENESIS"} else VALIDITY_BENCHMARK_INVALID
    if benchmark_applicability == VALIDITY_BENCHMARK_INVALID:
        validity_state = VALIDITY_BENCHMARK_INVALID
        reason_codes.append("PORTFOLIO_POLICY_BASELINE_INVALID")
    dependency_refs = [
        _governed_ref("portfolio_performance_truth_v1", performance_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="portfolio_validity_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_validity_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="portfolio_validity_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_validity_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"portfolio_validity:{_parse_day(day_utc)}",
    )
    return {
        "schema_id": "C2_PORTFOLIO_VALIDITY_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "portfolio_scope": "CONSTELLATION_PORTFOLIO",
        "window": {"window_kind": "WEEKLY"},
        "sample_trade_count": 0,
        "validity_state": validity_state,
        "execution_contamination_state": "execution_clean",
        "benchmark_applicability_state": benchmark_applicability,
        "data_completeness_state": data_state,
        "reason_codes": reason_codes,
        "upstream_refs": [dict(row) for row in dependency_refs],
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_portfolio_evaluation_payload(
    *,
    day_utc: str,
    performance_ref: SurfaceRefV1,
    validity_ref: SurfaceRefV1,
    policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "portfolio_evaluation_state_v1", WRITER_ID)
    performance = performance_ref.payload
    validity = validity_ref.payload
    drawdown_truth = performance.get("drawdown_truth") if isinstance(performance.get("drawdown_truth"), dict) else {}
    risk_truth = performance.get("risk_truth") if isinstance(performance.get("risk_truth"), dict) else {}
    portfolio_return = performance.get("portfolio_return") if isinstance(performance.get("portfolio_return"), dict) else {}
    benchmark_truth = performance.get("benchmark_truth") if isinstance(performance.get("benchmark_truth"), dict) else {}
    benchmark_state = benchmark_truth.get("policy_baseline") if isinstance(benchmark_truth.get("policy_baseline"), dict) else {}
    reason_codes: List[str] = []
    if str(validity.get("validity_state") or "") != VALIDITY_VALID:
        risk_state = NOT_ENOUGH_EVIDENCE
        benchmark_relative_state = NOT_ENOUGH_EVIDENCE
        outcome_state = NOT_ENOUGH_EVIDENCE
        reason_codes.append("NO_VALID_PORTFOLIO_EVIDENCE")
    else:
        drawdown_pct = drawdown_truth.get("drawdown_pct")
        drawdown_value = _decimal(drawdown_pct, default="0") if drawdown_pct not in (None, "") else Decimal("0")
        policy = _read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve())
        portfolio_policy = policy.get("portfolio_evaluation_policy") if isinstance(policy.get("portfolio_evaluation_policy"), dict) else {}
        risk_warning_drawdown = _decimal(portfolio_policy.get("risk_warning_drawdown_pct"), default="-0.050000")
        risk_breach_drawdown = _decimal(portfolio_policy.get("risk_breach_drawdown_pct"), default="-0.100000")
        if str(risk_truth.get("capital_risk_status") or "").upper() not in {"PASS", "OK"} or drawdown_value <= risk_breach_drawdown:
            risk_state = RISK_BREACH
            outcome_state = RISK_BREACH
        elif drawdown_value <= risk_warning_drawdown:
            risk_state = RISK_WARNING
            outcome_state = RISK_WARNING
        else:
            risk_state = RISK_WITHIN_POLICY
            outcome_state = EDGE_VALIDATED
        comparison = benchmark_state.get("comparison_vs_portfolio_return")
        if comparison in (None, ""):
            benchmark_relative_state = NOT_ENOUGH_EVIDENCE
        else:
            cmp_value = _decimal(comparison)
            if cmp_value > 0:
                benchmark_relative_state = "outperforming_benchmark"
            elif cmp_value < 0:
                benchmark_relative_state = "underperforming_benchmark"
            else:
                benchmark_relative_state = "at_benchmark"
    dependency_refs = [
        _governed_ref("portfolio_performance_truth_v1", performance_ref.path),
        _governed_ref("portfolio_validity_state_v1", validity_ref.path),
        _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
    ]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="portfolio_evaluation_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_evaluation_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="portfolio_evaluation_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_evaluation_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)],
        code_version=repo_git_sha_v1(),
        run_id=f"portfolio_evaluation:{_parse_day(day_utc)}",
    )
    return {
        "schema_id": "C2_PORTFOLIO_EVALUATION_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "portfolio_scope": "CONSTELLATION_PORTFOLIO",
        "window": {"window_kind": "WEEKLY"},
        "return": dict(portfolio_return),
        "drawdown": dict(drawdown_truth),
        "risk_state": risk_state,
        "benchmark_relative_state": benchmark_relative_state,
        "outcome_state": outcome_state,
        "validity_ref": _plain_ref("portfolio_validity_state_v1", validity_ref.path),
        "upstream_refs": [dict(row) for row in dependency_refs],
        "reason_codes": reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_sleeve_action_payload(*, day_utc: str, evaluation_ref: SurfaceRefV1, policy_ref: SurfaceRefV1) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_governance_action_state_v1", WRITER_ID)
    evaluation = evaluation_ref.payload
    sleeve_id = str(evaluation.get("sleeve_id") or "").strip()
    policy = _read_json((REPO_ROOT / POLICY_REGISTRY_RELPATH).resolve())
    sleeve_policy = policy.get("sleeve_evaluation_policy") if isinstance(policy.get("sleeve_evaluation_policy"), dict) else {}
    sample_count = 0
    evaluation_ref_row = _plain_ref("sleeve_evaluation_state_v1", evaluation_ref.path)
    policy_ref_row = _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)
    edge_state = str(evaluation.get("edge_state") or "").strip()
    outcome_state = str(evaluation.get("outcome_state") or "").strip()
    if edge_state == NOT_ENOUGH_EVIDENCE or outcome_state == NOT_ENOUGH_EVIDENCE:
        action_state = "no_conclusion"
        reason_codes = ["INSUFFICIENT_VALID_EVIDENCE"]
    elif edge_state == EDGE_VALIDATED and str(evaluation.get("stability_state") or "") == STABILITY_STABLE:
        action_state = "continue"
        reason_codes = ["EVALUATION_CONTINUE"]
    elif edge_state == EDGE_POSITIVE_UNVALIDATED:
        action_state = "watch"
        reason_codes = ["EDGE_POSITIVE_UNVALIDATED"]
    elif edge_state == EDGE_DETERIORATING:
        action_state = "reduce"
        reason_codes = ["EDGE_DETERIORATING"]
    elif edge_state == EDGE_NEGATIVE:
        retire_min = int(sleeve_policy.get("retire_min_sample_count") or 8)
        action_state = "retire" if sample_count >= retire_min else "pause"
        reason_codes = ["EDGE_NEGATIVE"]
    else:
        action_state = "no_conclusion"
        reason_codes = ["ACTION_STATE_UNRESOLVED"]
    mandatory = "MANDATORY" if action_state in {"pause", "retire"} else "ADVISORY"
    dependency_refs = [
        _governed_ref("sleeve_evaluation_state_v1", evaluation_ref.path),
        policy_ref_row,
    ]
    bundle = build_frozen_decision_input_bundle_v1(
        artifact_type="sleeve_governance_action_state_v1",
        authority_id="sleeve_governance_action_state_v1",
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        run_id=f"sleeve_action:{_parse_day(day_utc)}:{sleeve_id}",
        reason_codes=reason_codes,
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_governance_action_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_governance_action_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_governance_action_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_governance_action_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_action:{_parse_day(day_utc)}:{sleeve_id}",
    )
    return {
        "schema_id": "C2_SLEEVE_GOVERNANCE_ACTION_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "scope_kind": "sleeve",
        "scope_id": sleeve_id,
        "action_state": action_state,
        "action_reason_codes": reason_codes,
        "mandatory_or_advisory": mandatory,
        "evaluation_ref": evaluation_ref_row,
        "policy_snapshot_ref": policy_ref_row,
        "frozen_input_bundle": bundle,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_sleeve_action_failsafe_payload(
    *,
    day_utc: str,
    sleeve_id: str,
    policy_ref: SurfaceRefV1,
    blocker_code: str,
    blocker_detail: str,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_governance_action_state_v1", WRITER_ID)
    policy_ref_row = _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)
    missing_dependencies = ["sleeve_evaluation_state_v1"]
    dependency_refs = [policy_ref_row]
    detail_code = str(blocker_detail or "").strip().split(":", 1)[0]
    reason_codes = [code for code in ["INSUFFICIENT_VALID_EVIDENCE", blocker_code, detail_code] if code]
    bundle = build_frozen_decision_input_bundle_v1(
        artifact_type="sleeve_governance_action_state_v1",
        authority_id="sleeve_governance_action_state_v1",
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        run_id=f"sleeve_action:{_parse_day(day_utc)}:{sleeve_id}:failsafe",
        reason_codes=reason_codes,
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_governance_action_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_governance_action_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(
        closure_state=CLOSURE_STATE_DEGRADED,
        reason_codes=[blocker_code],
        missing_dependency_artifacts=missing_dependencies,
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_governance_action_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_governance_action_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_action:{_parse_day(day_utc)}:{sleeve_id}:failsafe",
    )
    return {
        "schema_id": "C2_SLEEVE_GOVERNANCE_ACTION_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "scope_kind": "sleeve",
        "scope_id": sleeve_id,
        "action_state": "no_conclusion",
        "action_reason_codes": reason_codes,
        "mandatory_or_advisory": "ADVISORY",
        "evaluation_ref": {
            "artifact_id": "sleeve_evaluation_state_v1",
            "path": "",
            "sha256": "",
            "status": "MISSING",
            "diagnostic": blocker_detail,
        },
        "policy_snapshot_ref": policy_ref_row,
        "frozen_input_bundle": bundle,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_sleeve_performance_failsafe_payload(
    *,
    day_utc: str,
    sleeve_id: str,
    execution_sleeve_id: str,
    mode: str,
    policy_ref: SurfaceRefV1,
    blocker_code: str,
    blocker_detail: str,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "sleeve_performance_truth_v1", WRITER_ID)
    _ = policy_ref
    required_deps = [
        str(dep).strip()
        for dep in (contract.get("required_upstream_dependencies") or [])
        if str(dep).strip()
    ]
    detail_code = str(blocker_detail or "").strip().split(":", 1)[0]
    reason_codes = [code for code in [blocker_code, detail_code, "PERFORMANCE_NOT_ENOUGH_EVIDENCE"] if code]
    dependency_refs: List[Dict[str, Any]] = []
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="sleeve_performance_truth_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_performance_truth_v1",
        declared_dependency_artifacts=required_deps,
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(
        closure_state=CLOSURE_STATE_DEGRADED,
        reason_codes=[blocker_code],
        missing_dependency_artifacts=required_deps,
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="sleeve_performance_truth_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="sleeve_performance_truth_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"sleeve_performance_truth:{_parse_day(day_utc)}:{sleeve_id}:failsafe",
    )
    trade_outcome_truth = _trade_outcome_truth_from_fact_ledger({})
    return {
        "schema_id": "C2_SLEEVE_PERFORMANCE_TRUTH_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "execution_sleeve_id": str(execution_sleeve_id).strip().upper(),
        "mode": str(mode).strip().upper(),
        "sleeve_id": sleeve_id,
        "strategy_family": sleeve_id,
        "performance_status": "NOT_ENOUGH_EVIDENCE",
        "sample_trade_count": 0,
        "measurement_window": {},
        "realized_pnl_truth": {
            "native_net_pnl": "0",
            "native_gross_pnl": "0",
            "native_net_expectancy": "0",
            "native_recent_vs_baseline_drift": "0",
            "adopted_net_pnl": "0",
            "adopted_management_expectancy": "0",
            "fee_drag": "0",
        },
        "drawdown_truth": {
            "native_realized_drawdown": "0",
            "adopted_drawdown": "0",
        },
        "execution_truth": {
            "slippage_gate_status": "UNKNOWN",
            "measured_slippage_drag": _optional_metric(value=None, status="UNAVAILABLE", reason_codes=[blocker_code]),
            "execution_data_completeness": {
                "fill_price_complete": False,
                "fee_complete": False,
                "decision_price_complete": False,
                "state": "UNAVAILABLE",
            },
            "exit_reconciliation_status": "UNKNOWN",
        },
        "benchmark_truth": {
            "state": VALIDITY_BENCHMARK_INVALID,
            "reason_codes": [blocker_code],
            "daily_return": _optional_metric(value=None, status="UNAVAILABLE", reason_codes=[blocker_code]),
        },
        "trade_attribution_truth": {
            "fact_input_hash": "",
            "native_trade_count": 0,
            "adopted_trade_count": 0,
            "unknown_attribution_count": 0,
            "included_trade_count": 0,
            "excluded_trade_count": 0,
        },
        "trade_outcome_truth": trade_outcome_truth,
        "legacy_measurement_refs": [],
        "reason_codes": reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_portfolio_performance_failsafe_payload(
    *,
    day_utc: str,
    policy_ref: SurfaceRefV1,
    blocker_code: str,
    blocker_detail: str,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "portfolio_performance_truth_v1", WRITER_ID)
    _ = policy_ref
    required_deps = [
        str(dep).strip()
        for dep in (contract.get("required_upstream_dependencies") or [])
        if str(dep).strip()
    ]
    detail_code = str(blocker_detail or "").strip().split(":", 1)[0]
    reason_codes = [code for code in [blocker_code, detail_code, "PERFORMANCE_NOT_ENOUGH_EVIDENCE"] if code]
    dependency_refs: List[Dict[str, Any]] = []
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="portfolio_performance_truth_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_performance_truth_v1",
        declared_dependency_artifacts=required_deps,
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(
        closure_state=CLOSURE_STATE_DEGRADED,
        reason_codes=[blocker_code],
        missing_dependency_artifacts=required_deps,
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="portfolio_performance_truth_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_performance_truth_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"portfolio_performance_truth:{_parse_day(day_utc)}:failsafe",
    )
    return {
        "schema_id": "C2_PORTFOLIO_PERFORMANCE_TRUTH_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "portfolio_scope": "CONSTELLATION_PORTFOLIO",
        "performance_status": "NOT_ENOUGH_EVIDENCE",
        "portfolio_return": {
            "daily_pnl": 0,
            "daily_return": None,
            "status": "NOT_ENOUGH_EVIDENCE",
        },
        "drawdown_truth": {
            "drawdown_pct": None,
            "drawdown_abs": None,
            "drawdown_scaling_multiplier": None,
        },
        "benchmark_truth": {
            "policy_baseline": {
                "status": "INVALID",
                "reason_codes": [blocker_code],
            },
            "reason_codes": [blocker_code],
        },
        "risk_truth": {
            "capital_risk_status": "UNKNOWN",
            "allowed_capital_at_risk_cents": 0,
            "portfolio_capital_at_risk_cents": 0,
            "hard_stop_proxy": "UNKNOWN",
        },
        "reason_codes": reason_codes,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _scorecard_sleeve_performance_projection(performance_payload: Mapping[str, Any]) -> Dict[str, Any]:
    realized = performance_payload.get("realized_pnl_truth") if isinstance(performance_payload.get("realized_pnl_truth"), dict) else {}
    drawdown = performance_payload.get("drawdown_truth") if isinstance(performance_payload.get("drawdown_truth"), dict) else {}
    outcomes = performance_payload.get("trade_outcome_truth") if isinstance(performance_payload.get("trade_outcome_truth"), dict) else {}
    return {
        "performance_status": str(performance_payload.get("performance_status") or "NOT_ENOUGH_EVIDENCE"),
        "sample_trade_count": int(performance_payload.get("sample_trade_count") or 0),
        "realized_net_pnl": str(realized.get("native_net_pnl") or "0"),
        "drawdown": str(drawdown.get("native_realized_drawdown") or "0"),
        "expectancy": str(realized.get("native_net_expectancy") or "0"),
        "trade_count": int(outcomes.get("trade_count") or 0),
        "winning_trade_count": int(outcomes.get("winning_trade_count") or 0),
        "losing_trade_count": int(outcomes.get("losing_trade_count") or 0),
        "win_rate": dict(outcomes.get("win_rate") or _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"])),
        "avg_win": dict(outcomes.get("avg_win") or _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"])),
        "avg_loss": dict(outcomes.get("avg_loss") or _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"])),
        "payoff_ratio": dict(outcomes.get("payoff_ratio") or _optional_metric(value=None, status="UNAVAILABLE", reason_codes=["TRADE_OUTCOME_DISTRIBUTION_NOT_BOUND"])),
    }


def _scorecard_portfolio_performance_projection(performance_payload: Mapping[str, Any]) -> Dict[str, Any]:
    portfolio_return = performance_payload.get("portfolio_return") if isinstance(performance_payload.get("portfolio_return"), dict) else {}
    drawdown = performance_payload.get("drawdown_truth") if isinstance(performance_payload.get("drawdown_truth"), dict) else {}
    return {
        "performance_status": str(performance_payload.get("performance_status") or portfolio_return.get("status") or "NOT_ENOUGH_EVIDENCE"),
        "daily_pnl": portfolio_return.get("daily_pnl"),
        "daily_return": portfolio_return.get("daily_return"),
        "drawdown_pct": drawdown.get("drawdown_pct"),
        "drawdown_abs": drawdown.get("drawdown_abs"),
    }


def _build_portfolio_action_payload(*, day_utc: str, evaluation_ref: SurfaceRefV1, policy_ref: SurfaceRefV1) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "portfolio_governance_action_state_v1", WRITER_ID)
    evaluation = evaluation_ref.payload
    evaluation_ref_row = _plain_ref("portfolio_evaluation_state_v1", evaluation_ref.path)
    policy_ref_row = _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)
    risk_state = str(evaluation.get("risk_state") or "").strip()
    benchmark_relative = str(evaluation.get("benchmark_relative_state") or "").strip()
    if str(evaluation.get("outcome_state") or "").strip() == NOT_ENOUGH_EVIDENCE:
        action_state = "no_conclusion"
        reason_codes = ["PORTFOLIO_NO_CONCLUSION"]
    elif risk_state == RISK_BREACH:
        action_state = "pause"
        reason_codes = ["PORTFOLIO_RISK_BREACH"]
    elif risk_state == RISK_WARNING:
        action_state = "reduce"
        reason_codes = ["PORTFOLIO_RISK_WARNING"]
    elif benchmark_relative == "underperforming_benchmark":
        action_state = "watch"
        reason_codes = ["PORTFOLIO_UNDERPERFORMING_BASELINE"]
    else:
        action_state = "continue"
        reason_codes = ["PORTFOLIO_CONTINUE"]
    mandatory = "MANDATORY" if action_state in {"pause", "retire"} else "ADVISORY"
    dependency_refs = [
        _governed_ref("portfolio_evaluation_state_v1", evaluation_ref.path),
        policy_ref_row,
    ]
    bundle = build_frozen_decision_input_bundle_v1(
        artifact_type="portfolio_governance_action_state_v1",
        authority_id="portfolio_governance_action_state_v1",
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        run_id=f"portfolio_action:{_parse_day(day_utc)}",
        reason_codes=reason_codes,
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="portfolio_governance_action_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_governance_action_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(closure_state=CLOSURE_STATE_COMPLETE, reason_codes=[])
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="portfolio_governance_action_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_governance_action_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        code_version=repo_git_sha_v1(),
        run_id=f"portfolio_action:{_parse_day(day_utc)}",
    )
    return {
        "schema_id": "C2_PORTFOLIO_GOVERNANCE_ACTION_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "scope_kind": "portfolio",
        "scope_id": "CONSTELLATION_PORTFOLIO",
        "action_state": action_state,
        "action_reason_codes": reason_codes,
        "mandatory_or_advisory": mandatory,
        "evaluation_ref": evaluation_ref_row,
        "policy_snapshot_ref": policy_ref_row,
        "frozen_input_bundle": bundle,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _build_portfolio_action_failsafe_payload(
    *,
    day_utc: str,
    policy_ref: SurfaceRefV1,
    blocker_code: str,
    blocker_detail: str,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, "portfolio_governance_action_state_v1", WRITER_ID)
    policy_ref_row = _governed_ref("evaluation_policy_snapshot_v1", policy_ref.path)
    missing_dependencies = ["portfolio_evaluation_state_v1"]
    detail_code = str(blocker_detail or "").strip().split(":", 1)[0]
    reason_codes = [code for code in ["PORTFOLIO_NO_CONCLUSION", blocker_code, detail_code] if code]
    dependency_refs = [policy_ref_row]
    bundle = build_frozen_decision_input_bundle_v1(
        artifact_type="portfolio_governance_action_state_v1",
        authority_id="portfolio_governance_action_state_v1",
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        run_id=f"portfolio_action:{_parse_day(day_utc)}:failsafe",
        reason_codes=reason_codes,
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type="portfolio_governance_action_state_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_governance_action_state_v1",
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    blocker = build_machine_blocker_envelope_v1(
        closure_state=CLOSURE_STATE_DEGRADED,
        reason_codes=[blocker_code],
        missing_dependency_artifacts=missing_dependencies,
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type="portfolio_governance_action_state_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="portfolio_governance_action_state_v1",
        producer_id=WRITER_ID,
        generated_at_utc=_now_utc(),
        effective_at_utc=f"{_parse_day(day_utc)}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_ref_row],
        code_version=repo_git_sha_v1(),
        run_id=f"portfolio_action:{_parse_day(day_utc)}:failsafe",
    )
    return {
        "schema_id": "C2_PORTFOLIO_GOVERNANCE_ACTION_STATE_V1",
        "schema_version": "v1",
        "day_utc": _parse_day(day_utc),
        "produced_utc": _now_utc(),
        "scope_kind": "portfolio",
        "scope_id": "CONSTELLATION_PORTFOLIO",
        "action_state": "no_conclusion",
        "action_reason_codes": reason_codes,
        "mandatory_or_advisory": "ADVISORY",
        "evaluation_ref": {
            "artifact_id": "portfolio_evaluation_state_v1",
            "path": "",
            "sha256": "",
            "status": "MISSING",
            "diagnostic": blocker_detail,
        },
        "policy_snapshot_ref": policy_ref_row,
        "frozen_input_bundle": bundle,
        "closure_state": str(blocker["closure_state"]),
        "first_blocker_code": str(blocker["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _write_scorecard(
    *,
    truth_root: Path,
    day_utc: str,
    sleeve_rows: Sequence[Mapping[str, Any]],
    portfolio_summary: Mapping[str, Any],
    source_refs: Sequence[Mapping[str, Any]],
) -> SurfaceRefV1:
    generated_at = _now_utc()
    output_path = _portfolio_paths(truth_root=truth_root, day_utc=day_utc)["scorecard"]
    payload = {
        "schema_id": "C2_WEEKLY_SCORECARD_VIEW_V1",
        "schema_version": "v1",
        "artifact_id": "weekly_scorecard_view_v1",
        "surface_kind": "projection",
        "day_utc": _parse_day(day_utc),
        "generated_at": generated_at,
        "generated_at_utc": generated_at,
        "git_commit": repo_git_sha_v1(),
        "git_dirty_status": _repo_dirty_status_v1(),
        "truth_root": str(truth_root.resolve()),
        "runtime_root": str(truth_root.resolve()),
        "producer": WRITER_ID,
        "authority": "ADVISORY_ONLY",
        "readiness_authority": "aegis_control_plane_v1",
        "advisory_status": "ANALYSIS_ONLY",
        "window_label": f"WEEK_ENDING_{_parse_day(day_utc)}",
        "portfolio_summary": dict(portfolio_summary),
        "sleeve_rows": [dict(row) for row in sleeve_rows],
        "source_refs": [dict(row) for row in source_refs],
        "warnings": [],
        "requires_human_review": True,
    }
    payload["producer_contract_v1"] = _scorecard_producer_contract_v1(day_utc=day_utc, output_path=output_path, source_refs=source_refs)
    validation = validate_read_model_payload_v1(payload)
    if not bool(validation.get("ok")):
        raise ValueError(f"INVALID_WEEKLY_SCORECARD_READ_MODEL:{validation.get('errors')}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, WEEKLY_SCORECARD_SCHEMA)
    return atomic_write_idempotent_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=WEEKLY_SCORECARD_SCHEMA,
        volatile_field_names=("generated_at_utc",),
    )


def materialize_governed_evaluation_day_v1(
    *,
    truth_root: Path,
    day_utc: str,
    execution_sleeve_id: str = "PRIMARY",
    mode: str = "PAPER",
    sleeve_ids: Sequence[str] | None = None,
) -> Dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    day = _parse_day(day_utc)
    mode = str(mode).strip().upper()
    execution_sleeve_id = str(execution_sleeve_id).strip().upper()
    policy_ref = write_evaluation_policy_snapshot_v1(truth_root=truth_root, day_utc=day)
    execution_truth_root = _resolve_execution_truth_root(
        truth_root=truth_root,
        execution_sleeve_id=execution_sleeve_id,
        mode=mode,
    )
    target_sleeve_ids = list(sleeve_ids or _discover_sleeve_ids(execution_truth_root=execution_truth_root, day_utc=day))
    sleeve_results: List[Dict[str, Any]] = []
    scorecard_rows: List[Dict[str, Any]] = []
    for sleeve_id in target_sleeve_ids:
        sleeve_paths = _sleeve_paths(execution_truth_root=execution_truth_root, day_utc=day, sleeve_id=sleeve_id)
        try:
            upstream = _resolve_sleeve_upstream(
                truth_root=truth_root,
                execution_sleeve_id=execution_sleeve_id,
                mode=mode,
                day_utc=day,
                sleeve_id=sleeve_id,
            )
        except ValueError as exc:
            detail = str(exc)
            if not detail.startswith("MISSING_SLEEVE_EDGE_SNAPSHOT:"):
                raise
            perf_ref = _write_governed(
                artifact_id="sleeve_performance_truth_v1",
                schema_relpath=SLEEVE_PERFORMANCE_SCHEMA,
                path=sleeve_paths["performance"],
                payload=_build_sleeve_performance_failsafe_payload(
                    day_utc=day,
                    sleeve_id=sleeve_id,
                    execution_sleeve_id=execution_sleeve_id,
                    mode=mode,
                    policy_ref=policy_ref,
                    blocker_code="SLEEVE_PERFORMANCE_UPSTREAM_MISSING",
                    blocker_detail=detail,
                ),
            )
            action_ref = _write_governed(
                artifact_id="sleeve_governance_action_state_v1",
                schema_relpath=SLEEVE_ACTION_SCHEMA,
                path=sleeve_paths["action"],
                payload=_build_sleeve_action_failsafe_payload(
                    day_utc=day,
                    sleeve_id=sleeve_id,
                    policy_ref=policy_ref,
                    blocker_code="SLEEVE_EVALUATION_STATE_MISSING_FOR_ACTION_STATE",
                    blocker_detail=detail,
                ),
            )
            fallback_evaluation_ref = {
                "artifact_id": "sleeve_evaluation_state_v1",
                "path": "",
                "sha256": "",
                "status": "MISSING",
                "diagnostic": detail,
            }
            sleeve_results.append(
                {
                    "sleeve_id": sleeve_id,
                    "performance_ref": _plain_ref("sleeve_performance_truth_v1", perf_ref.path),
                    "validity_ref": {},
                    "evaluation_ref": fallback_evaluation_ref,
                    "action_ref": _plain_ref("sleeve_governance_action_state_v1", action_ref.path),
                }
            )
            scorecard_rows.append(
                {
                    "sleeve_id": sleeve_id,
                    "validity_state": VALIDITY_INSUFFICIENT_SAMPLE,
                    "edge_state": NOT_ENOUGH_EVIDENCE,
                    "stability_state": NOT_ENOUGH_EVIDENCE,
                    "action_state": action_ref.payload["action_state"],
                    "display_grade": str(action_ref.payload["action_state"]).upper(),
                    "performance_ref": _plain_ref("sleeve_performance_truth_v1", perf_ref.path),
                    **_scorecard_sleeve_performance_projection(perf_ref.payload),
                    "evaluation_ref": fallback_evaluation_ref,
                    "action_ref": _plain_ref("sleeve_governance_action_state_v1", action_ref.path),
                }
            )
            continue
        perf_ref = _write_governed(
            artifact_id="sleeve_performance_truth_v1",
            schema_relpath=SLEEVE_PERFORMANCE_SCHEMA,
            path=sleeve_paths["performance"],
            payload=_build_sleeve_performance_payload(
                truth_root=truth_root,
                upstream=upstream,
                day_utc=day,
                execution_sleeve_id=execution_sleeve_id,
                mode=mode,
                policy_snapshot_ref=_governed_ref("evaluation_policy_snapshot_v1", policy_ref.path),
            ),
        )
        validity_ref = _write_governed(
            artifact_id="sleeve_validity_state_v1",
            schema_relpath=SLEEVE_VALIDITY_SCHEMA,
            path=sleeve_paths["validity"],
            payload=_build_sleeve_validity_payload(
                day_utc=day,
                execution_sleeve_id=execution_sleeve_id,
                mode=mode,
                performance_ref=perf_ref,
                policy_ref=policy_ref,
            ),
        )
        evaluation_ref = _write_governed(
            artifact_id="sleeve_evaluation_state_v1",
            schema_relpath=SLEEVE_EVALUATION_SCHEMA,
            path=sleeve_paths["evaluation"],
            payload=_build_sleeve_evaluation_payload(
                day_utc=day,
                execution_sleeve_id=execution_sleeve_id,
                mode=mode,
                performance_ref=perf_ref,
                validity_ref=validity_ref,
                policy_ref=policy_ref,
            ),
        )
        action_ref = _write_governed(
            artifact_id="sleeve_governance_action_state_v1",
            schema_relpath=SLEEVE_ACTION_SCHEMA,
            path=sleeve_paths["action"],
            payload=_build_sleeve_action_payload(
                day_utc=day,
                evaluation_ref=evaluation_ref,
                policy_ref=policy_ref,
            ),
        )
        sleeve_results.append(
            {
                "sleeve_id": sleeve_id,
                "performance_ref": _plain_ref("sleeve_performance_truth_v1", perf_ref.path),
                "validity_ref": _plain_ref("sleeve_validity_state_v1", validity_ref.path),
                "evaluation_ref": _plain_ref("sleeve_evaluation_state_v1", evaluation_ref.path),
                "action_ref": _plain_ref("sleeve_governance_action_state_v1", action_ref.path),
            }
        )
        scorecard_rows.append(
            {
                "sleeve_id": sleeve_id,
                "validity_state": validity_ref.payload["validity_state"],
                "edge_state": evaluation_ref.payload["edge_state"],
                "stability_state": evaluation_ref.payload["stability_state"],
                "action_state": action_ref.payload["action_state"],
                "display_grade": str(action_ref.payload["action_state"]).upper(),
                "performance_ref": _plain_ref("sleeve_performance_truth_v1", perf_ref.path),
                **_scorecard_sleeve_performance_projection(perf_ref.payload),
                "evaluation_ref": _plain_ref("sleeve_evaluation_state_v1", evaluation_ref.path),
                "action_ref": _plain_ref("sleeve_governance_action_state_v1", action_ref.path),
            }
        )
    portfolio_paths = _portfolio_paths(truth_root=truth_root, day_utc=day)
    portfolio_perf_ref: SurfaceRefV1 | None = None
    portfolio_validity_ref: SurfaceRefV1 | None = None
    portfolio_evaluation_ref: SurfaceRefV1 | None = None
    try:
        portfolio_perf_ref = _write_governed(
            artifact_id="portfolio_performance_truth_v1",
            schema_relpath=PORTFOLIO_PERFORMANCE_SCHEMA,
            path=portfolio_paths["performance"],
            payload=_build_portfolio_performance_payload(
                truth_root=truth_root,
                day_utc=day,
                policy_ref=policy_ref,
            ),
        )
        portfolio_validity_ref = _write_governed(
            artifact_id="portfolio_validity_state_v1",
            schema_relpath=PORTFOLIO_VALIDITY_SCHEMA,
            path=portfolio_paths["validity"],
            payload=_build_portfolio_validity_payload(
                day_utc=day,
                performance_ref=portfolio_perf_ref,
                policy_ref=policy_ref,
            ),
        )
        portfolio_evaluation_ref = _write_governed(
            artifact_id="portfolio_evaluation_state_v1",
            schema_relpath=PORTFOLIO_EVALUATION_SCHEMA,
            path=portfolio_paths["evaluation"],
            payload=_build_portfolio_evaluation_payload(
                day_utc=day,
                performance_ref=portfolio_perf_ref,
                validity_ref=portfolio_validity_ref,
                policy_ref=policy_ref,
            ),
        )
        portfolio_action_ref = _write_governed(
            artifact_id="portfolio_governance_action_state_v1",
            schema_relpath=PORTFOLIO_ACTION_SCHEMA,
            path=portfolio_paths["action"],
            payload=_build_portfolio_action_payload(
                day_utc=day,
                evaluation_ref=portfolio_evaluation_ref,
                policy_ref=policy_ref,
            ),
        )
    except ValueError as exc:
        detail = str(exc)
        if not detail.startswith("MISSING_ECONOMIC_STATE_BUILD:"):
            raise
        portfolio_perf_ref = _write_governed(
            artifact_id="portfolio_performance_truth_v1",
            schema_relpath=PORTFOLIO_PERFORMANCE_SCHEMA,
            path=portfolio_paths["performance"],
            payload=_build_portfolio_performance_failsafe_payload(
                day_utc=day,
                policy_ref=policy_ref,
                blocker_code="PORTFOLIO_PERFORMANCE_UPSTREAM_MISSING",
                blocker_detail=detail,
            ),
        )
        portfolio_action_ref = _write_governed(
            artifact_id="portfolio_governance_action_state_v1",
            schema_relpath=PORTFOLIO_ACTION_SCHEMA,
            path=portfolio_paths["action"],
            payload=_build_portfolio_action_failsafe_payload(
                day_utc=day,
                policy_ref=policy_ref,
                blocker_code="PORTFOLIO_EVALUATION_STATE_MISSING_FOR_ACTION_STATE",
                blocker_detail=detail,
            ),
        )
    scorecard_ref = _write_scorecard(
        truth_root=truth_root,
        day_utc=day,
        sleeve_rows=scorecard_rows,
        portfolio_summary={
            "validity_state": (
                portfolio_validity_ref.payload["validity_state"]
                if portfolio_validity_ref is not None
                else VALIDITY_INSUFFICIENT_SAMPLE
            ),
            "risk_state": (
                portfolio_evaluation_ref.payload["risk_state"]
                if portfolio_evaluation_ref is not None
                else NOT_ENOUGH_EVIDENCE
            ),
            "benchmark_relative_state": (
                portfolio_evaluation_ref.payload["benchmark_relative_state"]
                if portfolio_evaluation_ref is not None
                else NOT_ENOUGH_EVIDENCE
            ),
            "action_state": portfolio_action_ref.payload["action_state"],
            "display_grade": str(portfolio_action_ref.payload["action_state"]).upper(),
            "performance_ref": (
                _plain_ref("portfolio_performance_truth_v1", portfolio_perf_ref.path)
                if portfolio_perf_ref is not None
                else {}
            ),
            **(
                _scorecard_portfolio_performance_projection(portfolio_perf_ref.payload)
                if portfolio_perf_ref is not None
                else {}
            ),
            "evaluation_ref": (
                _plain_ref("portfolio_evaluation_state_v1", portfolio_evaluation_ref.path)
                if portfolio_evaluation_ref is not None
                else {
                    "artifact_id": "portfolio_evaluation_state_v1",
                    "path": "",
                    "sha256": "",
                    "status": "MISSING",
                }
            ),
            "action_ref": _plain_ref("portfolio_governance_action_state_v1", portfolio_action_ref.path),
        },
        source_refs=[
            _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
            _plain_ref("portfolio_governance_action_state_v1", portfolio_action_ref.path),
        ]
        + (
            [_plain_ref("portfolio_performance_truth_v1", portfolio_perf_ref.path)]
            if portfolio_perf_ref is not None
            else []
        )
        + (
            [_plain_ref("portfolio_evaluation_state_v1", portfolio_evaluation_ref.path)]
            if portfolio_evaluation_ref is not None
            else []
        )
        + [row["performance_ref"] for row in sleeve_results if isinstance(row.get("performance_ref"), dict) and row.get("performance_ref")]
        + [row["evaluation_ref"] for row in sleeve_results]
        + [row["action_ref"] for row in sleeve_results],
    )
    return {
        "day_utc": day,
        "execution_sleeve_id": execution_sleeve_id,
        "mode": mode,
        "policy_snapshot_ref": _plain_ref("evaluation_policy_snapshot_v1", policy_ref.path),
        "sleeves": sleeve_results,
        "portfolio": {
            "performance_ref": (
                _plain_ref("portfolio_performance_truth_v1", portfolio_perf_ref.path)
                if portfolio_perf_ref is not None
                else {}
            ),
            "validity_ref": (
                _plain_ref("portfolio_validity_state_v1", portfolio_validity_ref.path)
                if portfolio_validity_ref is not None
                else {}
            ),
            "evaluation_ref": (
                _plain_ref("portfolio_evaluation_state_v1", portfolio_evaluation_ref.path)
                if portfolio_evaluation_ref is not None
                else {
                    "artifact_id": "portfolio_evaluation_state_v1",
                    "path": "",
                    "sha256": "",
                    "status": "MISSING",
                }
            ),
            "action_ref": _plain_ref("portfolio_governance_action_state_v1", portfolio_action_ref.path),
        },
        "weekly_scorecard_ref": _plain_ref("weekly_scorecard_view_v1", scorecard_ref.path),
    }


__all__ = [
    "materialize_governed_evaluation_day_v1",
    "write_evaluation_policy_snapshot_v1",
    "VALIDITY_INSUFFICIENT_SAMPLE",
    "VALIDITY_VALID",
    "EDGE_POSITIVE_UNVALIDATED",
    "EDGE_VALIDATED",
    "EDGE_DETERIORATING",
    "EDGE_NEGATIVE",
    "NOT_ENOUGH_EVIDENCE",
]
