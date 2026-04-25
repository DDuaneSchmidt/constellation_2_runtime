from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    _build_scope,
    _check_path_boundary,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.common.tax.decision_v1 import build_tax_decision_dependency_fingerprint_v1
from constellation_2.common.tax.harvest_v1 import preview_harvest_candidate_decision_v1
from constellation_2.common.tax.policy_v1 import build_tax_policy_registry_v1, resolve_tax_policy_set_v1
from constellation_2.common.tax.state_v1 import build_tax_snapshot_v1
from constellation_2.common.tax.truth_v1 import (
    accept_tax_fact_candidate_v1,
    build_tax_fact_candidate_v1,
    build_tax_observed_event_v1,
)
from constellation_2.common.tax_state_explanation_mapping_v1 import (
    EXPLANATION_MAPPING_VERSION,
    map_tax_state_explanation_v1,
)
from constellation_2.common.tax_state_precedence_v1 import evaluate_tax_state_precedence_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.tax_state_kernel_v1"
KERNEL_VERSION = WRITER_ID
ARTIFACT_ID = "tax_state_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/tax_state.v1.schema.json"
POSITIONS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"
CASH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"
NAV_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json"
ACCEPTED_TAX_FACT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact.v1.schema.json"
ACCEPTED_TAX_FACT_JSONL = "accepted_tax_fact.v1.jsonl"
ACCOUNT_REGISTRY_PATH = (
    REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json"
).resolve()


class TaxStateKernelError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _plain_ref(*, artifact_id: str, path: Path, sha256: str) -> Dict[str, str]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def _plain_ref_from_surface(surface: SurfaceRefV1, artifact_id: str) -> Dict[str, str]:
    return _plain_ref(artifact_id=artifact_id, path=surface.path, sha256=surface.sha256)


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _scope(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str,
    canonical_truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
) -> Any:
    return _build_scope(
        repo_root=REPO_ROOT,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        candidate_path=None,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )


def _read_surface(
    *,
    scope: Any,
    path: Path,
    schema_relpath: str,
    label: str,
) -> SurfaceRefV1:
    report: Dict[str, Any] = {"errors": [], "forbidden_path_hits": []}
    _check_path_boundary(
        report,
        scope=scope,
        label=label,
        path=path,
        expected_root_type="canonical_truth_root",
        reject_derived=False,
    )
    if report["errors"]:
        raise TaxStateKernelError(str(report["errors"][0]))
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _read_jsonl_accepted_facts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for index, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise TaxStateKernelError(f"TAX_ACCEPTED_FACT_JOURNAL_LINE_INVALID:{path}:{index}")
        validate_against_repo_schema_v1(payload, REPO_ROOT, ACCEPTED_TAX_FACT_SCHEMA)
        rows.append(payload)
    return rows


def _symbol_for_position(item: Mapping[str, Any]) -> str:
    instrument = item.get("instrument")
    if isinstance(instrument, Mapping):
        for key in ("symbol", "underlying"):
            value = str(instrument.get(key) or "").strip()
            if value:
                return value
    value = str(item.get("symbol") or "").strip()
    if value:
        return value
    raise TaxStateKernelError("TAX_POSITION_SYMBOL_MISSING")


def _decimal_from_value(value: Any) -> Decimal:
    text = str(value or "").strip()
    return Decimal(text or "0")


def _money_from_cents(value: Any) -> str:
    cents = int(value or 0)
    return f"{Decimal(cents) / Decimal('100'):.2f}"


def _lot_bridge_fact_from_position(
    *,
    scope_id: str,
    produced_utc: str,
    position_item: Mapping[str, Any],
    lot_row: Mapping[str, Any],
    positions_ref: Mapping[str, Any],
) -> dict[str, Any] | None:
    symbol = _symbol_for_position(position_item)
    source_kind = str(lot_row.get("source_kind") or "").strip().upper()
    event_family = "lot_imported" if source_kind == "BROKER_AGGREGATE" or str(position_item.get("origin") or "").upper() == "IMPORTED" else "lot_opened"
    payload = {
        "lot_id": str(lot_row.get("lot_id") or "").strip(),
        "account_id": str(position_item.get("account_id") or "").strip(),
        "security_id": symbol,
        "quantity": str(int(lot_row.get("remaining_qty_abs") or 0)),
        "basis_total": _money_from_cents(lot_row.get("cost_basis_cents")),
        "holding_period_start_at": f"{str(lot_row.get('opened_day_utc') or '').strip()}T00:00:00Z",
        "confidence_state": "broker_provided" if source_kind == "BROKER_AGGREGATE" else "exact",
        "maturity_state": "restricted" if source_kind == "BROKER_AGGREGATE" else "decision_eligible",
    }
    observed = build_tax_observed_event_v1(
        event_family=event_family,
        scope_ids=(scope_id,),
        payload=payload,
        source_event_ref=f"{positions_ref['artifact_path']}#{position_item.get('position_id')}#{lot_row.get('lot_id')}",
        observed_at=produced_utc,
        recorded_at=produced_utc,
    )
    candidate = build_tax_fact_candidate_v1(observed)
    _, accepted = accept_tax_fact_candidate_v1(candidate)
    return accepted


def _bridge_position_facts_v1(
    *,
    scope_id: str,
    positions_payload: Mapping[str, Any],
    positions_ref: Mapping[str, Any],
) -> list[dict[str, Any]]:
    produced_utc = str(positions_payload.get("produced_utc") or "").strip() or _utc_now()
    rows: list[dict[str, Any]] = []
    for item in positions_payload.get("items") or ():
        if not isinstance(item, Mapping):
            continue
        for lot in item.get("lots") or ():
            if not isinstance(lot, Mapping):
                continue
            accepted = _lot_bridge_fact_from_position(
                scope_id=scope_id,
                produced_utc=produced_utc,
                position_item=item,
                lot_row=lot,
                positions_ref=positions_ref,
            )
            if accepted is not None:
                rows.append(accepted)
    return rows


def _market_prices_by_security(nav_payload: Mapping[str, Any]) -> dict[str, str]:
    prices: dict[str, str] = {}
    nav = nav_payload.get("nav")
    if not isinstance(nav, Mapping):
        return prices
    for row in nav.get("components") or ():
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or "").strip()
        if not symbol or symbol == "USD":
            continue
        qty = _decimal_from_value(row.get("qty"))
        mv = _decimal_from_value(row.get("mv"))
        if qty == 0:
            continue
        price = abs(mv / qty)
        prices[symbol] = f"{price:.6f}".rstrip("0").rstrip(".") or "0"
    return prices


def _latest_timestamp(*values: str) -> str:
    present = [str(value).strip() for value in values if str(value).strip()]
    return max(present) if present else ""


def _freshness_state(*, day_utc: str, latest_source_utc: str, evaluated_at_utc: str) -> str:
    if not latest_source_utc:
        return "unknown"
    try:
        source = datetime.fromisoformat(latest_source_utc.replace("Z", "+00:00"))
        evaluated = datetime.fromisoformat(evaluated_at_utc.replace("Z", "+00:00"))
    except Exception:
        return "unknown"
    if str(day_utc).strip() and latest_source_utc[:10] != str(day_utc).strip():
        return "stale"
    return "stale" if (evaluated - source).total_seconds() > 36 * 3600 else "fresh"


def _state_quality_from_lots(lot_states: Sequence[Mapping[str, Any]], code: str) -> str:
    if not lot_states:
        return "unknown"
    affected = sum(1 for row in lot_states if code in set(row.get("reason_codes") or ()))
    if affected == 0:
        return "known"
    if affected == len(lot_states):
        return "unknown"
    return "partial"


def _account_profiles_v1(
    *,
    positions_payload: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    account_registry_payload: Mapping[str, Any],
    evidence_refs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    regimes = {
        str(row.get("account_id") or "").strip(): str(row.get("tax_regime") or "").strip()
        for row in snapshot.get("account_tax_regimes") or ()
        if isinstance(row, Mapping)
    }
    by_account_lots: dict[str, list[Mapping[str, Any]]] = {}
    for row in snapshot.get("lot_states") or ():
        if not isinstance(row, Mapping):
            continue
        by_account_lots.setdefault(str(row.get("account_id") or "").strip(), []).append(row)
    account_rows = {str(row.get("account_id") or "").strip(): row for row in positions_payload.get("accounts") or () if isinstance(row, Mapping)}
    registry_rows = {str(row.get("account_id") or "").strip(): row for row in account_registry_payload.get("accounts") or () if isinstance(row, Mapping)}
    account_ids = sorted({*account_rows.keys(), *registry_rows.keys(), *regimes.keys(), *by_account_lots.keys()} - {""})
    profiles: list[dict[str, Any]] = []
    for account_id in account_ids:
        lots = by_account_lots.get(account_id, [])
        regime = regimes.get(account_id) or None
        if not lots:
            profile_state = "no_positions"
        elif regime is None:
            profile_state = "missing_regime"
        elif any("TAX_SAFE_DEGRADED_MODE" in set(row.get("reason_codes") or ()) for row in lots):
            profile_state = "degraded"
        else:
            profile_state = "materialized"
        profiles.append(
            {
                "entity_id": f"tax-profile:{account_id}",
                "account_id": account_id,
                "environment": str((registry_rows.get(account_id) or {}).get("environment") or "UNKNOWN"),
                "tax_profile_state": profile_state,
                "tax_regime": regime,
                "lot_basis_state": _state_quality_from_lots(lots, "TAX_BASIS_UNKNOWN"),
                "holding_period_state": _state_quality_from_lots(lots, "TAX_HOLDING_PERIOD_UNKNOWN"),
                "source_authority": ["positions_snapshot_v5", "c2_ib_account_registry", "tax_position_snapshot"],
                "provenance_refs": [dict(ref) for ref in evidence_refs],
            }
        )
    return profiles


def _sum_realized_unrealized_v1(
    *,
    accepted_facts: Sequence[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    market_prices: Mapping[str, str],
    evidence_refs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    realized = Decimal("0")
    realized_present = False
    for fact in accepted_facts:
        if str(fact.get("fact_family") or "") != "realization_recorded":
            continue
        realized += _decimal_from_value((fact.get("payload") or {}).get("realized_gain_loss"))
        realized_present = True
    unrealized = Decimal("0")
    lots = list(snapshot.get("lot_states") or ())
    priced = 0
    for row in lots:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("security_id") or "").strip()
        price = market_prices.get(symbol)
        if not price:
            continue
        priced += 1
        unrealized += (_decimal_from_value(price) * _decimal_from_value(row.get("remaining_quantity"))) - _decimal_from_value(row.get("basis_total"))
    available = realized_present or (bool(lots) and priced == len(lots))
    return {
        "status": "available" if available else "unavailable",
        "realized_gain_loss_ytd_usd": f"{realized:.2f}" if realized_present else None,
        "unrealized_gain_loss_usd": f"{unrealized:.2f}" if available and lots else None,
        "source_refs": [dict(ref) for ref in evidence_refs],
    }


def _blocker_states_v1(
    *,
    snapshot: Mapping[str, Any],
    accepted_facts: Sequence[Mapping[str, Any]],
    positions_payload: Mapping[str, Any],
    harvest_decision: Mapping[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    lot_states = list(snapshot.get("lot_states") or ())
    account_ids = {
        str(row.get("account_id") or "").strip()
        for row in positions_payload.get("accounts") or ()
        if isinstance(row, Mapping)
    } - {""}
    regimes = {
        str(row.get("account_id") or "").strip()
        for row in snapshot.get("account_tax_regimes") or ()
        if isinstance(row, Mapping)
    } - {""}
    if lot_states and not accepted_facts:
        blockers.append("accepted_tax_truth_missing")
    if account_ids and not regimes:
        blockers.append("account_regime_missing")
    if any("TAX_BASIS_UNKNOWN" in set(row.get("reason_codes") or ()) for row in lot_states):
        blockers.append("basis_unknown")
    if any("TAX_HOLDING_PERIOD_UNKNOWN" in set(row.get("reason_codes") or ()) for row in lot_states):
        blockers.append("holding_period_unknown")
    if any("TAX_CORP_ACTION_UNRESOLVED" in set(row.get("reason_codes") or ()) for row in lot_states):
        blockers.append("corporate_action_unresolved")
    if harvest_decision is not None:
        reason_codes = set(harvest_decision.get("reason_codes") or ())
        if "TAX_WASH_RISK_BLOCK" in reason_codes:
            blockers.append("wash_sale_conflict")
        if "TAX_POLICY_BLOCK" in reason_codes:
            blockers.append("policy_block")
    seen: set[str] = set()
    ordered: list[str] = []
    for row in blockers:
        if row in seen:
            continue
        seen.add(row)
        ordered.append(row)
    return ordered


def _wash_sale_state_v1(snapshot: Mapping[str, Any], blocker_states: Sequence[str]) -> str:
    if "wash_sale_conflict" in set(blocker_states):
        return "blocked"
    if (snapshot.get("wash_state") or {}).get("active_windows"):
        return "warning"
    return "clear"


def _opportunity_states_v1(*, harvest_decision: Mapping[str, Any] | None) -> list[str]:
    if harvest_decision is None:
        return []
    if harvest_decision.get("candidate_lots"):
        return ["harvest_candidate_available"]
    return []


def _harvest_candidates_payload_v1(
    *,
    candidates: Sequence[Mapping[str, Any]],
    opportunity_visibility: str,
    evidence_refs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if opportunity_visibility == "visible":
        status = "available"
        items = [dict(row) for row in candidates]
    elif candidates:
        status = "suppressed"
        items = []
    else:
        status = "unavailable"
        items = []
    return {
        "status": status,
        "candidate_count": len(items),
        "items": items,
        "source_refs": [dict(ref) for ref in evidence_refs],
    }


def _semantic_event(
    *,
    event_type: str,
    authority_label: str,
    completeness_state: str,
    freshness_state: str,
    visibility_state: str,
    blocker_states: Sequence[str],
    opportunity_states: Sequence[str],
    degraded_reason_id: str,
    governing_refs: Sequence[Mapping[str, Any]],
    kernel_version: str,
) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "authority_label": authority_label,
        "completeness_state": completeness_state,
        "freshness_state": freshness_state,
        "visibility_state": visibility_state,
        "blocker_states": [str(item).strip() for item in blocker_states if str(item).strip()],
        "opportunity_states": [str(item).strip() for item in opportunity_states if str(item).strip()],
        "degraded_reason_id": degraded_reason_id,
        "governing_refs": [dict(row) for row in governing_refs],
        "kernel_version": kernel_version,
    }


def _load_account_registry_ref() -> tuple[dict[str, Any], dict[str, str]]:
    payload = json.loads(ACCOUNT_REGISTRY_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TaxStateKernelError("ACCOUNT_REGISTRY_NOT_OBJECT")
    return payload, _plain_ref(
        artifact_id="c2_ib_account_registry",
        path=ACCOUNT_REGISTRY_PATH,
        sha256=_sha256_file(ACCOUNT_REGISTRY_PATH),
    )


def _tax_state_root(*, canonical_truth_root: Path, day_utc: str, scope_id: str) -> Path:
    return (canonical_truth_root / "reports" / ARTIFACT_ID / day_utc / scope_id).resolve()


def list_tax_states_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/tax_state.v1.json" if not scope_id else f"{scope_id}/*/tax_state.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("tax_state_id") or ""),
            str(ref.path),
        )
    )
    return refs


def find_latest_tax_state_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> SurfaceRefV1 | None:
    refs = list_tax_states_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc, scope_id=scope_id)
    if not refs:
        return None
    superseded_paths = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in refs
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    current_refs = [ref for ref in refs if str(ref.path.resolve()) not in superseded_paths]
    if current_refs:
        refs = current_refs
    return refs[-1]


def materialize_tax_state_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    tax_scope_id: str | None = None,
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    emit_artifact: bool = False,
    evaluated_at_utc: str | None = None,
) -> Dict[str, Any]:
    evaluated_utc = str(evaluated_at_utc or "").strip() or _utc_now()
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    tax_scope = str(tax_scope_id or scope.scope_id).strip()
    if not tax_scope:
        raise TaxStateKernelError("TAX_SCOPE_ID_REQUIRED")

    nav_ref = _read_surface(
        scope=scope,
        path=(scope.canonical_truth_root / "accounting_v2" / "nav" / scope.day_utc / "nav.v2.json").resolve(),
        schema_relpath=NAV_SCHEMA,
        label="TAX_STATE_NAV",
    )
    cash_ref = _read_surface(
        scope=scope,
        path=(scope.canonical_truth_root / "cash_ledger_v1" / "snapshots" / scope.day_utc / "cash_ledger_snapshot.v1.json").resolve(),
        schema_relpath=CASH_SCHEMA,
        label="TAX_STATE_CASH",
    )
    positions_ref = _read_surface(
        scope=scope,
        path=(scope.canonical_truth_root / "positions_v1" / "snapshots" / scope.day_utc / "positions_snapshot.v5.json").resolve(),
        schema_relpath=POSITIONS_SCHEMA,
        label="TAX_STATE_POSITIONS",
    )
    account_registry_payload, account_registry_ref = _load_account_registry_ref()
    journal_path = (
        scope.canonical_truth_root
        / "journals"
        / "accepted_tax_fact_v1"
        / "scopes"
        / tax_scope
        / ACCEPTED_TAX_FACT_JSONL
    ).resolve()
    journal_facts = _read_jsonl_accepted_facts(journal_path)
    journal_ref = (
        _plain_ref(
            artifact_id="accepted_tax_fact_v1",
            path=journal_path,
            sha256=_sha256_file(journal_path),
        )
        if journal_path.exists()
        else None
    )
    bridge_facts = _bridge_position_facts_v1(
        scope_id=tax_scope,
        positions_payload=positions_ref.payload,
        positions_ref=_plain_ref_from_surface(positions_ref, "positions_snapshot_v5"),
    )
    accepted_facts: list[dict[str, Any]] = []
    seen_fact_ids: set[str] = set()
    for fact in [*journal_facts, *bridge_facts]:
        fact_id = str(fact.get("accepted_fact_id") or "").strip()
        if not fact_id or fact_id in seen_fact_ids:
            continue
        seen_fact_ids.add(fact_id)
        accepted_facts.append(dict(fact))

    snapshot, manifest = build_tax_snapshot_v1(
        scope_id=tax_scope,
        as_of_effective_at=f"{scope.day_utc}T23:59:59Z",
        facts_included_through_recorded_at=evaluated_utc,
        accepted_facts=accepted_facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    market_prices = _market_prices_by_security(nav_ref.payload)
    account_regime = "UNKNOWN"
    for row in snapshot.get("account_tax_regimes") or ():
        if str(row.get("account_id") or "").strip() == scope.ib_account:
            account_regime = str(row.get("tax_regime") or "UNKNOWN")
            break
        if account_regime == "UNKNOWN":
            account_regime = str(row.get("tax_regime") or "UNKNOWN")
    harvest_decision = None
    if market_prices and account_regime and account_regime != "UNKNOWN":
        policy_registry = build_tax_policy_registry_v1(scope_id=tax_scope)
        resolved_policy_set = resolve_tax_policy_set_v1(
            policy_registry=policy_registry,
            scope_id=tax_scope,
            account_tax_regime=account_regime,
        )
        harvest_decision, _ = preview_harvest_candidate_decision_v1(
            snapshot=snapshot,
            resolved_policy_set=resolved_policy_set,
            request={
                "as_of_effective_at": f"{scope.day_utc}T23:59:59Z",
                "market_prices_by_security": market_prices,
            },
        )

    blocker_states = _blocker_states_v1(
        snapshot=snapshot,
        accepted_facts=accepted_facts,
        positions_payload=positions_ref.payload,
        harvest_decision=harvest_decision,
    )
    opportunity_states = _opportunity_states_v1(harvest_decision=harvest_decision)
    completeness_state = "complete"
    if any(item in set(blocker_states) for item in {"accepted_tax_truth_missing", "account_regime_missing", "basis_unknown", "holding_period_unknown"}):
        completeness_state = "incomplete_basis"
    elif "TAX_SAFE_DEGRADED_MODE" in set(snapshot.get("reason_codes") or ()) or (positions_ref.payload.get("status") != "OK"):
        completeness_state = "degraded_runtime"
    freshness_state = _freshness_state(
        day_utc=scope.day_utc,
        latest_source_utc=_latest_timestamp(
            str(nav_ref.payload.get("produced_utc") or ""),
            str((cash_ref.payload.get("snapshot") or {}).get("observed_at_utc") or ""),
            str(positions_ref.payload.get("produced_utc") or ""),
            *(str(fact.get("recorded_at") or "") for fact in accepted_facts),
        ),
        evaluated_at_utc=evaluated_utc,
    )
    matrix = evaluate_tax_state_precedence_v1(
        completeness_state=completeness_state,
        freshness_state=freshness_state,
        blocker_states=blocker_states,
        opportunity_states=opportunity_states,
    )
    effective_blockers = list(matrix["effective_blocker_states"])
    effective_opportunities = list(matrix["effective_opportunity_states"])
    authority_label = "governed_deterministic_tax_state"
    governing_refs = [
        _plain_ref_from_surface(nav_ref, "accounting_nav_v2"),
        _plain_ref_from_surface(cash_ref, "cash_ledger_snapshot_v1"),
        _plain_ref_from_surface(positions_ref, "positions_snapshot_v5"),
        account_registry_ref,
        *([journal_ref] if journal_ref is not None else []),
    ]
    primary_explanation = map_tax_state_explanation_v1(
        completeness_state=completeness_state,
        freshness_state=freshness_state,
        blocker_states=effective_blockers,
        opportunity_states=effective_opportunities,
        degraded_reason_id=str(matrix["primary_rule_id"]),
        evidence_refs=governing_refs,
        authority_label=authority_label,
        detail_fields={
            "tax_scope_id": tax_scope,
            "snapshot_reason_codes": list(snapshot.get("reason_codes") or []),
            "manifest_id": str(manifest.get("build_manifest_id") or ""),
        },
    )
    lot_level_entries = {
        "status": "available" if snapshot.get("lot_states") else "unavailable",
        "total_lots": len(snapshot.get("lot_states") or []),
        "items": [
            {
                "lot_id": str(row.get("lot_id") or ""),
                "account_id": str(row.get("account_id") or ""),
                "security_id": str(row.get("security_id") or ""),
                "remaining_quantity": str(row.get("remaining_quantity") or ""),
                "basis_total": str(row.get("basis_total") or ""),
                "holding_period_state": str(row.get("holding_period_state") or "unknown"),
                "confidence_state": str(row.get("confidence_state") or "unknown"),
                "maturity_state": str(row.get("maturity_state") or "unverified"),
                "restriction_flags": list(row.get("restriction_flags") or []),
                "data_quality_flags": list(row.get("data_quality_flags") or []),
                "reason_codes": list(row.get("reason_codes") or []),
                "source_kind": "ACCEPTED_TAX_FACT",
            }
            for row in snapshot.get("lot_states") or ()
        ],
        "source_refs": [dict(ref) for ref in governing_refs],
    }
    harvest_candidates = _harvest_candidates_payload_v1(
        candidates=(harvest_decision or {}).get("candidate_lots") or (),
        opportunity_visibility=str(matrix["opportunity_visibility"]),
        evidence_refs=governing_refs,
    )
    tax_state_id = hashlib.sha256(
        canonical_json_bytes_v1(
            {
                "artifact_id": ARTIFACT_ID,
                "scope_id": scope.scope_id,
                "tax_scope_id": tax_scope,
                "day_utc": scope.day_utc,
                "completeness_state": completeness_state,
                "freshness_state": freshness_state,
                "primary_rule_id": matrix["primary_rule_id"],
                "governing_refs": governing_refs,
                "blocker_states": effective_blockers,
                "opportunity_states": effective_opportunities,
            }
        )
    ).hexdigest()
    predicted_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=ARTIFACT_ID,
        day_utc=scope.day_utc,
        canonical_truth_root=scope.canonical_truth_root,
        extra_variables={"scope_id": scope.scope_id, "tax_state_id": tax_state_id},
    )
    payload = {
        "schema_id": "tax_state",
        "schema_version": "v1",
        "tax_state_id": tax_state_id,
        "artifact_id": ARTIFACT_ID,
        "kernel_version": KERNEL_VERSION,
        "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
        "generated_at_utc": evaluated_utc,
        "authority_label": authority_label,
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "tax_scope_id": tax_scope,
        "operation_type": scope.operation_type,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "completeness_state": completeness_state,
        "freshness_state": freshness_state,
        "visibility_state": matrix["visibility_state"],
        "blocker_states": effective_blockers,
        "opportunity_states": effective_opportunities,
        "lot_basis_state": _state_quality_from_lots(snapshot.get("lot_states") or (), "TAX_BASIS_UNKNOWN"),
        "holding_period_state": _state_quality_from_lots(snapshot.get("lot_states") or (), "TAX_HOLDING_PERIOD_UNKNOWN"),
        "wash_sale_state": _wash_sale_state_v1(snapshot, effective_blockers),
        "primary_rule_id": matrix["primary_rule_id"],
        "primary_explanation": primary_explanation,
        "historical_visibility": matrix["historical_visibility"],
        "advisory_binding_state": {
            "effect_state": matrix["advisory_effect_state"],
            "completeness_state": completeness_state,
            "freshness_state": freshness_state,
            "blocker_states": effective_blockers,
            "opportunity_states": effective_opportunities,
            "binding_reason_id": matrix["primary_rule_id"],
            "tax_state_ref": _plain_ref(
                artifact_id=ARTIFACT_ID,
                path=predicted_path,
                sha256="PENDING_SHA256",
            ),
        },
        "account_tax_profiles": _account_profiles_v1(
            positions_payload=positions_ref.payload,
            snapshot=snapshot,
            account_registry_payload=account_registry_payload,
            evidence_refs=governing_refs,
        ),
        "realized_unrealized_tax_posture": _sum_realized_unrealized_v1(
            accepted_facts=accepted_facts,
            snapshot=snapshot,
            market_prices=market_prices,
            evidence_refs=governing_refs,
        ),
        "lot_level_entries": lot_level_entries,
        "harvesting_candidates": harvest_candidates,
        "governing_refs": governing_refs,
        "evidence_refs": governing_refs,
        "semantic_events": [],
    }
    prior_ref = find_latest_tax_state_v1(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    if prior_ref is not None and str(prior_ref.payload.get("tax_state_id") or "").strip() != tax_state_id:
        payload["supersedes_ref"] = _plain_ref_from_surface(prior_ref, ARTIFACT_ID)
    payload["advisory_binding_state"]["tax_state_ref"]["artifact_sha256"] = tax_state_id

    semantic_events = [
        _semantic_event(
            event_type="tax_state_computed",
            authority_label=authority_label,
            completeness_state=completeness_state,
            freshness_state=freshness_state,
            visibility_state=str(matrix["visibility_state"]),
            blocker_states=effective_blockers,
            opportunity_states=effective_opportunities,
            degraded_reason_id=str(matrix["primary_rule_id"]),
            governing_refs=governing_refs,
            kernel_version=KERNEL_VERSION,
        )
    ]
    if completeness_state != "complete":
        semantic_events.append(
            _semantic_event(
                event_type="tax_state_degraded",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=effective_opportunities,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    if effective_blockers:
        semantic_events.append(
            _semantic_event(
                event_type="tax_blocker_raised",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=effective_opportunities,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    elif prior_ref is not None and list(prior_ref.payload.get("blocker_states") or ()):
        semantic_events.append(
            _semantic_event(
                event_type="tax_blocker_cleared",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=effective_opportunities,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    if effective_opportunities:
        semantic_events.append(
            _semantic_event(
                event_type="tax_opportunity_visible",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=effective_opportunities,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    elif opportunity_states:
        semantic_events.append(
            _semantic_event(
                event_type="tax_opportunity_suppressed",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=opportunity_states,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    if str(matrix["visibility_state"]) == "historical_only":
        semantic_events.append(
            _semantic_event(
                event_type="tax_state_historical_only",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=effective_opportunities,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    if str(matrix["advisory_effect_state"]) in {"downgrade", "block"}:
        semantic_events.append(
            _semantic_event(
                event_type="tax_advisory_downgraded_due_to_tax",
                authority_label=authority_label,
                completeness_state=completeness_state,
                freshness_state=freshness_state,
                visibility_state=str(matrix["visibility_state"]),
                blocker_states=effective_blockers,
                opportunity_states=effective_opportunities,
                degraded_reason_id=str(matrix["primary_rule_id"]),
                governing_refs=governing_refs,
                kernel_version=KERNEL_VERSION,
            )
        )
    payload["semantic_events"] = semantic_events

    emitted_ref = None
    if emit_artifact:
        assert_constitutional_writer_allowed_v1(REPO_ROOT, ARTIFACT_ID, WRITER_ID)
        emitted_ref = atomic_write_idempotent_validated_json_v1(
            path=predicted_path,
            payload=payload,
            schema_relpath=SCHEMA_RELPATH,
            volatile_field_names=("generated_at_utc",),
        )
    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "tax_state": payload,
        "artifact_ref": _plain_ref_from_surface(emitted_ref, ARTIFACT_ID) if emitted_ref is not None else None,
    }
