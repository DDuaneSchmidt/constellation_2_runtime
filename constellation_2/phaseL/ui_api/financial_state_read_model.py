from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .common import (
    GLOBAL_TRUTH_ROOT,
    REPO_ROOT,
    SLEEVE_TRUTH_ROOT,
    evidence_ref,
    freshness_state,
    iso_from_mtime,
    latest_timestamp,
    normalize_symbol,
    read_json_dict,
    resolve_ui_day,
)
from .dto import evidence_refs, markers, view_envelope
from .metadata_contract import validate_projection_envelope


CONTRACT_ID = "financial_state_v1"
CONTRACT_VERSION = "v1"
REQUIRED_SOURCE_AUTHORITY = (
    "accounting_nav_v2",
    "cash_ledger_snapshot_v1",
    "positions_snapshot_v5",
    "exposure_net_v1",
    "portfolio_governance_snapshot_v1",
    "c2_ib_account_registry",
)


def _usd_from_cents(value: Any) -> Optional[float]:
    if isinstance(value, int):
        return value / 100.0
    return None


def _usd_from_numeric(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _int_from_numeric(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _string_or_none(value: Any) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _artifact_ref(path: Path, *, label: str, artifact_type: str) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    return evidence_ref(path, label=label, artifact_type=artifact_type)


def _input_entry(
    *,
    path: Path,
    label: str,
    artifact_type: str,
    doc: Optional[Dict[str, Any]],
    err: Optional[str],
) -> Dict[str, Any]:
    ref = _artifact_ref(path, label=label, artifact_type=artifact_type)
    return {
        "artifact_type": artifact_type,
        "label": label,
        "path": str(path),
        "required": True,
        "status": "PROVEN" if doc is not None else (err or "UNREADABLE"),
        "last_update_utc": None if ref is None else ref.get("last_update_utc"),
        "provenance_ref": ref,
    }


def _load_required_artifact(
    *,
    path: Path,
    label: str,
    artifact_type: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    doc, err = read_json_dict(path)
    ref = _artifact_ref(path, label=label, artifact_type=artifact_type)
    entry = _input_entry(path=path, label=label, artifact_type=artifact_type, doc=doc, err=err)
    return doc, ref, entry, (None if doc is not None else f"{artifact_type.upper()}_{err or 'UNREADABLE'}")


def _runtime_environment(account_registry: Dict[str, Dict[str, Any]]) -> str:
    for registry_row in account_registry.values():
        candidate = _string_or_none(registry_row.get("environment"))
        if candidate:
            return candidate
    return "UNKNOWN"


def _load_account_registry(registry_doc: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    accounts = registry_doc.get("accounts")
    if not isinstance(accounts, list):
        return {}, ["C2_IB_ACCOUNT_REGISTRY_ACCOUNTS_INVALID"]
    by_id: Dict[str, Dict[str, Any]] = {}
    for row in accounts:
        if not isinstance(row, dict):
            continue
        account_id = _string_or_none(row.get("account_id"))
        if account_id:
            by_id[account_id] = row
    if not by_id:
        return {}, ["C2_IB_ACCOUNT_REGISTRY_EMPTY"]
    return by_id, []


def _fail_closed_payload(
    *,
    resolved_day: Optional[str],
    as_of_utc: Optional[str],
    source_refs_list: List[Dict[str, Any]],
    provenance_refs_list: List[Dict[str, Any]],
    authority_inputs: List[Dict[str, Any]],
    reason_codes: List[str],
    runtime_environment: str,
) -> Dict[str, Any]:
    payload = view_envelope(
        view_name="financial_state",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=markers("fail_closed"),
        source_refs=source_refs_list,
        surface_kind="projection",
        entity_scope="financial_state",
        truth_state="fail_closed",
        data_condition="fail_closed",
        source_authority=list(REQUIRED_SOURCE_AUTHORITY),
        contract_id=CONTRACT_ID,
        contract_version=CONTRACT_VERSION,
        provenance_refs=provenance_refs_list,
        degradation_codes=reason_codes,
        authority_inputs=authority_inputs,
        current_day=resolved_day,
        financial_status="FAIL_CLOSED",
        financial_warnings=reason_codes,
        runtime_context={
            "environment": runtime_environment,
            "current_day": resolved_day,
            "global_truth_root": str(GLOBAL_TRUTH_ROOT),
            "sleeve_truth_root": str(SLEEVE_TRUTH_ROOT),
        },
        investable_summary={
            "status": "FAIL_CLOSED",
            "investable_assets_total_usd": None,
            "investable_cash_total_usd": None,
            "gross_positions_value_usd": None,
            "realized_pnl_to_date_usd": None,
            "unrealized_pnl_usd": None,
            "currency": None,
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
        account_rollups=[],
        account_rollup_summary={
            "status": "FAIL_CLOSED",
            "total_accounts": 0,
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
        holdings_rollup={
            "status": "FAIL_CLOSED",
            "total_holdings": 0,
            "items": [],
            "warnings": reason_codes,
            "source_refs": source_refs_list,
        },
        liquidity_summary={
            "status": "FAIL_CLOSED",
            "cash_total_usd": None,
            "available_funds_usd": None,
            "buying_power_usd": None,
            "net_liquidation_value_usd": None,
            "excess_liquidity_usd": None,
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
        reserve_summary={
            "status": "FAIL_CLOSED",
            "reserve_buffer_usd": None,
            "risk_budget_total_usd": None,
            "risk_budget_effective_usd": None,
            "drawdown_scaling": None,
            "correlation_compression": None,
            "hard_stop_state": None,
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
        exposure_summary={
            "status": "FAIL_CLOSED",
            "gross_notional_usd": None,
            "net_notional_usd": None,
            "capital_at_risk_usd": None,
            "symbol_count": None,
            "by_symbol": [],
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
        concentration_summary={
            "status": "FAIL_CLOSED",
            "top_symbol_exposures": [],
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
        backing_status={
            "status": "FAIL_CLOSED",
            "positions_reconciliation_status": "UNKNOWN",
            "cash_reconciliation_status": "UNKNOWN",
            "portfolio_governance_status": "UNKNOWN",
            "reserve_buffer_usd": None,
            "source_refs": source_refs_list,
            "warnings": reason_codes,
        },
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload


def _build_holdings_rollup(
    nav_doc: Optional[Dict[str, Any]],
    positions_doc: Optional[Dict[str, Any]],
    nav_ref: Optional[Dict[str, Any]],
    positions_ref: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    nav_section = nav_doc.get("nav") if isinstance(nav_doc, dict) and isinstance(nav_doc.get("nav"), dict) else {}
    nav_components = nav_section.get("components") if isinstance(nav_section.get("components"), list) else []
    position_items = positions_doc.get("items") if isinstance(positions_doc, dict) and isinstance(positions_doc.get("items"), list) else []

    account_ids_by_symbol: Dict[str, List[str]] = {}
    for item in position_items:
        if not isinstance(item, dict):
            continue
        symbol = normalize_symbol(item)
        if not symbol:
            continue
        account_id = _string_or_none(item.get("account_id"))
        bucket = account_ids_by_symbol.setdefault(symbol, [])
        if account_id and account_id not in bucket:
            bucket.append(account_id)

    items: List[Dict[str, Any]] = []
    for component in nav_components:
        if not isinstance(component, dict):
            continue
        symbol = _string_or_none(component.get("symbol")) or "UNKNOWN"
        mark = component.get("mark") if isinstance(component.get("mark"), dict) else {}
        as_of_utc = _string_or_none(mark.get("asof_utc")) or _string_or_none(nav_doc.get("produced_utc") if isinstance(nav_doc, dict) else None)
        refs = evidence_refs(nav_ref, positions_ref)
        items.append(
            {
                "entity_id": f"holding:{symbol}",
                "symbol": symbol,
                "kind": _string_or_none(component.get("kind")) or "UNKNOWN",
                "quantity": component.get("qty"),
                "market_value_usd": _usd_from_numeric(component.get("mv")),
                "mark_last": _usd_from_numeric(mark.get("last")),
                "mark_source": _string_or_none(mark.get("source")),
                "account_ids": account_ids_by_symbol.get(symbol, []),
                "truth_state": "canonical" if nav_ref else "UNKNOWN",
                "as_of_utc": as_of_utc,
                "freshness_state": freshness_state(as_of_utc),
                "source_authority": ["accounting_nav_v2", "positions_snapshot_v5"],
                "provenance_refs": refs,
                "degradation_codes": [],
            }
        )

    return {
        "status": "OK" if items or nav_ref else "MISSING",
        "total_holdings": len(items),
        "items": items,
        "warnings": [] if items or nav_ref else ["ACCOUNTING_NAV_COMPONENTS_MISSING"],
        "source_refs": evidence_refs(nav_ref, positions_ref),
    }


def build_financial_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    nav_path = (GLOBAL_TRUTH_ROOT / "accounting_v2" / "nav" / (resolved_day or "UNKNOWN") / "nav.v2.json").resolve()
    cash_path = (GLOBAL_TRUTH_ROOT / "cash_ledger_v1" / "snapshots" / (resolved_day or "UNKNOWN") / "cash_ledger_snapshot.v1.json").resolve()
    positions_path = (SLEEVE_TRUTH_ROOT / "positions_v1" / "snapshots" / (resolved_day or "UNKNOWN") / "positions_snapshot.v5.json").resolve()
    exposure_path = (SLEEVE_TRUTH_ROOT / "risk_v1" / "exposure_net_v1" / (resolved_day or "UNKNOWN") / "exposure_net.v1.json").resolve()
    allocation_path = (
        SLEEVE_TRUTH_ROOT / "allocation_v1" / "capital_authority_allocation_v1" / (resolved_day or "UNKNOWN") / "capital_authority_allocation.v1.json"
    ).resolve()
    execution_reconciliation_path = (
        GLOBAL_TRUTH_ROOT / "reports" / "execution_reconciliation_v1" / (resolved_day or "UNKNOWN") / "execution_reconciliation.v1.json"
    ).resolve()

    nav_doc, nav_err = read_json_dict(nav_path)
    cash_doc, cash_err = read_json_dict(cash_path)
    positions_doc, positions_err = read_json_dict(positions_path)
    exposure_doc, exposure_err = read_json_dict(exposure_path)
    allocation_doc, allocation_err = read_json_dict(allocation_path)
    execution_reconciliation_doc, execution_reconciliation_err = read_json_dict(execution_reconciliation_path)
    account_registry, account_registry_path, account_registry_warnings = _load_account_registry()

    nav_ref = evidence_ref(nav_path if nav_doc else None, label="Accounting NAV", artifact_type="accounting_nav_v2")
    cash_ref = evidence_ref(cash_path if cash_doc else None, label="Cash Ledger Snapshot", artifact_type="cash_ledger_snapshot_v1")
    positions_ref = evidence_ref(positions_path if positions_doc else None, label="Positions Snapshot v5", artifact_type="positions_snapshot_v5")
    exposure_ref = evidence_ref(exposure_path if exposure_doc else None, label="Exposure Net", artifact_type="exposure_net_v1")
    allocation_ref = evidence_ref(
        allocation_path if allocation_doc else None,
        label="Capital Authority Allocation",
        artifact_type="capital_authority_allocation_v1",
    )
    execution_reconciliation_ref = evidence_ref(
        execution_reconciliation_path if execution_reconciliation_doc else None,
        label="Execution Reconciliation",
        artifact_type="execution_reconciliation_v1",
    )
    account_registry_ref = evidence_ref(
        account_registry_path if account_registry_path else None,
        label="IB Account Registry",
        artifact_type="c2_ib_account_registry",
    )

    nav_section = nav_doc.get("nav") if isinstance(nav_doc, dict) and isinstance(nav_doc.get("nav"), dict) else {}
    cash_snapshot = cash_doc.get("snapshot") if isinstance(cash_doc, dict) and isinstance(cash_doc.get("snapshot"), dict) else {}
    positions_accounts = positions_doc.get("accounts") if isinstance(positions_doc, dict) and isinstance(positions_doc.get("accounts"), list) else []
    positions_items = positions_doc.get("items") if isinstance(positions_doc, dict) and isinstance(positions_doc.get("items"), list) else []
    positions_reconciliation = positions_doc.get("reconciliation") if isinstance(positions_doc, dict) and isinstance(positions_doc.get("reconciliation"), dict) else {}
    exposure_portfolio = exposure_doc.get("portfolio") if isinstance(exposure_doc, dict) and isinstance(exposure_doc.get("portfolio"), dict) else {}
    allocation_portfolio = allocation_doc.get("portfolio") if isinstance(allocation_doc, dict) and isinstance(allocation_doc.get("portfolio"), dict) else {}

    as_of_utc = latest_timestamp(
        _string_or_none(nav_doc.get("produced_utc") if isinstance(nav_doc, dict) else None),
        _string_or_none(cash_snapshot.get("observed_at_utc")),
        _string_or_none(positions_doc.get("produced_utc") if isinstance(positions_doc, dict) else None),
        iso_from_mtime(exposure_path if exposure_doc else None) if exposure_doc else None,
        iso_from_mtime(allocation_path if allocation_doc else None) if allocation_doc else None,
    )

    financial_warnings: List[str] = []
    if nav_doc is None:
        financial_warnings.append(f"ACCOUNTING_NAV_{nav_err or 'UNREADABLE'}")
    if cash_doc is None:
        financial_warnings.append(f"CASH_LEDGER_{cash_err or 'UNREADABLE'}")
    if positions_doc is None:
        financial_warnings.append(f"POSITIONS_SNAPSHOT_V5_{positions_err or 'UNREADABLE'}")
    if exposure_doc is None:
        financial_warnings.append(f"EXPOSURE_NET_{exposure_err or 'UNREADABLE'}")
    if allocation_doc is None:
        financial_warnings.append(f"CAPITAL_AUTHORITY_ALLOCATION_{allocation_err or 'UNREADABLE'}")
    if execution_reconciliation_doc is None:
        financial_warnings.append(f"EXECUTION_RECONCILIATION_{execution_reconciliation_err or 'UNREADABLE'}")
    financial_warnings.extend(account_registry_warnings)

    investable_assets_total_usd = _usd_from_numeric(nav_section.get("nav_total"))
    investable_cash_total_usd = _usd_from_numeric(nav_section.get("cash_total"))
    gross_positions_value_usd = _usd_from_numeric(nav_section.get("gross_positions_value"))
    realized_pnl_to_date_usd = _usd_from_numeric(nav_section.get("realized_pnl_to_date"))
    unrealized_pnl_usd = _usd_from_numeric(nav_section.get("unrealized_pnl"))

    runtime_environment = None
    for registry_row in account_registry.values():
        candidate = _string_or_none(registry_row.get("environment"))
        if candidate:
            runtime_environment = candidate
            break

    account_rows: List[Dict[str, Any]] = []
    seen_account_ids: List[str] = []
    for account in positions_accounts:
        if not isinstance(account, dict):
            continue
        account_id = _string_or_none(account.get("account_id"))
        if not account_id:
            continue
        seen_account_ids.append(account_id)
        registry_row = account_registry.get(account_id, {})
        buying_power_usd = None
        if _string_or_none(cash_snapshot.get("account_id")) == account_id:
            buying_power_usd = _usd_from_cents(cash_snapshot.get("available_funds_cents"))
        refs = evidence_refs(positions_ref, cash_ref, account_registry_ref)
        account_rows.append(
            {
                "entity_id": account_id,
                "account_id": account_id,
                "full_account_number": account_id,
                "institution": None,
                "account_type": None,
                "tax_classification": None,
                "base_currency": _string_or_none(account.get("currency")) or _string_or_none(cash_snapshot.get("currency")) or "USD",
                "cash_usd": _usd_from_cents(account.get("cash_total_cents")),
                "market_value_usd": None,
                "buying_power_usd": buying_power_usd,
                "restrictions": {
                    "submission_enabled": bool(registry_row.get("enabled_for_submission")) if registry_row else None,
                    "allowed_engine_ids": registry_row.get("allowed_engine_ids") if isinstance(registry_row.get("allowed_engine_ids"), list) else [],
                    "allowed_sleeve_ids": registry_row.get("allowed_sleeve_ids") if isinstance(registry_row.get("allowed_sleeve_ids"), list) else [],
                    "allowed_symbols": registry_row.get("allowed_symbols") if isinstance(registry_row.get("allowed_symbols"), list) else [],
                    "notes": registry_row.get("notes") if isinstance(registry_row.get("notes"), list) else [],
                },
                "metadata_availability": {
                    "institution_proven": False,
                    "account_type_proven": False,
                    "tax_classification_proven": False,
                    "market_value_proven": False,
                    "buying_power_proven": buying_power_usd is not None,
                },
                "truth_state": "derived",
                "as_of_utc": as_of_utc,
                "freshness_state": freshness_state(as_of_utc),
                "source_authority": ["positions_snapshot_v5", "cash_ledger_snapshot_v1", "c2_ib_account_registry"],
                "provenance_refs": refs,
                "degradation_codes": [
                    "ACCOUNT_METADATA_UNAVAILABLE",
                    "ACCOUNT_MARKET_VALUE_UNAVAILABLE",
                ] + (["ACCOUNT_BUYING_POWER_UNAVAILABLE"] if buying_power_usd is None else []),
            }
        )

    holdings_rollup = _build_holdings_rollup(nav_doc, positions_doc, nav_ref, positions_ref)

    concentration_rows: List[Dict[str, Any]] = []
    by_symbol = exposure_portfolio.get("by_symbol") if isinstance(exposure_portfolio.get("by_symbol"), list) else []
    sorted_symbols = sorted(
        [row for row in by_symbol if isinstance(row, dict)],
        key=lambda row: abs(_usd_from_numeric(row.get("gross_notional_usd")) or 0.0),
        reverse=True,
    )
    for row in sorted_symbols[:5]:
        symbol = _string_or_none(row.get("symbol")) or "UNKNOWN"
        refs = evidence_refs(exposure_ref)
        concentration_rows.append(
            {
                "entity_id": f"concentration:{symbol}",
                "symbol": symbol,
                "gross_notional_usd": _usd_from_numeric(row.get("gross_notional_usd")),
                "net_notional_usd": _usd_from_numeric(row.get("net_notional_usd")),
                "capital_at_risk_usd": _usd_from_cents(row.get("capital_at_risk_cents")),
                "sector": _string_or_none(row.get("sector")),
                "truth_state": "canonical" if exposure_ref else "UNKNOWN",
                "as_of_utc": as_of_utc,
                "freshness_state": freshness_state(as_of_utc),
                "source_authority": ["exposure_net_v1"],
                "provenance_refs": refs,
                "degradation_codes": [],
            }
        )

    payload = view_envelope(
        view_name="financial_state",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=markers("canonical", "derived"),
        source_refs=evidence_refs(
            nav_ref,
            cash_ref,
            positions_ref,
            exposure_ref,
            allocation_ref,
            execution_reconciliation_ref,
            account_registry_ref,
        ),
        surface_kind="projection",
        entity_scope="financial_state",
        truth_state=_top_level_truth_state(
            evidence_refs(nav_ref, cash_ref, positions_ref, exposure_ref, account_registry_ref),
            financial_warnings,
        ),
        data_condition="degraded" if financial_warnings else "fresh",
        source_authority=[
            "accounting_nav_v2",
            "cash_ledger_snapshot_v1",
            "positions_snapshot_v5",
            "exposure_net_v1",
            "capital_authority_allocation_v1",
            "execution_reconciliation_v1",
            "c2_ib_account_registry",
        ],
        contract_id="financial_state_projection",
        contract_version="v1",
        provenance_refs=evidence_refs(
            nav_ref,
            cash_ref,
            positions_ref,
            exposure_ref,
            allocation_ref,
            execution_reconciliation_ref,
            account_registry_ref,
        ),
        degradation_codes=financial_warnings,
        current_day=resolved_day,
        financial_status=_status_from_warnings(financial_warnings),
        financial_warnings=financial_warnings,
        runtime_context={
            "environment": runtime_environment or "UNKNOWN",
            "current_day": resolved_day,
            "global_truth_root": str(GLOBAL_TRUTH_ROOT),
            "sleeve_truth_root": str(SLEEVE_TRUTH_ROOT),
            "source_refs": evidence_refs(account_registry_ref),
            "warnings": [] if runtime_environment else ["RUNTIME_ENVIRONMENT_UNAVAILABLE"],
        },
        investable_summary={
            "label": "investable_assets_total_usd",
            "investable_assets_total_usd": investable_assets_total_usd,
            "investable_cash_total_usd": investable_cash_total_usd,
            "gross_positions_value_usd": gross_positions_value_usd,
            "realized_pnl_to_date_usd": realized_pnl_to_date_usd,
            "unrealized_pnl_usd": unrealized_pnl_usd,
            "currency": _string_or_none(nav_section.get("currency")) or "USD",
            "source_refs": evidence_refs(nav_ref),
            "warnings": [] if nav_doc else [f"ACCOUNTING_NAV_{nav_err or 'UNREADABLE'}"],
        },
        account_rollups=account_rows,
        account_rollup_summary={
            "total_accounts": len(account_rows),
            "source_refs": evidence_refs(positions_ref, cash_ref, account_registry_ref),
            "warnings": [
                "ACCOUNT_METADATA_FIELDS_NOT_PROVEN"
            ] + ([] if account_rows else ["NO_ACCOUNT_ROWS_PRODUCED"]),
        },
        holdings_rollup=holdings_rollup,
        liquidity_summary={
            "cash_total_usd": _usd_from_cents(cash_snapshot.get("cash_total_cents")),
            "available_funds_usd": _usd_from_cents(cash_snapshot.get("available_funds_cents")),
            "buying_power_usd": _usd_from_cents(cash_snapshot.get("available_funds_cents")),
            "net_liquidation_value_usd": _usd_from_cents(cash_snapshot.get("nlv_total_cents")),
            "excess_liquidity_usd": _usd_from_cents(cash_snapshot.get("excess_liquidity_cents")),
            "status": _string_or_none(cash_doc.get("status") if isinstance(cash_doc, dict) else None) or "UNKNOWN",
            "source_refs": evidence_refs(cash_ref, positions_ref),
            "warnings": ([] if cash_doc else [f"CASH_LEDGER_{cash_err or 'UNREADABLE'}"])
            + (["AVAILABLE_FUNDS_UNAVAILABLE"] if cash_snapshot.get("available_funds_cents") is None else [])
            + (["EXCESS_LIQUIDITY_UNAVAILABLE"] if cash_snapshot.get("excess_liquidity_cents") is None else []),
        },
        reserve_summary={
            "status": "UNAVAILABLE_POLICY_REFERENCE",
            "cash_like_assets_usd": investable_cash_total_usd,
            "reserve_target_usd": None,
            "reserve_floor_usd": None,
            "source_refs": evidence_refs(cash_ref, allocation_ref),
            "warnings": [
                "RESERVE_POLICY_NOT_PROVEN_IN_PHASEL_UI_AUTHORITY",
            ],
        },
        exposure_summary={
            "gross_notional_usd": _usd_from_numeric(exposure_portfolio.get("gross_notional_usd")),
            "net_notional_usd": _usd_from_numeric(exposure_portfolio.get("net_notional_usd")),
            "capital_at_risk_usd": _usd_from_cents(exposure_portfolio.get("capital_at_risk_cents")),
            "symbol_count": exposure_portfolio.get("symbol_count"),
            "by_symbol": concentration_rows,
            "source_refs": evidence_refs(exposure_ref),
            "warnings": [] if exposure_doc else [f"EXPOSURE_NET_{exposure_err or 'UNREADABLE'}"],
        },
        concentration_summary={
            "top_symbol_exposures": concentration_rows,
            "source_refs": evidence_refs(exposure_ref),
            "warnings": [] if concentration_rows else ["CONCENTRATION_DATA_UNAVAILABLE"],
        },
        backing_status={
            "positions_reconciliation_status": _string_or_none(positions_reconciliation.get("positions_status")) or "UNKNOWN",
            "cash_reconciliation_status": _string_or_none(positions_reconciliation.get("cash_status")) or "UNKNOWN",
            "execution_reconciliation_status": _string_or_none(
                execution_reconciliation_doc.get("status") if isinstance(execution_reconciliation_doc, dict) else None
            ) or "UNKNOWN",
            "allocation_capital_at_risk_usd": _usd_from_cents(allocation_portfolio.get("used_capital_at_risk_cents")),
            "allocation_headroom_usd": _usd_from_cents(allocation_portfolio.get("headroom_cents")),
            "source_refs": evidence_refs(positions_ref, execution_reconciliation_ref, allocation_ref),
            "warnings": []
            if execution_reconciliation_doc and allocation_doc
            else [
                warning
                for warning in [
                    None if execution_reconciliation_doc else f"EXECUTION_RECONCILIATION_{execution_reconciliation_err or 'UNREADABLE'}",
                    None if allocation_doc else f"CAPITAL_AUTHORITY_ALLOCATION_{allocation_err or 'UNREADABLE'}",
                ]
                if warning
            ],
        },
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload


from . import financial_state_authority_v1 as _financial_state_authority_v1


def build_financial_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    _financial_state_authority_v1.GLOBAL_TRUTH_ROOT = GLOBAL_TRUTH_ROOT
    _financial_state_authority_v1.SLEEVE_TRUTH_ROOT = SLEEVE_TRUTH_ROOT
    _financial_state_authority_v1.REPO_ROOT = REPO_ROOT
    return _financial_state_authority_v1.build_financial_state_view(day)
