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
    *,
    nav_doc: Dict[str, Any],
    positions_doc: Dict[str, Any],
    nav_ref: Dict[str, Any],
    positions_ref: Dict[str, Any],
    as_of_utc: Optional[str],
) -> Dict[str, Any]:
    nav_section = nav_doc.get("nav") if isinstance(nav_doc.get("nav"), dict) else {}
    nav_components = nav_section.get("components") if isinstance(nav_section.get("components"), list) else []
    position_items = positions_doc.get("items") if isinstance(positions_doc.get("items"), list) else []

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
        if str(component.get("kind") or "").strip().upper() == "CASH":
            continue
        symbol = _string_or_none(component.get("symbol")) or "UNKNOWN"
        mark = component.get("mark") if isinstance(component.get("mark"), dict) else {}
        account_ids = account_ids_by_symbol.get(symbol, [])
        items.append(
            {
                "entity_id": f"holding:{symbol}",
                "symbol": symbol,
                "kind": _string_or_none(component.get("kind")) or "UNKNOWN",
                "quantity": component.get("qty"),
                "market_value_usd": _usd_from_numeric(component.get("mv")),
                "mark_last": _usd_from_numeric(mark.get("last")),
                "mark_source": _string_or_none(mark.get("source")),
                "account_ids": account_ids,
                "truth_state": "canonical",
                "as_of_utc": _string_or_none(mark.get("asof_utc")) or as_of_utc,
                "freshness_state": freshness_state(_string_or_none(mark.get("asof_utc")) or as_of_utc),
                "source_authority": ["accounting_nav_v2", "positions_snapshot_v5"],
                "provenance_refs": evidence_refs(nav_ref, positions_ref),
                "degradation_codes": [] if account_ids else ["HOLDING_ACCOUNT_MAPPING_UNPROVEN"],
            }
        )

    return {
        "status": "OK" if items else "DEGRADED",
        "total_holdings": len(items),
        "items": items,
        "warnings": [] if items else ["ACCOUNTING_NAV_COMPONENTS_EMPTY"],
        "source_refs": evidence_refs(nav_ref, positions_ref),
    }


def build_financial_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    if not resolved_day:
        return _fail_closed_payload(
            resolved_day=None,
            as_of_utc=None,
            source_refs_list=[],
            provenance_refs_list=[],
            authority_inputs=[],
            reason_codes=["FINANCIAL_STATE_DAY_UNRESOLVED"],
            runtime_environment="UNKNOWN",
        )

    nav_path = (GLOBAL_TRUTH_ROOT / "accounting_v2" / "nav" / resolved_day / "nav.v2.json").resolve()
    cash_path = (GLOBAL_TRUTH_ROOT / "cash_ledger_v1" / "snapshots" / resolved_day / "cash_ledger_snapshot.v1.json").resolve()
    positions_path = (SLEEVE_TRUTH_ROOT / "positions_v1" / "snapshots" / resolved_day / "positions_snapshot.v5.json").resolve()
    exposure_path = (SLEEVE_TRUTH_ROOT / "risk_v1" / "exposure_net_v1" / resolved_day / "exposure_net.v1.json").resolve()
    reserve_path = (
        SLEEVE_TRUTH_ROOT / "risk_v1" / "portfolio_governance_snapshot_v1" / resolved_day / "portfolio_governance_snapshot.v1.json"
    ).resolve()
    account_registry_path = (REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()

    nav_doc, nav_ref, nav_input, nav_error = _load_required_artifact(path=nav_path, label="Accounting NAV", artifact_type="accounting_nav_v2")
    cash_doc, cash_ref, cash_input, cash_error = _load_required_artifact(path=cash_path, label="Cash Ledger Snapshot", artifact_type="cash_ledger_snapshot_v1")
    positions_doc, positions_ref, positions_input, positions_error = _load_required_artifact(path=positions_path, label="Positions Snapshot v5", artifact_type="positions_snapshot_v5")
    exposure_doc, exposure_ref, exposure_input, exposure_error = _load_required_artifact(path=exposure_path, label="Exposure Net", artifact_type="exposure_net_v1")
    reserve_doc, reserve_ref, reserve_input, reserve_error = _load_required_artifact(
        path=reserve_path,
        label="Portfolio Governance Snapshot",
        artifact_type="portfolio_governance_snapshot_v1",
    )
    registry_doc, registry_ref, registry_input, registry_error = _load_required_artifact(
        path=account_registry_path,
        label="IB Account Registry",
        artifact_type="c2_ib_account_registry",
    )

    authority_inputs = [nav_input, cash_input, positions_input, exposure_input, reserve_input, registry_input]
    source_refs_list = evidence_refs(nav_ref, cash_ref, positions_ref, exposure_ref, reserve_ref, registry_ref)
    provenance_refs_list = list(source_refs_list)
    provisional_as_of = latest_timestamp(
        None if nav_ref is None else nav_ref.get("last_update_utc"),
        None if cash_ref is None else cash_ref.get("last_update_utc"),
        None if positions_ref is None else positions_ref.get("last_update_utc"),
        None if exposure_ref is None else exposure_ref.get("last_update_utc"),
        None if reserve_ref is None else reserve_ref.get("last_update_utc"),
    )

    load_errors = [code for code in (nav_error, cash_error, positions_error, exposure_error, reserve_error, registry_error) if code]
    if load_errors:
        return _fail_closed_payload(
            resolved_day=resolved_day,
            as_of_utc=provisional_as_of,
            source_refs_list=source_refs_list,
            provenance_refs_list=provenance_refs_list,
            authority_inputs=authority_inputs,
            reason_codes=load_errors,
            runtime_environment="UNKNOWN",
        )

    account_registry, registry_errors = _load_account_registry(registry_doc)
    validation_errors: List[str] = list(registry_errors)

    nav_section = nav_doc.get("nav") if isinstance(nav_doc.get("nav"), dict) else {}
    cash_snapshot = cash_doc.get("snapshot") if isinstance(cash_doc.get("snapshot"), dict) else {}
    positions_accounts = positions_doc.get("accounts") if isinstance(positions_doc.get("accounts"), list) else []
    positions_reconciliation = positions_doc.get("reconciliation") if isinstance(positions_doc.get("reconciliation"), dict) else {}
    exposure_portfolio = exposure_doc.get("portfolio") if isinstance(exposure_doc.get("portfolio"), dict) else {}

    if not nav_section:
        validation_errors.append("ACCOUNTING_NAV_V2_NAV_BLOCK_INVALID")
    if not cash_snapshot:
        validation_errors.append("CASH_LEDGER_SNAPSHOT_BLOCK_INVALID")
    if not positions_accounts:
        validation_errors.append("POSITIONS_SNAPSHOT_V5_ACCOUNTS_INVALID")
    if not isinstance(positions_doc.get("items"), list):
        validation_errors.append("POSITIONS_SNAPSHOT_V5_ITEMS_INVALID")
    if not exposure_portfolio:
        validation_errors.append("EXPOSURE_NET_V1_PORTFOLIO_BLOCK_INVALID")

    investable_assets_total_usd = _usd_from_numeric(nav_section.get("nav_total"))
    investable_cash_total_usd = _usd_from_numeric(nav_section.get("cash_total"))
    gross_positions_value_usd = _usd_from_numeric(nav_section.get("gross_positions_value"))
    if investable_assets_total_usd is None:
        validation_errors.append("ACCOUNTING_NAV_V2_NAV_TOTAL_UNPROVEN")
    if investable_cash_total_usd is None:
        validation_errors.append("ACCOUNTING_NAV_V2_CASH_TOTAL_UNPROVEN")
    if gross_positions_value_usd is None:
        validation_errors.append("ACCOUNTING_NAV_V2_GROSS_POSITIONS_VALUE_UNPROVEN")

    cash_account_id = _string_or_none(cash_snapshot.get("account_id"))
    cash_total_cents = _int_from_numeric(cash_snapshot.get("cash_total_cents"))
    nlv_total_cents = _int_from_numeric(cash_snapshot.get("nlv_total_cents"))
    if cash_account_id is None:
        validation_errors.append("CASH_LEDGER_ACCOUNT_ID_UNPROVEN")
    if cash_total_cents is None:
        validation_errors.append("CASH_LEDGER_TOTAL_UNPROVEN")
    if nlv_total_cents is None:
        validation_errors.append("CASH_LEDGER_NLV_TOTAL_UNPROVEN")

    positions_account_rows = [row for row in positions_accounts if isinstance(row, dict)]
    positions_account_ids = {
        account_id
        for row in positions_account_rows
        for account_id in [_string_or_none(row.get("account_id"))]
        if account_id
    }
    if not positions_account_ids:
        validation_errors.append("POSITIONS_SNAPSHOT_V5_ACCOUNT_IDS_UNPROVEN")

    if cash_account_id and positions_account_ids and cash_account_id not in positions_account_ids:
        validation_errors.append("CASH_LEDGER_ACCOUNT_NOT_PRESENT_IN_POSITIONS_SNAPSHOT")
    if cash_account_id and account_registry and cash_account_id not in account_registry:
        validation_errors.append("C2_IB_ACCOUNT_REGISTRY_MISSING_CASH_ACCOUNT")
    for account_id in sorted(positions_account_ids):
        if account_id not in account_registry:
            validation_errors.append(f"C2_IB_ACCOUNT_REGISTRY_MISSING_ACCOUNT:{account_id}")

    if cash_account_id and cash_total_cents is not None:
        matching_account = next((row for row in positions_account_rows if _string_or_none(row.get("account_id")) == cash_account_id), None)
        matching_cash = None if matching_account is None else _int_from_numeric(matching_account.get("cash_total_cents"))
        if matching_cash is None:
            validation_errors.append("POSITIONS_SNAPSHOT_V5_ACCOUNT_CASH_TOTAL_UNPROVEN")
        elif matching_cash != cash_total_cents:
            validation_errors.append("POSITIONS_CASH_TOTAL_MISMATCH")

    by_symbol = exposure_portfolio.get("by_symbol") if isinstance(exposure_portfolio.get("by_symbol"), list) else []
    if _usd_from_numeric(exposure_portfolio.get("gross_notional_usd")) is None:
        validation_errors.append("EXPOSURE_NET_GROSS_NOTIONAL_UNPROVEN")
    if _usd_from_numeric(exposure_portfolio.get("net_notional_usd")) is None:
        validation_errors.append("EXPOSURE_NET_NET_NOTIONAL_UNPROVEN")
    if _int_from_numeric(exposure_portfolio.get("capital_at_risk_cents")) is None:
        validation_errors.append("EXPOSURE_NET_CAPITAL_AT_RISK_UNPROVEN")
    if not isinstance(exposure_portfolio.get("by_symbol"), list):
        validation_errors.append("EXPOSURE_NET_BY_SYMBOL_UNPROVEN")

    reserve_buffer_cents = _int_from_numeric(reserve_doc.get("reserve_buffer"))
    risk_budget_total_cents = _int_from_numeric(reserve_doc.get("risk_budget_total"))
    risk_budget_effective_cents = _int_from_numeric(reserve_doc.get("risk_budget_effective"))
    drawdown_scaling = _string_or_none(reserve_doc.get("drawdown_scaling"))
    correlation_compression = _string_or_none(reserve_doc.get("correlation_compression"))
    hard_stop_state = _string_or_none(reserve_doc.get("hard_stop_state"))
    reserve_as_of_utc = _string_or_none(reserve_doc.get("produced_utc"))
    if reserve_buffer_cents is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_RESERVE_BUFFER_UNPROVEN")
    if risk_budget_total_cents is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_RISK_BUDGET_TOTAL_UNPROVEN")
    if risk_budget_effective_cents is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_RISK_BUDGET_EFFECTIVE_UNPROVEN")
    if drawdown_scaling is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_DRAWDOWN_SCALING_UNPROVEN")
    if correlation_compression is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_CORRELATION_COMPRESSION_UNPROVEN")
    if hard_stop_state is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_HARD_STOP_STATE_UNPROVEN")
    if reserve_as_of_utc is None:
        validation_errors.append("PORTFOLIO_GOVERNANCE_PRODUCED_UTC_UNPROVEN")

    as_of_utc = latest_timestamp(
        _string_or_none(nav_doc.get("produced_utc")),
        _string_or_none(cash_snapshot.get("observed_at_utc")),
        _string_or_none(positions_doc.get("produced_utc")),
        iso_from_mtime(exposure_path),
        reserve_as_of_utc,
    )

    if validation_errors:
        return _fail_closed_payload(
            resolved_day=resolved_day,
            as_of_utc=as_of_utc,
            source_refs_list=source_refs_list,
            provenance_refs_list=provenance_refs_list,
            authority_inputs=authority_inputs,
            reason_codes=validation_errors,
            runtime_environment=_runtime_environment(account_registry),
        )

    account_rows: List[Dict[str, Any]] = []
    for account in positions_account_rows:
        account_id = _string_or_none(account.get("account_id"))
        if not account_id:
            continue
        registry_row = account_registry.get(account_id, {})
        buying_power_usd = None
        if cash_account_id == account_id:
            buying_power_usd = _usd_from_cents(_int_from_numeric(cash_snapshot.get("available_funds_cents")))
        account_rows.append(
            {
                "entity_id": account_id,
                "account_id": account_id,
                "full_account_number": account_id,
                "institution": _string_or_none(registry_row.get("institution")),
                "account_type": _string_or_none(registry_row.get("account_type")),
                "tax_classification": _string_or_none(registry_row.get("tax_classification")),
                "base_currency": _string_or_none(account.get("currency")) or _string_or_none(cash_snapshot.get("currency")) or "USD",
                "cash_usd": _usd_from_cents(_int_from_numeric(account.get("cash_total_cents"))),
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
                    "institution_proven": _string_or_none(registry_row.get("institution")) is not None,
                    "account_type_proven": _string_or_none(registry_row.get("account_type")) is not None,
                    "tax_classification_proven": _string_or_none(registry_row.get("tax_classification")) is not None,
                    "market_value_proven": False,
                    "buying_power_proven": buying_power_usd is not None,
                },
                "truth_state": "canonical",
                "as_of_utc": as_of_utc,
                "freshness_state": freshness_state(as_of_utc),
                "source_authority": ["positions_snapshot_v5", "cash_ledger_snapshot_v1", "c2_ib_account_registry"],
                "provenance_refs": evidence_refs(positions_ref, cash_ref, registry_ref),
                "degradation_codes": ["ACCOUNT_MARKET_VALUE_UNPROVEN"]
                + ([] if _string_or_none(registry_row.get("institution")) is not None else ["ACCOUNT_INSTITUTION_UNPROVEN"])
                + ([] if _string_or_none(registry_row.get("account_type")) is not None else ["ACCOUNT_TYPE_UNPROVEN"])
                + ([] if _string_or_none(registry_row.get("tax_classification")) is not None else ["ACCOUNT_TAX_CLASSIFICATION_UNPROVEN"])
                + ([] if buying_power_usd is not None else ["ACCOUNT_BUYING_POWER_UNPROVEN"]),
            }
        )

    holdings_rollup = _build_holdings_rollup(
        nav_doc=nav_doc,
        positions_doc=positions_doc,
        nav_ref=nav_ref,
        positions_ref=positions_ref,
        as_of_utc=as_of_utc,
    )

    concentration_rows: List[Dict[str, Any]] = []
    sorted_symbols = sorted(
        [row for row in by_symbol if isinstance(row, dict)],
        key=lambda row: abs(_usd_from_numeric(row.get("gross_notional_usd")) or 0.0),
        reverse=True,
    )
    for row in sorted_symbols[:5]:
        symbol = _string_or_none(row.get("symbol")) or "UNKNOWN"
        concentration_rows.append(
            {
                "entity_id": f"concentration:{symbol}",
                "symbol": symbol,
                "gross_notional_usd": _usd_from_numeric(row.get("gross_notional_usd")),
                "net_notional_usd": _usd_from_numeric(row.get("net_notional_usd")),
                "capital_at_risk_usd": _usd_from_cents(_int_from_numeric(row.get("capital_at_risk_cents"))),
                "sector": _string_or_none(row.get("sector")),
                "truth_state": "canonical",
                "as_of_utc": as_of_utc,
                "freshness_state": freshness_state(as_of_utc),
                "source_authority": ["exposure_net_v1"],
                "provenance_refs": evidence_refs(exposure_ref),
                "degradation_codes": [],
            }
        )

    payload = view_envelope(
        view_name="financial_state",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=markers("canonical"),
        source_refs=source_refs_list,
        surface_kind="projection",
        entity_scope="financial_state",
        truth_state="canonical",
        data_condition="fresh",
        source_authority=list(REQUIRED_SOURCE_AUTHORITY),
        contract_id=CONTRACT_ID,
        contract_version=CONTRACT_VERSION,
        provenance_refs=provenance_refs_list,
        degradation_codes=[],
        authority_inputs=authority_inputs,
        current_day=resolved_day,
        financial_status="OK",
        financial_warnings=[],
        runtime_context={
            "environment": _runtime_environment(account_registry),
            "current_day": resolved_day,
            "global_truth_root": str(GLOBAL_TRUTH_ROOT),
            "sleeve_truth_root": str(SLEEVE_TRUTH_ROOT),
        },
        investable_summary={
            "status": "OK",
            "investable_assets_total_usd": investable_assets_total_usd,
            "investable_cash_total_usd": investable_cash_total_usd,
            "gross_positions_value_usd": gross_positions_value_usd,
            "realized_pnl_to_date_usd": _usd_from_numeric(nav_section.get("realized_pnl_to_date")),
            "unrealized_pnl_usd": _usd_from_numeric(nav_section.get("unrealized_pnl")),
            "currency": _string_or_none(nav_section.get("currency")) or "USD",
            "as_of_utc": _string_or_none(nav_doc.get("produced_utc")) or as_of_utc,
            "source_refs": evidence_refs(nav_ref),
            "warnings": [],
        },
        account_rollups=account_rows,
        account_rollup_summary={
            "status": "OK",
            "total_accounts": len(account_rows),
            "source_refs": evidence_refs(positions_ref, cash_ref, registry_ref),
            "warnings": [],
        },
        holdings_rollup=holdings_rollup,
        liquidity_summary={
            "status": "OK",
            "cash_total_usd": _usd_from_cents(cash_total_cents),
            "available_funds_usd": _usd_from_cents(_int_from_numeric(cash_snapshot.get("available_funds_cents"))),
            "buying_power_usd": _usd_from_cents(_int_from_numeric(cash_snapshot.get("available_funds_cents"))),
            "net_liquidation_value_usd": _usd_from_cents(nlv_total_cents),
            "excess_liquidity_usd": _usd_from_cents(_int_from_numeric(cash_snapshot.get("excess_liquidity_cents"))),
            "status": _string_or_none(cash_doc.get("status")) or "UNKNOWN",
            "source_refs": evidence_refs(cash_ref),
            "warnings": [],
        },
        reserve_summary={
            "status": "PROVEN",
            "reserve_buffer_usd": _usd_from_cents(reserve_buffer_cents),
            "risk_budget_total_usd": _usd_from_cents(risk_budget_total_cents),
            "risk_budget_effective_usd": _usd_from_cents(risk_budget_effective_cents),
            "drawdown_scaling": drawdown_scaling,
            "correlation_compression": correlation_compression,
            "hard_stop_state": hard_stop_state,
            "as_of_utc": reserve_as_of_utc,
            "source_refs": evidence_refs(reserve_ref),
            "warnings": [],
        },
        exposure_summary={
            "status": "OK",
            "gross_notional_usd": _usd_from_numeric(exposure_portfolio.get("gross_notional_usd")),
            "net_notional_usd": _usd_from_numeric(exposure_portfolio.get("net_notional_usd")),
            "capital_at_risk_usd": _usd_from_cents(_int_from_numeric(exposure_portfolio.get("capital_at_risk_cents"))),
            "symbol_count": exposure_portfolio.get("symbol_count"),
            "by_symbol": concentration_rows,
            "source_refs": evidence_refs(exposure_ref),
            "warnings": [],
        },
        concentration_summary={
            "status": "OK" if concentration_rows else "DEGRADED",
            "top_symbol_exposures": concentration_rows,
            "source_refs": evidence_refs(exposure_ref),
            "warnings": [] if concentration_rows else ["CONCENTRATION_DATA_UNPROVEN"],
        },
        backing_status={
            "status": "OK",
            "positions_reconciliation_status": _string_or_none(positions_reconciliation.get("positions_status")) or "UNKNOWN",
            "cash_reconciliation_status": _string_or_none(positions_reconciliation.get("cash_status")) or "UNKNOWN",
            "portfolio_governance_status": "PROVEN",
            "reserve_buffer_usd": _usd_from_cents(reserve_buffer_cents),
            "source_refs": evidence_refs(positions_ref, reserve_ref),
            "warnings": [],
        },
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload
