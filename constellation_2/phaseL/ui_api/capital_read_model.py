from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from constellation_2.common.capital.service_v1 import CapitalDomainServiceV1
from constellation_2.common.capital.storage_v1 import resolve_capital_db_path_v1

from .common import evidence_ref, freshness_state
from .dto import markers, view_envelope
from .metadata_contract import validate_projection_envelope


def _as_of_utc_from_day(day: str | None) -> str | None:
    if not isinstance(day, str) or not day:
        return None
    return f"{day}T00:00:00Z"


def _load_service_readonly() -> CapitalDomainServiceV1 | None:
    db_path = resolve_capital_db_path_v1()
    if not db_path.exists() or not db_path.is_file():
        return None
    return CapitalDomainServiceV1(db_path=db_path, actor="phaseL.ui_api.capital_read_model", ensure_schema=False)


def _db_ref(db_path: Path | None) -> list[dict[str, Any]]:
    if db_path is None:
        return []
    ref = evidence_ref(db_path, label="Capital SQLite DB", artifact_type="capital_domain_sqlite_v1")
    return [] if ref is None else [ref]


def _envelope(
    *,
    view_name: str,
    contract_id: str,
    payload: dict[str, Any],
    as_of_day: str | None,
    source_refs: list[dict[str, Any]],
    degraded_codes: list[str],
) -> dict[str, Any]:
    as_of_utc = _as_of_utc_from_day(as_of_day)
    data = view_envelope(
        view_name=view_name,
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc, stale_after_hours=24 * 3),
        provenance_markers=markers("capital_domain_v1", "derived_projection"),
        source_refs=source_refs,
        surface_kind="projection",
        entity_scope="capital",
        truth_state="degraded" if degraded_codes else "canonical",
        data_condition="degraded" if degraded_codes else "ok",
        source_authority=["capital_domain_sqlite_v1"],
        contract_id=contract_id,
        contract_version="v1",
        provenance_refs=source_refs,
        degradation_codes=degraded_codes,
        **payload,
    )
    validation = validate_projection_envelope(data)
    data["_metadata_validation"] = validation
    if not validation.get("ok"):
        data["_metadata_errors"] = validation
    return data


def _missing_db_view(*, view_name: str, contract_id: str, message: str) -> dict[str, Any]:
    db_path = resolve_capital_db_path_v1()
    payload = {
        "status": "FAIL_CLOSED",
        "warnings": ["CAPITAL_DB_MISSING"],
        "message": message,
        "db_path": str(db_path),
    }
    return _envelope(
        view_name=view_name,
        contract_id=contract_id,
        payload=payload,
        as_of_day=None,
        source_refs=[],
        degraded_codes=["CAPITAL_DB_MISSING"],
    )


def build_capital_overview_view() -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_overview",
            contract_id="capital_overview_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    overview = service.overview_surface()
    validations = service.validation_errors(as_of_date=overview.get("as_of_date"))
    degraded = [str(item["code"]) for item in validations.get("findings", [])]
    report_basis = overview.get("report_basis_metadata") or {}
    payload = {
        "status": validations.get("status", "UNKNOWN"),
        "overview": overview,
        "validation_status": validations.get("status"),
        "validation_error_count": validations.get("error_count"),
        "validation_severity_counts": validations.get("severity_counts"),
        "validation_findings": validations.get("findings"),
        "freshness_completeness": overview.get("freshness_completeness"),
        "report_basis_metadata": report_basis,
        "basis": {
            "as_of_date": report_basis.get("as_of_date", overview.get("as_of_date")),
            "basis_type": report_basis.get("report_basis", "latest_per_account"),
            "included_account_count": report_basis.get("included_account_count", overview.get("included_account_count")),
            "excluded_account_count": report_basis.get("excluded_account_count", overview.get("excluded_account_count")),
            "stale_account_count": report_basis.get("stale_account_count", 0),
            "freshness_status": report_basis.get("freshness_status", "UNKNOWN"),
            "validation_status": report_basis.get("validation_status", validations.get("status")),
        },
    }
    return _envelope(
        view_name="capital_overview",
        contract_id="capital_overview_v1",
        payload=payload,
        as_of_day=overview.get("as_of_date"),
        source_refs=_db_ref(service.db_path),
        degraded_codes=degraded,
    )


def build_capital_accounts_view() -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_accounts",
            contract_id="capital_accounts_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    balances = service.latest_balance_per_account()
    classes = service.current_classification_per_account(as_of_date=balances.get("as_of_date"))
    by_account = {str(row["account_id"]): row for row in classes.get("rows", [])}
    rows = []
    for row in balances.get("rows", []):
        account_id = str(row["account_id"])
        cls = by_account.get(account_id, {})
        rows.append(
            {
                "account_id": account_id,
                "account_name": row.get("account_name"),
                "latest_balance": row.get("balance"),
                "as_of_date": row.get("as_of_date"),
                "input_source": row.get("input_source"),
                "capital_type": cls.get("capital_type"),
                "control_type": cls.get("control_type"),
                "bucket_type": cls.get("bucket_type"),
                "include_in_allocation": cls.get("include_in_allocation"),
                "confidence_level": cls.get("confidence_level"),
                "confidence_band": cls.get("confidence_band"),
                "notes": cls.get("notes"),
            }
        )
    payload = {
        "status": "OK",
        "rows": rows,
        "recent_audit_entries": service.list_audit_entries(limit=50),
        "freshness_completeness": service._freshness_completeness_summary(as_of_date=balances.get("as_of_date")),
        "basis": {
            "as_of_date": balances.get("as_of_date"),
            "basis_type": "latest_per_account",
            "row_count": len(rows),
        },
    }
    return _envelope(
        view_name="capital_accounts",
        contract_id="capital_accounts_v1",
        payload=payload,
        as_of_day=balances.get("as_of_date"),
        source_refs=_db_ref(service.db_path),
        degraded_codes=[],
    )


def build_capital_allocation_view() -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_allocation",
            contract_id="capital_allocation_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    control = service.allocation_by_control()
    bucket = service.allocation_by_bucket(as_of_date=control.get("as_of_date"))
    matrix = service.bucket_control_matrix(as_of_date=control.get("as_of_date"))
    included_excluded = service.included_excluded_summary(as_of_date=control.get("as_of_date"))
    validations = service.validation_errors(as_of_date=control.get("as_of_date"))
    report_basis = control.get("report_basis_metadata") or {}
    degraded_codes = [str(item["code"]) for item in validations.get("findings", [])]
    payload = {
        "status": validations.get("status", "UNKNOWN"),
        "allocation_by_control": control,
        "allocation_by_bucket": bucket,
        "bucket_control_matrix": matrix,
        "included_excluded_summary": included_excluded,
        "freshness_completeness": control.get("freshness_completeness"),
        "validation_status_summary": control.get("validation_status_summary"),
        "report_basis_metadata": report_basis,
        "basis": {
            "as_of_date": report_basis.get("as_of_date", control.get("as_of_date")),
            "basis_type": report_basis.get("report_basis", "latest_per_account"),
            "included_account_count": report_basis.get("included_account_count", control.get("included_account_count")),
            "excluded_account_count": report_basis.get("excluded_account_count", control.get("excluded_account_count")),
            "stale_account_count": report_basis.get("stale_account_count", 0),
            "freshness_status": report_basis.get("freshness_status", "UNKNOWN"),
            "validation_status": report_basis.get("validation_status", validations.get("status")),
            "numerator": "included_balance_by_dimension",
            "denominator": "total_included_balance",
        },
    }
    return _envelope(
        view_name="capital_allocation",
        contract_id="capital_allocation_v1",
        payload=payload,
        as_of_day=control.get("as_of_date"),
        source_refs=_db_ref(service.db_path),
        degraded_codes=degraded_codes,
    )


def build_capital_history_view() -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_history",
            contract_id="capital_history_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    investable = service.history_investable_total()
    aegis_pct = service.history_aegis_allocation_pct()
    latest_day = None
    if investable.get("points"):
        latest_day = str(investable["points"][-1].get("day"))
    payload = {
        "status": "OK",
        "investable_time_series": investable,
        "aegis_allocation_pct_time_series": aegis_pct,
        "basis": {
            "as_of_date": latest_day,
            "basis_type": "latest_per_account_on_or_before_day",
            "day_count": len(investable.get("points", [])),
        },
    }
    return _envelope(
        view_name="capital_history",
        contract_id="capital_history_v1",
        payload=payload,
        as_of_day=latest_day,
        source_refs=_db_ref(service.db_path),
        degraded_codes=[],
    )


def build_capital_flows_view() -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_flows",
            contract_id="capital_flows_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    rows = service.list_cash_flows()
    latest_day = None
    if rows:
        latest_day = str(rows[0].get("flow_date"))
    payload = {
        "status": "OK",
        "rows": rows,
        "summary_by_period": service.flow_summary_by_period(),
        "basis": {
            "as_of_date": latest_day,
            "basis_type": "append_only_flows",
            "row_count": len(rows),
            "flow_truth_owner": "capital_cash_flows_v1",
        },
    }
    return _envelope(
        view_name="capital_flows",
        contract_id="capital_flows_v1",
        payload=payload,
        as_of_day=latest_day,
        source_refs=_db_ref(service.db_path),
        degraded_codes=[],
    )


def build_capital_cashflow_view(
    *,
    scenario: str = "florida",
    include_inheritance: bool = False,
    horizon_months: int = 24,
    start_month: str | None = None,
) -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_cashflow",
            contract_id="capital_cashflow_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    try:
        projection = service.cashflow_projection(
            scenario=scenario,
            include_nondeterministic=include_inheritance,
            horizon_months=horizon_months,
            start_month=start_month,
        )
    except Exception:
        payload = {
            "status": "FAIL_CLOSED",
            "operator_status": "AT_RISK",
            "scenario": scenario,
            "monthly_projection": [],
            "min_net": None,
            "max_negative_streak": 0,
            "operator_console": {
                "safety": {
                    "answer": "NOT_SAFE",
                    "status": "AT_RISK",
                    "operator_status": "AT_RISK",
                    "reason_codes": ["CAPITAL_CASHFLOW_PROJECTION_UNAVAILABLE"],
                },
                "weakest_month": None,
                "failure": {
                    "status": "FAIL_CLOSED",
                    "month": None,
                    "condition": "projection_unavailable",
                    "message": "Projection kernel did not return a trusted cashflow result.",
                },
                "drivers": {
                    "basis_month": None,
                    "contributors": [],
                    "expense_contributors": [],
                    "income_contributors": [],
                },
                "trust": {
                    "calculation_authority": "CapitalDomainServiceV1.cashflow_projection",
                    "projection_view": "v_capital_cashflow_projection_v1",
                    "validation_status": "AT_RISK",
                    "ui_calculation_policy": "UI renders projection-kernel fields and does not calculate financial truth.",
                },
            },
            "validation": {
                "status": "AT_RISK",
                "severity_counts": {"CRITICAL": 1},
                "findings": [
                    {
                        "code": "CAPITAL_CASHFLOW_PROJECTION_UNAVAILABLE",
                        "severity": "CRITICAL",
                        "message": "Cashflow projection could not be generated from current Capital storage.",
                        "details": {"scenario": scenario},
                    }
                ],
            },
            "basis": {
                "deterministic_only": not include_inheritance,
                "inheritance_excluded": not include_inheritance,
                "report_basis": "v_capital_cashflow_projection_v1",
            },
        }
        return _envelope(
            view_name="capital_cashflow",
            contract_id="capital_cashflow_v1",
            payload=payload,
            as_of_day=None,
            source_refs=_db_ref(service.db_path),
            degraded_codes=["CAPITAL_CASHFLOW_PROJECTION_UNAVAILABLE"],
        )
    findings = projection.get("validation", {}).get("findings", [])
    degraded_codes = [str(item.get("code")) for item in findings if str(item.get("severity") or "") in {"CRITICAL", "WARNING"}]
    payload = {
        "status": projection.get("status", "UNKNOWN"),
        "operator_status": projection.get("operator_status", "UNKNOWN"),
        "scenario": projection.get("scenario", scenario),
        "monthly_projection": projection.get("monthly_projection", []),
        "min_net": projection.get("min_net"),
        "max_negative_streak": projection.get("max_negative_streak"),
        "operator_console": projection.get("operator_console", {}),
        "validation": projection.get("validation", {}),
        "basis": {
            "deterministic_only": not include_inheritance,
            "inheritance_excluded": not include_inheritance,
            **(projection.get("basis") or {}),
        },
    }
    as_of_day = None
    rows = payload.get("monthly_projection", [])
    if rows:
        month_value = str(rows[-1].get("month") or "")
        if len(month_value) == 7:
            as_of_day = f"{month_value}-01"
    return _envelope(
        view_name="capital_cashflow",
        contract_id="capital_cashflow_v1",
        payload=payload,
        as_of_day=as_of_day,
        source_refs=_db_ref(service.db_path),
        degraded_codes=degraded_codes,
    )


def build_cashflow_projection_view(
    scenario: str = "florida",
    *,
    include_inheritance: bool = False,
    horizon_months: int = 24,
    start_month: str | None = None,
) -> dict[str, Any]:
    return build_capital_cashflow_view(
        scenario=scenario,
        include_inheritance=include_inheritance,
        horizon_months=horizon_months,
        start_month=start_month,
    )


def build_capital_validation_view() -> dict[str, Any]:
    service = _load_service_readonly()
    if service is None:
        return _missing_db_view(
            view_name="capital_validation",
            contract_id="capital_validation_v1",
            message="Capital database is missing. Run ops/tools/run_capital_domain_v1.py init && seed.",
        )
    validation = service.validation_errors()
    degraded_codes = [str(item["code"]) for item in validation.get("findings", [])]
    payload = {
        "status": validation.get("status", "UNKNOWN"),
        "validation": validation,
        "basis": {
            "as_of_date": validation.get("as_of_date"),
            "basis_type": "validation_projection",
            "finding_count": validation.get("error_count"),
        },
    }
    return _envelope(
        view_name="capital_validation",
        contract_id="capital_validation_v1",
        payload=payload,
        as_of_day=validation.get("as_of_date"),
        source_refs=_db_ref(service.db_path),
        degraded_codes=degraded_codes,
    )


def build_capital_query_surface_v1() -> dict[str, Any]:
    # Single aggregate payload for non-page consumers.
    overview = build_capital_overview_view()
    accounts = build_capital_accounts_view()
    allocation = build_capital_allocation_view()
    history = build_capital_history_view()
    flows = build_capital_flows_view()
    cashflow = build_capital_cashflow_view()
    validation = build_capital_validation_view()
    warning_codes: list[str] = []
    for payload in [overview, accounts, allocation, history, flows, cashflow, validation]:
        for code in payload.get("degradation_codes", []) or []:
            normalized = str(code)
            if normalized and normalized not in warning_codes:
                warning_codes.append(normalized)
    as_of_candidates = [payload.get("as_of_utc") for payload in [overview, accounts, allocation, history, flows, cashflow, validation]]
    as_of_utc = None
    for candidate in as_of_candidates:
        if isinstance(candidate, str) and candidate:
            as_of_utc = candidate if as_of_utc is None else max(as_of_utc, candidate)
    return {
        "schema_id": "capital_query_surface",
        "schema_version": "v1",
        "generated_utc": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "DEGRADED" if warning_codes else "OK",
        "as_of_utc": as_of_utc,
        "warning_codes": warning_codes,
        "overview": overview,
        "accounts": accounts,
        "allocation": allocation,
        "history": history,
        "flows": flows,
        "cashflow": cashflow,
        "validation": validation,
    }
