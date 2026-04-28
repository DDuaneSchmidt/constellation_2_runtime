from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_PORTFOLIO_ACCOUNT_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {
    "READY",
    "STALE",
    "MISSING_ACCOUNT_SNAPSHOT",
    "CONFLICTING_ACCOUNT_STATE",
    "OPERATOR_STATEMENT_ONLY",
    "RECONCILIATION_REQUIRED",
    "UNKNOWN",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _cents(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value * 100))
    text = str(value).strip()
    if not text:
        return None
    try:
        if "." in text:
            return int(round(float(text) * 100))
        return int(text)
    except ValueError:
        return None


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists() and path.is_file():
            return path.resolve()
    return None


def _account_values(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    nav = payload.get("nav") if isinstance(payload.get("nav"), dict) else {}
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
    values = payload.get("values") if isinstance(payload.get("values"), dict) else {}
    return {
        "account_id": payload.get("account_id") or snapshot.get("account_id") or account.get("account_id"),
        "currency": payload.get("currency") or snapshot.get("currency") or nav.get("currency") or account.get("currency"),
        "cash_total_cents": _cents(payload.get("cash_total_cents") or snapshot.get("cash_total_cents") or values.get("cash_total_cents") or nav.get("cash_total")),
        "available_funds_cents": _cents(payload.get("available_funds_cents") or snapshot.get("available_funds_cents") or values.get("available_funds_cents") or payload.get("available_funds")),
        "buying_power_cents": _cents(payload.get("buying_power_cents") or snapshot.get("buying_power_cents") or values.get("buying_power_cents") or payload.get("buying_power")),
        "excess_liquidity_cents": _cents(payload.get("excess_liquidity_cents") or snapshot.get("excess_liquidity_cents") or payload.get("excess_liquidity")),
        "net_liquidation_cents": _cents(payload.get("nlv_total_cents") or snapshot.get("nlv_total_cents") or values.get("net_liquidation_cents") or payload.get("net_liquidation") or nav.get("nav_total")),
        "observed_at_utc": payload.get("observed_at_utc") or snapshot.get("observed_at_utc") or payload.get("produced_utc"),
    }


def _fresh_for_day(values: dict[str, Any], day_utc: str) -> bool:
    observed = str(values.get("observed_at_utc") or "").strip()
    return bool(observed.startswith(day_utc))


def portfolio_account_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "portfolio_account_authority_v1" / day_utc / "portfolio_account_authority.v1.json"


def evaluate_portfolio_account_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    repo_root: Path | None = None,
    produced_utc: str | None = None,
    mode: str = "PAPER",
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    repo_root = Path(repo_root).resolve() if repo_root is not None else Path.cwd().resolve()
    roots = [execution_root, truth_root] if execution_root != truth_root else [truth_root]

    broker_path = _first_existing(
        [
            root / "broker_account_snapshot_v1" / day_utc / "broker_account_snapshot.v1.json"
            for root in roots
        ]
        + [
            root / "account_snapshot_v1" / day_utc / "account_snapshot.v1.json"
            for root in roots
        ]
    )
    nav_path = _first_existing([root / "accounting_v2" / "nav" / day_utc / "nav.v2.json" for root in roots])
    cash_path = _first_existing([root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json" for root in roots])
    operator_path = repo_root / "constellation_2" / "operator_inputs" / "cash_ledger_operator_statements" / day_utc / "operator_statement.v1.json"
    risk_path = execution_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json"
    recon_path = truth_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json"
    closure_prev_path = truth_root / "reports" / "trading_day_closure_authority_v1" / day_utc / "trading_day_closure_authority.v1.json"
    fills_root = execution_root / "fill_ledger_v1" / day_utc

    broker_values = _account_values(_read_json(broker_path) if broker_path else None)
    cash_values = _account_values(_read_json(cash_path) if cash_path else None)
    nav_values = _account_values(_read_json(nav_path) if nav_path else None)
    operator_values = _account_values(_read_json(operator_path))
    source = "UNKNOWN"
    values: dict[str, Any] = {}
    conflicts: list[dict[str, Any]] = []
    if broker_values:
        source = "BROKER"
        values = broker_values
        compare = operator_values or cash_values
        if compare:
            for key in ("cash_total_cents", "net_liquidation_cents"):
                if broker_values.get(key) is not None and compare.get(key) is not None and broker_values.get(key) != compare.get(key):
                    conflicts.append({"field": key, "broker": broker_values.get(key), "observed": compare.get(key)})
    elif cash_values or operator_values or nav_values:
        source = "OPERATOR_STATEMENT"
        values = cash_values or operator_values or nav_values

    fill_files = sorted(fills_root.glob("*.fill_ledger.v1.json")) if fills_root.exists() else []
    reconciliation_required = bool(fill_files and not recon_path.exists())
    if conflicts:
        state = "CONFLICTING_ACCOUNT_STATE"
    elif reconciliation_required:
        state = "RECONCILIATION_REQUIRED"
    elif broker_values and not _fresh_for_day(broker_values, day_utc):
        state = "STALE"
    elif broker_values:
        state = "READY"
    elif values:
        state = "OPERATOR_STATEMENT_ONLY"
    elif broker_path is None:
        state = "MISSING_ACCOUNT_SNAPSHOT"
    else:
        state = "UNKNOWN"

    operator_statement_acceptable = bool(state == "OPERATOR_STATEMENT_ONLY" and str(mode).upper() == "PAPER")
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "PORTFOLIO_ACCOUNT_STATE",
        "status": "PASS" if state in {"READY", "OPERATOR_STATEMENT_ONLY"} else ("WARN" if state in {"STALE", "RECONCILIATION_REQUIRED"} else "FAIL"),
        "account_state": state,
        "source_type": source,
        "operator_statement_acceptable": operator_statement_acceptable,
        "account_values": values,
        "conflicts": conflicts,
        "reconciliation_required": reconciliation_required,
        "first_blocker": "ACCOUNT_STATE_CONFLICT" if conflicts else ("ACCOUNT_RECONCILIATION_REQUIRED" if reconciliation_required else ("" if state in {"READY", "OPERATOR_STATEMENT_ONLY"} else state)),
        "input_evidence": [
            {"artifact_type": "broker_account_snapshot", "path": str(broker_path or ""), "exists": bool(broker_path)},
            {"artifact_type": "cash_ledger_v1", "path": str(cash_path or ""), "exists": bool(cash_path)},
            {"artifact_type": "operator_statement", "path": str(operator_path), "exists": operator_path.exists()},
            {"artifact_type": "capital_risk_envelope_v2", "path": str(risk_path), "exists": risk_path.exists()},
            {"artifact_type": "positions", "path": str((execution_root / "positions_v1" / "snapshots" / day_utc).resolve()), "exists": (execution_root / "positions_v1" / "snapshots" / day_utc).exists()},
            {"artifact_type": "fill_ledger_v1", "path": str(fills_root), "exists": fills_root.exists()},
            {"artifact_type": "execution_reconciliation_v1", "path": str(recon_path), "exists": recon_path.exists()},
            {"artifact_type": "prior_day_closure_authority", "path": str(closure_prev_path), "exists": closure_prev_path.exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_portfolio_account_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = portfolio_account_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
