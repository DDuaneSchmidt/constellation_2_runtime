from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_RISK_SIZING_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {
    "NO_INTENTS",
    "SIZED",
    "CLIPPED",
    "ROUNDED",
    "ZERO_SIZED",
    "RISK_BLOCKED",
    "ACCOUNT_DATA_MISSING",
    "SIZING_ERROR",
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


def _json_files(root: Path, pattern: str) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.rglob(pattern) if path.is_file())


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _intent_hash(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_hash") or payload.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip()


def risk_sizing_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "risk_sizing_authority_v1" / day_utc / "risk_sizing_authority.v1.json"


def evaluate_risk_sizing_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    portfolio_path = truth_root / "reports" / "portfolio_account_authority_v1" / day_utc / "portfolio_account_authority.v1.json"
    portfolio = _read_json(portfolio_path) or {}
    risk_path = execution_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json"
    risk = _read_json(risk_path) or {}
    envelope = risk.get("envelope") if isinstance(risk.get("envelope"), dict) else {}
    account_values = portfolio.get("account_values") if isinstance(portfolio.get("account_values"), dict) else {}
    nav_cents = _int(account_values.get("net_liquidation_cents") or envelope.get("nav_total_cents"))
    allowed_risk_cents = _int(envelope.get("allowed_capital_at_risk_cents") or envelope.get("headroom_cents"))
    portfolio_state = str(portfolio.get("account_state") or "").strip().upper()

    intents: list[tuple[Path, dict[str, Any]]] = []
    for root in [execution_root, truth_root] if execution_root != truth_root else [truth_root]:
        for path in _json_files(root / "intents_v1" / "snapshots" / day_utc, "*.json"):
            obj = _read_json(path) or {}
            intents.append((path, obj))
    seen: set[str] = set()
    unique_intents: list[tuple[Path, dict[str, Any]]] = []
    for path, obj in intents:
        ih = _intent_hash(path, obj)
        if ih in seen:
            continue
        seen.add(ih)
        unique_intents.append((path, obj))

    packages: dict[str, tuple[Path, dict[str, Any]]] = {}
    for root in [execution_root, truth_root] if execution_root != truth_root else [truth_root]:
        for path in _json_files(root / "execution_package_v1" / day_utc, "execution_package.v1.json"):
            obj = _read_json(path) or {}
            ih = str(obj.get("intent_hash") or "").strip()
            if ih:
                packages[ih] = (path, obj)
    builds = _json_files(truth_root / "reports" / "execution_build_v1" / day_utc, "execution_build.v1.json")
    decisions: list[dict[str, Any]] = []
    if not unique_intents:
        state = "NO_INTENTS"
    elif portfolio_state in {"", "MISSING_ACCOUNT_SNAPSHOT", "UNKNOWN"} or nav_cents is None or nav_cents <= 0:
        state = "ACCOUNT_DATA_MISSING"
    elif str(risk.get("status") or "").strip().upper() not in {"PASS", "OK"}:
        state = "RISK_BLOCKED"
    else:
        state = "SIZED"

    for path, intent in unique_intents:
        ih = _intent_hash(path, intent)
        target_pct = _decimal(intent.get("target_notional_pct") or (intent.get("constraints") or {}).get("max_risk_pct"))
        requested_risk_cents = int((target_pct * Decimal(nav_cents or 0)).to_integral_value()) if target_pct is not None and nav_cents is not None else None
        package_path, package = packages.get(ih, (None, {}))
        quantity = _int(package.get("quantity"))
        risk_per_unit_cents = _int(package.get("risk_per_unit_cents") or package.get("max_defined_loss_cents"))
        final_risk_cents = _int(package.get("required_risk_cents"))
        if final_risk_cents is None and quantity is not None and risk_per_unit_cents is not None:
            final_risk_cents = quantity * risk_per_unit_cents
        decision_state = "SIZED"
        reason = ""
        if state == "ACCOUNT_DATA_MISSING":
            decision_state = "ACCOUNT_DATA_MISSING"
            reason = "ACCOUNT_DATA_MISSING"
        elif state == "RISK_BLOCKED":
            decision_state = "RISK_BLOCKED"
            reason = "CAPITAL_RISK_ENVELOPE_NOT_PASS"
        elif quantity == 0:
            decision_state = "ZERO_SIZED"
            reason = "FINAL_QUANTITY_ZERO"
        elif quantity is None and package_path is not None:
            decision_state = "SIZING_ERROR"
            reason = "EXECUTION_PACKAGE_QUANTITY_MISSING"
        elif requested_risk_cents is not None and final_risk_cents is not None and allowed_risk_cents is not None and final_risk_cents > allowed_risk_cents:
            decision_state = "RISK_BLOCKED"
            reason = "FINAL_RISK_EXCEEDS_ENVELOPE"
        elif requested_risk_cents is not None and final_risk_cents is not None and final_risk_cents < requested_risk_cents:
            decision_state = "CLIPPED" if allowed_risk_cents is not None and requested_risk_cents > allowed_risk_cents else "ROUNDED"
            reason = "FINAL_RISK_BELOW_REQUESTED"
        decisions.append(
            {
                "intent_id": str(intent.get("intent_id") or "").strip(),
                "intent_hash": ih,
                "intent_path": str(path),
                "requested_target_pct": str(target_pct) if target_pct is not None else "",
                "requested_risk_cents": requested_risk_cents,
                "account_net_liquidation_cents": nav_cents,
                "allowed_risk_cents": allowed_risk_cents,
                "final_quantity": quantity,
                "risk_per_unit_cents": risk_per_unit_cents,
                "final_risk_cents": final_risk_cents,
                "sizing_state": decision_state,
                "reason_code": reason,
                "execution_package_path": str(package_path or ""),
                "final_size_agrees_with_execution_package": bool(package_path and quantity is not None),
            }
        )
    row_states = {row["sizing_state"] for row in decisions}
    if "ACCOUNT_DATA_MISSING" in row_states:
        state = "ACCOUNT_DATA_MISSING"
    elif "RISK_BLOCKED" in row_states:
        state = "RISK_BLOCKED"
    elif "SIZING_ERROR" in row_states:
        state = "SIZING_ERROR"
    elif "ZERO_SIZED" in row_states:
        state = "ZERO_SIZED"
    elif "CLIPPED" in row_states:
        state = "CLIPPED"
    elif "ROUNDED" in row_states:
        state = "ROUNDED"
    elif decisions:
        state = "SIZED"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "RISK_SIZING_DECISIONS",
        "status": "PASS" if state in {"NO_INTENTS", "SIZED", "CLIPPED", "ROUNDED"} else "FAIL",
        "risk_sizing_state": state,
        "intent_count": len(unique_intents),
        "account_values_used": account_values,
        "risk_envelope": envelope,
        "sizing_decisions": decisions,
        "final_size_summary": decisions[0] if decisions else {},
        "first_sizing_reason": next((row["reason_code"] for row in decisions if row.get("reason_code")), ""),
        "first_blocker": next(
            (row["reason_code"] for row in decisions if row.get("sizing_state") in {"ZERO_SIZED", "RISK_BLOCKED", "ACCOUNT_DATA_MISSING", "SIZING_ERROR"} and row.get("reason_code")),
            "" if state in {"NO_INTENTS", "SIZED", "CLIPPED", "ROUNDED"} else state,
        ),
        "input_evidence": [
            {"artifact_type": "intents_v1", "path": str((execution_root / "intents_v1" / "snapshots" / day_utc).resolve()), "exists": (execution_root / "intents_v1" / "snapshots" / day_utc).exists()},
            {"artifact_type": "capital_risk_envelope_v2", "path": str(risk_path), "exists": risk_path.exists()},
            {"artifact_type": "portfolio_account_authority_v1", "path": str(portfolio_path), "exists": portfolio_path.exists()},
            {"artifact_type": "execution_build_v1", "path": str((truth_root / "reports" / "execution_build_v1" / day_utc).resolve()), "exists": bool(builds)},
            {"artifact_type": "execution_package_v1", "path": str((execution_root / "execution_package_v1" / day_utc).resolve()), "exists": (execution_root / "execution_package_v1" / day_utc).exists()},
            {"artifact_type": "submit_decision_trace_v1", "path": str((truth_root / "reports" / "submit_decision_trace_v1" / day_utc).resolve()), "exists": (truth_root / "reports" / "submit_decision_trace_v1" / day_utc).exists()},
            {"artifact_type": "sleeve_intent_trade_attribution_v1", "path": str((truth_root / "reports" / "sleeve_intent_trade_attribution_v1" / day_utc / "sleeve_intent_trade_attribution.v1.json").resolve()), "exists": (truth_root / "reports" / "sleeve_intent_trade_attribution_v1" / day_utc / "sleeve_intent_trade_attribution.v1.json").exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_risk_sizing_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = risk_sizing_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
