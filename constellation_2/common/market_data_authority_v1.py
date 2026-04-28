from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_MARKET_DATA_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {
    "NO_INTENTS",
    "NOT_REQUIRED",
    "READY",
    "PARTIAL",
    "STALE",
    "MISSING_REQUIRED_DATA",
    "COVERAGE_GAP",
    "INVALID_SCHEMA",
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


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _json_files(root: Path, pattern: str) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.rglob(pattern) if path.is_file())


def _symbol_from_intent(payload: dict[str, Any]) -> str:
    underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
    instrument = payload.get("instrument") if isinstance(payload.get("instrument"), dict) else {}
    for value in (underlying.get("symbol"), instrument.get("symbol"), payload.get("symbol")):
        text = str(value or "").strip().upper()
        if text:
            return text
    return ""


def _is_option_intent(payload: dict[str, Any]) -> bool:
    if "option" in payload or "options" in payload:
        return True
    text = json.dumps(payload, sort_keys=True).lower()
    return "option" in text


def _discover_intents(roots: list[Path], day_utc: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for root in roots:
        day_root = root / "intents_v1" / "snapshots" / day_utc
        for path in _json_files(day_root, "*.json"):
            obj = _read_json(path) or {}
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "intent_id": str(obj.get("intent_id") or "").strip(),
                    "intent_hash": str(obj.get("intent_hash") or obj.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip(),
                    "symbol": _symbol_from_intent(obj),
                    "requires_options": _is_option_intent(obj),
                    "path": str(path),
                }
            )
    return out


def _latest_snapshot_for_symbol(roots: list[Path], day_utc: str, symbol: str) -> tuple[Path | None, dict[str, Any] | None, Path | None, dict[str, Any] | None]:
    candidates: list[tuple[Path, dict[str, Any], Path, dict[str, Any]]] = []
    for root in roots:
        day_root = root / "options_chain_snapshot_v1" / day_utc
        for path in _json_files(day_root, "options_chain_snapshot.v1.json"):
            obj = _read_json(path) or {}
            underlying = obj.get("underlying") if isinstance(obj.get("underlying"), dict) else {}
            observed = str(underlying.get("symbol") or obj.get("symbol") or "").strip().upper()
            cert_path = path.parent / "freshness_certificate.v1.json"
            cert = _read_json(cert_path) or {}
            if observed == symbol.upper():
                candidates.append((path, obj, cert_path, cert))
    if not candidates:
        return None, None, None, None
    candidates.sort(key=lambda item: str(item[0]))
    return candidates[-1]


def market_data_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "market_data_authority_v1"
        / day_utc
        / "market_data_authority.v1.json"
    ).resolve()


def _phase_context(*, truth_root: Path, execution_root: Path, day_utc: str) -> tuple[str, str]:
    boundary_path = truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    boundary = _read_json(boundary_path) or {}
    if str(boundary.get("submit_mode_status") or boundary.get("boundary_status") or "").strip().upper() == "DRY_RUN_COMPLETE":
        return "POST_SUBMIT_DRY_RUN_COMPLETE", "POST_SUBMIT_DIAGNOSTIC"

    closure_path = truth_root / "reports" / "trading_day_closure_authority_v1" / day_utc / "trading_day_closure_authority.v1.json"
    closure = _read_json(closure_path) or {}
    if str(closure.get("closure_state") or "").strip().upper() == "DRY_RUN_CLOSED":
        return "POST_SUBMIT_DRY_RUN_COMPLETE", "POST_SUBMIT_DIAGNOSTIC"

    lifecycle_path = execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json"
    lifecycle = _read_json(lifecycle_path) or {}
    if str(lifecycle.get("current_lifecycle_state") or "").strip().upper() == "DRY_RUN_COMPLETE":
        return "POST_SUBMIT_DRY_RUN_COMPLETE", "POST_SUBMIT_DIAGNOSTIC"

    return "PRE_SUBMIT", "PRE_SUBMIT_BLOCKER"


def evaluate_market_data_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    roots = [execution_root, truth_root] if execution_root != truth_root else [truth_root]
    produced = produced_utc or _utc_now_iso()
    produced_dt = _parse_iso(produced) or datetime.now(UTC)
    intents = _discover_intents(roots, day_utc)
    option_intents = [row for row in intents if row.get("requires_options") and row.get("symbol")]
    required_symbols = sorted({str(row.get("symbol") or "").upper() for row in option_intents if row.get("symbol")})

    coverage: list[dict[str, Any]] = []
    for symbol in required_symbols:
        snapshot_path, snapshot, cert_path, cert = _latest_snapshot_for_symbol(roots, day_utc, symbol)
        if snapshot_path is None or snapshot is None:
            coverage.append(
                {
                    "symbol": symbol,
                    "required": True,
                    "state": "MISSING_REQUIRED_DATA",
                    "snapshot_path": "",
                    "freshness_certificate_path": "",
                    "contract_count": 0,
                    "fresh": False,
                    "valid_schema": False,
                    "blocker_code": "OPTIONS_CHAIN_SNAPSHOT_MISSING",
                }
            )
            continue
        underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
        observed_symbol = str(underlying.get("symbol") or snapshot.get("symbol") or "").strip().upper()
        contracts = snapshot.get("contracts") if isinstance(snapshot.get("contracts"), list) else []
        valid_schema = str(snapshot.get("schema_id") or "").strip() in {"options_chain_snapshot", "options_chain_snapshot_v1"} and bool(contracts)
        valid_until = _parse_iso((cert or {}).get("valid_until_utc"))
        fresh = bool(valid_until and valid_until >= produced_dt)
        state = "READY"
        blocker = ""
        if observed_symbol != symbol:
            state = "COVERAGE_GAP"
            blocker = "OPTIONS_CHAIN_SYMBOL_COVERAGE_GAP"
        elif not valid_schema:
            state = "INVALID_SCHEMA"
            blocker = "OPTIONS_CHAIN_SNAPSHOT_INVALID_SCHEMA"
        elif not fresh:
            state = "STALE"
            blocker = "OPTIONS_CHAIN_SNAPSHOT_STALE"
        coverage.append(
            {
                "symbol": symbol,
                "required": True,
                "state": state,
                "snapshot_path": str(snapshot_path),
                "freshness_certificate_path": str(cert_path or ""),
                "contract_count": len(contracts),
                "fresh": fresh,
                "valid_schema": valid_schema,
                "blocker_code": blocker,
            }
        )

    if not intents:
        state = "NO_INTENTS"
    elif not required_symbols:
        state = "NOT_REQUIRED"
    elif all(row["state"] == "READY" for row in coverage):
        state = "READY"
    elif any(row["state"] == "MISSING_REQUIRED_DATA" for row in coverage):
        state = "MISSING_REQUIRED_DATA"
    elif any(row["state"] == "COVERAGE_GAP" for row in coverage):
        state = "COVERAGE_GAP"
    elif any(row["state"] == "INVALID_SCHEMA" for row in coverage):
        state = "INVALID_SCHEMA"
    elif any(row["state"] == "STALE" for row in coverage):
        state = "STALE"
    else:
        state = "PARTIAL"
    phase_context, operator_impact = _phase_context(truth_root=truth_root, execution_root=execution_root, day_utc=day_utc)
    failing_state = state not in {"NO_INTENTS", "NOT_REQUIRED", "READY"}
    status = "PASS"
    if failing_state and operator_impact == "POST_SUBMIT_DIAGNOSTIC":
        status = "WARN"
    elif failing_state:
        status = "FAIL"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced,
        "authority_scope": "ACTIVE_INTENT_MARKET_DATA",
        "status": status,
        "market_data_state": state,
        "phase_context": phase_context,
        "operator_impact": operator_impact,
        "active_intent_count": len(intents),
        "active_options_intent_count": len(option_intents),
        "required_symbols": required_symbols,
        "coverage": coverage,
        "first_blocker": next((str(row.get("blocker_code") or "") for row in coverage if row.get("blocker_code")), ""),
        "input_evidence": [
            {"artifact_type": "trading_day_intent_generation_v1", "path": str((truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json").resolve()), "exists": (truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json").exists()},
            {"artifact_type": "intents_v1", "path": str((execution_root / "intents_v1" / "snapshots" / day_utc).resolve()), "exists": (execution_root / "intents_v1" / "snapshots" / day_utc).exists()},
            {"artifact_type": "options_chain_snapshot_v1", "path": str((execution_root / "options_chain_snapshot_v1" / day_utc).resolve()), "exists": (execution_root / "options_chain_snapshot_v1" / day_utc).exists()},
            {"artifact_type": "paper_day_manifest_v1", "path": str((truth_root / "reports" / "paper_day_manifest_v1" / day_utc / "paper_day_manifest.v1.json").resolve()), "exists": (truth_root / "reports" / "paper_day_manifest_v1" / day_utc / "paper_day_manifest.v1.json").exists()},
            {"artifact_type": "phaseC_preflight_v1", "path": str((execution_root / "phaseC_preflight_v1" / day_utc).resolve()), "exists": (execution_root / "phaseC_preflight_v1" / day_utc).exists()},
            {"artifact_type": "submit_boundary_status_v1", "path": str((truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").resolve()), "exists": (truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").exists()},
            {"artifact_type": "execution_lifecycle_authority_v1", "path": str((execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json").resolve()), "exists": (execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json").exists()},
            {"artifact_type": "trading_day_closure_authority_v1", "path": str((truth_root / "reports" / "trading_day_closure_authority_v1" / day_utc / "trading_day_closure_authority.v1.json").resolve()), "exists": (truth_root / "reports" / "trading_day_closure_authority_v1" / day_utc / "trading_day_closure_authority.v1.json").exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_market_data_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = market_data_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
