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
SCOPE_ALL_ACTIVE_INTENTS = "ALL_ACTIVE_INTENTS"
SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH = "SELECTED_INTENT_REQUIREMENT_GRAPH"


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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().upper()
    return text not in {"", "0", "FALSE", "NO", "N", "NONE", "NULL"}


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


def _latest_equity_snapshot_for_symbol(
    roots: list[Path],
    day_utc: str,
    symbol: str,
) -> tuple[Path | None, dict[str, Any] | None, Path | None, dict[str, Any] | None]:
    candidates: list[tuple[Path, dict[str, Any], Path, dict[str, Any]]] = []
    for root in roots:
        day_root = root / "market_data_snapshot_v1" / "snapshots" / day_utc
        snapshot_path = day_root / f"{symbol.upper()}.market_data_snapshot.v1.json"
        snapshot = _read_json(snapshot_path) or {}
        if not snapshot:
            continue
        observed = str(snapshot.get("symbol") or "").strip().upper()
        cert_candidates = [
            day_root / f"{symbol.upper()}.freshness_certificate.v1.json",
            day_root / f"{symbol.upper()}.market_data_freshness_certificate.v1.json",
        ]
        cert_path = next((path for path in cert_candidates if path.exists() and path.is_file()), cert_candidates[0])
        cert = _read_json(cert_path) or {}
        if observed == symbol.upper():
            candidates.append((snapshot_path.resolve(), snapshot, cert_path.resolve(), cert))
    if not candidates:
        return None, None, None, None
    candidates.sort(key=lambda item: str(item[0]))
    return candidates[-1]


def _selected_market_data_requirements(requirement_graph_path: Path | None, day_utc: str) -> list[dict[str, Any]]:
    if requirement_graph_path is None:
        return []
    payload = _read_json(Path(requirement_graph_path)) or {}
    if str(payload.get("day_utc") or "").strip() != day_utc:
        return []
    rows = payload.get("requirements")
    if not isinstance(rows, list):
        return []
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("owner_phase") or "").strip().upper() != "MARKET_DATA":
            continue
        if str(row.get("source_type") or "").strip().upper() != "ACTIVE_INTENT":
            continue
        if row.get("required") is False:
            continue
        selected.append(row)
    return selected


def _symbol_groups_from_requirements(requirements: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    options: set[str] = set()
    equities: set[str] = set()
    for row in requirements:
        symbol = str(row.get("instrument") or "").strip().upper()
        if not symbol:
            continue
        family = str(row.get("market_data_family") or "").strip().upper()
        artifact = str(row.get("required_artifact") or row.get("data_type") or "").strip().upper()
        requirement_id = str(row.get("requirement_id") or "").strip().upper()
        if family == "OPTIONS" or artifact in {"OPTION_CHAIN", "OPTIONS_SNAPSHOT", "OPTIONS_SNAPSHOT_ARTIFACT"} or "OPTION" in requirement_id:
            options.add(symbol)
            continue
        if family == "EQUITY" or artifact in {"UNDERLYING_SPOT", "BID_ASK_QUOTES", "FRESHNESS_CERTIFICATE"}:
            equities.add(symbol)
    return sorted(options), sorted(equities)


def _option_coverage_row(
    *,
    roots: list[Path],
    day_utc: str,
    symbol: str,
    produced_dt: datetime,
) -> dict[str, Any]:
    snapshot_path, snapshot, cert_path, cert = _latest_snapshot_for_symbol(roots, day_utc, symbol)
    if snapshot_path is None or snapshot is None:
        return {
            "symbol": symbol,
            "required": True,
            "market_data_family": "OPTIONS",
            "state": "MISSING_REQUIRED_DATA",
            "snapshot_path": "",
            "freshness_certificate_path": "",
            "contract_count": 0,
            "fresh": False,
            "valid_schema": False,
            "blocker_code": "OPTIONS_CHAIN_SNAPSHOT_MISSING",
        }
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
    return {
        "symbol": symbol,
        "required": True,
        "market_data_family": "OPTIONS",
        "state": state,
        "snapshot_path": str(snapshot_path),
        "freshness_certificate_path": str(cert_path or ""),
        "contract_count": len(contracts),
        "fresh": fresh,
        "valid_schema": valid_schema,
        "blocker_code": blocker,
    }


def _equity_coverage_row(
    *,
    roots: list[Path],
    day_utc: str,
    symbol: str,
    produced_dt: datetime,
) -> dict[str, Any]:
    snapshot_path, snapshot, cert_path, cert = _latest_equity_snapshot_for_symbol(roots, day_utc, symbol)
    base = {
        "symbol": symbol,
        "required": True,
        "market_data_family": "EQUITY",
        "snapshot_path": str(snapshot_path or ""),
        "freshness_certificate_path": str(cert_path or ""),
        "contract_count": 0,
    }
    if snapshot_path is None or snapshot is None:
        return {**base, "state": "MISSING_REQUIRED_DATA", "fresh": False, "valid_schema": False, "blocker_code": "EQUITY_MARKET_DATA_SNAPSHOT_MISSING"}
    if not cert:
        return {**base, "state": "MISSING_REQUIRED_DATA", "fresh": False, "valid_schema": True, "blocker_code": "EQUITY_FRESHNESS_CERTIFICATE_MISSING"}
    observed_symbol = str(snapshot.get("symbol") or "").strip().upper()
    if observed_symbol != symbol:
        return {**base, "state": "COVERAGE_GAP", "fresh": False, "valid_schema": False, "blocker_code": "EQUITY_MARKET_DATA_SYMBOL_COVERAGE_GAP"}
    quote = snapshot.get("quote") if isinstance(snapshot.get("quote"), dict) else {}
    bid = snapshot.get("bid") or quote.get("bid")
    ask = snapshot.get("ask") or quote.get("ask")
    spot = snapshot.get("spot") or snapshot.get("last") or snapshot.get("close")
    if not _truthy(bid) or not _truthy(ask):
        return {**base, "state": "MISSING_REQUIRED_DATA", "fresh": False, "valid_schema": False, "blocker_code": "EQUITY_QUOTES_MISSING_BID_ASK"}
    if not _truthy(spot):
        return {**base, "state": "MISSING_REQUIRED_DATA", "fresh": False, "valid_schema": False, "blocker_code": "EQUITY_MARKET_DATA_SNAPSHOT_INVALID"}
    quote_ts = _parse_iso(snapshot.get("quote_as_of_utc") or snapshot.get("timestamp_utc"))
    valid_until = _parse_iso((cert or {}).get("valid_until_utc"))
    fresh = bool(quote_ts and quote_ts.date().isoformat() == day_utc and valid_until and valid_until >= produced_dt)
    if not fresh:
        return {**base, "state": "STALE", "fresh": False, "valid_schema": True, "blocker_code": "EQUITY_MARKET_DATA_STALE"}
    return {**base, "state": "READY", "fresh": True, "valid_schema": True, "blocker_code": ""}


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
    evaluation_scope: str = SCOPE_ALL_ACTIVE_INTENTS,
    requirement_graph_path: Path | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    roots = [execution_root, truth_root] if execution_root != truth_root else [truth_root]
    produced = produced_utc or _utc_now_iso()
    produced_dt = _parse_iso(produced) or datetime.now(UTC)
    scope = str(evaluation_scope or SCOPE_ALL_ACTIVE_INTENTS).strip().upper()
    if scope not in {SCOPE_ALL_ACTIVE_INTENTS, SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH}:
        raise ValueError(f"unsupported market-data authority evaluation scope: {evaluation_scope}")
    intents = _discover_intents(roots, day_utc)
    coverage: list[dict[str, Any]] = []
    selected_requirements_missing = False
    if scope == SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH:
        requirements = _selected_market_data_requirements(requirement_graph_path, day_utc)
        selected_requirements_missing = not requirements
        option_symbols, equity_symbols = _symbol_groups_from_requirements(requirements)
        required_symbols = sorted({*option_symbols, *equity_symbols})
        option_intents = [
            row for row in intents
            if str(row.get("symbol") or "").strip().upper() in option_symbols
            and row.get("requires_options")
        ]
        if selected_requirements_missing:
            coverage.append(
                {
                    "symbol": "",
                    "required": True,
                    "market_data_family": "",
                    "state": "MISSING_REQUIRED_DATA",
                    "snapshot_path": "",
                    "freshness_certificate_path": "",
                    "contract_count": 0,
                    "fresh": False,
                    "valid_schema": False,
                    "blocker_code": "MARKET_DATA_REQUIREMENT_GRAPH_MISSING",
                }
            )
        for symbol in option_symbols:
            coverage.append(_option_coverage_row(roots=roots, day_utc=day_utc, symbol=symbol, produced_dt=produced_dt))
        for symbol in equity_symbols:
            coverage.append(_equity_coverage_row(roots=roots, day_utc=day_utc, symbol=symbol, produced_dt=produced_dt))
    else:
        option_intents = [row for row in intents if row.get("requires_options") and row.get("symbol")]
        required_symbols = sorted({str(row.get("symbol") or "").upper() for row in option_intents if row.get("symbol")})
        for symbol in required_symbols:
            coverage.append(_option_coverage_row(roots=roots, day_utc=day_utc, symbol=symbol, produced_dt=produced_dt))

    if selected_requirements_missing:
        state = "MISSING_REQUIRED_DATA"
    elif scope == SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH and not required_symbols:
        state = "NOT_REQUIRED"
    elif not intents:
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
        "evaluation_scope": scope,
        "requirement_graph_path": str(Path(requirement_graph_path).resolve()) if requirement_graph_path is not None else "",
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
