from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_STRATEGY_DECISION_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {
    "NO_STRATEGIES_ENABLED",
    "EVALUATED_NO_INTENT",
    "INTENT_CREATED",
    "INTENT_SUPPRESSED",
    "STRATEGY_DISABLED",
    "SIGNAL_MISSING",
    "MARKET_DATA_BLOCKED",
    "STRATEGY_ERROR",
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


def _intent_hash(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_hash") or payload.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip()


def strategy_decision_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "strategy_decision_authority_v1" / day_utc / "strategy_decision_authority.v1.json"


def evaluate_strategy_decision_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    intent_generation_path = truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json"
    market_data_path = truth_root / "reports" / "market_data_authority_v1" / day_utc / "market_data_authority.v1.json"
    paper_authority_path = truth_root / "reports" / "paper_trading_day_authority_v1" / day_utc / "paper_trading_day_authority.v1.json"
    intent_generation = _read_json(intent_generation_path) or {}
    market_data = _read_json(market_data_path) or {}

    roots = [execution_root, truth_root] if execution_root != truth_root else [truth_root]
    intents_by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for root in roots:
        for path in _json_files(root / "intents_v1" / "snapshots" / day_utc, "*.json"):
            obj = _read_json(path) or {}
            engine = obj.get("engine") if isinstance(obj.get("engine"), dict) else {}
            strategy_id = str(engine.get("engine_id") or obj.get("strategy_id") or "UNKNOWN").strip()
            intents_by_strategy[strategy_id].append(
                {
                    "strategy_id": strategy_id,
                    "intent_id": str(obj.get("intent_id") or "").strip(),
                    "intent_hash": _intent_hash(path, obj),
                    "intent_path": str(path),
                    "symbol": str((obj.get("underlying") or {}).get("symbol") if isinstance(obj.get("underlying"), dict) else obj.get("symbol") or "").strip().upper(),
                }
            )

    topology = intent_generation.get("producer_topology") if isinstance(intent_generation.get("producer_topology"), list) else []
    results = intent_generation.get("producer_results") if isinstance(intent_generation.get("producer_results"), list) else []
    results_by_engine = {
        str(row.get("engine_id") or row.get("logical_name") or "").strip(): row
        for row in results
        if isinstance(row, dict)
    }
    strategies: list[dict[str, Any]] = []
    market_blocked = str(market_data.get("operator_impact") or "").strip().upper() == "PRE_SUBMIT_BLOCKER" and str(market_data.get("status") or "").strip().upper() == "FAIL"
    if not topology and not intents_by_strategy:
        state = "NO_STRATEGIES_ENABLED"
    else:
        state = "UNKNOWN"
    for item in topology:
        if not isinstance(item, dict):
            continue
        strategy_id = str(item.get("engine_id") or "").strip()
        required = bool(item.get("required") is True)
        result = results_by_engine.get(strategy_id) or {}
        row_intents = intents_by_strategy.get(strategy_id, [])
        reason_codes = [str(code) for code in (result.get("reason_codes") or [])]
        status = str(result.get("status") or "").strip().upper()
        if row_intents:
            decision_state = "INTENT_CREATED"
        elif market_blocked:
            decision_state = "MARKET_DATA_BLOCKED"
        elif status in {"SKIPPED", "PASS", "OK"} and "EXISTING_CANONICAL_INTENTS_PRESENT" not in reason_codes:
            decision_state = "EVALUATED_NO_INTENT"
        elif status in {"SKIPPED"}:
            decision_state = "INTENT_SUPPRESSED"
        elif status in {"DISABLED", "NOT_ENABLED"} or not required:
            decision_state = "STRATEGY_DISABLED"
        elif status in {"FAIL", "ERROR"}:
            decision_state = "STRATEGY_ERROR"
        else:
            decision_state = "SIGNAL_MISSING" if required else "STRATEGY_DISABLED"
        strategies.append(
            {
                "strategy_id": strategy_id,
                "enabled": required,
                "decision_state": decision_state,
                "status": status or "UNKNOWN",
                "reason_codes": reason_codes,
                "script_path": str(item.get("script_path") or result.get("script_path") or ""),
                "signal_evidence_paths": [str(path) for path in result.get("output_paths", [])] if isinstance(result.get("output_paths"), list) else [],
                "intents": row_intents,
            }
        )
    for strategy_id, row_intents in intents_by_strategy.items():
        if any(row["strategy_id"] == strategy_id for row in strategies):
            continue
        strategies.append(
            {
                "strategy_id": strategy_id,
                "enabled": True,
                "decision_state": "INTENT_CREATED",
                "status": "INTENT_PRESENT",
                "reason_codes": [],
                "script_path": "",
                "signal_evidence_paths": [],
                "intents": row_intents,
            }
        )

    states = {str(row.get("decision_state") or "") for row in strategies}
    if "MARKET_DATA_BLOCKED" in states:
        state = "MARKET_DATA_BLOCKED"
    elif "STRATEGY_ERROR" in states:
        state = "STRATEGY_ERROR"
    elif "SIGNAL_MISSING" in states:
        state = "SIGNAL_MISSING"
    elif "INTENT_CREATED" in states:
        state = "INTENT_CREATED"
    elif states == {"STRATEGY_DISABLED"}:
        state = "STRATEGY_DISABLED"
    elif "EVALUATED_NO_INTENT" in states or "INTENT_SUPPRESSED" in states:
        state = "EVALUATED_NO_INTENT"
    elif not strategies:
        state = "NO_STRATEGIES_ENABLED"

    intent_count = sum(len(row.get("intents", [])) for row in strategies)
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "STRATEGY_DECISIONING",
        "status": "PASS" if state in {"INTENT_CREATED", "EVALUATED_NO_INTENT", "STRATEGY_DISABLED", "NO_STRATEGIES_ENABLED"} else "FAIL",
        "strategy_decision_state": state,
        "enabled_strategy_count": len([row for row in strategies if row.get("enabled")]),
        "intent_count": intent_count,
        "zero_intent_reason": "" if intent_count else ("NO_STRATEGIES_ENABLED" if not strategies else state),
        "strategies": strategies,
        "intent_mappings": [intent for row in strategies for intent in row.get("intents", [])],
        "first_blocker": "MARKET_DATA_AUTHORITY_BLOCKED" if state == "MARKET_DATA_BLOCKED" else (state if state in {"SIGNAL_MISSING", "STRATEGY_ERROR"} else ""),
        "input_evidence": [
            {"artifact_type": "trading_day_intent_generation_v1", "path": str(intent_generation_path), "exists": intent_generation_path.exists()},
            {"artifact_type": "intents_v1", "path": str((execution_root / "intents_v1" / "snapshots" / day_utc).resolve()), "exists": (execution_root / "intents_v1" / "snapshots" / day_utc).exists()},
            {"artifact_type": "market_data_authority_v1", "path": str(market_data_path), "exists": market_data_path.exists()},
            {"artifact_type": "paper_trading_day_authority_v1", "path": str(paper_authority_path), "exists": paper_authority_path.exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_strategy_decision_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = strategy_decision_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
