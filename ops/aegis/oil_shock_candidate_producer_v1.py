from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from statistics import quantiles
from typing import Any, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import resolve_paper_intent_truth_root_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.oil_shock_candidate_construction_v1 import build_oil_shock_candidate_construction_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

getcontext().prec = 28

FAMILY = "aegis_oil_shock_candidate_producer_v1"
FILENAME = "oil_shock_candidate_producer.v1.json"
POLICY_VERSION = "AEGIS_OIL_SHOCK_CANDIDATE_PRODUCER_V1"
ENGINE_ID = "C2_OIL_SHOCK_REVERSAL_V1"
ENGINE_SUITE = "C2_HYBRID_V1"
RISK_CLASS = "OIL_SHOCK_REVERSAL"
HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
THESIS_ID = "THESIS_EVENT_DISLOCATION_V1"
HYPOTHESIS_NAME = "Oil shock reversals across energy ETFs"
EVENT_FAMILY_ID = "oil_shock"
EXPOSURE_INTENT_SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v1.schema.json"
SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "forced_candidate_creation_allowed": False,
    "candidate_contracts_authoritative": True,
    "paper_lifecycle_authoritative": True,
    "paper_observation_created_by_producer": False,
    "allocation_mutation_performed": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def oil_shock_candidate_producer_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def oil_shock_sleeve_evaluation_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "sleeve_evaluation_kernel_v1" / str(day_utc) / ENGINE_ID / "sleeve_evaluation.v1.json"


def build_oil_shock_candidate_producer_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str, intent_truth_root: Path | str | None = None, write_intent: bool = True) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve()
    intent_root = Path(intent_truth_root).expanduser().resolve() if intent_truth_root else resolve_paper_intent_truth_root_v1(truth_root=root, repo_root=repo)
    paths = _paths(root, repo, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items() if path.suffix == ".json"}
    evidence = _find_oil(payloads.get("evidence_packets", {}).get("evidence_packets"))
    blueprint = _find_oil(payloads.get("paper_blueprint", {}).get("paper_sleeve_blueprints"))
    setup = _find_oil(payloads.get("paper_setup", {}).get("paper_tracking_setups"))
    readiness = _find_oil(payloads.get("paper_readiness", {}).get("paper_readiness_certifications"))
    proposal_path = Path(text_v1(evidence.get("input_proposal_file"))).expanduser() if text_v1(evidence.get("input_proposal_file")) else Path()
    proposal = read_json_v1(proposal_path) if proposal_path.exists() else {}
    market = _load_market_data(truth_root=root, intent_root=intent_root, day_utc=str(day_utc), universe=_universe(evidence, blueprint))
    result = _evaluate(
        repo=repo,
        day_utc=str(day_utc),
        intent_root=intent_root,
        evidence=evidence,
        blueprint=blueprint,
        setup=setup,
        readiness=readiness,
        proposal=proposal,
        proposal_path=proposal_path,
        market=market,
        construction=build_oil_shock_candidate_construction_v1(truth_root=root, repo_root=repo, day_utc=str(day_utc)),
        write_intent=write_intent,
    )
    sleeve = _sleeve_evaluation_payload(
        truth_root=root,
        repo=repo,
        day_utc=str(day_utc),
        intent_truth_root=intent_root,
        result=result,
        paths=paths,
        proposal_path=proposal_path,
    )
    artifact = {
        "schema_id": "aegis_oil_shock_candidate_producer",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "policy_version": POLICY_VERSION,
        "engine_id": ENGINE_ID,
        "hypothesis_id": HYPOTHESIS_ID,
        "hypothesis_name": HYPOTHESIS_NAME,
        "producer_status": result["producer_status"],
        "last_evaluation_status": result["producer_status"],
        "reason_codes": result["reason_codes"],
        "candidate_count": len(result["output_intents"]),
        "raw_signal_count": len(result["output_intents"]),
        "candidate_flow_started": bool(result["output_intents"]),
        "exact_blocker": result["exact_blocker"],
        "david_action_required": False,
        "source_artifact_paths": {**{key: str(path) for key, path in paths.items()}, "source_proposal": str(proposal_path) if proposal_path else "", "intent_truth_root": str(intent_root)},
        "input_artifact_hashes": {**{key: file_hash_v1(path) for key, path in paths.items()}, "source_proposal": file_hash_v1(proposal_path)},
        "instrument_universe": result["instrument_universe"],
        "required_evidence_fields": result["required_evidence_fields"],
        "available_evidence_fields": result["available_evidence_fields"],
        "missing_evidence_fields": result["missing_evidence_fields"],
        "required_market_symbols": result["required_market_symbols"],
        "available_market_symbols": result["available_market_symbols"],
        "missing_market_symbols": result["missing_market_symbols"],
        "candidate_generation_rules": result["candidate_generation_rules"],
        "market_evaluation": result["market_evaluation"],
        "output_intents": result["output_intents"],
        "raw_signal_emitted": bool(result["output_intents"]),
        "normal_pipeline_required": True,
        "sleeve_evaluation_path": str(oil_shock_sleeve_evaluation_path_v1(truth_root=root, day_utc=str(day_utc))),
        "computed_at_utc": _now(),
        **SAFETY,
        "safety": dict(SAFETY),
    }
    artifact["deterministic_rerun_id"] = stable_hash_v1({"day_utc": str(day_utc), "policy_version": POLICY_VERSION, "inputs": artifact["input_artifact_hashes"], "status": result["producer_status"]})
    artifact["content_hash"] = stable_hash_v1(_without_generated_time(artifact))
    artifact["sleeve_evaluation"] = sleeve
    return artifact


def write_oil_shock_candidate_producer_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> dict[str, str]:
    body = payload or build_oil_shock_candidate_producer_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    producer_path = write_json_v1(oil_shock_candidate_producer_path_v1(truth_root=truth_root, day_utc=day_utc), body)
    sleeve_path = write_json_v1(oil_shock_sleeve_evaluation_path_v1(truth_root=truth_root, day_utc=day_utc), body["sleeve_evaluation"])
    return {"producer": str(producer_path), "sleeve_evaluation": str(sleeve_path)}


def build_or_reuse_oil_shock_intent_v1(*, repo_root: Path | str, day_utc: str, mode: str, truth_root: Path | str, symbols: list[str] | None = None, write_intent: bool = True) -> dict[str, Any]:
    repo = Path(repo_root).expanduser().resolve()
    intent_root = Path(truth_root).expanduser().resolve()
    canonical_root = Path("/home/node/constellation_runtime_data/truth")
    if not (canonical_root / "reports").exists():
        canonical_root = repo
    payload = build_oil_shock_candidate_producer_v1(truth_root=canonical_root, repo_root=repo, day_utc=day_utc, intent_truth_root=intent_root, write_intent=write_intent)
    return payload


def _paths(root: Path, repo: Path, day: str) -> dict[str, Path]:
    return {
        "evidence_packets": report_path_v1(root, "aegis_hypothesis_evidence_packet_v1", day, "evidence_packets.v1.json"),
        "paper_blueprint": report_path_v1(root, "aegis_paper_sleeve_blueprint_v1", day, "paper_sleeve_blueprint.v1.json"),
        "paper_setup": report_path_v1(root, "aegis_approved_hypothesis_paper_tracking_setup_v1", day, "approved_hypothesis_paper_tracking_setup.v1.json"),
        "paper_readiness": report_path_v1(root, "aegis_paper_readiness_certification_v1", day, "paper_readiness_certification.v1.json"),
        "candidate_construction": report_path_v1(root, "aegis_oil_shock_candidate_construction_v1", day, "oil_shock_candidate_construction.v1.json"),
        "exit_policy": repo / "ops/aegis/trade_lifecycle/exit_policy_registry_v1.py",
        "exposure_intent_schema": repo / EXPOSURE_INTENT_SCHEMA_RELPATH,
    }


def _find_oil(rows: Any) -> dict[str, Any]:
    for row in rows or []:
        if isinstance(row, Mapping):
            text = json.dumps(row, sort_keys=True, default=str).lower()
            if HYPOTHESIS_ID.lower() in text or "oil shock reversals" in text:
                return dict(row)
    return {}


def _universe(evidence: Mapping[str, Any], blueprint: Mapping[str, Any]) -> list[str]:
    return sorted({text_v1(item).upper() for item in [*(evidence.get("instrument_universe") or []), *(blueprint.get("instrument_universe") or [])] if text_v1(item)})


def _available_fields(evidence: Mapping[str, Any], blueprint: Mapping[str, Any]) -> set[str]:
    values = {
        "hypothesis_statement": evidence.get("hypothesis_statement") or blueprint.get("hypothesis_name"),
        "instrument_universe": evidence.get("instrument_universe") or blueprint.get("instrument_universe"),
        "entry_logic": evidence.get("entry_logic") or blueprint.get("entry_logic"),
        "exit_logic": evidence.get("exit_logic") or blueprint.get("exit_logic"),
        "data_availability": (evidence.get("readiness") or {}).get("data_requirement_status") or (evidence.get("readiness") or {}).get("market_data_availability"),
        "sample_frequency": evidence.get("expected_sample_frequency") or blueprint.get("expected_sample_frequency"),
    }
    return {key for key, value in values.items() if value}


def _event_rules(proposal: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"daily_return_threshold": None, "range_percentile": None, "primary_test": text_v1((proposal.get("proposed_event_definition") or {}).get("primary_test"))}
    for row in ((proposal.get("proposed_event_definition") or {}).get("default_event_definitions") or []):
        if not isinstance(row, Mapping):
            continue
        kind = text_v1(row.get("type"))
        params = row.get("params") if isinstance(row.get("params"), Mapping) else {}
        if kind == "daily_return_above_threshold":
            out["daily_return_threshold"] = text_v1(params.get("threshold"))
        elif kind == "daily_range_percentile_above":
            out["range_percentile"] = text_v1(params.get("percentile"))
    return out


def _load_market_data(*, truth_root: Path, intent_root: Path, day_utc: str, universe: list[str]) -> dict[str, Any]:
    manifest_path = intent_root / "market_data_snapshot_v1" / "dataset_manifest.json"
    manifest = read_json_v1(manifest_path)
    files = manifest.get("files") if isinstance(manifest.get("files"), list) else []
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in files:
        if isinstance(row, Mapping):
            sym = text_v1(row.get("symbol")).upper()
            if sym:
                by_symbol.setdefault(sym, []).append(dict(row))
    certified_current = _current_market_data_symbols(truth_root=truth_root, day_utc=day_utc, universe=universe)
    missing = [sym for sym in universe if sym not in by_symbol and sym not in certified_current]
    bars: dict[str, list[dict[str, Any]]] = {}
    for sym in universe:
        rows: list[dict[str, Any]] = []
        for entry in sorted(by_symbol.get(sym, []), key=lambda item: (int(item.get("year") or 0), text_v1(item.get("file")))):
            rel = text_v1(entry.get("file"))
            expected = text_v1(entry.get("sha256")).lower()
            path = (intent_root / "market_data_snapshot_v1" / rel).resolve()
            if not path.exists() or not str(path).startswith(str((intent_root / "market_data_snapshot_v1").resolve())):
                missing.append(sym)
                continue
            if expected and hashlib.sha256(path.read_bytes()).hexdigest().lower() != expected:
                missing.append(sym)
                continue
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if text_v1(obj.get("symbol")).upper() == sym and text_v1(obj.get("timestamp_utc"))[:10] <= day_utc:
                    rows.append(obj)
        bars[sym] = sorted(rows, key=lambda item: text_v1(item.get("timestamp_utc")))
    return {"manifest_path": str(manifest_path), "available_symbols": sorted(set(by_symbol) | set(certified_current)), "missing_symbols": sorted(set(missing)), "bars": bars, "current_market_data_symbols": certified_current}


def _current_market_data_symbols(*, truth_root: Path, day_utc: str, universe: list[str]) -> list[str]:
    market_path = Path(truth_root).expanduser().resolve() / "reports" / "aegis_market_data_v1" / str(day_utc) / "market_data.v1.json"
    market = read_json_v1(market_path)
    rows = market.get("symbols") if isinstance(market.get("symbols"), dict) else {}
    out: list[str] = []
    for symbol in universe:
        row = rows.get(symbol) if isinstance(rows.get(symbol), Mapping) else {}
        session = text_v1(row.get("market_session_date") or row.get("returned_data_date") or market.get("market_session_date"))
        if text_v1(row.get("freshness_status")).upper() == "CURRENT" and (not session or session == day_utc):
            out.append(symbol)
    return sorted(set(out))


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(str(value)) from exc


def _daily_range_pct(row: Mapping[str, Any]) -> Decimal:
    high = _dec(row.get("high")); low = _dec(row.get("low")); close = _dec(row.get("close"))
    if close <= 0:
        raise ValueError("NON_POSITIVE_CLOSE")
    return (high - low) / close


def _evaluate_market(*, day_utc: str, universe: list[str], market: Mapping[str, Any], rules: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    threshold = Decimal(str(rules.get("daily_return_threshold") or "0"))
    percentile_raw = Decimal(str(rules.get("range_percentile") or "0"))
    source_symbols = [sym for sym in ["USO", "XLE", "XOP"] if sym in universe]
    if not source_symbols:
        source_symbols = list(universe)
    evaluations: list[dict[str, Any]] = []
    qualifiers: list[dict[str, Any]] = []
    bars_by_symbol = market.get("bars") if isinstance(market.get("bars"), Mapping) else {}
    for sym in source_symbols:
        bars = [row for row in bars_by_symbol.get(sym, []) if isinstance(row, Mapping)]
        if len(bars) < 2:
            evaluations.append({"symbol": sym, "status": "MISSING_DATA", "reason_codes": ["INSUFFICIENT_MARKET_HISTORY"]})
            continue
        today = next((row for row in reversed(bars) if text_v1(row.get("timestamp_utc"))[:10] == day_utc), None)
        if not today:
            evaluations.append({"symbol": sym, "status": "MISSING_DATA", "reason_codes": ["MISSING_TARGET_DAY_BAR"]})
            continue
        idx = bars.index(today)
        if idx < 1:
            evaluations.append({"symbol": sym, "status": "MISSING_DATA", "reason_codes": ["INSUFFICIENT_MARKET_HISTORY"]})
            continue
        prev = bars[idx - 1]
        prev_close = _dec(prev.get("close")); close = _dec(today.get("close"))
        if prev_close <= 0:
            evaluations.append({"symbol": sym, "status": "MISSING_DATA", "reason_codes": ["NON_POSITIVE_PREV_CLOSE"]})
            continue
        daily_return = (close - prev_close) / prev_close
        range_pct = _daily_range_pct(today)
        history_ranges = []
        for row in bars[: idx + 1]:
            try:
                history_ranges.append(_daily_range_pct(row))
            except Exception:
                pass
        percentile_threshold = Decimal("Infinity")
        if len(history_ranges) >= 20 and percentile_raw > 0:
            ordered = sorted(history_ranges)
            pos = int((len(ordered) - 1) * (float(percentile_raw) / 100.0))
            percentile_threshold = ordered[max(0, min(pos, len(ordered) - 1))]
        return_qualifies = abs(daily_return) >= threshold if threshold > 0 else False
        range_qualifies = range_pct >= percentile_threshold if percentile_threshold.is_finite() else False
        row = {
            "symbol": sym,
            "status": "VALID_CANDIDATE_SIGNAL" if (return_qualifies or range_qualifies) else "NO_MARKET_SETUP",
            "daily_return": str(daily_return),
            "daily_return_threshold": str(threshold),
            "range_pct": str(range_pct),
            "range_percentile_threshold": str(percentile_threshold) if percentile_threshold.is_finite() else "INSUFFICIENT_HISTORY_FOR_PERCENTILE",
            "return_qualifies": return_qualifies,
            "range_qualifies": range_qualifies,
            "reason_codes": ["VALID_CANDIDATE_SIGNAL"] if (return_qualifies or range_qualifies) else ["NO_MARKET_SETUP"],
        }
        evaluations.append(row)
        if return_qualifies or range_qualifies:
            qualifiers.append(row)
    return qualifiers, {"source_symbols": source_symbols, "evaluations": evaluations}


def _evaluate(*, repo: Path, day_utc: str, intent_root: Path, evidence: Mapping[str, Any], blueprint: Mapping[str, Any], setup: Mapping[str, Any], readiness: Mapping[str, Any], proposal: Mapping[str, Any], proposal_path: Path, market: Mapping[str, Any], construction: Mapping[str, Any], write_intent: bool) -> dict[str, Any]:
    required = list(evidence.get("required_evidence_fields") or blueprint.get("required_evidence_fields") or [])
    available = _available_fields(evidence, blueprint)
    missing_fields = [field for field in required if field not in available]
    universe = _universe(evidence, blueprint)
    rules = _event_rules(proposal)
    construction_policy = blueprint.get("candidate_construction_policy") if isinstance(blueprint.get("candidate_construction_policy"), Mapping) else {}
    risk_policy = blueprint.get("risk_policy") if isinstance(blueprint.get("risk_policy"), Mapping) else {}
    construction_status = text_v1(construction.get("candidate_construction_status")).upper()
    construction_bridge_ready = construction_status == "READY_FOR_MARKET_EVALUATION" and not construction.get("missing_construction_fields")
    policy_complete = construction_bridge_ready or (bool(construction_policy.get("requires_valid_candidate_contract")) and bool(construction_policy.get("requires_entry_reference_price_certification")) and bool(readiness.get("readiness_checks", {}).get("entry_exit_price_certification_path_exists", True)) and (repo / "ops/aegis/trade_lifecycle/exit_policy_registry_v1.py").exists() and bool(risk_policy))
    setup_ready = construction_bridge_ready or (setup.get("candidate_generation_eligible") is True and not setup.get("missing_fields"))
    missing_market = [sym for sym in universe if sym in set(market.get("missing_symbols") or [])]
    base = {
        "instrument_universe": universe,
        "required_evidence_fields": required,
        "available_evidence_fields": sorted(available),
        "missing_evidence_fields": missing_fields,
        "required_market_symbols": universe,
        "available_market_symbols": sorted(set(market.get("available_symbols") or []).intersection(universe)),
        "missing_market_symbols": missing_market,
        "candidate_generation_rules": rules,
        "candidate_construction_status": text_v1(construction.get("candidate_construction_status")),
        "missing_construction_fields": list(construction.get("missing_construction_fields") or []),
        "candidate_construction_artifact": construction,
        "market_evaluation": {},
        "output_intents": [],
    }
    if not evidence or missing_fields:
        return {**base, "producer_status": "MISSING_DATA", "reason_codes": ["MISSING_DATA"], "exact_blocker": "MISSING_DATA"}
    if missing_market:
        return {**base, "producer_status": "SYSTEM_DATA_PIPELINE_REQUIRED", "reason_codes": ["SYSTEM_DATA_PIPELINE_REQUIRED", "MISSING_MARKET_DATA"], "exact_blocker": "SYSTEM_DATA_PIPELINE_REQUIRED"}
    construction_missing = [text_v1(item) for item in construction.get("missing_construction_fields") or [] if text_v1(item)]
    if construction_status == "POLICY_INCOMPLETE" or construction_missing:
        return {**base, "producer_status": "POLICY_INCOMPLETE", "reason_codes": ["POLICY_INCOMPLETE", *[f"MISSING_{item.upper()}" for item in construction_missing]], "exact_blocker": "POLICY_INCOMPLETE"}
    if not setup_ready:
        return {**base, "producer_status": "POLICY_INCOMPLETE", "reason_codes": ["POLICY_INCOMPLETE", "MISSING_PAPER_TRACKING_SETUP"], "exact_blocker": "POLICY_INCOMPLETE"}
    if not policy_complete:
        return {**base, "producer_status": "POLICY_INCOMPLETE", "reason_codes": ["POLICY_INCOMPLETE"], "exact_blocker": "POLICY_INCOMPLETE"}
    if not proposal_path.exists() or not rules.get("daily_return_threshold") or not rules.get("range_percentile"):
        return {**base, "producer_status": "CANDIDATE_CONSTRUCTION_INCOMPLETE", "reason_codes": ["CANDIDATE_CONSTRUCTION_INCOMPLETE", "EVENT_RULES_MISSING"], "exact_blocker": "CANDIDATE_CONSTRUCTION_INCOMPLETE"}
    qualifiers, market_eval = _evaluate_market(day_utc=day_utc, universe=universe, market=market, rules=rules)
    if not qualifiers:
        return {**base, "producer_status": "NO_MARKET_SETUP", "reason_codes": ["NO_MARKET_SETUP"], "exact_blocker": "NO_MARKET_SETUP", "market_evaluation": market_eval}
    qualifiers.sort(key=lambda row: (abs(Decimal(row.get("daily_return") or "0")), row.get("symbol", "")), reverse=True)
    selected = qualifiers[0]
    intent = _build_intent(repo=repo, day_utc=day_utc, intent_root=intent_root, symbol=str(selected["symbol"]), write_intent=write_intent)
    return {**base, "producer_status": "VALID_CANDIDATE_SIGNAL", "reason_codes": ["VALID_CANDIDATE_SIGNAL"], "exact_blocker": "NONE", "market_evaluation": market_eval, "output_intents": [intent]}


def _build_intent(*, repo: Path, day_utc: str, intent_root: Path, symbol: str, write_intent: bool) -> dict[str, Any]:
    intent_obj = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": f"c2_oil_shock_reversal_{symbol.lower()}_{day_utc}_v1",
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "engine": {"engine_id": ENGINE_ID, "suite": ENGINE_SUITE, "mode": "PAPER"},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.05",
        "expected_holding_days": 5,
        "risk_class": RISK_CLASS,
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }
    validate_against_repo_schema_v1(intent_obj, repo, EXPOSURE_INTENT_SCHEMA_RELPATH)
    payload = canonical_json_bytes_v1(intent_obj) + b"\n"
    intent_hash = hashlib.sha256(payload).hexdigest()
    out_dir = intent_root / "intents_v1" / "snapshots" / day_utc
    out_path = out_dir / f"{intent_hash}.exposure_intent.v1.json"
    if write_intent:
        out_dir.mkdir(parents=True, exist_ok=True)
        if not out_path.exists():
            out_path.write_bytes(payload)
    return {"intent_id": intent_obj["intent_id"], "intent_hash": intent_hash, "raw_signal_id": intent_obj["intent_id"], "engine_id": ENGINE_ID, "sleeve_id": ENGINE_ID, "symbol": symbol, "intent_path": str(out_path), "raw_intent_path": str(out_path), "hypothesis_id": HYPOTHESIS_ID, "thesis_id": THESIS_ID}


def _sleeve_evaluation_payload(*, truth_root: Path, repo: Path, day_utc: str, intent_truth_root: Path, result: Mapping[str, Any], paths: Mapping[str, Path], proposal_path: Path) -> dict[str, Any]:
    output_intents = list(result.get("output_intents") or [])
    status = "INTENT_CREATED" if output_intents else ("NO_INTENT" if result.get("producer_status") == "NO_MARKET_SETUP" else "BLOCKED")
    blocker = "" if status in {"INTENT_CREATED", "NO_INTENT"} else text_v1(result.get("exact_blocker"))
    now = _now()
    return {
        "schema_id": "sleeve_evaluation_kernel",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": "PAPER",
        "run_id": f"oil_shock_candidate_producer_v1:{day_utc}",
        "run_mode": "INTRADAY_OPERATIONAL",
        "market_data_mode": "INTRADAY_OPERATIONAL",
        "final_eod_certification_status": "PENDING",
        "sleeve_id": ENGINE_ID,
        "engine_id": ENGINE_ID,
        "hypothesis_id": HYPOTHESIS_ID,
        "thesis_id": THESIS_ID,
        "enabled": True,
        "activation_status": "ACTIVE",
        "expected_intent_type": "ExposureIntent",
        "allowed_symbols": result.get("instrument_universe") or [],
        "registry_allowed_symbols": result.get("instrument_universe") or [],
        "producer_requested_symbols": result.get("instrument_universe") or [],
        "symbol_source": "approved_oil_shock_hypothesis_artifacts",
        "symbol_source_path": str(paths.get("evidence_packets") or ""),
        "resolved_symbol_count": len(result.get("instrument_universe") or []),
        "governing_policy_paths": [str(paths.get("evidence_packets") or ""), str(paths.get("paper_blueprint") or ""), str(proposal_path or "")],
        "status": status,
        "current_status": status,
        "producer_status": result.get("producer_status"),
        "last_evaluation_status": result.get("producer_status"),
        "canonical_blocker": blocker,
        "reason_codes": result.get("reason_codes") or [],
        "input_artifacts": [{"artifact_type": key, "path": str(path), "sha256": file_hash_v1(path)} for key, path in paths.items()],
        "market_data_manifest_check": {"status": "PASS" if not result.get("missing_market_symbols") else "BLOCKED", "canonical_blocker": "" if not result.get("missing_market_symbols") else "MISSING_MARKET_DATA", "missing_symbols": result.get("missing_market_symbols") or []},
        "output_intents": output_intents,
        "output_count": len(output_intents),
        "rejected_intents": [],
        "rejected_count": 0,
        "exposure_intent_batch": {"schema_id": "exposure_intent_batch", "schema_version": "v1", "producer_id": ENGINE_ID, "sleeve_id": ENGINE_ID, "engine_id": ENGINE_ID, "source_day": day_utc, "output_intents": output_intents, "output_count": len(output_intents), "blockers": result.get("reason_codes") if blocker else [], "immutable_hash": stable_hash_v1({"engine_id": ENGINE_ID, "day_utc": day_utc, "status": status, "outputs": output_intents})},
        "output_intent_path": text_v1(output_intents[0].get("intent_path")) if output_intents else "",
        "output_intent_id": text_v1(output_intents[0].get("intent_id")) if output_intents else "",
        "output_intent_hash": text_v1(output_intents[0].get("intent_hash")) if output_intents else "",
        "intent_symbol": text_v1(output_intents[0].get("symbol")) if output_intents else "",
        "intent_artifact_path": text_v1(output_intents[0].get("intent_path")) if output_intents else "",
        "producer_command": "npm run aegis:oil-shock-candidate-producer",
        "operator_next_action": "" if status != "BLOCKED" else "Resolve deterministic Oil Shock producer blocker before candidate construction.",
        "started_at_utc": now,
        "completed_at_utc": now,
        "duration_ms": 0,
        "exit_code": 0,
        "stdout_summary": json.dumps({"producer_status": result.get("producer_status"), "reason_codes": result.get("reason_codes")}, sort_keys=True),
        "stderr_summary": "",
        "signal_state": {"state": "ACTIVE" if output_intents else "INACTIVE", "duration_cycles": 1},
        "artifact_path": str(oil_shock_sleeve_evaluation_path_v1(truth_root=truth_root, day_utc=day_utc)),
        **SAFETY,
        "safety": dict(SAFETY),
    }


def _without_generated_time(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return {key: _without_generated_time(value) for key, value in payload.items() if key not in {"computed_at_utc", "started_at_utc", "completed_at_utc"}}
    if isinstance(payload, list):
        return [_without_generated_time(item) for item in payload]
    return payload


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
