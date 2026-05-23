from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1
from ops.aegis.trade_ticket_lineage_v1 import canonical_construction_contract_v1


SCHEMA_ID = "paper_trade_construction"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "paper_trade_construction_v1"
REPORT_FILENAME = "paper_trade_construction.v1.json"
DEFAULT_POLICY_PATH = Path("ops/config/aegis_paper_trade_construction_defaults.json")
RISK_POLICY_REGISTRY_PATH = Path("governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json")
CAPITAL_AUTHORITY_POLICY_PATH = Path("governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json")
SUBMIT_BOUNDARY_BLOCKERS = {"SUBMIT_BOUNDARY_MISSING", "SUBMIT_BOUNDARY_BLOCKED"}

ALLOWED_STATUSES = {
    "complete",
    "blocked_missing_market_data",
    "blocked_missing_capital_authority",
    "blocked_missing_entry_policy",
    "blocked_missing_sizing_policy",
    "blocked_missing_stop_policy",
    "blocked_missing_risk_policy",
    "blocked_submit_boundary",
    "blocked_incomplete_trade_definition",
}

BLOCKER_MESSAGES = {
    "NO_SELECTED_EXPOSURE": "No current selected exposure was found.",
    "SELECTED_EXPOSURE_IS_NOT_TRADE": "Selected exposure is review input only until paper trade construction is complete.",
    "MISSING_CURRENT_MARKET_DATA": "Latest valid market data for the selected symbol is missing or stale.",
    "MISSING_CAPITAL_AUTHORITY": "Capital authority allocation artifact or selected-intent authorization is missing.",
    "CAPITAL_AUTHORITY_BLOCKED": "Existing capital authority evidence is present but blocks this selected intent.",
    "CAPITAL_POLICY_ZERO_HEADROOM": "Existing capital authority policy allows zero headroom for this sleeve or engine.",
    "CAPITAL_RISK_ENVELOPE_BLOCKED": "Existing capital risk envelope is present but blocks paper construction.",
    "MISSING_ENTRY_POLICY": "No governed entry-reference policy is available.",
    "MISSING_ENTRY_REFERENCE_PRICE": "Entry/reference price could not be constructed.",
    "MISSING_SIZING_POLICY": "No governed sizing policy is available.",
    "MISSING_SUGGESTED_QUANTITY": "Suggested quantity could not be constructed.",
    "MISSING_STOP_POLICY": "No governed stop or invalidation policy is available.",
    "MISSING_STOP_OR_INVALIDATION_LEVEL": "Stop price or invalidation level is required.",
    "MISSING_RISK_POLICY": "No governed risk policy is available.",
    "MISSING_RISK_ESTIMATE": "Risk estimate requires entry, quantity, and stop or invalidation.",
    "SUBMIT_BOUNDARY_MISSING": "Submit-boundary precheck artifact is missing.",
    "SUBMIT_BOUNDARY_BLOCKED": "Submit-boundary precheck is not clear.",
    "INCOMPLETE_TRADE_DEFINITION": "One or more required paper trade fields are still missing.",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _source_ref(path: Path | None, artifact_id: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    exists = bool(path and path.exists() and path.is_file())
    return {
        "artifact_id": artifact_id,
        "path": str(path or ""),
        "exists": exists,
        "sha256": _sha256_file(path) if exists and path is not None else "",
        "schema_id": str((payload or {}).get("schema_id") or ""),
    }


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    return parsed


def _money(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def _decimal_text(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.normalize(), "f")


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _nested(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _report_path(root: Path, day: str) -> Path:
    return root / "reports" / REPORT_FAMILY / day / REPORT_FILENAME


def paper_trade_construction_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(Path(truth_root).expanduser().resolve(), str(day_utc))


def _selected_intent(selected: Mapping[str, Any]) -> tuple[dict[str, Any], Path | None]:
    path_text = str(selected.get("source_artifact_path") or selected.get("intent_path") or "").strip()
    if not path_text:
        return {}, None
    path = Path(path_text).expanduser().resolve()
    return _read_json(path), path


def _market_year_path(root: Path, symbol: str, day: str) -> Path:
    return root / "market_data_snapshot_v1" / symbol.upper() / f"{day[:4]}.jsonl"


def _latest_market_row(root: Path, symbol: str, day: str) -> tuple[dict[str, Any], Path]:
    path = _market_year_path(root, symbol, day)
    if not path.exists():
        return {}, path
    latest: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        ts = str(row.get("timestamp_utc") or "")
        if ts[:10] <= day:
            latest = row
    return latest, path


def _policy(root: Path) -> tuple[dict[str, Any], Path]:
    local = root / "config" / "paper_trade_construction_policy.v1.json"
    path = local if local.exists() else (_repo_root() / DEFAULT_POLICY_PATH).resolve()
    return _read_json(path), path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _repo_ref(relative_path: Path, artifact_id: str) -> tuple[dict[str, Any], Path]:
    path = (_repo_root() / relative_path).resolve()
    return _read_json(path), path


def _risk_policy(engine_id: str) -> tuple[dict[str, Any], Path]:
    registry, path = _repo_ref(RISK_POLICY_REGISTRY_PATH, "risk_policy_registry")
    policies = registry.get("policies") if isinstance(registry.get("policies"), Mapping) else registry
    policy = policies.get(engine_id) if isinstance(policies.get(engine_id), Mapping) else {}
    return dict(policy), path


def _capital_policy(engine_id: str) -> tuple[dict[str, Any], Path]:
    registry, path = _repo_ref(CAPITAL_AUTHORITY_POLICY_PATH, "capital_authority_policy")
    rows = registry.get("sleeves") if isinstance(registry.get("sleeves"), list) else []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        engine_ids = row.get("engine_ids") if isinstance(row.get("engine_ids"), list) else []
        if engine_id == str(row.get("sleeve_id") or "") or engine_id in {str(value) for value in engine_ids}:
            return dict(row), path
    return {}, path


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def _candidate_execution_roots(root: Path) -> list[Path]:
    candidates = [root]
    if root.name == "truth":
        candidates.append(root.parent / "truth_sleeves" / "PRIMARY" / "PAPER")
    runtime_truth = Path("/home/node/constellation_runtime_data/truth").resolve()
    c2_runtime_truth = Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth").resolve()
    if _is_relative_to(root, runtime_truth):
        candidates.append(Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER"))
    if _is_relative_to(root, c2_runtime_truth):
        candidates.append(Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth_sleeves/PRIMARY/PAPER"))
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        key = str(resolved)
        if key not in seen:
            seen.add(key)
            unique.append(resolved)
    return unique


def _allocation_decision(root: Path, day: str, intent_id: str, intent_path: Path | None) -> tuple[dict[str, Any], Path | None]:
    intent_path_text = str(intent_path or "")
    for execution_root in _candidate_execution_roots(root):
        directory = execution_root / "allocation_v1" / "decisions" / day
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.allocation_decision.v1.json")):
            payload = _read_json(path)
            decision = payload.get("decision") if isinstance(payload.get("decision"), Mapping) else {}
            manifest = payload.get("input_manifest") if isinstance(payload.get("input_manifest"), Mapping) else {}
            manifest_path = str(manifest.get("intent_path") or manifest.get("exposure_intent_path") or "")
            if str(decision.get("intent_id") or payload.get("intent_id") or "") == intent_id:
                return payload, path
            if intent_path_text and manifest_path and Path(manifest_path).expanduser().resolve() == Path(intent_path_text).expanduser().resolve():
                return payload, path
    return {}, None


def _capital_risk_envelope(root: Path, day: str) -> tuple[dict[str, Any], Path | None]:
    for execution_root in _candidate_execution_roots(root):
        path = execution_root / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json"
        if path.exists():
            return _read_json(path), path
    return {}, None


def _capital_path(root: Path, day: str) -> Path:
    return root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json"


def _capital_authority_allocation(root: Path, day: str) -> tuple[dict[str, Any], Path]:
    fallback = _capital_path(root, day)
    for execution_root in _candidate_execution_roots(root):
        path = _capital_path(execution_root, day)
        if path.exists():
            return _read_json(path), path
    return {}, fallback


def _authorized_intent(capital: Mapping[str, Any], intent_id: str) -> dict[str, Any]:
    chain = capital.get("decision_chain") if isinstance(capital.get("decision_chain"), Mapping) else {}
    rows = chain.get("authorized_trade_intents") if isinstance(chain.get("authorized_trade_intents"), list) else []
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("intent_id") or "") == intent_id:
            return dict(row)
    return {}


def _submit_boundary(root: Path, day: str) -> tuple[dict[str, Any], Path]:
    path = root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json"
    return _read_json(path), path


def _runtime_evaluation(root: Path, day: str) -> tuple[dict[str, Any], Path]:
    path = runtime_evaluation_path_v1(truth_root=root, day_utc=day)
    return read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day), path

def _direction(selected: Mapping[str, Any], intent: Mapping[str, Any]) -> str:
    explicit = _first_text(selected.get("direction"), selected.get("proposed_direction"))
    if explicit:
        return explicit.upper()
    exposure_type = str(intent.get("exposure_type") or "").upper()
    if exposure_type == "LONG_EQUITY":
        return "LONG"
    if exposure_type == "SHORT_EQUITY":
        return "SHORT"
    return ""


def _blocker(codes: list[str], code: str) -> None:
    if code not in codes:
        codes.append(code)


def _status(codes: list[str]) -> str:
    if "MISSING_CURRENT_MARKET_DATA" in codes:
        return "blocked_missing_market_data"
    if any(code in codes for code in {"MISSING_CAPITAL_AUTHORITY", "CAPITAL_AUTHORITY_BLOCKED", "CAPITAL_POLICY_ZERO_HEADROOM", "CAPITAL_RISK_ENVELOPE_BLOCKED"}):
        return "blocked_missing_capital_authority"
    if "MISSING_ENTRY_POLICY" in codes or "MISSING_ENTRY_REFERENCE_PRICE" in codes:
        return "blocked_missing_entry_policy"
    if "MISSING_SIZING_POLICY" in codes or "MISSING_SUGGESTED_QUANTITY" in codes:
        return "blocked_missing_sizing_policy"
    if "MISSING_STOP_POLICY" in codes or "MISSING_STOP_OR_INVALIDATION_LEVEL" in codes:
        return "blocked_missing_stop_policy"
    if "MISSING_RISK_POLICY" in codes or "MISSING_RISK_ESTIMATE" in codes:
        return "blocked_missing_risk_policy"
    if "SUBMIT_BOUNDARY_MISSING" in codes or "SUBMIT_BOUNDARY_BLOCKED" in codes:
        return "blocked_submit_boundary"
    if codes:
        return "blocked_incomplete_trade_definition"
    return "complete"


def _missing_fields(payload: Mapping[str, Any], codes: list[str]) -> list[dict[str, str]]:
    field_map = [
        ("entry_reference_price", "entry reference price", "MISSING_ENTRY_REFERENCE_PRICE", "Entry/reference price could not be constructed.", "Resolve market data and entry policy, then rerun paper trade construction."),
        ("suggested_quantity", "suggested quantity", "MISSING_SUGGESTED_QUANTITY", "Suggested quantity could not be constructed.", "Resolve capital authority and sizing policy."),
        ("suggested_notional", "notional", "INCOMPLETE_TRADE_DEFINITION", "Notional requires entry price and quantity.", "Construct entry and quantity first."),
        ("stop_price", "stop/invalidation policy", "MISSING_STOP_OR_INVALIDATION_LEVEL", "No stop price or invalidation level is present.", "Generate stop/invalidation policy."),
        ("risk_estimate", "risk estimate", "MISSING_RISK_ESTIMATE", "Risk requires entry, stop/invalidation, and quantity.", "Resolve stop and sizing policy."),
    ]
    rows: list[dict[str, str]] = []
    for field, label, code, why, upstream in field_map:
        missing = False
        if field == "risk_estimate":
            missing = not payload.get("risk_per_share") or not payload.get("max_loss_estimate")
        elif field == "suggested_notional":
            missing = not payload.get("suggested_notional")
        elif field == "stop_price":
            missing = not payload.get("stop_price") and not payload.get("invalidation_level")
        else:
            missing = payload.get(field) in {"", None}
        if missing:
            rows.append({"field": field, "label": label, "blocker_code": code, "why_missing": why, "upstream_step": upstream})
    for code in codes:
        if code in {row["blocker_code"] for row in rows}:
            continue
        rows.append({"field": code.lower(), "label": code.lower().replace("_", " "), "blocker_code": code, "why_missing": BLOCKER_MESSAGES.get(code, code), "upstream_step": "Regenerate the upstream paper trade construction input."})
    return rows


def build_paper_trade_construction_v1(
    *,
    truth_root: Path | str,
    day_utc: str | None = None,
    current_operator_truth: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    generated = generated_at_utc or _now_iso()
    root_in = Path(truth_root).expanduser().resolve()
    current_truth = dict(current_operator_truth or resolve_current_operator_truth_v1(truth_root=root_in, day_utc=day_utc, generated_at_utc=generated))
    root = Path(str(current_truth.get("truth_root") or root_in)).expanduser().resolve()
    day = str(current_truth.get("source_day") or day_utc or generated[:10])
    selected = _nested(current_truth, "selected_exposure")
    intent, intent_path = _selected_intent(selected)
    constraints = intent.get("constraints") if isinstance(intent.get("constraints"), Mapping) else {}
    selected_id = _first_text(selected.get("selected_exposure_intent_id"), selected.get("candidate_id"), current_truth.get("selected_exposure_intent_id"), intent.get("intent_id"))
    symbol = _first_text(selected.get("symbol"), _nested(intent, "underlying").get("symbol")).upper()
    direction = _direction(selected, intent)
    sleeve_id = _first_text(selected.get("sleeve_id"), selected.get("engine_id"), _nested(intent, "engine").get("engine_id"))
    engine_id = _first_text(selected.get("engine_id"), sleeve_id, _nested(intent, "engine").get("engine_id"))
    source_run_id = str(current_truth.get("source_run_id") or "")
    conversion_artifact = _nested(current_truth, "conversion")
    conversion_path_text = str(current_truth.get("conversion_path") or "").strip()
    conversion_path = Path(conversion_path_text).expanduser().resolve() if conversion_path_text else None
    # Conversion is downstream of paper construction. Do not use conversion fields
    # to construct the ticket, or conversion attempts churn governed risk/identity evidence.
    conversion: dict[str, Any] = {}

    policy, policy_path = _policy(root)
    defaults_enabled = bool(policy.get("enabled") is True)
    paper_defaults = policy.get("paper_defaults") if isinstance(policy.get("paper_defaults"), Mapping) else {}
    entry_rule = str(policy.get("entry_reference_rule") or paper_defaults.get("entry_reference_rule") or "").strip()
    rounding_rule = str(policy.get("quantity_rounding_rule") or paper_defaults.get("quantity_rounding_rule") or "").strip()
    default_stop_policy = policy.get("default_stop_policy") if isinstance(policy.get("default_stop_policy"), Mapping) else {}
    paper_capital_base = _dec(policy.get("paper_test_capital_base") or paper_defaults.get("paper_test_capital_base"))
    max_notional = _dec(policy.get("max_notional_per_trade") or paper_defaults.get("max_notional_per_trade"))
    max_risk = _dec(policy.get("max_risk_per_trade") or paper_defaults.get("max_risk_per_trade"))
    risk_policy, risk_policy_path = _risk_policy(engine_id)
    risk_contract_ref = str(risk_policy.get("contract_ref") or "").strip()
    risk_contract_path = (_repo_root() / risk_contract_ref).resolve() if risk_contract_ref else None
    stop_policy_id = _first_text(risk_policy.get("policy_id"))
    invalidation_policy = dict(risk_policy.get("invalidation_policy")) if isinstance(risk_policy.get("invalidation_policy"), Mapping) else {}
    expected_holding_days = _first_text(constraints.get("expected_holding_days"), intent.get("expected_holding_days"), risk_policy.get("expected_holding_days_default"))
    capital_policy, capital_policy_path = _capital_policy(engine_id)

    market_row, market_path = _latest_market_row(root, symbol, day)
    market_session = str(market_row.get("timestamp_utc") or "")[:10]
    market_current = bool(symbol and market_session == day)
    entry = _dec(conversion.get("entry_reference_price") or conversion.get("entry_price"))
    entry_source = "exposure_intent_conversion" if entry is not None else ""
    blockers: list[str] = ["SELECTED_EXPOSURE_IS_NOT_TRADE"]
    if not selected_id:
        _blocker(blockers, "NO_SELECTED_EXPOSURE")
    if not market_current:
        _blocker(blockers, "MISSING_CURRENT_MARKET_DATA")
    if entry is None and market_current:
        market_close = _dec(market_row.get("close"))
        if market_close is not None and market_close > 0:
            entry = market_close
            entry_source = "market_data_snapshot_v1.latest_close"
        elif defaults_enabled and entry_rule in {"latest_close", "latest_close_or_midpoint", "midpoint_or_latest_close"}:
            entry = _dec(market_row.get("close"))
            entry_source = f"paper_default:{entry_rule}"
        else:
            _blocker(blockers, "MISSING_ENTRY_POLICY")
    if entry is None or entry <= 0:
        _blocker(blockers, "MISSING_ENTRY_REFERENCE_PRICE")

    capital, cap_path = _capital_authority_allocation(root, day)
    cap_row = _authorized_intent(capital, selected_id)
    cap_status = str(cap_row.get("authorization_outcome") or "").upper()
    cap_qty = int(cap_row.get("authorized_quantity") or 0) if str(cap_row.get("authorized_quantity") or "").strip().isdigit() else 0
    allocation_decision, allocation_decision_path = _allocation_decision(root, day, selected_id, intent_path)
    envelope, envelope_path = _capital_risk_envelope(root, day)
    capital_authority_source = ""
    if cap_row:
        capital_authority_source = "capital_authority_allocation_v1"
        if cap_status not in {"APPROVED", "RESIZED"}:
            _blocker(blockers, "CAPITAL_AUTHORITY_BLOCKED")
    elif allocation_decision:
        capital_authority_source = "allocation_decision_v1"
        decision = allocation_decision.get("decision") if isinstance(allocation_decision.get("decision"), Mapping) else {}
        decision_status = str(allocation_decision.get("status") or decision.get("status") or decision.get("decision") or "").upper()
        raw_qty = decision.get("authorized_quantity") or decision.get("contracts_allowed") or allocation_decision.get("authorized_quantity") or allocation_decision.get("contracts_allowed")
        cap_qty = int(raw_qty or 0) if str(raw_qty or "").strip().isdigit() else 0
        if decision_status not in {"PASS", "ALLOW", "AUTHORIZED", "APPROVED", "RESIZED"} or cap_qty <= 0:
            _blocker(blockers, "CAPITAL_AUTHORITY_BLOCKED")
    capital_limits = capital_policy.get("limits") if isinstance(capital_policy.get("limits"), Mapping) else capital_policy
    max_capital_at_risk_cents = _dec(capital_limits.get("max_capital_at_risk_cents")) if capital_policy else None
    max_symbols = _dec(capital_limits.get("max_symbols")) if capital_policy else None
    if paper_capital_base is None and capital_policy:
        paper_capital_base = _dec(capital_limits.get("paper_test_capital_base"))
    if max_notional is None and capital_policy:
        max_notional = _dec(capital_limits.get("max_notional_per_trade"))
    if max_risk is None and capital_policy:
        max_risk = _dec(capital_limits.get("max_risk_per_trade"))
    if capital_policy and ((max_capital_at_risk_cents is not None and max_capital_at_risk_cents <= 0) or (max_symbols is not None and max_symbols <= 0)):
        _blocker(blockers, "CAPITAL_POLICY_ZERO_HEADROOM")
    envelope_status = str(envelope.get("status") or "").upper()
    if envelope and envelope_status not in {"PASS", "OK", "READY", "CLEAR"}:
        _blocker(blockers, "CAPITAL_RISK_ENVELOPE_BLOCKED")
    envelope_headroom = _dec(_nested(envelope, "envelope").get("headroom_cents"))
    paper_enabled = bool(capital_limits.get("paper_enabled") is True) if isinstance(capital_limits, Mapping) else False
    if not cap_row and not allocation_decision and not capital_authority_source:
        if capital_policy and paper_enabled and envelope and envelope_status in {"PASS", "OK", "READY", "CLEAR"} and (envelope_headroom is None or envelope_headroom > 0):
            capital_authority_source = "capital_policy_and_risk_envelope_v1"
            cap_status = "POLICY_HEADROOM"
        else:
            _blocker(blockers, "MISSING_CAPITAL_AUTHORITY")

    allocation = _dec(conversion.get("allocation_percent") or intent.get("target_notional_pct"))
    if allocation is None and risk_policy:
        allocation = _dec(risk_policy.get("target_notional_pct_default"))
    if allocation is None:
        _blocker(blockers, "MISSING_SIZING_POLICY")
    quantity: int | None = cap_qty if cap_qty > 0 else None
    notional: Decimal | None = None

    stop_price = _dec(conversion.get("stop_price"))
    invalidation = _first_text(conversion.get("invalidation_level"), intent.get("invalidation_level"))
    stop_source = "exposure_intent_conversion" if stop_price is not None or invalidation else ""
    stop_bps = _dec(constraints.get("stop_loss_bps") or intent.get("stop_loss_bps"))
    if stop_price is None and entry is not None and entry > 0:
        if stop_bps is None and risk_policy:
            stop_bps = _dec(risk_policy.get("stop_loss_bps_default"))
            stop_source = f"C2_RISK_POLICY_REGISTRY_V1:{engine_id}.stop_loss_bps_default" if stop_bps is not None else stop_source
        if stop_bps is None and defaults_enabled:
            stop_bps = _dec(default_stop_policy.get("stop_loss_bps"))
            stop_source = "paper_default:default_stop_policy" if stop_bps is not None else stop_source
        elif stop_bps is not None and not stop_source:
            stop_source = "exposure_intent.constraints.stop_loss_bps"
        if stop_bps is not None and stop_bps > 0:
            multiplier = Decimal("1") + (stop_bps / Decimal("10000")) if direction == "SHORT" else Decimal("1") - (stop_bps / Decimal("10000"))
            if multiplier > 0:
                stop_price = (entry * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if stop_price is None and not invalidation:
        _blocker(blockers, "MISSING_STOP_POLICY")
        _blocker(blockers, "MISSING_STOP_OR_INVALIDATION_LEVEL")

    preliminary_risk_per_share: Decimal | None = abs(entry - stop_price) if entry is not None and stop_price is not None else None
    if entry is not None and entry > 0:
        if quantity is None and paper_capital_base is not None and allocation is not None:
            candidate_notional = paper_capital_base * allocation
            if max_notional is not None:
                candidate_notional = min(candidate_notional, max_notional)
            quantity_by_notional = int((candidate_notional / entry).to_integral_value(rounding=ROUND_FLOOR))
            quantity = quantity_by_notional
            if preliminary_risk_per_share is not None and preliminary_risk_per_share > 0 and max_risk is not None:
                quantity_by_risk = int((max_risk / preliminary_risk_per_share).to_integral_value(rounding=ROUND_FLOOR))
                quantity = min(quantity_by_notional, quantity_by_risk)
        elif quantity is None and defaults_enabled and rounding_rule in {"floor", "floor_shares"} and paper_capital_base is not None and allocation is not None:
            candidate_notional = paper_capital_base * allocation
            if max_notional is not None:
                candidate_notional = min(candidate_notional, max_notional)
            quantity = int((candidate_notional / entry).to_integral_value(rounding=ROUND_FLOOR))
        if quantity is not None and quantity > 0:
            notional = entry * Decimal(quantity)
    if quantity is None or quantity <= 0:
        _blocker(blockers, "MISSING_SUGGESTED_QUANTITY")
    if notional is None or notional <= 0:
        _blocker(blockers, "INCOMPLETE_TRADE_DEFINITION")

    risk_budget_source = ""
    risk_budget = _dec(constraints.get("max_risk_pct") or conversion.get("risk_budget_reference") or allocation)
    if risk_budget is not None:
        risk_budget_source = "exposure_intent.constraints.max_risk_pct_or_target_notional_pct"
    elif defaults_enabled and max_risk is not None:
        risk_budget_source = "paper_default:max_risk_per_trade"
    else:
        _blocker(blockers, "MISSING_RISK_POLICY")
    risk_per_share: Decimal | None = None
    max_loss: Decimal | None = None
    estimated_notional_risk_pct: Decimal | None = None
    if entry is not None and stop_price is not None and quantity is not None and quantity > 0:
        risk_per_share = abs(entry - stop_price)
        max_loss = risk_per_share * Decimal(quantity)
        if notional is not None and notional > 0:
            estimated_notional_risk_pct = (max_loss / notional).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        if max_risk is not None and max_loss > max_risk:
            _blocker(blockers, "MISSING_RISK_POLICY")
    if risk_per_share is None or max_loss is None:
        _blocker(blockers, "MISSING_RISK_ESTIMATE")

    submit, submit_path = _submit_boundary(root, day)
    submit_status = str(submit.get("status") or "")
    if not submit:
        _blocker(blockers, "SUBMIT_BOUNDARY_MISSING")
    elif submit_status.upper() not in {"PASS", "READY", "CLEAR", "BLOCKED"}:
        _blocker(blockers, "SUBMIT_BOUNDARY_BLOCKED")
    elif submit_status.upper() == "BLOCKED":
        _blocker(blockers, "SUBMIT_BOUNDARY_BLOCKED")
    runtime_evaluation, runtime_evaluation_path = _runtime_evaluation(root, day)
    runtime_evaluation_hash = str(runtime_evaluation.get("deterministic_output_hash") or "")

    source_artifacts = [
        _source_ref(Path(str(current_truth.get("portfolio_gate_report_path") or "")) if current_truth.get("portfolio_gate_report_path") else None, "portfolio_gate_candidate_report", _nested(current_truth, "portfolio_gate_report")),
        _source_ref(intent_path, "selected_exposure_intent", intent),
        _source_ref(conversion_path, "exposure_intent_conversion", conversion_artifact),
        _source_ref(market_path, "market_data_snapshot_v1", market_row),
        _source_ref(cap_path, "capital_authority_allocation_v1", capital),
        _source_ref(allocation_decision_path, "allocation_decision_v1", allocation_decision),
        _source_ref(envelope_path, "capital_risk_envelope_v2", envelope),
        _source_ref(capital_policy_path, "capital_authority_policy_v1", capital_policy),
        _source_ref(risk_policy_path, "risk_policy_registry_v1", risk_policy),
        _source_ref(risk_contract_path, "risk_policy_contract_v1", {"schema_id": stop_policy_id or "risk_policy_contract"}),
        _source_ref(policy_path, "paper_trade_construction_policy_v1", policy),
        _source_ref(submit_path, "submit_boundary_status_v1", submit),
        _source_ref(runtime_evaluation_path, "aegis_runtime_evaluation", runtime_evaluation),
    ]

    payload_core: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated,
        "construction_id": "",
        "selected_exposure_intent_id": selected_id,
        "symbol": symbol,
        "direction": direction,
        "sleeve_id": sleeve_id,
        "engine_id": engine_id,
        "source_day": day,
        "source_run_id": source_run_id,
        "portfolio_gate_decision_id": source_run_id,
        "symbol_authority_source": "selected_exposure",
        "market_data_snapshot_id": _sha256_file(market_path) if market_path.exists() else "",
        "market_data_latest_session": market_session,
        "trade_construction_status": "",
        "entry_reference_price": _money(entry),
        "entry_reference_source": entry_source,
        "suggested_quantity": quantity if quantity and quantity > 0 else None,
        "suggested_notional": _money(notional),
        "allocation_percent": _decimal_text(allocation),
        "stop_price": _money(stop_price),
        "invalidation_level": invalidation,
        "stop_policy_source": stop_source,
        "stop_policy_id": stop_policy_id,
        "stop_loss_bps": _decimal_text(stop_bps),
        "invalidation_policy": invalidation_policy,
        "expected_holding_days": expected_holding_days,
        "risk_per_share": _money(risk_per_share),
        "max_loss_estimate": _money(max_loss),
        "estimated_notional_risk_pct": _decimal_text(estimated_notional_risk_pct),
        "risk_budget_source": risk_budget_source,
        "capital_authority_source": capital_authority_source,
        "capital_authority_status": cap_status or str(allocation_decision.get("status") or ""),
        "submit_boundary_status": submit_status,
        "runtime_evaluation_hash": runtime_evaluation_hash,
        "manual_capture_ready": False,
        "blocker_codes": [],
        "blocker_messages": [],
        "missing_fields": [],
        "source_artifacts": source_artifacts,
        "paper_default_policy_enabled": defaults_enabled,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "paper_submit_created": False,
        "selected_candidate_mutation_allowed": False,
    }
    effective_blockers = [code for code in blockers if code != "SELECTED_EXPOSURE_IS_NOT_TRADE"]
    construction_blockers = [code for code in effective_blockers if code not in SUBMIT_BOUNDARY_BLOCKERS]
    status = _status(construction_blockers)
    payload_core["trade_construction_status"] = status
    payload_core["manual_capture_ready"] = status == "complete"
    payload_core["blocker_codes"] = sorted(set(effective_blockers))
    payload_core["blocker_messages"] = [BLOCKER_MESSAGES.get(code, code) for code in payload_core["blocker_codes"]]
    payload_core["missing_fields"] = _missing_fields(payload_core, payload_core["blocker_codes"])
    contract = canonical_construction_contract_v1(payload_core)
    payload_core["construction_contract_id"] = contract["construction_contract_id"]
    payload_core["construction_contract_hash"] = contract["construction_contract_hash"]
    payload_core["construction_contract"] = contract
    construction_id = "paper-trade-construction:" + contract["construction_contract_hash"][:24]
    payload_core["construction_id"] = construction_id
    payload_core["paper_trade_construction_id"] = construction_id
    payload_core["artifact_id"] = f"paper_trade_construction_v1:{day}:{construction_id.rsplit(':', 1)[-1]}"
    payload_core["immutable_hash"] = _stable_hash({**payload_core, "immutable_hash": ""})
    return payload_core


def write_paper_trade_construction_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    path = paper_trade_construction_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    return path


def build_and_write_paper_trade_construction_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    current_operator_truth: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> tuple[dict[str, Any], Path]:
    payload = build_paper_trade_construction_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        current_operator_truth=current_operator_truth,
        generated_at_utc=generated_at_utc,
    )
    path = write_paper_trade_construction_v1(truth_root=truth_root, day_utc=str(payload.get("source_day") or day_utc), payload=payload)
    return {**payload, "artifact_path": str(path)}, path


def read_paper_trade_construction_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(paper_trade_construction_path_v1(truth_root=truth_root, day_utc=day_utc))


def latest_paper_trade_construction_v1(*, truth_root: Path | str, day_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc or "")
    if not day:
        base = root / "reports" / REPORT_FAMILY
        days = sorted(path.name for path in base.iterdir() if path.is_dir()) if base.exists() else []
        day = days[-1] if days else _now_iso()[:10]
    payload = read_paper_trade_construction_v1(truth_root=root, day_utc=day)
    if payload:
        payload["artifact_path"] = str(paper_trade_construction_path_v1(truth_root=root, day_utc=day))
        return payload
    payload, _path = build_and_write_paper_trade_construction_v1(truth_root=root, day_utc=day)
    return payload


def paper_trade_construction_view_v1(construction: Mapping[str, Any]) -> dict[str, Any]:
    status = str(construction.get("trade_construction_status") or "blocked_incomplete_trade_definition")
    return {
        **dict(construction),
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "candidate_available": bool(construction.get("selected_exposure_intent_id")),
        "trade_ticket_status": status,
        "capture_status": "Ready for manual paper capture" if status == "complete" else "Not capture-ready",
        "captured_manually_allowed": status == "complete",
        "capture_as_trade_disabled": status != "complete",
        "operator_action_allowed": status == "complete",
        "next_required_action": "Ready for manual paper capture." if status == "complete" else _next_action(status),
        "trade_ticket_projection_v1": dict(construction),
        "latest_market_session": str(construction.get("market_data_latest_session") or ""),
        "required_market_session": str(construction.get("source_day") or ""),
        "stale_market_data": construction.get("market_data_latest_session") != construction.get("source_day"),
        "data_freshness_status": {
            "status": "CURRENT" if construction.get("market_data_latest_session") == construction.get("source_day") else "STALE_OR_MISSING",
            "expected_session": str(construction.get("source_day") or ""),
            "observed_session": str(construction.get("market_data_latest_session") or ""),
            "current": construction.get("market_data_latest_session") == construction.get("source_day"),
        },
    }


def _next_action(status: str) -> str:
    return {
        "blocked_missing_market_data": "Refresh canonical market data, then rerun paper trade construction.",
        "blocked_missing_capital_authority": "Generate capital authority allocation for the selected exposure, then rerun construction.",
        "blocked_missing_entry_policy": "Enable governed PAPER entry policy or generate entry policy upstream.",
        "blocked_missing_sizing_policy": "Generate sizing policy or approved quantity, then rerun construction.",
        "blocked_missing_stop_policy": "Generate stop/invalidation policy, then rerun construction.",
        "blocked_missing_risk_policy": "Generate risk policy, then rerun construction.",
        "blocked_submit_boundary": "Run submit-boundary precheck; do not submit or bypass it.",
    }.get(status, "Resolve missing construction blockers, then rerun paper trade construction.")
