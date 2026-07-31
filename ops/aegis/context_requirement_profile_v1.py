from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1

from ops.aegis.intelligence_common_v1 import now_utc_v1


REPORT_FAMILY = "aegis_context_requirement_profile_v1"
DEFAULT_ACTIVE_PROFILE_ID = "HUMAN_REVIEWED_PAPER_MODE"


def profile_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / "context_requirement_profile.v1.json"


def active_profile_id_v1() -> str:
    explicit = str(os.environ.get("AEGIS_CONTEXT_REQUIREMENT_PROFILE") or "").strip().upper()
    if explicit:
        profile_id = explicit
    else:
        legacy_vix_mode = str(os.environ.get("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE") or "").strip().upper()
        if legacy_vix_mode == "STRICT_CURRENT_SESSION":
            profile_id = "STRICT_CURRENT_SESSION_MODE"
        elif legacy_vix_mode == "EOD_ADVISORY":
            profile_id = "EOD_ADVISORY_MODE"
        else:
            raw = os.environ.get("AEGIS_OPERATING_MODE") or DEFAULT_ACTIVE_PROFILE_ID
            profile_id = str(raw or "").strip().upper()
    if profile_id == "HUMAN_APPROVED_ADVISORY_RUNTIME":
        return "EOD_ADVISORY_MODE"
    return profile_id if profile_id in context_profiles_v1() else DEFAULT_ACTIVE_PROFILE_ID


def context_profiles_v1() -> dict[str, Any]:
    return {
        "HUMAN_REVIEWED_PAPER_MODE": {
            "profile_id": "HUMAN_REVIEWED_PAPER_MODE",
            "description": "Human-reviewed paper candidate review. Allows prior certified reference context when explicitly non-blocking; forbids trade advice and broker execution.",
            "requirements": [
                _req("market.price.<symbol>", "Candidate entry/reference pricing", "CURRENT_SESSION", ["LOCAL_CACHE_CURRENT_SESSION", "PROVIDER_CURRENT_SESSION"], "BLOCKING", "PAPER_REVIEW_ALLOWED"),
                _req("market.breadth.*", "Breadth regime context", "CURRENT_OR_PROXY", ["BREADTH_PROXY", "MANUAL_CSV_DROP"], "WARNING_OR_POLICY_BLOCKING", "EVENT_READY"),
                _req("market.volatility.VIX", "Volatility regime reference", "PRIOR_EOD_REFERENCE_ALLOWED", ["CURRENT_SESSION_VIX", "PRIOR_CERTIFIED_EOD_VIX", "MANUAL_CSV_DROP"], "NON_BLOCKING", "PAPER_REVIEW_ALLOWED", max_prior_trading_days_allowed=5),
                _req("trade_advice", "Trade advice permission", "FORBIDDEN", [], "BLOCKING", "TRADE_ADVICE_ALLOWED"),
                _req("broker_execution", "Broker execution permission", "FORBIDDEN", [], "BLOCKING", "BROKER_SUBMIT_TRANSMIT"),
            ],
        },
        "CANDIDATE_VISIBILITY_ONLY": {
            "profile_id": "CANDIDATE_VISIBILITY_ONLY",
            "description": "Read-only candidate visibility. Context gaps must be labeled but do not promote, advise, or execute.",
            "requirements": [
                _req("market.price.<symbol>", "Candidate visibility pricing", "CURRENT_OR_LATEST_REFERENCE", ["LOCAL_CACHE_CURRENT_SESSION", "PRIOR_EOD_REFERENCE"], "WARNING", "portal_runtime_model"),
                _req("market.breadth.*", "Context label only", "CURRENT_OR_PROXY", ["BREADTH_PROXY", "MANUAL_CSV_DROP"], "WARNING", "portal_runtime_model"),
                _req("market.volatility.VIX", "Volatility label only", "PRIOR_EOD_REFERENCE_ALLOWED", ["CURRENT_SESSION_VIX", "PRIOR_CERTIFIED_EOD_VIX", "MANUAL_CSV_DROP"], "WARNING", "portal_runtime_model", max_prior_trading_days_allowed=10),
                _req("trade_advice", "Trade advice permission", "FORBIDDEN", [], "BLOCKING", "TRADE_ADVICE_ALLOWED"),
                _req("broker_execution", "Broker execution permission", "FORBIDDEN", [], "BLOCKING", "BROKER_SUBMIT_TRANSMIT"),
            ],
        },
        "EOD_ADVISORY_MODE": {
            "profile_id": "EOD_ADVISORY_MODE",
            "description": "End-of-day advisory context. Current/final EOD evidence is required for advice; broker execution remains forbidden.",
            "requirements": [
                _req("market.price.<symbol>", "Advisory pricing", "FINAL_EOD_OR_CURRENT_SESSION", ["FINAL_EOD_CERTIFIED", "PROVIDER_CURRENT_SESSION"], "BLOCKING", "TRADE_ADVICE_ALLOWED"),
                _req("market.breadth.*", "Breadth regime context", "CURRENT_OR_PROXY", ["BREADTH_PROXY", "MANUAL_CSV_DROP"], "WARNING_OR_POLICY_BLOCKING", "EVENT_READY"),
                _req("market.volatility.VIX", "Volatility regime context", "CURRENT_SESSION_AFTER_EOD_CUTOFF", ["CURRENT_SESSION_VIX", "PRIOR_CERTIFIED_EOD_VIX"], "BLOCKING_AFTER_CUTOFF", "EVENT_READY", max_prior_trading_days_allowed=1),
                _req("trade_advice", "Trade advice permission", "REQUIRES_FULL_RUNTIME_READY", [], "BLOCKING", "TRADE_ADVICE_ALLOWED"),
                _req("broker_execution", "Broker execution permission", "FORBIDDEN", [], "BLOCKING", "BROKER_SUBMIT_TRANSMIT"),
            ],
        },
        "STRICT_CURRENT_SESSION_MODE": {
            "profile_id": "STRICT_CURRENT_SESSION_MODE",
            "description": "Strict current-session certification. Prior references are not accepted for blocking context.",
            "requirements": [
                _req("market.price.<symbol>", "Strict pricing", "CURRENT_SESSION", ["PROVIDER_CURRENT_SESSION"], "BLOCKING", "DATA_READY"),
                _req("market.breadth.*", "Strict breadth", "CURRENT_SESSION", ["BREADTH_PROXY"], "BLOCKING", "EVENT_READY"),
                _req("market.volatility.VIX", "Strict VIX", "CURRENT_SESSION", ["CURRENT_SESSION_VIX"], "BLOCKING", "EVENT_READY", max_prior_trading_days_allowed=0),
                _req("trade_advice", "Trade advice permission", "REQUIRES_FULL_RUNTIME_READY", [], "BLOCKING", "TRADE_ADVICE_ALLOWED"),
                _req("broker_execution", "Broker execution permission", "FORBIDDEN", [], "BLOCKING", "BROKER_SUBMIT_TRANSMIT"),
            ],
        },
    }


def build_context_requirement_profile_v1(
    *, truth_root: Path, day_utc: str, generated_at_utc: str | None = None, active_profile_id: str | None = None
) -> dict[str, Any]:
    profiles = context_profiles_v1()
    profile_id = str(active_profile_id or active_profile_id_v1()).strip().upper()
    if profile_id not in profiles:
        profile_id = DEFAULT_ACTIVE_PROFILE_ID
    payload = {
        "schema_id": "aegis_context_requirement_profile",
        "schema_version": "v1",
        "artifact_id": "aegis_context_requirement_profile_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or now_utc_v1(),
        "active_profile_id": profile_id,
        "profiles": profiles,
        "active_profile": profiles[profile_id],
        "truth_root": str(Path(truth_root).resolve()),
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def write_context_requirement_profile_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = profile_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return {"json": str(path)}


def load_or_build_context_requirement_profile_v1(
    *, truth_root: Path, day_utc: str, generated_at_utc: str | None = None
) -> dict[str, Any]:
    payload = build_context_requirement_profile_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        generated_at_utc=generated_at_utc,
    )
    write_context_requirement_profile_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return payload


def requirement_for_item_v1(profile_payload: dict[str, Any], data_item_id: str, context_item_id: str = "") -> dict[str, Any]:
    active = profile_payload.get("active_profile") if isinstance(profile_payload.get("active_profile"), dict) else {}
    requirements = active.get("requirements") if isinstance(active.get("requirements"), list) else []
    item = str(data_item_id or "")
    context = str(context_item_id or "")
    for row in requirements:
        if not isinstance(row, dict):
            continue
        pattern = str(row.get("required_evidence_item") or "")
        if _matches_requirement(pattern, item, context):
            return row
    return {}


def severity_is_blocking_v1(requirement: dict[str, Any]) -> bool:
    severity = str(requirement.get("blocker_severity") or "BLOCKING").upper()
    return severity in {"BLOCKING", "BLOCKING_AFTER_CUTOFF"}


def prior_eod_reference_allowed_v1(requirement: dict[str, Any], *, reference_age: int | None) -> bool:
    freshness = str(requirement.get("freshness_requirement") or "").upper()
    fallbacks = {str(item).upper() for item in requirement.get("allowed_fallback_reference_types") or []}
    if "PRIOR_EOD_REFERENCE_ALLOWED" not in freshness and "PRIOR_CERTIFIED_EOD_VIX" not in fallbacks:
        return False
    if reference_age is None:
        return False
    max_prior = requirement.get("max_prior_trading_days_allowed")
    if max_prior in (None, ""):
        return True
    try:
        return int(reference_age) <= max(0, int(max_prior))
    except Exception:
        return False


def profile_summary_for_output_v1(profile_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "active_profile_id": str(profile_payload.get("active_profile_id") or ""),
        "path": str(profile_path_v1(truth_root=Path(str(profile_payload.get("truth_root") or ".")), day_utc=str(profile_payload.get("day_utc") or ""))),
        "canonical_json_hash": str(profile_payload.get("canonical_json_hash") or ""),
    }


def _req(
    evidence_item: str,
    purpose: str,
    freshness: str,
    fallbacks: list[str],
    severity: str,
    capability: str,
    *,
    max_prior_trading_days_allowed: int | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "required_evidence_item": evidence_item,
        "purpose": purpose,
        "freshness_requirement": freshness,
        "allowed_fallback_reference_types": fallbacks,
        "blocker_severity": severity,
        "consuming_capability": capability,
    }
    if max_prior_trading_days_allowed is not None:
        row["max_prior_trading_days_allowed"] = max_prior_trading_days_allowed
    return row


def _matches_requirement(pattern: str, data_item_id: str, context_item_id: str) -> bool:
    if pattern == data_item_id:
        return True
    if pattern == "market.price.<symbol>" and data_item_id.startswith("market.price."):
        return True
    if pattern == "market.breadth.*" and (data_item_id.startswith("market.breadth.") or context_item_id in {"advance_decline_delta", "breadth_down_pct"}):
        return True
    return False
