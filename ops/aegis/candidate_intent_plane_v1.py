from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPORT_FAMILY = "candidate_intent_plane_v1"
SCHEMA_ID = "candidate_intent_plane"
SCHEMA_VERSION = "v1"
NY_TZ = ZoneInfo("America/New_York")

INTENT_STATES = {
    "DISCOVERED",
    "QUALIFIED",
    "SCORED",
    "SELECTED",
    "CONFIDENCE_ACCUMULATING",
    "CAPTURE_RECOMMENDED",
    "CAPTURE_DEFERRED",
    "CERTIFIED_CONFIRMED",
    "CERTIFICATION_DIVERGED",
    "SUPPRESSED",
    "BLOCKED",
    "EXPIRED",
}

CAPTURE_GUIDANCE = {
    "NO_USER_ACTION",
    "OBSERVE",
    "AWAIT_CERTIFICATION",
    "MANUAL_IB_CAPTURE_RECOMMENDED",
    "DO_NOT_CAPTURE",
    "SYSTEM_REPAIR_REQUIRED",
}

DEFAULT_INTENT_POLICY_V1 = {
    "policy_id": "aegis_intent_confidence_lifecycle_v1",
    "policy_version": "2026-05-22.1",
    "preliminary_capture_enabled": False,
    "confidence_threshold": 0.72,
    "stability_threshold": 0.70,
    "convergence_threshold": 0.75,
    "minimum_snapshot_observations": 2,
    "post_close_earliest_recommendation_time": "16:15:00 America/New_York",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).replace(microsecond=0)
    except ValueError:
        return None


def _iso(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _content_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        if path.exists() and path.is_file():
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
    except Exception:
        return {}
    return {}


def _sha(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _path(root: Path, day: str, family: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def candidate_intent_plane_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_intent_plane.v1.json"


def candidate_intent_history_path_v1(*, truth_root: Path | str, day_utc: str, intent_id: str) -> Path:
    safe_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(intent_id or "UNKNOWN"))
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "intents" / safe_id / "intent_history.v1.json"


def _latest_candidate_manifest(root: Path, day: str) -> Path:
    base = root / "reports" / "candidate_generation_manifest_v1" / day
    paths = sorted(base.glob("*/candidate_generation_manifest.v1.json")) if base.exists() else []
    if not paths:
        return base / "candidate_generation_manifest.v1.json"
    return max(paths, key=lambda p: (1 if p.parent.name.startswith(f"aegis_intraday_sleeves_now:{day}:") else 0, p.stat().st_mtime_ns, p.parent.name))


def _rows_by_intent(payload: dict[str, Any], key: str = "intent_id") -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("rankings") if isinstance(payload.get("rankings"), list) else []:
        if isinstance(row, dict) and str(row.get(key) or "").strip():
            out[str(row.get(key) or "").strip()] = row
    return out


def _arbitration_by_intent(payload: dict[str, Any]) -> tuple[str, dict[str, dict[str, Any]]]:
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else {}
    selected_id = str(selected.get("intent_id") or "").strip()
    out: dict[str, dict[str, Any]] = {}
    for collection in ("raw_candidate_intents", "candidate_intents", "rejected_or_filtered_intents"):
        for row in payload.get(collection) if isinstance(payload.get(collection), list) else []:
            if not isinstance(row, dict):
                continue
            intent_id = str(row.get("intent_id") or row.get("raw_intent_id") or "").strip()
            if intent_id:
                out[intent_id] = {**out.get(intent_id, {}), **row}
    if selected_id:
        out[selected_id] = {**out.get(selected_id, {}), **selected}
    return selected_id, out


def _promotion_by_intent(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("candidate_rows") if isinstance(payload.get("candidate_rows"), list) else []:
        if not isinstance(row, dict):
            continue
        for key in ("raw_intent_id", "candidate_id"):
            value = str(row.get(key) or "").strip()
            if value:
                out[value] = row
    return out


def _history(root: Path, day: str, intent_id: str) -> dict[str, Any]:
    return _read(candidate_intent_history_path_v1(truth_root=root, day_utc=day, intent_id=intent_id))


def _history_snapshots(root: Path, day: str, intent_id: str) -> list[dict[str, Any]]:
    history = _history(root, day, intent_id)
    snapshots = history.get("snapshots") if isinstance(history.get("snapshots"), list) else []
    return [row for row in snapshots if isinstance(row, dict)]


def _score(value: Any, max_value: float = 100.0) -> float:
    try:
        numeric = float(value)
    except Exception:
        return 0.0
    if max_value <= 0:
        return 0.0
    return round(max(0.0, min(1.0, numeric / max_value)), 6)


def _data_completeness(row: dict[str, Any], score_row: dict[str, Any], promotion: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    reason_codes = [
        *([str(code) for code in row.get("reason_codes")] if isinstance(row.get("reason_codes"), list) else []),
        *([str(code) for code in score_row.get("reason_codes")] if isinstance(score_row.get("reason_codes"), list) else []),
    ]
    missing_inputs = [code for code in reason_codes if "MISSING" in code.upper() or "STALE" in code.upper()]
    if str(promotion.get("exact_blocker") or "").strip():
        missing_inputs.append(str(promotion.get("exact_blocker") or ""))
    penalty = min(0.65, 0.16 * len(set(missing_inputs)))
    if str(row.get("candidate_data_status") or "").upper() in {"MISSING", "STALE", "INVALID"}:
        penalty += 0.25
    score = round(max(0.0, 1.0 - penalty), 6)
    return score, {"missing_or_stale_inputs": sorted(set(missing_inputs)), "penalty": round(penalty, 6)}


def _certification_convergence(row: dict[str, Any], score_row: dict[str, Any], promotion: dict[str, Any]) -> tuple[float, str]:
    state = str(
        promotion.get("certification_state")
        or row.get("certification_state")
        or score_row.get("certification_state")
        or row.get("final_eod_certification_status")
        or score_row.get("final_eod_certification_status")
        or ""
    ).upper()
    if state in {"CERTIFIED", "VALID", "PASS", "CERTIFIED_CONFIRMED"}:
        return 1.0, "CERTIFIED_CONFIRMED"
    if state in {"DIVERGED", "CERTIFICATION_DIVERGED", "INVALID", "REJECTED", "FAILED"}:
        return 0.0, "CERTIFICATION_DIVERGED"
    if state in {"PENDING", "CERTIFICATION_PENDING", "PROVISIONAL_INTRADAY", ""}:
        return 0.60, "CERTIFICATION_PENDING"
    return 0.35, state


def _stability(
    *,
    root: Path,
    day: str,
    intent_id: str,
    score_total: float,
    rank: int,
    blocker: str,
    certification_state: str,
) -> tuple[float, float, dict[str, Any], str]:
    snapshots = _history_snapshots(root, day, intent_id)
    prior = snapshots[-1] if snapshots else {}
    observation_count = len(snapshots) + 1
    if not prior:
        return 0.50, 0.50, {"observation_count": observation_count, "prior_intent_snapshot_id": "", "reason": "first observation"}, ""
    prior_score = float(prior.get("source_score_total") or 0.0)
    prior_rank = int(prior.get("rank") or 999999)
    prior_blocker = str(prior.get("blocker_reason") or "")
    prior_cert = str(prior.get("certification_state") or "")
    score_drift = min(1.0, abs(score_total - prior_score) / 100.0)
    rank_drift = min(1.0, abs(rank - prior_rank) / 10.0) if rank and prior_rank != 999999 else 0.15
    blocker_drift = 0.20 if blocker != prior_blocker else 0.0
    cert_drift = 0.15 if certification_state != prior_cert else 0.0
    drift = round(min(1.0, score_drift + rank_drift + blocker_drift + cert_drift), 6)
    stability = round(max(0.0, 1.0 - drift), 6)
    return stability, drift, {
        "observation_count": observation_count,
        "prior_intent_snapshot_id": str(prior.get("intent_snapshot_id") or ""),
        "prior_rank": prior_rank if prior_rank != 999999 else None,
        "current_rank": rank or None,
        "prior_blocker": prior_blocker,
        "current_blocker": blocker,
        "prior_certification_state": prior_cert,
        "current_certification_state": certification_state,
        "score_drift": round(score_drift, 6),
        "rank_drift": round(rank_drift, 6),
        "blocker_drift": blocker_drift,
        "certification_drift": cert_drift,
    }, str(prior.get("intent_snapshot_id") or "")


def _recommendation_time_passed(day_utc: str, now_utc: str, policy: dict[str, Any]) -> bool:
    now = _parse_dt(now_utc) or datetime.now(UTC).replace(microsecond=0)
    try:
        day = datetime.strptime(day_utc, "%Y-%m-%d").date()
    except ValueError:
        return False
    configured = str(policy.get("post_close_earliest_recommendation_time") or DEFAULT_INTENT_POLICY_V1["post_close_earliest_recommendation_time"])
    clock = configured.split()[0]
    try:
        hh, mm, ss = [int(part) for part in clock.split(":")]
    except Exception:
        hh, mm, ss = 16, 15, 0
    earliest = datetime.combine(day, time(hh, mm, ss), tzinfo=NY_TZ).astimezone(UTC)
    return now >= earliest


def _intent_state(
    *,
    selected: bool,
    status: str,
    score_available: bool,
    blocker: str,
    suppressed: bool,
    certification_state: str,
    guidance: str,
) -> str:
    if suppressed:
        return "SUPPRESSED"
    if blocker and not selected:
        return "BLOCKED"
    if certification_state == "CERTIFICATION_DIVERGED":
        return "CERTIFICATION_DIVERGED"
    if guidance == "MANUAL_IB_CAPTURE_RECOMMENDED":
        return "CAPTURE_RECOMMENDED"
    if selected and certification_state == "CERTIFIED_CONFIRMED":
        return "CERTIFIED_CONFIRMED"
    if selected:
        return "CONFIDENCE_ACCUMULATING"
    if score_available:
        return "SCORED"
    if status in {"CANDIDATE_CREATED", "INTENT_CREATED", "ACTIVE_CURRENT"}:
        return "QUALIFIED"
    return "DISCOVERED"


def _guidance(
    *,
    selected: bool,
    blocker: str,
    suppression_reason: str,
    data_score: float,
    confidence: float,
    stability: float,
    convergence: float,
    certification_state: str,
    observations: int,
    policy: dict[str, Any],
    now_utc: str,
    day_utc: str,
) -> tuple[str, str, str]:
    if blocker:
        return "SYSTEM_REPAIR_REQUIRED", f"Intent is blocked by {blocker}.", "BLOCKER_ACTIVE"
    if suppression_reason:
        return "DO_NOT_CAPTURE", f"Intent is suppressed: {suppression_reason}.", "SUPPRESSED"
    if not selected:
        return "OBSERVE", "Intent is not selected by portfolio arbitration.", "NOT_SELECTED"
    if data_score < 0.80:
        return "SYSTEM_REPAIR_REQUIRED", "Required data is incomplete or stale.", "DATA_INCOMPLETE"
    if observations < int(policy.get("minimum_snapshot_observations") or 1):
        return "AWAIT_CERTIFICATION", "Aegis needs another snapshot before stability can be trusted.", "MINIMUM_SNAPSHOTS_NOT_MET"
    if confidence < float(policy.get("confidence_threshold") or 1.0):
        return "AWAIT_CERTIFICATION", "Confidence is still below the configured recommendation threshold.", "CONFIDENCE_BELOW_THRESHOLD"
    if stability < float(policy.get("stability_threshold") or 1.0):
        return "AWAIT_CERTIFICATION", "Intent ranking or blockers changed too much between snapshots.", "STABILITY_BELOW_THRESHOLD"
    if certification_state == "CERTIFIED_CONFIRMED" and convergence >= float(policy.get("convergence_threshold") or 1.0):
        return "MANUAL_IB_CAPTURE_RECOMMENDED", "Final certification agrees with the selected intent; manual IB capture can be recorded if the operator acted externally.", "FINAL_RECOMMENDATION"
    if policy.get("preliminary_capture_enabled") is True:
        if not _recommendation_time_passed(day_utc, now_utc, policy):
            return "AWAIT_CERTIFICATION", "Preliminary recommendation window has not opened yet.", "TOO_EARLY_FOR_PRELIMINARY"
        if convergence >= float(policy.get("convergence_threshold") or 1.0):
            return "MANUAL_IB_CAPTURE_RECOMMENDED", "Preliminary confidence, stability, and convergence thresholds passed; final reconciliation remains required.", "PRELIMINARY_RECOMMENDATION"
    return "AWAIT_CERTIFICATION", "Final EOD certification has not converged yet.", "AWAITING_CERTIFICATION_CONVERGENCE"


def build_candidate_intent_plane_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    now_utc: str | None = None,
    policy_overrides: dict[str, Any] | None = None,
    write_histories: bool = False,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = now_utc or _now_iso()
    policy = {**DEFAULT_INTENT_POLICY_V1, **(policy_overrides or {})}
    manifest_path = _latest_candidate_manifest(root, day_utc)
    scoring_path = _path(root, day_utc, "portfolio_scoring_v1", "portfolio_scoring.v1.json")
    arbitration_path = _path(root, day_utc, "intent_arbitration_v1", "intent_arbitration.v1.json")
    promotion_path = _path(root, day_utc, "candidate_promotion_map_v1", "candidate_promotion_map.v1.json")
    market_inputs_path = _path(root, day_utc, "market_data_inputs_v1", "market_data_inputs.v1.json")
    manifest = _read(manifest_path)
    scoring = _read(scoring_path)
    arbitration = _read(arbitration_path)
    promotion_map = _read(promotion_path)
    market_inputs = _read(market_inputs_path)
    score_by_intent = _rows_by_intent(scoring)
    selected_id, arbitration_by_intent = _arbitration_by_intent(arbitration)
    promotion_by_intent = _promotion_by_intent(promotion_map)
    candidate_rows = manifest.get("candidate_rows") if isinstance(manifest.get("candidate_rows"), list) else []
    market_snapshot_ids = (
        manifest.get("input_market_data_snapshot_ids")
        if isinstance(manifest.get("input_market_data_snapshot_ids"), list)
        else market_inputs.get("input_market_data_snapshot_ids")
        if isinstance(market_inputs.get("input_market_data_snapshot_ids"), list)
        else []
    )
    intent_snapshots: list[dict[str, Any]] = []
    for row in candidate_rows:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("raw_intent_id") or row.get("intent_id") or row.get("candidate_id") or "").strip()
        if not intent_id:
            continue
        score_row = score_by_intent.get(intent_id, {})
        arbitration_row = arbitration_by_intent.get(intent_id, {})
        promotion = promotion_by_intent.get(intent_id, {})
        selected = intent_id == selected_id
        rank = int(score_row.get("rank") or promotion.get("rank") or 0)
        score_total = float(score_row.get("score_total") or promotion.get("score") or 0.0)
        score_available = bool(score_row and str(score_row.get("score_status") or "").upper() != "SCORE_UNAVAILABLE")
        certification_convergence, certification_state = _certification_convergence(row, score_row, promotion)
        suppression_reason = str(arbitration_row.get("rejection_reason") or row.get("suppression_reason") or "")
        if selected:
            suppression_reason = ""
        blocker = str(promotion.get("exact_blocker") or row.get("canonical_blocker") or "")
        if blocker in {"NON_CERTIFIED_CANDIDATE_SNAPSHOT"}:
            blocker = ""
        if certification_state in {"CERTIFICATION_PENDING", "PENDING", "PROVISIONAL_INTRADAY"} and blocker in {"CONVERSION_MISSING", "SUBMIT_BOUNDARY_REJECTED", "TICKET_LINEAGE_MISSING"}:
            blocker = ""
        data_score, data_breakdown = _data_completeness(row, score_row, promotion)
        stability, drift, drift_breakdown, prior_snapshot_id = _stability(
            root=root,
            day=day_utc,
            intent_id=intent_id,
            score_total=score_total,
            rank=rank,
            blocker=blocker,
            certification_state=certification_state,
        )
        analytical = 1.0 if str(row.get("status") or row.get("lifecycle_decision") or "").upper() in {"CANDIDATE_CREATED", "INTENT_CREATED", "ACTIVE_CURRENT", "CAPTURE_READY"} else 0.5
        sleeve_score = _score(score_total)
        portfolio_score = _score(score_total)
        liquidity = 0.80 if str(row.get("source_data_mode") or score_row.get("market_data_mode") or "").upper() in {"INTRADAY_OPERATIONAL", "PROVISIONAL_INTRADAY", "FINAL_EOD_CERTIFIED"} else 0.55
        volatility = 0.75
        blocker_component = 0.0 if blocker else 1.0
        certification_component = certification_convergence
        confidence_components = {
            "analytical_qualification": analytical,
            "sleeve_score": sleeve_score,
            "portfolio_score": portfolio_score,
            "liquidity": liquidity,
            "volatility": volatility,
            "data_completeness": data_score,
            "suppression_blocker_clear": blocker_component if not suppression_reason else 0.0,
            "certification_state": certification_component,
            "ranking_stability": stability,
            "market_calendar_state": 1.0,
        }
        confidence = round(
            (confidence_components["analytical_qualification"] * 0.10)
            + (confidence_components["sleeve_score"] * 0.15)
            + (confidence_components["portfolio_score"] * 0.16)
            + (confidence_components["liquidity"] * 0.08)
            + (confidence_components["volatility"] * 0.05)
            + (confidence_components["data_completeness"] * 0.14)
            + (confidence_components["suppression_blocker_clear"] * 0.10)
            + (confidence_components["certification_state"] * 0.12)
            + (confidence_components["ranking_stability"] * 0.08)
            + (confidence_components["market_calendar_state"] * 0.02),
            6,
        )
        guidance, guidance_reason, decision_code = _guidance(
            selected=selected,
            blocker=blocker,
            suppression_reason=suppression_reason,
            data_score=data_score,
            confidence=confidence,
            stability=stability,
            convergence=certification_convergence,
            certification_state=certification_state,
            observations=int(drift_breakdown.get("observation_count") or 1),
            policy=policy,
            now_utc=generated,
            day_utc=day_utc,
        )
        state = _intent_state(
            selected=selected,
            status=str(row.get("status") or row.get("lifecycle_decision") or "").upper(),
            score_available=score_available,
            blocker=blocker,
            suppressed=bool(suppression_reason),
            certification_state=certification_state,
            guidance=guidance,
        )
        base_snapshot = {
            "schema_id": "candidate_intent_snapshot",
            "schema_version": "v1",
            "intent_id": intent_id,
            "symbol": str(row.get("symbol") or row.get("symbol_or_pair") or score_row.get("symbol") or "").upper(),
            "sleeve": str(row.get("sleeve_id") or row.get("engine_id") or score_row.get("sleeve_id") or ""),
            "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or score_row.get("sleeve_id") or ""),
            "trading_day": day_utc,
            "created_at": str(row.get("created_at_utc") or manifest.get("produced_at_utc") or manifest.get("generated_at_utc") or generated),
            "updated_at": generated,
            "source_candidate_snapshot_ids": [str(row.get("candidate_id") or intent_id), str(manifest.get("manifest_id") or manifest.get("artifact_id") or _sha(manifest_path))],
            "input_market_data_snapshot_ids": [str(item) for item in market_snapshot_ids],
            "intent_state": state,
            "confidence_score": confidence,
            "stability_score": stability,
            "certification_convergence_score": certification_convergence,
            "data_completeness_score": data_score,
            "suppression_reason": suppression_reason,
            "blocker_reason": blocker,
            "capture_guidance": guidance,
            "capture_guidance_reason": guidance_reason,
            "certification_state": certification_state,
            "execution_eligibility_state": "BROKER_AUTOMATION_DISABLED",
            "recommendation_mode": "FINAL" if decision_code == "FINAL_RECOMMENDATION" else "PRELIMINARY" if decision_code == "PRELIMINARY_RECOMMENDATION" else "NONE",
            "selected": selected,
            "rank": rank,
            "source_score_total": score_total,
            "prior_intent_snapshot_id": prior_snapshot_id,
            "drift_score": drift,
            "scoring_component_breakdown": confidence_components,
            "data_completeness_breakdown": data_breakdown,
            "drift_breakdown": drift_breakdown,
            "thresholds_used": {
                "confidence_threshold": policy["confidence_threshold"],
                "stability_threshold": policy["stability_threshold"],
                "convergence_threshold": policy["convergence_threshold"],
                "minimum_snapshot_observations": policy["minimum_snapshot_observations"],
                "post_close_earliest_recommendation_time": policy["post_close_earliest_recommendation_time"],
                "preliminary_capture_enabled": policy["preliminary_capture_enabled"],
            },
            "recommendation_decision": decision_code,
            "lineage_metadata": {
                "candidate_generation_manifest_path": str(manifest_path),
                "portfolio_scoring_path": str(scoring_path),
                "intent_arbitration_path": str(arbitration_path),
                "candidate_promotion_map_path": str(promotion_path),
                "market_data_inputs_path": str(market_inputs_path),
                "source_hashes": {
                    "candidate_generation_manifest": _sha(manifest_path),
                    "portfolio_scoring": _sha(scoring_path),
                    "intent_arbitration": _sha(arbitration_path),
                    "candidate_promotion_map": _sha(promotion_path),
                    "market_data_inputs": _sha(market_inputs_path),
                },
            },
        }
        snapshot_hash = _content_hash(base_snapshot)
        intent_snapshots.append({**base_snapshot, "intent_snapshot_id": f"candidate_intent_snapshot_v1:{intent_id}:{snapshot_hash[:16]}", "content_hash": snapshot_hash})
    selected = [row for row in intent_snapshots if row.get("selected") is True]
    recommended = [row for row in intent_snapshots if row.get("capture_guidance") == "MANUAL_IB_CAPTURE_RECOMMENDED"]
    payload_base = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "trading_day": day_utc,
        "generated_at_utc": generated,
        "policy": policy,
        "intent_count": len(intent_snapshots),
        "selected_intent_count": len(selected),
        "manual_ib_capture_recommended_count": len(recommended),
        "certification_diverged_count": len([row for row in intent_snapshots if row.get("intent_state") == "CERTIFICATION_DIVERGED"]),
        "intent_snapshots": intent_snapshots,
        "source_artifacts": {
            "candidate_generation_manifest": str(manifest_path),
            "portfolio_scoring": str(scoring_path),
            "intent_arbitration": str(arbitration_path),
            "candidate_promotion_map": str(promotion_path),
            "market_data_inputs": str(market_inputs_path),
        },
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "order_routing_allowed": False,
        },
    }
    plane_hash = _content_hash(payload_base)
    payload = {**payload_base, "artifact_id": f"{REPORT_FAMILY}:{day_utc}:{plane_hash[:16]}", "content_hash": plane_hash}
    if write_histories:
        for snapshot in intent_snapshots:
            history_path = candidate_intent_history_path_v1(truth_root=root, day_utc=day_utc, intent_id=str(snapshot.get("intent_id") or ""))
            history = _read(history_path)
            existing = history.get("snapshots") if isinstance(history.get("snapshots"), list) else []
            if not any(str(row.get("content_hash") or "") == snapshot["content_hash"] for row in existing if isinstance(row, dict)):
                existing.append(snapshot)
            history_payload = {
                "schema_id": "candidate_intent_history",
                "schema_version": "v1",
                "intent_id": snapshot["intent_id"],
                "day_utc": day_utc,
                "updated_at_utc": generated,
                "snapshot_count": len(existing),
                "snapshots": existing,
            }
            history_path.parent.mkdir(parents=True, exist_ok=True)
            history_path.write_bytes(_canonical_bytes(history_payload) + b"\n")
    return payload


def write_candidate_intent_plane_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    path = candidate_intent_plane_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")
    return path


def build_and_write_candidate_intent_plane_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    now_utc: str | None = None,
    policy_overrides: dict[str, Any] | None = None,
    write_histories: bool = True,
) -> tuple[dict[str, Any], Path]:
    payload = build_candidate_intent_plane_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        now_utc=now_utc,
        policy_overrides=policy_overrides,
        write_histories=write_histories,
    )
    return payload, write_candidate_intent_plane_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)

