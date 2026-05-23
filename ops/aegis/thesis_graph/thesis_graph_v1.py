from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "v1"
THESIS_SCHEMA_ID = "aegis_thesis"
EVIDENCE_SCHEMA_ID = "aegis_thesis_evidence"
PROJECTION_SCHEMA_ID = "aegis_thesis_graph_projection"
TIMELINE_SCHEMA_ID = "aegis_thesis_timeline"

THESIS_STATES = {"ACTIVE", "STRENGTHENING", "WEAKENING", "WATCHING", "CONTRADICTED", "EXPIRED", "ARCHIVED"}
EVIDENCE_DIRECTIONS = {"SUPPORTS", "CONTRADICTS", "NEUTRAL"}
SAFETY_FLAGS = {
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "order_routing_allowed": False,
    "trade_advice_allowed": False,
    "execution_firewall_bypass_allowed": False,
}
DEFAULT_THRESHOLDS = {
    "strengthen_delta": 0.02,
    "weaken_delta": -0.02,
    "contradiction_score_threshold": 0.25,
    "high_confidence": 0.72,
    "low_confidence": 0.38,
}


def now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_json_bytes_v1(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def content_hash_v1(payload: dict[str, Any]) -> str:
    clean = {k: v for k, v in payload.items() if k != "content_hash"}
    return hashlib.sha256(canonical_json_bytes_v1(clean)).hexdigest()


def _slug(text: str) -> str:
    chars = []
    for ch in str(text or "").lower():
        chars.append(ch if ch.isalnum() else "-")
    compact = "-".join(part for part in "".join(chars).split("-") if part)
    return compact[:80] or "thesis"


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _unique_text(values: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in _as_list(values):
        text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def thesis_id_from_title_v1(title: str) -> str:
    return f"ths-{_slug(title)}-v1"


def investigation_id_v1(investigation: dict[str, Any]) -> str:
    return str(
        investigation.get("investigation_id")
        or investigation.get("research_hypothesis_id")
        or investigation.get("hypothesis_id")
        or investigation.get("id")
        or ""
    ).strip()


def build_thesis_artifact_v1(
    *,
    title: str,
    thesis_type: str = "MARKET_THESIS",
    regime: str = "UNKNOWN",
    created_at: str | None = None,
    updated_at: str | None = None,
    thesis_id: str | None = None,
    confidence_score: float = 0.5,
    conviction_trend: str = "WATCHING",
    evidence_score: float = 0.0,
    contradiction_score: float = 0.0,
    related_symbols: list[str] | None = None,
    related_sleeves: list[str] | None = None,
    linked_investigation_ids: list[str] | None = None,
    linked_intent_ids: list[str] | None = None,
    linked_candidate_snapshot_ids: list[str] | None = None,
    linked_market_data_snapshot_ids: list[str] | None = None,
    linked_capture_ids: list[str] | None = None,
    linked_outcome_ids: list[str] | None = None,
    supporting_evidence_ids: list[str] | None = None,
    contradicting_evidence_ids: list[str] | None = None,
    thesis_state: str = "ACTIVE",
    lineage_metadata: dict[str, Any] | None = None,
    explanation: str = "Initial thesis artifact.",
) -> dict[str, Any]:
    ts = now_iso_v1()
    state = str(thesis_state or "ACTIVE").upper()
    if state not in THESIS_STATES:
        state = "ACTIVE"
    payload = {
        "schema_id": THESIS_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "thesis_id": thesis_id or thesis_id_from_title_v1(title),
        "title": str(title or "Untitled Thesis"),
        "thesis_type": str(thesis_type or "MARKET_THESIS"),
        "regime": str(regime or "UNKNOWN"),
        "created_at": created_at or ts,
        "updated_at": updated_at or ts,
        "confidence_score": round(_clamp(float(confidence_score)), 4),
        "conviction_trend": str(conviction_trend or "WATCHING").upper(),
        "evidence_score": round(max(0.0, float(evidence_score)), 4),
        "contradiction_score": round(max(0.0, float(contradiction_score)), 4),
        "related_symbols": _unique_text(related_symbols),
        "related_sleeves": _unique_text(related_sleeves),
        "linked_investigation_ids": _unique_text(linked_investigation_ids),
        "linked_intent_ids": _unique_text(linked_intent_ids),
        "linked_candidate_snapshot_ids": _unique_text(linked_candidate_snapshot_ids),
        "linked_market_data_snapshot_ids": _unique_text(linked_market_data_snapshot_ids),
        "linked_capture_ids": _unique_text(linked_capture_ids),
        "linked_outcome_ids": _unique_text(linked_outcome_ids),
        "supporting_evidence_ids": _unique_text(supporting_evidence_ids),
        "contradicting_evidence_ids": _unique_text(contradicting_evidence_ids),
        "thesis_state": state,
        "lineage_metadata": lineage_metadata or {"producer": "aegis_thesis_graph_v1", "immutable": True},
        "explanation": str(explanation or ""),
        "safety": dict(SAFETY_FLAGS),
    }
    payload["content_hash"] = content_hash_v1(payload)
    return payload


def create_thesis_from_investigation_v1(investigation: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    inv_id = investigation_id_v1(investigation)
    title = str(investigation.get("title") or investigation.get("hypothesis_title") or investigation.get("name") or inv_id or "Market Thesis")
    return build_thesis_artifact_v1(
        title=title,
        thesis_type=str(investigation.get("thesis_type") or investigation.get("hypothesis_type") or "INVESTIGATION_DERIVED"),
        regime=str(investigation.get("regime") or "UNKNOWN"),
        created_at=generated_at,
        updated_at=generated_at,
        confidence_score=_float(investigation.get("confidence_score"), 0.5),
        related_symbols=_unique_text(investigation.get("symbols") or investigation.get("related_symbols")),
        related_sleeves=_unique_text(investigation.get("sleeves") or investigation.get("related_sleeves")),
        linked_investigation_ids=[inv_id] if inv_id else [],
        linked_market_data_snapshot_ids=_unique_text(investigation.get("input_market_data_snapshot_ids")),
        explanation="Created from governed investigation workflow; thesis persists after investigation completion.",
        lineage_metadata={
            "producer": "aegis_thesis_graph_v1",
            "source": "investigation",
            "source_investigation_id": inv_id,
            "immutable": True,
        },
    )


def build_evidence_item_v1(
    *,
    thesis_id: str,
    source: str,
    summary: str,
    evidence_direction: str = "NEUTRAL",
    confidence_impact: float = 0.0,
    evidence_type: str = "market data signal",
    timestamp: str | None = None,
    linked_symbols: list[str] | None = None,
    linked_sleeves: list[str] | None = None,
    linked_intents: list[str] | None = None,
    linked_candidate_snapshot_ids: list[str] | None = None,
    linked_market_data_snapshot_ids: list[str] | None = None,
    linked_capture_ids: list[str] | None = None,
    linked_outcome_ids: list[str] | None = None,
    provenance: dict[str, Any] | None = None,
    evidence_id: str | None = None,
) -> dict[str, Any]:
    direction = str(evidence_direction or "NEUTRAL").upper()
    if direction not in EVIDENCE_DIRECTIONS:
        direction = "NEUTRAL"
    ts = timestamp or now_iso_v1()
    seed = {"thesis_id": thesis_id, "source": source, "summary": summary, "timestamp": ts, "direction": direction}
    eid = evidence_id or f"evd-{hashlib.sha256(canonical_json_bytes_v1(seed)).hexdigest()[:20]}"
    payload = {
        "schema_id": EVIDENCE_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "evidence_id": eid,
        "thesis_id": str(thesis_id),
        "evidence_type": str(evidence_type or "market data signal"),
        "source": str(source or "UNKNOWN"),
        "timestamp": ts,
        "summary": str(summary or ""),
        "evidence_direction": direction,
        "confidence_impact": round(abs(_float(confidence_impact, 0.0)), 4),
        "linked_symbols": _unique_text(linked_symbols),
        "linked_sleeves": _unique_text(linked_sleeves),
        "linked_intents": _unique_text(linked_intents),
        "linked_candidate_snapshot_ids": _unique_text(linked_candidate_snapshot_ids),
        "linked_market_data_snapshot_ids": _unique_text(linked_market_data_snapshot_ids),
        "linked_capture_ids": _unique_text(linked_capture_ids),
        "linked_outcome_ids": _unique_text(linked_outcome_ids),
        "provenance": provenance or {"producer": "aegis_thesis_graph_v1", "source": source},
        "safety": dict(SAFETY_FLAGS),
    }
    payload["content_hash"] = content_hash_v1(payload)
    return payload


def _trend_state(prior_confidence: float, new_confidence: float, evidence_score: float, contradiction_score: float, thresholds: dict[str, Any]) -> tuple[str, str]:
    delta = new_confidence - prior_confidence
    if contradiction_score >= max(evidence_score, _float(thresholds.get("contradiction_score_threshold"), 0.25)):
        return "WEAKENING", "CONTRADICTED"
    if delta >= _float(thresholds.get("strengthen_delta"), 0.02):
        return "STRENGTHENING", "STRENGTHENING"
    if delta <= _float(thresholds.get("weaken_delta"), -0.02):
        return "WEAKENING", "WEAKENING"
    return "WATCHING", "WATCHING"


def update_thesis_with_evidence_v1(
    prior_thesis: dict[str, Any],
    evidence_items: list[dict[str, Any]],
    *,
    thresholds: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    prior_confidence = _float(prior_thesis.get("confidence_score"), 0.5)
    support_delta = 0.0
    contradiction_delta = 0.0
    neutral_count = 0
    supporting_ids = _unique_text(prior_thesis.get("supporting_evidence_ids"))
    contradicting_ids = _unique_text(prior_thesis.get("contradicting_evidence_ids"))
    related_symbols = _unique_text(prior_thesis.get("related_symbols"))
    related_sleeves = _unique_text(prior_thesis.get("related_sleeves"))
    linked_intent_ids = _unique_text(prior_thesis.get("linked_intent_ids"))
    linked_candidate_snapshot_ids = _unique_text(prior_thesis.get("linked_candidate_snapshot_ids"))
    linked_market_data_snapshot_ids = _unique_text(prior_thesis.get("linked_market_data_snapshot_ids"))
    linked_capture_ids = _unique_text(prior_thesis.get("linked_capture_ids"))
    linked_outcome_ids = _unique_text(prior_thesis.get("linked_outcome_ids"))

    for evidence in evidence_items:
        direction = str(evidence.get("evidence_direction") or "NEUTRAL").upper()
        impact = abs(_float(evidence.get("confidence_impact"), 0.0))
        evidence_id = str(evidence.get("evidence_id") or "").strip()
        if direction == "SUPPORTS":
            support_delta += impact
            supporting_ids = _unique_text([*supporting_ids, evidence_id])
        elif direction == "CONTRADICTS":
            contradiction_delta += impact
            contradicting_ids = _unique_text([*contradicting_ids, evidence_id])
        else:
            neutral_count += 1
        related_symbols = _unique_text([*related_symbols, *evidence.get("linked_symbols", [])])
        related_sleeves = _unique_text([*related_sleeves, *evidence.get("linked_sleeves", [])])
        linked_intent_ids = _unique_text([*linked_intent_ids, *evidence.get("linked_intents", [])])
        linked_candidate_snapshot_ids = _unique_text([*linked_candidate_snapshot_ids, *evidence.get("linked_candidate_snapshot_ids", [])])
        linked_market_data_snapshot_ids = _unique_text([*linked_market_data_snapshot_ids, *evidence.get("linked_market_data_snapshot_ids", [])])
        linked_capture_ids = _unique_text([*linked_capture_ids, *evidence.get("linked_capture_ids", [])])
        linked_outcome_ids = _unique_text([*linked_outcome_ids, *evidence.get("linked_outcome_ids", [])])

    evidence_score = _float(prior_thesis.get("evidence_score"), 0.0) + support_delta
    contradiction_score = _float(prior_thesis.get("contradiction_score"), 0.0) + contradiction_delta
    new_confidence = _clamp(prior_confidence + support_delta - contradiction_delta)
    trend, state = _trend_state(prior_confidence, new_confidence, evidence_score, contradiction_score, thresholds)
    explanation = (
        f"Confidence moved from {prior_confidence:.2f} to {new_confidence:.2f}: "
        f"support +{support_delta:.2f}, contradiction -{contradiction_delta:.2f}, neutral evidence {neutral_count}."
    )
    return build_thesis_artifact_v1(
        title=str(prior_thesis.get("title") or "Untitled Thesis"),
        thesis_type=str(prior_thesis.get("thesis_type") or "MARKET_THESIS"),
        regime=str(prior_thesis.get("regime") or "UNKNOWN"),
        created_at=str(prior_thesis.get("created_at") or generated_at or now_iso_v1()),
        updated_at=generated_at or now_iso_v1(),
        thesis_id=str(prior_thesis.get("thesis_id") or thesis_id_from_title_v1(str(prior_thesis.get("title") or "Untitled Thesis"))),
        confidence_score=new_confidence,
        conviction_trend=trend,
        evidence_score=evidence_score,
        contradiction_score=contradiction_score,
        related_symbols=related_symbols,
        related_sleeves=related_sleeves,
        linked_investigation_ids=_unique_text(prior_thesis.get("linked_investigation_ids")),
        linked_intent_ids=linked_intent_ids,
        linked_candidate_snapshot_ids=linked_candidate_snapshot_ids,
        linked_market_data_snapshot_ids=linked_market_data_snapshot_ids,
        linked_capture_ids=linked_capture_ids,
        linked_outcome_ids=linked_outcome_ids,
        supporting_evidence_ids=supporting_ids,
        contradicting_evidence_ids=contradicting_ids,
        thesis_state=state,
        explanation=explanation,
        lineage_metadata={
            "producer": "aegis_thesis_graph_v1",
            "prior_thesis_id": prior_thesis.get("thesis_id"),
            "prior_content_hash": prior_thesis.get("content_hash"),
            "evidence_ids": [e.get("evidence_id") for e in evidence_items if isinstance(e, dict)],
            "thresholds": thresholds,
            "immutable": True,
        },
    )


def attach_investigation_to_thesis_v1(thesis: dict[str, Any], investigation: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    inv_id = investigation_id_v1(investigation)
    updated = dict(thesis)
    updated["linked_investigation_ids"] = _unique_text([*updated.get("linked_investigation_ids", []), inv_id]) if inv_id else _unique_text(updated.get("linked_investigation_ids"))
    updated["related_symbols"] = _unique_text([*updated.get("related_symbols", []), *(_as_list(investigation.get("symbols") or investigation.get("related_symbols")))])
    updated["related_sleeves"] = _unique_text([*updated.get("related_sleeves", []), *(_as_list(investigation.get("sleeves") or investigation.get("related_sleeves")))])
    updated["updated_at"] = generated_at or now_iso_v1()
    updated["lineage_metadata"] = {
        "producer": "aegis_thesis_graph_v1",
        "prior_content_hash": thesis.get("content_hash"),
        "attached_investigation_id": inv_id,
        "immutable": True,
    }
    updated["explanation"] = f"Attached investigation {inv_id} to persistent thesis."
    updated["content_hash"] = content_hash_v1(updated)
    return updated


def link_intent_to_thesis_v1(intent: dict[str, Any], thesis: dict[str, Any], *, secondary_theses: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    secondaries = secondary_theses or []
    enriched = dict(intent)
    enriched.update(
        {
            "primary_thesis_id": thesis.get("thesis_id"),
            "secondary_thesis_ids": [row.get("thesis_id") for row in secondaries if isinstance(row, dict) and row.get("thesis_id")],
            "thesis_confidence_at_creation": thesis.get("confidence_score"),
            "thesis_confidence_at_recommendation": thesis.get("confidence_score"),
            "thesis_evidence_summary": {
                "evidence_score": thesis.get("evidence_score"),
                "supporting_evidence_ids": thesis.get("supporting_evidence_ids", []),
                "recent_explanation": thesis.get("explanation", ""),
            },
            "thesis_contradiction_summary": {
                "contradiction_score": thesis.get("contradiction_score"),
                "contradicting_evidence_ids": thesis.get("contradicting_evidence_ids", []),
            },
            "thesis_linkage_read_only": True,
            "safety": dict(SAFETY_FLAGS),
        }
    )
    return enriched


def thesis_artifact_path_v1(*, truth_root: Path | str, thesis: dict[str, Any], day_utc: str) -> Path:
    root = Path(truth_root).expanduser().resolve()
    thesis_id = str(thesis.get("thesis_id") or "unknown-thesis")
    hash_id = str(thesis.get("content_hash") or content_hash_v1(thesis))
    return root / "reports" / "aegis_thesis_v1" / day_utc / thesis_id / hash_id / "thesis.v1.json"


def evidence_artifact_path_v1(*, truth_root: Path | str, evidence: dict[str, Any], day_utc: str) -> Path:
    root = Path(truth_root).expanduser().resolve()
    thesis_id = str(evidence.get("thesis_id") or "unknown-thesis")
    evidence_id = str(evidence.get("evidence_id") or "unknown-evidence")
    return root / "reports" / "aegis_thesis_evidence_v1" / day_utc / thesis_id / evidence_id / "thesis_evidence.v1.json"


def projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_thesis_graph_projection_v1" / day_utc / "thesis_graph_projection.v1.json"


def timeline_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_thesis_timeline_v1" / day_utc / "thesis_timeline.v1.json"


def _write_json(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload))
    return {"json": str(path), "hash": hashlib.sha256(path.read_bytes()).hexdigest()}


def write_thesis_artifact_v1(*, truth_root: Path | str, thesis: dict[str, Any], day_utc: str) -> dict[str, Any]:
    path = thesis_artifact_path_v1(truth_root=truth_root, thesis=thesis, day_utc=day_utc)
    return {**_write_json(path, thesis), "thesis_id": thesis.get("thesis_id"), "content_hash": thesis.get("content_hash")}


def write_evidence_artifact_v1(*, truth_root: Path | str, evidence: dict[str, Any], day_utc: str) -> dict[str, Any]:
    path = evidence_artifact_path_v1(truth_root=truth_root, evidence=evidence, day_utc=day_utc)
    return {**_write_json(path, evidence), "evidence_id": evidence.get("evidence_id"), "content_hash": evidence.get("content_hash")}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def latest_thesis_artifacts_v1(*, truth_root: Path | str, day_utc: str | None = None) -> list[dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    base = root / "reports" / "aegis_thesis_v1"
    if not base.exists():
        return []
    paths = sorted(base.glob("*/*/*/thesis.v1.json"), key=lambda p: (str(p.parts[-4]), p.stat().st_mtime_ns, str(p)))
    if day_utc:
        paths = [p for p in paths if len(p.parts) >= 4 and p.parts[-4] <= day_utc]
    latest: dict[str, dict[str, Any]] = {}
    for path in paths:
        payload = _read_json(path)
        thesis_id = str(payload.get("thesis_id") or "")
        if not thesis_id:
            continue
        payload["artifact_path"] = str(path)
        payload["artifact_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        latest[thesis_id] = payload
    return sorted(latest.values(), key=lambda row: str(row.get("updated_at") or ""), reverse=True)


def evidence_artifacts_v1(*, truth_root: Path | str, thesis_id: str | None = None, day_utc: str | None = None) -> list[dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    base = root / "reports" / "aegis_thesis_evidence_v1"
    if not base.exists():
        return []
    paths = sorted(base.glob("*/*/*/thesis_evidence.v1.json"), key=lambda p: (str(p.parts[-4]), p.stat().st_mtime_ns, str(p)))
    out: list[dict[str, Any]] = []
    for path in paths:
        if day_utc and len(path.parts) >= 4 and path.parts[-4] > day_utc:
            continue
        payload = _read_json(path)
        if thesis_id and str(payload.get("thesis_id") or "") != thesis_id:
            continue
        payload["artifact_path"] = str(path)
        payload["artifact_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        if payload.get("evidence_id"):
            out.append(payload)
    return out


def _thesis_summary(thesis: dict[str, Any], evidence: list[dict[str, Any]]) -> dict[str, Any]:
    recent = sorted(evidence, key=lambda row: str(row.get("timestamp") or ""), reverse=True)[:3]
    contradictions = [row for row in evidence if str(row.get("evidence_direction") or "").upper() == "CONTRADICTS"]
    return {
        "thesis_id": thesis.get("thesis_id"),
        "title": thesis.get("title"),
        "state": thesis.get("thesis_state"),
        "confidence_score": thesis.get("confidence_score"),
        "confidence_label": "High" if _float(thesis.get("confidence_score")) >= DEFAULT_THRESHOLDS["high_confidence"] else "Low" if _float(thesis.get("confidence_score")) <= DEFAULT_THRESHOLDS["low_confidence"] else "Medium",
        "conviction_trend": thesis.get("conviction_trend"),
        "regime": thesis.get("regime"),
        "related_symbols": thesis.get("related_symbols", []),
        "related_sleeves": thesis.get("related_sleeves", []),
        "active_investigations": len(thesis.get("linked_investigation_ids", [])),
        "impacted_intents": len(thesis.get("linked_intent_ids", [])),
        "impacted_candidates": len(thesis.get("linked_candidate_snapshot_ids", [])),
        "capture_recommendations_produced": len(thesis.get("linked_capture_ids", [])),
        "recent_evidence": [{"evidence_id": row.get("evidence_id"), "summary": row.get("summary"), "direction": row.get("evidence_direction")} for row in recent],
        "blockers_or_contradictions": [row.get("summary") for row in contradictions[:3]],
        "last_updated": thesis.get("updated_at"),
        "artifact_path": thesis.get("artifact_path", ""),
    }


def build_thesis_timeline_v1(*, theses: list[dict[str, Any]], evidence: list[dict[str, Any]], generated_at: str | None = None) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    for thesis in theses:
        thesis_id = str(thesis.get("thesis_id") or "")
        events.append({"event_type": "THESIS_REFRESH", "timestamp": thesis.get("updated_at"), "thesis_id": thesis_id, "summary": f"Refreshed thesis {thesis.get('title')}."})
        events.append({"event_type": "THESIS_CONFIDENCE_RECALCULATION", "timestamp": thesis.get("updated_at"), "thesis_id": thesis_id, "summary": thesis.get("explanation", "Confidence recalculated.")})
        if thesis.get("linked_investigation_ids"):
            events.append({"event_type": "INVESTIGATION_TO_THESIS_UPDATE", "timestamp": thesis.get("updated_at"), "thesis_id": thesis_id, "summary": f"{len(thesis.get('linked_investigation_ids', []))} investigations linked."})
        if thesis.get("linked_outcome_ids") or thesis.get("linked_capture_ids"):
            events.append({"event_type": "OUTCOME_FEEDBACK_UPDATE", "timestamp": thesis.get("updated_at"), "thesis_id": thesis_id, "summary": "Outcome or capture feedback linked to thesis."})
    counts = Counter(str(row.get("thesis_id") or "") for row in evidence)
    for thesis_id, count in counts.items():
        events.append({"event_type": "EVIDENCE_AGGREGATION", "timestamp": generated_at or now_iso_v1(), "thesis_id": thesis_id, "summary": f"Aggregated {count} evidence items."})
    events = sorted(events, key=lambda row: str(row.get("timestamp") or ""), reverse=True)
    payload = {"schema_id": TIMELINE_SCHEMA_ID, "schema_version": SCHEMA_VERSION, "generated_at": generated_at or now_iso_v1(), "events": events, "event_count": len(events), "safety": dict(SAFETY_FLAGS)}
    payload["content_hash"] = content_hash_v1(payload)
    return payload


def build_thesis_graph_projection_v1(*, truth_root: Path | str, day_utc: str, generated_at: str | None = None) -> dict[str, Any]:
    generated = generated_at or now_iso_v1()
    theses = latest_thesis_artifacts_v1(truth_root=truth_root, day_utc=day_utc)
    all_evidence = evidence_artifacts_v1(truth_root=truth_root, day_utc=day_utc)
    evidence_by_thesis: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for evidence in all_evidence:
        evidence_by_thesis[str(evidence.get("thesis_id") or "")].append(evidence)
    summaries = [_thesis_summary(thesis, evidence_by_thesis.get(str(thesis.get("thesis_id") or ""), [])) for thesis in theses]
    counts_by_state = Counter(str(thesis.get("thesis_state") or "UNKNOWN") for thesis in theses)
    mission = {
        "strengthening_theses": [row for row in summaries if row.get("conviction_trend") == "STRENGTHENING" or row.get("state") == "STRENGTHENING"],
        "weakening_theses": [row for row in summaries if row.get("conviction_trend") == "WEAKENING" or row.get("state") in {"WEAKENING", "CONTRADICTED"}],
        "contradicted_theses": [row for row in summaries if row.get("state") == "CONTRADICTED"],
        "theses_with_active_investigations": [row for row in summaries if int(row.get("active_investigations") or 0) > 0],
        "theses_producing_capture_recommendations": [row for row in summaries if int(row.get("capture_recommendations_produced") or 0) > 0],
        "theses_blocked_by_missing_data_or_governance": [row for row in summaries if row.get("blockers_or_contradictions")],
    }
    timeline = build_thesis_timeline_v1(theses=theses, evidence=all_evidence, generated_at=generated)
    payload = {
        "schema_id": PROJECTION_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated,
        "day_utc": day_utc,
        "thesis_count": len(theses),
        "evidence_count": len(all_evidence),
        "theses": theses,
        "thesis_cards": summaries,
        "counts_by_state": dict(counts_by_state),
        "mission_control_thesis_summary": mission,
        "runtime_timeline_events": timeline["events"],
        "timeline": timeline,
        "read_only": True,
        "safety": dict(SAFETY_FLAGS),
    }
    payload["content_hash"] = content_hash_v1(payload)
    return payload


def write_thesis_graph_projection_v1(*, truth_root: Path | str, projection: dict[str, Any], day_utc: str) -> dict[str, Any]:
    projection_result = _write_json(projection_path_v1(truth_root=truth_root, day_utc=day_utc), projection)
    timeline = projection.get("timeline") if isinstance(projection.get("timeline"), dict) else build_thesis_timeline_v1(theses=[], evidence=[])
    timeline_result = _write_json(timeline_path_v1(truth_root=truth_root, day_utc=day_utc), timeline)
    return {"projection": projection_result, "timeline": timeline_result}


def load_or_build_thesis_graph_response_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    path = projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    projection = _read_json(path) if path.exists() else {}
    if not projection:
        projection = build_thesis_graph_projection_v1(truth_root=truth_root, day_utc=day_utc)
        write_thesis_graph_projection_v1(truth_root=truth_root, projection=projection, day_utc=day_utc)
    return {"ok": True, "degraded": False, "errors": [], "data": projection, "read_only": True, "safety": dict(SAFETY_FLAGS)}


def replay_thesis_from_artifacts_v1(*, prior_thesis: dict[str, Any], evidence_items: list[dict[str, Any]], thresholds: dict[str, Any] | None = None, generated_at: str | None = None) -> dict[str, Any]:
    replayed = update_thesis_with_evidence_v1(prior_thesis, evidence_items, thresholds=thresholds, generated_at=generated_at)
    return {"replay_status": "PASS", "replayed_thesis": replayed, "input_prior_content_hash": prior_thesis.get("content_hash"), "input_evidence_ids": [row.get("evidence_id") for row in evidence_items], "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})}}


def apply_capture_outcome_to_thesis_v1(thesis: dict[str, Any], outcome: dict[str, Any], *, generated_at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    direction = "SUPPORTS" if str(outcome.get("outcome_status") or outcome.get("status") or "").upper() in {"WIN", "POSITIVE", "THESIS_CONFIRMED", "CAPTURE_COMPLETED"} else "CONTRADICTS" if str(outcome.get("outcome_status") or outcome.get("status") or "").upper() in {"LOSS", "NEGATIVE", "THESIS_INVALIDATED"} else "NEUTRAL"
    impact = _float(outcome.get("confidence_impact"), 0.03 if direction != "NEUTRAL" else 0.0)
    evidence = build_evidence_item_v1(
        thesis_id=str(thesis.get("thesis_id") or ""),
        source=str(outcome.get("source") or "capture_outcome"),
        summary=str(outcome.get("summary") or "Capture/outcome feedback recorded."),
        evidence_direction=direction,
        confidence_impact=impact,
        evidence_type="capture outcome",
        timestamp=generated_at,
        linked_capture_ids=_unique_text(outcome.get("capture_id") or outcome.get("linked_capture_ids")),
        linked_outcome_ids=_unique_text(outcome.get("outcome_id") or outcome.get("linked_outcome_ids")),
        provenance={"producer": "aegis_thesis_graph_v1", "source_outcome": outcome},
    )
    updated = update_thesis_with_evidence_v1(thesis, [evidence], generated_at=generated_at)
    return updated, evidence
