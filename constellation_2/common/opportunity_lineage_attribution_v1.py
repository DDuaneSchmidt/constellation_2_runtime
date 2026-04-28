from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.common.runtime_authority_bridge_v1 import (
    resolve_canonical_truth_root_bridge_v1,
    resolve_truth_sleeves_root_bridge_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import (
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]

LINEAGE_EVENT_ARTIFACT_ID = "opportunity_lineage_event_v1"
ATTRIBUTION_ARTIFACT_ID = "sleeve_intent_trade_attribution_v1"
SUBMIT_DECISION_TRACE_ARTIFACT_ID = "submit_decision_trace_v1"
LINEAGE_EVENT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/opportunity_lineage_event.v1.schema.json"
)
ATTRIBUTION_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_intent_trade_attribution.v1.schema.json"
)
SUBMIT_DECISION_TRACE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_decision_trace.v1.schema.json"
)
LINEAGE_EVENT_FAMILY = "opportunity_lineage_event_v1"
ATTRIBUTION_FAMILY = "sleeve_intent_trade_attribution_v1"
SUBMIT_DECISION_TRACE_FAMILY = "submit_decision_trace_v1"

LINEAGE_STAGES = (
    "sleeve_run",
    "signal_eval",
    "signal_filtered",
    "intent_created",
    "submit_attempted",
    "order_ack",
    "fill_event",
    "order_rejected",
    "order_cancelled",
    "attribution_complete",
)

FINAL_CLASSIFICATION_PRECEDENCE = (
    "FILLED",
    "PARTIAL_FILL",
    "ORDER_CANCELLED",
    "ORDER_REJECTED",
    "ORDER_ACCEPTED_NO_FILL",
    "INTENT_SUBMITTED_NO_ORDER",
    "INTENT_CREATED_NOT_SUBMITTED",
    "RAN_SIGNAL_FILTERED",
    "RAN_NO_SIGNAL",
    "SLEEVE_NOT_RUN",
    "ATTRIBUTION_INCOMPLETE",
)

GENERIC_SKIP_REASON_CODES = {
    "",
    "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
    "SUBMISSION_BOUNDARY_DENIED",
    "MULTIPLE_SKIP_REASONS",
    "NO_SUBMIT_DECISION_EVIDENCE",
}
HEADROOM_CONSTRAINT_KEYS = (
    "HEADROOM_REQUESTED_QUANTITY",
    "HEADROOM_AUTHORIZED_QUANTITY",
    "HEADROOM_REJECTED_QUANTITY",
    "HEADROOM_RISK_PER_UNIT_CENTS",
    "HEADROOM_REQUIRED_RISK_CENTS",
    "HEADROOM_AVAILABLE_CENTS",
    "HEADROOM_ALLOWED_CAPITAL_AT_RISK_CENTS",
    "HEADROOM_AVAILABLE_SLEEVE_CENTS",
    "HEADROOM_AVAILABLE_PORTFOLIO_CENTS",
)

COMPLETENESS_ORDER = ("COMPLETE", "PARTIAL", "LATE_UPSTREAM", "CONTRADICTORY")
ORDER_STATUS_ACK = {
    "SUBMITTED",
    "PRESUBMITTED",
    "ACKNOWLEDGED",
    "PARTIALLY_FILLED",
    "FILLED",
    "API_PENDING",
}
ORDER_STATUS_REJECT = {"REJECTED", "INACTIVE"}
ORDER_STATUS_CANCEL = {"CANCELLED"}
DRY_RUN_NO_BROKER_ID_ERROR_CODE = "DRY_RUN_NO_BROKER_ID"
SUBMIT_DECISION_DRY_RUN_REASON_CODE = "SUBMITTED_DRY_RUN_NO_BROKER_ID"


@dataclass(frozen=True)
class MaterializedLineageResultV1:
    day_utc: str
    environment: str
    event_count: int
    event_paths: tuple[str, ...]
    warnings: tuple[str, ...]
    evidence_cutoff_utc: str


@dataclass(frozen=True)
class AttributionWriteResultV1:
    day_utc: str
    environment: str
    report_path: str
    versioned_report_path: str
    report_sha256: str
    event_count: int
    opportunity_count: int
    completeness_status: str
    classification_counts: dict[str, int]


@dataclass(frozen=True)
class SubmitDecisionTraceWriteResultV1:
    day_utc: str
    environment: str
    trace_count: int
    trace_paths: tuple[str, ...]
    warnings: tuple[str, ...]
    evidence_cutoff_utc: str


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _as_iso_utc(value: str, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized).astimezone(UTC)
    except ValueError:
        return fallback
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_utc_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day_start_iso(day_utc: str) -> str:
    return f"{day_utc}T00:00:00Z"


def _safe_reason_code(raw_codes: Sequence[str], default: str) -> str:
    for item in raw_codes:
        code = str(item or "").strip().upper()
        if code:
            return code
    return default


def _is_filter_reason(code: str) -> bool:
    text = str(code or "").strip().upper()
    return ("FILTER" in text) or text.startswith("RISK_") or text.startswith("SUBMISSION_BOUNDARY")


def _event_evidence_ref(path: Path, artifact_type: str) -> dict[str, str]:
    return {
        "artifact_type": str(artifact_type).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": _sha256_file(path),
    }


def _event_upstream_ref(path: Path, artifact_type: str) -> dict[str, str]:
    return {
        "artifact_type": str(artifact_type).strip(),
        "artifact_path": str(path.resolve()),
    }


def _event_id_seed(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "day_utc": str(payload.get("day_utc") or "").strip(),
        "environment": str(payload.get("environment") or "").strip(),
        "sleeve_id": str(payload.get("sleeve_id") or "").strip(),
        "opportunity_id": str(payload.get("opportunity_id") or "").strip(),
        "lineage_stage": str(payload.get("lineage_stage") or "").strip(),
        "stage_status": str(payload.get("stage_status") or "").strip(),
        "observed_at_utc": str(payload.get("observed_at_utc") or "").strip(),
        "reason_code": str(payload.get("reason_code") or "").strip(),
        "reason_detail": str(payload.get("reason_detail") or "").strip(),
        "stage_metrics": dict(payload.get("stage_metrics") or {}),
        "evidence_refs": list(payload.get("evidence_refs") or []),
        "upstream_refs": list(payload.get("upstream_refs") or []),
        "emitted_by": str(payload.get("emitted_by") or "").strip(),
    }


def _build_lineage_event_payload(
    *,
    day_utc: str,
    environment: str,
    sleeve_id: str,
    opportunity_id: str,
    lineage_stage: str,
    stage_status: str,
    observed_at_utc: str,
    reason_code: str,
    reason_detail: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    upstream_refs: Sequence[Mapping[str, Any]],
    emitted_by: str,
    stage_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if lineage_stage not in LINEAGE_STAGES:
        raise RuntimeError(f"LINEAGE_STAGE_UNSUPPORTED:{lineage_stage}")
    payload = {
        "schema_id": "opportunity_lineage_event",
        "schema_version": "v1",
        "artifact_id": LINEAGE_EVENT_ARTIFACT_ID,
        "day_utc": str(day_utc).strip(),
        "environment": str(environment).strip().upper() or "PAPER",
        "sleeve_id": str(sleeve_id).strip(),
        "opportunity_id": str(opportunity_id).strip(),
        "lineage_stage": lineage_stage,
        "stage_status": str(stage_status).strip().upper(),
        "observed_at_utc": str(observed_at_utc).strip(),
        "reason_code": str(reason_code).strip().upper(),
        "reason_detail": str(reason_detail).strip(),
        "stage_metrics": dict(stage_metrics or {}),
        "evidence_refs": [dict(item) for item in evidence_refs],
        "upstream_refs": [dict(item) for item in upstream_refs],
        "emitted_by": str(emitted_by).strip(),
    }
    payload["event_id"] = canonical_hash_for_c2_artifact_v1(_event_id_seed(payload))
    validate_against_repo_schema_v1(payload, REPO_ROOT, LINEAGE_EVENT_SCHEMA_RELPATH)
    return payload


def _opportunity_path_token(opportunity_id: str) -> str:
    return hashlib.sha256(str(opportunity_id).encode("utf-8")).hexdigest()


def _write_lineage_event_immutable(*, truth_root: Path, payload: Mapping[str, Any]) -> Path:
    day_utc = str(payload.get("day_utc") or "").strip()
    sleeve_id = str(payload.get("sleeve_id") or "").strip()
    opportunity_id = str(payload.get("opportunity_id") or "").strip()
    lineage_stage = str(payload.get("lineage_stage") or "").strip()
    event_id = str(payload.get("event_id") or "").strip()
    out_path = (
        truth_root
        / "reports"
        / LINEAGE_EVENT_FAMILY
        / day_utc
        / sleeve_id
        / _opportunity_path_token(opportunity_id)
        / lineage_stage
        / event_id
        / "opportunity_lineage_event.v1.json"
    ).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload_bytes = canonical_json_bytes_v1(dict(payload)) + b"\n"
    if out_path.exists():
        existing = out_path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload_bytes):
            return out_path
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES:path={out_path}")
    with NamedTemporaryFile(prefix=f".{out_path.name}.tmp.", dir=str(out_path.parent), delete=False) as tmp:
        tmp.write(payload_bytes)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(out_path)
    return out_path


def _load_intent_generation_report(*, truth_root: Path, day_utc: str) -> tuple[dict[str, Any] | None, Path]:
    report_path = (
        truth_root
        / "reports"
        / "trading_day_intent_generation_v1"
        / day_utc
        / "trading_day_intent_generation.v1.json"
    ).resolve()
    if not report_path.exists() or not report_path.is_file():
        return None, report_path
    return _read_json_object(report_path), report_path


def _intent_snapshot_roots(*, truth_root: Path, day_utc: str, environment: str) -> list[Path]:
    day = str(day_utc).strip()
    roots: list[Path] = [((truth_root / "intents_v1" / "snapshots" / day).resolve())]
    if str(environment).strip().upper() == "PAPER":
        canonical_truth_root: Path | None = None
        try:
            canonical_truth_root = resolve_canonical_truth_root_bridge_v1(
                caller="constellation_2.common.opportunity_lineage_attribution_v1"
            ).resolve()
        except Exception:
            canonical_truth_root = None
        try:
            sleeves_root = (
                resolve_truth_sleeves_root_bridge_v1(
                    caller="constellation_2.common.opportunity_lineage_attribution_v1"
                ).resolve()
                if canonical_truth_root is not None and truth_root.resolve() == canonical_truth_root
                else None
            )
        except Exception:
            sleeves_root = None
        if sleeves_root is not None and sleeves_root.exists() and sleeves_root.is_dir():
            for sleeve_dir in sorted(path for path in sleeves_root.iterdir() if path.is_dir()):
                roots.append((sleeve_dir / "PAPER" / "intents_v1" / "snapshots" / day).resolve())
    out: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        token = str(root)
        if token in seen:
            continue
        seen.add(token)
        out.append(root)
    return out


def _list_intent_snapshots(*, truth_root: Path, day_utc: str, environment: str = "PAPER") -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for root in _intent_snapshot_roots(truth_root=truth_root, day_utc=day_utc, environment=environment):
        if not root.exists() or not root.is_dir():
            continue
        for path in sorted(root.glob("*.exposure_intent.v1.json")):
            token = str(path.resolve())
            if token in seen:
                continue
            seen.add(token)
            out.append(path.resolve())
    return out


def _submission_day_roots(*, truth_root: Path, day_utc: str, environment: str) -> list[Path]:
    day = str(day_utc).strip()
    env = str(environment).strip().upper()
    roots: list[Path] = [((truth_root / "execution_evidence_v1" / "submissions" / day).resolve())]
    if env == "PAPER":
        canonical_truth_root: Path | None = None
        try:
            canonical_truth_root = resolve_canonical_truth_root_bridge_v1(
                caller="constellation_2.common.opportunity_lineage_attribution_v1"
            ).resolve()
        except Exception:
            canonical_truth_root = None
        if canonical_truth_root is None or truth_root.resolve() != canonical_truth_root:
            canonical_truth_root = None
        try:
            sleeves_root = (
                resolve_truth_sleeves_root_bridge_v1(
                    caller="constellation_2.common.opportunity_lineage_attribution_v1"
                ).resolve()
                if canonical_truth_root is not None
                else None
            )
        except Exception:
            sleeves_root = None
        if sleeves_root is not None and sleeves_root.exists() and sleeves_root.is_dir():
            for sleeve_dir in sorted(path for path in sleeves_root.iterdir() if path.is_dir()):
                roots.append((sleeve_dir / "PAPER" / "execution_evidence_v1" / "submissions" / day).resolve())
    out: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        token = str(root)
        if token in seen:
            continue
        seen.add(token)
        out.append(root)
    return out


def _list_submission_dirs(*, truth_root: Path, day_utc: str, environment: str) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for root in _submission_day_roots(truth_root=truth_root, day_utc=day_utc, environment=environment):
        if not root.exists() or not root.is_dir():
            continue
        for path in sorted(child for child in root.iterdir() if child.is_dir()):
            token = str(path.resolve())
            if token in seen:
                continue
            seen.add(token)
            out.append(path.resolve())
    return out


def _authorization_day_roots(*, truth_root: Path, day_utc: str, environment: str) -> list[Path]:
    day = str(day_utc).strip()
    env = str(environment).strip().upper()
    roots: list[Path] = [((truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve())]
    if env == "PAPER":
        canonical_truth_root: Path | None = None
        try:
            canonical_truth_root = resolve_canonical_truth_root_bridge_v1(
                caller="constellation_2.common.opportunity_lineage_attribution_v1"
            ).resolve()
        except Exception:
            canonical_truth_root = None
        if canonical_truth_root is None or truth_root.resolve() != canonical_truth_root:
            canonical_truth_root = None
        try:
            sleeves_root = (
                resolve_truth_sleeves_root_bridge_v1(
                    caller="constellation_2.common.opportunity_lineage_attribution_v1"
                ).resolve()
                if canonical_truth_root is not None
                else None
            )
        except Exception:
            sleeves_root = None
        if sleeves_root is not None and sleeves_root.exists() and sleeves_root.is_dir():
            for sleeve_dir in sorted(path for path in sleeves_root.iterdir() if path.is_dir()):
                roots.append((sleeve_dir / "PAPER" / "engine_activity_v1" / "authorization_v1" / day).resolve())
    out: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        token = str(root)
        if token in seen:
            continue
        seen.add(token)
        out.append(root)
    return out


def _normalize_reason_codes(raw_codes: Sequence[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_codes:
        code = str(raw or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _extract_authorization_reason_codes(payload: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    out.extend(_normalize_reason_codes(payload.get("reason_codes") or []))
    constitutional_shadow = (
        payload.get("constitutional_shadow") if isinstance(payload.get("constitutional_shadow"), Mapping) else {}
    )
    decision = constitutional_shadow.get("decision") if isinstance(constitutional_shadow.get("decision"), Mapping) else {}
    out.extend(_normalize_reason_codes(decision.get("blocker_rules") or []))
    return _normalize_reason_codes(out)


def _extract_authorization_headroom_detail(payload: Mapping[str, Any]) -> str:
    auth_block = payload.get("authorization") if isinstance(payload.get("authorization"), Mapping) else {}
    raw_constraints = auth_block.get("constraints") if isinstance(auth_block.get("constraints"), Sequence) else []
    constraint_map: dict[str, str] = {}
    for raw in raw_constraints:
        token = str(raw or "").strip()
        if not token or "=" not in token:
            continue
        key, value = token.split("=", 1)
        key_norm = str(key).strip().upper()
        value_norm = str(value).strip()
        if key_norm in HEADROOM_CONSTRAINT_KEYS and value_norm:
            constraint_map[key_norm] = value_norm
    if not constraint_map:
        return ""
    return "HEADROOM_METRICS:" + "|".join(
        f"{key}={constraint_map[key]}"
        for key in HEADROOM_CONSTRAINT_KEYS
        if key in constraint_map
    )


def _resolve_specific_authz_reason_for_skip(
    *,
    truth_root: Path,
    day_utc: str,
    environment: str,
    submission_metas: Sequence[Mapping[str, Any]],
) -> tuple[str | None, str, dict[str, str] | None]:
    candidate_paths: list[Path] = []
    seen_paths: set[str] = set()
    for meta in submission_metas:
        raw_pointers = meta.get("veto_pointers") if isinstance(meta.get("veto_pointers"), Sequence) else []
        for raw in raw_pointers:
            pointer_text = str(raw or "").strip()
            if not pointer_text:
                continue
            pointer_path = Path(pointer_text)
            if not pointer_path.is_absolute():
                pointer_path = (truth_root / pointer_path).resolve()
            if str(pointer_path).endswith(".authorization.v1.json"):
                token = str(pointer_path)
                if token not in seen_paths:
                    seen_paths.add(token)
                    candidate_paths.append(pointer_path)
        intent_hash = str(meta.get("intent_hash") or "").strip().lower()
        if intent_hash:
            for root in _authorization_day_roots(truth_root=truth_root, day_utc=day_utc, environment=environment):
                token_path = (root / f"{intent_hash}.authorization.v1.json").resolve()
                token = str(token_path)
                if token in seen_paths:
                    continue
                seen_paths.add(token)
                candidate_paths.append(token_path)
    fallback: tuple[str, str, dict[str, str] | None] | None = None
    for path in candidate_paths:
        if not path.exists() or not path.is_file():
            continue
        payload = _read_json_object(path)
        reason_codes = _extract_authorization_reason_codes(payload)
        if not reason_codes:
            continue
        reason_code = reason_codes[0]
        preferred_authz = next((code for code in reason_codes if code.startswith("AUTHZ_")), "")
        if preferred_authz:
            reason_code = preferred_authz
        detail = f"AUTHORIZATION_REASON_CODES:{'|'.join(reason_codes)}"
        headroom_detail = _extract_authorization_headroom_detail(payload)
        if headroom_detail:
            detail = f"{detail}; {headroom_detail}"
        ref = _event_evidence_ref(path, "authorization_v1")
        if preferred_authz:
            return reason_code, detail, ref
        if fallback is None:
            fallback = (reason_code, detail, ref)
    if fallback is not None:
        return fallback
    return None, "", None


def _list_execution_stream_records(*, truth_root: Path, day_utc: str) -> list[Path]:
    root = (truth_root / "execution_stream_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return []
    return sorted(root.glob("*.execution_event_stream_record.v1.json"))


def _list_fill_ledgers(*, truth_root: Path, day_utc: str) -> list[Path]:
    root = (truth_root / "fill_ledger_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return []
    return sorted(root.glob("*.fill_ledger.v1.json"))


def _submission_meta(*, submission_dir: Path) -> dict[str, Any]:
    bsr_path = (submission_dir / "broker_submission_record.v2.json").resolve()
    bsr = _read_json_object(bsr_path) if bsr_path.exists() else {}
    veto_path = (submission_dir / "veto_record.v1.json").resolve()
    veto = _read_json_object(veto_path) if veto_path.exists() else {}
    execution_event_path = (submission_dir / "execution_event_record.v1.json").resolve()
    execution_event = _read_json_object(execution_event_path) if execution_event_path.exists() else {}
    plan_v2_path = (submission_dir / "equity_order_plan.v2.json").resolve()
    plan_v1_path = (submission_dir / "equity_order_plan.v1.json").resolve()
    order_plan_path = (submission_dir / "order_plan.v1.json").resolve()
    plan = {}
    plan_path = None
    if plan_v2_path.exists():
        plan = _read_json_object(plan_v2_path)
        plan_path = plan_v2_path
    elif plan_v1_path.exists():
        plan = _read_json_object(plan_v1_path)
        plan_path = plan_v1_path
    elif order_plan_path.exists():
        plan = _read_json_object(order_plan_path)
        plan_path = order_plan_path
    source_intent_id = (
        str(execution_event.get("source_intent_id") or "").strip()
        or str(plan.get("source_intent_id") or "").strip()
        or str(bsr.get("intent_id") or "").strip()
        or str(plan.get("plan_id") or "").strip()
    )
    sleeve_id = (
        str(execution_event.get("engine_id") or "").strip()
        or str(plan.get("engine_id") or "").strip()
        or "UNKNOWN_ENGINE"
    )
    order_qty = None
    qty_any = plan.get("qty_shares")
    if isinstance(qty_any, int) and qty_any > 0:
        order_qty = qty_any
    risk = plan.get("risk_proof") if isinstance(plan.get("risk_proof"), dict) else {}
    contracts = risk.get("contracts")
    if order_qty is None and isinstance(contracts, int) and contracts > 0:
        order_qty = contracts
    submitted_at_utc = (
        str(bsr.get("submitted_at_utc") or "").strip()
        or str(execution_event.get("event_time_utc") or "").strip()
        or str(veto.get("observed_at_utc") or "").strip()
    )
    veto_inputs = veto.get("inputs") if isinstance(veto.get("inputs"), Mapping) else {}
    veto_pointers = veto.get("pointers") if isinstance(veto.get("pointers"), Sequence) else []
    intent_hash = (
        str(veto_inputs.get("intent_hash") or "").strip().lower()
        or str(plan.get("intent_hash") or "").strip().lower()
        or str(plan.get("intent_sha256") or "").strip().lower()
    )
    broker_ids = bsr.get("broker_ids") if isinstance(bsr.get("broker_ids"), Mapping) else {}
    broker_order_id = broker_ids.get("order_id")
    broker_perm_id = broker_ids.get("perm_id")
    broker_error = bsr.get("error") if isinstance(bsr.get("error"), Mapping) else {}
    broker_error_code = str(broker_error.get("code") or "").strip().upper()
    dry_run_no_broker_id = (
        broker_error_code == DRY_RUN_NO_BROKER_ID_ERROR_CODE
        and not isinstance(broker_order_id, int)
        and not isinstance(broker_perm_id, int)
    )
    order_linkage = bsr.get("order_linkage") if isinstance(bsr.get("order_linkage"), Mapping) else {}
    protection_status = str(bsr.get("protection_status") or "").strip().upper()
    stop_order_id = order_linkage.get("stop_order_id")
    protected_submission = (
        protection_status == "PROTECTED"
        or isinstance(stop_order_id, int)
    )
    return {
        "submission_id": str(bsr.get("submission_id") or submission_dir.name).strip(),
        "sleeve_id": sleeve_id,
        "opportunity_id": source_intent_id or f"submission:{submission_dir.name}",
        "intent_id": source_intent_id or "",
        "submitted_at_utc": submitted_at_utc,
        "broker_status": str(bsr.get("status") or "").strip().upper(),
        "broker_error": bsr.get("error"),
        "order_qty": order_qty,
        "submit_attempt_evidence_present": bool(bsr_path.exists() and bsr_path.is_file()),
        "bsr_path": bsr_path if bsr_path.exists() else None,
        "veto_path": veto_path if veto_path.exists() else None,
        "veto_reason_code": str(veto.get("reason_code") or "").strip().upper(),
        "veto_reason_detail": str(veto.get("reason_detail") or "").strip(),
        "veto_pointers": [str(item).strip() for item in veto_pointers if str(item).strip()],
        "intent_hash": intent_hash,
        "execution_event_path": execution_event_path if execution_event_path.exists() else None,
        "plan_path": plan_path,
        "execution_event": execution_event,
        "broker_error_code": broker_error_code,
        "broker_order_id": broker_order_id,
        "broker_perm_id": broker_perm_id,
        "dry_run_no_broker_id": dry_run_no_broker_id,
        "protected_submission": protected_submission,
        "protection_status": protection_status or "UNKNOWN",
        "order_linkage": dict(order_linkage) if isinstance(order_linkage, Mapping) else {},
    }


def _submission_attempt_is_dry_run_no_broker_id(
    submission_metas: Sequence[Mapping[str, Any]],
) -> bool:
    attempted_rows = [
        row
        for row in submission_metas
        if isinstance(row, Mapping) and bool(row.get("submit_attempt_evidence_present"))
    ]
    if not attempted_rows:
        return False
    return all(bool(row.get("dry_run_no_broker_id")) for row in attempted_rows)


def _load_submit_decision_context(
    *,
    truth_root: Path,
    day_utc: str,
) -> tuple[str, bool, str, list[dict[str, str]]]:
    boundary_status = "UNKNOWN"
    submission_authorized = False
    authority_status = "UNKNOWN"
    refs: list[dict[str, str]] = []

    boundary_path = (
        truth_root
        / "reports"
        / "submit_boundary_status_v1"
        / day_utc
        / "submit_boundary_status.v1.json"
    ).resolve()
    if boundary_path.exists() and boundary_path.is_file():
        payload = _read_json_object(boundary_path)
        boundary_status = str(payload.get("boundary_status") or "").strip().upper() or "UNKNOWN"
        submission_authorized = bool(payload.get("submission_authorized") is True)
        refs.append(_event_evidence_ref(boundary_path, "submit_boundary_status_v1"))

    ledger_path = (
        truth_root
        / "reports"
        / "paper_session_ledger_v1"
        / day_utc
        / "paper_session_ledger.v1.json"
    ).resolve()
    if ledger_path.exists() and ledger_path.is_file():
        payload = _read_json_object(ledger_path)
        control_state = payload.get("control_state") if isinstance(payload.get("control_state"), Mapping) else {}
        authority_status = (
            str(control_state.get("authority_status") or "").strip().upper()
            or str(payload.get("authority_status") or "").strip().upper()
            or "UNKNOWN"
        )
        if (not submission_authorized) and bool(control_state.get("submission_authorized") is True):
            submission_authorized = True
        refs.append(_event_evidence_ref(ledger_path, "paper_session_ledger_v1"))
    return boundary_status, submission_authorized, authority_status, refs


def _submit_decision_seed(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "day_utc": str(payload.get("day_utc") or "").strip(),
        "environment": str(payload.get("environment") or "").strip(),
        "sleeve_id": str(payload.get("sleeve_id") or "").strip(),
        "opportunity_id": str(payload.get("opportunity_id") or "").strip(),
        "intent_id": str(payload.get("intent_id") or "").strip(),
        "submit_decision": str(payload.get("submit_decision") or "").strip(),
        "submit_decision_reason_code": str(payload.get("submit_decision_reason_code") or "").strip(),
        "submit_decision_reason_detail": str(payload.get("submit_decision_reason_detail") or "").strip(),
        "boundary_status_at_decision": str(payload.get("boundary_status_at_decision") or "").strip(),
        "submission_authorized_at_decision": bool(payload.get("submission_authorized_at_decision") is True),
        "authority_status_at_decision": str(payload.get("authority_status_at_decision") or "").strip(),
        "observed_at_utc": str(payload.get("observed_at_utc") or "").strip(),
        "evidence_refs": list(payload.get("evidence_refs") or []),
    }


def _build_submit_decision_trace_payload(
    *,
    day_utc: str,
    environment: str,
    sleeve_id: str,
    opportunity_id: str,
    intent_id: str,
    submit_decision: str,
    submit_decision_reason_code: str,
    submit_decision_reason_detail: str,
    boundary_status_at_decision: str,
    submission_authorized_at_decision: bool,
    authority_status_at_decision: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    observed_at_utc: str,
    emitted_by: str,
) -> dict[str, Any]:
    decision = str(submit_decision).strip().upper()
    if decision not in {"ATTEMPTED", "SKIPPED", "UNKNOWN"}:
        raise RuntimeError(f"SUBMIT_DECISION_UNSUPPORTED:{decision}")
    payload = {
        "schema_id": "submit_decision_trace",
        "schema_version": "v1",
        "artifact_id": SUBMIT_DECISION_TRACE_ARTIFACT_ID,
        "day_utc": str(day_utc).strip(),
        "environment": str(environment).strip().upper() or "PAPER",
        "sleeve_id": str(sleeve_id).strip(),
        "opportunity_id": str(opportunity_id).strip(),
        "intent_id": str(intent_id).strip(),
        "submit_decision": decision,
        "submit_decision_reason_code": str(submit_decision_reason_code).strip().upper(),
        "submit_decision_reason_detail": str(submit_decision_reason_detail).strip(),
        "boundary_status_at_decision": str(boundary_status_at_decision).strip().upper() or "UNKNOWN",
        "submission_authorized_at_decision": bool(submission_authorized_at_decision),
        "authority_status_at_decision": str(authority_status_at_decision).strip().upper() or "UNKNOWN",
        "evidence_refs": [dict(item) for item in evidence_refs],
        "observed_at_utc": str(observed_at_utc).strip(),
        "emitted_by": str(emitted_by).strip(),
    }
    payload["decision_id"] = canonical_hash_for_c2_artifact_v1(_submit_decision_seed(payload))
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    validate_against_repo_schema_v1(payload, REPO_ROOT, SUBMIT_DECISION_TRACE_SCHEMA_RELPATH)
    return payload


def _write_submit_decision_trace_immutable(*, truth_root: Path, payload: Mapping[str, Any]) -> Path:
    day_utc = str(payload.get("day_utc") or "").strip()
    sleeve_id = str(payload.get("sleeve_id") or "").strip()
    opportunity_id = str(payload.get("opportunity_id") or "").strip()
    decision_id = str(payload.get("decision_id") or "").strip()
    out_path = (
        truth_root
        / "reports"
        / SUBMIT_DECISION_TRACE_FAMILY
        / day_utc
        / sleeve_id
        / _opportunity_path_token(opportunity_id)
        / decision_id
        / "submit_decision_trace.v1.json"
    ).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload_bytes = canonical_json_bytes_v1(dict(payload)) + b"\n"
    if out_path.exists():
        existing = out_path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload_bytes):
            return out_path
        existing_obj = _read_json_object(out_path)
        try:
            existing_seed = _submit_decision_seed(existing_obj)
        except Exception:  # noqa: BLE001
            existing_seed = {}
        incoming_seed = _submit_decision_seed(payload)
        if existing_seed == incoming_seed:
            return out_path
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES:path={out_path}")
    with NamedTemporaryFile(prefix=f".{out_path.name}.tmp.", dir=str(out_path.parent), delete=False) as tmp:
        tmp.write(payload_bytes)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(out_path)
    return out_path


def materialize_submit_decision_traces_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    environment: str = "PAPER",
    emitted_by: str = "constellation_2.common.opportunity_lineage_attribution_v1",
) -> SubmitDecisionTraceWriteResultV1:
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    env = str(environment).strip().upper() or "PAPER"
    fallback_time = _day_start_iso(day)
    submit_trace_emitter = "constellation_2.common.opportunity_lineage_attribution_v1.materialize_submit_decision_traces_v1"
    warnings: list[str] = []
    trace_paths: list[str] = []
    max_observed_at = fallback_time
    boundary_status, submission_authorized, authority_status, context_refs = _load_submit_decision_context(
        truth_root=root,
        day_utc=day,
    )
    intent_paths = _list_intent_snapshots(truth_root=root, day_utc=day, environment=env)
    submission_dirs = _list_submission_dirs(truth_root=root, day_utc=day, environment=env)

    intent_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for intent_path in intent_paths:
        payload = _read_json_object(intent_path)
        engine = payload.get("engine") if isinstance(payload.get("engine"), Mapping) else {}
        sleeve_id = str(engine.get("engine_id") or "").strip() or "UNKNOWN_ENGINE"
        intent_id = str(payload.get("intent_id") or "").strip() or f"intent:{intent_path.stem}"
        observed_at = _as_iso_utc(str(payload.get("created_at_utc") or ""), fallback_time)
        max_observed_at = max(max_observed_at, observed_at)
        key = (sleeve_id, intent_id)
        intent_rows[key] = {
            "intent_id": intent_id,
            "sleeve_id": sleeve_id,
            "opportunity_id": intent_id,
            "observed_at_utc": observed_at,
            "intent_path": intent_path,
        }

    decision_signals: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "attempt_refs": [],
            "skip_refs": [],
            "skip_reason_codes": [],
            "skip_reason_details": [],
            "observed_at_utc": fallback_time,
            "submission_metas": [],
        }
    )
    for submission_dir in submission_dirs:
        meta = _submission_meta(submission_dir=submission_dir)
        sleeve_id = str(meta.get("sleeve_id") or "").strip() or "UNKNOWN_ENGINE"
        opportunity_id = str(meta.get("opportunity_id") or "").strip() or f"submission:{submission_dir.name}"
        observed_at = _as_iso_utc(str(meta.get("submitted_at_utc") or ""), fallback_time)
        max_observed_at = max(max_observed_at, observed_at)
        key = (sleeve_id, opportunity_id)
        signals = decision_signals[key]
        signals["observed_at_utc"] = max(str(signals["observed_at_utc"]), observed_at)
        signals["submission_metas"].append(meta)
        if meta.get("submit_attempt_evidence_present") and meta.get("bsr_path") is not None:
            signals["attempt_refs"].append(
                _event_evidence_ref(Path(meta["bsr_path"]), "broker_submission_record_v2")
            )
            if meta.get("execution_event_path") is not None:
                signals["attempt_refs"].append(
                    _event_evidence_ref(Path(meta["execution_event_path"]), "execution_event_record_v1")
                )
        if meta.get("veto_path") is not None:
            signals["skip_refs"].append(
                _event_evidence_ref(Path(meta["veto_path"]), "veto_record_v1")
            )
            code = str(meta.get("veto_reason_code") or "").strip().upper() or "SUBMIT_PIPELINE_NOT_REACHED"
            signals["skip_reason_codes"].append(code)
            detail = str(meta.get("veto_reason_detail") or "").strip()
            if detail:
                signals["skip_reason_details"].append(detail)

    for key in sorted(intent_rows.keys()):
        intent_row = intent_rows[key]
        signals = decision_signals.get(key, {})
        attempt_refs = list(signals.get("attempt_refs") or [])
        skip_refs = list(signals.get("skip_refs") or [])
        skip_reason_codes = sorted({str(item).strip().upper() for item in (signals.get("skip_reason_codes") or []) if str(item).strip()})
        skip_reason_details = [str(item).strip() for item in (signals.get("skip_reason_details") or []) if str(item).strip()]
        observed_at = _as_iso_utc(str(signals.get("observed_at_utc") or ""), str(intent_row["observed_at_utc"]))
        max_observed_at = max(max_observed_at, observed_at)

        if attempt_refs:
            decision = "ATTEMPTED"
            if _submission_attempt_is_dry_run_no_broker_id(
                [dict(item) for item in (signals.get("submission_metas") or []) if isinstance(item, Mapping)]
            ):
                reason_code = SUBMIT_DECISION_DRY_RUN_REASON_CODE
                reason_detail = (
                    "Submission attempt evidence exists, but governed submit ran in dry-run mode; "
                    "broker order identifiers were not assigned."
                )
            else:
                has_protected_attempt = any(
                    bool(item.get("protected_submission"))
                    for item in (signals.get("submission_metas") or [])
                    if isinstance(item, Mapping)
                )
                if has_protected_attempt:
                    reason_code = "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT_PROTECTED"
                else:
                    reason_code = "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT"
                if skip_refs:
                    reason_detail = (
                        "Primary submission evidence exists for this intent; "
                        "historical skip evidence was preserved as audit context."
                    )
                else:
                    if has_protected_attempt:
                        reason_detail = "Primary protected bracket submission evidence exists for this intent."
                    else:
                        reason_detail = "Primary submission evidence exists for this intent."
        elif skip_refs:
            decision = "SKIPPED"
            reason_code = skip_reason_codes[0] if len(skip_reason_codes) == 1 else "MULTIPLE_SKIP_REASONS"
            reason_detail = "; ".join(skip_reason_details) if skip_reason_details else "Explicit submit veto evidence exists."
            if "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED" in skip_reason_codes:
                specific_code, specific_detail, auth_ref = _resolve_specific_authz_reason_for_skip(
                    truth_root=root,
                    day_utc=day,
                    environment=env,
                    submission_metas=[dict(item) for item in (signals.get("submission_metas") or []) if isinstance(item, Mapping)],
                )
                if specific_code:
                    reason_code = specific_code
                    if specific_detail:
                        reason_detail = f"{reason_detail}; {specific_detail}" if reason_detail else specific_detail
                if auth_ref is not None:
                    skip_refs.append(auth_ref)
        else:
            decision = "UNKNOWN"
            reason_code = "NO_SUBMIT_DECISION_EVIDENCE"
            reason_detail = "No primary submission-attempt or explicit skip evidence was found."

        evidence_refs: list[dict[str, str]] = []
        seen_paths: set[str] = set()
        for ref in [*attempt_refs, *skip_refs, _event_evidence_ref(Path(intent_row["intent_path"]), "exposure_intent_v1"), *context_refs]:
            path = str(ref.get("artifact_path") or "").strip()
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)
            evidence_refs.append(dict(ref))

        payload = _build_submit_decision_trace_payload(
            day_utc=day,
            environment=env,
            sleeve_id=str(intent_row["sleeve_id"]),
            opportunity_id=str(intent_row["opportunity_id"]),
            intent_id=str(intent_row["intent_id"]),
            submit_decision=decision,
            submit_decision_reason_code=reason_code,
            submit_decision_reason_detail=reason_detail,
            boundary_status_at_decision=boundary_status,
            submission_authorized_at_decision=submission_authorized,
            authority_status_at_decision=authority_status,
            evidence_refs=evidence_refs,
            observed_at_utc=observed_at,
            emitted_by=submit_trace_emitter,
        )
        trace_path = _write_submit_decision_trace_immutable(truth_root=root, payload=payload)
        trace_paths.append(str(trace_path))

    if not intent_rows:
        warnings.append("MISSING_INTENT_EVIDENCE_FOR_SUBMIT_DECISION_TRACE")

    return SubmitDecisionTraceWriteResultV1(
        day_utc=day,
        environment=env,
        trace_count=len(trace_paths),
        trace_paths=tuple(sorted(trace_paths)),
        warnings=tuple(sorted(set(warnings))),
        evidence_cutoff_utc=max_observed_at,
    )


def read_submit_decision_traces_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    root = (
        Path(truth_root).resolve()
        / "reports"
        / SUBMIT_DECISION_TRACE_FAMILY
        / str(day_utc).strip()
    ).resolve()
    if not root.exists() or not root.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/*/*/submit_decision_trace.v1.json")):
        payload = _read_json_object(path)
        validate_against_repo_schema_v1(payload, REPO_ROOT, SUBMIT_DECISION_TRACE_SCHEMA_RELPATH)
        payload["_source_path"] = str(path.resolve())
        rows.append(payload)
    return rows


def _event_append(
    *,
    events: list[dict[str, Any]],
    day_utc: str,
    environment: str,
    sleeve_id: str,
    opportunity_id: str,
    stage: str,
    stage_status: str,
    observed_at_utc: str,
    reason_code: str,
    reason_detail: str,
    evidence_refs: Sequence[Mapping[str, Any]],
    upstream_refs: Sequence[Mapping[str, Any]],
    emitted_by: str,
    stage_metrics: Mapping[str, Any] | None = None,
) -> None:
    events.append(
        _build_lineage_event_payload(
            day_utc=day_utc,
            environment=environment,
            sleeve_id=sleeve_id or "UNKNOWN_ENGINE",
            opportunity_id=opportunity_id or f"unknown:{day_utc}:{sleeve_id or 'UNKNOWN_ENGINE'}",
            lineage_stage=stage,
            stage_status=stage_status,
            observed_at_utc=observed_at_utc,
            reason_code=reason_code,
            reason_detail=reason_detail,
            evidence_refs=evidence_refs,
            upstream_refs=upstream_refs,
            emitted_by=emitted_by,
            stage_metrics=stage_metrics,
        )
    )


def materialize_opportunity_lineage_events_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    environment: str = "PAPER",
    emitted_by: str = "constellation_2.common.opportunity_lineage_attribution_v1",
) -> MaterializedLineageResultV1:
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    env = str(environment).strip().upper() or "PAPER"
    fallback_time = _day_start_iso(day)

    events: list[dict[str, Any]] = []
    warnings: list[str] = []
    sleeves_with_events: set[str] = set()
    max_observed_at = fallback_time

    intent_report, intent_report_path = _load_intent_generation_report(truth_root=root, day_utc=day)
    intent_paths = _list_intent_snapshots(truth_root=root, day_utc=day, environment=env)
    submission_dirs = _list_submission_dirs(truth_root=root, day_utc=day, environment=env)
    stream_paths = _list_execution_stream_records(truth_root=root, day_utc=day)
    fill_paths = _list_fill_ledgers(truth_root=root, day_utc=day)

    intent_sleeves: set[str] = set()
    for intent_path in intent_paths:
        intent_payload = _read_json_object(intent_path)
        engine = intent_payload.get("engine") if isinstance(intent_payload.get("engine"), dict) else {}
        sleeve_id = str(engine.get("engine_id") or "").strip() or "UNKNOWN_ENGINE"
        opportunity_id = str(intent_payload.get("intent_id") or "").strip() or f"intent:{intent_path.stem}"
        observed = _as_iso_utc(str(intent_payload.get("created_at_utc") or ""), fallback_time)
        max_observed_at = max(max_observed_at, observed)
        sleeves_with_events.add(sleeve_id)
        intent_sleeves.add(sleeve_id)
        refs = (_event_evidence_ref(intent_path, "exposure_intent_v1"),)
        upstream = (_event_upstream_ref(intent_path, "exposure_intent_v1"),)
        _event_append(
            events=events,
            day_utc=day,
            environment=env,
            sleeve_id=sleeve_id,
            opportunity_id=opportunity_id,
            stage="sleeve_run",
            stage_status="PASS",
            observed_at_utc=observed,
            reason_code="SLEEVE_RUN_EVIDENCED_BY_INTENT",
            reason_detail="Intent snapshot exists for this opportunity.",
            evidence_refs=refs,
            upstream_refs=upstream,
            emitted_by=emitted_by,
        )
        _event_append(
            events=events,
            day_utc=day,
            environment=env,
            sleeve_id=sleeve_id,
            opportunity_id=opportunity_id,
            stage="signal_eval",
            stage_status="PASS",
            observed_at_utc=observed,
            reason_code="SIGNAL_PRESENT",
            reason_detail="Intent snapshot implies signal evaluation produced an actionable intent.",
            evidence_refs=refs,
            upstream_refs=upstream,
            emitted_by=emitted_by,
        )
        _event_append(
            events=events,
            day_utc=day,
            environment=env,
            sleeve_id=sleeve_id,
            opportunity_id=opportunity_id,
            stage="intent_created",
            stage_status="PASS",
            observed_at_utc=observed,
            reason_code="INTENT_CREATED",
            reason_detail="Canonical intent snapshot materialized.",
            evidence_refs=refs,
            upstream_refs=upstream,
            emitted_by=emitted_by,
        )

    expected_sleeves: set[str] = set()
    if intent_report is not None:
        report_time = _as_iso_utc(str(intent_report.get("produced_at_utc") or ""), fallback_time)
        max_observed_at = max(max_observed_at, report_time)
        producer_results = (
            intent_report.get("producer_results")
            if isinstance(intent_report.get("producer_results"), list)
            else []
        )
        producer_topology = (
            intent_report.get("producer_topology")
            if isinstance(intent_report.get("producer_topology"), list)
            else []
        )
        for row in producer_topology:
            if isinstance(row, dict):
                engine_id = str(row.get("engine_id") or "").strip()
                if engine_id:
                    expected_sleeves.add(engine_id)
        for row in producer_results:
            if not isinstance(row, dict):
                continue
            engine_id = str(row.get("engine_id") or "").strip() or "UNKNOWN_ENGINE"
            status = str(row.get("status") or "").strip().upper()
            reason_codes = [
                str(code).strip().upper()
                for code in (row.get("reason_codes") or [])
                if str(code).strip()
            ]
            reason_code = _safe_reason_code(reason_codes, f"INTENT_GENERATION_STATUS_{status or 'UNKNOWN'}")
            opportunity_id = f"intent_generation:{day}:{engine_id}"
            evidence_ref = _event_evidence_ref(intent_report_path, "trading_day_intent_generation_v1")
            upstream_ref = _event_upstream_ref(intent_report_path, "trading_day_intent_generation_v1")
            if status == "NO_INTENT":
                sleeves_with_events.add(engine_id)
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=engine_id,
                    opportunity_id=opportunity_id,
                    stage="sleeve_run",
                    stage_status="PASS",
                    observed_at_utc=report_time,
                    reason_code="SLEEVE_RUN_NO_INTENT",
                    reason_detail="Engine evaluated for day and emitted NO_INTENT.",
                    evidence_refs=(evidence_ref,),
                    upstream_refs=(upstream_ref,),
                    emitted_by=emitted_by,
                )
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=engine_id,
                    opportunity_id=opportunity_id,
                    stage="signal_eval",
                    stage_status="PASS",
                    observed_at_utc=report_time,
                    reason_code=reason_code,
                    reason_detail="Signal evaluation completed without creating an intent.",
                    evidence_refs=(evidence_ref,),
                    upstream_refs=(upstream_ref,),
                    emitted_by=emitted_by,
                )
                if _is_filter_reason(reason_code):
                    _event_append(
                        events=events,
                        day_utc=day,
                        environment=env,
                        sleeve_id=engine_id,
                        opportunity_id=opportunity_id,
                        stage="signal_filtered",
                        stage_status="PASS",
                        observed_at_utc=report_time,
                        reason_code=reason_code,
                        reason_detail="Signal filtered by risk or policy.",
                        evidence_refs=(evidence_ref,),
                        upstream_refs=(upstream_ref,),
                        emitted_by=emitted_by,
                    )
            elif status == "BLOCKED_BY_DEFECT":
                sleeves_with_events.add(engine_id)
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=engine_id,
                    opportunity_id=opportunity_id,
                    stage="sleeve_run",
                    stage_status="FAIL",
                    observed_at_utc=report_time,
                    reason_code=reason_code,
                    reason_detail="Intent-generation producer reported a defect-level block.",
                    evidence_refs=(evidence_ref,),
                    upstream_refs=(upstream_ref,),
                    emitted_by=emitted_by,
                )
            elif status == "SKIPPED" and reason_code == "EXISTING_VALID_ZERO_MARKER_PRESENT":
                sleeves_with_events.add(engine_id)
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=engine_id,
                    opportunity_id=opportunity_id,
                    stage="sleeve_run",
                    stage_status="PASS",
                    observed_at_utc=report_time,
                    reason_code="VALID_ZERO_MARKER_PRESENT",
                    reason_detail="Prior valid-zero marker confirms no-intent state.",
                    evidence_refs=(evidence_ref,),
                    upstream_refs=(upstream_ref,),
                    emitted_by=emitted_by,
                )
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=engine_id,
                    opportunity_id=opportunity_id,
                    stage="signal_eval",
                    stage_status="PASS",
                    observed_at_utc=report_time,
                    reason_code="NO_ENTRY_CONDITION_MET",
                    reason_detail="Valid-zero marker indicates no entry condition.",
                    evidence_refs=(evidence_ref,),
                    upstream_refs=(upstream_ref,),
                    emitted_by=emitted_by,
                )
    else:
        warnings.append("MISSING_INTENT_GENERATION_REPORT")

    submission_meta_by_id: dict[str, dict[str, Any]] = {}
    for submission_dir in submission_dirs:
        meta = _submission_meta(submission_dir=submission_dir)
        submission_meta_by_id[str(meta["submission_id"])] = meta
        sleeve_id = str(meta["sleeve_id"] or "").strip() or "UNKNOWN_ENGINE"
        opportunity_id = str(meta["opportunity_id"] or "").strip()
        submitted_at = _as_iso_utc(str(meta.get("submitted_at_utc") or ""), fallback_time)
        max_observed_at = max(max_observed_at, submitted_at)
        sleeves_with_events.add(sleeve_id)
        refs: list[dict[str, str]] = []
        upstream: list[dict[str, str]] = []
        if meta.get("bsr_path") is not None:
            refs.append(_event_evidence_ref(Path(meta["bsr_path"]), "broker_submission_record_v2"))
            upstream.append(_event_upstream_ref(Path(meta["bsr_path"]), "broker_submission_record_v2"))
        if meta.get("execution_event_path") is not None:
            refs.append(_event_evidence_ref(Path(meta["execution_event_path"]), "execution_event_record_v1"))
            upstream.append(_event_upstream_ref(Path(meta["execution_event_path"]), "execution_event_record_v1"))
        if meta.get("plan_path") is not None:
            refs.append(_event_evidence_ref(Path(meta["plan_path"]), "order_plan"))
            upstream.append(_event_upstream_ref(Path(meta["plan_path"]), "order_plan"))
        if meta.get("submit_attempt_evidence_present"):
            submit_attempt_reason_code = (
                SUBMIT_DECISION_DRY_RUN_REASON_CODE
                if bool(meta.get("dry_run_no_broker_id"))
                else "SUBMISSION_EVIDENCE_PRESENT"
            )
            submit_attempt_reason_detail = (
                "Submission evidence exists in governed dry-run mode without broker order identifiers."
                if bool(meta.get("dry_run_no_broker_id"))
                else f"Submission evidence exists for submission_id={meta['submission_id']}."
            )
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="submit_attempted",
                stage_status="PASS",
                observed_at_utc=submitted_at,
                reason_code=submit_attempt_reason_code,
                reason_detail=submit_attempt_reason_detail,
                evidence_refs=refs,
                upstream_refs=upstream,
                emitted_by=emitted_by,
            )
        broker_status = str(meta.get("broker_status") or "").strip().upper()
        if broker_status in ORDER_STATUS_REJECT:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_rejected",
                stage_status="PASS",
                observed_at_utc=submitted_at,
                reason_code="BROKER_PRE_ACK_REJECT",
                reason_detail=f"Broker status={broker_status}.",
                evidence_refs=refs,
                upstream_refs=upstream,
                emitted_by=emitted_by,
            )
        elif broker_status in ORDER_STATUS_CANCEL:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_cancelled",
                stage_status="PASS",
                observed_at_utc=submitted_at,
                reason_code="BROKER_ORDER_CANCELLED",
                reason_detail=f"Broker status={broker_status}.",
                evidence_refs=refs,
                upstream_refs=upstream,
                emitted_by=emitted_by,
            )
        elif broker_status in ORDER_STATUS_ACK and not bool(meta.get("dry_run_no_broker_id")):
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_ack",
                stage_status="PASS",
                observed_at_utc=submitted_at,
                reason_code=f"BROKER_STATUS_{broker_status}",
                reason_detail="Broker submission record indicates order acknowledgement lifecycle.",
                evidence_refs=refs,
                upstream_refs=upstream,
                emitted_by=emitted_by,
            )
        execution_event = meta.get("execution_event")
        if isinstance(execution_event, dict):
            execution_status = str(execution_event.get("status") or "").strip().upper()
            if execution_status in ORDER_STATUS_REJECT:
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=sleeve_id,
                    opportunity_id=opportunity_id,
                    stage="order_rejected",
                    stage_status="PASS",
                    observed_at_utc=_as_iso_utc(str(execution_event.get("event_time_utc") or ""), submitted_at),
                    reason_code=f"EXECUTION_EVENT_{execution_status}",
                    reason_detail="Execution event record indicates rejection.",
                    evidence_refs=refs,
                    upstream_refs=upstream,
                    emitted_by=emitted_by,
                )
            elif execution_status in ORDER_STATUS_CANCEL:
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=sleeve_id,
                    opportunity_id=opportunity_id,
                    stage="order_cancelled",
                    stage_status="PASS",
                    observed_at_utc=_as_iso_utc(str(execution_event.get("event_time_utc") or ""), submitted_at),
                    reason_code=f"EXECUTION_EVENT_{execution_status}",
                    reason_detail="Execution event record indicates cancellation.",
                    evidence_refs=refs,
                    upstream_refs=upstream,
                    emitted_by=emitted_by,
                )
            elif execution_status in ORDER_STATUS_ACK:
                _event_append(
                    events=events,
                    day_utc=day,
                    environment=env,
                    sleeve_id=sleeve_id,
                    opportunity_id=opportunity_id,
                    stage="order_ack",
                    stage_status="PASS",
                    observed_at_utc=_as_iso_utc(str(execution_event.get("event_time_utc") or ""), submitted_at),
                    reason_code=f"EXECUTION_EVENT_{execution_status}",
                    reason_detail="Execution event record indicates acknowledged order lifecycle.",
                    evidence_refs=refs,
                    upstream_refs=upstream,
                    emitted_by=emitted_by,
                )

    for stream_path in stream_paths:
        stream_payload = _read_json_object(stream_path)
        submission_id = str(stream_payload.get("submission_id") or "").strip()
        meta = submission_meta_by_id.get(submission_id, {})
        sleeve_id = (
            str(stream_payload.get("engine_id") or "").strip()
            or str(meta.get("sleeve_id") or "").strip()
            or "UNKNOWN_ENGINE"
        )
        opportunity_id = (
            str(stream_payload.get("source_intent_id") or "").strip()
            or str(meta.get("opportunity_id") or "").strip()
            or f"submission:{submission_id or stream_path.stem}"
        )
        observed = _as_iso_utc(
            str(stream_payload.get("event_time_utc") or stream_payload.get("observed_at_utc") or ""),
            fallback_time,
        )
        max_observed_at = max(max_observed_at, observed)
        sleeves_with_events.add(sleeve_id)
        evidence_ref = _event_evidence_ref(stream_path, "execution_event_stream_record_v1")
        upstream_ref = _event_upstream_ref(stream_path, "execution_event_stream_record_v1")
        order_state = (
            stream_payload.get("order_state")
            if isinstance(stream_payload.get("order_state"), dict)
            else {}
        )
        stream_status = str(order_state.get("status") or "").strip().upper()
        if stream_status in ORDER_STATUS_REJECT:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_rejected",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code=f"STREAM_STATUS_{stream_status}",
                reason_detail="Execution stream indicates order rejection.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
            )
        elif stream_status in ORDER_STATUS_CANCEL:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_cancelled",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code=f"STREAM_STATUS_{stream_status}",
                reason_detail="Execution stream indicates order cancellation.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
            )
        elif stream_status in ORDER_STATUS_ACK:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_ack",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code=f"STREAM_STATUS_{stream_status}",
                reason_detail="Execution stream indicates acknowledged order lifecycle.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
            )
        fill = stream_payload.get("fill") if isinstance(stream_payload.get("fill"), dict) else {}
        fill_qty = fill.get("fill_qty")
        if isinstance(fill_qty, int) and fill_qty > 0:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="fill_event",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code="EXECUTION_STREAM_FILL_EVENT",
                reason_detail="Execution stream reports non-zero fill quantity.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
                stage_metrics={"filled_qty": fill_qty},
            )

    for fill_path in fill_paths:
        fill_payload = _read_json_object(fill_path)
        sleeve_id = str(fill_payload.get("engine_id") or "").strip() or "UNKNOWN_ENGINE"
        opportunity_id = (
            str(fill_payload.get("source_intent_id") or "").strip()
            or f"submission:{str(fill_payload.get('submission_id') or fill_path.stem).strip()}"
        )
        observed = _as_iso_utc(str(fill_payload.get("produced_utc") or ""), fallback_time)
        max_observed_at = max(max_observed_at, observed)
        sleeves_with_events.add(sleeve_id)
        evidence_ref = _event_evidence_ref(fill_path, "fill_ledger_v1")
        upstream_ref = _event_upstream_ref(fill_path, "fill_ledger_v1")
        lifecycle_status = str(fill_payload.get("lifecycle_status") or "").strip().upper()
        filled_qty = fill_payload.get("filled_qty")
        order_qty = fill_payload.get("order_qty")
        if lifecycle_status in {"OPEN", "PARTIALLY_FILLED", "FILLED"}:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_ack",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code=f"FILL_LEDGER_{lifecycle_status}",
                reason_detail="Fill ledger confirms order lifecycle progressed beyond submit.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
            )
        if lifecycle_status == "REJECTED":
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_rejected",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code="FILL_LEDGER_REJECTED",
                reason_detail="Fill ledger lifecycle is REJECTED.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
            )
        if lifecycle_status == "CANCELLED":
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="order_cancelled",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code="FILL_LEDGER_CANCELLED",
                reason_detail="Fill ledger lifecycle is CANCELLED.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
            )
        if isinstance(filled_qty, int) and filled_qty > 0:
            _event_append(
                events=events,
                day_utc=day,
                environment=env,
                sleeve_id=sleeve_id,
                opportunity_id=opportunity_id,
                stage="fill_event",
                stage_status="PASS",
                observed_at_utc=observed,
                reason_code="FILL_LEDGER_FILLED_QTY_GT_ZERO",
                reason_detail="Fill ledger reports non-zero filled quantity.",
                evidence_refs=(evidence_ref,),
                upstream_refs=(upstream_ref,),
                emitted_by=emitted_by,
                stage_metrics={
                    "filled_qty": filled_qty,
                    "order_qty": order_qty if isinstance(order_qty, int) and order_qty > 0 else None,
                },
            )

    for sleeve_id in sorted(expected_sleeves):
        if sleeve_id in sleeves_with_events:
            continue
        opportunity_id = f"sleeve_not_run:{day}:{sleeve_id}"
        observed = max_observed_at
        _event_append(
            events=events,
            day_utc=day,
            environment=env,
            sleeve_id=sleeve_id,
            opportunity_id=opportunity_id,
            stage="sleeve_run",
            stage_status="SKIPPED",
            observed_at_utc=observed,
            reason_code="SLEEVE_NOT_RUN",
            reason_detail="No primary run/signal/intent evidence observed for expected sleeve.",
            evidence_refs=(),
            upstream_refs=(),
            emitted_by=emitted_by,
        )

    dedup: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = str(event.get("event_id") or "").strip()
        if event_id and event_id not in dedup:
            dedup[event_id] = event

    ordered_events = sorted(
        dedup.values(),
        key=lambda item: (
            str(item.get("observed_at_utc") or ""),
            str(item.get("sleeve_id") or ""),
            str(item.get("opportunity_id") or ""),
            str(item.get("lineage_stage") or ""),
            str(item.get("event_id") or ""),
        ),
    )
    event_paths: list[str] = []
    for event in ordered_events:
        written_path = _write_lineage_event_immutable(truth_root=root, payload=event)
        event_paths.append(str(written_path))

    return MaterializedLineageResultV1(
        day_utc=day,
        environment=env,
        event_count=len(event_paths),
        event_paths=tuple(event_paths),
        warnings=tuple(sorted(set(warnings))),
        evidence_cutoff_utc=max_observed_at,
    )


def read_lineage_events_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    root = (
        Path(truth_root).resolve()
        / "reports"
        / LINEAGE_EVENT_FAMILY
        / str(day_utc).strip()
    ).resolve()
    if not root.exists() or not root.is_dir():
        return []
    events: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/*/*/*/opportunity_lineage_event.v1.json")):
        payload = _read_json_object(path)
        validate_against_repo_schema_v1(payload, REPO_ROOT, LINEAGE_EVENT_SCHEMA_RELPATH)
        payload["_source_path"] = str(path.resolve())
        events.append(payload)
    return events


def classify_opportunity_from_lineage_events_v1(
    *,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ordered = sorted(
        (dict(item) for item in events),
        key=lambda row: (
            str(row.get("observed_at_utc") or ""),
            str(row.get("lineage_stage") or ""),
            str(row.get("event_id") or ""),
        ),
    )
    if not ordered:
        return {
            "final_classification": "ATTRIBUTION_INCOMPLETE",
            "completeness_status": "LATE_UPSTREAM",
            "farthest_stage": "",
            "root_reason_code": "ATTRIBUTION_EVIDENCE_GAP",
            "reason_codes": ["ATTRIBUTION_EVIDENCE_GAP"],
            "contradictory": False,
            "evidence_gap": True,
            "stage_counts": {},
        }

    stage_counts: Counter[str] = Counter()
    pass_flags = {stage: False for stage in LINEAGE_STAGES}
    reason_codes: set[str] = set()
    has_fail = False
    has_skipped = False
    filled_qty_max = 0
    order_qty_max = 0
    farthest_stage = ""
    stage_rank = {stage: index for index, stage in enumerate(LINEAGE_STAGES, start=1)}
    for event in ordered:
        stage = str(event.get("lineage_stage") or "").strip()
        status = str(event.get("stage_status") or "").strip().upper()
        if stage in LINEAGE_STAGES:
            stage_counts[stage] += 1
            if stage_rank.get(stage, 0) >= stage_rank.get(farthest_stage, 0):
                farthest_stage = stage
        if stage in pass_flags and status == "PASS":
            pass_flags[stage] = True
        if status == "FAIL":
            has_fail = True
        if status == "SKIPPED":
            has_skipped = True
        code = str(event.get("reason_code") or "").strip().upper()
        if code:
            reason_codes.add(code)
        metrics = event.get("stage_metrics") if isinstance(event.get("stage_metrics"), Mapping) else {}
        filled_qty = metrics.get("filled_qty")
        order_qty = metrics.get("order_qty")
        if isinstance(filled_qty, int) and filled_qty > filled_qty_max:
            filled_qty_max = filled_qty
        if isinstance(order_qty, int) and order_qty > order_qty_max:
            order_qty_max = order_qty

    contradictory = False
    if pass_flags["order_rejected"] and (pass_flags["fill_event"] or pass_flags["order_ack"]):
        contradictory = True
    if pass_flags["order_rejected"] and pass_flags["order_cancelled"]:
        contradictory = True

    evidence_gap = False
    if pass_flags["submit_attempted"] and not pass_flags["intent_created"]:
        evidence_gap = True
    if has_fail and not (
        pass_flags["submit_attempted"]
        or pass_flags["intent_created"]
        or pass_flags["fill_event"]
    ):
        evidence_gap = True

    if contradictory:
        classification = "ATTRIBUTION_INCOMPLETE"
        completeness = "CONTRADICTORY"
        root_reason = "ATTRIBUTION_CONTRADICTORY_EVIDENCE"
    elif filled_qty_max > 0 and order_qty_max > 0 and filled_qty_max >= order_qty_max:
        classification = "FILLED"
        completeness = "COMPLETE"
        root_reason = "FILLED"
    elif filled_qty_max > 0:
        classification = "PARTIAL_FILL"
        completeness = "COMPLETE"
        root_reason = "PARTIAL_FILL"
    elif pass_flags["order_cancelled"]:
        classification = "ORDER_CANCELLED"
        completeness = "COMPLETE"
        root_reason = "ORDER_CANCELLED"
    elif pass_flags["order_rejected"]:
        classification = "ORDER_REJECTED"
        completeness = "COMPLETE"
        root_reason = "ORDER_REJECTED"
    elif pass_flags["order_ack"]:
        classification = "ORDER_ACCEPTED_NO_FILL"
        completeness = "COMPLETE"
        root_reason = "ORDER_ACCEPTED_NO_FILL"
    elif pass_flags["submit_attempted"]:
        classification = "INTENT_SUBMITTED_NO_ORDER"
        completeness = "PARTIAL" if evidence_gap else "COMPLETE"
        if SUBMIT_DECISION_DRY_RUN_REASON_CODE in reason_codes:
            root_reason = SUBMIT_DECISION_DRY_RUN_REASON_CODE
        else:
            root_reason = "INTENT_SUBMITTED_NO_ORDER"
    elif pass_flags["intent_created"]:
        classification = "INTENT_CREATED_NOT_SUBMITTED"
        completeness = "COMPLETE"
        root_reason = "INTENT_CREATED_NOT_SUBMITTED"
    elif pass_flags["signal_filtered"]:
        classification = "RAN_SIGNAL_FILTERED"
        completeness = "COMPLETE"
        root_reason = "RAN_SIGNAL_FILTERED"
    elif pass_flags["sleeve_run"]:
        classification = "RAN_NO_SIGNAL"
        completeness = "COMPLETE"
        root_reason = "RAN_NO_SIGNAL"
    elif has_skipped and "SLEEVE_NOT_RUN" in reason_codes:
        classification = "SLEEVE_NOT_RUN"
        completeness = "COMPLETE"
        root_reason = "SLEEVE_NOT_RUN"
    elif evidence_gap:
        classification = "ATTRIBUTION_INCOMPLETE"
        completeness = "LATE_UPSTREAM"
        root_reason = "ATTRIBUTION_EVIDENCE_GAP"
    else:
        classification = "SLEEVE_NOT_RUN"
        completeness = "LATE_UPSTREAM"
        root_reason = "SLEEVE_NOT_RUN"

    if evidence_gap and completeness == "COMPLETE":
        completeness = "PARTIAL"
    if contradictory:
        reason_codes.add("ATTRIBUTION_CONTRADICTORY_EVIDENCE")
    elif evidence_gap:
        reason_codes.add("ATTRIBUTION_EVIDENCE_GAP")

    reason_codes.add(root_reason)
    if not farthest_stage:
        farthest_stage = ""
    return {
        "final_classification": classification,
        "completeness_status": completeness,
        "farthest_stage": farthest_stage,
        "root_reason_code": root_reason,
        "reason_codes": sorted(reason_codes),
        "contradictory": contradictory,
        "evidence_gap": evidence_gap,
        "stage_counts": dict(stage_counts),
    }


def _aggregate_completeness(values: Iterable[str]) -> str:
    seen = {str(item).strip().upper() for item in values if str(item).strip()}
    for status in reversed(COMPLETENESS_ORDER):
        if status in seen:
            return status
    return "LATE_UPSTREAM"


def _write_bytes_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(prefix=f".{path.name}.tmp.", dir=str(path.parent), delete=False) as tmp:
        tmp.write(payload)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _resolve_submit_decision_trace_for_opportunity(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    def _row_specificity(row: Mapping[str, Any]) -> int:
        decision = str(row.get("submit_decision") or "").strip().upper()
        reason = str(row.get("submit_decision_reason_code") or "").strip().upper()
        if decision == "ATTEMPTED":
            return 5
        if decision == "SKIPPED":
            if reason.startswith("AUTHZ_"):
                return 4
            if reason == "BUNDLE_B_HEADROOM_REJECTED":
                return 4
            return 3 if reason not in GENERIC_SKIP_REASON_CODES else 1
        return 0

    if not rows:
        return {
            "submit_decision": "",
            "submit_decision_reason_code": "",
            "submit_decision_reason_detail": "",
            "trace_conflict": False,
            "trace_refs": [],
        }
    ordered = sorted(
        (dict(item) for item in rows),
        key=lambda row: (
            str(row.get("observed_at_utc") or ""),
            str(row.get("decision_id") or ""),
        ),
    )
    attempted_rows = [
        row for row in ordered if str(row.get("submit_decision") or "").strip().upper() == "ATTEMPTED"
    ]
    if attempted_rows:
        chosen_attempt = max(
            attempted_rows,
            key=lambda row: (
                _row_specificity(row),
                str(row.get("observed_at_utc") or ""),
                str(row.get("decision_id") or ""),
            ),
        )
        chosen_ref = str(chosen_attempt.get("_source_path") or "").strip()
        return {
            "submit_decision": "ATTEMPTED",
            "submit_decision_reason_code": str(chosen_attempt.get("submit_decision_reason_code") or "").strip().upper(),
            "submit_decision_reason_detail": str(chosen_attempt.get("submit_decision_reason_detail") or "").strip(),
            "trace_conflict": False,
            "trace_refs": [chosen_ref] if chosen_ref else [],
        }
    decisions = {str(row.get("submit_decision") or "").strip().upper() for row in ordered if str(row.get("submit_decision") or "").strip()}
    reason_codes = {str(row.get("submit_decision_reason_code") or "").strip().upper() for row in ordered if str(row.get("submit_decision_reason_code") or "").strip()}
    concrete_decisions = {item for item in decisions if item and item != "UNKNOWN"}
    latest = ordered[-1]
    trace_conflict = False
    if "ATTEMPTED" in concrete_decisions and "SKIPPED" in concrete_decisions:
        trace_conflict = True
    elif len(concrete_decisions) > 1:
        trace_conflict = True
    if trace_conflict:
        return {
            "submit_decision": "UNKNOWN",
            "submit_decision_reason_code": "CONTRADICTORY_SUBMIT_DECISION_TRACE",
            "submit_decision_reason_detail": "Multiple conflicting submit decision traces exist for this opportunity.",
            "trace_conflict": True,
            "trace_refs": sorted(
                str(row.get("_source_path") or "").strip() for row in ordered if str(row.get("_source_path") or "").strip()
            ),
        }
    if concrete_decisions:
        chosen = "ATTEMPTED" if "ATTEMPTED" in concrete_decisions else sorted(concrete_decisions)[-1]
        chosen_rows = [row for row in ordered if str(row.get("submit_decision") or "").strip().upper() == chosen]
        latest = (
            max(
                chosen_rows,
                key=lambda row: (
                    _row_specificity(row),
                    str(row.get("observed_at_utc") or ""),
                    str(row.get("decision_id") or ""),
                ),
            )
            if chosen_rows
            else latest
        )
    elif len(reason_codes) > 1:
        return {
            "submit_decision": "UNKNOWN",
            "submit_decision_reason_code": "CONTRADICTORY_SUBMIT_DECISION_TRACE",
            "submit_decision_reason_detail": "Multiple conflicting UNKNOWN submit decision traces exist for this opportunity.",
            "trace_conflict": True,
            "trace_refs": sorted(
                str(row.get("_source_path") or "").strip() for row in ordered if str(row.get("_source_path") or "").strip()
            ),
        }

    latest_ref = str(latest.get("_source_path") or "").strip()
    return {
        "submit_decision": str(latest.get("submit_decision") or "").strip().upper(),
        "submit_decision_reason_code": str(latest.get("submit_decision_reason_code") or "").strip().upper(),
        "submit_decision_reason_detail": str(latest.get("submit_decision_reason_detail") or "").strip(),
        "trace_conflict": False,
        "trace_refs": [latest_ref] if latest_ref else [],
    }


def write_sleeve_intent_trade_attribution_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    environment: str = "PAPER",
    report_generated_at_utc: str | None = None,
    evidence_cutoff_utc: str | None = None,
    emitted_by: str = "constellation_2.common.opportunity_lineage_attribution_v1",
) -> AttributionWriteResultV1:
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    env = str(environment).strip().upper() or "PAPER"
    events = read_lineage_events_v1(truth_root=root, day_utc=day)
    submit_decision_rows = read_submit_decision_traces_v1(truth_root=root, day_utc=day)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    submit_decision_grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    all_observed_at = []
    for event in events:
        sleeve_id = str(event.get("sleeve_id") or "").strip() or "UNKNOWN_ENGINE"
        opportunity_id = str(event.get("opportunity_id") or "").strip()
        grouped[(sleeve_id, opportunity_id)].append(event)
        observed = str(event.get("observed_at_utc") or "").strip()
        if observed:
            all_observed_at.append(observed)
    for row in submit_decision_rows:
        sleeve_id = str(row.get("sleeve_id") or "").strip() or "UNKNOWN_ENGINE"
        opportunity_id = str(row.get("opportunity_id") or "").strip()
        submit_decision_grouped[(sleeve_id, opportunity_id)].append(row)
        observed = str(row.get("observed_at_utc") or "").strip()
        if observed:
            all_observed_at.append(observed)

    default_cutoff = max(all_observed_at) if all_observed_at else _day_start_iso(day)
    cutoff = _as_iso_utc(evidence_cutoff_utc or default_cutoff, default_cutoff)
    generated_at = _as_iso_utc(report_generated_at_utc or cutoff, cutoff)

    opportunity_rows: list[dict[str, Any]] = []
    by_sleeve_counts: dict[str, Counter[str]] = defaultdict(Counter)
    completeness_values: list[str] = []
    for (sleeve_id, opportunity_id), rows in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        classification = classify_opportunity_from_lineage_events_v1(events=rows)
        final_classification = str(classification["final_classification"]).strip()
        completeness = str(classification["completeness_status"]).strip().upper() or "LATE_UPSTREAM"
        root_reason_code = str(classification["root_reason_code"]).strip()
        reason_codes = set(str(code).strip().upper() for code in list(classification["reason_codes"]) if str(code).strip())
        submit_trace = _resolve_submit_decision_trace_for_opportunity(
            submit_decision_grouped.get((sleeve_id, opportunity_id), [])
        )
        submit_decision = str(submit_trace["submit_decision"]).strip().upper()
        submit_decision_reason_code = str(submit_trace["submit_decision_reason_code"]).strip().upper()
        submit_decision_reason_detail = str(submit_trace["submit_decision_reason_detail"]).strip()
        trace_conflict = bool(submit_trace["trace_conflict"])
        if final_classification == "INTENT_CREATED_NOT_SUBMITTED":
            if trace_conflict:
                final_classification = "ATTRIBUTION_INCOMPLETE"
                completeness = "CONTRADICTORY"
                root_reason_code = "CONTRADICTORY_SUBMIT_DECISION_TRACE"
                reason_codes.add("CONTRADICTORY_SUBMIT_DECISION_TRACE")
            elif submit_decision == "ATTEMPTED":
                final_classification = "INTENT_SUBMITTED_NO_ORDER"
                completeness = "PARTIAL"
                if submit_decision_reason_code == SUBMIT_DECISION_DRY_RUN_REASON_CODE:
                    root_reason_code = SUBMIT_DECISION_DRY_RUN_REASON_CODE
                else:
                    root_reason_code = "INTENT_SUBMITTED_NO_ORDER"
                reason_codes.add("INTENT_SUBMITTED_NO_ORDER")
                reason_codes.add("SUBMIT_DECISION_ATTEMPTED")
                if submit_decision_reason_code:
                    reason_codes.add(submit_decision_reason_code)
            elif submit_decision == "SKIPPED":
                completeness = "COMPLETE"
                root_reason_code = submit_decision_reason_code or "SUBMISSION_BOUNDARY_DENIED"
                if submit_decision_reason_code:
                    reason_codes.add(submit_decision_reason_code)
            elif submit_decision == "UNKNOWN":
                completeness = "PARTIAL"
                root_reason_code = submit_decision_reason_code or "NO_SUBMIT_DECISION_EVIDENCE"
                reason_codes.add(root_reason_code)
                reason_codes.add("ATTRIBUTION_EVIDENCE_GAP")
            else:
                final_classification = "ATTRIBUTION_INCOMPLETE"
                completeness = "LATE_UPSTREAM"
                root_reason_code = "SUBMIT_DECISION_TRACE_MISSING"
                reason_codes.add("SUBMIT_DECISION_TRACE_MISSING")
                reason_codes.add("ATTRIBUTION_EVIDENCE_GAP")
        if root_reason_code:
            reason_codes.add(root_reason_code)
        completeness_values.append(completeness)
        by_sleeve_counts[sleeve_id][final_classification] += 1
        evidence_refs: list[dict[str, str]] = []
        seen_paths: set[str] = set()
        for row in rows:
            for ref in row.get("evidence_refs") if isinstance(row.get("evidence_refs"), list) else []:
                if not isinstance(ref, Mapping):
                    continue
                path = str(ref.get("artifact_path") or "").strip()
                if not path or path in seen_paths:
                    continue
                seen_paths.add(path)
                evidence_refs.append(
                    {
                        "artifact_type": str(ref.get("artifact_type") or "").strip(),
                        "artifact_path": path,
                        "artifact_sha256": str(ref.get("artifact_sha256") or "").strip(),
                    }
                )
        for trace_path in submit_trace["trace_refs"]:
            path = str(trace_path).strip()
            if not path or path in seen_paths:
                continue
            trace_ref_path = Path(path).resolve()
            if not trace_ref_path.exists() or not trace_ref_path.is_file():
                continue
            seen_paths.add(path)
            evidence_refs.append(
                {
                    "artifact_type": "submit_decision_trace_v1",
                    "artifact_path": path,
                    "artifact_sha256": _sha256_file(trace_ref_path),
                }
            )
        opportunity_rows.append(
            {
                "sleeve_id": sleeve_id,
                "opportunity_id": opportunity_id,
                "final_classification": final_classification,
                "farthest_stage": str(classification["farthest_stage"]),
                "completeness_status": completeness,
                "root_reason_code": root_reason_code,
                "reason_codes": sorted(reason_codes),
                "stage_counts": dict(classification["stage_counts"]),
                "event_count": len(rows),
                "evidence_refs": evidence_refs,
                "submit_decision": submit_decision or "UNKNOWN",
                "submit_decision_reason_code": submit_decision_reason_code or "NO_SUBMIT_DECISION_TRACE",
                "submit_decision_reason_detail": submit_decision_reason_detail,
            }
        )

    if not opportunity_rows:
        completeness_values.append("LATE_UPSTREAM")

    classification_counts: Counter[str] = Counter(
        str(row["final_classification"]) for row in opportunity_rows
    )
    sleeve_rows = []
    for sleeve_id in sorted(by_sleeve_counts):
        counter = by_sleeve_counts[sleeve_id]
        sleeve_rows.append(
            {
                "sleeve_id": sleeve_id,
                "opportunity_count": int(sum(counter.values())),
                "classification_counts": dict(sorted(counter.items())),
            }
        )

    active_session_path = (root / "active_session_v1" / "current.json").resolve()
    session_context = {
        "active_session_path": str(active_session_path),
        "active_day": "",
        "target_day": "",
    }
    if active_session_path.exists() and active_session_path.is_file():
        active_payload = _read_json_object(active_session_path)
        session_context["active_day"] = str(active_payload.get("active_day") or "").strip()
        session_context["target_day"] = str(active_payload.get("target_day") or "").strip()

    payload: dict[str, Any] = {
        "schema_id": "sleeve_intent_trade_attribution",
        "schema_version": "v1",
        "artifact_id": ATTRIBUTION_ARTIFACT_ID,
        "day_utc": day,
        "environment": env,
        "report_generated_at_utc": generated_at,
        "evidence_cutoff_utc": cutoff,
        "session_context": session_context,
        "day_summary": {
            "opportunity_count": len(opportunity_rows),
            "classification_counts": dict(sorted(classification_counts.items())),
        },
        "sleeves": sleeve_rows,
        "opportunities": opportunity_rows,
        "completeness_status": _aggregate_completeness(completeness_values),
        "global_warnings": (
            ["ATTRIBUTION_EVIDENCE_GAP"] if not opportunity_rows else []
        ),
        "evidence_status": {
            "lineage_event_count": len(events),
            "lineage_root": str((root / "reports" / LINEAGE_EVENT_FAMILY / day).resolve()),
            "submit_decision_trace_count": len(submit_decision_grouped),
            "submit_decision_trace_root": str((root / "reports" / SUBMIT_DECISION_TRACE_FAMILY / day).resolve()),
            "primary_evidence_only": True,
            "derived_sources_used": [],
        },
        "producer": {
            "module": emitted_by,
            "repo": REPO_ROOT.name,
        },
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    validate_against_repo_schema_v1(payload, REPO_ROOT, ATTRIBUTION_SCHEMA_RELPATH)
    payload_bytes = canonical_json_bytes_v1(payload) + b"\n"
    payload_sha = _sha256_bytes(payload_bytes)

    versioned_path = (
        root
        / "reports"
        / ATTRIBUTION_FAMILY
        / day
        / payload_sha
        / "sleeve_intent_trade_attribution.v1.json"
    ).resolve()
    versioned_path.parent.mkdir(parents=True, exist_ok=True)
    if not versioned_path.exists():
        with NamedTemporaryFile(prefix=f".{versioned_path.name}.tmp.", dir=str(versioned_path.parent), delete=False) as tmp:
            tmp.write(payload_bytes)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        tmp_path.replace(versioned_path)

    report_path = (
        root
        / "reports"
        / ATTRIBUTION_FAMILY
        / day
        / "sleeve_intent_trade_attribution.v1.json"
    ).resolve()
    if report_path.exists():
        existing = report_path.read_bytes()
        if _sha256_bytes(existing) != payload_sha:
            _write_bytes_replace(report_path, payload_bytes)
    else:
        _write_bytes_replace(report_path, payload_bytes)

    return AttributionWriteResultV1(
        day_utc=day,
        environment=env,
        report_path=str(report_path),
        versioned_report_path=str(versioned_path),
        report_sha256=payload_sha,
        event_count=len(events),
        opportunity_count=len(opportunity_rows),
        completeness_status=str(payload["completeness_status"]),
        classification_counts=dict(sorted(classification_counts.items())),
    )


def materialize_lineage_and_write_attribution_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    environment: str = "PAPER",
    emitted_by: str = "ops/tools/run_sleeve_intent_trade_attribution_v1.py",
) -> tuple[MaterializedLineageResultV1, AttributionWriteResultV1]:
    submit_trace = materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
        emitted_by=emitted_by,
    )
    lineage = materialize_opportunity_lineage_events_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
        emitted_by=emitted_by,
    )
    cutoff = max(str(lineage.evidence_cutoff_utc), str(submit_trace.evidence_cutoff_utc))
    attribution = write_sleeve_intent_trade_attribution_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
        evidence_cutoff_utc=cutoff,
        emitted_by=emitted_by,
    )
    return lineage, attribution
