#!/usr/bin/env python3
"""
run_execution_stream_snapshot_day_v1.py

Bundle 1 / Execution Observer (pull snapshot) v1 — hardened.

Writes:
  <TRUTH_ROOT>/execution_stream_v1/<DAY>/
    <event_hash>.execution_event_stream_record.v1.json

On failure, writes governed failure artifact:
  <TRUTH_ROOT>/execution_stream_v1/failures/<DAY>/failure.json
Schema:
  governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_failure.v1.schema.json

Fail-closed:
- ib_insync missing
- broker connect/pull failure (timeouts, disconnects)
- submissions day dir missing
- any broker order cannot be attributed to known submission evidence
- schema validation failure
- overwrite attempts with different bytes
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line
from constellation_2.common.execution_status_normalization_v1 import (
    UNKNOWN_BROKER_ORDER_STATUS,
    normalize_broker_order_status_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA_STREAM = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json"
SCHEMA_FAILURE = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_failure.v1.schema.json"
REPLAY_MODE_DEFAULT = "default"
REPLAY_MODE_GOVERNED_NEW_ATTEMPT = "governed_new_attempt"
ATTRIBUTION_METHOD_DIRECT_BROKER_IDS = "DIRECT_BROKER_IDS"
ATTRIBUTION_METHOD_PERM_ID_BRIDGE = "PERM_ID_BRIDGE"
ATTRIBUTION_METHOD_POST_HANDOFF_ORPHAN_FALLBACK = "POST_HANDOFF_ORPHAN_FALLBACK"
ATTRIBUTION_METHOD_SYNTHETIC_SUBMISSION_SNAPSHOT = "SYNTHETIC_SUBMISSION_SNAPSHOT"
ATTRIBUTION_CONFIDENCE_HIGH = "HIGH"
ATTRIBUTION_CONFIDENCE_MEDIUM = "MEDIUM"
ATTRIBUTION_CONFIDENCE_EXACT_SINGLE_MATCH = "EXACT_SINGLE_MATCH"

SUBMISSIONS_DAY_ROOT: Path | None = None
OUT_ROOT: Path | None = None
FAIL_ROOT: Path | None = None


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --truth_root: {p}")
    return p


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _sha256_dir_deterministic(root: Path) -> str:
    if not root.exists() or not root.is_dir():
        return _sha256_bytes(b"")
    items: List[Tuple[str, str]] = []
    for p in root.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(root)).replace("\\", "/")
            items.append((rel, _sha256_file(p)))
    items.sort(key=lambda x: x[0])
    h = hashlib.sha256()
    for rel, fsha in items:
        h.update(rel.encode("utf-8"))
        h.update(b"\n")
        h.update(fsha.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _normalize_side(raw: Any) -> str:
    side = str(raw or "").strip().upper()
    if side in {"BUY", "BOT"}:
        return "BUY"
    if side in {"SELL", "SLD"}:
        return "SELL"
    return side


def _build_order_state_v1(
    *,
    raw_broker_status: str,
    filled_qty: int,
    remaining_qty: int,
    avg_fill_price: str,
) -> Dict[str, Any]:
    normalized = normalize_broker_order_status_v1(raw_broker_status)
    return {
        "status": normalized.normalized_lifecycle_status,
        "raw_broker_status": normalized.raw_broker_status,
        "normalized_lifecycle_status": normalized.normalized_lifecycle_status,
        "is_terminal": bool(normalized.is_terminal),
        "filled_qty": int(filled_qty),
        "remaining_qty": int(remaining_qty),
        "avg_fill_price": str(avg_fill_price),
    }


def _fallback_match_key(*, symbol: str, action: str, qty: int) -> str:
    return f"{str(symbol).strip().upper()}|{_normalize_side(action)}|{int(qty)}"


def _orphan_submission_is_post_handoff(subdir: Path) -> bool:
    veto_p = (subdir / "veto_record.v1.json").resolve()
    if not veto_p.exists():
        return False
    veto = _read_json_obj(veto_p)
    reason_code = str(veto.get("reason_code") or "").strip()
    reason_detail = str(veto.get("reason_detail") or "").strip()
    return (
        reason_code == "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
        and "broker_submission_record.v2.schema.json" in reason_detail
    )


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_immutable(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload):
            return
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    import os
    os.replace(tmp, path)


def _event_hash_key(fields: List[str]) -> str:
    h = hashlib.sha256()
    for f in fields:
        h.update(f.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _write_failure(
    *,
    day: str,
    produced_utc: str,
    code: str,
    message: str,
    details: Dict[str, Any],
    input_manifest: List[Dict[str, Any]],
    attempted_outputs: List[Dict[str, Any]],
    failure_path_override: Path | None = None,
) -> None:
    fail_obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_EVIDENCE_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation", "git_sha": _git_sha(), "module": "ops/tools/run_execution_stream_snapshot_day_v1.py"},
        "status": "FAIL_CORRUPT_INPUTS",
        "reason_codes": [code],
        "input_manifest": list(input_manifest),
        "failure": {
            "code": code,
            "message": message,
            "details": dict(details),
            "attempted_outputs": list(attempted_outputs),
        },
    }
    validate_against_repo_schema_v1(fail_obj, REPO_ROOT, SCHEMA_FAILURE)
    payload = canonical_json_bytes_v1(fail_obj) + b"\n"
    out_path = (
        failure_path_override.resolve()
        if failure_path_override is not None
        else (FAIL_ROOT / day / "failure.json").resolve()
    )
    _write_immutable(out_path, payload)


def _build_replay_id(now_utc: str) -> str:
    normalized = str(now_utc).strip().replace(":", "").replace("-", "").replace("T", "_").replace("Z", "")
    if not normalized:
        normalized = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"replay_{normalized}"


def _list_submission_dirs(day: str) -> List[Path]:
    if SUBMISSIONS_DAY_ROOT is None:
        raise RuntimeError("SUBMISSIONS_DAY_ROOT_UNSET")
    d = (SUBMISSIONS_DAY_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        raise RuntimeError(f"MISSING_SUBMISSIONS_DAY_DIR: {d}")
    return sorted([p for p in d.iterdir() if p.is_dir()])


def _order_qty_from_submission_dir(subdir: Path) -> int:
    plan_v2_p = (subdir / "equity_order_plan.v2.json").resolve()
    if plan_v2_p.exists():
        plan = _read_json_obj(plan_v2_p)
        qty_any = plan.get("qty_shares")
        if not isinstance(qty_any, int) or qty_any <= 0:
            raise RuntimeError(f"EQUITY_ORDER_PLAN_V2_QTY_INVALID: {plan_v2_p}")
        return int(qty_any)

    plan_p = (subdir / "equity_order_plan.v1.json").resolve()
    if plan_p.exists():
        plan = _read_json_obj(plan_p)
        qty_any = plan.get("qty_shares")
        if not isinstance(qty_any, int) or qty_any <= 0:
            raise RuntimeError(f"EQUITY_ORDER_PLAN_QTY_INVALID: {plan_p}")
        return int(qty_any)

    order_plan_p = (subdir / "order_plan.v1.json").resolve()
    if order_plan_p.exists():
        plan = _read_json_obj(order_plan_p)
        risk = plan.get("risk_proof") if isinstance(plan.get("risk_proof"), dict) else {}
        qty_any = risk.get("contracts")
        if not isinstance(qty_any, int) or qty_any <= 0:
            raise RuntimeError(f"OPTIONS_ORDER_PLAN_CONTRACTS_INVALID: {order_plan_p}")
        return int(qty_any)

    raise RuntimeError(f"MISSING_SUPPORTED_PLAN_FOR_ORDER_QTY: {subdir}")


def _submission_meta_rows(day: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for subdir in _list_submission_dirs(day):
        bsr_p = subdir / "broker_submission_record.v2.json"
        has_broker_submission = bsr_p.exists()
        bsr: Dict[str, Any] = _read_json_obj(bsr_p) if has_broker_submission else {}

        submission_id = str(bsr.get("submission_id") or "").strip() or subdir.name
        binding_hash = str(bsr.get("binding_hash") or "").strip()
        if not binding_hash:
            binding_p = (subdir / "binding_record.v2.json").resolve()
            if binding_p.exists():
                binding = _read_json_obj(binding_p)
                binding_hash = str(binding.get("canonical_json_hash") or "").strip()
        broker = bsr.get("broker") if isinstance(bsr.get("broker"), dict) else {}
        env = str(broker.get("environment") or "PAPER").strip()
        submitted_at_utc = str(bsr.get("submitted_at_utc") or "").strip()
        status = str(bsr.get("status") or "UNKNOWN").strip().upper() or "UNKNOWN"

        engine_id = ""
        source_intent_id = ""
        intent_sha256 = ""
        symbol = ""
        action = ""

        evt_p = subdir / "execution_event_record.v1.json"
        if evt_p.exists():
            evt = _read_json_obj(evt_p)
            engine_id = str(evt.get("engine_id") or "").strip()
            source_intent_id = str(evt.get("source_intent_id") or "").strip()
            intent_sha256 = str(evt.get("intent_sha256") or "").strip()

        order_qty = 0
        plan_p = subdir / "equity_order_plan.v2.json"
        if plan_p.exists():
            plan = _read_json_obj(plan_p)
            engine_id = engine_id or str(plan.get("engine_id") or "").strip()
            source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
            intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()
            symbol = str(plan.get("symbol") or "").strip().upper()
            action = _normalize_side(plan.get("action"))
        else:
            plan_p = subdir / "equity_order_plan.v1.json"
        if plan_p.exists():
            plan = _read_json_obj(plan_p)
            engine_id = engine_id or str(plan.get("engine_id") or "").strip()
            source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
            intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()
            symbol = symbol or str(plan.get("symbol") or "").strip().upper()
            action = action or _normalize_side(plan.get("action"))
        else:
            plan_p = subdir / "order_plan.v1.json"
            if plan_p.exists():
                plan = _read_json_obj(plan_p)
                engine_id = engine_id or str(plan.get("engine_id") or "").strip()
                source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
                intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()

        try:
            order_qty = _order_qty_from_submission_dir(subdir)
        except RuntimeError:
            order_qty = 0

        broker_ids = bsr.get("broker_ids") if isinstance(bsr.get("broker_ids"), dict) else {}
        eligible_orphan_fallback = (not has_broker_submission) and _orphan_submission_is_post_handoff(subdir)
        rows.append({
            "submission_id": submission_id,
            "submission_record_path": str(bsr_p.resolve()),
            "binding_hash": binding_hash,
            "engine_id": engine_id,
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "symbol": symbol,
            "action": action,
            "broker_env": env,
            "submitted_at_utc": submitted_at_utc,
            "broker_status": status,
            "order_id": broker_ids.get("order_id"),
            "perm_id": broker_ids.get("perm_id"),
            "order_qty": order_qty,
            "eligible_orphan_fallback": eligible_orphan_fallback,
        })
    return rows


def _build_orderid_index(day: str) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for meta in _submission_meta_rows(day):
        order_id = meta.get("order_id")
        perm_id = meta.get("perm_id")
        base = {
            "submission_id": meta["submission_id"],
            "binding_hash": meta["binding_hash"],
            "engine_id": meta["engine_id"],
            "source_intent_id": meta["source_intent_id"],
            "intent_sha256": meta["intent_sha256"],
            "broker_env": meta["broker_env"],
        }
        if isinstance(order_id, int) and order_id >= 0:
            idx[f"order_id:{order_id}"] = base
        if isinstance(perm_id, int) and perm_id >= 0:
            idx[f"perm_id:{perm_id}"] = base
    return idx


def _build_orphan_fallback_index(day: str) -> Dict[str, List[Dict[str, Any]]]:
    idx: Dict[str, List[Dict[str, Any]]] = {}
    for meta in _submission_meta_rows(day):
        if not bool(meta.get("eligible_orphan_fallback")):
            continue
        symbol = str(meta.get("symbol") or "").strip().upper()
        action = _normalize_side(meta.get("action"))
        order_qty = meta.get("order_qty")
        if not symbol or not action or not isinstance(order_qty, int) or order_qty <= 0:
            continue
        idx.setdefault(_fallback_match_key(symbol=symbol, action=action, qty=order_qty), []).append({
            "submission_id": meta["submission_id"],
            "binding_hash": meta["binding_hash"],
            "engine_id": meta["engine_id"],
            "source_intent_id": meta["source_intent_id"],
            "intent_sha256": meta["intent_sha256"],
            "broker_env": meta["broker_env"],
        })
    return idx


def _build_perm_id_bridge_candidates(
    *,
    day: str,
    truth_root: Path,
    submission_meta_rows: List[Dict[str, Any]],
) -> Tuple[Dict[int, List[Dict[str, Any]]], str]:
    candidates_by_perm_id: Dict[int, List[Dict[str, Any]]] = {}
    submission_index_path = (Path(truth_root).resolve() / "submission_index_v1" / day / "submission_index.v1.json").resolve()
    if not submission_index_path.exists() or not submission_index_path.is_file():
        return candidates_by_perm_id, f"SUBMISSION_INDEX_MISSING:{submission_index_path}"
    try:
        submission_index_obj = _read_json_obj(submission_index_path)
    except Exception as exc:  # noqa: BLE001
        return candidates_by_perm_id, (
            f"SUBMISSION_INDEX_INVALID:{submission_index_path}:{type(exc).__name__}:{exc}"
        )
    attempts = submission_index_obj.get("attempts")
    if not isinstance(attempts, list):
        return candidates_by_perm_id, f"SUBMISSION_INDEX_INVALID_ATTEMPTS:{submission_index_path}"

    by_submission_id: Dict[str, List[Dict[str, Any]]] = {}
    by_submission_record_path: Dict[str, List[Dict[str, Any]]] = {}
    for row in submission_meta_rows:
        submission_id = str(row.get("submission_id") or "").strip()
        if submission_id:
            by_submission_id.setdefault(submission_id, []).append(row)
        submission_record_path = str(row.get("submission_record_path") or "").strip()
        if submission_record_path:
            by_submission_record_path.setdefault(str(Path(submission_record_path).resolve()), []).append(row)

    for attempt_row in attempts:
        if not isinstance(attempt_row, dict):
            continue
        broker_perm_id = attempt_row.get("broker_perm_id")
        if not isinstance(broker_perm_id, int) or broker_perm_id == 0:
            continue
        attempt_id = str(attempt_row.get("attempt_id") or "").strip()
        if not attempt_id:
            continue
        submission_record_path = str(attempt_row.get("submission_record_path") or "").strip()
        raw_candidates: List[Dict[str, Any]] = []
        raw_candidates.extend(by_submission_id.get(attempt_id, []))
        if submission_record_path:
            raw_candidates.extend(
                by_submission_record_path.get(str(Path(submission_record_path).resolve()), [])
            )
        unique_candidates: Dict[str, Dict[str, Any]] = {}
        for row in raw_candidates:
            candidate_submission_id = str(row.get("submission_id") or "").strip()
            if candidate_submission_id:
                unique_candidates[candidate_submission_id] = row
        for row in unique_candidates.values():
            candidates_by_perm_id.setdefault(broker_perm_id, []).append(
                {
                    "submission_id": row["submission_id"],
                    "binding_hash": row["binding_hash"],
                    "engine_id": row["engine_id"],
                    "source_intent_id": row["source_intent_id"],
                    "intent_sha256": row["intent_sha256"],
                    "broker_env": row["broker_env"],
                    "attempt_id": attempt_id,
                    "bridge_source_path": str(submission_index_path),
                    "broker_order_id": attempt_row.get("broker_order_id"),
                    "broker_perm_id": attempt_row.get("broker_perm_id"),
                }
            )
    for broker_perm_id, rows in list(candidates_by_perm_id.items()):
        deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows:
            deduped[(str(row.get("submission_id") or ""), str(row.get("attempt_id") or ""))] = row
        candidates_by_perm_id[broker_perm_id] = list(deduped.values())
    return candidates_by_perm_id, ""


def _build_event_attribution(
    *,
    raw_order_id: Optional[int],
    raw_perm_id: Optional[int],
    attribution_method: str,
    attribution_confidence: str,
    attempt_id: str = "",
    matched_attempt_id: str = "",
    bridge_source_path: str = "",
) -> Dict[str, Any]:
    attribution: Dict[str, Any] = {
        "raw_order_id": raw_order_id if isinstance(raw_order_id, int) else None,
        "raw_perm_id": raw_perm_id if isinstance(raw_perm_id, int) else None,
        "attribution_method": str(attribution_method).strip(),
        "attribution_confidence": str(attribution_confidence).strip(),
    }
    if attempt_id:
        attribution["attempt_id"] = attempt_id
    if matched_attempt_id:
        attribution["matched_attempt_id"] = matched_attempt_id
    if bridge_source_path:
        attribution["bridge_source_path"] = bridge_source_path
    return attribution


def _resolve_submission_meta_for_event(
    *,
    event_type: str,
    idx: Dict[str, Dict[str, Any]],
    orphan_fallback_idx: Dict[str, List[Dict[str, Any]]],
    orphan_claims: Dict[str, Tuple[Optional[int], Optional[int]]],
    perm_id_bridge_candidates: Dict[int, List[Dict[str, Any]]],
    perm_id_bridge_error: str,
    order_id: Optional[int],
    perm_id: Optional[int],
    symbol: str = "",
    action: str = "",
    order_qty: Optional[int] = None,
) -> Tuple[Dict[str, Any], List[str], Dict[str, Any]]:
    if isinstance(order_id, int) and order_id != 0:
        order_key = f"order_id:{order_id}"
        if order_key in idx:
            return (
                idx[order_key],
                [],
                _build_event_attribution(
                    raw_order_id=order_id,
                    raw_perm_id=perm_id,
                    attribution_method=ATTRIBUTION_METHOD_DIRECT_BROKER_IDS,
                    attribution_confidence=ATTRIBUTION_CONFIDENCE_HIGH,
                ),
            )

    if isinstance(order_id, int) and order_id == 0 and isinstance(perm_id, int) and perm_id != 0:
        perm_key = f"perm_id:{perm_id}"
        if perm_key in idx:
            return (
                idx[perm_key],
                [],
                _build_event_attribution(
                    raw_order_id=order_id,
                    raw_perm_id=perm_id,
                    attribution_method=ATTRIBUTION_METHOD_DIRECT_BROKER_IDS,
                    attribution_confidence=ATTRIBUTION_CONFIDENCE_HIGH,
                ),
            )
        if perm_id_bridge_error:
            raise RuntimeError(
                "UNATTRIBUTABLE_BROKER_EVENT: "
                f"event_type={event_type} order_id={order_id} perm_id={perm_id} "
                f"perm_bridge_error={perm_id_bridge_error}"
            )
        bridge_matches = perm_id_bridge_candidates.get(perm_id, [])
        if len(bridge_matches) == 0:
            raise RuntimeError(
                "UNATTRIBUTABLE_BROKER_EVENT: "
                f"event_type={event_type} order_id={order_id} perm_id={perm_id} "
                f"perm_bridge_match_count={len(bridge_matches)}"
            )
        if len(bridge_matches) > 1:
            raise RuntimeError(
                "AMBIGUOUS_BROKER_EVENT_ATTRIBUTION: "
                f"event_type={event_type} order_id={order_id} perm_id={perm_id} "
                f"perm_bridge_match_count={len(bridge_matches)}"
            )
        bridge_meta = bridge_matches[0]
        return (
            bridge_meta,
            ["ATTRIBUTED_BY_PERM_ID_BRIDGE"],
            _build_event_attribution(
                raw_order_id=order_id,
                raw_perm_id=perm_id,
                attribution_method=ATTRIBUTION_METHOD_PERM_ID_BRIDGE,
                attribution_confidence=ATTRIBUTION_CONFIDENCE_EXACT_SINGLE_MATCH,
                attempt_id=str(bridge_meta.get("attempt_id") or ""),
                matched_attempt_id=str(bridge_meta.get("attempt_id") or ""),
                bridge_source_path=str(bridge_meta.get("bridge_source_path") or ""),
            ),
        )

    if isinstance(perm_id, int):
        perm_key = f"perm_id:{perm_id}"
        if perm_key in idx:
            return (
                idx[perm_key],
                [],
                _build_event_attribution(
                    raw_order_id=order_id,
                    raw_perm_id=perm_id,
                    attribution_method=ATTRIBUTION_METHOD_DIRECT_BROKER_IDS,
                    attribution_confidence=ATTRIBUTION_CONFIDENCE_HIGH,
                ),
            )

    if isinstance(order_id, int):
        order_key = f"order_id:{order_id}"
        if order_key in idx:
            return (
                idx[order_key],
                [],
                _build_event_attribution(
                    raw_order_id=order_id,
                    raw_perm_id=perm_id,
                    attribution_method=ATTRIBUTION_METHOD_DIRECT_BROKER_IDS,
                    attribution_confidence=ATTRIBUTION_CONFIDENCE_HIGH,
                ),
            )

    raise RuntimeError(f"UNATTRIBUTABLE_BROKER_EVENT: event_type={event_type} order_id={order_id} perm_id={perm_id}")


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_execution_stream_snapshot_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--truth_root", required=True, help="Authoritative runtime truth root")
    ap.add_argument("--ib_host", default="127.0.0.1")
    ap.add_argument("--ib_port", type=int, default=4002)
    ap.add_argument("--ib_client_id", type=int, default=7)
    ap.add_argument(
        "--replay_mode",
        default=REPLAY_MODE_DEFAULT,
        choices=[REPLAY_MODE_DEFAULT, REPLAY_MODE_GOVERNED_NEW_ATTEMPT],
    )
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"
    observed_at = _now_iso_z()

    truth_root = _require_truth_root(args.truth_root)
    replay_mode = str(args.replay_mode or REPLAY_MODE_DEFAULT).strip().lower()
    replay_enabled = replay_mode == REPLAY_MODE_GOVERNED_NEW_ATTEMPT
    replay_id = _build_replay_id(observed_at) if replay_enabled else ""
    global SUBMISSIONS_DAY_ROOT, OUT_ROOT, FAIL_ROOT
    SUBMISSIONS_DAY_ROOT = (truth_root / "execution_evidence_v1" / "submissions").resolve()
    OUT_ROOT = (truth_root / "execution_stream_v1").resolve()
    FAIL_ROOT = (OUT_ROOT / "failures").resolve()

    out_day = (
        (OUT_ROOT / "replays" / day / replay_id).resolve()
        if replay_enabled
        else (OUT_ROOT / day).resolve()
    )
    out_day.mkdir(parents=True, exist_ok=True)
    replay_artifact_path = (out_day / "execution_stream_snapshot.v1.json").resolve() if replay_enabled else None
    replay_failure_override = (out_day / "failure.json").resolve() if replay_enabled else None
    source_failure_artifact_path = (OUT_ROOT / "failures" / day / "failure.json").resolve()
    attempted_outputs = [{"path": str(out_day), "sha256": None}]
    input_manifest: List[Dict[str, Any]] = []
    replay_raw_broker_status = ""
    replay_normalized_lifecycle_status = ""
    replay_submission_record_path = ""

    def emit_replay_summary(status_value: str) -> None:
        if not replay_enabled or replay_artifact_path is None:
            return
        payload = {
            "schema_version": "execution_stream_snapshot_replay.v1",
            "day": day,
            "replay_id": replay_id,
            "source_failure_artifact_path": str(source_failure_artifact_path),
            "source_submission_record_path": replay_submission_record_path,
            "raw_broker_status": replay_raw_broker_status,
            "normalized_lifecycle_status": replay_normalized_lifecycle_status,
            "immutable_history_preserved": True,
            "status": str(status_value).strip().upper() or "FAIL",
            "generated_at_utc": observed_at,
        }
        replay_artifact_path.parent.mkdir(parents=True, exist_ok=True)
        replay_artifact_path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    # Inputs: phaseD submissions root + day dir hash
    day_dir = (SUBMISSIONS_DAY_ROOT / day).resolve()
    subs_root_sha = _sha256_dir_deterministic(SUBMISSIONS_DAY_ROOT)
    input_manifest.append({"type": "phaseD_submissions_root", "path": str(SUBMISSIONS_DAY_ROOT), "sha256": subs_root_sha, "day_utc": None, "producer": "phaseD_submissions_root"})
    input_manifest.append({"type": "phaseD_submission_dir", "path": str(day_dir), "sha256": _sha256_dir_deterministic(day_dir), "day_utc": day, "producer": "phaseD_submission_dir"})

    try:
        idx = _build_orderid_index(day)
        orphan_fallback_idx = _build_orphan_fallback_index(day)
    except Exception as e:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_MISSING_SUBMISSIONS_DAY_DIR",
            message="Missing or unreadable submissions day dir",
            details={"error": str(e), "day_dir": str(day_dir)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
            failure_path_override=replay_failure_override,
        )
        emit_replay_summary("FAIL")
        print(f"FAIL: {e}", file=sys.stderr)  # type: ignore[name-defined]
        return 2

    submission_meta_rows = _submission_meta_rows(day)
    perm_id_bridge_candidates, perm_id_bridge_error = _build_perm_id_bridge_candidates(
        day=day,
        truth_root=truth_root,
        submission_meta_rows=submission_meta_rows,
    )
    if submission_meta_rows:
        first_submission_id = str(submission_meta_rows[0].get("submission_id") or "").strip()
        if first_submission_id:
            replay_submission_record_path = str(
                (
                    SUBMISSIONS_DAY_ROOT
                    / day
                    / first_submission_id
                    / "broker_submission_record.v2.json"
                ).resolve()
            )

    synthetic_rows = [
        row
        for row in submission_meta_rows
        if row.get("engine_id") and row.get("source_intent_id") and row.get("intent_sha256") and isinstance(row.get("order_qty"), int) and int(row.get("order_qty")) > 0
    ]
    if not idx and synthetic_rows:
        wrote = 0
        try:
            for meta in synthetic_rows:
                event_hash = _event_hash_key([
                    day,
                    "ORDER_STATUS",
                    str(meta["submission_id"]),
                    "",
                    "",
                    str(meta.get("broker_status") or "UNKNOWN"),
                    "0",
                    "0",
                    "0",
                    str(meta.get("submitted_at_utc") or produced_utc),
                ])
                raw_status = str(meta.get("broker_status") or "UNKNOWN").strip()
                rec: Dict[str, Any] = {
                    "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
                    "schema_version": 1,
                    "produced_utc": str(meta.get("submitted_at_utc") or produced_utc),
                    "day_utc": day,
                    "producer": {"repo": "constellation", "git_sha": _git_sha(), "module": "ops/tools/run_execution_stream_snapshot_day_v1.py"},
                    "status": "OK",
                    "reason_codes": ["DRY_RUN_SUBMISSION_SNAPSHOT"],
                    "submission_id": str(meta["submission_id"]),
                    "binding_hash": str(meta["binding_hash"]),
                    "engine_id": str(meta["engine_id"]),
                    "source_intent_id": str(meta["source_intent_id"]),
                    "intent_sha256": str(meta["intent_sha256"]),
                    "broker": {"name": "INTERACTIVE_BROKERS", "environment": str(meta.get("broker_env") or "PAPER")},
                    "event_type": "ORDER_STATUS",
                    "event_time_utc": str(meta.get("submitted_at_utc") or produced_utc),
                    "observed_at_utc": str(meta.get("submitted_at_utc") or produced_utc),
                    "broker_ids": {"order_id": None, "perm_id": None},
                    "event_attribution": _build_event_attribution(
                        raw_order_id=None,
                        raw_perm_id=None,
                        attribution_method=ATTRIBUTION_METHOD_SYNTHETIC_SUBMISSION_SNAPSHOT,
                        attribution_confidence=ATTRIBUTION_CONFIDENCE_HIGH,
                    ),
                    "order_state": _build_order_state_v1(
                        raw_broker_status=raw_status,
                        filled_qty=0,
                        remaining_qty=int(meta.get("order_qty") or 0),
                        avg_fill_price="0",
                    ),
                    "fill": {"fill_qty": 0, "fill_price": "0", "commission": "0", "currency": "USD"},
                    "canonical_json_hash": "",
                }
                rec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(rec)
                if not replay_raw_broker_status:
                    replay_raw_broker_status = str(rec["order_state"].get("raw_broker_status") or "")
                    replay_normalized_lifecycle_status = str(rec["order_state"].get("normalized_lifecycle_status") or "")
                validate_against_repo_schema_v1(rec, REPO_ROOT, SCHEMA_STREAM)
                out_path = (out_day / f"{event_hash}.execution_event_stream_record.v1.json").resolve()
                _write_immutable(out_path, canonical_json_bytes_v1(rec) + b"\n")
                wrote += 1
        except Exception as e:
            failure_code = (
                UNKNOWN_BROKER_ORDER_STATUS
                if UNKNOWN_BROKER_ORDER_STATUS in str(e).upper()
                else "EXEC_STREAM_TRADE_PARSE_OR_ATTRIBUTION_FAILED"
            )
            _write_failure(
                day=day,
                produced_utc=produced_utc,
                code=failure_code,
                message="Synthetic submission snapshot failed",
                details={"error": str(e)},
                input_manifest=input_manifest,
                attempted_outputs=attempted_outputs,
                failure_path_override=replay_failure_override,
            )
            emit_replay_summary("FAIL")
            print(f"FAIL: {e}", file=sys.stderr)
            return 2
        emit_replay_summary("PASS")
        print(f"OK: EXECUTION_STREAM_SNAPSHOT_WRITTEN day={day} wrote={wrote} out_dir={out_day}")
        return 0

    # Import ib_insync
    try:
        from ib_insync import IB  # type: ignore
    except Exception as e:  # noqa: BLE001
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_IB_INSYNC_IMPORT_FAILED",
            message="ib_insync import failed",
            details={"error": repr(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
            failure_path_override=replay_failure_override,
        )
        emit_replay_summary("FAIL")
        print(f"FAIL: ib_insync import failed: {e!r}")
        return 2

    # Connect with bounded retry/backoff
    ib = IB()
    last_err: Optional[str] = None
    for backoff in (1, 2, 5):
        try:
            ok = ib.connect(str(args.ib_host), int(args.ib_port), clientId=int(args.ib_client_id))
            if ok:
                last_err = None
                break
            last_err = "connect returned false"
        except Exception as e:  # noqa: BLE001
            last_err = repr(e)
        time.sleep(backoff)

    if last_err is not None:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_BROKER_CONNECT_FAILED",
            message="Broker connect failed (timeouts/disconnect)",
            details={"host": str(args.ib_host), "port": int(args.ib_port), "client_id": int(args.ib_client_id), "error": last_err},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
            failure_path_override=replay_failure_override,
        )
        emit_replay_summary("FAIL")
        print(f"FAIL: BROKER_CONNECT_FAILED: {last_err}")
        return 2

    # Pull broker state
    try:
        trades = list(ib.trades())
        executions = list(ib.executions())
    except Exception as e:  # noqa: BLE001
        ib.disconnect()
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_BROKER_PULL_FAILED",
            message="Broker pull failed",
            details={"error": repr(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
            failure_path_override=replay_failure_override,
        )
        emit_replay_summary("FAIL")
        print(f"FAIL: BROKER_PULL_FAILED: {e!r}")
        return 2

    ib.disconnect()

    wrote = 0
    orphan_claims: Dict[str, Tuple[Optional[int], Optional[int]]] = {}

    def write_record(
        *,
        event_type: str,
        order_id: Optional[int],
        perm_id: Optional[int],
        symbol: str,
        action: str,
        order_qty: Optional[int],
        raw_broker_status: str,
        filled_qty: int,
        remaining_qty: int,
        avg_fill_price: str,
        fill: Dict[str, Any],
        raw: Dict[str, Any],
    ) -> None:
        nonlocal wrote
        meta, attribution_reason_codes, event_attribution = _resolve_submission_meta_for_event(
            event_type=event_type,
            idx=idx,
            orphan_fallback_idx=orphan_fallback_idx,
            orphan_claims=orphan_claims,
            perm_id_bridge_candidates=perm_id_bridge_candidates,
            perm_id_bridge_error=perm_id_bridge_error,
            order_id=order_id,
            perm_id=perm_id,
            symbol=symbol,
            action=action,
            order_qty=order_qty,
        )
        submission_id = meta["submission_id"]
        binding_hash = meta["binding_hash"]
        engine_id = meta["engine_id"]
        source_intent_id = meta["source_intent_id"]
        intent_sha256 = meta["intent_sha256"]

        if not (engine_id and source_intent_id and intent_sha256):
            raise RuntimeError(f"MISSING_LINEAGE_FOR_SUBMISSION: submission_id={submission_id}")
        order_state = _build_order_state_v1(
            raw_broker_status=raw_broker_status,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            avg_fill_price=avg_fill_price,
        )
        nonlocal replay_raw_broker_status, replay_normalized_lifecycle_status
        if not replay_raw_broker_status:
            replay_raw_broker_status = str(order_state.get("raw_broker_status") or "")
            replay_normalized_lifecycle_status = str(order_state.get("normalized_lifecycle_status") or "")

        event_hash = _event_hash_key(
            [
                day,
                event_type,
                submission_id,
                str(order_id) if order_id is not None else "",
                str(perm_id) if perm_id is not None else "",
                str(order_state.get("status") or ""),
                str(order_state.get("filled_qty") or 0),
                str(fill.get("fill_qty") or 0),
                str(fill.get("fill_price") or "0"),
                str(raw.get("event_time_utc") or observed_at),
            ]
        )

        attributed_order_id = order_id
        attributed_perm_id = perm_id
        if str(event_attribution.get("attribution_method") or "").strip() == ATTRIBUTION_METHOD_PERM_ID_BRIDGE:
            bridged_order_id = meta.get("broker_order_id")
            bridged_perm_id = meta.get("broker_perm_id")
            if isinstance(bridged_order_id, int) and bridged_order_id >= 0:
                attributed_order_id = bridged_order_id
            if isinstance(bridged_perm_id, int) and bridged_perm_id >= 0:
                attributed_perm_id = bridged_perm_id

        rec: Dict[str, Any] = {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "constellation", "git_sha": _git_sha(), "module": "ops/tools/run_execution_stream_snapshot_day_v1.py"},
            "status": "OK",
            "reason_codes": attribution_reason_codes,
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": engine_id,
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": str(meta["broker_env"] or "PAPER")},
            "event_type": event_type,
            "event_time_utc": str(raw.get("event_time_utc") or observed_at),
            "observed_at_utc": observed_at,
            "broker_ids": {"order_id": attributed_order_id, "perm_id": attributed_perm_id},
            "event_attribution": event_attribution,
            "order_state": order_state,
            "fill": fill,
            "canonical_json_hash": "",
        }
        rec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(rec)
        validate_against_repo_schema_v1(rec, REPO_ROOT, SCHEMA_STREAM)

        payload = canonical_json_bytes_v1(rec) + b"\n"
        out_path = (out_day / f"{event_hash}.execution_event_stream_record.v1.json").resolve()
        _write_immutable(out_path, payload)
        wrote += 1

    # Trades -> ORDER_STATUS
    try:
        for t in trades:
            os = getattr(t, "orderStatus", None)
            o = getattr(t, "order", None)
            st = str(getattr(os, "status", "UNKNOWN") or "UNKNOWN").upper()
            filled = int(getattr(os, "filled", 0) or 0)
            remaining = int(getattr(os, "remaining", 0) or 0)
            avg = getattr(os, "avgFillPrice", 0) or 0
            avg_s = str(Decimal(str(avg)))
            order_id = getattr(o, "orderId", None)
            perm_id = getattr(o, "permId", None)
            action = _normalize_side(getattr(o, "action", ""))
            qty = getattr(o, "totalQuantity", 0) or 0
            qty_i = int(qty) if float(qty) > 0 else None
            symbol = str(getattr(getattr(t, "contract", None), "symbol", "") or "").strip().upper()

            fill = {"fill_qty": 0, "fill_price": "0", "commission": "0", "currency": "USD"}
            raw = {"event_time_utc": observed_at, "trade": str(t)}
            write_record(
                event_type="ORDER_STATUS",
                order_id=order_id if isinstance(order_id, int) else None,
                perm_id=perm_id if isinstance(perm_id, int) else None,
                symbol=symbol,
                action=action,
                order_qty=qty_i,
                raw_broker_status=st,
                filled_qty=filled,
                remaining_qty=remaining,
                avg_fill_price=avg_s,
                fill=fill,
                raw=raw,
            )
    except Exception as e:
        failure_code = (
            UNKNOWN_BROKER_ORDER_STATUS
            if UNKNOWN_BROKER_ORDER_STATUS in str(e).upper()
            else "EXEC_STREAM_TRADE_PARSE_OR_ATTRIBUTION_FAILED"
        )
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code=failure_code,
            message="Trade parse/attribution failed (day purity violation or malformed trade)",
            details={"error": str(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
            failure_path_override=replay_failure_override,
        )
        emit_replay_summary("FAIL")
        print(f"FAIL: {e}")
        return 2

    # Executions -> EXEC_DETAILS (fills)
    try:
        for ex in executions:
            exec_obj = getattr(ex, "execution", None) or ex
            order_id = getattr(exec_obj, "orderId", None)
            perm_id = getattr(exec_obj, "permId", None)
            shares = getattr(exec_obj, "shares", 0) or 0
            price = getattr(exec_obj, "price", 0) or 0
            time_s = getattr(exec_obj, "time", None)
            event_time = str(time_s) if time_s is not None else observed_at

            fill = {"fill_qty": int(shares), "fill_price": str(Decimal(str(price))), "commission": "0", "currency": "USD"}
            raw = {"event_time_utc": event_time, "execution": str(exec_obj)}
            write_record(
                event_type="EXEC_DETAILS",
                order_id=order_id if isinstance(order_id, int) else None,
                perm_id=perm_id if isinstance(perm_id, int) else None,
                symbol="",
                action="",
                order_qty=None,
                raw_broker_status="FILLED",
                filled_qty=int(shares),
                remaining_qty=0,
                avg_fill_price=str(Decimal(str(price))),
                fill=fill,
                raw=raw,
            )
    except Exception as e:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_EXECUTION_PARSE_OR_ATTRIBUTION_FAILED",
            message="Execution parse/attribution failed (day purity violation or malformed execution)",
            details={"error": str(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
            failure_path_override=replay_failure_override,
        )
        emit_replay_summary("FAIL")
        print(f"FAIL: {e}")
        return 2

    emit_replay_summary("PASS")
    print(f"OK: EXECUTION_STREAM_SNAPSHOT_WRITTEN day={day} wrote={wrote} out_dir={str(out_day)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(format_failure_line(
            "run_execution_stream_snapshot_day_v1",
            classify_failure(exc),
            error=repr(exc),
        ), file=sys.stderr)
        raise SystemExit(2)
