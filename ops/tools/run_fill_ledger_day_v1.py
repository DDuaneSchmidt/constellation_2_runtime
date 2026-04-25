#!/usr/bin/env python3
"""
run_fill_ledger_day_v1.py

Bundle 1: Fill Ledger Spine v1 (deterministic aggregation).

Reads:
  <TRUTH_ROOT>/execution_stream_v1/<DAY>/*.execution_event_stream_record.v1.json
  <TRUTH_ROOT>/execution_evidence_v1/submissions/<DAY>/<submission_id>/
    - broker_submission_record.v2.json
    - equity_order_plan.v2.json (preferred) or equity_order_plan.v1.json (legacy)
    - execution_event_record.v1.json (lineage if needed)

Writes (immutable):
  <TRUTH_ROOT>/fill_ledger_v1/<DAY>/<submission_id>.fill_ledger.v1.json

Fail-closed:
- missing submissions day dir
- stream records fail schema parse
- order_qty missing
- overfill (filled_qty > order_qty)
- missing lineage
- schema validation failure
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line
from constellation_2.common.truth_root_v1 import resolve_truth_root


STREAM_ROOT: Path | None = None
SUB_ROOT: Path | None = None
OUT_ROOT: Path | None = None


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f"FAIL: invalid --truth_root: {p}")
    return p

SCHEMA_LEDGER = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"
EQUITY_PLAN_V1_SCHEMA = "constellation_2/schemas/equity_order_plan.v1.schema.json"
EQUITY_PLAN_V2_SCHEMA = "constellation_2/schemas/equity_order_plan.v2.schema.json"


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


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


def _write_bytes_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(prefix=f".{path.name}.tmp.", dir=str(path.parent), delete=False) as tmp:
        tmp.write(payload)
        tmp.flush()
        import os
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _list_stream_files(day: str) -> List[Path]:
    if STREAM_ROOT is None:
        raise RuntimeError("STREAM_ROOT_UNSET")
    d = (STREAM_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        return []
    return sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".execution_event_stream_record.v1.json")])


def _supported_plan_path(subdir: Path) -> Path | None:
    equity_plan_v2_p = (subdir / "equity_order_plan.v2.json").resolve()
    if equity_plan_v2_p.exists():
        return equity_plan_v2_p
    equity_plan_p = (subdir / "equity_order_plan.v1.json").resolve()
    if equity_plan_p.exists():
        return equity_plan_p
    options_plan_p = (subdir / "order_plan.v1.json").resolve()
    if options_plan_p.exists():
        return options_plan_p
    return None


def _is_authoritative_submission_dir(subdir: Path) -> bool:
    bsr_p = (subdir / "broker_submission_record.v2.json").resolve()
    plan_p = _supported_plan_path(subdir)
    if not bsr_p.exists() or plan_p is None:
        return False
    try:
        bsr = _read_json_obj(bsr_p)
        validate_against_repo_schema_v1(bsr, REPO_ROOT, "constellation_2/schemas/broker_submission_record.v2.schema.json")
        plan = _read_json_obj(plan_p)
    except Exception:
        return False
    if plan_p.name == "equity_order_plan.v2.json":
        try:
            validate_against_repo_schema_v1(plan, REPO_ROOT, EQUITY_PLAN_V2_SCHEMA)
        except Exception:
            return False
    elif plan_p.name == "equity_order_plan.v1.json":
        schema_id = str(plan.get("schema_id") or "").strip()
        schema_version = str(plan.get("schema_version") or "").strip()
        if schema_id != "equity_order_plan":
            return False
        # Compatibility bridge: legacy filename may carry a v2 payload.
        if schema_version == "v2":
            try:
                validate_against_repo_schema_v1(plan, REPO_ROOT, EQUITY_PLAN_V2_SCHEMA)
            except Exception:
                return False
        elif schema_version == "v1":
            try:
                validate_against_repo_schema_v1(plan, REPO_ROOT, EQUITY_PLAN_V1_SCHEMA)
            except Exception:
                return False
        else:
            return False
    else:
        try:
            validate_against_repo_schema_v1(plan, REPO_ROOT, "constellation_2/schemas/order_plan.v1.schema.json")
        except Exception:
            return False
    return str(bsr.get("submission_id") or "").strip() == subdir.name


def _list_submission_dirs(day: str) -> List[Path]:
    if SUB_ROOT is None:
        raise RuntimeError("SUB_ROOT_UNSET")
    d = (SUB_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        raise RuntimeError(f"MISSING_SUBMISSIONS_DAY_DIR: {d}")
    dirs = sorted([p for p in d.iterdir() if p.is_dir() and _is_authoritative_submission_dir(p)])
    if not dirs:
        raise RuntimeError(f"NO_AUTHORITATIVE_SUBMISSIONS_DAY_DIR: {d}")
    return dirs


def _submission_dir(day: str, submission_id: str) -> Path:
    if SUB_ROOT is None:
        raise RuntimeError("SUB_ROOT_UNSET")
    subdir = (SUB_ROOT / day / submission_id).resolve()
    if not subdir.exists() or not subdir.is_dir():
        raise RuntimeError(f"MISSING_SUBMISSION_DIR: {subdir}")
    if not _is_authoritative_submission_dir(subdir):
        raise RuntimeError(f"SUBMISSION_DIR_NOT_AUTHORITATIVE: {subdir}")
    return subdir


def _order_qty_from_submission(subdir: Path) -> int:
    p = (subdir / "equity_order_plan.v2.json").resolve()
    if p.exists():
        o = _read_json_obj(p)
        qty = o.get("qty_shares")
        if not isinstance(qty, int) or qty <= 0:
            raise RuntimeError("EQUITY_ORDER_PLAN_V2_QTY_INVALID")
        return int(qty)

    p = (subdir / "equity_order_plan.v1.json").resolve()
    if p.exists():
        o = _read_json_obj(p)
        qty = o.get("qty_shares")
        if not isinstance(qty, int) or qty <= 0:
            raise RuntimeError("EQUITY_ORDER_PLAN_QTY_INVALID")
        return int(qty)

    p = (subdir / "order_plan.v1.json").resolve()
    if not p.exists():
        raise RuntimeError(f"MISSING_SUPPORTED_PLAN_FOR_ORDER_QTY: {subdir}")
    o = _read_json_obj(p)
    risk = o.get("risk_proof") if isinstance(o.get("risk_proof"), dict) else {}
    qty = risk.get("contracts")
    if not isinstance(qty, int) or qty <= 0:
        raise RuntimeError("OPTIONS_ORDER_PLAN_CONTRACTS_INVALID")
    return int(qty)


def _lineage_from_submission(subdir: Path) -> Tuple[str, str, str, str]:
    """
    Returns (engine_id, source_intent_id, intent_sha256, binding_hash)
    """
    bsr_p = (subdir / "broker_submission_record.v2.json").resolve()
    if not bsr_p.exists():
        raise RuntimeError(f"MISSING_BROKER_SUBMISSION_RECORD: {bsr_p}")
    bsr = _read_json_obj(bsr_p)
    binding_hash = str(bsr.get("binding_hash") or "").strip()
    if not binding_hash:
        raise RuntimeError("BINDING_HASH_MISSING")

    engine_id = ""
    source_intent_id = ""
    intent_sha256 = ""

    evt_p = (subdir / "execution_event_record.v1.json").resolve()
    if evt_p.exists():
        evt = _read_json_obj(evt_p)
        engine_id = str(evt.get("engine_id") or "").strip()
        source_intent_id = str(evt.get("source_intent_id") or "").strip()
        intent_sha256 = str(evt.get("intent_sha256") or "").strip()

    if not engine_id or not source_intent_id or not intent_sha256:
        plan_p = (subdir / "equity_order_plan.v2.json").resolve()
        if plan_p.exists():
            plan = _read_json_obj(plan_p)
            engine_id = engine_id or str(plan.get("engine_id") or "").strip()
            source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
            intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()

    if not engine_id or not source_intent_id or not intent_sha256:
        plan_p = (subdir / "equity_order_plan.v1.json").resolve()
        if plan_p.exists():
            plan = _read_json_obj(plan_p)
            engine_id = engine_id or str(plan.get("engine_id") or "").strip()
            source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
            intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()

    if not engine_id or not source_intent_id or not intent_sha256:
        plan_p = (subdir / "order_plan.v1.json").resolve()
        if plan_p.exists():
            plan = _read_json_obj(plan_p)
            engine_id = engine_id or str(plan.get("engine_id") or "").strip()
            source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
            intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()

    if not (engine_id and source_intent_id and intent_sha256):
        raise RuntimeError("LINEAGE_MISSING_IN_SUBMISSION_DIR")

    return engine_id, source_intent_id, intent_sha256, binding_hash


def _sort_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        events,
        key=lambda ev: (
            str(ev.get("event_time_utc") or ""),
            str(ev.get("canonical_json_hash") or ""),
        ),
    )


def _build_fill_ledger(subdir: Path, day: str, events: List[Dict[str, Any]]) -> Dict[str, Any]:
    submission_id = subdir.name
    order_qty = _order_qty_from_submission(subdir)
    engine_id, source_intent_id, intent_sha256, binding_hash = _lineage_from_submission(subdir)
    events_sorted = _sort_events(events)

    filled_qty = 0
    num = Decimal("0")
    den = Decimal("0")
    status_latest = "UNKNOWN"

    event_hashes: List[str] = []
    for ev in events_sorted:
        event_hashes.append(str(ev.get("canonical_json_hash") or "").strip() or "0" * 64)
        os_obj = ev.get("order_state") if isinstance(ev.get("order_state"), dict) else {}
        st = str(os_obj.get("status") or "UNKNOWN").strip().upper()
        if st:
            status_latest = st
        fill = ev.get("fill") if isinstance(ev.get("fill"), dict) else {}
        q = fill.get("fill_qty", 0)
        pr = fill.get("fill_price", "0")
        if isinstance(q, int) and q > 0:
            filled_qty += int(q)
            den += Decimal(str(q))
            num += (Decimal(str(q)) * Decimal(str(pr)))

    if filled_qty > order_qty:
        raise RuntimeError(f"OVERFILL_FAIL_CLOSED: submission_id={submission_id} filled_qty={filled_qty} order_qty={order_qty}")

    remaining = order_qty - filled_qty
    avg = "0"
    if den > 0:
        avg = str((num / den).quantize(Decimal("0.0001")))

    lifecycle = "UNKNOWN"
    st = status_latest.upper()
    if st in ("CANCELLED", "REJECTED", "INACTIVE"):
        lifecycle = st
    elif filled_qty == 0:
        lifecycle = "OPEN"
    elif filled_qty < order_qty:
        lifecycle = "PARTIALLY_FILLED"
    else:
        lifecycle = "FILLED"

    ledger: Dict[str, Any] = {
        "schema_id": "C2_FILL_LEDGER_V1",
        "schema_version": 1,
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_fill_ledger_day_v1.py"},
        "status": "OK",
        "reason_codes": [],
        "submission_id": submission_id,
        "binding_hash": binding_hash,
        "engine_id": engine_id,
        "source_intent_id": source_intent_id,
        "intent_sha256": intent_sha256,
        "order_qty": int(order_qty),
        "filled_qty": int(filled_qty),
        "remaining_qty": int(remaining),
        "avg_fill_price_weighted": avg,
        "lifecycle_status": lifecycle,
        "event_hashes": event_hashes,
        "canonical_json_hash": "",
    }
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=CLOSURE_STATE_COMPLETE,
        reason_codes=[],
        missing_dependency_artifacts=[],
    )
    ledger["blocking_codes"] = list(blocker_envelope["blocking_codes"])
    ledger["closure_state"] = str(blocker_envelope["closure_state"])
    ledger["first_blocker_code"] = str(blocker_envelope["first_blocker_code"])
    ledger["missing_dependency_artifacts"] = list(blocker_envelope["missing_dependency_artifacts"])
    ledger["constitutional_dependency_declaration"] = build_artifact_dependency_declaration_v1(
        artifact_type="fill_ledger_v1",
        artifact_class="execution_result",
        authority_id="fill_ledger_v1",
        declared_dependency_artifacts=[],
        dependency_refs=[],
    )
    ledger["constitutional_lineage"] = build_governed_artifact_lineage_v1(
        artifact_type="fill_ledger_v1",
        artifact_version="1",
        artifact_class="execution_result",
        authority_id="fill_ledger_v1",
        producer_id="ops/tools/run_fill_ledger_day_v1.py",
        generated_at_utc=f"{day}T00:00:00Z",
        effective_at_utc=f"{day}T00:00:00Z",
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version=_git_sha(),
        run_id=f"fill_ledger_v1:{day}:{submission_id}",
    )
    ledger["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(ledger)
    validate_against_repo_schema_v1(ledger, REPO_ROOT, SCHEMA_LEDGER)
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="fill_ledger_v1",
        payload=ledger,
        required_finality_states=["finalized", "corrected"],
    )
    return ledger


def _lifecycle_rank(value: str) -> int:
    ranks = {
        "UNKNOWN": 0,
        "OPEN": 1,
        "PARTIALLY_FILLED": 2,
        "FILLED": 3,
        "CANCELLED": 4,
        "REJECTED": 4,
    }
    return ranks.get(str(value or "").strip().upper(), -1)


def _is_safe_fill_ledger_refresh_upgrade(existing: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    identity_fields = (
        "schema_id",
        "schema_version",
        "day_utc",
        "submission_id",
        "binding_hash",
        "engine_id",
        "source_intent_id",
        "intent_sha256",
        "order_qty",
    )
    for field in identity_fields:
        if existing.get(field) != candidate.get(field):
            return False

    existing_hashes = existing.get("event_hashes")
    candidate_hashes = candidate.get("event_hashes")
    if not isinstance(existing_hashes, list) or not isinstance(candidate_hashes, list):
        return False
    if len(candidate_hashes) < len(existing_hashes):
        return False
    if candidate_hashes[: len(existing_hashes)] != existing_hashes:
        return False

    existing_filled = existing.get("filled_qty")
    candidate_filled = candidate.get("filled_qty")
    if not isinstance(existing_filled, int) or not isinstance(candidate_filled, int):
        return False
    if candidate_filled < existing_filled:
        return False

    existing_remaining = existing.get("remaining_qty")
    candidate_remaining = candidate.get("remaining_qty")
    if not isinstance(existing_remaining, int) or not isinstance(candidate_remaining, int):
        return False
    if candidate_remaining > existing_remaining:
        return False

    if candidate_filled == existing_filled and candidate.get("avg_fill_price_weighted") != existing.get("avg_fill_price_weighted"):
        return False

    if _lifecycle_rank(str(candidate.get("lifecycle_status") or "")) < _lifecycle_rank(str(existing.get("lifecycle_status") or "")):
        return False

    return True


def _write_fill_ledger(path: Path, ledger: Dict[str, Any]) -> str:
    payload = canonical_json_bytes_v1(ledger) + b"\n"
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload):
            return "SKIP_IDENTICAL"
        existing_obj = _read_json_obj(path)
        if not _is_safe_fill_ledger_refresh_upgrade(existing_obj, ledger):
            raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
        _write_bytes_replace(path, payload)
        return "BACKFILL_REPAIRED"
    _write_immutable(path, payload)
    return "WROTE"


def main(argv: List[str] | None = None) -> int:
    assert_constitutional_writer_allowed_v1(REPO_ROOT, "fill_ledger_v1", "ops/tools/run_fill_ledger_day_v1.py")
    ap = argparse.ArgumentParser(prog="run_fill_ledger_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True, help="Authoritative runtime truth root")
    ap.add_argument("--submission_id", default=None, help="Optional single submission_id to refresh deterministically")
    args = ap.parse_args(argv)

    day = str(args.day_utc).strip()
    submission_id_filter = str(args.submission_id or "").strip() or None

    truth_root = _require_truth_root(args.truth_root)
    global STREAM_ROOT, SUB_ROOT, OUT_ROOT
    STREAM_ROOT = (truth_root / "execution_stream_v1").resolve()
    SUB_ROOT = (truth_root / "execution_evidence_v1" / "submissions").resolve()
    OUT_ROOT = (truth_root / "fill_ledger_v1").resolve()

    # Load stream records once, group by submission_id
    stream_files = _list_stream_files(day)
    by_sub: Dict[str, List[Dict[str, Any]]] = {}
    for p in stream_files:
        o = _read_json_obj(p)
        subid = str(o.get("submission_id") or "").strip()
        if not subid:
            raise RuntimeError(f"STREAM_RECORD_MISSING_SUBMISSION_ID: {p}")
        by_sub.setdefault(subid, []).append(o)

    if submission_id_filter is not None:
        subdirs = [_submission_dir(day, submission_id_filter)]
    else:
        subdirs = _list_submission_dirs(day)

    wrote = 0
    repaired = 0
    skipped = 0
    for subdir in subdirs:
        submission_id = subdir.name
        ledger = _build_fill_ledger(subdir, day, by_sub.get(submission_id, []))
        out_path = (OUT_ROOT / day / f"{submission_id}.fill_ledger.v1.json").resolve()
        action = _write_fill_ledger(out_path, ledger)
        if action == "WROTE":
            wrote += 1
        elif action == "BACKFILL_REPAIRED":
            repaired += 1
        else:
            skipped += 1

    print(
        f"OK: FILL_LEDGER_WRITTEN day={day} wrote={wrote} repaired={repaired} "
        f"skipped={skipped} stream_records={len(stream_files)} scoped_submission={submission_id_filter or ''}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(format_failure_line("run_fill_ledger_day_v1", classify_failure(exc), error=repr(exc)), file=sys.stderr)
        raise SystemExit(2)
