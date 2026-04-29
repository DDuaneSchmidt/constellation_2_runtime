#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_canonical_lifecycle_closure_path
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_accounting_nav_path,
    resolve_fill_ledger_submission_path,
    resolve_positions_effective_pointer_path,
    resolve_positions_snapshot_candidate_paths,
)
from constellation_2.common.runtime_ledger_v1 import (
    append_runtime_ledger_events_v1,
    projection_over_runtime_ledger_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseF.execution_evidence.run import run_execution_evidence_truth_day_v1 as exec_truth


OUTPUT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_lifecycle_closure.v1.schema.json"
PAPER_ENVIRONMENT = "PAPER"
PRIMARY_SLEEVE_ID = "PRIMARY"
MATCHABLE_ATTRIBUTION_STATUSES = {"ATTRIBUTED", "PARTIAL"}


def _require_dir(raw: str, *, label: str) -> Path:
    path = Path(str(raw).strip()).expanduser().resolve()
    if (not path.is_absolute()) or (not path.exists()) or (not path.is_dir()):
        raise SystemExit(f"FAIL: INVALID_{label.upper()}:{path}")
    return path


def _resolve_ib_account(raw: str) -> str:
    ib_account = str(raw or "").strip()
    if ib_account:
        return ib_account
    return resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)


def _resolve_canonical_truth_root(raw: str) -> Path:
    if str(raw).strip():
        return _require_dir(raw, label="truth_root")
    return resolve_decision_truth_root_v1("", repo_root=REPO_ROOT)


def _resolve_source_truth_root(raw: str, *, environment: str, ib_account: str) -> Path:
    if str(raw).strip():
        return _require_dir(raw, label="source_truth_root")
    binding = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )[0]
    return binding.truth_root.resolve()


def _first_nonempty(values: Iterable[Any]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _extract_reason_codes(payload: Mapping[str, Any]) -> list[str]:
    reason_codes = payload.get("reason_codes")
    if isinstance(reason_codes, list):
        return [str(item).strip() for item in reason_codes if str(item).strip()]
    return []


def _artifact_ref(path: Path, *, status_fields: tuple[str, ...] = ()) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {
            "path": str(path),
            "exists": False,
            "sha256": "",
            "status": "",
            "reason_codes": [],
        }
    payload = read_json_object_v1(path)
    return {
        "path": str(path),
        "exists": True,
        "sha256": sha256_file_v1(path),
        "status": _first_nonempty(payload.get(field) for field in status_fields),
        "reason_codes": _extract_reason_codes(payload),
    }


def _artifact_ref_from_details(
    *,
    path: Path,
    exists: bool,
    sha256: str,
    status: str,
    reason_codes: Iterable[Any],
) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": bool(exists),
        "sha256": str(sha256 or ""),
        "status": str(status or ""),
        "reason_codes": [str(item).strip() for item in reason_codes if str(item).strip()],
    }


def _broker_identity(broker_payload: Mapping[str, Any]) -> tuple[str, str, str]:
    submission_id = str(broker_payload.get("submission_id") or "").strip()
    broker_ids = broker_payload.get("broker_ids") if isinstance(broker_payload.get("broker_ids"), dict) else {}
    order_id_raw = broker_ids.get("order_id")
    perm_id_raw = broker_ids.get("perm_id")
    order_id = "" if order_id_raw is None else str(order_id_raw).strip()
    perm_id = "" if perm_id_raw is None else str(perm_id_raw).strip()
    return submission_id, order_id, perm_id


def _safe_int(value: Any) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _safe_decimal(value: Any) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_str(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _fact_reason_codes(fact: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    for field in ("quality_reason_codes", "attribution_reason_codes"):
        raw = fact.get(field)
        if isinstance(raw, list):
            codes.extend(str(item).strip() for item in raw if str(item).strip())
    return list(dict.fromkeys(codes))


def _read_jsonl_objects(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _resolve_broker_fact_path(*, source_truth_root: Path, day_utc: str, filename: str) -> Path:
    return (
        source_truth_root
        / "broker_fact_spine_v1"
        / "fact_ledger"
        / day_utc
        / filename
    ).resolve()


def _matching_fact_rows(
    *,
    ledger_path: Path,
    expected_schema_id: str,
    order_id: str,
    perm_id: str,
) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for row in _read_jsonl_objects(ledger_path):
        if str(row.get("schema_id") or "").strip() != expected_schema_id:
            continue
        if str(row.get("quality_status") or "").strip().upper() != "OK":
            continue
        if str(row.get("attribution_status") or "").strip().upper() not in MATCHABLE_ATTRIBUTION_STATUSES:
            continue
        if str(row.get("order_id") or "").strip() != order_id:
            continue
        if str(row.get("perm_id") or "").strip() != perm_id:
            continue
        matched.append(row)
    return matched


def _fact_sort_key(fact: Mapping[str, Any]) -> tuple[str, int, str]:
    return (
        str(fact.get("observed_utc") or fact.get("normalized_utc") or "").strip(),
        _safe_int(fact.get("journal_sequence_number")),
        str(fact.get("fact_record_id") or "").strip(),
    )


def _select_broker_status_fact(*, source_truth_root: Path, day_utc: str, order_id: str, perm_id: str) -> dict[str, Any]:
    ledger_path = _resolve_broker_fact_path(
        source_truth_root=source_truth_root,
        day_utc=day_utc,
        filename="observed_order_status_fact.v1.jsonl",
    )
    matched = _matching_fact_rows(
        ledger_path=ledger_path,
        expected_schema_id="observed_order_status_fact",
        order_id=order_id,
        perm_id=perm_id,
    )
    if not matched:
        return {
            "record": {},
            "match_count": 0,
            "ref": _artifact_ref_from_details(
                path=ledger_path,
                exists=False,
                sha256="",
                status="",
                reason_codes=[],
            ),
        }
    selected = sorted(matched, key=_fact_sort_key)[-1]
    return {
        "record": selected,
        "match_count": len(matched),
        "ref": _artifact_ref_from_details(
            path=ledger_path,
            exists=True,
            sha256=sha256_file_v1(ledger_path),
            status=str(selected.get("status") or "").strip(),
            reason_codes=_fact_reason_codes(selected),
        ),
    }


def _aggregate_fill_facts(*, source_truth_root: Path, day_utc: str, order_id: str, perm_id: str) -> dict[str, Any]:
    ledger_path = _resolve_broker_fact_path(
        source_truth_root=source_truth_root,
        day_utc=day_utc,
        filename="observed_fill_fact.v1.jsonl",
    )
    matched = _matching_fact_rows(
        ledger_path=ledger_path,
        expected_schema_id="observed_fill_fact",
        order_id=order_id,
        perm_id=perm_id,
    )
    deduped: list[dict[str, Any]] = []
    seen_execution_ids: set[str] = set()
    total_qty = Decimal("0")
    total_notional = Decimal("0")
    for row in sorted(matched, key=_fact_sort_key):
        fill_qty = _safe_decimal(row.get("fill_quantity"))
        if fill_qty is None or fill_qty <= 0:
            continue
        execution_id = str(row.get("execution_id") or row.get("fact_record_id") or "").strip()
        if execution_id and execution_id in seen_execution_ids:
            continue
        if execution_id:
            seen_execution_ids.add(execution_id)
        fill_price = _safe_decimal(row.get("fill_price")) or Decimal("0")
        total_qty += fill_qty
        total_notional += fill_qty * fill_price
        deduped.append(row)
    if not deduped:
        return {
            "records": [],
            "match_count": 0,
            "ref": _artifact_ref_from_details(
                path=ledger_path,
                exists=False,
                sha256="",
                status="",
                reason_codes=[],
            ),
            "fill_status": {
                "filled_qty": 0,
                "avg_price": "0",
            },
        }
    avg_price = "0"
    if total_qty > 0:
        avg_price = _decimal_to_str(total_notional / total_qty)
    reason_codes: list[str] = []
    for row in deduped:
        reason_codes.extend(_fact_reason_codes(row))
    return {
        "records": deduped,
        "match_count": len(deduped),
        "ref": _artifact_ref_from_details(
            path=ledger_path,
            exists=True,
            sha256=sha256_file_v1(ledger_path),
            status="FILL_MATCHED",
            reason_codes=list(dict.fromkeys(reason_codes)),
        ),
        "fill_status": {
            "filled_qty": int(total_qty),
            "avg_price": avg_price,
        },
    }


def _derive_observation_basis(*, fact_present: bool, event_present: bool) -> str:
    if fact_present and event_present:
        return "BROKER_FACT_AND_EXECUTION_EVENT"
    if fact_present:
        return "BROKER_FACT_ONLY"
    if event_present:
        return "EXECUTION_EVENT_ONLY"
    return "NONE"


def _safe_positions_count(snapshot_payload: Mapping[str, Any]) -> int:
    positions = snapshot_payload.get("positions")
    if not isinstance(positions, Mapping):
        return 0
    items = positions.get("items")
    if not isinstance(items, list):
        return 0
    return len(items)


def _resolve_positions_snapshot_path(*, truth_root: Path, day_utc: str) -> Path:
    pointer_path = resolve_positions_effective_pointer_path(truth_root=truth_root, day_utc=day_utc)
    if pointer_path.exists() and pointer_path.is_file():
        pointer_payload = read_json_object_v1(pointer_path)
        pointers = pointer_payload.get("pointers")
        if isinstance(pointers, Mapping):
            snapshot_path_raw = str(pointers.get("snapshot_path") or "").strip()
            if snapshot_path_raw:
                return Path(snapshot_path_raw).expanduser().resolve()
    for candidate in resolve_positions_snapshot_candidate_paths(truth_root=truth_root, day_utc=day_utc):
        if candidate.exists() and candidate.is_file():
            return candidate
    candidates = resolve_positions_snapshot_candidate_paths(truth_root=truth_root, day_utc=day_utc)
    return candidates[-1]


def _derive_propagation_state(
    *,
    day_utc: str,
    truth_root: Path,
    submission_id: str,
    fill_observed: bool,
    explicit_terminal_broker_state_observed: bool,
    hard_blockers: list[str],
) -> dict[str, Any]:
    fill_ledger_path = resolve_fill_ledger_submission_path(
        truth_root=truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
    )
    positions_effective_path = resolve_positions_effective_pointer_path(truth_root=truth_root, day_utc=day_utc)
    positions_snapshot_path = _resolve_positions_snapshot_path(truth_root=truth_root, day_utc=day_utc)
    nav_path = resolve_accounting_nav_path(truth_root=truth_root, day_utc=day_utc)

    fill_ledger_ref = _artifact_ref(fill_ledger_path, status_fields=("status",))
    positions_effective_ref = _artifact_ref(
        positions_effective_path,
        status_fields=("status",),
    )
    positions_snapshot_ref = _artifact_ref(
        positions_snapshot_path,
        status_fields=("status",),
    )
    nav_ref = _artifact_ref(nav_path, status_fields=("status",))

    positions_item_count = 0
    positions_effective_snapshot_path = ""
    if positions_snapshot_ref["exists"]:
        positions_item_count = _safe_positions_count(read_json_object_v1(positions_snapshot_path))
    if positions_effective_ref["exists"]:
        positions_effective_payload = read_json_object_v1(positions_effective_path)
        pointers = positions_effective_payload.get("pointers")
        if isinstance(pointers, Mapping):
            positions_effective_snapshot_path = str(pointers.get("snapshot_path") or "").strip()

    nav_total_observed = False
    if nav_ref["exists"]:
        nav_payload = read_json_object_v1(nav_path)
        nav_total_observed = nav_payload.get("nav_total") is not None

    blocker_chain: list[str] = []
    state = "PROPAGATION_PENDING"
    if hard_blockers:
        state = "PROPAGATION_BLOCKED"
        blocker_chain.extend(hard_blockers)
    elif explicit_terminal_broker_state_observed and not fill_observed:
        state = "PROPAGATION_COMPLETE"
    elif not fill_observed:
        blocker_chain.append("FILL_NOT_YET_OBSERVED")
    else:
        hard_propagation_blockers: list[str] = []
        if not fill_ledger_ref["exists"]:
            blocker_chain.append("FILL_LEDGER_ARTIFACT_MISSING")
        if not positions_snapshot_ref["exists"]:
            blocker_chain.append("POSITIONS_SNAPSHOT_MISSING")
        elif positions_item_count == 0:
            blocker_chain.append("POSITIONS_EMPTY_AFTER_FILL_NOT_EXPLAINED")
        if not positions_effective_ref["exists"]:
            blocker_chain.append("POSITIONS_EFFECTIVE_POINTER_MISSING")
        elif positions_effective_snapshot_path:
            resolved_pointer_path = Path(positions_effective_snapshot_path).expanduser().resolve()
            if resolved_pointer_path != positions_snapshot_path:
                hard_propagation_blockers.append("POSITIONS_EFFECTIVE_POINTER_MISMATCH")
        if not nav_ref["exists"]:
            blocker_chain.append("NAV_ARTIFACT_MISSING")
        elif not nav_total_observed:
            blocker_chain.append("NAV_TOTAL_NOT_YET_OBSERVED")
        if hard_propagation_blockers:
            state = "PROPAGATION_BLOCKED"
            blocker_chain = hard_propagation_blockers + blocker_chain
        elif not blocker_chain:
            state = "PROPAGATION_COMPLETE"

    return {
        "status": state,
        "fill_ledger_ref": fill_ledger_ref,
        "positions_snapshot_ref": positions_snapshot_ref,
        "positions_effective_ref": positions_effective_ref,
        "nav_ref": nav_ref,
        "positions_item_count": positions_item_count,
        "positions_effective_snapshot_path": positions_effective_snapshot_path,
        "nav_total_observed": nav_total_observed,
        "blocker_chain": blocker_chain,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_canonical_lifecycle_closure_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--source_truth_root", default="")
    ap.add_argument("--environment", default=PAPER_ENVIRONMENT)
    ap.add_argument("--ib_account", default="")
    ap.add_argument("--submission_id", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "").strip().upper()
    if environment != PAPER_ENVIRONMENT:
        raise SystemExit(f"FAIL: PAPER_ONLY_TOOL:environment={environment}")
    ib_account = _resolve_ib_account(args.ib_account)
    canonical_truth_root = _resolve_canonical_truth_root(args.truth_root)
    source_truth_root = _resolve_source_truth_root(
        args.source_truth_root,
        environment=environment,
        ib_account=ib_account,
    )
    producer_git_sha = repo_git_sha_v1()
    produced_utc = now_utc_iso_v1()
    submission_filter = str(args.submission_id or "").strip()

    refresh_rc = exec_truth.main(
        [
            "--day_utc",
            day_utc,
            "--producer_git_sha",
            producer_git_sha,
            "--producer_repo",
            REPO_ROOT.name,
            "--truth_root",
            str(canonical_truth_root),
            "--source_truth_root",
            str(source_truth_root),
        ]
    )
    if refresh_rc != 0:
        raise SystemExit(f"FAIL: CANONICAL_EXECUTION_EVIDENCE_REFRESH_FAILED:rc={refresh_rc}")

    source_day = (source_truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if not source_day.exists() or not source_day.is_dir():
        raise SystemExit(f"FAIL: SOURCE_SUBMISSIONS_DAY_DIR_MISSING:{source_day}")

    broker_submission_paths = sorted(source_day.glob("*/broker_submission_record.v2.json"))
    if not broker_submission_paths:
        raise SystemExit(f"FAIL: NO_SOURCE_BROKER_SUBMISSIONS_FOUND:{source_day}")

    closure_paths: list[str] = []
    blocker_chain: list[str] = []
    processed_submission_ids: list[str] = []
    for source_broker_path in broker_submission_paths:
        source_submission = read_json_object_v1(source_broker_path)
        submission_id, order_id, perm_id = _broker_identity(source_submission)
        if submission_filter and submission_id != submission_filter:
            continue
        if not submission_id:
            blocker_chain.append("SUBMISSION_ID_MISSING")
            continue
        if not order_id:
            blocker_chain.append("ORDER_ID_MISSING")
            continue
        if not perm_id:
            blocker_chain.append("PERM_ID_MISSING")
            continue

        source_submission_dir = source_broker_path.parent.resolve()
        source_execution_event_path = (source_submission_dir / "execution_event_record.v1.json").resolve()
        canonical_submission_dir = (
            canonical_truth_root
            / "execution_evidence_v1"
            / "submissions"
            / day_utc
            / submission_id
        ).resolve()
        canonical_broker_path = (canonical_submission_dir / "broker_submission_record.v2.json").resolve()
        canonical_execution_event_path = (canonical_submission_dir / "execution_event_record.v1.json").resolve()

        source_execution_event = read_json_object_v1(source_execution_event_path) if source_execution_event_path.exists() else {}
        broker_status_fact = _select_broker_status_fact(
            source_truth_root=source_truth_root,
            day_utc=day_utc,
            order_id=order_id,
            perm_id=perm_id,
        )
        fill_fact = _aggregate_fill_facts(
            source_truth_root=source_truth_root,
            day_utc=day_utc,
            order_id=order_id,
            perm_id=perm_id,
        )
        canonical_submission_ref = _artifact_ref(canonical_broker_path, status_fields=("status",))
        canonical_execution_event_ref = _artifact_ref(
            canonical_execution_event_path,
            status_fields=("status", "raw_broker_status"),
        )

        local_blockers: list[str] = []
        if not canonical_submission_ref["exists"]:
            local_blockers.append("CANONICAL_BROKER_SUBMISSION_MISSING")
        else:
            canonical_submission = read_json_object_v1(canonical_broker_path)
            canonical_submission_id, canonical_order_id, canonical_perm_id = _broker_identity(canonical_submission)
            if canonical_submission_id != submission_id:
                local_blockers.append("CANONICAL_SUBMISSION_ID_MISMATCH")
            if canonical_order_id != order_id:
                local_blockers.append("CANONICAL_ORDER_ID_MISMATCH")
            if canonical_perm_id != perm_id:
                local_blockers.append("CANONICAL_PERM_ID_MISMATCH")

        broker_status_observed = _first_nonempty(
            [
                broker_status_fact["record"].get("status") if isinstance(broker_status_fact.get("record"), Mapping) else "",
                source_execution_event.get("raw_broker_status"),
                source_execution_event.get("status"),
                source_submission.get("status"),
                "UNKNOWN",
            ]
        )
        event_filled_qty = int(source_execution_event.get("filled_qty") or 0)
        event_fill_status = {
            "filled_qty": event_filled_qty,
            "avg_price": str(source_execution_event.get("avg_price") or "0"),
        }
        fact_fill_status = fill_fact["fill_status"]
        fact_fill_observed = int(fact_fill_status.get("filled_qty") or 0) > 0
        event_fill_observed = event_filled_qty > 0
        fill_status_observed = fact_fill_status if fact_fill_observed else event_fill_status
        broker_status_normalized = broker_status_observed.upper()
        broker_status_advanced_observed = broker_status_normalized in {
            "SUBMITTED",
            "ACKNOWLEDGED",
            "PARTIALLY_FILLED",
            "FILLED",
            "CANCELLED",
            "REJECTED",
        }
        explicit_terminal_broker_state_observed = broker_status_normalized in {"FILLED", "CANCELLED", "REJECTED"}
        fill_observed = int(fill_status_observed.get("filled_qty") or 0) > 0
        broker_observation_basis = _derive_observation_basis(
            fact_present=bool(broker_status_fact["match_count"]),
            event_present=bool(source_execution_event_path.exists()),
        )
        fill_observation_basis = _derive_observation_basis(
            fact_present=bool(fill_fact["match_count"]),
            event_present=bool(source_execution_event_path.exists()),
        )
        broker_observation_ref = broker_status_fact["ref"] if broker_status_fact["match_count"] else _artifact_ref(
            source_execution_event_path,
            status_fields=("status", "raw_broker_status"),
        )
        fill_observation_ref = fill_fact["ref"] if fill_fact["match_count"] else _artifact_ref(
            source_execution_event_path,
            status_fields=("status",),
        )
        propagation_state = _derive_propagation_state(
            day_utc=day_utc,
            truth_root=canonical_truth_root,
            submission_id=submission_id,
            fill_observed=fill_observed,
            explicit_terminal_broker_state_observed=explicit_terminal_broker_state_observed,
            hard_blockers=local_blockers,
        )
        lifecycle_states = ["SUBMISSION_RECORDED"]
        if broker_status_advanced_observed:
            lifecycle_states.append("BROKER_STATUS_OBSERVED")
        else:
            lifecycle_states.append("BROKER_STATUS_NOT_YET_OBSERVED")
        if fill_observed:
            lifecycle_states.append("FILL_OBSERVED")
        else:
            lifecycle_states.append("FILL_NOT_YET_OBSERVED")
        if explicit_terminal_broker_state_observed:
            lifecycle_states.append("TERMINAL_STATE_OBSERVED")
        lifecycle_states.append(str(propagation_state["status"]))
        submission_payload_hash = str(canonical_submission_ref.get("sha256") or _artifact_ref(source_broker_path, status_fields=("status",)).get("sha256") or "")
        if not submission_payload_hash:
            submission_payload_hash = canonical_hash_for_c2_artifact_v1(
                {
                    "submission_id": submission_id,
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "status": source_submission.get("status"),
                }
            )
        broker_payload_hash = str(broker_observation_ref.get("sha256") or "")
        if not broker_payload_hash:
            broker_payload_hash = canonical_hash_for_c2_artifact_v1(
                {
                    "broker_status_observed": broker_status_observed,
                    "broker_observation_basis": broker_observation_basis,
                    "broker_observation_match_count": int(broker_status_fact["match_count"]),
                }
            )
        fill_payload_hash = str(fill_observation_ref.get("sha256") or "")
        if not fill_payload_hash:
            fill_payload_hash = canonical_hash_for_c2_artifact_v1(
                {
                    "fill_status_observed": fill_status_observed,
                    "fill_observation_basis": fill_observation_basis,
                    "fill_observation_match_count": int(fill_fact["match_count"]),
                }
            )
        propagation_payload_hash = canonical_hash_for_c2_artifact_v1(
            {
                "status": propagation_state.get("status"),
                "fill_ledger_ref": propagation_state.get("fill_ledger_ref"),
                "positions_snapshot_ref": propagation_state.get("positions_snapshot_ref"),
                "positions_effective_ref": propagation_state.get("positions_effective_ref"),
                "nav_ref": propagation_state.get("nav_ref"),
                "blocker_chain": propagation_state.get("blocker_chain"),
            }
        )
        runtime_ledger_append = append_runtime_ledger_events_v1(
            truth_root=canonical_truth_root,
            day_utc=day_utc,
            events=[
                {
                    "event_type": "SUBMISSION_RECORDED",
                    "produced_utc": produced_utc,
                    "owner_plane": "EXECUTION_PLANE",
                    "owner_tool": "ops/tools/run_canonical_lifecycle_closure_day_v1.py",
                    "owner_run_id": repo_git_sha_v1(),
                    "run_id": repo_git_sha_v1(),
                    "session_id": "",
                    "submission_id": submission_id,
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "payload_ref": str(canonical_broker_path if canonical_submission_ref["exists"] else source_broker_path),
                    "payload_hash": submission_payload_hash,
                    "identity_key": f"{submission_id}:{order_id}:{perm_id}:SUBMISSION",
                    "identity_tuple": {
                        "day_utc": day_utc,
                        "owner_plane": "EXECUTION_PLANE",
                        "owner_run_id": repo_git_sha_v1(),
                        "session_id": "",
                        "submission_id": submission_id,
                        "order_id": order_id,
                        "perm_id": perm_id,
                    },
                    "event_payload_summary": {
                        "sleeve_submission_status": str(source_submission.get("status") or "UNKNOWN"),
                        "canonical_submission_exists": bool(canonical_submission_ref["exists"]),
                    },
                },
                {
                    "event_type": "BROKER_STATUS_OBSERVED" if broker_status_advanced_observed else "BROKER_STATUS_NOT_YET_OBSERVED",
                    "produced_utc": produced_utc,
                    "owner_plane": "EXECUTION_PLANE",
                    "owner_tool": "ops/tools/run_canonical_lifecycle_closure_day_v1.py",
                    "owner_run_id": repo_git_sha_v1(),
                    "run_id": repo_git_sha_v1(),
                    "session_id": "",
                    "submission_id": submission_id,
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "payload_ref": str(broker_observation_ref.get("path") or source_execution_event_path),
                    "payload_hash": broker_payload_hash,
                    "identity_key": f"{submission_id}:{order_id}:{perm_id}:BROKER_STATUS",
                    "identity_tuple": {
                        "day_utc": day_utc,
                        "owner_plane": "EXECUTION_PLANE",
                        "owner_run_id": repo_git_sha_v1(),
                        "session_id": "",
                        "submission_id": submission_id,
                        "order_id": order_id,
                        "perm_id": perm_id,
                    },
                    "event_payload_summary": {
                        "broker_status_observed": broker_status_observed,
                        "broker_observation_basis": broker_observation_basis,
                        "broker_observation_match_count": int(broker_status_fact["match_count"]),
                        "explicit_terminal_broker_state_observed": explicit_terminal_broker_state_observed,
                    },
                },
                {
                    "event_type": "FILL_OBSERVED" if fill_observed else "FILL_NOT_YET_OBSERVED",
                    "produced_utc": produced_utc,
                    "owner_plane": "EXECUTION_PLANE",
                    "owner_tool": "ops/tools/run_canonical_lifecycle_closure_day_v1.py",
                    "owner_run_id": repo_git_sha_v1(),
                    "run_id": repo_git_sha_v1(),
                    "session_id": "",
                    "submission_id": submission_id,
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "payload_ref": str(fill_observation_ref.get("path") or source_execution_event_path),
                    "payload_hash": fill_payload_hash,
                    "identity_key": f"{submission_id}:{order_id}:{perm_id}:FILL",
                    "identity_tuple": {
                        "day_utc": day_utc,
                        "owner_plane": "EXECUTION_PLANE",
                        "owner_run_id": repo_git_sha_v1(),
                        "session_id": "",
                        "submission_id": submission_id,
                        "order_id": order_id,
                        "perm_id": perm_id,
                    },
                    "event_payload_summary": {
                        "fill_status_observed": fill_status_observed,
                        "fill_observation_basis": fill_observation_basis,
                        "fill_observation_match_count": int(fill_fact["match_count"]),
                    },
                },
                {
                    "event_type": str(propagation_state["status"]),
                    "produced_utc": produced_utc,
                    "owner_plane": "EXECUTION_PLANE",
                    "owner_tool": "ops/tools/run_canonical_lifecycle_closure_day_v1.py",
                    "owner_run_id": repo_git_sha_v1(),
                    "run_id": repo_git_sha_v1(),
                    "session_id": "",
                    "submission_id": submission_id,
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "payload_ref": _first_nonempty(
                        [
                            (propagation_state.get("fill_ledger_ref") or {}).get("path"),
                            (propagation_state.get("positions_snapshot_ref") or {}).get("path"),
                            (propagation_state.get("positions_effective_ref") or {}).get("path"),
                            (propagation_state.get("nav_ref") or {}).get("path"),
                            str(canonical_broker_path),
                        ]
                    ),
                    "payload_hash": propagation_payload_hash,
                    "identity_key": f"{submission_id}:{order_id}:{perm_id}:PROPAGATION",
                    "identity_tuple": {
                        "day_utc": day_utc,
                        "owner_plane": "EXECUTION_PLANE",
                        "owner_run_id": repo_git_sha_v1(),
                        "session_id": "",
                        "submission_id": submission_id,
                        "order_id": order_id,
                        "perm_id": perm_id,
                    },
                    "event_payload_summary": {
                        "status": propagation_state.get("status"),
                        "blocker_chain": propagation_state.get("blocker_chain"),
                        "positions_item_count": propagation_state.get("positions_item_count"),
                        "nav_total_observed": propagation_state.get("nav_total_observed"),
                    },
                },
            ],
        )

        closure_payload = {
            "schema_id": "canonical_lifecycle_closure",
            "schema_version": "v1",
            "day_utc": day_utc,
            "produced_utc": produced_utc,
            "producer": producer_block_v1(module="ops/tools/run_canonical_lifecycle_closure_day_v1.py"),
            "canonical_truth_root": str(canonical_truth_root),
            "source_truth_root": str(source_truth_root),
            "submission_id": submission_id,
            "order_id": order_id,
            "perm_id": perm_id,
            "linkage_key": f"{submission_id}:{order_id}:{perm_id}",
            "source_submission_ref": _artifact_ref(source_broker_path, status_fields=("status",)),
            "canonical_submission_ref": canonical_submission_ref,
            "source_execution_event_ref": _artifact_ref(
                source_execution_event_path,
                status_fields=("status", "raw_broker_status"),
            ),
            "canonical_execution_event_ref": canonical_execution_event_ref,
            "broker_observation_ref": broker_observation_ref,
            "fill_observation_ref": fill_observation_ref,
            "broker_observation_basis": broker_observation_basis,
            "fill_observation_basis": fill_observation_basis,
            "broker_observation_match_count": int(broker_status_fact["match_count"]),
            "fill_observation_match_count": int(fill_fact["match_count"]),
            "sleeve_submission_status": str(source_submission.get("status") or "UNKNOWN"),
            "broker_status_observed": broker_status_observed,
            "broker_status_advanced_observed": broker_status_advanced_observed,
            "explicit_terminal_broker_state_observed": explicit_terminal_broker_state_observed,
            "fill_status_observed": fill_status_observed,
            "fill_observed": fill_observed,
            "canonical_lifecycle_status": "CANONICALIZED" if not local_blockers else "BLOCKED",
            "lifecycle_states": lifecycle_states,
            "broker_observation_state": "BROKER_STATUS_NOT_YET_OBSERVED" if not broker_status_advanced_observed else (
                "TERMINAL_STATE_OBSERVED" if explicit_terminal_broker_state_observed else "BROKER_STATUS_OBSERVED"
            ),
            "fill_observation_state": "FILL_NOT_YET_OBSERVED" if not fill_observed else "FILL_OBSERVED",
            "propagation_state": propagation_state,
            "blocker_chain": local_blockers,
            "runtime_ledger_projection": projection_over_runtime_ledger_v1(
                truth_root=canonical_truth_root,
                day_utc=day_utc,
                append_result=runtime_ledger_append,
                projection_notice=(
                    "Operator-facing lifecycle closure report derived from canonical runtime ledger execution truth "
                    "plus canonical execution evidence."
                ),
            ),
        }

        out_path = resolve_canonical_lifecycle_closure_path(
            truth_root=canonical_truth_root,
            day_utc=day_utc,
            submission_id=submission_id,
        )
        atomic_write_idempotent_validated_json_v1(
            path=out_path,
            payload=closure_payload,
            schema_relpath=OUTPUT_SCHEMA_RELPATH,
            volatile_field_names=("produced_utc",),
        )
        closure_paths.append(str(out_path))
        processed_submission_ids.append(submission_id)
        blocker_chain.extend(local_blockers)

    if submission_filter and submission_filter not in processed_submission_ids:
        raise SystemExit(f"FAIL: FILTERED_SUBMISSION_NOT_FOUND:{submission_filter}")
    if not processed_submission_ids:
        raise SystemExit(f"FAIL: NO_MATCHING_SUBMISSIONS_FOR_DAY:{day_utc}")

    unique_blockers = list(dict.fromkeys(code for code in blocker_chain if str(code).strip()))
    summary = {
        "day_utc": day_utc,
        "canonical_truth_root": str(canonical_truth_root),
        "source_truth_root": str(source_truth_root),
        "processed_submission_ids": processed_submission_ids,
        "closure_paths": closure_paths,
        "blocker_chain": unique_blockers,
        "status": "PASS" if not unique_blockers else "FAIL",
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if not unique_blockers else 2


if __name__ == "__main__":
    raise SystemExit(main())
