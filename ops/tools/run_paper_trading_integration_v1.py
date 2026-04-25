#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_fact_plane_v1 import (
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_canonical_lifecycle_closure_path,
    resolve_execution_reconciliation_path,
    resolve_paper_session_bootstrap_path,
    resolve_paper_submit_smoke_test_report_path,
    resolve_paper_trading_integration_path,
)
from constellation_2.common.runtime_ledger_v1 import (
    append_runtime_ledger_events_v1,
    projection_over_runtime_ledger_v1,
    read_runtime_ledger_events_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


OUTPUT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_integration.v1.schema.json"
PAPER_ENVIRONMENT = "PAPER"
PRIMARY_SLEEVE_ID = "PRIMARY"


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
    return resolve_decision_truth_root_bridge_v1(
        "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_paper_trading_integration_v1.py",
    )


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


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write_immutable(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if _sha256_bytes(path.read_bytes()) == _sha256_bytes(payload):
            return
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES:{path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _broker_identity(broker_payload: Mapping[str, Any]) -> tuple[str, str, str]:
    broker_ids = broker_payload.get("broker_ids") if isinstance(broker_payload.get("broker_ids"), dict) else {}
    submission_id = str(broker_payload.get("submission_id") or "").strip()
    order_id = str(broker_ids.get("order_id") or "").strip()
    perm_id = str(broker_ids.get("perm_id") or "").strip()
    return submission_id, order_id, perm_id


def _resolve_reconciliation_refs(*, canonical_truth_root: Path, day_utc: str) -> tuple[dict[str, Any], dict[str, Any]]:
    alias_path = resolve_execution_reconciliation_path(truth_root=canonical_truth_root, day_utc=day_utc)
    versioned_candidates = sorted(
        (
            canonical_truth_root
            / "reports"
            / "execution_reconciliation_v1"
            / day_utc
        ).glob("*/execution_reconciliation.v1.json"),
        key=lambda item: (item.stat().st_mtime_ns, str(item)),
    )
    versioned_path = versioned_candidates[-1].resolve() if versioned_candidates else alias_path
    primary_ref = _artifact_ref(versioned_path, status_fields=("status",))
    alias_ref = _artifact_ref(alias_path, status_fields=("status",))
    return primary_ref, alias_ref


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_trading_integration_v1")
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
    produced_utc = now_utc_iso_v1()
    runtime_ledger_events = read_runtime_ledger_events_v1(truth_root=canonical_truth_root, day_utc=day_utc)

    bootstrap_path = resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=day_utc)
    smoke_report_path = resolve_paper_submit_smoke_test_report_path(sleeve_truth_root=source_truth_root, day_utc=day_utc)
    reconciliation_ref, reconciliation_alias_ref = _resolve_reconciliation_refs(
        canonical_truth_root=canonical_truth_root,
        day_utc=day_utc,
    )

    smoke_report = read_json_object_v1(smoke_report_path) if smoke_report_path.exists() else {}
    bootstrap_ref = _artifact_ref(
        bootstrap_path,
        status_fields=("bootstrap_semantic_status", "bootstrap_status"),
    )
    smoke_report_ref = _artifact_ref(smoke_report_path, status_fields=("kernel_run_outcome",))
    request_path_raw = str(smoke_report.get("request_path") or "").strip()
    if request_path_raw:
        request_path = Path(request_path_raw).expanduser().resolve()
        request_ref = _artifact_ref(request_path, status_fields=("request_nonce",))
    else:
        request_ref = {
            "path": "",
            "exists": False,
            "sha256": "",
            "status": "",
            "reason_codes": [],
        }
    submission_id = str(args.submission_id or "").strip() or str(smoke_report.get("submission_id") or "").strip()
    if not submission_id:
        raise SystemExit(f"FAIL: SUBMISSION_ID_REQUIRED:{smoke_report_path}")

    sleeve_broker_path = Path(
        str(smoke_report.get("broker_submission_record_path") or "")
    ).expanduser().resolve() if smoke_report.get("broker_submission_record_path") else (
        source_truth_root
        / "execution_evidence_v1"
        / "submissions"
        / day_utc
        / submission_id
        / "broker_submission_record.v2.json"
    ).resolve()
    sleeve_execution_event_path = (
        source_truth_root
        / "execution_evidence_v1"
        / "submissions"
        / day_utc
        / submission_id
        / "execution_event_record.v1.json"
    ).resolve()
    sleeve_broker = read_json_object_v1(sleeve_broker_path) if sleeve_broker_path.exists() else {}
    sleeve_execution_event = read_json_object_v1(sleeve_execution_event_path) if sleeve_execution_event_path.exists() else {}
    source_submission_id, order_id, perm_id = _broker_identity(sleeve_broker)
    if source_submission_id and source_submission_id != submission_id:
        raise SystemExit(
            f"FAIL: SUBMISSION_ID_MISMATCH:expected={submission_id}:observed={source_submission_id}:path={sleeve_broker_path}"
        )

    canonical_lifecycle_path = resolve_canonical_lifecycle_closure_path(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
    )
    canonical_lifecycle = read_json_object_v1(canonical_lifecycle_path) if canonical_lifecycle_path.exists() else {}
    if not order_id:
        order_id = str(canonical_lifecycle.get("order_id") or "").strip()
    if not perm_id:
        perm_id = str(canonical_lifecycle.get("perm_id") or "").strip()

    reconciliation_path = Path(str(reconciliation_ref.get("path") or "")).expanduser().resolve()
    reconciliation = read_json_object_v1(reconciliation_path) if reconciliation_ref["exists"] else {}
    canonical_lifecycle_ref = _artifact_ref(canonical_lifecycle_path, status_fields=("canonical_lifecycle_status",))
    canonical_lifecycle_status = str(canonical_lifecycle.get("canonical_lifecycle_status") or "MISSING").strip() or "MISSING"
    canonical_reconciliation_status = str(reconciliation.get("status") or "MISSING").strip() or "MISSING"
    sleeve_submission_status = str(sleeve_broker.get("status") or "MISSING").strip() or "MISSING"
    broker_status_observed = _first_nonempty(
        [
            canonical_lifecycle.get("broker_status_observed"),
            sleeve_execution_event.get("raw_broker_status"),
            sleeve_execution_event.get("status"),
            sleeve_broker.get("status"),
            "UNKNOWN",
        ]
    )
    fill_status_observed = canonical_lifecycle.get("fill_status_observed")
    if not isinstance(fill_status_observed, dict):
        fill_status_observed = {
            "filled_qty": int(sleeve_execution_event.get("filled_qty") or 0),
            "avg_price": str(sleeve_execution_event.get("avg_price") or "0"),
        }

    broker_status_advanced_observed = broker_status_observed.upper() in {
        "SUBMITTED",
        "ACKNOWLEDGED",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCELLED",
        "REJECTED",
    }
    explicit_terminal_broker_state_observed = broker_status_observed.upper() in {"FILLED", "CANCELLED", "REJECTED"}
    fill_observed = int(fill_status_observed.get("filled_qty") or 0) > 0
    lifecycle_states = canonical_lifecycle.get("lifecycle_states")
    if not isinstance(lifecycle_states, list):
        lifecycle_states = []
    propagation_state = canonical_lifecycle.get("propagation_state")
    if not isinstance(propagation_state, dict):
        propagation_state = {"status": "MISSING", "blocker_chain": []}
    propagation_status = str(propagation_state.get("status") or "MISSING").strip() or "MISSING"
    propagation_blockers = propagation_state.get("blocker_chain")
    if not isinstance(propagation_blockers, list):
        propagation_blockers = []

    broker_observation_state = str(canonical_lifecycle.get("broker_observation_state") or "MISSING").strip() or "MISSING"
    fill_observation_state = str(canonical_lifecycle.get("fill_observation_state") or "MISSING").strip() or "MISSING"
    broker_observation_basis = str(canonical_lifecycle.get("broker_observation_basis") or "NONE").strip() or "NONE"
    fill_observation_basis = str(canonical_lifecycle.get("fill_observation_basis") or "NONE").strip() or "NONE"
    broker_observation_ref = canonical_lifecycle.get("broker_observation_ref")
    if not isinstance(broker_observation_ref, dict):
        broker_observation_ref = {
            "path": "",
            "exists": False,
            "sha256": "",
            "status": "",
            "reason_codes": [],
        }
    fill_observation_ref = canonical_lifecycle.get("fill_observation_ref")
    if not isinstance(fill_observation_ref, dict):
        fill_observation_ref = {
            "path": "",
            "exists": False,
            "sha256": "",
            "status": "",
            "reason_codes": [],
        }

    blocker_chain: list[str] = []
    if not sleeve_broker_path.exists():
        blocker_chain.append("BROKER_SUBMISSION_RECORD_MISSING")
    if not submission_id or not order_id or not perm_id:
        blocker_chain.append("BROKER_ID_LINKAGE_MISSING")
    if not canonical_lifecycle_ref["exists"] or canonical_lifecycle_status != "CANONICALIZED":
        blocker_chain.append("CANONICAL_LIFECYCLE_CLOSURE_MISSING_OR_BLOCKED")
    if not reconciliation_ref["exists"]:
        blocker_chain.append("CANONICAL_RECONCILIATION_MISSING")
    elif "NO_SUBMISSIONS_FOUND" in _extract_reason_codes(reconciliation):
        blocker_chain.append("CANONICAL_RECONCILIATION_NO_SUBMISSIONS")
    if propagation_status == "PROPAGATION_BLOCKED":
        blocker_chain.append("CANONICAL_PROPAGATION_BLOCKED")
    if fill_observed and propagation_status != "PROPAGATION_COMPLETE":
        blocker_chain.append("DOWNSTREAM_PROPAGATION_NOT_COMPLETE_AFTER_FILL")
    if not (broker_status_advanced_observed or fill_observed or explicit_terminal_broker_state_observed):
        blocker_chain.append("BROKER_STATUS_ADVANCEMENT_NOT_OBSERVED")

    verdict_basis = {
        "broker_submission_record_exists": sleeve_broker_path.exists(),
        "broker_id_linkage_exists": bool(submission_id and order_id and perm_id),
        "canonical_lifecycle_exists": bool(canonical_lifecycle_ref["exists"] and canonical_lifecycle_status == "CANONICALIZED"),
        "canonical_reconciliation_exists": bool(reconciliation_ref["exists"]),
        "canonical_reconciliation_no_submissions_clear": "NO_SUBMISSIONS_FOUND" not in _extract_reason_codes(reconciliation),
        "canonical_reconciliation_ref_is_versioned": reconciliation_ref["path"] != reconciliation_alias_ref["path"],
        "broker_observation_explicit": broker_observation_state != "MISSING" and broker_observation_ref.get("exists", False),
        "fill_observation_explicit": fill_observation_state != "MISSING" and fill_observation_ref.get("exists", False),
        "propagation_state_explicit": propagation_status != "MISSING",
        "broker_status_advanced_observed": broker_status_advanced_observed,
        "fill_observed": fill_observed,
        "explicit_terminal_broker_state_observed": explicit_terminal_broker_state_observed,
        "propagation_complete": propagation_status == "PROPAGATION_COMPLETE",
    }

    payload = {
        "schema_id": "paper_trading_integration",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "produced_utc": produced_utc,
        "producer": producer_block_v1(module="ops/tools/run_paper_trading_integration_v1.py"),
        "bootstrap_ref": bootstrap_ref,
        "request_ref": request_ref,
        "smoke_report_ref": smoke_report_ref,
        "broker_observation_ref": broker_observation_ref,
        "fill_observation_ref": fill_observation_ref,
        "canonical_lifecycle_ref": canonical_lifecycle_ref,
        "canonical_reconciliation_ref": reconciliation_ref,
        "canonical_reconciliation_alias_ref": reconciliation_alias_ref,
        "submission_id": submission_id,
        "order_id": order_id,
        "perm_id": perm_id,
        "sleeve_submission_status": sleeve_submission_status,
        "canonical_lifecycle_status": canonical_lifecycle_status,
        "lifecycle_states": lifecycle_states,
        "broker_status_observed": broker_status_observed,
        "broker_observation_state": broker_observation_state,
        "fill_observation_state": fill_observation_state,
        "broker_observation_basis": broker_observation_basis,
        "fill_observation_basis": fill_observation_basis,
        "fill_status_observed": {
            "filled_qty": int(fill_status_observed.get("filled_qty") or 0),
            "avg_price": str(fill_status_observed.get("avg_price") or "0"),
        },
        "propagation_state": propagation_state,
        "canonical_reconciliation_status": canonical_reconciliation_status,
        "broker_status_advanced_observed": broker_status_advanced_observed,
        "explicit_terminal_broker_state_observed": explicit_terminal_broker_state_observed,
        "audit_guidance": {
            "immutable_snapshot_refs_preferred": True,
            "preferred_canonical_reconciliation_ref": "canonical_reconciliation_ref",
            "alias_refs_best_effort_only": True,
        },
        "verdict_basis": verdict_basis,
        "integration_verdict": "PASS" if not blocker_chain else "FAIL",
        "blocker_chain": list(dict.fromkeys(
            [str(code).strip() for code in blocker_chain if str(code).strip()] +
            [str(code).strip() for code in propagation_blockers if str(code).strip() and propagation_status == "PROPAGATION_BLOCKED"]
        )),
    }
    integration_event_payload_hash = canonical_hash_for_c2_artifact_v1(
        {
            "integration_verdict": payload["integration_verdict"],
            "blocker_chain": payload["blocker_chain"],
            "submission_id": submission_id,
            "order_id": order_id,
            "perm_id": perm_id,
            "verdict_basis": verdict_basis,
        }
    )
    out_path = resolve_paper_trading_integration_path(truth_root=canonical_truth_root, day_utc=day_utc)
    runtime_ledger_append = append_runtime_ledger_events_v1(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        events=[
            {
                "event_type": "INTEGRATION_PASS" if payload["integration_verdict"] == "PASS" else "INTEGRATION_FAIL",
                "produced_utc": produced_utc,
                "owner_plane": "PROJECTION_PLANE",
                "owner_tool": "ops/tools/run_paper_trading_integration_v1.py",
                "owner_run_id": str(smoke_report.get("kernel_run_id") or submission_id or day_utc),
                "run_id": str(smoke_report.get("kernel_run_id") or submission_id or day_utc),
                "session_id": "",
                "submission_id": submission_id,
                "order_id": order_id,
                "perm_id": perm_id,
                "payload_ref": str(out_path),
                "payload_hash": integration_event_payload_hash,
                "identity_key": f"{submission_id}:{order_id}:{perm_id}:INTEGRATION",
                "identity_tuple": {
                    "day_utc": day_utc,
                    "owner_plane": "PROJECTION_PLANE",
                    "owner_run_id": str(smoke_report.get("kernel_run_id") or submission_id or day_utc),
                    "session_id": "",
                    "submission_id": submission_id,
                    "order_id": order_id,
                    "perm_id": perm_id,
                },
                "event_payload_summary": {
                    "integration_verdict": payload["integration_verdict"],
                    "blocker_chain": payload["blocker_chain"],
                    "broker_status_observed": broker_status_observed,
                    "propagation_status": propagation_status,
                },
            }
        ],
    )
    payload["runtime_ledger_projection"] = projection_over_runtime_ledger_v1(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        append_result=runtime_ledger_append,
        matched_event_types=sorted(
            {
                str(event.get("event_type") or "").strip()
                for event in runtime_ledger_events
                if str(event.get("submission_id") or "").strip() == submission_id
                and str(event.get("event_type") or "").strip()
            }
        ),
        projection_notice=(
            "Paper trading integration is a non-authoritative projection over canonical runtime ledger truth, "
            "canonical lifecycle closure, and canonical reconciliation."
        ),
    )
    validate_against_repo_schema_v1(payload, REPO_ROOT, OUTPUT_SCHEMA_RELPATH)
    payload_bytes = canonical_json_bytes_v1(payload) + b"\n"
    payload_sha = _sha256_bytes(payload_bytes)
    versioned_path = (
        canonical_truth_root
        / "reports"
        / "paper_trading_integration_v1"
        / day_utc
        / payload_sha
        / "paper_trading_integration.v1.json"
    ).resolve()
    _write_immutable(versioned_path, payload_bytes)

    alias_status = "REUSED"
    if out_path.exists():
        if _sha256_bytes(out_path.read_bytes()) != payload_sha:
            alias_status = "STALE_ALIAS"
    else:
        _write_immutable(out_path, payload_bytes)
        alias_status = "WRITTEN"

    print(json.dumps({
        "path": str(out_path),
        "versioned_path": str(versioned_path),
        "alias_status": alias_status,
        "integration_verdict": payload["integration_verdict"],
        "blocker_chain": payload["blocker_chain"],
    }, sort_keys=True))
    return 0 if payload["integration_verdict"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
