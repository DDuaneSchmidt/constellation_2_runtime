#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.verified_runtime_graph_v1 import (  # noqa: E402
    chatgpt_hydrate_packet_path_v1,
    file_hash_v1,
    git_commit_hash_v1,
    graph_staleness_warnings_v1,
    load_graph_v1,
    portal_runtime_model_path_v1,
    render_query_response_v1,
    stable_hash_v1,
    verified_runtime_graph_path_v1,
    write_canonical_json_v1,
)

SCHEMA_VERSION = "v1"
GENERATOR_NAME = "run_aegis_operator_smoke_test_v1"
GENERATOR_VERSION = "aegis_operator_smoke_test.v1"


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _smoke_dir(*, truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports" / "aegis_operator_smoke_test_v1" / day_utc


def smoke_summary_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return _smoke_dir(truth_root=truth_root, day_utc=day_utc) / "aegis_operator_smoke_test.v1.json"


def _excerpt(text: str, limit: int = 4000) -> str:
    return str(text or "")[-limit:]


def _hydrate_graph_hash(packet_text: str) -> str:
    match = re.search(r"^- verified_runtime_graph_hash:\s*(\S+)\s*$", packet_text, flags=re.MULTILINE)
    return match.group(1) if match else ""


def _check_status(ok: bool) -> str:
    return "PASS" if ok else "BLOCKED"


def build_aegis_operator_smoke_test_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    truth_root = Path(truth_root).expanduser().resolve()
    generated_at = _now_utc()
    graph_path = verified_runtime_graph_path_v1(truth_root=truth_root, day_utc=day_utc)
    portal_path = portal_runtime_model_path_v1(truth_root=truth_root, day_utc=day_utc)
    hydrate_path = chatgpt_hydrate_packet_path_v1(truth_root=truth_root, day_utc=day_utc)
    blockers: list[str] = []
    input_hashes: dict[str, str] = {}

    graph: dict[str, Any] = {}
    portal_model: dict[str, Any] = {}
    hydrate_text = ""

    try:
        portal_model = _load_json_object(portal_path)
        input_hashes[str(portal_path)] = file_hash_v1(portal_path)
        portal_open_status = "PASS"
    except Exception as exc:
        portal_open_status = "BLOCKED"
        blockers.append(f"PORTAL_MODEL_UNREADABLE:{portal_path}:{exc}")

    try:
        graph = load_graph_v1(truth_root=truth_root, day_utc=day_utc)
        input_hashes[str(graph_path)] = file_hash_v1(graph_path)
        graph_open_status = "PASS"
    except Exception as exc:
        graph_open_status = "BLOCKED"
        blockers.append(f"VERIFIED_GRAPH_UNREADABLE:{graph_path}:{exc}")

    try:
        hydrate_text = hydrate_path.read_text(encoding="utf-8")
        input_hashes[str(hydrate_path)] = file_hash_v1(hydrate_path)
        hydrate_open_status = "PASS"
    except Exception as exc:
        hydrate_open_status = "BLOCKED"
        blockers.append(f"HYDRATE_PACKET_UNREADABLE:{hydrate_path}:{exc}")

    stale_warnings = graph_staleness_warnings_v1(graph=graph, truth_root=truth_root, day_utc=day_utc) if graph else ["GRAPH_NOT_LOADED"]
    if stale_warnings:
        blockers.extend(stale_warnings)

    claim_lookup_text = render_query_response_v1(graph=graph, query="claim: trade advice allowed", warnings=stale_warnings) if graph else ""
    why_blocked_text = render_query_response_v1(graph=graph, query="why blocked?", warnings=stale_warnings) if graph else ""
    claim_lookup_ok = bool(claim_lookup_text and "- allowed: false" in claim_lookup_text and "kernel_evidence:" in claim_lookup_text)
    why_blocked_ok = bool(why_blocked_text and "Blockers:" in why_blocked_text and "Evidence:" in why_blocked_text)
    if not claim_lookup_ok:
        blockers.append("CLAIM_LOOKUP_DID_NOT_PROVE_TRADE_ADVICE_FALSE")
    if not why_blocked_ok:
        blockers.append("WHY_BLOCKED_QUERY_MISSING_BLOCKERS_OR_EVIDENCE")

    packet_graph_hash = _hydrate_graph_hash(hydrate_text)
    graph_output_hash = str(graph.get("output_hash") or "") if graph else ""
    hydrate_day_ok = f"- day_utc: {day_utc}" in hydrate_text if hydrate_text else False
    hydrate_hash_ok = bool(packet_graph_hash and graph_output_hash and packet_graph_hash == graph_output_hash)
    hydrate_fresh_ok = bool(hydrate_open_status == "PASS" and hydrate_day_ok and hydrate_hash_ok and not stale_warnings)
    if hydrate_open_status == "PASS" and not hydrate_day_ok:
        blockers.append("HYDRATE_PACKET_DAY_MISMATCH")
    if hydrate_open_status == "PASS" and not hydrate_hash_ok:
        blockers.append("HYDRATE_PACKET_GRAPH_HASH_MISMATCH")

    checks = {
        "portal_model_open": {
            "status": portal_open_status,
            "path": str(portal_path),
            "model_status": portal_model.get("graph_status") or portal_model.get("portal_status") or "UNKNOWN",
        },
        "claim_lookup_trade_advice_allowed": {
            "status": _check_status(claim_lookup_ok),
            "query": "claim: trade advice allowed",
            "expected_allowed": False,
            "excerpt": _excerpt(claim_lookup_text),
        },
        "why_blocked_query": {
            "status": _check_status(why_blocked_ok),
            "query": "why blocked?",
            "runtime_blocker_count": len(graph.get("runtime_blockers") or []) if graph else 0,
            "audit_blocker_count": len(graph.get("audit_blockers") or []) if graph else 0,
            "excerpt": _excerpt(why_blocked_text),
        },
        "hydrate_freshness": {
            "status": _check_status(hydrate_fresh_ok),
            "path": str(hydrate_path),
            "hydrate_day_matches": hydrate_day_ok,
            "hydrate_graph_hash": packet_graph_hash,
            "verified_graph_output_hash": graph_output_hash,
            "graph_staleness_warnings": stale_warnings,
        },
    }
    summary_status = "PASS" if all(check.get("status") == "PASS" for check in checks.values()) else "BLOCKED"
    payload: dict[str, Any] = {
        "schema_id": "aegis_operator_smoke_test",
        "schema_version": SCHEMA_VERSION,
        "run_id": f"aegis-operator-smoke:{day_utc}:{generated_at}",
        "day_utc": day_utc,
        "generated_at": generated_at,
        "generator_name": GENERATOR_NAME,
        "generator_version": GENERATOR_VERSION,
        "git_commit_hash": git_commit_hash_v1(),
        "truth_root": str(truth_root),
        "summary_status": summary_status,
        "status": summary_status,
        "blockers": sorted(set(blockers)),
        "runtime_truth_classification": (graph.get("readiness_linkage_to_runtime_truth_kernel") or {}).get("runtime_truth_classification") if graph else "UNKNOWN",
        "runtime_readiness_status": graph.get("runtime_readiness_status") if graph else "UNKNOWN",
        "portal_runtime_model_path": str(portal_path),
        "verified_runtime_graph_path": str(graph_path),
        "hydrate_packet_path": str(hydrate_path),
        "checks": checks,
        "safety_policy_summary": {
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "manual_capture_policy_changed": False,
            "readiness_inference_policy": "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE",
        },
        "input_artifact_hashes": input_hashes,
        "output_hash": "",
    }
    payload["output_hash"] = stable_hash_v1({**payload, "output_hash": ""})
    return payload


def write_aegis_operator_smoke_test_v1(*, truth_root: Path, day_utc: str) -> Path:
    payload = build_aegis_operator_smoke_test_v1(truth_root=truth_root, day_utc=day_utc)
    out_path = smoke_summary_path_v1(truth_root=Path(truth_root).expanduser().resolve(), day_utc=day_utc)
    write_canonical_json_v1(out_path, payload)
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_operator_smoke_test_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--allow-blocked-exit-zero", action="store_true")
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    out_path = write_aegis_operator_smoke_test_v1(truth_root=truth_root, day_utc=str(args.day))
    payload = _load_json_object(out_path)
    print("AEGIS OPERATOR SMOKE TEST v1")
    print(f"day_utc: {payload.get('day_utc')}")
    print(f"summary_status: {payload.get('summary_status')}")
    print(f"path: {out_path}")
    blockers = payload.get("blockers") if isinstance(payload.get("blockers"), list) else []
    if blockers:
        print("blockers:")
        for item in blockers[:20]:
            print(f"- {item}")
    if payload.get("summary_status") == "PASS" or args.allow_blocked_exit_zero:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
