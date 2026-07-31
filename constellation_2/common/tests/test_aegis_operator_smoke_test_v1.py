from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.verified_runtime_graph_v1 import file_hash_v1, stable_hash_v1, write_canonical_json_v1
from ops.tools.run_aegis_operator_smoke_test_v1 import (
    build_aegis_operator_smoke_test_v1,
    smoke_summary_path_v1,
    write_aegis_operator_smoke_test_v1,
)

DAY = "2026-05-22"


def _write_minimal_verified_runtime_artifacts(root: Path, *, hydrate: bool = True) -> dict:
    day_root = root / "reports" / "aegis_verified_runtime_graph_v1" / DAY
    kernel_path = root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json"
    runtime_eval_path = root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_evaluation.v1.json"
    kernel = {
        "schema_id": "aegis_runtime_truth_kernel",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-05-22T21:00:00Z",
        "runtime_truth_classification": "PARTIAL_CONTEXT",
        "highest_readiness_layer": "BLOCKED",
    }
    runtime_eval = {"schema_id": "runtime_evaluation", "day_utc": DAY, "policy_gates": {"trade_advice_allowed": False}}
    write_canonical_json_v1(kernel_path, kernel)
    write_canonical_json_v1(runtime_eval_path, runtime_eval)
    graph = {
        "schema_id": "aegis_verified_runtime_graph",
        "schema_version": "v1",
        "run_id": "graph-test-run",
        "day_utc": DAY,
        "generated_at": "2026-05-22T21:05:00Z",
        "generator_name": "build_aegis_verified_runtime_graph_v1",
        "generator_version": "aegis_verified_runtime_graph.v1",
        "git_commit_hash": "TEST",
        "truth_root": str(root),
        "graph_status": "READY",
        "runtime_readiness_status": "BLOCKED",
        "audit_blockers": [],
        "runtime_blockers": ["runtime_truth_kernel:TRADE_ADVICE_ALLOWED:BLOCKED_BY_KERNEL"],
        "readiness_linkage_to_runtime_truth_kernel": {
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "highest_readiness_layer": "BLOCKED",
            "runtime_truth_kernel_path": str(kernel_path),
            "runtime_evaluation_path": str(runtime_eval_path),
            "blocked_capabilities": ["TRADE_ADVICE_ALLOWED"],
            "allowed_capabilities": [],
        },
        "policy_gates": {"trade_advice_allowed": False, "manual_trade_capture_allowed": False},
        "safety_invariants": {"broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
        "evidence_links": [
            {"evidence_id": "kernel", "artifact_path": str(kernel_path), "validation_status": "VALID", "freshness_status": "CURRENT", "hash_verification_status": "VERIFIED"}
        ],
        "do_not_claim": ["Trade advice remains forbidden until every runtime truth dependency is evidence-backed and current."],
        "input_artifact_hashes": {str(kernel_path): file_hash_v1(kernel_path)},
        "output_hash": "",
    }
    graph["output_hash"] = stable_hash_v1({**graph, "output_hash": ""})
    graph_path = day_root / "verified_runtime_graph.v1.json"
    portal_path = day_root / "portal_runtime_model.v1.json"
    write_canonical_json_v1(graph_path, graph)
    portal = {
        "schema_id": "portal_runtime_model",
        "schema_version": "v1",
        "day_utc": DAY,
        "graph_status": "READY",
        "portal_status": "READY",
        "runtime_readiness_status": "BLOCKED",
        "derivation_source": "verified_runtime_graph.v1.json",
        "readiness_inference_policy": "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE",
        "top_blockers": graph["runtime_blockers"],
    }
    write_canonical_json_v1(portal_path, portal)
    if hydrate:
        hydrate_path = day_root / "chatgpt_hydrate_packet.v1.md"
        hydrate_path.write_text(
            "\n".join([
                "# Aegis ChatGPT Hydrate Packet v1",
                "",
                f"- day_utc: {DAY}",
                f"- verified_runtime_graph: {graph_path}",
                f"- verified_runtime_graph_hash: {graph['output_hash']}",
                "",
            ]),
            encoding="utf-8",
        )
    return {"graph": graph, "portal_path": portal_path, "kernel_path": kernel_path}


def test_operator_smoke_passes_when_portal_queries_and_hydrate_are_fresh(tmp_path: Path) -> None:
    _write_minimal_verified_runtime_artifacts(tmp_path)

    payload = build_aegis_operator_smoke_test_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary_status"] == "PASS"
    assert payload["checks"]["portal_model_open"]["status"] == "PASS"
    assert payload["checks"]["claim_lookup_trade_advice_allowed"]["status"] == "PASS"
    assert "- allowed: false" in payload["checks"]["claim_lookup_trade_advice_allowed"]["excerpt"]
    assert payload["checks"]["why_blocked_query"]["status"] == "PASS"
    assert payload["checks"]["hydrate_freshness"]["status"] == "PASS"
    assert payload["safety_policy_summary"]["trade_advice_allowed"] is False


def test_operator_smoke_blocks_when_hydrate_missing(tmp_path: Path) -> None:
    _write_minimal_verified_runtime_artifacts(tmp_path, hydrate=False)

    payload = build_aegis_operator_smoke_test_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary_status"] == "BLOCKED"
    assert payload["checks"]["hydrate_freshness"]["status"] == "BLOCKED"
    assert any(item.startswith("HYDRATE_PACKET_UNREADABLE") for item in payload["blockers"])


def test_operator_smoke_writes_single_summary_artifact(tmp_path: Path) -> None:
    _write_minimal_verified_runtime_artifacts(tmp_path)

    out_path = write_aegis_operator_smoke_test_v1(truth_root=tmp_path, day_utc=DAY)

    assert out_path == smoke_summary_path_v1(truth_root=tmp_path, day_utc=DAY)
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["schema_id"] == "aegis_operator_smoke_test"
    assert payload["summary_status"] == "PASS"
    assert payload["output_hash"]
