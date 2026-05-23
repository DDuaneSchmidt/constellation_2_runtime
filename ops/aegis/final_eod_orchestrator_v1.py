from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.data_registry_v1 import build_data_registry_v1, write_data_registry_v1
from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1, write_domain_certification_report_v1
from ops.aegis.domain_repair_orchestrator_v1 import run_domain_repair_orchestration_v1
from ops.aegis.market_data.symbol_map_v1 import build_runtime_symbol_universe_v1
from ops.aegis.market_data_inputs_v1 import build_market_data_inputs_v1, emit_market_data_inputs_events_v1, write_market_data_inputs_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT
from ops.aegis.sleeve_readiness_v1 import build_sleeve_readiness_v1, write_sleeve_readiness_v1
from ops.tools.manage_us_equities_eod_source_v1 import certify_tiingo_provider_v1

SCHEMA_ID = "aegis_final_eod_certification_run_ledger"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "final_eod_certification_run_ledger_v1"

SAFETY = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file_v1(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ledger_dir_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc


def ledger_jsonl_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return ledger_dir_v1(truth_root=truth_root, day_utc=day_utc) / "final_eod_certification_run_ledger.v1.jsonl"


def ledger_latest_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return ledger_dir_v1(truth_root=truth_root, day_utc=day_utc) / "final_eod_certification_run_ledger.latest.v1.json"


def _hashes(paths: dict[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in (paths or {}).items():
        path = Path(str(value))
        if path.exists() and path.is_file():
            out[str(key)] = sha256_file_v1(path)
    return out


def _append_stage(*, truth_root: Path, day_utc: str, run_id: str, stage: str, status: str, started_at: str, completed_at: str, input_hashes: dict[str, str] | None = None, output_paths: dict[str, Any] | None = None, result: dict[str, Any] | None = None, failure_reason: str = "") -> dict[str, Any]:
    row = {
        "schema_id": "aegis_final_eod_certification_stage",
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "day_utc": day_utc,
        "stage": stage,
        "status": status,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "input_hashes": input_hashes or {},
        "output_paths": output_paths or {},
        "output_hashes": _hashes(output_paths),
        "failure_reason": failure_reason,
        "result_summary": {key: (result or {}).get(key) for key in ("ok", "status", "result_status", "certification_status", "provider_fetch_status", "provider_fetch_skipped") if key in (result or {})},
        **SAFETY,
    }
    out = ledger_jsonl_path_v1(truth_root=truth_root, day_utc=day_utc)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
    return row


def _write_latest(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = ledger_latest_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"json": str(path), "sha256": sha256_file_v1(path)}


def final_run_outcome_v1(*, status: str, stages: list[dict[str, Any]]) -> str:
    normalized = str(status or "").upper()
    stage_statuses = [str(row.get("status") or "").upper() for row in stages if isinstance(row, dict)]
    if normalized in {"SUCCEEDED", "RUN_COMPLETED"}:
        return "RUN_COMPLETED"
    if normalized in {"INTERRUPTED", "ABORTED", "RUN_ABORTED"}:
        return "RUN_ABORTED"
    if normalized in {"SUPERSEDED", "RUN_SUPERSEDED"}:
        return "RUN_SUPERSEDED"
    if normalized in {"FAILED", "RUN_FAILED"} and any(row == "SUCCEEDED" for row in stage_statuses):
        return "RUN_PARTIAL"
    if normalized in {"FAILED", "RUN_FAILED"}:
        return "RUN_FAILED"
    return "RUN_UNVERIFIED"


def run_final_eod_certification_pipeline_v1(*, truth_root: Path | str = DEFAULT_TRUTH_ROOT, repo_root: Path | str | None = None, day_utc: str, run_id: str | None = None, force_provider_refresh: bool = False, reuse_valid_artifact: bool = True, run_repair: bool = True, stop_after_stage: str = "") -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve() if repo_root else Path(__file__).resolve().parents[2]
    run_id = run_id or f"final-eod-certification:{day_utc}:{uuid.uuid4().hex[:12]}"
    started = utc_now_v1()
    stages: list[dict[str, Any]] = []
    final_status = "SUCCEEDED"
    failure_reason = ""

    def stage(name: str, fn):
        nonlocal final_status, failure_reason
        s = utc_now_v1()
        try:
            result, outputs = fn()
            status = "SUCCEEDED" if bool(result.get("ok", True)) is not False and str(result.get("status") or result.get("result_status") or "").upper() not in {"FAILED", "INVALID", "SOURCE_SETUP_REQUIRED"} else "FAILED"
            reason = "" if status == "SUCCEEDED" else str(result.get("failure_reason") or result.get("message") or result.get("result_status") or "")
        except Exception as exc:  # fail closed and ledger the interruption
            result, outputs = {"ok": False, "status": "FAILED", "failure_reason": str(exc)}, {}
            status, reason = "FAILED", str(exc)
        row = _append_stage(truth_root=root, day_utc=day_utc, run_id=run_id, stage=name, status=status, started_at=s, completed_at=utc_now_v1(), output_paths=outputs, result=result, failure_reason=reason)
        stages.append(row)
        if status != "SUCCEEDED":
            final_status = "FAILED"
            failure_reason = f"{name}: {reason}"
        if stop_after_stage and stop_after_stage == name and final_status == "SUCCEEDED":
            final_status = "INTERRUPTED"
            failure_reason = f"Interrupted after {name} by stop_after_stage."
        return result

    def _eod_source():
        result = certify_tiingo_provider_v1(day_utc=day_utc, truth_root=root, force_provider_refresh=force_provider_refresh, reuse_valid_artifact=reuse_valid_artifact)
        build = result.get("build_result") if isinstance(result.get("build_result"), dict) else {}
        return result, {"final_eod_artifact": build.get("artifact_path", ""), "final_eod_manifest": build.get("current_manifest_json", "")}

    stage("EOD_SOURCE_CERTIFY", _eod_source)
    if final_status == "SUCCEEDED":
        def _market_inputs():
            payload = build_market_data_inputs_v1(truth_root=root, day_utc=day_utc)
            paths = write_market_data_inputs_v1(truth_root=root, day_utc=day_utc, payload=payload)
            emit_market_data_inputs_events_v1(truth_root=root, day_utc=day_utc, payload=payload, artifact_path=Path(paths["market_data_inputs_json"]))
            return {"ok": payload.get("status") in {"READY", "NO_REQUIRED_INPUTS"}, "status": payload.get("status"), "downstream_certification_invariant": payload.get("downstream_certification_invariant")}, paths
        stage("MARKET_DATA_INPUTS", _market_inputs)
    if final_status == "SUCCEEDED":
        def _registry():
            universe = build_runtime_symbol_universe_v1(repo_root=repo)
            payload = build_data_registry_v1(truth_root=root, day_utc=day_utc, symbols=universe["requested_symbols"], universe_metadata=universe)
            return {"ok": True, "status": "BUILT", "missing_items": len(payload.get("missing_items") or [])}, write_data_registry_v1(truth_root=root, day_utc=day_utc, payload=payload)
        stage("DATA_REGISTRY", _registry)
    if final_status == "SUCCEEDED":
        def _sleeve():
            payload = build_sleeve_readiness_v1(truth_root=root, day_utc=day_utc)
            return {"ok": True, "status": "BUILT", "blocked": payload.get("blocked_count")}, write_sleeve_readiness_v1(truth_root=root, day_utc=day_utc, payload=payload)
        stage("SLEEVE_READINESS", _sleeve)
    if final_status == "SUCCEEDED":
        def _domain():
            payload = build_domain_certification_report_v1(truth_root=root, day_utc=day_utc)
            return {"ok": True, "status": "BUILT", "summary": payload.get("summary")}, write_domain_certification_report_v1(truth_root=root, day_utc=day_utc, payload=payload)
        stage("DOMAIN_CERTIFICATION", _domain)
    if final_status == "SUCCEEDED" and run_repair and not stop_after_stage:
        def _repair():
            result = run_domain_repair_orchestration_v1(truth_root=root, repo_root=repo, day_utc=day_utc, domain_id="US_EQUITIES_EOD", execute=True)
            return {"ok": True, "status": "BUILT", "results": result.get("results")}, result.get("lifecycle_paths") if isinstance(result.get("lifecycle_paths"), dict) else {}
        stage("DOMAIN_REPAIR_US_EQUITIES_EOD", _repair)

    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "day_utc": day_utc,
        "started_at_utc": started,
        "completed_at_utc": utc_now_v1(),
        "status": final_status,
        "final_run_outcome": final_run_outcome_v1(status=final_status, stages=stages),
        "failure_reason": failure_reason,
        "stages": stages,
        "ledger_jsonl": str(ledger_jsonl_path_v1(truth_root=root, day_utc=day_utc)),
        **SAFETY,
    }
    paths = _write_latest(truth_root=root, day_utc=day_utc, payload=payload)
    return {**payload, "paths": paths}
