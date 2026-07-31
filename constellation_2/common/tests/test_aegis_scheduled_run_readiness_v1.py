from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.scheduled_run_dependency_manifest_v1 import build_scheduled_run_dependency_manifest_v1
from ops.aegis.scheduled_run_readiness_certificate_v1 import build_scheduled_run_readiness_certificate_v1, write_scheduled_run_readiness_certificate_v1
from ops.aegis.scheduled_run_readiness_self_check_v1 import build_scheduled_run_readiness_self_check_v1
from ops.aegis.scheduled_run_reconciliation_v1 import build_scheduled_run_reconciliation_v1
from ops.aegis.scheduled_run_registry_v1 import RUN_IDS, build_scheduled_run_registry_v1
from ops.aegis.scheduled_run_safe_repair_v1 import build_scheduled_run_safe_repair_v1


DAY = "2026-05-30"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_candidate_ready(root: Path) -> None:
    manifest = build_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=DAY)
    for dep in manifest["dependencies_by_run_id"]["CANDIDATE_GENERATION_0950"]:
        if dep["dependency_key"] == "VIX_INPUT":
            continue
        _write_json(Path(dep["current_artifact_path"]), {"schema_version": "v1", "day_utc": DAY, "generated_at": f"{DAY}T13:45:00Z", "ok": True})


def test_scheduled_run_registry_generation() -> None:
    payload = build_scheduled_run_registry_v1(truth_root=Path("/tmp/aegis-empty"), day_utc=DAY)
    assert [row["scheduled_run_id"] for row in payload["scheduled_runs"]] == RUN_IDS
    assert payload["scheduled_runs_by_id"]["CANDIDATE_GENERATION_0950"]["target_time_local"].startswith(f"{DAY}T09:50:00")


def test_dependency_manifest_generation(tmp_path: Path) -> None:
    payload = build_scheduled_run_dependency_manifest_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["summary"]["dependency_count"] >= 12
    keys = {row["dependency_key"] for row in payload["dependencies"]}
    assert {"MARKET_DATA", "VIX_INPUT", "SLEEVE_INPUT_CONTRACTS", "ALLOWED_SYMBOL_UNIVERSE", "PORTAL_RUNTIME_MODEL"}.issubset(keys)


def test_readiness_certificate_generation_and_blocked_dependency_path(tmp_path: Path) -> None:
    payload = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    cert = payload["certificates_by_run_id"]["CANDIDATE_GENERATION_0950"]
    assert cert["readiness_status"] in {"BLOCKED", "MARKET_NOT_READY"}
    assert cert["dependencies_blocked"] > 0
    assert cert["certificate_hash"]


def test_certified_ready_path(tmp_path: Path) -> None:
    _seed_candidate_ready(tmp_path)
    payload = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    cert = payload["certificates_by_run_id"]["CANDIDATE_GENERATION_0950"]
    assert cert["readiness_status"] == "CERTIFIED_READY"
    assert cert["remaining_blockers"] == []


def test_safe_repair_success_for_operator_action_model(tmp_path: Path) -> None:
    dep_id = "CANDIDATE_GENERATION_0950:OPERATOR_ACTION_MODEL"
    payload = build_scheduled_run_safe_repair_v1(truth_root=tmp_path, day_utc=DAY, dependency_ids=[dep_id], execute=True)
    row = payload["repairs"][0]
    assert row["result"] == "SUCCESS"
    assert row["before_status"] == "MISSING"
    assert row["after_status"] == "READY"


def test_safe_repair_forbidden_action(tmp_path: Path) -> None:
    payload = build_scheduled_run_safe_repair_v1(truth_root=tmp_path, day_utc=DAY, dependency_ids=["MARKET_DATA"], command_override="enable broker execution", execute=True)
    assert payload["repairs"][0]["result"] == "FORBIDDEN"


def test_expired_certificate_self_check_failure_path(tmp_path: Path) -> None:
    _seed_candidate_ready(tmp_path)
    payload = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    payload["certificates"][0]["readiness_status"] = "EXPIRED"
    payload["certificates_by_run_id"][payload["certificates"][0]["scheduled_run_id"]] = payload["certificates"][0]
    write_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    check = build_scheduled_run_readiness_self_check_v1(truth_root=tmp_path, day_utc=DAY)
    assert any(row["failure_code"] == "EXPIRED_CERTIFICATE_USED" for row in check["failures"])


def test_post_run_predictable_failure_detection(tmp_path: Path) -> None:
    payload = build_scheduled_run_reconciliation_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["scheduled_runs_by_id"]["CANDIDATE_GENERATION_0950"]
    assert row["did_valid_certificate_exist"] is False
    assert row["run_result"] == "NOT_EXECUTED"


def test_self_check_failure_path_for_missing_hash(tmp_path: Path) -> None:
    _seed_candidate_ready(tmp_path)
    cert = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    cert["certificates"][0]["source_hashes"] = {}
    cert["certificates_by_run_id"][cert["certificates"][0]["scheduled_run_id"]] = cert["certificates"][0]
    write_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY, payload=cert)
    check = build_scheduled_run_readiness_self_check_v1(truth_root=tmp_path, day_utc=DAY)
    assert any(row["failure_code"] == "CERTIFICATE_MISSING_HASH" for row in check["failures"])


def test_deterministic_rerun_stability(tmp_path: Path) -> None:
    _seed_candidate_ready(tmp_path)
    one = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    two = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    assert one == two


def test_ui_data_shape(tmp_path: Path) -> None:
    _seed_candidate_ready(tmp_path)
    payload = build_scheduled_run_readiness_certificate_v1(truth_root=tmp_path, day_utc=DAY)
    cert = payload["certificates_by_run_id"]["CANDIDATE_GENERATION_0950"]
    assert {"scheduled_run_id", "readiness_status", "readiness_valid_until", "remaining_blockers"}.issubset(cert)
    assert payload["summary"]["next_scheduled_run_id"] == "CANDIDATE_GENERATION_0950"
