#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path("/home/node/constellation_2_runtime")
TRUTH_ROOT = REPO_ROOT / "constellation_2" / "runtime" / "truth"
RUNTIME_ROOT = REPO_ROOT / "constellation_2" / "runtime"
SYSTEM_SNAPSHOT_ROOT = TRUTH_ROOT / "system_snapshot"
OUTPUT_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_runtime_state.v1.json"

FRESHNESS_POLICY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_DIAGNOSTICS_FRESHNESS_POLICY_V1.json"
LIFECYCLE_DEP_GOV_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION_V1.json"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DENY_NAME_TOKENS = (".INVALID_", ".QUARANTINED_")
DENY_DIR_PREFIXES = ("__quarantine", "__quarantined", "__archived")
LIFECYCLE_DEPENDENCY_SPECS = [
    {
        "dep_id": "position_lifecycle_v2",
        "stream_path": "constellation_2/runtime/truth/position_lifecycle_v2",
        "filename": "position_lifecycle_snapshot.v2.json",
        "producer_tool": "ops/tools/run_position_lifecycle_snapshot_v2.py",
        "producer_stage_id_v2": None,
        "legacy_stage_id_v1": "F_POSITION_LIFECYCLE_SNAPSHOT_V2",
    },
    {
        "dep_id": "exit_obligations_v1",
        "stream_path": "constellation_2/runtime/truth/exit_obligations_v1",
        "filename": "exit_obligations.v1.json",
        "producer_tool": "ops/tools/run_exit_obligations_v1.py",
        "producer_stage_id_v2": None,
        "legacy_stage_id_v1": "F_EXIT_OBLIGATIONS_V1",
    },
    {
        "dep_id": "exposure_reconciliation_v2",
        "stream_path": "constellation_2/runtime/truth/exposure_reconciliation_v2",
        "filename": "exposure_reconciliation.v2.json",
        "producer_tool": "ops/tools/run_exposure_reconciliation_v2.py",
        "producer_stage_id_v2": None,
        "legacy_stage_id_v1": "F_EXPOSURE_RECONCILIATION_V2",
    },
]


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def is_authoritative_candidate(path: Path) -> bool:
    n = path.name
    if any(tok in n for tok in DENY_NAME_TOKENS):
        return False
    for part in path.parts:
        if part.startswith(DENY_DIR_PREFIXES):
            return False
    return True


def iter_authoritative_json_files(root: Path):
    if not root.exists():
        return
    for p in root.rglob("*.json"):
        if not p.is_file():
            continue
        if not is_authoritative_candidate(p):
            continue
        yield p


def latest_day_under(root: Path) -> str | None:
    if not root.exists():
        return None
    days: set[str] = set()
    for p in iter_authoritative_json_files(root):
        for part in p.parts:
            if DATE_RE.match(part):
                days.add(part)
    return max(days) if days else None


def latest_json_file_under(root: Path, preferred_names: list[str] | None = None) -> Path | None:
    if not root.exists():
        return None
    files = list(iter_authoritative_json_files(root))
    if not files:
        return None

    def sort_key(path: Path) -> tuple[str, int, str]:
        day = "0000-00-00"
        for part in path.parts:
            if DATE_RE.match(part):
                day = part
        pref_rank = 1
        if preferred_names:
            for name in preferred_names:
                if path.name == name:
                    pref_rank = 0
                    break
        return (day, -pref_rank, str(path))

    files.sort(key=sort_key)
    return files[-1]


def count_json_files(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(1 for _ in iter_authoritative_json_files(root))


def assert_stream_root_is_authoritative(rel_path: str) -> None:
    p = (REPO_ROOT / rel_path).resolve()
    if not str(p).startswith(str(RUNTIME_ROOT.resolve())):
        raise SystemExit(f"FAIL_CLOSED: stream path outside runtime root: {p}")
    for part in p.parts:
        if part.startswith(DENY_DIR_PREFIXES):
            raise SystemExit(f"FAIL_CLOSED: stream path in non-authoritative residue: {p}")


def stream_summary(rel_path: str, preferred_names: list[str] | None = None) -> dict[str, Any]:
    assert_stream_root_is_authoritative(rel_path)
    root = REPO_ROOT / rel_path
    latest_file = latest_json_file_under(root, preferred_names=preferred_names)
    return {
        "path": rel_path,
        "exists": root.exists(),
        "latest_day": latest_day_under(root),
        "json_count": count_json_files(root),
        "latest_json_file": str(latest_file.relative_to(REPO_ROOT)) if latest_file else None,
    }


def load_if_present(rel_path: str, preferred_names: list[str] | None = None) -> dict[str, Any] | None:
    assert_stream_root_is_authoritative(rel_path)
    root = REPO_ROOT / rel_path
    latest_file = latest_json_file_under(root, preferred_names=preferred_names)
    if latest_file is None:
        return None
    return safe_read_json(latest_file)


def load_registry_json(rel_path: str) -> dict[str, Any]:
    path = REPO_ROOT / rel_path
    data = safe_read_json(path)
    if data is None:
        raise SystemExit(f"FAIL_CLOSED: cannot parse required registry {path}")
    return data


def load_md_lines(rel_path: str) -> list[str]:
    path = REPO_ROOT / rel_path
    if not path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing required markdown {path}")
    return path.read_text(encoding="utf-8").splitlines()


def coalesce_status(data: dict[str, Any] | None) -> str:
    if not data:
        return "UNKNOWN"
    for key in ["overall_status", "status", "verdict", "gate_status", "readiness_status"]:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "UNKNOWN"


def normalize_health_status(value: str | None) -> str:
    v = str(value or "").strip().upper()
    if v in {"PASS", "OK", "ACTIVE", "SUCCESS"}:
        return "PASS"
    if v in {"DEGRADED", "PARTIALLY_PROVEN"}:
        return "DEGRADED"
    if v in {"FAIL", "ABORTED", "BLOCKING", "ERROR"}:
        return "FAIL"
    return "UNKNOWN"


def find_declared_engine_ids() -> list[str]:
    lines = load_md_lines("governance/02_REGISTRIES/C2_ENGINE_IDS_V1.md")
    out: list[str] = []
    for line in lines:
        m = re.search(r"`(C2_[A-Z0-9_]+_V1)`", line)
        if m:
            out.append(m.group(1))
    seen = set()
    deduped = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def sleeve_summary() -> list[dict[str, Any]]:
    reg = load_registry_json("governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json")
    sleeves = []
    for sleeve in reg.get("sleeves", []):
        sleeve_id = sleeve.get("sleeve_id", "UNKNOWN")
        mode = sleeve.get("mode", "UNKNOWN")
        partition = sleeve.get("truth_partition")
        partition_path = RUNTIME_ROOT / partition if isinstance(partition, str) else None
        sleeves.append({
            "sleeve_id": sleeve_id,
            "enabled": sleeve.get("enabled", False),
            "mode": mode,
            "ib_account": sleeve.get("ib_account", "UNKNOWN"),
            "symbols": sleeve.get("symbols", []),
            "truth_partition": partition,
            "truth_partition_exists": partition_path.exists() if partition_path else False,
            "status": "ACTIVE" if sleeve.get("enabled", False) else "DISABLED",
        })
    return sleeves


def latest_operating_day(candidates: list[dict[str, Any]]) -> str | None:
    days = [c["latest_day"] for c in candidates if c.get("latest_day")]
    return max(days) if days else None


def lifecycle_stage_presence(truth_root: Path, day: str | None) -> dict[str, Any]:
    if day is None:
        return {}

    checks = {
        "intent_generation": truth_root / "intents_v1" / "snapshots" / day,
        "phaseC_preflight": truth_root / "phaseC_preflight_v1" / day,
        "governed_submission": truth_root / "execution_evidence_v1" / "submissions" / day,
        "broker_events": truth_root / "execution_evidence_v1" / "broker_events" / day,
        "fill_ledger": truth_root / "fill_ledger_v1" / day,
        "positions": truth_root / "positions_v1" / "snapshots" / day,
        "position_lifecycle": truth_root / "position_lifecycle_v2" / day,
        "exit_reconciliation": truth_root / "exit_reconciliation_v1" / day,
        "exposure_reconciliation": truth_root / "exposure_reconciliation_v2" / day,
    }
    return {
        stage: {
            "path": str(path.relative_to(REPO_ROOT)),
            "present": path.exists()
        }
        for stage, path in checks.items()
    }


def detect_quarantine_activity() -> list[str]:
    hits = []
    for p in RUNTIME_ROOT.iterdir():
        if p.name.startswith("__quarantine") or p.name.startswith("__quarantined"):
            hits.append(str(p.relative_to(REPO_ROOT)))
    return sorted(hits)


def _parse_day(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _day_from_generated_utc(v: Any) -> str | None:
    if not isinstance(v, str) or "T" not in v:
        return None
    return v.split("T", 1)[0]


def _eval_day_freshness(*, policy: str, ref_day: str, actual_day: str | None) -> tuple[bool, str | None]:
    if actual_day is None:
        return (False, "MISSING_DAY")
    if policy == "SAME_DAY_REQUIRED":
        return (actual_day == ref_day, None if actual_day == ref_day else f"EXPECT_SAME_DAY:{ref_day}:GOT:{actual_day}")
    if policy == "PREVIOUS_DAY_ACCEPTABLE":
        ref_dt = _parse_day(ref_day)
        min_dt = ref_dt - timedelta(days=1)
        act_dt = _parse_day(actual_day)
        ok = min_dt <= act_dt <= ref_dt
        return (ok, None if ok else f"EXPECT_PREV_OR_SAME:{ref_day}:GOT:{actual_day}")
    if policy == "PER_RUN_REQUIRED":
        return (actual_day == ref_day, None if actual_day == ref_day else f"EXPECT_PER_RUN_DAY:{ref_day}:GOT:{actual_day}")
    return (False, f"UNKNOWN_FRESHNESS_POLICY:{policy}")


def _load_freshness_policy() -> dict[str, Any]:
    obj = safe_read_json(FRESHNESS_POLICY_PATH)
    if obj is None:
        raise SystemExit(f"FAIL_CLOSED: cannot parse freshness policy {FRESHNESS_POLICY_PATH}")
    if str(obj.get("schema_id") or "") != "C2_DIAGNOSTICS_FRESHNESS_POLICY":
        raise SystemExit(f"FAIL_CLOSED: unexpected freshness policy schema_id in {FRESHNESS_POLICY_PATH}")
    if int(obj.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL_CLOSED: unexpected freshness policy schema_version in {FRESHNESS_POLICY_PATH}")
    surfaces = obj.get("surfaces")
    if not isinstance(surfaces, list) or not surfaces:
        raise SystemExit(f"FAIL_CLOSED: freshness policy surfaces missing/empty in {FRESHNESS_POLICY_PATH}")
    return obj


def _load_lifecycle_dependency_governance() -> dict[str, dict[str, Any]]:
    obj = safe_read_json(LIFECYCLE_DEP_GOV_PATH)
    if obj is None:
        raise SystemExit(f"FAIL_CLOSED: cannot parse lifecycle dependency governance {LIFECYCLE_DEP_GOV_PATH}")
    if str(obj.get("schema_id") or "") != "C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION":
        raise SystemExit(f"FAIL_CLOSED: unexpected lifecycle dependency schema_id in {LIFECYCLE_DEP_GOV_PATH}")
    if int(obj.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL_CLOSED: unexpected lifecycle dependency schema_version in {LIFECYCLE_DEP_GOV_PATH}")
    deps = obj.get("dependencies")
    if not isinstance(deps, list) or not deps:
        raise SystemExit(f"FAIL_CLOSED: lifecycle dependency list missing/empty in {LIFECYCLE_DEP_GOV_PATH}")

    out: dict[str, dict[str, Any]] = {}
    allowed = {"REQUIRED_FOR_EXECUTION", "REQUIRED_FOR_READINESS", "OPTIONAL_MONITORING"}
    for d in deps:
        if not isinstance(d, dict):
            continue
        dep_id = str(d.get("dep_id") or "").strip()
        cls = str(d.get("classification") or "").strip().upper()
        if not dep_id:
            continue
        if cls not in allowed:
            raise SystemExit(f"FAIL_CLOSED: invalid lifecycle dependency classification for {dep_id}: {cls}")
        out[dep_id] = d
    if not out:
        raise SystemExit(f"FAIL_CLOSED: lifecycle dependency governance has no valid entries in {LIFECYCLE_DEP_GOV_PATH}")
    return out


def _surface_stream_summary(rel_path: str) -> dict[str, Any]:
    root = REPO_ROOT / rel_path
    latest_file = latest_json_file_under(root)
    return {
        "exists": root.exists(),
        "latest_day": latest_day_under(root),
        "latest_json_file": str(latest_file.relative_to(REPO_ROOT)) if latest_file else None,
    }


def _surface_file_summary(rel_path: str, timestamp_field: str) -> dict[str, Any]:
    p = (REPO_ROOT / rel_path).resolve()
    if not p.exists() or not p.is_file():
        return {
            "exists": False,
            "day": None,
            "path": rel_path,
            "timestamp_field": timestamp_field,
        }
    obj = safe_read_json(p)
    day = _day_from_generated_utc(obj.get(timestamp_field)) if isinstance(obj, dict) else None
    return {
        "exists": True,
        "day": day,
        "path": rel_path,
        "timestamp_field": timestamp_field,
    }


def _latest_sleeve_stage_status_map_for_day(reference_day: str) -> dict[str, Any]:
    reg = load_registry_json("governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json")
    sleeves = reg.get("sleeves") if isinstance(reg.get("sleeves"), list) else []
    for sleeve in sleeves:
        if not isinstance(sleeve, dict):
            continue
        if not bool(sleeve.get("enabled")):
            continue
        if str(sleeve.get("mode") or "").upper() != "PAPER":
            continue
        partition = str(sleeve.get("truth_partition") or "").strip()
        if not partition:
            continue
        root = (RUNTIME_ROOT / partition / "reports" / "orchestrator_run_verdict_v2" / reference_day).resolve()
        if not root.exists() or not root.is_dir():
            continue
        candidates = sorted(root.glob("*/orchestrator_run_verdict.v2.json"), key=lambda p: str(p))
        if not candidates:
            continue
        p = candidates[-1]
        obj = safe_read_json(p)
        if not isinstance(obj, dict):
            continue
        stages = obj.get("stages") if isinstance(obj.get("stages"), list) else []
        stage_map: dict[str, str] = {}
        for s in stages:
            if not isinstance(s, dict):
                continue
            sid = str(s.get("stage_id") or "").strip()
            if not sid:
                continue
            st = str(s.get("status") or "").strip().upper() or "UNKNOWN"
            stage_map[sid] = st
        return {
            "sleeve_id": str(sleeve.get("sleeve_id") or "UNKNOWN"),
            "mode": "PAPER",
            "verdict_path": str(p.relative_to(REPO_ROOT)),
            "stage_status_map": stage_map,
        }
    return {
        "sleeve_id": None,
        "mode": None,
        "verdict_path": None,
        "stage_status_map": {},
    }


def _find_artifact_in_other_scope(dep_id: str, day: str, filename: str, expected_path: Path) -> list[str]:
    out: list[str] = []
    for p in RUNTIME_ROOT.rglob(filename):
        if not p.is_file():
            continue
        if p.resolve() == expected_path.resolve():
            continue
        parts = p.parts
        if dep_id not in parts:
            continue
        if day not in parts:
            continue
        out.append(str(p.relative_to(REPO_ROOT)))
    return sorted(out)


def evaluate_monitoring_freshness(policy: dict[str, Any], reference_day: str | None) -> dict[str, Any]:
    if reference_day is None:
        return {
            "status": "UNKNOWN",
            "reference_day": None,
            "surface_results": [],
            "reason_codes": ["NO_REFERENCE_DAY_FOR_MONITORING_FRESHNESS"],
        }

    surfaces = policy.get("surfaces", [])
    lifecycle_dep_gov = _load_lifecycle_dependency_governance()
    for spec in LIFECYCLE_DEPENDENCY_SPECS:
        dep_id = str(spec["dep_id"])
        if dep_id not in lifecycle_dep_gov:
            raise SystemExit(f"FAIL_CLOSED: lifecycle dependency governance missing dep_id: {dep_id}")
    results: list[dict[str, Any]] = []
    reason_codes: list[str] = []

    for surface in surfaces:
        if not isinstance(surface, dict):
            continue
        sid = str(surface.get("surface_id") or "").strip()
        rel_path = str(surface.get("path") or "").strip()
        freshness = str(surface.get("freshness_policy") or "").strip().upper()
        s_type = str(surface.get("surface_type") or "").strip().upper()
        violation_status = str(surface.get("violation_status") or "DEGRADED").strip().upper()
        status_field = str(surface.get("status_field") or "status").strip()

        if not sid or not rel_path or not freshness:
            continue

        actual_day: str | None = None
        status_value: str | None = None
        exists = False
        latest_json_file: str | None = None
        fail_reasons: list[str] = []
        contract_violations: list[str] = []
        source_reason_codes: list[str] = []
        source_check_failures: list[str] = []
        lifecycle_diagnosis: dict[str, Any] | None = None

        if s_type == "DAY_STREAM":
            summary = _surface_stream_summary(rel_path)
            exists = bool(summary["exists"])
            actual_day = summary["latest_day"]
            latest_json_file = summary["latest_json_file"]
            if latest_json_file:
                obj = safe_read_json(REPO_ROOT / latest_json_file)
                if isinstance(obj, dict):
                    sv = obj.get(status_field)
                    status_value = str(sv).strip() if isinstance(sv, str) else None
                    rc = obj.get("reason_codes")
                    if isinstance(rc, list):
                        source_reason_codes = [str(x) for x in rc if str(x).strip()]
                    checks = obj.get("checks")
                    if isinstance(checks, list):
                        for c in checks:
                            if not isinstance(c, dict):
                                continue
                            cst = str(c.get("status") or "").strip().upper()
                            if cst in {"FAIL", "ERROR", "BLOCKING"}:
                                source_check_failures.append(str(c.get("name") or "UNKNOWN_CHECK"))
                    if sid == "lifecycle_monitor":
                        if str(obj.get("schema_id") or "") != "C2_LIFECYCLE_MONITOR_REPORT":
                            contract_violations.append("UNEXPECTED_SCHEMA_ID")
                        if str(obj.get("schema_version") or "") != "1":
                            contract_violations.append("UNEXPECTED_SCHEMA_VERSION")
            if sid == "lifecycle_monitor":
                vmap = _latest_sleeve_stage_status_map_for_day(reference_day)
                stage_map = vmap.get("stage_status_map") if isinstance(vmap.get("stage_status_map"), dict) else {}
                dep_rows = []
                present_for_ref = 0
                for spec in LIFECYCLE_DEPENDENCY_SPECS:
                    dep_id = str(spec["dep_id"])
                    gov = lifecycle_dep_gov.get(dep_id, {})
                    dep_classification = str(gov.get("classification") or "").strip().upper()
                    dep_path = str(spec["stream_path"])
                    dep_file = str(spec["filename"])
                    producer_tool = str(gov.get("producer_tool") or spec.get("producer_tool") or "")
                    stage_id_v2 = gov.get("producer_stage_id_v2") if "producer_stage_id_v2" in gov else spec.get("producer_stage_id_v2")
                    legacy_stage_id_v1 = str(gov.get("legacy_stage_id_v1") or spec.get("legacy_stage_id_v1") or "")

                    dep_summary = _surface_stream_summary(dep_path)
                    dep_latest_day = dep_summary.get("latest_day")
                    dep_latest_file = dep_summary.get("latest_json_file")
                    expected_abs = (REPO_ROOT / dep_path / reference_day / dep_file).resolve()
                    dep_present_ref = expected_abs.exists() and expected_abs.is_file()
                    if dep_present_ref:
                        present_for_ref += 1
                    alt_paths = _find_artifact_in_other_scope(dep_id, reference_day, dep_file, expected_abs)

                    producer_execution_state = "NOT_SCHEDULED"
                    stage_status = None
                    if isinstance(stage_id_v2, str) and stage_id_v2:
                        stage_status = str(stage_map.get(stage_id_v2) or "").strip().upper() or None
                        if stage_status in {"OK", "PASS"}:
                            producer_execution_state = "EXECUTED_OK"
                        elif stage_status in {"SKIP"}:
                            producer_execution_state = "SCHEDULED_BUT_NOT_EXECUTED"
                        elif stage_status in {"FAIL", "BLOCKED", "ABORTED", "ERROR"}:
                            producer_execution_state = "EXECUTED_BUT_FAILED"
                        else:
                            producer_execution_state = "SCHEDULED_STATUS_UNKNOWN"

                    artifact_state = "MISSING_FOR_REFERENCE_DAY"
                    if dep_present_ref:
                        artifact_state = "PRESENT_FOR_REFERENCE_DAY"
                    elif alt_paths:
                        artifact_state = "PRESENT_WRONG_LOCATION_OR_SCOPE"
                    elif isinstance(dep_latest_day, str) and dep_latest_day < reference_day:
                        artifact_state = "FRESHNESS_LAPSE"

                    chain_state = "UNKNOWN"
                    if producer_execution_state == "NOT_SCHEDULED":
                        chain_state = "NOT_SCHEDULED"
                    elif producer_execution_state == "SCHEDULED_BUT_NOT_EXECUTED":
                        chain_state = "SCHEDULED_BUT_NOT_EXECUTED"
                    elif producer_execution_state == "EXECUTED_BUT_FAILED":
                        chain_state = "EXECUTED_BUT_FAILED"
                    elif producer_execution_state == "EXECUTED_OK" and artifact_state == "PRESENT_WRONG_LOCATION_OR_SCOPE":
                        chain_state = "EXECUTED_BUT_WROTE_WRONG_LOCATION_OR_SCOPE"
                    elif producer_execution_state == "EXECUTED_OK" and artifact_state == "MISSING_FOR_REFERENCE_DAY":
                        chain_state = "EXECUTED_BUT_OUTPUT_MISSING"
                    elif artifact_state == "FRESHNESS_LAPSE":
                        chain_state = "FRESHNESS_LAPSE"
                    elif artifact_state == "PRESENT_FOR_REFERENCE_DAY":
                        chain_state = "CLEAR"

                    dep_rows.append({
                        "dep_id": dep_id,
                        "classification": dep_classification,
                        "path": dep_path,
                        "expected_artifact_path": str((REPO_ROOT / dep_path / reference_day / dep_file).relative_to(REPO_ROOT)),
                        "latest_day": dep_latest_day,
                        "latest_json_file": dep_latest_file,
                        "present_for_reference_day": dep_present_ref,
                        "producer_tool": producer_tool,
                        "producer_stage_id_v2": stage_id_v2,
                        "legacy_stage_id_v1": legacy_stage_id_v1,
                        "producer_execution_state": producer_execution_state,
                        "producer_stage_status": stage_status,
                        "artifact_state": artifact_state,
                        "other_scope_matches": alt_paths[:5],
                        "chain_state": chain_state,
                    })
                lifecycle_diagnosis = {
                    "dependency_count": len(dep_rows),
                    "dependencies_present_for_reference_day": present_for_ref,
                    "orchestrator_v2_verdict_path": vmap.get("verdict_path"),
                    "governance_registry_path": str(LIFECYCLE_DEP_GOV_PATH.relative_to(REPO_ROOT)),
                    "dependencies": dep_rows,
                }
        elif s_type == "FILE_GENERATED_UTC":
            timestamp_field = str(surface.get("timestamp_field") or "generated_utc").strip()
            summary = _surface_file_summary(rel_path, timestamp_field)
            exists = bool(summary["exists"])
            actual_day = summary["day"]
            obj = safe_read_json(REPO_ROOT / rel_path) if exists else None
            if isinstance(obj, dict):
                sv = obj.get(status_field)
                status_value = str(sv).strip() if isinstance(sv, str) else None
            latest_json_file = rel_path
        else:
            reason_codes.append(f"UNKNOWN_SURFACE_TYPE:{sid}:{s_type}")
            continue

        freshness_ok, freshness_reason = _eval_day_freshness(policy=freshness, ref_day=reference_day, actual_day=actual_day)

        status = "PASS"
        if not exists:
            status = "FAIL"
            reason_codes.append(f"MISSING_SURFACE:{sid}")
            fail_reasons.append("ARTIFACT_MISSING")
        elif status_value is not None and normalize_health_status(status_value) == "FAIL":
            status = "FAIL"
            reason_codes.append(f"SURFACE_FAIL:{sid}:{status_value}")
            fail_reasons.append("SURFACE_FAIL")
        elif not freshness_ok:
            status = "FAIL" if violation_status == "FAIL" else "DEGRADED"
            reason_codes.append(f"FRESHNESS_VIOLATION:{sid}:{freshness_reason}")
            fail_reasons.append("FRESHNESS_STALE")
        if contract_violations:
            status = "FAIL"
            fail_reasons.append("CONTRACT_VIOLATION")
            reason_codes.append(f"CONTRACT_VIOLATION:{sid}:{','.join(contract_violations)}")

        if not fail_reasons:
            fail_reasons = ["NONE"]
        else:
            dedup = []
            seen_fr = set()
            for fr in fail_reasons:
                if fr not in seen_fr:
                    seen_fr.add(fr)
                    dedup.append(fr)
            fail_reasons = dedup

        lifecycle_cause_class = None
        lifecycle_governance_summary = None
        if sid == "lifecycle_monitor":
            dep_chain_states = []
            dep_rows = []
            if isinstance(lifecycle_diagnosis, dict):
                deps_any = lifecycle_diagnosis.get("dependencies")
                if isinstance(deps_any, list):
                    dep_rows = [d for d in deps_any if isinstance(d, dict)]
                    dep_chain_states = [str(d.get("chain_state") or "") for d in dep_rows]
            cls_counts = {"REQUIRED_FOR_EXECUTION": 0, "REQUIRED_FOR_READINESS": 0, "OPTIONAL_MONITORING": 0}
            blocked_counts = {"REQUIRED_FOR_EXECUTION": 0, "REQUIRED_FOR_READINESS": 0, "OPTIONAL_MONITORING": 0}
            for d in dep_rows:
                cls = str(d.get("classification") or "").strip().upper()
                if cls in cls_counts:
                    cls_counts[cls] += 1
                    if str(d.get("chain_state") or "").strip().upper() != "CLEAR":
                        blocked_counts[cls] += 1
            lifecycle_governance_summary = {
                "counts_by_classification": cls_counts,
                "blocked_by_classification": blocked_counts,
            }
            if "CONTRACT_VIOLATION" in fail_reasons:
                lifecycle_cause_class = "CONTRACT_SCHEMA_VIOLATION"
            elif "FRESHNESS_STALE" in fail_reasons:
                lifecycle_cause_class = "FRESHNESS_LAPSE"
            elif any(s == "NOT_SCHEDULED" for s in dep_chain_states):
                lifecycle_cause_class = "NOT_SCHEDULED"
            elif any(s == "SCHEDULED_BUT_NOT_EXECUTED" for s in dep_chain_states):
                lifecycle_cause_class = "SCHEDULED_BUT_NOT_EXECUTED"
            elif any(s == "EXECUTED_BUT_FAILED" for s in dep_chain_states):
                lifecycle_cause_class = "EXECUTED_BUT_FAILED"
            elif any(s == "EXECUTED_BUT_WROTE_WRONG_LOCATION_OR_SCOPE" for s in dep_chain_states):
                lifecycle_cause_class = "EXECUTED_BUT_WROTE_WRONG_LOCATION_OR_SCOPE"
            elif "SURFACE_FAIL" in fail_reasons:
                lifecycle_cause_class = "ACTUAL_SURFACE_FAIL_FROM_PRODUCED_ARTIFACT"
            elif "ARTIFACT_MISSING" in fail_reasons:
                dep_present = 0
                if isinstance(lifecycle_diagnosis, dict):
                    dep_present = int(lifecycle_diagnosis.get("dependencies_present_for_reference_day") or 0)
                if dep_present == 0:
                    lifecycle_cause_class = "EXPECTED_NOT_YET_PRODUCED_SURFACE"
                elif dep_present < 3:
                    lifecycle_cause_class = "MISSING_UPSTREAM_PRODUCER"
                else:
                    lifecycle_cause_class = "MISSING_LIFECYCLE_MONITOR_PRODUCER"
            else:
                lifecycle_cause_class = "NONE"

        results.append({
            "surface_id": sid,
            "path": rel_path,
            "surface_type": s_type,
            "freshness_policy": freshness,
            "violation_status": violation_status,
            "status": status,
            "exists": exists,
            "expected_day": reference_day,
            "actual_day": actual_day,
            "freshness_ok": freshness_ok,
            "freshness_reason": freshness_reason,
            "status_value": status_value,
            "evidence": latest_json_file,
            "fail_reasons": fail_reasons,
            "contract_violations": contract_violations,
            "source_reason_codes": source_reason_codes,
            "source_check_failures": source_check_failures,
            "lifecycle_dependency_diagnosis": lifecycle_diagnosis if sid == "lifecycle_monitor" else None,
            "lifecycle_cause_class": lifecycle_cause_class if sid == "lifecycle_monitor" else None,
            "lifecycle_governance_summary": lifecycle_governance_summary if sid == "lifecycle_monitor" else None,
        })

    aggregate = "PASS"
    if any(r["status"] == "FAIL" for r in results):
        aggregate = "FAIL"
    elif any(r["status"] == "DEGRADED" for r in results):
        aggregate = "DEGRADED"
    elif not results:
        aggregate = "UNKNOWN"

    seen = set()
    deduped_reasons = []
    for rc in reason_codes:
        if rc not in seen:
            seen.add(rc)
            deduped_reasons.append(rc)

    return {
        "status": aggregate,
        "reference_day": reference_day,
        "surface_results": results,
        "reason_codes": deduped_reasons,
    }


def resolve_sleeve_execution_health(sleeves: list[dict[str, Any]]) -> dict[str, Any]:
    details: list[dict[str, Any]] = []

    for sleeve in sleeves:
        if not sleeve.get("enabled"):
            continue
        mode = str(sleeve.get("mode") or "").upper()
        if mode != "PAPER":
            continue
        partition_rel = sleeve.get("truth_partition")
        if not isinstance(partition_rel, str) or not partition_rel.strip():
            details.append({
                "sleeve_id": sleeve.get("sleeve_id", "UNKNOWN"),
                "status": "FAIL",
                "reason": "MISSING_TRUTH_PARTITION",
            })
            continue

        verdict_root = (RUNTIME_ROOT / partition_rel / "reports" / "orchestrator_run_verdict_v2").resolve()
        latest_verdict = latest_json_file_under(verdict_root, preferred_names=["orchestrator_run_verdict.v2.json"])
        if latest_verdict is None:
            details.append({
                "sleeve_id": sleeve.get("sleeve_id", "UNKNOWN"),
                "status": "FAIL",
                "reason": "MISSING_ORCHESTRATOR_VERDICT",
                "evidence_root": str(verdict_root.relative_to(REPO_ROOT)),
            })
            continue

        verdict_obj = safe_read_json(latest_verdict)
        verdict_status = normalize_health_status(str((verdict_obj or {}).get("status") or "UNKNOWN"))
        latest_day = None
        for part in latest_verdict.parts:
            if DATE_RE.match(part):
                latest_day = part

        details.append({
            "sleeve_id": sleeve.get("sleeve_id", "UNKNOWN"),
            "mode": mode,
            "status": verdict_status,
            "verdict_status_raw": str((verdict_obj or {}).get("status") or "UNKNOWN"),
            "latest_day": latest_day,
            "latest_verdict_path": str(latest_verdict.relative_to(REPO_ROOT)),
            "attempt_id": (verdict_obj or {}).get("attempt_id"),
            "produced_utc": (verdict_obj or {}).get("produced_utc"),
        })

    if not details:
        return {
            "status": "UNKNOWN",
            "reason_codes": ["NO_ENABLED_PAPER_SLEEVES"],
            "sleeves": [],
            "latest_execution_day": None,
        }

    latest_execution_day = max([d["latest_day"] for d in details if d.get("latest_day")], default=None)

    if any(d["status"] == "FAIL" for d in details):
        status = "FAIL"
        reason_codes = ["SLEEVE_EXECUTION_FAIL_OR_MISSING"]
    elif any(d["status"] == "DEGRADED" for d in details):
        status = "DEGRADED"
        reason_codes = ["SLEEVE_EXECUTION_DEGRADED"]
    elif all(d["status"] == "PASS" for d in details):
        status = "PASS"
        reason_codes = []
    else:
        status = "UNKNOWN"
        reason_codes = ["SLEEVE_EXECUTION_UNKNOWN"]

    return {
        "status": status,
        "reason_codes": reason_codes,
        "sleeves": details,
        "latest_execution_day": latest_execution_day,
    }


def derive_overall_status(execution_status: str, monitoring_status: str) -> tuple[str, list[str]]:
    ex = normalize_health_status(execution_status)
    mon = normalize_health_status(monitoring_status)

    if ex == "FAIL":
        return ("FAIL", ["EXECUTION_AUTHORITY_FAIL"]) 
    if ex == "PASS" and mon == "PASS":
        return ("PASS", [])
    if ex == "PASS" and mon in {"DEGRADED", "FAIL"}:
        return ("PARTIALLY_PROVEN", ["MONITORING_DEGRADED_WITH_EXECUTION_PASS"])
    if ex == "DEGRADED" and mon == "PASS":
        return ("DEGRADED", ["EXECUTION_DEGRADED"])
    if ex == "DEGRADED" and mon in {"DEGRADED", "FAIL"}:
        return ("PARTIALLY_PROVEN", ["EXECUTION_AND_MONITORING_DEGRADED"])
    return ("UNKNOWN", ["INSUFFICIENT_SCOPE_HEALTH_EVIDENCE"])


def overall_health_from_scope(overall_status: str) -> str:
    s = str(overall_status or "").upper()
    if s == "PASS":
        return "OK"
    if s in {"PARTIALLY_PROVEN", "DEGRADED"}:
        return "DEGRADED"
    if s in {"FAIL", "UNKNOWN"}:
        return "BLOCKING"
    return "BLOCKING"


def diagnostics(
    stream_map: dict[str, dict[str, Any]],
    lifecycle_presence: dict[str, Any],
    scope_health: dict[str, Any],
    monitoring_freshness: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    required_streams = [
        "constellation_2/runtime/truth/intents_v1/snapshots",
        "constellation_2/runtime/truth/phaseC_preflight_v1",
        "constellation_2/runtime/truth/execution_evidence_v1/submissions",
        "constellation_2/runtime/truth/execution_evidence_v1/broker_events",
        "constellation_2/runtime/truth/fill_ledger_v1",
        "constellation_2/runtime/truth/positions_v1/snapshots",
        "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
        "constellation_2/runtime/truth/monitoring_v1/paper_readiness",
    ]
    for key in required_streams:
        summary = stream_map[key]
        if not summary["exists"]:
            out.append({
                "code": "MISSING_REQUIRED_STREAM",
                "severity": "BLOCKING",
                "status": "FAIL",
                "summary": f"Required canonical stream missing: {key}",
                "evidence": [key],
            })

    execution = scope_health.get("sleeve_execution_health", {})
    monitoring = scope_health.get("system_monitoring_health", {})
    overall = scope_health.get("overall", {})

    if execution.get("status") == "FAIL":
        out.append({
            "code": "SLEEVE_EXECUTION_AUTHORITY_FAIL",
            "severity": "BLOCKING",
            "status": "FAIL",
            "summary": "Sleeve execution authority is FAIL in PAPER scope.",
            "evidence": [d.get("latest_verdict_path") for d in execution.get("sleeves", []) if d.get("latest_verdict_path")],
        })

    if execution.get("status") == "PASS" and monitoring.get("status") in {"DEGRADED", "FAIL"}:
        out.append({
            "code": "EXECUTION_PASS_MONITORING_DEGRADED",
            "severity": "ERROR",
            "status": "DEGRADED",
            "summary": "Execution authority is PASS but monitoring freshness/health is degraded.",
            "evidence": [
                r.get("evidence")
                for r in monitoring_freshness.get("surface_results", [])
                if r.get("status") in {"DEGRADED", "FAIL"} and r.get("evidence")
            ],
        })

    if overall.get("status") == "PARTIALLY_PROVEN":
        out.append({
            "code": "OVERALL_PARTIALLY_PROVEN",
            "severity": "WARN",
            "status": "DEGRADED",
            "summary": "Overall status is PARTIALLY_PROVEN due to cross-scope health split.",
            "evidence": overall.get("reason_codes", []),
        })

    if monitoring_freshness.get("status") in {"DEGRADED", "FAIL"}:
        out.append({
            "code": "MONITORING_FRESHNESS_POLICY_VIOLATION",
            "severity": "ERROR" if monitoring_freshness.get("status") == "FAIL" else "WARN",
            "status": "DEGRADED",
            "summary": "Monitoring freshness policy violations detected.",
            "evidence": monitoring_freshness.get("reason_codes", []),
        })

    if lifecycle_presence:
        missing = [k for k, v in lifecycle_presence.items() if not v["present"]]
        if missing:
            out.append({
                "code": "GLOBAL_LIFECYCLE_GAP_ON_GLOBAL_LATEST_DAY",
                "severity": "WARN",
                "status": "DEGRADED",
                "summary": "Global truth has lifecycle gaps on its own latest day; execution authority remains sleeve-scoped.",
                "evidence": [lifecycle_presence[m]["path"] for m in missing],
                "missing_stages": missing,
            })

    non_auth_exec_v2 = (TRUTH_ROOT / "execution_evidence_v2" / "broker_events")
    auth_exec_v1 = (TRUTH_ROOT / "execution_evidence_v1" / "broker_events")
    if non_auth_exec_v2.exists() and auth_exec_v1.exists():
        out.append({
            "code": "NON_AUTHORITATIVE_EXECUTION_SURFACE_PRESENT",
            "severity": "WARN",
            "status": "DEGRADED",
            "summary": "Non-authoritative execution_evidence_v2 broker_events surface is present alongside authoritative execution_evidence_v1 broker_events.",
            "evidence": [
                "constellation_2/runtime/truth/execution_evidence_v1/broker_events",
                "constellation_2/runtime/truth/execution_evidence_v2/broker_events",
                "governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json",
            ],
        })

    quarantine_hits = detect_quarantine_activity()
    if quarantine_hits:
        out.append({
            "code": "QUARANTINE_ACTIVITY_PRESENT",
            "severity": "WARN",
            "status": "DEGRADED",
            "summary": "Quarantine roots are present under runtime. Review if recent incidents are unresolved.",
            "evidence": quarantine_hits[:20],
        })

    if not (REPO_ROOT / "governance" / "00_INDEX.md").exists() or not (REPO_ROOT / "governance" / "00_MANIFEST.yaml").exists():
        out.append({
            "code": "GOVERNANCE_ROOT_INCOMPLETE",
            "severity": "BLOCKING",
            "status": "FAIL",
            "summary": "Governance index or manifest is missing.",
            "evidence": [
                "governance/00_INDEX.md",
                "governance/00_MANIFEST.yaml",
            ],
        })

    return out


def overall_health(diags: list[dict[str, Any]]) -> str:
    if any(d["severity"] == "BLOCKING" for d in diags):
        return "BLOCKING"
    if any(d["severity"] in {"ERROR", "WARN"} for d in diags):
        return "DEGRADED"
    return "OK"


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build constellation runtime state snapshot from canonical runtime truth surfaces."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _ = _parse_args(argv)
    required_roots = [
        REPO_ROOT,
        TRUTH_ROOT,
        REPO_ROOT / "governance" / "00_INDEX.md",
        REPO_ROOT / "governance" / "00_MANIFEST.yaml",
        REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
        REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_TRUTH_AUTHORITY_REGISTRY_V1.json",
        REPO_ROOT / "governance" / "02_REGISTRIES" / "TRUTH_SURFACE_AUTHORITY_V1.json",
        REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_SPINE_AUTHORITY_V1.json",
        REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_ENGINE_IDS_V1.md",
        FRESHNESS_POLICY_PATH,
    ]
    for path in required_roots:
        if not path.exists():
            raise SystemExit(f"FAIL_CLOSED: required path missing: {path}")

    ensure_dir(SYSTEM_SNAPSHOT_ROOT)

    stream_paths = [
        "constellation_2/runtime/truth/intents_v1/snapshots",
        "constellation_2/runtime/truth/intents_v1/day_rollup",
        "constellation_2/runtime/truth/phaseC_preflight_v1",
        "constellation_2/runtime/truth/execution_evidence_v1/submissions",
        "constellation_2/runtime/truth/execution_evidence_v1/submission_index",
        "constellation_2/runtime/truth/execution_evidence_v1/broker_events",
        "constellation_2/runtime/truth/fill_ledger_v1",
        "constellation_2/runtime/truth/positions_v1/snapshots",
        "constellation_2/runtime/truth/position_lifecycle_v2",
        "constellation_2/runtime/truth/monitoring_v1/lifecycle_monitor",
        "constellation_2/runtime/truth/monitoring_v1/paper_readiness",
        "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
        "constellation_2/runtime/truth/reports/gate_stack_verdict_v1",
        "constellation_2/runtime/truth/reports/reconciliation_report_v3",
        "constellation_2/runtime/truth/reports/broker_reconciliation_v3",
        "constellation_2/runtime/truth/reports/execution_reconciliation_v1",
        "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1",
        "constellation_2/runtime/truth/reports/capital_risk_envelope_v2",
        "constellation_2/runtime/truth/ib_api_handshake",
        "constellation_2/runtime/truth_sleeves/PRIMARY/PAPER",
    ]

    if "constellation_2/runtime/truth/execution_evidence_v2/broker_events" in stream_paths:
        raise SystemExit("FAIL_CLOSED: non-authoritative execution_evidence_v2 stream configured")

    stream_map = {path: stream_summary(path) for path in stream_paths}

    global_activity_day = latest_operating_day([
        stream_map["constellation_2/runtime/truth/intents_v1/snapshots"],
        stream_map["constellation_2/runtime/truth/phaseC_preflight_v1"],
        stream_map["constellation_2/runtime/truth/execution_evidence_v1/submissions"],
        stream_map["constellation_2/runtime/truth/execution_evidence_v1/broker_events"],
        stream_map["constellation_2/runtime/truth/fill_ledger_v1"],
        stream_map["constellation_2/runtime/truth/positions_v1/snapshots"],
    ])

    lifecycle_presence_global = lifecycle_stage_presence(TRUTH_ROOT, global_activity_day)

    paper_readiness = load_if_present(
        "constellation_2/runtime/truth/monitoring_v1/paper_readiness",
        preferred_names=["paper_readiness_report.v1.json"],
    )
    lifecycle_monitor = load_if_present(
        "constellation_2/runtime/truth/monitoring_v1/lifecycle_monitor",
        preferred_names=["lifecycle_monitor_report.v1.json"],
    )
    operator_daily_gate = load_if_present(
        "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
        preferred_names=["operator_daily_gate.v3.json"],
    )
    gate_stack_verdict = load_if_present(
        "constellation_2/runtime/truth/reports/gate_stack_verdict_v1",
        preferred_names=["gate_stack_verdict.v1.json"],
    )
    capital_authority = load_if_present(
        "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1",
        preferred_names=["capital_authority_allocation.v1.json"],
    )
    capital_risk = load_if_present(
        "constellation_2/runtime/truth/reports/capital_risk_envelope_v2",
        preferred_names=["capital_risk_envelope.v2.json"],
    )
    ib_handshake = load_if_present(
        "constellation_2/runtime/truth/ib_api_handshake",
        preferred_names=["ib_api_handshake.v1.json"],
    )

    sleeves = sleeve_summary()
    execution_health = resolve_sleeve_execution_health(sleeves)

    freshness_policy = _load_freshness_policy()
    monitoring_freshness = evaluate_monitoring_freshness(freshness_policy, execution_health.get("latest_execution_day"))

    monitoring_status_from_artifacts = normalize_health_status(coalesce_status(paper_readiness))
    monitoring_status = monitoring_freshness.get("status", "UNKNOWN")
    if monitoring_status == "PASS" and monitoring_status_from_artifacts == "FAIL":
        monitoring_status = "FAIL"

    overall_status, overall_reason_codes = derive_overall_status(
        execution_health.get("status", "UNKNOWN"),
        monitoring_status,
    )

    scope_health = {
        "authority_model": {
            "paper_mode_execution_authority": "sleeve_truth",
            "paper_mode_monitoring_authority": "global_truth",
        },
        "sleeve_execution_health": execution_health,
        "system_monitoring_health": {
            "status": monitoring_status,
            "status_from_paper_readiness": coalesce_status(paper_readiness),
            "freshness": monitoring_freshness,
        },
        "overall": {
            "status": overall_status,
            "reason_codes": overall_reason_codes,
        },
    }

    latest_operating_day_resolved = execution_health.get("latest_execution_day") or global_activity_day

    bug_list = diagnostics(
        stream_map=stream_map,
        lifecycle_presence=lifecycle_presence_global,
        scope_health=scope_health,
        monitoring_freshness=monitoring_freshness,
    )

    data = {
        "artifact_id": "constellation_runtime_state",
        "schema_version": "1.1",
        "generated_utc": now_utc(),
        "system_identity": {
            "name": "Constellation 2.0",
            "repo_root": str(REPO_ROOT),
            "canonical_truth_root": str(TRUTH_ROOT),
        },
        "system_status": {
            "mode": "PAPER",
            "overall_health": overall_health_from_scope(overall_status),
            "overall_status": overall_status,
            "paper_trading_ready_status": coalesce_status(paper_readiness),
            "operator_daily_gate_status": coalesce_status(operator_daily_gate),
            "gate_stack_verdict_status": coalesce_status(gate_stack_verdict),
        },
        "scope_health": scope_health,
        "latest_operating_day": latest_operating_day_resolved,
        "latest_execution_day": execution_health.get("latest_execution_day"),
        "latest_global_activity_day": global_activity_day,
        "lifecycle_state": {
            "latest_day": global_activity_day,
            "stage_presence": lifecycle_presence_global,
            "lifecycle_monitor_status": coalesce_status(lifecycle_monitor),
        },
        "sleeves": sleeves,
        "engines": [
            {
                "engine_id": engine_id,
                "status": "GOVERNED"
            }
            for engine_id in find_declared_engine_ids()
        ],
        "capital_state": {
            "capital_authority_status": coalesce_status(capital_authority),
            "capital_risk_envelope_status": coalesce_status(capital_risk),
            "latest_capital_authority_day": stream_map["constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1"]["latest_day"],
        },
        "execution_state": {
            "broker_connection_status": coalesce_status(ib_handshake),
            "latest_submission_day": stream_map["constellation_2/runtime/truth/execution_evidence_v1/submissions"]["latest_day"],
            "latest_broker_event_day": stream_map["constellation_2/runtime/truth/execution_evidence_v1/broker_events"]["latest_day"],
            "latest_fill_ledger_day": stream_map["constellation_2/runtime/truth/fill_ledger_v1"]["latest_day"],
        },
        "stream_summaries": stream_map,
        "self_diagnostics": {
            "status": overall_health(bug_list),
            "diagnostic_count": len(bug_list),
            "diagnostics": bug_list,
        },
        "unknowns": [
            "engine_to_sleeve_active_mapping_not_proven_in_this_snapshot",
            "capital_amounts_not_extracted_without proven stable field contracts",
            "current_open_position_counts_not normalized here because authoritative field-level position schema mapping was not proven in this script",
        ],
    }

    write_json(OUTPUT_PATH, data)
    print(f"WROTE: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
