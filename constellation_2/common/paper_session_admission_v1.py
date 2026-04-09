from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time as wall_time
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_CONTRACT_PATHS_V1,
    PAPER_SESSION_REPORT_FAMILIES_V1,
    REPO_ROOT,
    canonical_paper_session_report_path,
)
from constellation_2.common.paper_session_admission_certificate_v1 import (
    PaperSessionAdmissionCertificateV1,
    build_paper_session_admission_certificate_v1,
)
from constellation_2.common.paper_session_blocker_ledger_v1 import (
    PaperSessionBlockerLedgerV1,
    build_paper_session_blocker_ledger_v1,
)
from constellation_2.common.paper_session_closure_v1 import (
    PaperSessionClosureV1,
    build_paper_session_closure_v1,
)
from constellation_2.common.paper_session_definition_v1 import (
    PaperSessionDefinitionV1,
    build_paper_session_definition_v1,
)
from constellation_2.common.paper_session_dependency_graph_v1 import (
    PaperSessionDependencyGraphV1,
    build_paper_session_dependency_graph_v1,
)
from constellation_2.common.paper_session_divergence_v1 import (
    PaperSessionDivergenceV1,
    build_paper_session_divergence_v1,
)
from constellation_2.common.paper_session_envelope_v1 import (
    PaperSessionEnvelopeV1,
    build_paper_session_envelope_v1,
)
from constellation_2.common.paper_session_producer_attempts_v1 import (
    PaperSessionProducerAttemptsV1,
    build_paper_session_producer_attempts_v1,
)
from constellation_2.common.paper_open_readiness_v1 import PaperOpenReadinessV1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_baseline_ready_path,
    resolve_operator_statement_path as resolve_aligned_operator_statement_path,
    resolve_paper_open_readiness_path,
    resolve_session_readiness_refresh_path,
    resolve_subsystem_readiness_report_path,
)
from constellation_2.common.truth_root_v1 import resolve_runtime_root
from constellation_2.common.subsystem_readiness_report_v1 import SubsystemReadinessReportV1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
DEFAULT_OPERATOR_ENTRYPOINT_V1 = "ops/tools/run_paper_session_admission_v1.py"
DEFAULT_EXECUTION_ENTRYPOINT_V1 = "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py"
SLEEVE_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"
ENGINE_REGISTRY_RELPATH = "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
GATE_HIERARCHY_RELPATH = "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json"
EXECUTION_DEPENDENCY_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_EXECUTION_DEPENDENCY_REGISTRY_V1.json"
ADMISSION_PRODUCER_MAP_RELPATH = "governance/02_REGISTRIES/PAPER_SESSION_ADMISSION_PRODUCER_MAP_V1.json"
PAPER_OPEN_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_open_readiness.v1.schema.json"
SUBSYSTEM_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/subsystem_readiness_report.v1.schema.json"
SESSION_REFRESH_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/session_readiness_refresh.v1.schema.json"


def _runtime_root_from_truth_root(truth_root: Path) -> Path:
    runtime_root = truth_root.resolve().parent
    if runtime_root == resolve_runtime_root():
        return runtime_root
    return runtime_root


@dataclass(frozen=True, slots=True)
class ClosureEvaluationV1:
    definition: PaperSessionDefinitionV1
    graph: PaperSessionDependencyGraphV1
    blocker_ledger: PaperSessionBlockerLedgerV1
    closure: PaperSessionClosureV1
    producer_attempts: PaperSessionProducerAttemptsV1


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _session_valid_until(day_utc: str) -> str:
    session_day = date.fromisoformat(day_utc)
    ny_close = datetime.combine(
        session_day,
        time(23, 59, 59),
        tzinfo=ZoneInfo("America/New_York"),
    )
    return ny_close.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _producer_attempts_relpath(day_utc: str) -> str:
    return f"reports/paper_session_producer_attempts_v1/{day_utc}/paper_session_producer_attempts.v1.json"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _json_hash(obj: Any) -> str:
    return _sha256_bytes((json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))


def _write_json(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    obj = _read_json(path)
    if not isinstance(obj, dict):
        raise ValueError(f"REGISTRY_NOT_OBJECT:{path}")
    return obj


def _trim_output(text: str, *, limit: int = 4000) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[-limit:]


def _load_admission_producer_map(repo_root: Path) -> dict[str, Any]:
    return _load_registry((repo_root / ADMISSION_PRODUCER_MAP_RELPATH).resolve())


def _producer_specs_by_dependency(repo_root: Path) -> dict[str, dict[str, Any]]:
    payload = _load_admission_producer_map(repo_root)
    producers = payload.get("producers")
    if not isinstance(producers, list):
        raise ValueError("ADMISSION_PRODUCER_MAP_INVALID")
    mapping: dict[str, dict[str, Any]] = {}
    for producer in producers:
        if not isinstance(producer, dict):
            continue
        for dependency_key in producer.get("dependency_keys") or []:
            key = str(dependency_key).strip()
            if key:
                mapping[key] = producer
    return mapping


def _producer_spec_for_dependency(*, producer_specs: dict[str, dict[str, Any]], dependency_key: str) -> dict[str, Any] | None:
    spec = producer_specs.get(dependency_key)
    if spec is not None:
        return spec
    if dependency_key.startswith("operator_input:operator_statement:"):
        return producer_specs.get("operator_input:operator_statement")
    return None


def _subprocess_result_dict(cmd: list[str], *, cwd: Path) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    return {
        "returncode": int(proc.returncode),
        "stdout": _trim_output(proc.stdout),
        "stderr": _trim_output(proc.stderr),
    }


def _build_operator_statement_producer_cmd(*, repo_root: Path, day_utc: str) -> list[str]:
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(repo_root)
    return [
        sys.executable,
        str((repo_root / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--truth_root",
        str((repo_root / "constellation_2").resolve()),
        "--ib_account",
        paper_account,
        "--mode",
        "SEED_100K",
        "--allow_create",
        "YES",
    ]


def _build_producer_command(*, repo_root: Path, producer_id: str, day_utc: str) -> list[str]:
    if producer_id == "testing_evidence_plane_refresh":
        return [
            sys.executable,
            str((repo_root / "ops/tools/run_testing_evidence_plane_v1.py").resolve()),
            "--day_utc",
            day_utc,
        ]
    if producer_id == "session_readiness_refresh":
        return [
            sys.executable,
            str((repo_root / "ops/tools/run_session_readiness_refresh_v1.py").resolve()),
            "--day_utc",
            day_utc,
        ]
    if producer_id == "baseline_readiness":
        return [
            sys.executable,
            str((repo_root / "ops/tools/run_baseline_readiness_admission_v1.py").resolve()),
            "--day_utc",
            day_utc,
        ]
    if producer_id == "operator_statement_seed":
        return _build_operator_statement_producer_cmd(repo_root=repo_root, day_utc=day_utc)
    raise ValueError(f"UNKNOWN_ADMISSION_PRODUCER:{producer_id}")


def _producer_status_for_attempt(*, returncode: int, output_paths: list[str], existing_before: set[str]) -> str:
    existing_after = {path for path in output_paths if Path(path).exists()}
    if returncode == 0 and existing_after == set(output_paths):
        return "PASS"
    if existing_after - existing_before:
        return "PARTIAL"
    return "FAIL"


def _await_output_paths(
    *,
    output_paths: list[str],
    timeout_seconds: float = 2.0,
    poll_interval_seconds: float = 0.05,
) -> set[str]:
    if not output_paths:
        return set()
    deadline = wall_time.monotonic() + max(timeout_seconds, 0.0)
    required = {str(path) for path in output_paths}
    present = {path for path in required if Path(path).exists()}
    while present != required and wall_time.monotonic() < deadline:
        wall_time.sleep(poll_interval_seconds)
        present = {path for path in required if Path(path).exists()}
    return present


def _empty_producer_attempts(*, session_id: str, day_utc: str, recorded_at: str) -> PaperSessionProducerAttemptsV1:
    return build_paper_session_producer_attempts_v1(
        attempt_set_id=f"paper_session_producer_attempts:{day_utc}:PAPER",
        session_id=session_id,
        day_utc=day_utc,
        status="NOT_NEEDED",
        attempted_producer_ids=[],
        producer_attempts=[],
        recorded_at=recorded_at,
    )


def _run_governed_admission_producers_v1(
    *,
    repo_root: Path,
    definition: PaperSessionDefinitionV1,
    graph: PaperSessionDependencyGraphV1,
    evaluation: "ClosureEvaluationV1 | None",
    recorded_at: str,
) -> PaperSessionProducerAttemptsV1:
    if evaluation is None:
        return _empty_producer_attempts(
            session_id=definition.session_id,
            day_utc=definition.day_utc,
            recorded_at=recorded_at,
        )
    producer_specs = _producer_specs_by_dependency(repo_root)
    blocked_dependency_keys = {str(blocker["dependency_key"]) for blocker in evaluation.blocker_ledger.blockers}
    candidate_specs: dict[str, dict[str, Any]] = {}
    for dependency_key in blocked_dependency_keys:
        spec = _producer_spec_for_dependency(producer_specs=producer_specs, dependency_key=dependency_key)
        if spec is None:
            continue
        producer_id = str(spec.get("producer_id") or "").strip()
        if producer_id:
            row = candidate_specs.setdefault(
                producer_id,
                {
                    "spec": spec,
                    "matched_dependency_keys": [],
                },
            )
            row["matched_dependency_keys"].append(dependency_key)
    if not candidate_specs:
        return _empty_producer_attempts(
            session_id=definition.session_id,
            day_utc=definition.day_utc,
            recorded_at=recorded_at,
        )

    node_path_by_dependency = {
        str(node["dependency_key"]): str(node["path"])
        for node in graph.nodes
    }
    ordered_specs = sorted(
        candidate_specs.values(),
        key=lambda row: (int(row["spec"].get("order") or 9999), str(row["spec"].get("producer_id") or "")),
    )
    attempts: list[dict[str, Any]] = []
    overall_status = "PASS"
    for row in ordered_specs:
        spec = dict(row["spec"])
        producer_id = str(spec.get("producer_id") or "").strip()
        dependency_keys = [
            str(item).strip()
            for item in row["matched_dependency_keys"]
            if str(item).strip()
        ]
        output_paths = [
            node_path_by_dependency[key]
            for key in dependency_keys
            if key in node_path_by_dependency
        ]
        existing_before = {path for path in output_paths if Path(path).exists()}
        cmd = _build_producer_command(repo_root=repo_root, producer_id=producer_id, day_utc=definition.day_utc)
        result = _subprocess_result_dict(cmd, cwd=repo_root)
        existing_after = _await_output_paths(output_paths=output_paths)
        status = _producer_status_for_attempt(
            returncode=int(result["returncode"]),
            output_paths=output_paths,
            existing_before=existing_before,
        )
        if status == "FAIL":
            overall_status = "FAIL"
        elif status == "PARTIAL" and overall_status == "PASS":
            overall_status = "PARTIAL"
        attempts.append(
            {
                "producer_id": producer_id,
                "dependency_keys": dependency_keys,
                "invocation_phase": str(spec.get("invocation_phase") or ""),
                "entrypoint_relpath": str(spec.get("entrypoint_relpath") or ""),
                "status": status,
                "returncode": int(result["returncode"]),
                "command": cmd,
                "output_paths": output_paths,
                "generated_paths": sorted(existing_after - existing_before),
                "stdout": str(result["stdout"]),
                "stderr": str(result["stderr"]),
            }
        )
    return build_paper_session_producer_attempts_v1(
        attempt_set_id=f"paper_session_producer_attempts:{definition.day_utc}:PAPER",
        session_id=definition.session_id,
        day_utc=definition.day_utc,
        status=overall_status,
        attempted_producer_ids=[str(row["spec"].get("producer_id") or "") for row in ordered_specs],
        producer_attempts=attempts,
        recorded_at=recorded_at,
    )


def _enabled_paper_sleeves(registry_path: Path) -> list[dict[str, str]]:
    reg = _load_registry(registry_path)
    sleeves = reg.get("sleeves")
    if not isinstance(sleeves, list):
        raise ValueError(f"SLEEVE_REGISTRY_INVALID:{registry_path}")
    rows: list[dict[str, str]] = []
    for sleeve in sleeves:
        if not isinstance(sleeve, dict):
            continue
        if not bool(sleeve.get("enabled")):
            continue
        if str(sleeve.get("mode") or "").strip().upper() != "PAPER":
            continue
        sleeve_id = str(sleeve.get("sleeve_id") or "").strip()
        truth_partition = str(sleeve.get("truth_partition") or "").strip()
        ib_account = str(sleeve.get("ib_account") or "").strip()
        if not sleeve_id or not truth_partition or not ib_account:
            raise ValueError("ENABLED_PAPER_SLEEVE_INCOMPLETE")
        rows.append(
            {
                "sleeve_id": sleeve_id,
                "truth_partition": truth_partition,
                "ib_account": ib_account,
            }
        )
    if not rows:
        raise ValueError("NO_ENABLED_PAPER_SLEEVES")
    return rows


def _resolve_single_paper_account_from_registry(registry_path: Path) -> str:
    rows = _enabled_paper_sleeves(registry_path)
    accounts = sorted({row["ib_account"] for row in rows})
    if len(accounts) != 1:
        raise ValueError(f"MULTIPLE_ENABLED_PAPER_ACCOUNTS:{accounts}")
    return accounts[0]


def _resolve_operator_statement_path(*, repo_root: Path, day_utc: str, registry_path: Path, operator_statement_path: Path | None = None) -> Path:
    if operator_statement_path is not None:
        return operator_statement_path.resolve()
    execution_registry = _load_registry(repo_root / EXECUTION_DEPENDENCY_REGISTRY_RELPATH)
    deps = execution_registry.get("dependencies")
    if not isinstance(deps, list):
        raise ValueError("EXECUTION_DEPENDENCY_REGISTRY_INVALID")
    operator_dep = next(
        (dep for dep in deps if isinstance(dep, dict) and dep.get("dependency_id") == "operator_statement_input"),
        None,
    )
    if operator_dep is None:
        raise ValueError("OPERATOR_STATEMENT_DEPENDENCY_NOT_FOUND")
    rel = str(operator_dep.get("expected_source_path") or "").replace("<DAY>", day_utc)
    if not rel:
        raise ValueError("OPERATOR_STATEMENT_DEPENDENCY_PATH_EMPTY")
    acct = _resolve_single_paper_account_from_registry(registry_path.resolve())
    path = (repo_root / rel).resolve()
    aligned_path = resolve_aligned_operator_statement_path(
        operator_input_root=(repo_root / "constellation_2").resolve(),
        day_utc=day_utc,
    )
    if path != aligned_path:
        raise ValueError(
            f"OPERATOR_STATEMENT_PATH_REGISTRY_MISMATCH: registry={path} aligned={aligned_path}"
        )
    # account is resolved to enforce the existing single-paper-account invariant during admission
    if not acct:
        raise ValueError("OPERATOR_STATEMENT_ACCOUNT_RESOLUTION_FAILED")
    return aligned_path


def build_default_paper_session_definition_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    recorded_at: str,
    registry_path: Path | None = None,
    engine_registry_path: Path | None = None,
    gate_hierarchy_path: Path | None = None,
    operator_statement_path: Path | None = None,
) -> PaperSessionDefinitionV1:
    registry_path = (registry_path or (repo_root / SLEEVE_REGISTRY_RELPATH)).resolve()
    engine_registry_path = (engine_registry_path or (repo_root / ENGINE_REGISTRY_RELPATH)).resolve()
    gate_hierarchy_path = (gate_hierarchy_path or (repo_root / GATE_HIERARCHY_RELPATH)).resolve()
    sleeves = _enabled_paper_sleeves(registry_path)
    operator_statement = _resolve_operator_statement_path(
        repo_root=repo_root,
        day_utc=day_utc,
        registry_path=registry_path,
        operator_statement_path=operator_statement_path,
    )
    dependency_keys = [
        "readiness:paper_open_readiness",
        "readiness:subsystem_readiness_report",
        "readiness:session_readiness_refresh",
        "readiness:baseline_ready",
        "registry:sleeve_registry",
        "registry:engine_registry",
        "registry:gate_hierarchy",
        f"operator_input:operator_statement:{operator_statement}",
    ]
    runtime_root = _runtime_root_from_truth_root(truth_root)
    for sleeve in sleeves:
        dependency_keys.append(f"truth_root:{(runtime_root / sleeve['truth_partition']).resolve()}")
    return build_paper_session_definition_v1(
        session_id=f"paper_session:{day_utc}:PAPER",
        day_utc=day_utc,
        mode="PAPER",
        operator_entrypoint=DEFAULT_OPERATOR_ENTRYPOINT_V1,
        execution_entrypoint=DEFAULT_EXECUTION_ENTRYPOINT_V1,
        required_artifact_families=(
            "paper_open_readiness_v1",
            "subsystem_readiness_report_v1",
            "session_readiness_refresh_v1",
            "baseline_ready_v1",
        ),
        required_dependency_keys=dependency_keys,
        dependency_source_refs=(
            str(registry_path),
            str(engine_registry_path),
            str(gate_hierarchy_path),
            str(repo_root / EXECUTION_DEPENDENCY_REGISTRY_RELPATH),
        ),
        declared_sleeve_ids=[row["sleeve_id"] for row in sleeves],
        recorded_at=recorded_at,
    )


def build_paper_session_graph_from_definition_v1(
    *,
    definition: PaperSessionDefinitionV1,
    repo_root: Path,
    truth_root: Path,
    registry_path: Path | None = None,
    engine_registry_path: Path | None = None,
    gate_hierarchy_path: Path | None = None,
    operator_statement_path: Path | None = None,
    recorded_at: str,
) -> PaperSessionDependencyGraphV1:
    registry_path = (registry_path or (repo_root / SLEEVE_REGISTRY_RELPATH)).resolve()
    engine_registry_path = (engine_registry_path or (repo_root / ENGINE_REGISTRY_RELPATH)).resolve()
    gate_hierarchy_path = (gate_hierarchy_path or (repo_root / GATE_HIERARCHY_RELPATH)).resolve()
    sleeves = _enabled_paper_sleeves(registry_path)
    operator_statement = _resolve_operator_statement_path(
        repo_root=repo_root,
        day_utc=definition.day_utc,
        registry_path=registry_path,
        operator_statement_path=operator_statement_path,
    )
    nodes: list[dict[str, Any]] = [
        {
            "node_id": "paper_open_readiness",
            "node_type": "report_artifact",
            "dependency_key": "readiness:paper_open_readiness",
            "path": str(resolve_paper_open_readiness_path(truth_root=truth_root, day_utc=definition.day_utc)),
            "required": True,
            "schema_relpath": PAPER_OPEN_SCHEMA_RELPATH,
            "expected_status": "READY",
            "producer_owner": "testing_evidence_plane",
        },
        {
            "node_id": "subsystem_readiness_report",
            "node_type": "report_artifact",
            "dependency_key": "readiness:subsystem_readiness_report",
            "path": str(resolve_subsystem_readiness_report_path(truth_root=truth_root, day_utc=definition.day_utc)),
            "required": True,
            "schema_relpath": SUBSYSTEM_SCHEMA_RELPATH,
            "expected_status": "READY",
            "producer_owner": "testing_evidence_plane",
        },
        {
            "node_id": "session_readiness_refresh",
            "node_type": "report_artifact",
            "dependency_key": "readiness:session_readiness_refresh",
            "path": str(resolve_session_readiness_refresh_path(truth_root=truth_root, day_utc=definition.day_utc)),
            "required": True,
            "schema_relpath": SESSION_REFRESH_SCHEMA_RELPATH,
            "expected_status": "PASS",
            "producer_owner": "session_refresh",
        },
        {
            "node_id": "baseline_ready",
            "node_type": "readiness_artifact",
            "dependency_key": "readiness:baseline_ready",
            "path": str(resolve_baseline_ready_path(truth_root=truth_root, day_utc=definition.day_utc)),
            "required": True,
            "producer_owner": "baseline_readiness",
        },
        {
            "node_id": "sleeve_registry",
            "node_type": "registry",
            "dependency_key": "registry:sleeve_registry",
            "path": str(registry_path),
            "required": True,
            "producer_owner": "governance",
            "source_registry_ref": str(registry_path),
        },
        {
            "node_id": "engine_registry",
            "node_type": "registry",
            "dependency_key": "registry:engine_registry",
            "path": str(engine_registry_path),
            "required": True,
            "producer_owner": "governance",
            "source_registry_ref": str(engine_registry_path),
        },
        {
            "node_id": "gate_hierarchy",
            "node_type": "registry",
            "dependency_key": "registry:gate_hierarchy",
            "path": str(gate_hierarchy_path),
            "required": True,
            "producer_owner": "governance",
            "source_registry_ref": str(gate_hierarchy_path),
        },
        {
            "node_id": "operator_statement",
            "node_type": "operator_input",
            "dependency_key": f"operator_input:operator_statement:{operator_statement}",
            "path": str(operator_statement),
            "required": True,
            "producer_owner": "operator",
            "source_registry_ref": str(repo_root / EXECUTION_DEPENDENCY_REGISTRY_RELPATH),
        },
    ]
    edges = [
        {"from_node_id": "paper_open_readiness", "to_node_id": "baseline_ready"},
        {"from_node_id": "subsystem_readiness_report", "to_node_id": "baseline_ready"},
        {"from_node_id": "session_readiness_refresh", "to_node_id": "baseline_ready"},
        {"from_node_id": "sleeve_registry", "to_node_id": "operator_statement"},
        {"from_node_id": "sleeve_registry", "to_node_id": "baseline_ready"},
        {"from_node_id": "engine_registry", "to_node_id": "baseline_ready"},
        {"from_node_id": "gate_hierarchy", "to_node_id": "baseline_ready"},
        {"from_node_id": "operator_statement", "to_node_id": "baseline_ready"},
    ]
    runtime_root = _runtime_root_from_truth_root(truth_root)
    for sleeve in sleeves:
        truth_root_path = (runtime_root / sleeve["truth_partition"]).resolve()
        node_id = f"sleeve_truth_root:{sleeve['sleeve_id']}"
        nodes.append(
            {
                "node_id": node_id,
                "node_type": "truth_partition",
                "dependency_key": f"truth_root:{truth_root_path}",
                "path": str(truth_root_path),
                "required": True,
                "producer_owner": "orchestrator",
            }
        )
        edges.append({"from_node_id": "sleeve_registry", "to_node_id": node_id})
    graph_payload = {
        "session_id": definition.session_id,
        "day_utc": definition.day_utc,
        "mode": definition.mode,
        "dependency_keys": [node["dependency_key"] for node in nodes],
        "admitted_sleeve_ids": list(definition.declared_sleeve_ids),
        "nodes": nodes,
        "edges": edges,
    }
    graph_fingerprint = _json_hash(graph_payload)
    return build_paper_session_dependency_graph_v1(
        graph_id=f"paper_session_graph:{definition.day_utc}:PAPER",
        session_id=definition.session_id,
        day_utc=definition.day_utc,
        mode=definition.mode,
        graph_fingerprint=graph_fingerprint,
        dependency_keys=graph_payload["dependency_keys"],
        admitted_sleeve_ids=graph_payload["admitted_sleeve_ids"],
        nodes=nodes,
        edges=edges,
        recorded_at=recorded_at,
    )


def _baseline_ready_ok(path: Path, *, day_utc: str) -> bool:
    obj = _read_json(path)
    if str(obj.get("day_utc") or "").strip() != day_utc:
        return False
    if str(obj.get("status") or "").strip().upper() != "PASS":
        return False
    nav = obj.get("nav") or {}
    nav_total = nav.get("nav_total")
    return isinstance(nav_total, int) and nav_total > 0


def _session_refresh_ok(path: Path, *, day_utc: str) -> bool:
    obj = _read_json(path)
    if str(obj.get("day_utc") or "").strip() != day_utc:
        return False
    status = str(obj.get("status") or "").strip().upper()
    return status in {"PASS", "OK", "READY"}


def _paper_open_ok(path: Path, *, day_utc: str) -> bool:
    obj = _read_json(path)
    PaperOpenReadinessV1.from_dict(obj)
    allowed_field = "paper_" "open_allowed"
    return str(obj.get("overall_status") or "").strip() == "READY" and bool(obj.get(allowed_field)) is True


def _subsystem_report_ok(path: Path, *, day_utc: str) -> bool:
    obj = _read_json(path)
    report = SubsystemReadinessReportV1.from_dict(obj)
    return report.status == "READY" and report.approved_for_next_gate is True


def _candidate_json_paths(path: Path) -> list[Path]:
    if path.suffix != ".json":
        return [path]
    parent = path.parent
    if not parent.exists():
        return []
    return sorted(candidate.resolve() for candidate in parent.glob("*.json"))


def _node_requires_ambiguity_check(node: dict[str, Any]) -> bool:
    return str(node.get("node_type") or "").strip() in {"report_artifact", "readiness_artifact"}


def evaluate_paper_session_closure_v1(
    *,
    definition: PaperSessionDefinitionV1,
    graph: PaperSessionDependencyGraphV1,
    recorded_at: str,
    producer_attempts: PaperSessionProducerAttemptsV1 | None = None,
) -> ClosureEvaluationV1:
    producer_attempts = producer_attempts or _empty_producer_attempts(
        session_id=definition.session_id,
        day_utc=definition.day_utc,
        recorded_at=recorded_at,
    )
    blockers: list[dict[str, Any]] = []
    node_results: list[dict[str, Any]] = []
    for node in graph.nodes:
        node_id = str(node["node_id"])
        dependency_key = str(node["dependency_key"])
        path = Path(str(node["path"]))
        checks: list[str] = []
        status = "PASS"
        if not path.exists():
            status = "FAIL"
            blockers.append(
                {
                    "blocker_id": f"{node_id}:missing",
                    "node_id": node_id,
                    "dependency_key": dependency_key,
                    "blocker_type": "missing_dependency",
                    "reason_code": "MISSING_REQUIRED_DEPENDENCY",
                    "message": f"Required dependency missing: {path}",
                    "path": str(path),
                }
            )
            checks.append("exists=missing")
        else:
            checks.append("exists=present")
            candidates = _candidate_json_paths(path)
            if _node_requires_ambiguity_check(node) and path.suffix == ".json" and len(candidates) > 1:
                status = "FAIL"
                blockers.append(
                    {
                        "blocker_id": f"{node_id}:ambiguity",
                        "node_id": node_id,
                        "dependency_key": dependency_key,
                        "blocker_type": "ambiguity",
                        "reason_code": "MULTIPLE_CANDIDATE_AUTHORITIES",
                        "message": f"Multiple candidate artifacts present for {node_id}",
                        "path": str(path.parent),
                    }
                )
                checks.append("ambiguity=multiple_candidates")
            schema_relpath = str(node.get("schema_relpath") or "").strip()
            if status == "PASS" and schema_relpath:
                try:
                    validate_against_repo_schema_v1(_read_json(path), REPO_ROOT, schema_relpath)
                    checks.append("schema=valid")
                except Exception as exc:
                    status = "FAIL"
                    blockers.append(
                        {
                            "blocker_id": f"{node_id}:schema",
                            "node_id": node_id,
                            "dependency_key": dependency_key,
                            "blocker_type": "schema_failure",
                            "reason_code": "SCHEMA_VALIDATION_FAILED",
                            "message": str(exc),
                            "path": str(path),
                        }
                    )
                    checks.append("schema=invalid")
            if status == "PASS":
                try:
                    if dependency_key == "readiness:paper_open_readiness":
                        ok = _paper_open_ok(path, day_utc=definition.day_utc)
                    elif dependency_key == "readiness:subsystem_readiness_report":
                        ok = _subsystem_report_ok(path, day_utc=definition.day_utc)
                    elif dependency_key == "readiness:session_readiness_refresh":
                        ok = _session_refresh_ok(path, day_utc=definition.day_utc)
                    elif dependency_key == "readiness:baseline_ready":
                        ok = _baseline_ready_ok(path, day_utc=definition.day_utc)
                    else:
                        ok = True
                    checks.append("semantic=ok" if ok else "semantic=fail")
                    if not ok:
                        status = "FAIL"
                        blockers.append(
                            {
                                "blocker_id": f"{node_id}:semantic",
                                "node_id": node_id,
                                "dependency_key": dependency_key,
                                "blocker_type": "semantic_failure",
                                "reason_code": "DEPENDENCY_SEMANTICS_NOT_SATISFIED",
                                "message": f"Dependency semantics not satisfied for {node_id}",
                                "path": str(path),
                            }
                        )
                except Exception as exc:
                    status = "FAIL"
                    blockers.append(
                        {
                            "blocker_id": f"{node_id}:semantic_exception",
                            "node_id": node_id,
                            "dependency_key": dependency_key,
                            "blocker_type": "semantic_failure",
                            "reason_code": "DEPENDENCY_EVALUATION_FAILED",
                            "message": str(exc),
                            "path": str(path),
                        }
                    )
                    checks.append("semantic=error")
        node_results.append(
            {
                "node_id": node_id,
                "dependency_key": dependency_key,
                "status": status,
                "checks": checks,
            }
        )
    node_status = {row["node_id"]: row["status"] for row in node_results}
    edge_results = []
    for edge in graph.edges:
        status = "PASS" if node_status.get(str(edge["from_node_id"])) == "PASS" and node_status.get(str(edge["to_node_id"])) == "PASS" else "FAIL"
        edge_results.append(
            {
                "from_node_id": str(edge["from_node_id"]),
                "to_node_id": str(edge["to_node_id"]),
                "status": status,
            }
        )
    blocker_ledger = build_paper_session_blocker_ledger_v1(
        blocker_ledger_id=f"paper_session_blockers:{definition.day_utc}:PAPER",
        session_id=definition.session_id,
        day_utc=definition.day_utc,
        blockers=blockers,
        recorded_at=recorded_at,
    )
    closure = build_paper_session_closure_v1(
        closure_id=f"paper_session_closure:{definition.day_utc}:PAPER",
        session_id=definition.session_id,
        day_utc=definition.day_utc,
        graph_ref=f"reports/paper_session_dependency_graph_v1/{definition.day_utc}/paper_session_dependency_graph.v1.json",
        blocker_ledger_ref=f"reports/paper_session_blocker_ledger_v1/{definition.day_utc}/paper_session_blocker_ledger.v1.json",
        producer_attempts_ref=_producer_attempts_relpath(definition.day_utc),
        status="PASS" if not blockers else "FAIL",
        node_results=node_results,
        edge_results=edge_results,
        unresolved_blockers=[str(item["blocker_id"]) for item in blockers],
        evaluated_at=recorded_at,
    )
    return ClosureEvaluationV1(
        definition=definition,
        graph=graph,
        blocker_ledger=blocker_ledger,
        closure=closure,
        producer_attempts=producer_attempts,
    )


def run_paper_session_admission_closure_with_producers_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    recorded_at: str,
    registry_path: Path | None = None,
    engine_registry_path: Path | None = None,
    gate_hierarchy_path: Path | None = None,
    operator_statement_path: Path | None = None,
) -> ClosureEvaluationV1:
    definition = build_default_paper_session_definition_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        recorded_at=recorded_at,
        registry_path=registry_path,
        engine_registry_path=engine_registry_path,
        gate_hierarchy_path=gate_hierarchy_path,
        operator_statement_path=operator_statement_path,
    )
    graph = build_paper_session_graph_from_definition_v1(
        definition=definition,
        repo_root=repo_root,
        truth_root=truth_root,
        registry_path=registry_path,
        engine_registry_path=engine_registry_path,
        gate_hierarchy_path=gate_hierarchy_path,
        operator_statement_path=operator_statement_path,
        recorded_at=recorded_at,
    )
    initial_evaluation = evaluate_paper_session_closure_v1(
        definition=definition,
        graph=graph,
        recorded_at=recorded_at,
    )
    producer_attempts = _run_governed_admission_producers_v1(
        repo_root=repo_root,
        definition=definition,
        graph=graph,
        evaluation=initial_evaluation,
        recorded_at=recorded_at,
    )
    return evaluate_paper_session_closure_v1(
        definition=definition,
        graph=graph,
        recorded_at=recorded_at,
        producer_attempts=producer_attempts,
    )


def build_paper_session_envelope_and_certificate_v1(
    *,
    evaluation: ClosureEvaluationV1,
    truth_root: Path,
    issued_at: str,
) -> tuple[PaperSessionEnvelopeV1, PaperSessionAdmissionCertificateV1]:
    if evaluation.closure.status != "PASS":
        raise ValueError("CLOSURE_NOT_PASS")
    truth_root = truth_root.resolve()
    contract_fingerprints = [
        {"path": relpath, "sha256": _sha256_file(REPO_ROOT / relpath)}
        for relpath in PAPER_SESSION_CONTRACT_PATHS_V1
    ]
    config_fingerprint = _json_hash(
        {
            "dependency_keys": list(evaluation.graph.dependency_keys),
            "admitted_sleeve_ids": list(evaluation.graph.admitted_sleeve_ids),
        }
    )
    valid_until = _session_valid_until(evaluation.definition.day_utc)
    runtime_root = _runtime_root_from_truth_root(Path(truth_root))
    envelope = build_paper_session_envelope_v1(
        envelope_id=f"paper_session_envelope:{evaluation.definition.day_utc}:PAPER",
        session_id=evaluation.definition.session_id,
        day_utc=evaluation.definition.day_utc,
        mode="PAPER",
        graph_fingerprint=evaluation.graph.graph_fingerprint,
        dependency_keys=list(evaluation.graph.dependency_keys),
        admitted_sleeve_ids=list(evaluation.graph.admitted_sleeve_ids),
        allowed_truth_roots=[str((runtime_root / f"truth_sleeves/{sleeve_id}/PAPER").resolve()) for sleeve_id in evaluation.graph.admitted_sleeve_ids],
        execution_entrypoint=evaluation.definition.execution_entrypoint,
        contract_fingerprints=contract_fingerprints,
        config_fingerprint=config_fingerprint,
        valid_from=issued_at,
        valid_until=valid_until,
    )
    certificate = build_paper_session_admission_certificate_v1(
        certificate_id=f"paper_session_admission:{evaluation.definition.day_utc}:PAPER",
        session_id=evaluation.definition.session_id,
        day_utc=evaluation.definition.day_utc,
        graph_fingerprint=evaluation.graph.graph_fingerprint,
        envelope_ref=f"reports/paper_session_envelope_v1/{evaluation.definition.day_utc}/paper_session_envelope.v1.json",
        closure_ref=f"reports/paper_session_closure_v1/{evaluation.definition.day_utc}/paper_session_closure.v1.json",
        blocker_ledger_ref=f"reports/paper_session_blocker_ledger_v1/{evaluation.definition.day_utc}/paper_session_blocker_ledger.v1.json",
        issued_at=issued_at,
        valid_until=valid_until,
    )
    return envelope, certificate


def write_paper_session_artifacts_v1(
    *,
    truth_root: Path,
    evaluation: ClosureEvaluationV1,
    envelope: PaperSessionEnvelopeV1 | None = None,
    certificate: PaperSessionAdmissionCertificateV1 | None = None,
) -> dict[str, Path]:
    truth_root = truth_root.resolve()
    day_utc = evaluation.definition.day_utc
    writes = {
        "definition": _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_definition_v1",
                day_utc=day_utc,
                filename="paper_session_definition.v1.json",
            ),
            evaluation.definition.to_dict(),
        ),
        "graph": _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_dependency_graph_v1",
                day_utc=day_utc,
                filename="paper_session_dependency_graph.v1.json",
            ),
            evaluation.graph.to_dict(),
        ),
        "blocker_ledger": _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_blocker_ledger_v1",
                day_utc=day_utc,
                filename="paper_session_blocker_ledger.v1.json",
            ),
            evaluation.blocker_ledger.to_dict(),
        ),
        "producer_attempts": _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_producer_attempts_v1",
                day_utc=day_utc,
                filename="paper_session_producer_attempts.v1.json",
            ),
            evaluation.producer_attempts.to_dict(),
        ),
        "closure": _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_closure_v1",
                day_utc=day_utc,
                filename="paper_session_closure.v1.json",
            ),
            evaluation.closure.to_dict(),
        ),
    }
    if envelope is not None:
        writes["envelope"] = _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_envelope_v1",
                day_utc=day_utc,
                filename="paper_session_envelope.v1.json",
            ),
            envelope.to_dict(),
        )
    if certificate is not None:
        writes["certificate"] = _write_json(
            canonical_paper_session_report_path(
                truth_root=truth_root,
                artifact_family="paper_session_admission_certificate_v1",
                day_utc=day_utc,
                filename="paper_session_admission_certificate.v1.json",
            ),
            certificate.to_dict(),
        )
    return writes


def write_paper_session_divergence_v1(
    *,
    truth_root: Path,
    divergence: PaperSessionDivergenceV1,
) -> Path:
    return _write_json(
        canonical_paper_session_report_path(
            truth_root=truth_root.resolve(),
            artifact_family="paper_session_divergence_v1",
            day_utc=divergence.day_utc,
            filename="paper_session_divergence.v1.json",
        ),
        divergence.to_dict(),
    )


def load_envelope_v1(path: Path) -> PaperSessionEnvelopeV1:
    return PaperSessionEnvelopeV1.from_dict(_read_json(path))


def load_certificate_v1(path: Path) -> PaperSessionAdmissionCertificateV1:
    return PaperSessionAdmissionCertificateV1.from_dict(_read_json(path))


def assert_admitted_dependency_v1(
    *,
    truth_root: Path,
    day_utc: str,
    envelope_path: Path,
    certificate_path: Path,
    dependency_key: str,
    accessed_path: str,
    now_utc: str | None = None,
) -> tuple[PaperSessionEnvelopeV1, PaperSessionAdmissionCertificateV1]:
    envelope = load_envelope_v1(envelope_path)
    certificate = load_certificate_v1(certificate_path)
    detected_at = str(now_utc or _now_utc())
    if certificate.status != "ADMITTED" or certificate.paper_ready is not True:
        raise SystemExit("FAIL: ADMISSION_CERTIFICATE_INVALID")
    if certificate.graph_fingerprint != envelope.graph_fingerprint:
        divergence = build_paper_session_divergence_v1(
            divergence_id=f"paper_session_divergence:{day_utc}:graph_fingerprint",
            session_id=certificate.session_id,
            day_utc=day_utc,
            certificate_ref=str(certificate_path),
            envelope_ref=str(envelope_path),
            dependency_key=dependency_key,
            accessed_path=accessed_path,
            reason_code="ADMISSION_GRAPH_FINGERPRINT_MISMATCH",
            detected_at=detected_at,
        )
        write_paper_session_divergence_v1(truth_root=truth_root, divergence=divergence)
        raise SystemExit("FAIL: ADMISSION_GRAPH_FINGERPRINT_MISMATCH")
    if certificate.day_utc != day_utc or envelope.day_utc != day_utc:
        divergence = build_paper_session_divergence_v1(
            divergence_id=f"paper_session_divergence:{day_utc}:day_mismatch",
            session_id=certificate.session_id,
            day_utc=day_utc,
            certificate_ref=str(certificate_path),
            envelope_ref=str(envelope_path),
            dependency_key=dependency_key,
            accessed_path=accessed_path,
            reason_code="ADMISSION_DAY_MISMATCH",
            detected_at=detected_at,
        )
        write_paper_session_divergence_v1(truth_root=truth_root, divergence=divergence)
        raise SystemExit("FAIL: ADMISSION_DAY_MISMATCH")
    if certificate.valid_until < detected_at:
        divergence = build_paper_session_divergence_v1(
            divergence_id=f"paper_session_divergence:{day_utc}:expiry",
            session_id=certificate.session_id,
            day_utc=day_utc,
            certificate_ref=str(certificate_path),
            envelope_ref=str(envelope_path),
            dependency_key=dependency_key,
            accessed_path=accessed_path,
            reason_code="ADMISSION_CERTIFICATE_EXPIRED",
            detected_at=detected_at,
        )
        write_paper_session_divergence_v1(truth_root=truth_root, divergence=divergence)
        raise SystemExit("FAIL: ADMISSION_CERTIFICATE_EXPIRED")
    if dependency_key not in envelope.dependency_keys:
        divergence = build_paper_session_divergence_v1(
            divergence_id=f"paper_session_divergence:{day_utc}:undeclared",
            session_id=certificate.session_id,
            day_utc=day_utc,
            certificate_ref=str(certificate_path),
            envelope_ref=str(envelope_path),
            dependency_key=dependency_key,
            accessed_path=accessed_path,
            reason_code="UNDECLARED_DEPENDENCY_ACCESS",
            detected_at=detected_at,
        )
        write_paper_session_divergence_v1(truth_root=truth_root, divergence=divergence)
        raise SystemExit(f"FAIL: UNDECLARED_DEPENDENCY_ACCESS dependency_key={dependency_key}")
    return envelope, certificate
