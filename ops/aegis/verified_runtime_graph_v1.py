from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


GRAPH_GENERATOR_NAME = "build_aegis_verified_runtime_graph_v1"
GRAPH_GENERATOR_VERSION = "aegis_verified_runtime_graph.v1"
HYDRATE_GENERATOR_NAME = "build_aegis_chatgpt_hydrate_packet_v1"
HYDRATE_GENERATOR_VERSION = "aegis_chatgpt_hydrate_packet.v1"
SCHEMA_VERSION = "v1"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODULES_ROOT = REPO_ROOT / "aegis" / "modules"

REQUIRED_MANIFEST_KEYS = {
    "module_id",
    "display_name",
    "type",
    "owner",
    "capabilities",
    "inputs",
    "outputs",
    "commands",
    "tests",
    "evidence_artifacts",
    "ui_surfaces",
    "policies",
    "freshness_requirements",
}

CAPABILITY_STATES = [
    "DECLARED",
    "SCHEMA_VALID",
    "EVIDENCE_PRESENT",
    "EVIDENCE_CURRENT",
    "TESTED",
    "VERIFIED",
    "READY",
    "ALLOWED",
    "BLOCKED",
]

DEFAULT_DO_NOT_CLAIM = [
    "Do not claim readiness from code or manifests alone.",
    "Do not claim broker submit/transmit; broker execution is disabled by policy and out of scope.",
    "Do not claim autonomous execution; autonomous execution is disabled by policy.",
    "Do not claim trade advice unless the runtime truth kernel explicitly allows it.",
    "Do not claim Portal truth; Portal is a thin consumer of verified runtime truth.",
]


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def verified_runtime_graph_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_verified_runtime_graph_v1" / day_utc


def verified_runtime_graph_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return verified_runtime_graph_dir_v1(truth_root=truth_root, day_utc=day_utc) / "verified_runtime_graph.v1.json"


def evidence_ledger_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return verified_runtime_graph_dir_v1(truth_root=truth_root, day_utc=day_utc) / "evidence_ledger.v1.json"


def portal_runtime_model_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return verified_runtime_graph_dir_v1(truth_root=truth_root, day_utc=day_utc) / "portal_runtime_model.v1.json"


def chatgpt_hydrate_packet_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return verified_runtime_graph_dir_v1(truth_root=truth_root, day_utc=day_utc) / "chatgpt_hydrate_packet.v1.md"


def canonical_json_bytes_v1(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(payload)).hexdigest()


def file_hash_v1(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_canonical_json_v1(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")


def git_commit_hash_v1(repo_root: Path = REPO_ROOT) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"
    if result.returncode != 0:
        return "UNKNOWN"
    return result.stdout.strip() or "UNKNOWN"



def parse_time_v1(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def graph_staleness_warnings_v1(*, graph: dict[str, Any], truth_root: Path, day_utc: str) -> list[str]:
    warnings: list[str] = []
    graph_day = str(graph.get("day_utc") or "")
    if graph_day != day_utc:
        warnings.append(f"GRAPH_DAY_MISMATCH:graph_day={graph_day}:requested_day={day_utc}")
        return warnings
    kernel_path = _kernel_paths(Path(truth_root).expanduser().resolve(), day_utc)["runtime_truth_kernel"]
    recorded_hash = str((graph.get("input_artifact_hashes") or {}).get(str(kernel_path)) or "")
    current_hash = file_hash_v1(kernel_path) if kernel_path.exists() else ""
    if not kernel_path.exists():
        warnings.append(f"RUNTIME_TRUTH_KERNEL_MISSING:{kernel_path}")
        return warnings
    if recorded_hash and current_hash and recorded_hash != current_hash:
        warnings.append(f"GRAPH_STALE_KERNEL_HASH_CHANGED:{kernel_path}")
    try:
        kernel = _load_json_object(kernel_path)
    except (OSError, ValueError, json.JSONDecodeError):
        warnings.append(f"RUNTIME_TRUTH_KERNEL_UNREADABLE:{kernel_path}")
        return warnings
    graph_generated = parse_time_v1(graph.get("generated_at"))
    kernel_generated = parse_time_v1(kernel.get("generated_at_utc") or kernel.get("generated_at"))
    if graph_generated and kernel_generated and graph_generated < kernel_generated:
        warnings.append(f"GRAPH_STALE_KERNEL_NEWER:{kernel_path}")
    return sorted(set(warnings))

def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _load_manifest(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    stripped = text.lstrip()
    if stripped.startswith("{"):
        payload = json.loads(text)
    else:
        payload = _parse_minimal_yaml_v1(text)
    if not isinstance(payload, dict):
        raise ValueError(f"Manifest object required: {path}")
    payload["_manifest_path"] = str(path)
    return payload


def _parse_minimal_yaml_v1(text: str) -> dict[str, Any]:
    """Parse the small manifest subset used by Aegis without adding a PyYAML dependency."""

    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]

    def scalar(value: str) -> Any:
        value = value.strip()
        if value in {"true", "True"}:
            return True
        if value in {"false", "False"}:
            return False
        if value in {"null", "None", "~"}:
            return None
        if value.startswith("[") or value.startswith("{"):
            return json.loads(value)
        return value.strip("\"'")

    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if line.startswith("- "):
            if not isinstance(parent, list):
                raise ValueError("minimal YAML parser only supports list items under list keys")
            item_text = line[2:].strip()
            if ":" in item_text:
                key, value = item_text.split(":", 1)
                item: dict[str, Any] = {key.strip(): scalar(value) if value.strip() else {}}
                parent.append(item)
                if not value.strip():
                    stack.append((indent, item[key.strip()]))
                else:
                    stack.append((indent, item))
            else:
                parent.append(scalar(item_text))
            continue
        if ":" not in line:
            raise ValueError(f"invalid manifest line: {line}")
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value:
            if isinstance(parent, dict):
                parent[key] = scalar(value)
            else:
                raise ValueError("invalid YAML parent for key")
            continue
        next_container: Any = []
        if isinstance(parent, dict):
            parent[key] = next_container
            stack.append((indent, next_container))
        else:
            raise ValueError("invalid YAML parent for nested key")
    return root


def discover_manifests_v1(modules_root: Path = DEFAULT_MODULES_ROOT) -> list[dict[str, Any]]:
    manifests = []
    for path in sorted(Path(modules_root).glob("**/aegis.module.yaml")):
        manifests.append(_load_manifest(path))
    return manifests


def validate_manifest_v1(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = sorted(REQUIRED_MANIFEST_KEYS - set(manifest))
    if missing:
        errors.append("missing required manifest keys: " + ",".join(missing))
    module_id = str(manifest.get("module_id") or "")
    if not module_id:
        errors.append("module_id is required")
    for key in ("capabilities", "inputs", "outputs", "commands", "tests", "evidence_artifacts", "ui_surfaces", "policies", "freshness_requirements"):
        if key in manifest and not isinstance(manifest.get(key), list):
            errors.append(f"{key} must be a list")
    capability_ids: set[str] = set()
    for capability in manifest.get("capabilities") or []:
        if not isinstance(capability, dict):
            errors.append("capability rows must be objects")
            continue
        capability_id = str(capability.get("capability_id") or "")
        if not capability_id:
            errors.append("capability_id is required")
        if capability_id in capability_ids:
            errors.append(f"duplicate capability_id: {capability_id}")
        capability_ids.add(capability_id)
        required_evidence = capability.get("required_evidence")
        if required_evidence is not None and not isinstance(required_evidence, list):
            errors.append(f"{capability_id}.required_evidence must be a list")
    evidence_ids: set[str] = set()
    for evidence in manifest.get("evidence_artifacts") or []:
        if not isinstance(evidence, dict):
            errors.append("evidence_artifacts rows must be objects")
            continue
        evidence_id = str(evidence.get("evidence_id") or evidence.get("artifact_id") or "")
        if not evidence_id:
            errors.append("evidence artifact evidence_id/artifact_id is required")
        if evidence_id in evidence_ids:
            errors.append(f"duplicate evidence id: {evidence_id}")
        evidence_ids.add(evidence_id)
        if not str(evidence.get("path") or ""):
            errors.append(f"{evidence_id}.path is required")
    if "commands" in manifest and not manifest.get("commands"):
        errors.append("registered module has no declared commands")
    if "tests" in manifest and not manifest.get("tests"):
        errors.append("registered module has no validation tests")
    if "evidence_artifacts" in manifest and not manifest.get("evidence_artifacts"):
        errors.append("registered module has no evidence artifacts")
    return errors


def validate_evidence_id_contract_v1(manifests: list[dict[str, Any]]) -> list[str]:
    evidence_ids = {
        str(evidence.get("evidence_id") or evidence.get("artifact_id") or "")
        for manifest in manifests
        for evidence in (manifest.get("evidence_artifacts") or [])
        if isinstance(evidence, dict)
    }
    errors: list[str] = []
    for manifest in manifests:
        module_id = str(manifest.get("module_id") or Path(str(manifest.get("_manifest_path") or "")).parent.name)
        for capability in manifest.get("capabilities") or []:
            if not isinstance(capability, dict):
                continue
            capability_id = str(capability.get("capability_id") or "")
            required_evidence = capability.get("required_evidence")
            if not isinstance(required_evidence, list):
                continue
            missing = sorted({str(item) for item in required_evidence if str(item) and str(item) not in evidence_ids})
            for evidence_id in missing:
                errors.append(
                    f"{module_id}:{capability_id}:required_evidence unresolved by graph-visible evidence_artifacts:{evidence_id}"
                )
    return sorted(errors)


def _artifact_path_from_spec(*, truth_root: Path, day_utc: str, spec: dict[str, Any]) -> Path:
    raw_path = str(spec.get("path") or "")
    expanded = raw_path.format(truth_root=str(truth_root), day=day_utc, day_utc=day_utc)
    path = Path(expanded)
    if not path.is_absolute():
        path = truth_root / expanded
    return path.expanduser().resolve()


def _artifact_day(payload: dict[str, Any]) -> str:
    for key in ("day_utc", "operational_day", "day", "as_of_day", "date"):
        value = payload.get(key)
        if isinstance(value, str) and len(value) >= 10:
            return value[:10]
    return ""


def _validation_status(path: Path) -> tuple[str, dict[str, Any]]:
    if not path.exists():
        return "MISSING", {}
    try:
        return "VALID", _load_json_object(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return "INVALID", {}


def _freshness_status(*, path: Path, payload: dict[str, Any], day_utc: str, day_scoped: bool) -> str:
    if not path.exists():
        return "MISSING"
    if not day_scoped:
        return "CURRENT"
    artifact_day = _artifact_day(payload)
    if artifact_day and artifact_day != day_utc:
        return "STALE"
    if day_utc not in str(path):
        return "STALE"
    return "CURRENT"


def _build_evidence_entry(
    *,
    truth_root: Path,
    day_utc: str,
    module_id: str,
    spec: dict[str, Any],
    consumed_by: list[str],
) -> dict[str, Any]:
    evidence_id = str(spec.get("evidence_id") or spec.get("artifact_id") or "")
    path = _artifact_path_from_spec(truth_root=truth_root, day_utc=day_utc, spec=spec)
    validation_status, payload = _validation_status(path)
    day_scoped = bool(spec.get("day_scoped", True))
    freshness_status = _freshness_status(path=path, payload=payload, day_utc=day_utc, day_scoped=day_scoped)
    artifact_hash = file_hash_v1(path) if path.exists() and validation_status == "VALID" else ""
    hash_status = "VERIFIED" if artifact_hash else ("MISSING" if not path.exists() else "UNVERIFIED_INVALID_ARTIFACT")
    return {
        "evidence_id": evidence_id,
        "artifact_path": str(path),
        "artifact_hash": artifact_hash,
        "current_artifact_hash": artifact_hash,
        "hash_verification_status": hash_status,
        "artifact_type": str(spec.get("artifact_type") or "runtime_artifact"),
        "producer": str(spec.get("producer") or module_id),
        "generated_at": str(payload.get("generated_at") or payload.get("generated_at_utc") or ""),
        "day_utc": _artifact_day(payload) or day_utc,
        "schema_version": str(payload.get("schema_version") or SCHEMA_VERSION),
        "freshness_status": freshness_status,
        "validation_status": validation_status,
        "consumed_by": sorted(consumed_by),
    }


def _kernel_paths(truth_root: Path, day_utc: str) -> dict[str, Path]:
    base = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc
    return {
        "runtime_truth_kernel": base / "runtime_truth_kernel.v1.json",
        "runtime_evaluation": base / "runtime_evaluation.v1.json",
        "readiness_dependencies": base / "readiness_dependencies.v1.json",
    }


def _control_packet_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports" / "aegis_chatgpt_control_packet_v1" / day_utc / "aegis_chatgpt_control_packet.v1.json"


def _runtime_kernel_support(kernel: dict[str, Any], capability: dict[str, Any]) -> tuple[bool, str]:
    if not kernel:
        return False, "RUNTIME_TRUTH_KERNEL_MISSING"
    support_capability = str(capability.get("kernel_capability") or "")
    if support_capability:
        dependency_graph = kernel.get("runtime_evaluation", {}).get("capabilities") if isinstance(kernel.get("runtime_evaluation"), dict) else {}
        if not isinstance(dependency_graph, dict):
            dependency_graph = kernel.get("dependency_graph") if isinstance(kernel.get("dependency_graph"), dict) else {}
        row = dependency_graph.get(support_capability) if isinstance(dependency_graph, dict) else None
        if not isinstance(row, dict):
            return False, f"KERNEL_CAPABILITY_MISSING:{support_capability}"
        if not bool(row.get("allowed", False)):
            return False, f"KERNEL_CAPABILITY_BLOCKED:{support_capability}"
    if str(kernel.get("runtime_truth_classification") or "") in {"", "UNKNOWN", "DEMO_ONLY"}:
        return False, "KERNEL_TRUTH_NOT_OPERATIONAL"
    return True, "KERNEL_SUPPORT_PRESENT"


def _capability_state(
    *,
    manifest_valid: bool,
    capability: dict[str, Any],
    evidence_by_id: dict[str, dict[str, Any]],
    test_status: str,
    kernel: dict[str, Any],
) -> dict[str, Any]:
    capability_id = str(capability.get("capability_id") or "")
    required_evidence = [str(item) for item in capability.get("required_evidence") or [] if str(item)]
    blockers: list[str] = []
    state_path = ["DECLARED"]
    if not manifest_valid:
        blockers.append("MANIFEST_SCHEMA_INVALID")
        return {"capability_id": capability_id, "state": "BLOCKED", "state_path": state_path + ["BLOCKED"], "blockers": blockers}
    state_path.append("SCHEMA_VALID")
    missing = [evidence_id for evidence_id in required_evidence if evidence_id not in evidence_by_id or evidence_by_id[evidence_id]["validation_status"] == "MISSING"]
    invalid = [evidence_id for evidence_id in required_evidence if evidence_id in evidence_by_id and evidence_by_id[evidence_id]["validation_status"] == "INVALID"]
    if missing:
        blockers.append("REQUIRED_EVIDENCE_MISSING:" + ",".join(sorted(missing)))
    if invalid:
        blockers.append("REQUIRED_EVIDENCE_INVALID:" + ",".join(sorted(invalid)))
    if blockers:
        return {"capability_id": capability_id, "state": "BLOCKED", "state_path": state_path + ["BLOCKED"], "blockers": blockers}
    state_path.append("EVIDENCE_PRESENT")
    stale = [evidence_id for evidence_id in required_evidence if evidence_by_id[evidence_id]["freshness_status"] != "CURRENT"]
    if stale:
        blockers.append("REQUIRED_EVIDENCE_STALE:" + ",".join(sorted(stale)))
        return {"capability_id": capability_id, "state": "BLOCKED", "state_path": state_path + ["BLOCKED"], "blockers": blockers}
    state_path.append("EVIDENCE_CURRENT")
    if test_status != "TESTS_DECLARED":
        blockers.append(test_status)
        return {"capability_id": capability_id, "state": "BLOCKED", "state_path": state_path + ["BLOCKED"], "blockers": blockers}
    state_path.append("TESTED")
    state_path.append("VERIFIED")
    kernel_ok, kernel_reason = _runtime_kernel_support(kernel, capability)
    if not kernel_ok:
        blockers.append(kernel_reason)
        return {"capability_id": capability_id, "state": "BLOCKED", "state_path": state_path + ["BLOCKED"], "blockers": blockers}
    state_path.append("READY")
    allowed_actions = sorted(str(item) for item in capability.get("allowed_actions") or [] if str(item))
    final_state = "READY"
    if allowed_actions and all(action.startswith("READ") or action in {"AUDIT", "HYDRATE", "QUERY"} for action in allowed_actions):
        final_state = "ALLOWED"
        state_path.append("ALLOWED")
    return {"capability_id": capability_id, "state": final_state, "state_path": state_path, "blockers": blockers}


def _module_test_status(manifest: dict[str, Any]) -> str:
    tests = manifest.get("tests") if isinstance(manifest.get("tests"), list) else []
    if not tests:
        return "REGISTERED_MODULE_HAS_NO_VALIDATION"
    for test in tests:
        if not isinstance(test, dict):
            return "TEST_DECLARATION_INVALID"
        path = str(test.get("path") or "")
        if path and not (REPO_ROOT / path).exists():
            return "TEST_PATH_MISSING:" + path
    return "TESTS_DECLARED"


def _portal_binding_errors(modules: list[dict[str, Any]]) -> list[str]:
    all_capabilities = {
        str(capability.get("capability_id") or "")
        for module in modules
        for capability in (module.get("capabilities") or [])
        if isinstance(capability, dict)
    }
    errors: list[str] = []
    for module in modules:
        module_id = str(module.get("module_id") or "")
        for surface in module.get("ui_surfaces") or []:
            if not isinstance(surface, dict):
                errors.append(f"{module_id}:UI_SURFACE_INVALID")
                continue
            capability_id = str(surface.get("capability_id") or "")
            if capability_id and capability_id not in all_capabilities:
                errors.append(f"{module_id}:PORTAL_BINDING_MISSING_CAPABILITY:{capability_id}")
    return sorted(errors)


def _input_hashes(paths: list[Path]) -> dict[str, str]:
    rows: dict[str, str] = {}
    for path in sorted(set(paths), key=lambda item: str(item)):
        rows[str(path)] = file_hash_v1(path) if path.exists() else ""
    return rows


def _with_output_hash(payload: dict[str, Any]) -> dict[str, Any]:
    copy = dict(payload)
    copy["output_hash"] = ""
    copy["output_hash"] = stable_hash_v1(copy)
    return copy


def _runtime_blockers_v1(kernel: dict[str, Any]) -> list[str]:
    if not kernel:
        return ["runtime_truth_kernel:RUNTIME_TRUTH_KERNEL_MISSING"]
    blockers = [f"runtime_truth_kernel:{item}:BLOCKED_BY_KERNEL" for item in kernel.get("blocked_capabilities") or [] if str(item)]
    for row in kernel.get("missing_or_stale_sources") or []:
        if not isinstance(row, dict):
            continue
        artifact_id = str(row.get("artifact_id") or row.get("source_id") or row.get("id") or "UNKNOWN")
        status = str(row.get("status") or row.get("freshness_status") or "MISSING_OR_STALE")
        path = str(row.get("path") or row.get("expected_path") or "")
        blockers.append(f"runtime_truth_kernel:{artifact_id}:{status}:{path}")
    return sorted(set(blockers))


def build_verified_runtime_graph_v1(
    *,
    truth_root: Path = DEFAULT_TRUTH_ROOT,
    day_utc: str,
    run_id: str | None = None,
    generated_at: str | None = None,
    modules_root: Path = DEFAULT_MODULES_ROOT,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = generated_at or now_utc_v1()
    run = run_id or f"aegis-verified-runtime-graph:{day_utc}:{stable_hash_v1({'day_utc': day_utc, 'generated_at': generated})[:16]}"
    manifests = discover_manifests_v1(modules_root)
    kernel_paths = _kernel_paths(root, day_utc)
    kernel = _load_json_object(kernel_paths["runtime_truth_kernel"]) if kernel_paths["runtime_truth_kernel"].exists() else {}
    control_path = _control_packet_path(root, day_utc)
    control_packet = _load_json_object(control_path) if control_path.exists() else {}
    git_sha = git_commit_hash_v1()

    manifest_errors_by_module: dict[str, list[str]] = {}
    modules: list[dict[str, Any]] = []
    all_evidence_specs: dict[str, dict[str, Any]] = {}
    evidence_consumers: dict[str, list[str]] = {}
    evidence_contract_errors = validate_evidence_id_contract_v1(manifests)

    for manifest in manifests:
        module_id = str(manifest.get("module_id") or Path(str(manifest.get("_manifest_path") or "")).parent.name)
        errors = validate_manifest_v1(manifest)
        manifest_errors_by_module[module_id] = errors
        test_status = _module_test_status(manifest)
        module_evidence_ids: set[str] = set()
        for evidence in manifest.get("evidence_artifacts") or []:
            if not isinstance(evidence, dict):
                continue
            evidence_id = str(evidence.get("evidence_id") or evidence.get("artifact_id") or "")
            module_evidence_ids.add(evidence_id)
            all_evidence_specs[evidence_id] = {**evidence, "_module_id": module_id}
            evidence_consumers.setdefault(evidence_id, [])
        for capability in manifest.get("capabilities") or []:
            if not isinstance(capability, dict):
                continue
            capability_id = str(capability.get("capability_id") or "")
            for evidence_id in capability.get("required_evidence") or []:
                evidence_consumers.setdefault(str(evidence_id), []).append(capability_id)
        modules.append({k: v for k, v in manifest.items() if not k.startswith("_")} | {"manifest_path": str(manifest.get("_manifest_path") or ""), "manifest_validation_errors": errors, "test_status": test_status})

    evidence_entries = [
        _build_evidence_entry(
            truth_root=root,
            day_utc=day_utc,
            module_id=str(spec.get("_module_id") or "unknown"),
            spec=spec,
            consumed_by=evidence_consumers.get(evidence_id, []),
        )
        for evidence_id, spec in sorted(all_evidence_specs.items())
    ]
    evidence_by_id = {entry["evidence_id"]: entry for entry in evidence_entries}

    verified_capabilities: list[dict[str, Any]] = []
    dependencies: list[dict[str, Any]] = []
    for module in modules:
        module_id = str(module.get("module_id") or "")
        manifest_valid = not manifest_errors_by_module.get(module_id)
        test_status = str(module.get("test_status") or "")
        for capability in module.get("capabilities") or []:
            if not isinstance(capability, dict):
                continue
            row = _capability_state(
                manifest_valid=manifest_valid,
                capability=capability,
                evidence_by_id=evidence_by_id,
                test_status=test_status,
                kernel=kernel,
            )
            required_evidence = sorted(str(item) for item in capability.get("required_evidence") or [])
            verified_capabilities.append(
                {
                    **{k: v for k, v in capability.items() if k != "required_evidence"},
                    **row,
                    "module_id": module_id,
                    "required_evidence": required_evidence,
                    "evidence_links": [evidence_by_id[evidence_id]["artifact_path"] for evidence_id in required_evidence if evidence_id in evidence_by_id],
                    "readiness_linkage": {
                        "kernel_required": True,
                        "kernel_capability": str(capability.get("kernel_capability") or ""),
                        "kernel_truth_classification": str(kernel.get("runtime_truth_classification") or ""),
                        "kernel_highest_readiness_layer": str(kernel.get("highest_readiness_layer") or ""),
                    },
                }
            )
            for evidence_id in required_evidence:
                dependencies.append({"from": evidence_id, "to": str(capability.get("capability_id") or ""), "type": "EVIDENCE_SUPPORTS_CAPABILITY"})

    portal_errors = _portal_binding_errors(modules)
    blockers = []
    for module_id, errors in sorted(manifest_errors_by_module.items()):
        blockers.extend(f"{module_id}:{error}" for error in errors)
    blockers.extend(evidence_contract_errors)
    blockers.extend(portal_errors)
    blockers.extend(
        f"{row['module_id']}:{row['capability_id']}:{blocker}"
        for row in sorted(verified_capabilities, key=lambda item: (item["module_id"], item["capability_id"]))
        for blocker in row.get("blockers", [])
    )
    if not kernel:
        blockers.append("runtime_truth_kernel:RUNTIME_TRUTH_KERNEL_MISSING")
    runtime_blockers = _runtime_blockers_v1(kernel)

    do_not_claim = sorted(set(DEFAULT_DO_NOT_CLAIM + [str(item) for item in kernel.get("do_not_claim", []) if str(item)] + [str(item) for item in control_packet.get("do_not_claim", []) if str(item)]))
    input_paths = [Path(str(module.get("manifest_path"))) for module in modules if module.get("manifest_path")]
    input_paths.extend(path for path in kernel_paths.values())
    if control_path.exists():
        input_paths.append(control_path)
    input_paths.extend(Path(entry["artifact_path"]) for entry in evidence_entries)

    graph_payload = {
        "schema_id": "aegis_verified_runtime_graph",
        "schema_version": SCHEMA_VERSION,
        "run_id": run,
        "day_utc": day_utc,
        "generated_at": generated,
        "generator_name": GRAPH_GENERATOR_NAME,
        "generator_version": GRAPH_GENERATOR_VERSION,
        "git_commit_hash": git_sha,
        "input_artifact_hashes": _input_hashes(input_paths),
        "output_hash": "",
        "truth_root": str(root),
        "capability_state_order": CAPABILITY_STATES,
        "graph_status": "BLOCKED" if blockers else "READY",
        "runtime_readiness_status": "BLOCKED" if runtime_blockers else "READY",
        "active_mode": str(kernel.get("active_mode") or ""),
        "active_mode_readiness_status": str(kernel.get("active_mode_readiness_status") or "UNKNOWN"),
        "mode_readiness": kernel.get("mode_readiness") if isinstance(kernel.get("mode_readiness"), dict) else {},
        "audit_blockers": sorted(set(blockers)),
        "runtime_blockers": runtime_blockers,
        "declared_modules": modules,
        "verified_capabilities": sorted(verified_capabilities, key=lambda item: (item["module_id"], item["capability_id"])),
        "dependencies": sorted(dependencies, key=lambda item: (item["from"], item["to"], item["type"])),
        "evidence_links": evidence_entries,
        "freshness_status": {
            "day_utc": day_utc,
            "runtime_truth_kernel": "CURRENT" if kernel and str(kernel.get("day_utc") or "") == day_utc else "MISSING_OR_STALE",
            "all_required_evidence_current": all(entry["freshness_status"] == "CURRENT" for entry in evidence_entries if entry["consumed_by"]),
        },
        "test_status": {
            "module_count": len(modules),
            "modules_with_declared_tests": sum(1 for module in modules if module.get("test_status") == "TESTS_DECLARED"),
            "status": "PASS" if modules and all(module.get("test_status") == "TESTS_DECLARED" for module in modules) else "BLOCKED",
        },
        "policy_gates": {
            "declared_not_verified": True,
            "verified_not_ready": True,
            "ready_not_allowed": True,
            "allowed_not_executed": True,
            "broker_submit_transmit_policy": str(kernel.get("broker_submit_transmit_policy") or "DISABLED_BY_DESIGN"),
            "autonomous_execution_policy": str(kernel.get("autonomous_execution_policy") or "DISABLED_BY_DESIGN"),
            "trade_advice_allowed": bool(kernel.get("trade_advice_allowed", False)),
            "manual_trade_capture_allowed": bool(kernel.get("manual_trade_capture_allowed", False)),
        },
        "portal_bindings": _portal_bindings_v1(modules, verified_capabilities, portal_errors),
        "ai_retrieval_routes": _ai_retrieval_routes_v1(verified_capabilities),
        "state_transitions": _state_transitions_v1(verified_capabilities),
        "readiness_linkage_to_runtime_truth_kernel": {
            "runtime_truth_kernel_path": str(kernel_paths["runtime_truth_kernel"]),
            "runtime_evaluation_path": str(kernel_paths["runtime_evaluation"]),
            "runtime_truth_classification": str(kernel.get("runtime_truth_classification") or "MISSING"),
            "highest_readiness_layer": str(kernel.get("highest_readiness_layer") or "MISSING"),
            "blocked_capabilities": sorted(str(item) for item in kernel.get("blocked_capabilities", []) if str(item)),
            "allowed_capabilities": sorted(str(item) for item in kernel.get("allowed_capabilities", []) if str(item)),
            "active_mode": str(kernel.get("active_mode") or ""),
            "active_mode_readiness_status": str(kernel.get("active_mode_readiness_status") or "UNKNOWN"),
        },
        "safety_invariants": {
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "human_approval_gates_removed": False,
            "trade_advice_forbidden_unless_kernel_allows": not bool(kernel.get("trade_advice_allowed", False)),
            "declared_not_verified": True,
            "verified_not_ready": True,
            "ready_not_allowed": True,
            "allowed_not_executed": True,
        },
        "do_not_claim": do_not_claim,
    }
    return _with_output_hash(graph_payload)


def _portal_bindings_v1(modules: list[dict[str, Any]], capabilities: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
    capability_by_id = {str(row.get("capability_id") or ""): row for row in capabilities}
    surfaces: list[dict[str, Any]] = []
    for module in modules:
        module_id = str(module.get("module_id") or "")
        for surface in module.get("ui_surfaces") or []:
            if not isinstance(surface, dict):
                continue
            capability_id = str(surface.get("capability_id") or "")
            cap = capability_by_id.get(capability_id, {})
            surfaces.append(
                {
                    "module_id": module_id,
                    "surface_id": str(surface.get("surface_id") or ""),
                    "route": str(surface.get("route") or ""),
                    "capability_id": capability_id,
                    "capability_state": str(cap.get("state") or "MISSING"),
                    "status": "READY" if cap and cap.get("state") in {"READY", "ALLOWED"} else "BLOCKED",
                }
            )
    return {"status": "BLOCKED" if errors or any(row["status"] == "BLOCKED" for row in surfaces) else "READY", "errors": errors, "surfaces": sorted(surfaces, key=lambda row: (row["route"], row["surface_id"]))}


def _ai_retrieval_routes_v1(capabilities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    routes = []
    for row in capabilities:
        capability_id = str(row.get("capability_id") or "")
        routes.append(
            {
                "capability_id": capability_id,
                "module_id": str(row.get("module_id") or ""),
                "queries": [
                    f"why blocked {capability_id}",
                    f"what evidence supports {capability_id}",
                    f"is {capability_id} ready",
                ],
                "evidence_links": row.get("evidence_links") or [],
            }
        )
    return sorted(routes, key=lambda item: (item["module_id"], item["capability_id"]))


def _state_transitions_v1(capabilities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    transitions: list[dict[str, Any]] = []
    for row in capabilities:
        path = [str(item) for item in row.get("state_path") or []]
        for index in range(1, len(path)):
            transitions.append(
                {
                    "capability_id": str(row.get("capability_id") or ""),
                    "module_id": str(row.get("module_id") or ""),
                    "from_state": path[index - 1],
                    "to_state": path[index],
                }
            )
    return sorted(transitions, key=lambda item: (item["module_id"], item["capability_id"], item["from_state"], item["to_state"]))



def verify_evidence_entry_hash_v1(entry: dict[str, Any]) -> dict[str, Any]:
    path = Path(str(entry.get("artifact_path") or ""))
    expected_hash = str(entry.get("artifact_hash") or "")
    current_hash = file_hash_v1(path) if path.exists() else ""
    if not path.exists():
        status = "MISSING"
    elif not expected_hash:
        status = "NO_RECORDED_HASH"
    elif current_hash == expected_hash:
        status = "VERIFIED"
    else:
        status = "MISMATCH"
    return {**entry, "current_artifact_hash": current_hash, "hash_verification_status": status}


def verify_evidence_ledger_hashes_v1(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [verify_evidence_entry_hash_v1(entry) for entry in entries]


def render_graph_diff_v1(*, from_graph: dict[str, Any], to_graph: dict[str, Any]) -> str:
    lines = [
        "AEGIS VERIFIED RUNTIME GRAPH DIFF v1",
        f"from_day: {from_graph.get('day_utc')}",
        f"to_day: {to_graph.get('day_utc')}",
        "",
    ]
    lines.append(f"graph_status: {from_graph.get('graph_status')} -> {to_graph.get('graph_status')}")
    lines.append(f"runtime_readiness_status: {from_graph.get('runtime_readiness_status')} -> {to_graph.get('runtime_readiness_status')}")
    from_blockers = set(from_graph.get("audit_blockers") or []) | set(from_graph.get("runtime_blockers") or [])
    to_blockers = set(to_graph.get("audit_blockers") or []) | set(to_graph.get("runtime_blockers") or [])
    lines.extend(["", "Added Blockers:"])
    added = sorted(to_blockers - from_blockers)
    lines.extend(f"- {item}" for item in added) if added else lines.append("- NONE")
    lines.extend(["", "Removed Blockers:"])
    removed = sorted(from_blockers - to_blockers)
    lines.extend(f"- {item}" for item in removed) if removed else lines.append("- NONE")
    from_caps = {(row.get("module_id"), row.get("capability_id")): row for row in from_graph.get("verified_capabilities") or []}
    to_caps = {(row.get("module_id"), row.get("capability_id")): row for row in to_graph.get("verified_capabilities") or []}
    lines.extend(["", "Capability State Changes:"])
    changes = []
    for key in sorted(set(from_caps) | set(to_caps)):
        from_state = (from_caps.get(key) or {}).get("state", "MISSING")
        to_state = (to_caps.get(key) or {}).get("state", "MISSING")
        if from_state != to_state:
            changes.append(f"{key[0]}.{key[1]}: {from_state} -> {to_state}")
    lines.extend(f"- {item}" for item in changes) if changes else lines.append("- NONE")
    lines.extend(["", "Evidence Hash Changes:"])
    from_hashes = {entry.get("evidence_id"): entry.get("artifact_hash") for entry in from_graph.get("evidence_links") or []}
    to_hashes = {entry.get("evidence_id"): entry.get("artifact_hash") for entry in to_graph.get("evidence_links") or []}
    hash_changes = [f"{key}: {from_hashes.get(key) or 'MISSING'} -> {to_hashes.get(key) or 'MISSING'}" for key in sorted(set(from_hashes) | set(to_hashes)) if from_hashes.get(key) != to_hashes.get(key)]
    lines.extend(f"- {item}" for item in hash_changes) if hash_changes else lines.append("- NONE")
    return "\n".join(lines) + "\n"

def build_evidence_ledger_v1(*, graph: dict[str, Any]) -> dict[str, Any]:
    entries = verify_evidence_ledger_hashes_v1(list(graph["evidence_links"]))
    verification_counts: dict[str, int] = {}
    for entry in entries:
        status = str(entry.get("hash_verification_status") or "UNKNOWN")
        verification_counts[status] = verification_counts.get(status, 0) + 1
    payload = {
        "schema_id": "aegis_verified_runtime_evidence_ledger",
        "schema_version": SCHEMA_VERSION,
        "run_id": graph["run_id"],
        "day_utc": graph["day_utc"],
        "generated_at": graph["generated_at"],
        "generator_name": GRAPH_GENERATOR_NAME,
        "generator_version": GRAPH_GENERATOR_VERSION,
        "git_commit_hash": graph["git_commit_hash"],
        "input_artifact_hashes": graph["input_artifact_hashes"],
        "output_hash": "",
        "entries": entries,
        "hash_verification_summary": verification_counts,
        "hash_verification_status": "PASS" if verification_counts and not any(key in verification_counts for key in {"MISMATCH", "MISSING", "NO_RECORDED_HASH"}) else "BLOCKED",
        "append_only_contract": "new evidence is added as new immutable artifact entries; existing evidence artifacts are never rewritten by this ledger",
    }
    return _with_output_hash(payload)


def build_portal_runtime_model_v1(*, graph: dict[str, Any]) -> dict[str, Any]:
    evidence_entries = verify_evidence_ledger_hashes_v1(list(graph.get("evidence_links") or []))
    hash_counts: dict[str, int] = {}
    for entry in evidence_entries:
        status = str(entry.get("hash_verification_status") or "UNKNOWN")
        hash_counts[status] = hash_counts.get(status, 0) + 1
    forbidden_actions = [
        "Broker submit/transmit",
        "Autonomous execution",
        "Trade advice unless runtime truth kernel explicitly allows it",
        "Manual capture policy changes",
        "Independent Portal readiness inference",
    ]
    payload = {
        "schema_id": "portal_runtime_model",
        "schema_version": SCHEMA_VERSION,
        "run_id": graph["run_id"],
        "day_utc": graph["day_utc"],
        "generated_at": graph["generated_at"],
        "generator_name": GRAPH_GENERATOR_NAME,
        "generator_version": GRAPH_GENERATOR_VERSION,
        "git_commit_hash": graph["git_commit_hash"],
        "input_artifact_hashes": {verified_runtime_graph_path_v1(truth_root=Path(graph["truth_root"]), day_utc=graph["day_utc"]).as_posix(): graph["output_hash"]},
        "output_hash": "",
        "portal_status": graph["portal_bindings"]["status"],
        "graph_status": graph.get("graph_status") or "UNKNOWN",
        "runtime_readiness_status": graph.get("runtime_readiness_status") or "UNKNOWN",
        "active_mode": graph.get("active_mode") or "",
        "active_mode_readiness_status": graph.get("active_mode_readiness_status") or "UNKNOWN",
        "mode_readiness": graph.get("mode_readiness") if isinstance(graph.get("mode_readiness"), dict) else {},
        "top_blockers": sorted(set((graph.get("audit_blockers") or []) + (graph.get("runtime_blockers") or [])))[:25],
        "evidence_hash_verification": {
            "status": "PASS" if hash_counts and not any(key in hash_counts for key in {"MISMATCH", "MISSING", "NO_RECORDED_HASH"}) else "BLOCKED",
            "summary": hash_counts,
            "entries": evidence_entries,
        },
        "allowed_actions": [
            "Run Audit",
            "Explain Blockers",
            "Show Evidence",
            "Graph Diff",
            "Hydrate ChatGPT",
            "Claim Lookup",
        ],
        "forbidden_actions": forbidden_actions,
        "do_not_claim": graph.get("do_not_claim") or [],
        "derivation_source": "verified_runtime_graph.v1.json",
        "readiness_inference_policy": "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE",
        "runtime_truth": graph["readiness_linkage_to_runtime_truth_kernel"],
        "capabilities": [
            {
                "module_id": row["module_id"],
                "capability_id": row["capability_id"],
                "display_name": row.get("display_name") or row["capability_id"],
                "state": row["state"],
                "blockers": row.get("blockers") or [],
                "evidence_links": row.get("evidence_links") or [],
            }
            for row in graph["verified_capabilities"]
        ],
        "surfaces": graph["portal_bindings"]["surfaces"],
        "blockers": sorted(set((graph.get("audit_blockers") or []) + (graph.get("runtime_blockers") or []))),
        "safety_invariants": graph["safety_invariants"],
    }
    return _with_output_hash(payload)


def write_verified_runtime_graph_outputs_v1(*, truth_root: Path, graph: dict[str, Any]) -> dict[str, str]:
    day_utc = str(graph["day_utc"])
    graph_path = verified_runtime_graph_path_v1(truth_root=truth_root, day_utc=day_utc)
    ledger_path = evidence_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    portal_path = portal_runtime_model_path_v1(truth_root=truth_root, day_utc=day_utc)
    ledger = build_evidence_ledger_v1(graph=graph)
    portal = build_portal_runtime_model_v1(graph=graph)
    write_canonical_json_v1(graph_path, graph)
    write_canonical_json_v1(ledger_path, ledger)
    write_canonical_json_v1(portal_path, portal)
    return {"verified_runtime_graph": str(graph_path), "evidence_ledger": str(ledger_path), "portal_runtime_model": str(portal_path)}


def load_graph_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return _load_json_object(verified_runtime_graph_path_v1(truth_root=truth_root, day_utc=day_utc))


def render_query_response_v1(*, graph: dict[str, Any], query: str, warnings: list[str] | None = None) -> str:
    q = query.lower().strip()
    lines = ["AEGIS VERIFIED RUNTIME GRAPH QUERY v1", f"day_utc: {graph.get('day_utc')}", f"query: {query}"]
    if warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {item}" for item in warnings)
    lines.append("")
    if q.startswith("claim:"):
        claim = q.split(":", 1)[1].strip()
        policy = graph.get("policy_gates") or {}
        linkage = graph.get("readiness_linkage_to_runtime_truth_kernel") or {}
        claim_map = {
            "trade advice allowed": bool(policy.get("trade_advice_allowed", False)),
            "manual trade capture allowed": bool(policy.get("manual_trade_capture_allowed", False)),
            "broker submit transmit allowed": bool((graph.get("safety_invariants") or {}).get("broker_submit_transmit_allowed", False)),
            "autonomous execution allowed": bool((graph.get("safety_invariants") or {}).get("autonomous_execution_allowed", False)),
            "portal state current": bool(str(graph.get("graph_status") or "").upper() == "READY" and not warnings),
            "portal state": bool(str(graph.get("graph_status") or "").upper() == "READY" and not warnings),
        }
        allowed = claim_map.get(claim, False)
        lines.extend([
            "Claim Lookup:",
            f"- claim: {claim}",
            f"- allowed: {str(allowed).lower()}",
            f"- runtime_truth_classification: {linkage.get('runtime_truth_classification')}",
            f"- kernel_evidence: {linkage.get('runtime_truth_kernel_path')}",
            f"- policy_evidence: {linkage.get('runtime_evaluation_path')}",
            "- policy_source: runtime_truth_kernel.policy_gates",
            "Do Not Claim:",
        ])
        for item in graph.get("do_not_claim") or []:
            if "Trade advice" in item or "trade advice" in item or "broker" in item or "autonomous" in item:
                lines.append(f"- {item}")
    elif "why" in q and "block" in q:
        lines.append("Blockers:")
        blockers = sorted(set((graph.get("audit_blockers") or []) + (graph.get("runtime_blockers") or [])))
        lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- NONE")
        lines.append("")
        lines.append("Evidence:")
        evidence_paths = sorted({path for row in graph.get("verified_capabilities") or [] for path in row.get("evidence_links", [])})
        lines.extend(f"- {path}" for path in evidence_paths) if evidence_paths else lines.append("- NONE")
    elif "changed" in q:
        lines.append("Change Inputs:")
        for path, digest in sorted((graph.get("input_artifact_hashes") or {}).items()):
            lines.append(f"- {path}: {digest or 'MISSING'}")
    elif "ai" in q and "improve" in q:
        lines.append("AI-Appropriate Improvements:")
        for row in graph.get("verified_capabilities") or []:
            if row.get("state") == "BLOCKED":
                lines.append(f"- Review evidence for {row.get('module_id')}.{row.get('capability_id')}: {', '.join(row.get('blockers') or [])}")
        lines.append("")
        lines.append("Do Not Claim:")
        lines.extend(f"- {item}" for item in graph.get("do_not_claim") or [])
    elif "portal" in q and "current" in q:
        bindings = graph.get("portal_bindings") or {}
        freshness = graph.get("freshness_status") or {}
        lines.append("Portal State:")
        lines.append(f"- status: {bindings.get('status') or 'UNKNOWN'}")
        lines.append(f"- runtime_truth_kernel: {freshness.get('runtime_truth_kernel') or 'UNKNOWN'}")
        lines.append(f"- all_required_evidence_current: {freshness.get('all_required_evidence_current')}")
        for surface in bindings.get("surfaces") or []:
            lines.append(f"- {surface.get('route')}: {surface.get('capability_state')}")
    elif "evidence" in q or "claim" in q:
        lines.append("Evidence Ledger Entries:")
        for entry in graph.get("evidence_links") or []:
            lines.append(f"- {entry.get('evidence_id')}: {entry.get('validation_status')}/{entry.get('freshness_status')}/{entry.get('hash_verification_status')} {entry.get('artifact_path')}")
    else:
        lines.append("Summary:")
        lines.append(f"- graph_status: {graph.get('graph_status')}")
        lines.append(f"- runtime_readiness_status: {graph.get('runtime_readiness_status')}")
        lines.append(f"- runtime_truth_classification: {(graph.get('readiness_linkage_to_runtime_truth_kernel') or {}).get('runtime_truth_classification')}")
        lines.append(f"- capability_count: {len(graph.get('verified_capabilities') or [])}")
        lines.append(f"- blocker_count: {len(graph.get('audit_blockers') or [])}")
    return "\n".join(lines) + "\n"


def build_chatgpt_hydrate_packet_v1(*, graph: dict[str, Any], truth_root: Path, day_utc: str, generated_at: str | None = None) -> str:
    stale_warnings = graph_staleness_warnings_v1(graph=graph, truth_root=truth_root, day_utc=day_utc)
    if stale_warnings:
        raise ValueError("hydrate refuses stale graph: " + ";".join(stale_warnings))
    if (graph.get("freshness_status") or {}).get("runtime_truth_kernel") != "CURRENT":
        raise ValueError("hydrate refuses graph without current runtime truth kernel")
    generated = generated_at or now_utc_v1()
    graph_path = verified_runtime_graph_path_v1(truth_root=truth_root, day_utc=day_utc)
    input_hashes = {str(graph_path): str(graph.get("output_hash") or "")}
    metadata = {
        "run_id": graph.get("run_id"),
        "day_utc": day_utc,
        "generated_at": generated,
        "generator_name": HYDRATE_GENERATOR_NAME,
        "generator_version": HYDRATE_GENERATOR_VERSION,
        "git_commit_hash": graph.get("git_commit_hash") or git_commit_hash_v1(),
        "input_artifact_hashes": input_hashes,
        "schema_version": SCHEMA_VERSION,
        "output_hash": "",
    }
    metadata["output_hash"] = stable_hash_v1(metadata)
    readiness = graph.get("readiness_linkage_to_runtime_truth_kernel") or {}
    blockers = sorted(set((graph.get("audit_blockers") or []) + (graph.get("runtime_blockers") or [])))
    evidence_paths = sorted({entry.get("artifact_path") for entry in graph.get("evidence_links") or [] if entry.get("artifact_path")})
    lines = [
        "# Aegis ChatGPT Hydrate Packet v1",
        "",
        f"- run_id: {metadata['run_id']}",
        f"- day_utc: {day_utc}",
        f"- generated_at: {generated}",
        f"- generator_name: {HYDRATE_GENERATOR_NAME}",
        f"- generator_version: {HYDRATE_GENERATOR_VERSION}",
        f"- git_commit_hash: {metadata['git_commit_hash']}",
        f"- schema_version: {SCHEMA_VERSION}",
        f"- input_artifact_hashes: {json.dumps(input_hashes, sort_keys=True)}",
        f"- output_hash: {metadata['output_hash']}",
        f"- verified_runtime_graph: {graph_path}",
        f"- verified_runtime_graph_hash: {graph.get('output_hash')}",
        "",
        "## Runtime Truth",
        f"- classification: {readiness.get('runtime_truth_classification')}",
        f"- highest_readiness_layer: {readiness.get('highest_readiness_layer')}",
        f"- graph_status: {graph.get('graph_status')}",
        f"- runtime_readiness_status: {graph.get('runtime_readiness_status')}",
        "",
        "## Allowed Actions",
        "- Read verified graph",
        "- Read evidence paths",
        "- Explain blockers from graph",
        "- Request audit, hydrate, repair, or operator review",
        "",
        "## Forbidden Actions",
        "- Broker submit/transmit",
        "- Autonomous execution",
        "- Trade advice unless runtime truth kernel explicitly allows it",
        "- Readiness claims not supported by the verified graph",
        "",
        "## Top Blockers",
    ]
    lines.extend(f"- {item}" for item in blockers[:20]) if blockers else lines.append("- NONE")
    lines.extend(["", "## Evidence Paths"])
    lines.extend(f"- {path}" for path in evidence_paths[:30]) if evidence_paths else lines.append("- NONE")
    lines.extend(["", "## Question Router"])
    for route in graph.get("ai_retrieval_routes") or []:
        lines.append(f"- {route.get('module_id')}.{route.get('capability_id')}: {', '.join(route.get('queries') or [])}")
    lines.extend(["", "## Do Not Claim"])
    for item in graph.get("do_not_claim") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_chatgpt_hydrate_packet_v1(*, truth_root: Path, day_utc: str, graph: dict[str, Any]) -> Path:
    text = build_chatgpt_hydrate_packet_v1(graph=graph, truth_root=truth_root, day_utc=day_utc)
    out_path = chatgpt_hydrate_packet_path_v1(truth_root=truth_root, day_utc=day_utc)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    return out_path


def graph_builder_cli_v1(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_verified_runtime_graph_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--generated-at", default="")
    parser.add_argument("--modules-root", default=str(DEFAULT_MODULES_ROOT))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    graph = build_verified_runtime_graph_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day),
        run_id=args.run_id or None,
        generated_at=args.generated_at or None,
        modules_root=Path(args.modules_root),
    )
    paths = write_verified_runtime_graph_outputs_v1(truth_root=Path(args.truth_root), graph=graph)
    result = {"day_utc": graph["day_utc"], "graph_status": graph["graph_status"], "audit_blocker_count": len(graph["audit_blockers"]), "paths": paths}
    print(json.dumps(result, sort_keys=True))
    if args.strict and graph["audit_blockers"]:
        return 2
    return 0


def query_cli_v1(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="query_aegis_verified_runtime_graph_v1")
    parser.add_argument("query")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    graph = load_graph_v1(truth_root=root, day_utc=str(args.day))
    warnings = graph_staleness_warnings_v1(graph=graph, truth_root=root, day_utc=str(args.day))
    print(render_query_response_v1(graph=graph, query=str(args.query), warnings=warnings), end="")
    return 0



def graph_diff_cli_v1(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="diff_aegis_verified_runtime_graph_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--from-day", required=True)
    parser.add_argument("--to-day", required=True)
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    from_graph = load_graph_v1(truth_root=root, day_utc=str(args.from_day))
    to_graph = load_graph_v1(truth_root=root, day_utc=str(args.to_day))
    print(render_graph_diff_v1(from_graph=from_graph, to_graph=to_graph), end="")
    return 0


def hydrate_cli_v1(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_chatgpt_hydrate_packet_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    graph = load_graph_v1(truth_root=root, day_utc=str(args.day))
    path = write_chatgpt_hydrate_packet_v1(truth_root=root, day_utc=str(args.day), graph=graph)
    print(json.dumps({"day_utc": str(args.day), "path": str(path), "verified_runtime_graph_hash": graph.get("output_hash")}, sort_keys=True))
    return 0
