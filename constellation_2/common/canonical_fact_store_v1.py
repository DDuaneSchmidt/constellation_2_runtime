from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1, canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]

FACT_SCHEMA_RELPATHS = {
    "market_close_fact.v1": "governance/04_DATA/SCHEMAS/C2/FACTS/market_close_fact.v1.schema.json",
    "positions_snapshot_fact.v1": "governance/04_DATA/SCHEMAS/C2/FACTS/positions_snapshot_fact.v1.schema.json",
    "intent_fact.v1": "governance/04_DATA/SCHEMAS/C2/FACTS/intent_fact.v1.schema.json",
    "lineage_envelope.v1": "governance/04_DATA/SCHEMAS/C2/FACTS/lineage_envelope.v1.schema.json",
    "risk_policy_fact.v1": "governance/04_DATA/SCHEMAS/C2/FACTS/risk_policy_fact.v1.schema.json",
    "submission_decision_fact.v1": "governance/04_DATA/SCHEMAS/C2/FACTS/submission_decision_fact.v1.schema.json",
}
INVARIANT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/invariant_result.v1.schema.json"
RUN_LEDGER_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/run_ledger.v1.schema.json"
RELEASE_MANIFEST_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json"
ATTEMPT_STATE_SCHEMA_ID = "attempt_state_pointer.v1"
ATTEMPT_STATES = {"ACTIVE", "SUPERSEDED", "HISTORICAL", "ABORTED"}


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_day_utc(payload: dict[str, Any]) -> str:
    for key in ("day_utc", "invoked_day_utc", "effective_day_utc"):
        value = payload.get(key)
        if isinstance(value, str) and len(value) == 10:
            return value
    raise ValueError("FAIL: payload missing canonical day key")


def _canonical_write(path: Path, payload: dict[str, Any]) -> str:
    payload_bytes = canonical_json_bytes_v1(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload_bytes)
    tmp.replace(path)
    return canonical_sha256_hex_v1(payload)


def _attempt_pointer_path(*, truth_root: Path, invoked_day_utc: str, scope_key: str) -> Path:
    return (
        truth_root
        / "run_ledgers_v1"
        / invoked_day_utc
        / "scopes"
        / scope_key
        / "latest_active_attempt.v1.json"
    )


def _run_ledger_path(*, truth_root: Path, invoked_day_utc: str, attempt_id: str) -> Path:
    return truth_root / "run_ledgers_v1" / invoked_day_utc / "attempts" / attempt_id / "run_ledger.v1.json"


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _scope_key(context: dict[str, Any]) -> str:
    sleeve = str(context.get("sleeve") or "GLOBAL")
    mode = str(context.get("mode") or "GLOBAL")
    symbol = str(context.get("symbol") or "GLOBAL")
    return f"{sleeve}__{mode}__{symbol}"


def _release_manifest_path(repo_root: Path) -> Path:
    return (Path(repo_root).resolve() / "release_manifest.v1.json").resolve()


def _load_release_manifest_if_present(*, repo_root: Path) -> dict[str, Any] | None:
    manifest_path = _release_manifest_path(repo_root)
    if not manifest_path.exists() or not manifest_path.is_file():
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(manifest, repo_root, RELEASE_MANIFEST_SCHEMA_RELPATH)
    return manifest


def capture_executed_code_identity(*, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    resolved_repo_root = Path(repo_root).resolve()
    release_manifest = _load_release_manifest_if_present(repo_root=resolved_repo_root)
    git_sha = ""
    branch = ""
    if release_manifest is not None:
        git_sha = str(release_manifest.get("git_sha") or "").strip()
    if not git_sha:
        try:
            git_sha = (
                subprocess.check_output(["git", "-C", str(resolved_repo_root), "rev-parse", "HEAD"], text=True).strip()
            )
        except Exception:
            git_sha = "UNKNOWN"
    try:
        branch = (
            subprocess.check_output(["git", "-C", str(resolved_repo_root), "branch", "--show-current"], text=True).strip()
        )
    except Exception:
        branch = "RELEASE" if release_manifest is not None else "UNKNOWN"
    return {
        "repo_root": str(resolved_repo_root),
        "git_sha": git_sha,
        "branch": branch,
        "release_id": None if release_manifest is None else str(release_manifest.get("release_id") or "").strip() or None,
        "release_manifest_path": None if release_manifest is None else str(_release_manifest_path(resolved_repo_root)),
    }


def validate_fact(*, fact_payload: dict[str, Any], repo_root: Path = REPO_ROOT) -> None:
    schema_id = str(fact_payload.get("schema_id") or "").strip()
    schema_relpath = FACT_SCHEMA_RELPATHS.get(schema_id)
    if not schema_relpath:
        raise ValueError(f"FAIL: unsupported fact schema_id: {schema_id}")
    validate_against_repo_schema_v1(fact_payload, repo_root, schema_relpath)


def write_fact(
    *,
    fact_payload: dict[str, Any],
    truth_root: Path,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    validate_fact(fact_payload=fact_payload, repo_root=repo_root)
    schema_id = str(fact_payload["schema_id"])
    day_utc = _require_day_utc(fact_payload)
    payload_sha256 = canonical_sha256_hex_v1(fact_payload)
    out_path = truth_root / "canonical_facts_v1" / schema_id / day_utc / f"{payload_sha256}.{schema_id}.json"
    written_sha256 = _canonical_write(out_path, fact_payload)
    return {"fact_type": schema_id, "path": str(out_path), "sha256": written_sha256}


def write_invariant_result(
    *,
    invariant_payload: dict[str, Any],
    truth_root: Path,
    attempt_id: str,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    validate_against_repo_schema_v1(invariant_payload, repo_root, INVARIANT_SCHEMA_RELPATH)
    invoked_day_utc = str(invariant_payload["invoked_day_utc"])
    invariant_name = str(invariant_payload["invariant_name"])
    out_path = (
        truth_root
        / "invariants_v1"
        / invoked_day_utc
        / "attempts"
        / attempt_id
        / f"{invariant_name}.invariant_result.v1.json"
    )
    written_sha256 = _canonical_write(out_path, invariant_payload)
    return {
        "invariant_name": invariant_name,
        "passed": bool(invariant_payload["passed"]),
        "path": str(out_path),
        "sha256": written_sha256,
    }


def open_or_create_run_ledger(
    *,
    truth_root: Path,
    run_id: str,
    attempt_id: str,
    invoked_day_utc: str,
    effective_day_utc: str,
    context: dict[str, Any],
    executed_code_identity: dict[str, Any] | None = None,
    supersedes_attempt_id: str | None = None,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    out_path = _run_ledger_path(truth_root=truth_root, invoked_day_utc=invoked_day_utc, attempt_id=attempt_id)
    existing = _load_json_if_exists(out_path)
    if existing is not None:
        validate_against_repo_schema_v1(existing, repo_root, RUN_LEDGER_SCHEMA_RELPATH)
        return existing
    code_identity = executed_code_identity or capture_executed_code_identity(repo_root=repo_root)
    ledger = {
        "schema_id": "run_ledger.v1",
        "schema_version": "v1",
        "run_id": str(run_id),
        "attempt_id": str(attempt_id),
        "invoked_day_utc": str(invoked_day_utc),
        "effective_day_utc": str(effective_day_utc),
        "release_id": code_identity.get("release_id"),
        "git_sha": str(code_identity.get("git_sha") or "UNKNOWN"),
        "context": dict(context),
        "fact_refs": [],
        "invariant_results": [],
        "earliest_failing_invariant": None,
        "executed_code_identity": code_identity,
        "status": "ACTIVE",
        "supersedes_attempt_id": supersedes_attempt_id,
        "generated_at_utc": _utc_now(),
    }
    validate_against_repo_schema_v1(ledger, repo_root, RUN_LEDGER_SCHEMA_RELPATH)
    _canonical_write(out_path, ledger)
    return ledger


def finalize_run_ledger(
    *,
    truth_root: Path,
    ledger_payload: dict[str, Any],
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    failures = [item["invariant_name"] for item in ledger_payload.get("invariant_results", []) if not item["passed"]]
    ledger_payload["earliest_failing_invariant"] = failures[0] if failures else None
    if failures and ledger_payload.get("status") == "ACTIVE":
        ledger_payload["status"] = "ABORTED"
    validate_against_repo_schema_v1(ledger_payload, repo_root, RUN_LEDGER_SCHEMA_RELPATH)
    _canonical_write(
        _run_ledger_path(
            truth_root=truth_root,
            invoked_day_utc=str(ledger_payload["invoked_day_utc"]),
            attempt_id=str(ledger_payload["attempt_id"]),
        ),
        ledger_payload,
    )
    return ledger_payload


def mark_attempt_state(
    *,
    truth_root: Path,
    invoked_day_utc: str,
    attempt_id: str,
    context: dict[str, Any],
    status: str,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    if status not in ATTEMPT_STATES:
        raise ValueError(f"FAIL: unsupported attempt state: {status}")
    ledger_path = _run_ledger_path(truth_root=truth_root, invoked_day_utc=invoked_day_utc, attempt_id=attempt_id)
    ledger_payload = _load_json_if_exists(ledger_path)
    if ledger_payload is None:
        raise ValueError(f"FAIL: run ledger missing for attempt state update: {ledger_path}")
    ledger_payload["status"] = status
    validate_against_repo_schema_v1(ledger_payload, repo_root, RUN_LEDGER_SCHEMA_RELPATH)
    _canonical_write(ledger_path, ledger_payload)

    scope_key = _scope_key(context)
    pointer_path = _attempt_pointer_path(truth_root=truth_root, invoked_day_utc=invoked_day_utc, scope_key=scope_key)
    pointer_payload = {
        "schema_id": ATTEMPT_STATE_SCHEMA_ID,
        "schema_version": "v1",
        "invoked_day_utc": str(invoked_day_utc),
        "scope_key": scope_key,
        "attempt_id": str(attempt_id),
        "status": status,
        "run_ledger_path": str(ledger_path),
        "generated_at_utc": _utc_now(),
    }
    if status == "ACTIVE":
        _canonical_write(pointer_path, pointer_payload)
    elif pointer_path.exists():
        current = _load_json_if_exists(pointer_path)
        if current and str(current.get("attempt_id")) == str(attempt_id):
            _canonical_write(pointer_path, pointer_payload)
    return ledger_payload


def resolve_latest_active_attempt(
    *,
    truth_root: Path,
    invoked_day_utc: str,
    scope_key: str,
) -> dict[str, Any] | None:
    pointer_path = _attempt_pointer_path(truth_root=truth_root, invoked_day_utc=invoked_day_utc, scope_key=scope_key)
    return _load_json_if_exists(pointer_path)
