#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft7Validator, Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1, git_commit_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1


SCHEMA_VERSION = "advisory_evidence_packet.v1"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/advisory_evidence_packet.v1.schema.json"
FORBIDDEN_UNQUALIFIED_FIELDS = {
    "final_status",
    "readiness",
    "ready",
    "submit_allowed",
    "authorized",
    "operator_next_action",
    "next_valid_actions",
}


ADVISORY_CANDIDATES: tuple[dict[str, str], ...] = (
    {
        "artifact_type": "eod_review_v1",
        "production_relpath": "reports/eod_review_v1/{day}/eod_review.v1.json",
        "legacy_relpath": "reports/eod_review_v1/{day}/eod_review.v1.json",
        "schema_path": "",
    },
    {
        "artifact_type": "ai_pattern_review_v1",
        "production_relpath": "reports/ai_pattern_review_v1/daily/{day}/ai_pattern_review.v1.json",
        "legacy_relpath": "reports/ai_pattern_review_v1/daily/{day}/ai_pattern_review.v1.json",
        "schema_path": "",
    },
    {
        "artifact_type": "operator_ai_process_review_v1",
        "production_relpath": "reports/operator_ai_process_review_v1/{day}/operator_ai_process_review.v1.json",
        "legacy_relpath": "reports/operator_ai_process_review_v1/{day}/operator_ai_process_review.v1.json",
        "schema_path": "",
    },
    {
        "artifact_type": "ai_advisory_review_v1",
        "production_relpath": "reports/ai_advisory_review_v1/{day}/ai_advisory_review.v1.json",
        "legacy_relpath": "reports/ai_advisory_review_v1/{day}/ai_advisory_review.v1.json",
        "schema_path": "",
    },
    {
        "artifact_type": "weekly_scorecard_view_v1",
        "production_relpath": "reports/weekly_scorecard_view_v1/{day}/weekly_scorecard_view.v1.json",
        "legacy_relpath": "../truth_sleeves/PRIMARY/PAPER/reports/weekly_scorecard_view_v1/{day}/weekly_scorecard_view.v1.json",
        "schema_path": "governance/04_DATA/SCHEMAS/C2/EVALUATION/weekly_scorecard_view.v1.schema.json",
    },
)


def advisory_evidence_packet_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "advisory_evidence_packet_v1" / day_utc / "advisory_evidence_packet.v1.json").resolve()


def _sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_packet_commit(packet_path: Path) -> str:
    if not packet_path.exists() or not packet_path.is_file():
        return ""
    for line in packet_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.strip().startswith("- git_commit:"):
            return line.split(":", 1)[1].strip()
    return ""


def _validate_schema(schema_path: Path, artifact_path: Path) -> tuple[bool, str]:
    if not schema_path.exists() or not schema_path.is_file():
        return False, "SCHEMA_PATH_MISSING"
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"SCHEMA_OR_ARTIFACT_UNREADABLE:{type(exc).__name__}"
    validator_cls = Draft202012Validator if "2020-12" in str(schema.get("$schema") or "") else Draft7Validator
    errors = sorted(validator_cls(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        return False, "SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3])
    return True, "SCHEMA_VALID"


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _generated_at(payload: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "generated_at", "produced_at_utc", "timestamp"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    pc = payload.get("producer_contract_v1") if isinstance(payload.get("producer_contract_v1"), dict) else {}
    return str(pc.get("generated_at_utc") or "").strip()


def _artifact_day(payload: dict[str, Any]) -> str:
    return str(payload.get("day_utc") or payload.get("trading_day") or "").strip()


def _exclude(artifact_type: str, path: Path, reason: str) -> dict[str, Any]:
    return {"artifact_type": artifact_type, "path": str(path), "reason": reason}


def _included_ref(artifact_type: str, path: Path, payload: dict[str, Any], schema_path: Path) -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "path": str(path),
        "sha256": _sha256(path),
        "schema_path": str(schema_path),
        "artifact_status": str(payload.get("status") or payload.get("surface_kind") or "UNKNOWN"),
        "generated_at": _generated_at(payload),
        "authority": "ADVISORY_ONLY" if artifact_type != "aegis_control_plane_v1" else "READINESS_AUTHORITY_ANCHOR",
        "readiness_usage": "CONTROL_PLANE_COPY_ONLY" if artifact_type == "aegis_control_plane_v1" else "NON_ACTIONABLE_ANALYSIS_ONLY",
    }


def _validate_candidate(
    *,
    artifact_type: str,
    path: Path,
    schema_path: Path,
    truth_root: Path,
    day_utc: str,
    promoted_commit: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not path.exists() or not path.is_file():
        return None, _exclude(artifact_type, path, "MISSING_IN_PRODUCTION_TRUTH")
    if not _is_under(path, truth_root):
        return None, _exclude(artifact_type, path, "WRONG_TRUTH_ROOT_OR_LEGACY_TRUTH")
    if not schema_path.exists():
        return None, _exclude(artifact_type, path, "MISSING_SCHEMA")
    schema_ok, schema_reason = _validate_schema(schema_path, path)
    if not schema_ok:
        return None, _exclude(artifact_type, path, schema_reason)
    payload = read_json_v1(path)
    if _artifact_day(payload) and _artifact_day(payload) != day_utc:
        return None, _exclude(artifact_type, path, "WRONG_DAY")
    if not _generated_at(payload):
        return None, _exclude(artifact_type, path, "MISSING_GENERATED_AT")
    pc = payload.get("producer_contract_v1") if isinstance(payload.get("producer_contract_v1"), dict) else {}
    if not pc:
        return None, _exclude(artifact_type, path, "MISSING_PRODUCER_CONTRACT")
    if str(pc.get("source_dirty_status") or "").strip().upper() != "CLEAN":
        return None, _exclude(artifact_type, path, "DIRTY_SOURCE_ARTIFACT")
    if promoted_commit and str(pc.get("code_version_git_commit") or "").strip() != promoted_commit:
        return None, _exclude(artifact_type, path, "PROMOTED_COMMIT_MISMATCH")
    return _included_ref(artifact_type, path, payload, schema_path), None


def _legacy_exclusions(*, runtime_root: Path, day_utc: str) -> list[dict[str, Any]]:
    legacy_root = runtime_root / "truth"
    out: list[dict[str, Any]] = []
    for candidate in ADVISORY_CANDIDATES:
        rel = candidate["legacy_relpath"].format(day=day_utc)
        path = (legacy_root / rel).resolve()
        if path.exists():
            out.append(_exclude(candidate["artifact_type"], path, "LEGACY_TRUTH_EXCLUDED"))
    return out


def _assert_no_forbidden_authority_fields(payload: dict[str, Any]) -> None:
    bad = sorted(key for key in payload.keys() if key in FORBIDDEN_UNQUALIFIED_FIELDS)
    if bad:
        raise ValueError("FORBIDDEN_UNQUALIFIED_AUTHORITY_FIELDS:" + ",".join(bad))


def _load_promotion_ledger(truth_root: Path, day_utc: str) -> dict[str, Any]:
    return read_json_v1(truth_root / "reports" / "aegis_promotion_validation_ledger_v1" / day_utc / "promotion_validation_ledger.v1.json")


def build_advisory_evidence_packet_v1(*, day_utc: str, truth_root: Path, runtime_root: Path | None = None) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    runtime_root = Path(runtime_root or truth_root).resolve()
    generated_at = now_iso_v1()
    evaluated_commit = git_commit_v1()
    promotion_version = read_json_v1(truth_root / "governance" / "production_version.v1.json")
    promotion_ledger = _load_promotion_ledger(truth_root, day_utc)
    promoted_commit = str(promotion_version.get("promoted_commit") or promotion_ledger.get("promoted_commit") or "").strip()
    packet_path = truth_root / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    packet_commit = _parse_packet_commit(packet_path)
    control_plane_path = truth_root / "reports" / "aegis_control_plane_v1" / day_utc / "control_plane.v1.json"
    control_plane_schema = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_control_plane.v1.schema.json"
    warnings: list[str] = []
    excluded: list[dict[str, Any]] = []
    included: list[dict[str, Any]] = []
    control_plane = read_json_v1(control_plane_path)
    blockers: list[str] = []

    if not promotion_ledger:
        blockers.append("PROMOTION_LEDGER_MISSING")
    if str(promotion_ledger.get("promotion_status") or "").strip().upper() not in {"PROMOTED", "APPROVED"}:
        blockers.append("PROMOTION_LEDGER_NOT_APPROVED")
    if promoted_commit and evaluated_commit != promoted_commit:
        blockers.append("EVALUATED_COMMIT_NOT_PROMOTED")
    if packet_commit and promoted_commit and packet_commit != promoted_commit:
        blockers.append("PACKET_COMMIT_MISMATCH")
    if str(promotion_ledger.get("truth_root") or "").strip() and str(Path(str(promotion_ledger.get("truth_root"))).resolve()) != str(truth_root):
        blockers.append("PROMOTION_TRUTH_ROOT_MISMATCH")
    if str(promotion_ledger.get("runtime_root") or "").strip() and str(Path(str(promotion_ledger.get("runtime_root"))).resolve()) != str(runtime_root):
        blockers.append("PROMOTION_RUNTIME_ROOT_MISMATCH")

    control_ref: dict[str, Any] = {"artifact_type": "aegis_control_plane_v1", "path": str(control_plane_path), "sha256": _sha256(control_plane_path)}
    control_included, control_excluded = _validate_candidate(
        artifact_type="aegis_control_plane_v1",
        path=control_plane_path,
        schema_path=control_plane_schema,
        truth_root=truth_root,
        day_utc=day_utc,
        promoted_commit=promoted_commit,
    )
    if control_included is None:
        blockers.append("CONTROL_PLANE_UNUSABLE_FOR_ADVISORY_PACKET")
        if control_excluded is not None:
            excluded.append(control_excluded)
    else:
        included.append(control_included)

    for candidate in ADVISORY_CANDIDATES:
        schema_text = candidate.get("schema_path", "")
        schema_path = (REPO_ROOT / schema_text).resolve() if schema_text else Path("/__missing_schema__")
        artifact_path = (truth_root / candidate["production_relpath"].format(day=day_utc)).resolve()
        inc, exc = _validate_candidate(
            artifact_type=candidate["artifact_type"],
            path=artifact_path,
            schema_path=schema_path,
            truth_root=truth_root,
            day_utc=day_utc,
            promoted_commit=promoted_commit,
        )
        if inc is not None:
            included.append(inc)
        elif exc is not None:
            excluded.append(exc)

    excluded.extend(_legacy_exclusions(runtime_root=runtime_root.parent if runtime_root.name in {"production_truth", "candidate_truth"} else runtime_root, day_utc=day_utc))
    deferred = [
        {
            "domain": str(row),
            "actionable": False,
            "reason": "DEFERRED_BY_CONTROL_PLANE_UPSTREAM_DOMAIN",
        }
        for row in (control_plane.get("deferred_domains") if isinstance(control_plane.get("deferred_domains"), list) else [])
    ]
    if blockers:
        warnings.extend(blockers)
    status = "BLOCKED" if blockers else "PASS"
    packet = {
        "schema_id": "advisory_evidence_packet",
        "schema_version": SCHEMA_VERSION,
        "advisory_packet_id": hashlib.sha256(f"{day_utc}:{truth_root}:{generated_at}".encode("utf-8")).hexdigest(),
        "generated_at": generated_at,
        "day_utc": day_utc,
        "status": status,
        "ai_consumption_allowed": status == "PASS",
        "promoted_commit": promoted_commit,
        "evaluated_commit": evaluated_commit,
        "packet_commit": packet_commit,
        "truth_root": str(truth_root),
        "runtime_root": str(runtime_root),
        "control_plane_ref": control_ref,
        "control_plane_final_status": str(control_plane.get("final_status") or "UNKNOWN"),
        "control_plane_current_domain": str(control_plane.get("current_domain") or ""),
        "control_plane_current_phase": str(control_plane.get("current_phase") or ""),
        "control_plane_canonical_blocker": str(control_plane.get("canonical_blocker") or ""),
        "control_plane_submit_allowed": bool(control_plane.get("submit_allowed") is True),
        "authority": "ADVISORY_ONLY",
        "readiness_authority": "aegis_control_plane_v1",
        "submit_authority": "aegis_submit_enforcement_v1",
        "operator_action_authority": "CONTROL_PLANE_OR_GOVERNED_RECOVERY_ONLY",
        "included_artifacts": included,
        "excluded_artifacts": excluded,
        "warnings": warnings,
        "non_actionable_deferred_evidence": deferred,
    }
    _assert_no_forbidden_authority_fields(packet)
    return packet


def run_advisory_evidence_gateway_v1(day_utc: str, truth_root: str, runtime_root: str = "") -> tuple[Path, dict[str, Any]]:
    day = parse_day_utc_v1(day_utc)
    root = Path(truth_root).resolve()
    runtime = Path(runtime_root).resolve() if str(runtime_root or "").strip() else root
    payload = build_advisory_evidence_packet_v1(day_utc=day, truth_root=root, runtime_root=runtime)
    out_path = advisory_evidence_packet_path(truth_root=root, day_utc=day)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_advisory_evidence_gateway_v1.py",
        producer_command=f"python3 ops/tools/run_advisory_evidence_gateway_v1.py --day_utc {day} --truth_root {root}",
        input_artifacts=[payload["control_plane_ref"]["path"], root / "governance" / "production_version.v1.json", root / "reports" / "aegis_promotion_validation_ledger_v1" / day / "promotion_validation_ledger.v1.json"],
        output_artifacts=[out_path],
        schema_versions={"advisory_evidence_packet": SCHEMA_VERSION},
    )
    _assert_no_forbidden_authority_fields(payload)
    preview_path = _write_preview(out_path, payload)
    schema_ok, schema_reason = _validate_schema(SCHEMA_PATH, preview_path)
    try:
        preview_path.unlink()
    except FileNotFoundError:
        pass
    if not schema_ok:
        raise SystemExit(f"FAIL: ADVISORY_PACKET_SCHEMA_INVALID: {schema_reason}")
    write_json_v1(out_path, payload)
    return out_path, payload


def _write_preview(path: Path, payload: dict[str, Any]) -> Path:
    tmp = path.with_suffix(path.suffix + ".preview")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")
    return tmp


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_advisory_evidence_gateway_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--runtime_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_advisory_evidence_gateway_v1(args.day_utc, args.truth_root, args.runtime_root)
    print(json.dumps({"status": payload["status"], "ai_consumption_allowed": payload["ai_consumption_allowed"], "advisory_evidence_packet_path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
