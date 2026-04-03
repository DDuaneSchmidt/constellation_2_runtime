#!/usr/bin/env python3
"""
run_feed_attestation_gate_v1.py

Feed Attestation Layer (FAL) — deterministic, fail-closed, pre-trade gate.

Writes:
- Attestation records (hash-chain):
  truth/feed_attestation_v1/records/<ARTIFACT_ID>/<DAY>/feed_attestation_record.v1.json
- Gate report:
  truth/reports/feed_attestation_gate_v1/<DAY>/feed_attestation_gate.v1.json

Non-negotiable:
- Deterministic canonical JSON
- Immutable writes (refuse overwrite)
- Hash-chain + monotonic seq enforcement
- Staleness enforcement (fail-closed)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1  # noqa: E402

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_FEED_ATTESTATION_POLICY_V1.json").resolve()

RECORD_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/FEED_ATTESTATION/feed_attestation_record.v1.schema.json"
GATE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/feed_attestation_gate.v1.schema.json"

RECORDS_ROOT_RELPATH = "feed_attestation_v1/records"
GATE_OUT_RELPATH = "reports/feed_attestation_gate_v1"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json_obj(p: Path) -> Dict[str, Any]:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: missing_or_not_file: {p}")
    try:
        o = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: json_parse_failed: {p}: {e!r}") from e
    if not isinstance(o, dict):
        raise SystemExit(f"FAIL: top_level_not_object: {p}")
    return o


def _canonical_json_bytes_v1(obj: Any) -> bytes:
    try:
        from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore
        return canonical_json_bytes_v1(obj)
    except Exception:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _validate(repo_root: Path, schema_relpath: str, obj: Any) -> None:
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore
    validate_against_repo_schema_v1(obj, repo_root, schema_relpath)


def _parse_day(d: str) -> str:
    s = str(d).strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {s!r}")
    return s


def _parse_utc_any(s: str) -> datetime:
    t = (s or "").strip()
    if not t:
        raise ValueError("EMPTY_TIMESTAMP")
    if t.endswith("Z"):
        return datetime.fromisoformat(t[:-1] + "+00:00").astimezone(timezone.utc).replace(microsecond=0)
    return datetime.fromisoformat(t).astimezone(timezone.utc).replace(microsecond=0)


def _require_truth_root_under_repo(truth_root: Path) -> Path:
    pr = truth_root.expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {pr}")
    try:
        pr.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FAIL: truth_root not under repo_root: truth_root={pr} repo_root={REPO_ROOT}")
    return pr


def _resolve_truth_root(arg_truth_root: str) -> Path:
    tr = (arg_truth_root or "").strip()
    if tr:
        return _require_truth_root_under_repo(Path(tr))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_under_repo(Path(env_root))
    resolved = resolve_truth_root(repo_root=REPO_ROOT)
    return _require_truth_root_under_repo(resolved)


def _latest_prior_day_record_dir(records_root: Path, artifact_id: str, day: str) -> Optional[Path]:
    base = (records_root / artifact_id).resolve()
    if not base.exists() or not base.is_dir():
        return None
    days = sorted([p.name for p in base.iterdir() if p.is_dir()], reverse=True)
    for d in days:
        if d < day:
            cand = (base / d).resolve()
            rec = cand / "feed_attestation_record.v1.json"
            if rec.exists():
                return cand
    return None


def _write_refreshable_day_artifact(
    path: Path,
    obj: Dict[str, Any],
    *,
    expected_schema_id: str,
    expected_schema_version: Any,
    expected_day_utc: str,
    preserve_statuses: Tuple[str, ...] = ("PASS",),
) -> str:
    payload = _canonical_json_bytes_v1(obj) + b"\n"
    wr = write_day_artifact_refreshable_v1(
        path=path,
        data=payload,
        expected_day_utc=expected_day_utc,
        expected_schema_id=expected_schema_id,
        expected_schema_version=expected_schema_version,
        preserve_statuses=preserve_statuses,
    )
    if wr.action == "REFRESHED":
        print(
            f"WARN: FAL_REFRESHED_STALE_ARTIFACT day_utc={expected_day_utc} "
            f"path={path} prior_sha256={wr.prior_sha256} quarantined_path={wr.quarantined_path}"
        )
    return wr.sha256


def _resolve_target_path(truth_root: Path, target_rel: str) -> Path:
    rel = target_rel.strip().lstrip("/")
    if not rel:
        raise SystemExit("FAIL: empty target_relpath")

    # Policy compatibility: some governed target relpaths are rooted at
    # "constellation_2/runtime/truth/...". When running on a sleeve truth root,
    # normalize these to truth-root-relative paths.
    prefixes = (
        "constellation_2/runtime/truth/",
        "constellation_2\\runtime\\truth\\",
    )
    rel_norm = rel
    for pref in prefixes:
        if rel_norm.startswith(pref):
            rel_norm = rel_norm[len(pref):]
            break

    p = (truth_root / rel_norm).resolve()
    try:
        p.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FAIL: target path escapes repo_root: {p}")
    return p


def _relpath_from_truth_root(truth_root: Path, path: Path) -> str:
    return str(path.resolve().relative_to(truth_root.resolve())).replace("\\", "/")


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_feed_attestation_gate_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    truth_root = _resolve_truth_root(args.truth_root)

    policy = _read_json_obj(POLICY_PATH)
    policy_sha = _sha256_file(POLICY_PATH)

    targets = policy.get("targets")
    if not isinstance(targets, list) or not targets:
        raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: targets missing/empty")

    records_root = (truth_root / "feed_attestation_v1" / "records").resolve()
    gate_out_dir = (truth_root / "reports" / "feed_attestation_gate_v1" / day).resolve()
    gate_out_path = (gate_out_dir / "feed_attestation_gate.v1.json").resolve()

    gate_checks: List[Dict[str, Any]] = []
    gate_reason_codes: List[str] = []
    any_fail = False

    for t in targets:
        if not isinstance(t, dict):
            raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: target not object")

        artifact_id = str(t.get("artifact_id") or "").strip()
        if not artifact_id:
            raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: artifact_id missing")

        max_stale = int(t.get("max_staleness_seconds"))
        require_upstream = bool(t.get("require_upstream_source_hash"))

        if "target_relpath" in t:
            target_rel = str(t.get("target_relpath") or "").strip()
        else:
            tmpl = str(t.get("target_relpath_template") or "").strip()
            target_rel = tmpl.replace("{DAY}", day)

        if not target_rel:
            raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: target_relpath missing")

        target_path = _resolve_target_path(truth_root, target_rel)
        target_rel_from_truth = _relpath_from_truth_root(truth_root, target_path)
        expected_rec_rel = f"{RECORDS_ROOT_RELPATH}/{artifact_id}/{day}/feed_attestation_record.v1.json"
        rcodes: List[str] = []

        if not target_path.exists():
            any_fail = True
            rcodes.append("FAL_TARGET_NOT_FOUND")
            gate_checks.append(
                {
                    "artifact_id": artifact_id,
                    "target_relpath": target_rel_from_truth,
                    "target_sha256": "0" * 64,
                    "attestation_relpath": expected_rec_rel,
                    "attestation_sha256": "0" * 64,
                    "sequence_id": 1,
                    "previous_attestation_sha256": None,
                    "source_snapshot_utc": "INVALID",
                    "max_staleness_seconds": max_stale,
                    "staleness_seconds": 0,
                    "upstream_source_hash": None,
                    "pass": False,
                    "reason_codes": rcodes,
                }
            )
            continue

        try:
            target_obj = _read_json_obj(target_path)
        except Exception:
            any_fail = True
            rcodes.append("FAL_TARGET_JSON_PARSE_ERROR")
            gate_checks.append(
                {
                    "artifact_id": artifact_id,
                    "target_relpath": target_rel_from_truth,
                    "target_sha256": _sha256_file(target_path),
                    "attestation_relpath": expected_rec_rel,
                    "attestation_sha256": "0" * 64,
                    "sequence_id": 1,
                    "previous_attestation_sha256": None,
                    "source_snapshot_utc": "INVALID",
                    "max_staleness_seconds": max_stale,
                    "staleness_seconds": 0,
                    "upstream_source_hash": None,
                    "pass": False,
                    "reason_codes": rcodes,
                }
            )
            continue

        target_sha = _sha256_file(target_path)

        snap_field = str(t.get("source_snapshot_field") or "").strip()
        if not snap_field:
            raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: source_snapshot_field missing")

        snap_val = str(target_obj.get(snap_field) or "").strip()
        if not snap_val:
            any_fail = True
            rcodes.append("FAL_SOURCE_SNAPSHOT_MISSING")
            gate_checks.append(
                {
                    "artifact_id": artifact_id,
                    "target_relpath": target_rel_from_truth,
                    "target_sha256": target_sha,
                    "attestation_relpath": expected_rec_rel,
                    "attestation_sha256": "0" * 64,
                    "sequence_id": 1,
                    "previous_attestation_sha256": None,
                    "source_snapshot_utc": "INVALID",
                    "max_staleness_seconds": max_stale,
                    "staleness_seconds": 0,
                    "upstream_source_hash": None,
                    "pass": False,
                    "reason_codes": rcodes,
                }
            )
            continue

        try:
            snap_dt = _parse_utc_any(snap_val)
        except Exception:
            any_fail = True
            rcodes.append("FAL_SOURCE_SNAPSHOT_INVALID")
            gate_checks.append(
                {
                    "artifact_id": artifact_id,
                    "target_relpath": target_rel_from_truth,
                    "target_sha256": target_sha,
                    "attestation_relpath": expected_rec_rel,
                    "attestation_sha256": "0" * 64,
                    "sequence_id": 1,
                    "previous_attestation_sha256": None,
                    "source_snapshot_utc": "INVALID",
                    "max_staleness_seconds": max_stale,
                    "staleness_seconds": 0,
                    "upstream_source_hash": None,
                    "pass": False,
                    "reason_codes": rcodes,
                }
            )
            continue

        produced_dt = _parse_utc_any(produced_utc)
        stale_seconds = int((produced_dt - snap_dt).total_seconds())
        if stale_seconds < 0:
            stale_seconds = 0

        upstream_source_hash = None
        if require_upstream:
            field = str(t.get("upstream_source_hash_field") or "").strip()
            if not field:
                raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: upstream_source_hash_field missing")
            upstream_source_hash = str(target_obj.get(field) or "").strip() or None
            if upstream_source_hash is None:
                rcodes.append("FAL_UPSTREAM_SOURCE_HASH_MISSING")

        if stale_seconds > max_stale:
            rcodes.append("FAL_STALE")

        prior_dir = _latest_prior_day_record_dir(records_root, artifact_id, day)
        prev_sha = None
        sequence_id = 1
        if prior_dir is not None:
            prev_path = (prior_dir / "feed_attestation_record.v1.json").resolve()
            prev_sha = _sha256_file(prev_path)
            prev_obj = _read_json_obj(prev_path)
            try:
                sequence_id = int(prev_obj.get("sequence_id") or 0) + 1
            except Exception:
                raise SystemExit(f"FAIL: FAL_PRIOR_SEQUENCE_INVALID: {prev_path}")

        record_rel = f"{RECORDS_ROOT_RELPATH}/{artifact_id}/{day}/feed_attestation_record.v1.json"
        record_path = (truth_root / record_rel).resolve()

        record_obj: Dict[str, Any] = {
            "schema_id": "C2_FEED_ATTESTATION_RECORD_V1",
            "schema_version": 1,
            "day_utc": day,
            "artifact_id": artifact_id,
            "target_relpath": target_rel_from_truth,
            "target_sha256": target_sha,
            "sequence_id": sequence_id,
            "previous_attestation_sha256": prev_sha,
            "source_snapshot_utc": snap_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "upstream_source_hash": upstream_source_hash,
            "produced_utc": produced_utc,
            "producer": {
                "repo": "constellation_2_runtime",
                "module": "ops/tools/run_feed_attestation_gate_v1.py",
                "git_sha": _git_sha(),
            },
            "attestation_sha256": None,
        }

        unsigned = dict(record_obj)
        unsigned["attestation_sha256"] = None
        record_obj["attestation_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned) + b"\n")

        _validate(REPO_ROOT, RECORD_SCHEMA_RELPATH, record_obj)
        rec_sha = _write_refreshable_day_artifact(
            record_path,
            record_obj,
            expected_schema_id="C2_FEED_ATTESTATION_RECORD_V1",
            expected_schema_version=1,
            expected_day_utc=day,
            preserve_statuses=(),
        )

        passed = len(rcodes) == 0
        if not passed:
            any_fail = True

        gate_checks.append(
            {
                "artifact_id": artifact_id,
                "target_relpath": target_rel_from_truth,
                "target_sha256": target_sha,
                "attestation_relpath": record_rel,
                "attestation_sha256": rec_sha,
                "sequence_id": sequence_id,
                "previous_attestation_sha256": prev_sha,
                "source_snapshot_utc": snap_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "max_staleness_seconds": max_stale,
                "staleness_seconds": stale_seconds,
                "upstream_source_hash": upstream_source_hash,
                "pass": passed,
                "reason_codes": rcodes,
            }
        )

    for chk in gate_checks:
        if not bool(chk.get("pass")):
            gate_reason_codes.extend([str(x) for x in chk.get("reason_codes") or []])

    gate_reason_codes = sorted(set(gate_reason_codes))
    status = "FAIL" if any_fail else "PASS"

    gate_obj: Dict[str, Any] = {
        "schema_id": "C2_FEED_ATTESTATION_GATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_feed_attestation_gate_v1.py",
            "git_sha": _git_sha(),
        },
        "policy": {
            "path": str(POLICY_PATH.relative_to(REPO_ROOT)).replace("\\", "/"),
            "sha256": policy_sha,
            "policy_id": "C2_FEED_ATTESTATION_POLICY_V1",
        },
        "fail_closed": True,
        "checks": gate_checks,
        "status": status,
        "gate_sha256": None,
    }

    unsigned_gate = dict(gate_obj)
    unsigned_gate["gate_sha256"] = None
    gate_obj["gate_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned_gate) + b"\n")

    _validate(REPO_ROOT, GATE_SCHEMA_RELPATH, gate_obj)
    gate_sha = _write_refreshable_day_artifact(
        gate_out_path,
        gate_obj,
        expected_schema_id="C2_FEED_ATTESTATION_GATE_V1",
        expected_schema_version=1,
        expected_day_utc=day,
        preserve_statuses=("PASS",),
    )

    print(
        f"OK: FEED_ATTESTATION_GATE_V1_WRITTEN day_utc={day} "
        f"status={status} path={gate_out_path} sha256={gate_sha} truth_root={truth_root}"
    )
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
