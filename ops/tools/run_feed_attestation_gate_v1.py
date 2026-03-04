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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    # Use repo canonicalizer if present (preferred), else deterministic json.dumps.
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
    # accept offset iso
    return datetime.fromisoformat(t).astimezone(timezone.utc).replace(microsecond=0)


def _resolve_truth_root(arg_truth_root: str) -> Path:
    tr = (arg_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        # Default (backward compatible): env-aware helper chooses C2_TRUTH_ROOT if set, else DEFAULT_TRUTH_ROOT.
        return resolve_truth_root(repo_root=REPO_ROOT)
    truth_root = Path(tr).resolve()
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {truth_root}")
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {truth_root}")
    try:
        truth_root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FAIL: truth_root not under repo_root: truth_root={truth_root} repo_root={REPO_ROOT}")
    return truth_root


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


def _write_immutable(path: Path, obj: Dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return sha
        raise SystemExit(f"FAIL: FAL_REFUSE_OVERWRITE: {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return sha


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

        # target relpath resolution
        if "target_relpath" in t:
            target_rel = str(t.get("target_relpath") or "").strip()
        else:
            tmpl = str(t.get("target_relpath_template") or "").strip()
            target_rel = tmpl.replace("{DAY}", day)

        if not target_rel:
            raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: target_relpath missing")

        target_path = (REPO_ROOT / target_rel).resolve()
        rcodes: List[str] = []

        if not target_path.exists():
            any_fail = True
            rcodes.append("FAL_TARGET_NOT_FOUND")
            expected_rec_rel = f"{RECORDS_ROOT_RELPATH}/{artifact_id}/{day}/feed_attestation_record.v1.json"

            gate_checks.append({
              "artifact_id": artifact_id,
                "target_relpath": target_rel,
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
                "reason_codes": rcodes
            })
            continue

        # read target json
        try:
            target_obj = _read_json_obj(target_path)
        except Exception:
            any_fail = True
            rcodes.append("FAL_TARGET_JSON_PARSE_ERROR")
            gate_checks.append({
                "artifact_id": artifact_id,
                "target_relpath": target_rel,
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
                "reason_codes": rcodes
            })
            continue

        target_sha = _sha256_file(target_path)

        snap_field = str(t.get("source_snapshot_field") or "").strip()
        if not snap_field:
            raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: source_snapshot_field missing")

        snap_val = str(target_obj.get(snap_field) or "").strip()
        if not snap_val:
            any_fail = True
            rcodes.append("FAL_SNAPSHOT_FIELD_MISSING")
            snap_utc = ""
            staleness_s = 0
        else:
            try:
                snap_dt = _parse_utc_any(snap_val)
                produced_dt = _parse_utc_any(produced_utc)
                staleness_s = int(max(0, (produced_dt - snap_dt).total_seconds()))
                snap_utc = snap_dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                if staleness_s > max_stale:
                    any_fail = True
                    rcodes.append("FAL_STALE_SNAPSHOT_EXCEEDED_THRESHOLD")
            except Exception:
                any_fail = True
                rcodes.append("FAL_SNAPSHOT_UTC_PARSE_FAIL")
                snap_utc = ""
                staleness_s = 0

        upstream_hash: Optional[str] = None
        if require_upstream:
            up_field = str(t.get("upstream_source_hash_field") or "").strip()
            if not up_field:
                raise SystemExit("FAIL: FAL_POLICY_SCHEMA_INVALID: upstream_source_hash_field missing")
            u = str(target_obj.get(up_field) or "").strip()
            if not u:
                any_fail = True
                rcodes.append("FAL_UPSTREAM_HASH_REQUIRED_MISSING")
            else:
                u2 = u.lower()
                if len(u2) != 64 or any(c not in "0123456789abcdef" for c in u2):
                    any_fail = True
                    rcodes.append("FAL_UPSTREAM_HASH_INVALID")
                else:
                    upstream_hash = u2

        # prior record (for seq + hash chain)
        prior_dir = _latest_prior_day_record_dir(records_root, artifact_id, day)
        prev_sha: Optional[str] = None
        prev_seq: int = 0
        if prior_dir is not None:
            prior_path = (prior_dir / "feed_attestation_record.v1.json").resolve()
            try:
                prior_obj = _read_json_obj(prior_path)
                prev_sha = str(prior_obj.get("attestation_sha256") or "").strip() or None
                prev_seq = int(prior_obj.get("sequence_id") or 0)
            except Exception:
                any_fail = True
                rcodes.append("FAL_ATTESTATION_RECORD_PARSE_ERROR")

        seq_id = prev_seq + 1

        # record path for today
        rec_rel = f"{RECORDS_ROOT_RELPATH}/{artifact_id}/{day}/feed_attestation_record.v1.json"
        rec_path = (REPO_ROOT / rec_rel).resolve()

        rec_obj: Dict[str, Any] = {
            "schema_id": "C2_FEED_ATTESTATION_RECORD_V1",
            "schema_version": 1,
            "artifact_id": artifact_id,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {
                "repo": "constellation_2_runtime",
                "git_sha": _git_sha(),
                "module": "ops/tools/run_feed_attestation_gate_v1.py"
            },
            "sequence_id": seq_id,
            "target_relpath": target_rel,
            "target_sha256": target_sha,
            "source_snapshot_utc": snap_utc,
            "upstream_source_hash": upstream_hash,
            "previous_attestation_sha256": prev_sha,
            "attestation_sha256": None
        }

        # self-hash
        unsigned = dict(rec_obj)
        unsigned["attestation_sha256"] = None
        rec_obj["attestation_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned) + b"\n")

        # schema validate record
        _validate(REPO_ROOT, RECORD_SCHEMA_RELPATH, rec_obj)

        # enforce chain validity if prior exists
        if prev_sha is not None:
            if rec_obj["previous_attestation_sha256"] != prev_sha:
                any_fail = True
                rcodes.append("FAL_HASH_CHAIN_BROKEN")
            if rec_obj["sequence_id"] != prev_seq + 1:
                any_fail = True
                rcodes.append("FAL_SEQUENCE_NON_MONOTONIC")

        # write record immutably (or require identical)
        rec_sha = _write_immutable(rec_path, rec_obj)

        # verify record sha matches expected attestation sha
        if rec_obj["attestation_sha256"] != rec_sha:
            any_fail = True
            rcodes.append("FAL_TARGET_SHA_MISMATCH")

        gate_checks.append({
            "artifact_id": artifact_id,
            "target_relpath": target_rel,
            "target_sha256": target_sha,
            "attestation_relpath": rec_rel,
            "attestation_sha256": rec_sha,
            "sequence_id": seq_id,
            "previous_attestation_sha256": prev_sha,
            "source_snapshot_utc": snap_utc,
            "max_staleness_seconds": max_stale,
            "staleness_seconds": int(staleness_s),
            "upstream_source_hash": upstream_hash,
            "pass": (len(rcodes) == 0),
            "reason_codes": rcodes
        })

    status = "PASS" if not any_fail else "FAIL"
    fail_closed = bool(any_fail)

    if status == "PASS":
        gate_reason_codes.append("FAL_PASS")
    else:
        gate_reason_codes.append("FAL_FAIL_CLOSED_REQUIRED")

    out: Dict[str, Any] = {
        "schema_id": "C2_FEED_ATTESTATION_GATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha(),
            "module": "ops/tools/run_feed_attestation_gate_v1.py"
        },
        "status": status,
        "fail_closed": fail_closed,
        "policy": {
            "path": str(POLICY_PATH.relative_to(REPO_ROOT)),
            "sha256": policy_sha,
            "policy_id": "C2_FEED_ATTESTATION_POLICY_V1"
        },
        "checks": gate_checks,
        "gate_sha256": None
    }

    unsigned_gate = dict(out)
    unsigned_gate["gate_sha256"] = None
    out["gate_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned_gate) + b"\n")

    _validate(REPO_ROOT, GATE_SCHEMA_RELPATH, out)

    # immutable write
    gate_out_dir.mkdir(parents=True, exist_ok=True)
    _ = _write_immutable(gate_out_path, out)

    print(out["gate_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
