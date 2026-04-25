#!/usr/bin/env python3
"""
run_global_kill_switch_v1.py

Bundled C: global_kill_switch_state.v1.json writer (immutable truth artifact).

Deterministic + audit-grade.
Fail-closed default: if required inputs are missing or invalid => state=ACTIVE.

Decision authority:
- Kill switch consumes ONLY authorization_gate_verdict_v1 as the entry-decision authority.
- If the authorization verdict is missing/invalid, kill switch must fail closed.

Rerun-safety:
- If artifact exists, treat as authoritative (do not rewrite),
  EXCEPT for provably invalid bootstrap artifacts (self-heal quarantine).

Writes:
  constellation_2/runtime/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json
"""

from __future__ import annotations

# --- deterministic import bootstrap (required for systemd execution) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List, Tuple

from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.runtime_contract_v1 import resolve_truth_sleeves_root
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_sleeve_truth_bindings,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.common.truth_root_v1 import resolve_runtime_root

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
TRUTH = (resolve_runtime_root() / "truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json"
OUT_ROOT = (TRUTH / "risk_v1" / "kill_switch_v1").resolve()
RC_BOOTSTRAP_ALLOW = "C2_DAY0_BOOTSTRAP_ALLOW_ENTRIES_BASELINE_OK_NO_SUBMISSIONS"
RC_MISSING_INPUTS = "C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS"
RC_INPUT_INVALID = "C2_KILL_SWITCH_INPUT_SCHEMA_INVALID"
ENTRY_ALLOWED_STATUSES = {"PASS", "BOOTSTRAP_PASS"}


def _git_sha() -> str:
    try:
        s = str(resolve_release_provenance().get("git_sha") or "").strip()
        if s:
            return s
    except Exception:
        pass
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    o2 = dict(obj)
    o2[field] = None
    return _sha256_bytes(_canonical_bytes(o2))


def _verify_written_report(*, out_path: Path, expected_day_utc: str, expected_payload: Dict[str, Any]) -> None:
    if not out_path.exists() or not out_path.is_file():
        raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_MISSING:path={out_path}")
    obj = _read_json_obj(out_path)
    if str(obj.get("schema_id") or "").strip() != "global_kill_switch_state":
        raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_SCHEMA:path={out_path}")
    if str(obj.get("schema_version") or "").strip() != "v1":
        raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_SCHEMA_VERSION:path={out_path}")
    if str(obj.get("day_utc") or "").strip() != expected_day_utc:
        raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_DAY:path={out_path}")
    for field in ("state", "allow_entries", "allow_exits", "forced_mode"):
        if obj.get(field) != expected_payload.get(field):
            raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_FIELD:path={out_path}:field={field}")
    expected_codes = sorted(str(code).strip() for code in (expected_payload.get("reason_codes") or []) if str(code).strip())
    actual_codes = sorted(str(code).strip() for code in (obj.get("reason_codes") or []) if str(code).strip())
    if actual_codes != expected_codes:
        raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_REASON_CODES:path={out_path}")
    expected_sha = _compute_self_sha(obj, "state_sha256")
    if str(obj.get("state_sha256") or "").strip() != expected_sha:
        raise SystemExit(f"FAIL: GLOBAL_KILL_SWITCH_VERIFY_SELF_SHA:path={out_path}")


def _bootstrap_invariant_ok(existing: Dict[str, Any]) -> bool:
    rcs = existing.get("reason_codes", [])
    if not isinstance(rcs, list):
        return True
    if RC_BOOTSTRAP_ALLOW not in [str(x) for x in rcs]:
        return True

    state = str(existing.get("state") or "").strip().upper()
    allow_entries = bool(existing.get("allow_entries") is True)
    forced_mode = str(existing.get("forced_mode") or "").strip().upper()
    return bool(state == "INACTIVE" and allow_entries and forced_mode == "NORMAL")


def _quarantine_invalid_existing_report(out_path: Path, existing_sha: str, reason: str) -> None:
    invalid_path = out_path.with_name(f"global_kill_switch_state.v1.json.INVALID_{existing_sha}.json")
    if invalid_path.exists():
        raise SystemExit(
            f"FAIL: INVALID_EXISTING_KILL_SWITCH_ALREADY_QUARANTINED: out_path={out_path} "
            f"invalid_path={invalid_path} existing_sha={existing_sha} reason={reason}"
        )
    out_path.rename(invalid_path)
    print(
        f"WARN: QUARANTINED_INVALID_EXISTING_KILL_SWITCH old_path={out_path} "
        f"quarantined_path={invalid_path} sha256={existing_sha} reason={reason}"
    )


def _validate_or_quarantine_existing_report(out_path: Path, expected_day_utc: str) -> None:
    if not out_path.exists():
        return

    existing_sha = _sha256_file(out_path)
    existing = _read_json_obj(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = str(existing.get("schema_version") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()

    if schema_id != "global_kill_switch_state":
        _quarantine_invalid_existing_report(
            out_path,
            existing_sha,
            f"EXISTING_KILL_SWITCH_SCHEMA_MISMATCH:schema_id={schema_id!r}",
        )
        return
    if schema_version != "v1":
        _quarantine_invalid_existing_report(
            out_path,
            existing_sha,
            f"EXISTING_KILL_SWITCH_SCHEMA_VERSION_MISMATCH:schema_version={schema_version!r}",
        )
        return
    if day_utc != expected_day_utc:
        _quarantine_invalid_existing_report(
            out_path,
            existing_sha,
            f"EXISTING_KILL_SWITCH_DAY_MISMATCH:day_utc={day_utc!r}:expected={expected_day_utc!r}",
        )
        return

    state = str(existing.get("state") or "").strip().upper()
    if state == "":
        _quarantine_invalid_existing_report(
            out_path,
            existing_sha,
            "EXISTING_KILL_SWITCH_STATE_MISSING",
        )
        return

    if not _bootstrap_invariant_ok(existing):
        _quarantine_invalid_existing_report(
            out_path,
            existing_sha,
            f"BOOTSTRAP_INVARIANT_FAIL:day_utc={expected_day_utc}",
        )
        return


def _discover_sleeve_kill_switch_paths(day: str) -> Tuple[Path, ...]:
    paths = set()
    try:
        truth_sleeves_root = resolve_truth_sleeves_root().resolve()
    except Exception:
        truth_sleeves_root = None
    if truth_sleeves_root is not None:
        pattern = f"*/*/risk_v1/kill_switch_v1/{day}/global_kill_switch_state.v1.json"
        paths.update(path.resolve() for path in truth_sleeves_root.glob(pattern) if path.is_file())
    try:
        bindings = resolve_governed_sleeve_truth_bindings(
            repo_root=REPO_ROOT,
            environment="PAPER",
            sleeve_id="PRIMARY",
        )
    except Exception:
        bindings = ()
    for binding in bindings:
        truth_root = getattr(binding, "truth_root", None)
        if truth_root is None:
            continue
        paths.add((Path(truth_root).resolve() / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json").resolve())
    return tuple(sorted(paths))


def _quarantine_conflicting_sleeve_kill_switch(path: Path, existing_sha: str, reason: str) -> None:
    quarantine_dir = (path.parent / "__quarantine__").resolve()
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    base = quarantine_dir / f"{path.name}.INVALID_{existing_sha}.json"
    target = base
    counter = 1
    while target.exists():
        target = quarantine_dir / f"{path.name}.INVALID_{existing_sha}.{counter}.json"
        counter += 1
    path.rename(target)
    print(
        "WARN: QUARANTINED_CONTRADICTORY_SLEEVE_KILL_SWITCH "
        f"path={path} quarantined_path={target} sha256={existing_sha} reason={reason}"
    )


def _reconcile_sleeve_kill_switch_mirrors(day: str, canonical_payload: Dict[str, Any]) -> None:
    canonical_bytes = _canonical_bytes(canonical_payload)
    for sleeve_path in _discover_sleeve_kill_switch_paths(day):
        if not sleeve_path.exists():
            sleeve_path.parent.mkdir(parents=True, exist_ok=True)
            sleeve_path.write_bytes(canonical_bytes)
            print(f"WARN: MATERIALIZED_MISSING_SLEEVE_KILL_SWITCH path={sleeve_path} day_utc={day}")
            continue
        try:
            sleeve_payload = _read_json_obj(sleeve_path)
            sleeve_bytes = _canonical_bytes(sleeve_payload)
            sleeve_sha = _sha256_file(sleeve_path)
        except Exception as exc:  # noqa: BLE001
            sleeve_sha = _sha256_file(sleeve_path) if sleeve_path.exists() and sleeve_path.is_file() else _sha256_bytes(b"")
            _quarantine_conflicting_sleeve_kill_switch(
                sleeve_path,
                sleeve_sha,
                f"SLEEVE_KILL_SWITCH_UNREADABLE:{type(exc).__name__}:{exc}",
            )
            sleeve_path.parent.mkdir(parents=True, exist_ok=True)
            sleeve_path.write_bytes(canonical_bytes)
            print(f"WARN: REFRESHED_SLEEVE_KILL_SWITCH_FROM_CANONICAL path={sleeve_path} day_utc={day}")
            continue
        if sleeve_bytes != canonical_bytes:
            _quarantine_conflicting_sleeve_kill_switch(
                sleeve_path,
                sleeve_sha,
                f"SLEEVE_KILL_SWITCH_AUTHORITY_MISMATCH:day_utc={day}",
            )
            sleeve_path.parent.mkdir(parents=True, exist_ok=True)
            sleeve_path.write_bytes(canonical_bytes)
            print(f"WARN: REFRESHED_SLEEVE_KILL_SWITCH_FROM_CANONICAL path={sleeve_path} day_utc={day}")


def _resolve_primary_scoped_authorization_verdict_path(day: str) -> Path:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment="PAPER",
        sleeve_id="PRIMARY",
    )
    primary = bindings[0]
    return (primary.truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()


def _resolve_canonical_authorization_verdict_path(day: str) -> Path:
    return (TRUTH / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()


def _load_inputs(day: str) -> Tuple[List[Dict[str, str]], List[str], Dict[str, Any]]:
    input_manifest: List[Dict[str, str]] = []
    rc: List[str] = []
    decisions: Dict[str, Any] = {}

    verdict_type = "authorization_gate_verdict_v1_missing"
    try:
        scoped_verdict_path = _resolve_primary_scoped_authorization_verdict_path(day)
    except Exception as e:  # noqa: BLE001
        scoped_verdict_path = _resolve_canonical_authorization_verdict_path(day)
        decisions["primary_scoped_authorization_verdict_resolution_error"] = str(e)
    canonical_verdict_path = _resolve_canonical_authorization_verdict_path(day)

    if scoped_verdict_path.exists() and scoped_verdict_path.is_file():
        verdict_path = scoped_verdict_path
        verdict_type = "authorization_gate_verdict_v1_scoped"
    elif canonical_verdict_path.exists() and canonical_verdict_path.is_file():
        verdict_path = canonical_verdict_path
        verdict_type = "authorization_gate_verdict_v1_canonical_fallback"
        decisions["authorization_verdict_fallback"] = "CANONICAL_TRUTH_ROOT"
    else:
        verdict_path = scoped_verdict_path

    if verdict_path.exists() and verdict_path.is_file():
        input_manifest.append({"type": verdict_type, "path": str(verdict_path), "sha256": _sha256_file(verdict_path)})
        try:
            verdict = _read_json_obj(verdict_path)
            if str(verdict.get("schema_id") or "").strip() != "authorization_gate_verdict_v1":
                raise ValueError(f"AUTHORIZATION_VERDICT_SCHEMA_ID_INVALID:{verdict.get('schema_id')!r}")
            decisions["authorization_verdict_status"] = str(verdict.get("status") or "").strip().upper()
        except Exception as e:  # noqa: BLE001
            rc.append(RC_INPUT_INVALID)
            decisions["authorization_verdict_parse_error"] = str(e)
    else:
        input_manifest.append({"type": verdict_type, "path": str(verdict_path), "sha256": _sha256_bytes(b"")})
        rc.append(RC_MISSING_INPUTS)

    return (input_manifest, rc, decisions)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_global_kill_switch_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day!r}")

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "global_kill_switch_state.v1.json").resolve()

    _validate_or_quarantine_existing_report(out_path=out_path, expected_day_utc=day)

    produced_utc = f"{day}T00:00:00Z"

    input_manifest, reason_codes, decisions = _load_inputs(day)

    missing_or_invalid = (RC_MISSING_INPUTS in reason_codes) or (RC_INPUT_INVALID in reason_codes)
    verdict_status = str(decisions.get("authorization_verdict_status") or "").strip().upper()

    if missing_or_invalid:
        state = "ACTIVE"
    elif verdict_status in ENTRY_ALLOWED_STATUSES:
        state = "INACTIVE"
    else:
        state = "ACTIVE"
        reason_codes.append("C2_KILL_SWITCH_ACTIVE")

    allow_entries = (state == "INACTIVE")
    allow_exits = True
    forced_mode = "NORMAL" if state == "INACTIVE" else "FLATTEN_ONLY"

    reason_codes = sorted(list(dict.fromkeys(reason_codes)))

    payload: Dict[str, Any] = {
        "schema_id": "global_kill_switch_state",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_global_kill_switch_v1.py", "git_sha": _git_sha()},
        "state": state,
        "allow_entries": bool(allow_entries),
        "allow_exits": bool(allow_exits),
        "forced_mode": forced_mode,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "state_sha256": None,
    }
    payload["state_sha256"] = _compute_self_sha(payload, "state_sha256")

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    out_dir.mkdir(parents=True, exist_ok=True)
    wr = write_day_artifact_refreshable_v1(
        path=out_path,
        data=_canonical_bytes(payload),
        expected_day_utc=day,
        expected_schema_id="global_kill_switch_state",
        expected_schema_version="v1",
    )
    if wr.action == "REFRESHED":
        print(
            f"WARN: REFRESHED_STALE_KILL_SWITCH day_utc={day} old_sha256={wr.prior_sha256} "
            f"new_sha256={wr.sha256} quarantine={wr.quarantined_path}"
        )

    # Canonical truth is the only governed kill-switch authority surface. If stale
    # sleeve-scoped copies exist and contradict it, quarantine them so authority
    # resolution fails closed only on live contradictions, not on non-authoritative residue.
    _reconcile_sleeve_kill_switch_mirrors(day, payload)
    _verify_written_report(out_path=out_path, expected_day_utc=day, expected_payload=payload)

    print(_canonical_bytes(payload).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
