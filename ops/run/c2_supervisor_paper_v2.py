#!/usr/bin/env python3
"""
C2 Paper Supervisor v2 (always-on, deterministic, fail-closed).

Watches PhaseD outputs submissions root (flat by submission_id). On change, derives affected days and:
- runs exec_evidence truth writer for each day (immutable-safe by producer sha locking)
- runs submission index writer ONLY if submission_index.v1.json is not already present (immutable-safe)

Writes local state only for fingerprints + health (NOT truth; NOT git tracked).
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
VENV_PY = (REPO_ROOT / ".venv_c2/bin/python").resolve()

TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
PHASED_SUBMISSIONS_ROOT = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()

STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()

DEFAULT_POLL_SECONDS = 30


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_day_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _git_sha_head() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _dir_fingerprint_sha256(root: Path) -> str:
    if not root.exists() or not root.is_dir():
        return ""
    items: List[Tuple[str, str]] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        items.append((rel, _sha256_file(p)))
    items.sort(key=lambda x: x[0])
    h = hashlib.sha256()
    for rel, fsha in items:
        h.update(rel.encode("utf-8"))
        h.update(b"\n")
        h.update(fsha.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _write_text_atomic(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(s + "\n", encoding="utf-8")
    tmp.replace(p)


def _write_json_atomic(p: Path, obj: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(p)


def _run(args: List[str]) -> int:
    p = subprocess.run(args, cwd=str(REPO_ROOT))
    return int(p.returncode)


def _extract_day_from_ts(ts: str) -> Optional[str]:
    s = (ts or "").strip()
    if len(s) < 10:
        return None
    day = s[:10]
    if len(day) == 10 and day[4] == "-" and day[7] == "-":
        return day
    return None


def _read_json_obj(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _submission_day_utc(sub_dir: Path) -> Optional[str]:
    bsr = _read_json_obj(sub_dir / "broker_submission_record.v2.json")
    if isinstance(bsr, dict):
        d = _extract_day_from_ts(str(bsr.get("submitted_at_utc") or ""))
        if d:
            return d

    ex = _read_json_obj(sub_dir / "execution_event_record.v1.json")
    if isinstance(ex, dict):
        d = _extract_day_from_ts(str(ex.get("created_at_utc") or ""))
        if d:
            return d
        d2 = _extract_day_from_ts(str(ex.get("event_time_utc") or ""))
        if d2:
            return d2

    b1 = _read_json_obj(sub_dir / "binding_record.v1.json")
    if isinstance(b1, dict):
        d = _extract_day_from_ts(str(b1.get("created_at_utc") or ""))
        if d:
            return d
    b2 = _read_json_obj(sub_dir / "binding_record.v2.json")
    if isinstance(b2, dict):
        d = _extract_day_from_ts(str(b2.get("created_at_utc") or ""))
        if d:
            return d

    return None


def _days_touched_by_phased_root() -> Set[str]:
    days: Set[str] = set()
    if not PHASED_SUBMISSIONS_ROOT.exists():
        return days
    subs = [p for p in PHASED_SUBMISSIONS_ROOT.iterdir() if p.is_dir()]
    subs.sort(key=lambda p: p.name)
    for d in subs:
        day = _submission_day_utc(d)
        if day:
            days.add(day)
    return days


def _lock_producer_sha_for_day(day_utc: str) -> str:
    mdir = (TRUTH / "execution_evidence_v1/manifests" / day_utc).resolve()
    if mdir.exists() and mdir.is_dir():
        manifests = sorted([p for p in mdir.iterdir() if p.is_file() and p.name.endswith(".manifest.json")])
        for mp in manifests:
            obj = _read_json_obj(mp)
            prod = obj.get("producer") if isinstance(obj, dict) else None
            sha = prod.get("git_sha") if isinstance(prod, dict) else None
            if isinstance(sha, str) and sha.strip():
                return sha.strip()
    return _git_sha_head()


def _submission_index_path(day_utc: str) -> Path:
    return (TRUTH / "execution_evidence_v1" / "submissions" / day_utc / "submission_index.v1.json").resolve()


@dataclass(frozen=True)
class CycleResult:
    phasd_fp: str
    phasd_fp_prev: str
    days_considered: List[str]
    days_ran_exec_evidence: List[str]
    days_ran_submission_index: List[str]
    days_skipped_submission_index_present: List[str]
    producer_sha_by_day: Dict[str, str]
    status: str
    reason: str


def _cycle_once(*, day_override: str) -> CycleResult:
    STATE_ROOT.mkdir(parents=True, exist_ok=True)

    fp_path = STATE_ROOT / "phaseD_submissions_root_fp.sha256"
    phasd_fp = _dir_fingerprint_sha256(PHASED_SUBMISSIONS_ROOT)
    phasd_prev = _read_text(fp_path)

    days_all = sorted(_days_touched_by_phased_root())
    days_to_run = days_all
    if day_override:
        if day_override not in days_all:
            return CycleResult(
                phasd_fp=phasd_fp,
                phasd_fp_prev=phasd_prev,
                days_considered=days_all,
                days_ran_exec_evidence=[],
                days_ran_submission_index=[],
                days_skipped_submission_index_present=[],
                producer_sha_by_day={},
                status="FAIL",
                reason=f"DAY_OVERRIDE_NOT_PRESENT_IN_PHASED_OUTPUTS: {day_override}",
            )
        days_to_run = [day_override]

    ran_exec: List[str] = []
    ran_idx: List[str] = []
    skipped_idx: List[str] = []
    sha_by_day: Dict[str, str] = {}

    if phasd_fp and phasd_fp != phasd_prev:
        for day_utc in days_to_run:
            producer_sha = _lock_producer_sha_for_day(day_utc)
            sha_by_day[day_utc] = producer_sha

            rc = _run(
                [
                    str(VENV_PY),
                    "-m",
                    "constellation_2.phaseF.execution_evidence.run.run_execution_evidence_truth_day_v1",
                    "--day_utc",
                    day_utc,
                    "--producer_git_sha",
                    producer_sha,
                    "--producer_repo",
                    "constellation_2_runtime",
                ]
            )
            if rc != 0:
                return CycleResult(
                    phasd_fp=phasd_fp,
                    phasd_fp_prev=phasd_prev,
                    days_considered=days_all,
                    days_ran_exec_evidence=ran_exec,
                    days_ran_submission_index=ran_idx,
                    days_skipped_submission_index_present=skipped_idx,
                    producer_sha_by_day=sha_by_day,
                    status="FAIL",
                    reason="EXEC_EVIDENCE_FAILED",
                )
            ran_exec.append(day_utc)

            idx_path = _submission_index_path(day_utc)
            if idx_path.exists():
                skipped_idx.append(day_utc)
            else:
                rc2 = _run(
                    [
                        str(VENV_PY),
                        "-m",
                        "constellation_2.phaseF.execution_evidence.run.run_submission_index_day_v1",
                        "--day",
                        day_utc,
                    ]
                )
                if rc2 != 0:
                    return CycleResult(
                        phasd_fp=phasd_fp,
                        phasd_fp_prev=phasd_prev,
                        days_considered=days_all,
                        days_ran_exec_evidence=ran_exec,
                        days_ran_submission_index=ran_idx,
                        days_skipped_submission_index_present=skipped_idx,
                        producer_sha_by_day=sha_by_day,
                        status="FAIL",
                        reason="SUBMISSION_INDEX_FAILED",
                    )
                ran_idx.append(day_utc)

        _write_text_atomic(fp_path, phasd_fp)

    return CycleResult(
        phasd_fp=phasd_fp,
        phasd_fp_prev=phasd_prev,
        days_considered=days_all,
        days_ran_exec_evidence=ran_exec,
        days_ran_submission_index=ran_idx,
        days_skipped_submission_index_present=skipped_idx,
        producer_sha_by_day=sha_by_day,
        status="OK",
        reason="OK",
    )


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="c2_supervisor_paper_v2")
    ap.add_argument("--poll_seconds", default=str(DEFAULT_POLL_SECONDS))
    ap.add_argument("--run_once", default="false", choices=["true", "false"])
    ap.add_argument("--day_utc", default="", help="Optional override day_utc to run when PhaseD changes.")
    args = ap.parse_args()

    poll = int(str(args.poll_seconds).strip())
    run_once = str(args.run_once).strip().lower() == "true"
    day_override = str(args.day_utc).strip()

    while True:
        res = _cycle_once(day_override=day_override)

        health_path = STATE_ROOT / f"supervisor_health_{_utc_day_now()}.v1.json"
        _write_json_atomic(
            health_path,
            {
                "schema_id": "C2_SUPERVISOR_HEALTH_V1",
                "schema_version": 1,
                "produced_utc": _utc_now(),
                "producer": {"git_sha": _git_sha_head(), "repo": "constellation_2_runtime", "module": "ops/run/c2_supervisor_paper_v2.py"},
                "status": res.status,
                "reason": res.reason,
                "phaseD_submissions_root": {
                    "dir": str(PHASED_SUBMISSIONS_ROOT),
                    "fingerprint_sha256": res.phasd_fp,
                    "prev_fingerprint_sha256": res.phasd_fp_prev,
                },
                "days_considered": res.days_considered,
                "days_ran_exec_evidence": res.days_ran_exec_evidence,
                "days_ran_submission_index": res.days_ran_submission_index,
                "days_skipped_submission_index_present": res.days_skipped_submission_index_present,
                "producer_sha_by_day": res.producer_sha_by_day,
            },
        )

        if res.status != "OK":
            return 1
        if run_once:
            return 0
        time.sleep(poll)


if __name__ == "__main__":
    raise SystemExit(main())
