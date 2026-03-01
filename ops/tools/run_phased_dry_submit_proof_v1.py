#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# ---- Roots used by this proof tool (truth-only) ----
PHASEC_PREFLIGHT_ROOT = (TRUTH / "phaseC_preflight_v1").resolve()
INTENTS_ROOT = (TRUTH / "intents_v1" / "snapshots").resolve()

# Identity-set filenames (broker-submission dry proof)
IDENTITY_NAMES = ("equity_order_plan.v2.json", "equity_order_plan.v1.json", "order_plan.v1.json")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _is_day(s: str) -> bool:
    if not isinstance(s, str):
        return False
    if len(s) != 10:
        return False
    if s[4] != "-" or s[7] != "-":
        return False
    y, m, d = s[0:4], s[5:7], s[8:10]
    return y.isdigit() and m.isdigit() and d.isdigit()


def _safe_read_json(path: Path) -> Tuple[Optional[object], Optional[str]]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _find_identity_set_dir(day: str) -> Path:
    """
    Identity-set dry proof requires one of:
      - equity_order_plan.v2.json
      - equity_order_plan.v1.json
      - order_plan.v1.json

    We search under truth/phaseC_preflight_v1/<day>/ for those identity-set files.
    """
    root = (PHASEC_PREFLIGHT_ROOT / day).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"FAIL: phaseC_preflight_v1 day dir missing: {root}")

    candidates: List[Path] = []
    for name in IDENTITY_NAMES:
        for p in root.rglob(name):
            if p.is_file():
                candidates.append(p.parent.resolve())

    if not candidates:
        raise SystemExit(f"FAIL: no identity set files found under: {root}")

    # Deterministic selection: choose lexicographically smallest repo-relative path
    def key(p: Path) -> str:
        rel = str(p.relative_to(REPO_ROOT)).replace("\\", "/")
        return rel

    return sorted(set(candidates), key=key)[0]


def _find_exposure_intent_file(day: str) -> Path:
    """
    Exposure-intent preflight proof:
      - Find the exposure_intent.v1 file under truth/intents_v1/snapshots/<day>/
      - Deterministic selection: lexicographically smallest filename
    """
    day_dir = (INTENTS_ROOT / day).resolve()
    if not day_dir.exists() or not day_dir.is_dir():
        raise SystemExit(f"FAIL: intents day dir missing: {day_dir}")

    files = sorted([p for p in day_dir.iterdir() if p.is_file() and p.name.endswith(".exposure_intent.v1.json")], key=lambda p: p.name)
    if not files:
        raise SystemExit(f"FAIL: no exposure_intent.v1 files found under: {day_dir}")

    return files[0].resolve()


def _intent_hash_for_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _require_submit_preflight_allow(day: str, intent_hash: str) -> Path:
    """
    Require a governed PhaseC submit preflight decision for this intent_hash:
      truth/phaseC_preflight_v1/<day>/<intent_hash>.submit_preflight_decision.v1.json

    Fail-closed if missing/unreadable or not ALLOW.
    """
    p = (PHASEC_PREFLIGHT_ROOT / day / f"{intent_hash}.submit_preflight_decision.v1.json").resolve()
    obj, err = _safe_read_json(p)
    if obj is None:
        raise SystemExit(f"FAIL: missing preflight decision for intent_hash={intent_hash}: path={p} err={err}")

    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: preflight decision not a JSON object: path={p}")

    schema_id = str(obj.get("schema_id") or "").strip()
    schema_version = str(obj.get("schema_version") or "").strip()

    # Fail-closed: require expected identity
    if schema_id != "submit_preflight_decision" or schema_version != "v1":
        raise SystemExit(
            f"FAIL: preflight decision schema mismatch: path={p} schema_id={schema_id!r} schema_version={schema_version!r}"
        )

    decision = str(obj.get("decision") or "").strip().upper()

    # Fail-closed: must be ALLOW and OK/AUTHORIZED-ish status
    if decision != "ALLOW":
        raise SystemExit(f"FAIL: preflight decision not ALLOW: path={p} decision={decision!r}")
    return p


def _run_identity_set_dry_submit(day: str, phasec_out_dir: Path, ib_host: str, ib_port: int, ib_client_id: int, ib_account: str) -> int:
    """
    Existing behavior: run the broker submission boundary in dry-run mode.
    """
    eval_time_utc = f"{day}T00:00:00Z"
    cmd = [
        "python3",
        "constellation_2/phaseD/tools/c2_submit_paper_v5.py",
        "--eval_time_utc",
        eval_time_utc,
        "--phasec_out_dir",
        str(phasec_out_dir),
        "--ib_host",
        str(ib_host).strip(),
        "--ib_port",
        str(int(ib_port)),
        "--ib_client_id",
        str(int(ib_client_id)),
        "--ib_account",
        str(ib_account).strip(),
        "--submissions_root_override",
        "/tmp/c2_dry_submit_proof_submissions_v1",
        "--dry_run",
        "YES",
    ]
    p = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=sys.stdout, stderr=sys.stderr, text=True)
    return int(p.returncode)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_phased_dry_submit_proof_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ib_host", required=True)
    ap.add_argument("--ib_port", required=True, type=int)
    ap.add_argument("--ib_client_id", required=True, type=int)
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not _is_day(day):
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    # Mode 1: Identity-set dry submit proof (preferred if identity set exists)
    # If identity set is missing, fall back to Mode 2 (exposure-intent preflight proof).
    try:
        phasec_out_dir = _find_identity_set_dir(day)
        rc = _run_identity_set_dry_submit(
            day=day,
            phasec_out_dir=phasec_out_dir,
            ib_host=args.ib_host,
            ib_port=int(args.ib_port),
            ib_client_id=int(args.ib_client_id),
            ib_account=str(args.ib_account).strip(),
        )
        out = {
            "ok": rc == 0,
            "rc": int(rc),
            "day_utc": day,
            "proof_kind": "IDENTITY_SET_DRY_SUBMIT",
            "phasec_out_dir": str(phasec_out_dir),
        }
        print(json.dumps(out, sort_keys=True))
        return int(rc)
    except SystemExit as e:
        # Only fall back if the failure is specifically "no identity set files found"
        msg = str(e)
        if "no identity set files found under:" not in msg:
            raise

    # Mode 2: Exposure-intent preflight proof (no broker submission)
    intent_path = _find_exposure_intent_file(day)
    intent_hash = _intent_hash_for_file(intent_path)
    decision_path = _require_submit_preflight_allow(day, intent_hash)

    out2 = {
        "ok": True,
        "rc": 0,
        "day_utc": day,
        "proof_kind": "EXPOSURE_INTENT_PREFLIGHT_ONLY",
        "intent_path": str(intent_path),
        "intent_hash": intent_hash,
        "preflight_decision_path": str(decision_path),
    }
    print(json.dumps(out2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
