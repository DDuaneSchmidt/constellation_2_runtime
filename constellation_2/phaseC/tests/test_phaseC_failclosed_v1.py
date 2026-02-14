"""
test_phaseC_failclosed_v1.py

Phase C fail-closed tests (framework-free):

1) Freshness expired => veto only
2) Snapshot hash binding mismatch => veto only

Requirements:
- On veto: output directory contains ONLY veto_record.v1.json
- Tool return code is non-zero
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES_DIR = REPO_ROOT / "constellation_2" / "acceptance" / "samples"
TOOL = REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v1.py"


def _run_tool(out_dir: Path, *, eval_time_utc: str, freshness_cert_path: Path) -> subprocess.CompletedProcess[str]:
    tick_size = "0.01"
    cmd = [
        "python3",
        str(TOOL),
        "--intent",
        str(SAMPLES_DIR / "sample_options_intent.v2.json"),
        "--chain_snapshot",
        str(SAMPLES_DIR / "sample_chain_snapshot.v1.json"),
        "--freshness_cert",
        str(freshness_cert_path),
        "--eval_time_utc",
        eval_time_utc,
        "--tick_size",
        tick_size,
        "--out_dir",
        str(out_dir),
    ]
    return subprocess.run(cmd, text=True, capture_output=True)


def _assert_veto_only(out_dir: Path) -> None:
    veto = out_dir / "veto_record.v1.json"
    if not veto.exists():
        raise AssertionError("expected veto_record.v1.json not found")

    forbidden = [
        out_dir / "order_plan.v1.json",
        out_dir / "mapping_ledger_record.v1.json",
        out_dir / "binding_record.v1.json",
        out_dir / "submit_preflight_decision.v1.json",
    ]
    for f in forbidden:
        if f.exists():
            raise AssertionError(f"forbidden output exists on veto path: {f}")

    files = sorted([p.name for p in out_dir.iterdir() if p.is_file()])
    if files != ["veto_record.v1.json"]:
        raise AssertionError(f"expected only veto_record.v1.json, found: {files}")


def test_phasec_failclosed_freshness_expired_v1() -> None:
    out = REPO_ROOT / "constellation_2" / "phaseC" / "tests" / "_tmp_out_fail_freshness"
    if out.exists():
        shutil.rmtree(out)

    r = _run_tool(
        out,
        eval_time_utc="2026-02-13T21:56:00Z",  # valid_until_utc is 21:55:00Z
        freshness_cert_path=(SAMPLES_DIR / "sample_freshness_certificate.v1.json"),
    )

    if r.returncode == 0:
        raise AssertionError(f"expected non-zero rc on freshness expired\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")

    _assert_veto_only(out)
    shutil.rmtree(out)


def test_phasec_failclosed_snapshot_hash_mismatch_v1() -> None:
    out = REPO_ROOT / "constellation_2" / "phaseC" / "tests" / "_tmp_out_fail_hash"
    if out.exists():
        shutil.rmtree(out)

    tmp_dir = REPO_ROOT / "constellation_2" / "phaseC" / "tests" / "_tmp_inputs"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    src = SAMPLES_DIR / "sample_freshness_certificate.v1.json"
    obj = json.loads(src.read_text(encoding="utf-8"))
    obj["snapshot_hash"] = "0" * 64  # mismatch

    tampered = tmp_dir / "tampered_freshness_certificate.v1.json"
    tampered.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    r = _run_tool(
        out,
        eval_time_utc="2026-02-13T21:52:00Z",
        freshness_cert_path=tampered,
    )

    if r.returncode == 0:
        raise AssertionError(f"expected non-zero rc on hash mismatch\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")

    _assert_veto_only(out)

    shutil.rmtree(out)
    shutil.rmtree(tmp_dir)


def _main() -> int:
    try:
        test_phasec_failclosed_freshness_expired_v1()
        print("OK: test_phasec_failclosed_freshness_expired_v1")
        test_phasec_failclosed_snapshot_hash_mismatch_v1()
        print("OK: test_phasec_failclosed_snapshot_hash_mismatch_v1")
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: test_phasec_failclosed_v1: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
