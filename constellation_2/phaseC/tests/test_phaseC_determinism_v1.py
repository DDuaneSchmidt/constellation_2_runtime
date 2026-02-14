"""
test_phaseC_determinism_v1.py

Phase C determinism test (framework-free):

- Run the Phase C CLI tool twice with identical inputs and eval_time_utc.
- Assert outputs exist.
- Assert outputs are byte-identical across runs.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES_DIR = REPO_ROOT / "constellation_2" / "acceptance" / "samples"
TOOL = REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v1.py"


def _run_tool(out_dir: Path) -> subprocess.CompletedProcess[str]:
    eval_time_utc = "2026-02-13T21:52:00Z"
    tick_size = "0.01"

    cmd = [
        "python3",
        str(TOOL),
        "--intent",
        str(SAMPLES_DIR / "sample_options_intent.v2.json"),
        "--chain_snapshot",
        str(SAMPLES_DIR / "sample_chain_snapshot.v1.json"),
        "--freshness_cert",
        str(SAMPLES_DIR / "sample_freshness_certificate.v1.json"),
        "--eval_time_utc",
        eval_time_utc,
        "--tick_size",
        tick_size,
        "--out_dir",
        str(out_dir),
    ]
    return subprocess.run(cmd, text=True, capture_output=True)


def test_phasec_determinism_v1() -> None:
    out1 = REPO_ROOT / "constellation_2" / "phaseC" / "tests" / "_tmp_out_run1"
    out2 = REPO_ROOT / "constellation_2" / "phaseC" / "tests" / "_tmp_out_run2"

    # Ensure clean slate
    for d in (out1, out2):
        if d.exists():
            shutil.rmtree(d)

    r1 = _run_tool(out1)
    if r1.returncode != 0:
        raise AssertionError(f"run1 failed rc={r1.returncode}\nSTDOUT:\n{r1.stdout}\nSTDERR:\n{r1.stderr}")

    r2 = _run_tool(out2)
    if r2.returncode != 0:
        raise AssertionError(f"run2 failed rc={r2.returncode}\nSTDOUT:\n{r2.stdout}\nSTDERR:\n{r2.stderr}")

    expected = [
        "order_plan.v1.json",
        "mapping_ledger_record.v1.json",
        "binding_record.v1.json",
        "submit_preflight_decision.v1.json",
    ]
    for name in expected:
        if not (out1 / name).exists():
            raise AssertionError(f"missing expected output (run1): {out1/name}")
        if not (out2 / name).exists():
            raise AssertionError(f"missing expected output (run2): {out2/name}")

    # Byte-identical across runs
    for name in expected:
        b1 = (out1 / name).read_bytes()
        b2 = (out2 / name).read_bytes()
        if b1 != b2:
            raise AssertionError(f"outputs differ for {name}")

    shutil.rmtree(out1)
    shutil.rmtree(out2)


def _main() -> int:
    try:
        test_phasec_determinism_v1()
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: test_phasec_determinism_v1: {e}")
        return 1
    print("OK: test_phasec_determinism_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
