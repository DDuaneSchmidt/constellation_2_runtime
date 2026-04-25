from __future__ import annotations

import json
import re
import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.kill_switch_authority_v1 import (  # noqa: E402
    RC_KILL_SWITCH_AUTHORITY_MISMATCH,
    RC_KILL_SWITCH_CANONICAL_MISSING,
    STATUS_FAIL_CLOSED,
    STATUS_PASS,
    resolve_kill_switch_authority_v1,
)


DAY = "2026-04-14"

DECISION_CRITICAL_CONSUMERS = [
    SOURCE_ROOT / "constellation_2/phaseD/lib/submit_boundary_paper_v1.py",
    SOURCE_ROOT / "constellation_2/phaseD/lib/submit_boundary_paper_v4.py",
    SOURCE_ROOT / "ops/tools/run_submit_boundary_status_v1.py",
    SOURCE_ROOT / "ops/tools/run_systemic_risk_gate_v1.py",
    SOURCE_ROOT / "ops/tools/run_systemic_risk_gate_v2.py",
    SOURCE_ROOT / "ops/tools/run_systemic_risk_gate_v3.py",
    SOURCE_ROOT / "ops/tools/run_operator_gate_verdict_v1.py",
    SOURCE_ROOT / "ops/tools/run_exposure_reconciliation_v1.py",
    SOURCE_ROOT / "ops/tools/run_c2_paper_day_orchestrator_v2.py",
]

FORBIDDEN_SLEEVE_READ_PATTERNS = [
    re.compile(r'truth_sleeves.+kill_switch_v1'),
]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _kill_payload(*, state: str, allow_entries: bool) -> dict:
    return {
        "schema_id": "global_kill_switch_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "produced_utc": f"{DAY}T00:00:00Z",
        "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
        "state": state,
        "allow_entries": allow_entries,
        "allow_exits": True,
        "forced_mode": "NORMAL" if state == "INACTIVE" else "FLATTEN_ONLY",
        "reason_codes": [] if (state == "INACTIVE" and allow_entries) else ["C2_KILL_SWITCH_ACTIVE"],
        "input_manifest": [],
        "state_sha256": "0" * 64,
    }


def _write_canonical(truth_root: Path, *, state: str, allow_entries: bool) -> None:
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        _kill_payload(state=state, allow_entries=allow_entries),
    )


def _write_sleeve(truth_root: Path, *, state: str, allow_entries: bool) -> None:
    _write_json(
        truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        _kill_payload(state=state, allow_entries=allow_entries),
    )


def test_resolver_passes_when_canonical_and_sleeve_both_off(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_canonical(truth_root, state="INACTIVE", allow_entries=True)
    _write_sleeve(truth_root, state="INACTIVE", allow_entries=True)

    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=DAY)

    assert result.status == STATUS_PASS
    assert result.state == "INACTIVE"
    assert result.allow_entries is True


def test_resolver_passes_when_canonical_and_sleeve_both_on(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_canonical(truth_root, state="ACTIVE", allow_entries=False)
    _write_sleeve(truth_root, state="ACTIVE", allow_entries=False)

    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=DAY)

    assert result.status == STATUS_PASS
    assert result.state == "ACTIVE"
    assert result.allow_entries is False


def test_resolver_fails_closed_when_canonical_off_and_sleeve_on(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_canonical(truth_root, state="INACTIVE", allow_entries=True)
    _write_sleeve(truth_root, state="ACTIVE", allow_entries=False)

    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=DAY)

    assert result.status == STATUS_FAIL_CLOSED
    assert result.reason_code == RC_KILL_SWITCH_AUTHORITY_MISMATCH


def test_resolver_fails_closed_when_canonical_on_and_sleeve_off(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_canonical(truth_root, state="ACTIVE", allow_entries=False)
    _write_sleeve(truth_root, state="INACTIVE", allow_entries=True)

    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=DAY)

    assert result.status == STATUS_FAIL_CLOSED
    assert result.reason_code == RC_KILL_SWITCH_AUTHORITY_MISMATCH


def test_resolver_fails_closed_when_canonical_missing(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"

    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=DAY)

    assert result.status == STATUS_FAIL_CLOSED
    assert result.reason_code == RC_KILL_SWITCH_CANONICAL_MISSING


def test_resolver_passes_when_sleeve_missing(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_canonical(truth_root, state="INACTIVE", allow_entries=True)

    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=DAY)

    assert result.status == STATUS_PASS
    assert result.reason_code is None
    assert result.sleeve_present is False


def test_decision_critical_consumers_reference_resolver_without_direct_sleeve_reads() -> None:
    for path in DECISION_CRITICAL_CONSUMERS:
        content = path.read_text(encoding="utf-8")
        assert "resolve_kill_switch_authority_v1" in content, str(path)
        for pattern in FORBIDDEN_SLEEVE_READ_PATTERNS:
            assert pattern.search(content) is None, f"{path}: matched {pattern.pattern}"
