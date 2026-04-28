#!/usr/bin/env python3
"""
run_c2_paper_day_orchestrator_v2.py

Constellation 2.0 — Orchestrator V2

Publishes:
- Orchestrator attempt manifest v2
- Orchestrator run verdict v2
- Pipeline manifest v3
- Pipeline manifest v2 compat
- Pipeline manifest v1 compat

Exit policy:
- 0 for PASS / DEGRADED / FAIL
- non-zero only for ABORTED

Account-binding enforcement:
- --ib_account must match the governed sleeve registry account for --truth_root partition
- fail-closed if truth_root partition cannot be resolved to a governed sleeve binding
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.canonical_fact_store_v1 import resolve_latest_active_attempt
from constellation_2.common.governed_evaluation_control_v1 import (
    resolve_adopted_governed_sleeve_bindings_v1,
)
from constellation_2.common.kill_switch_authority_v1 import (
    STATUS_PASS as KILL_SWITCH_STATUS_PASS,
    resolve_kill_switch_authority_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_profile,
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import (
    write_execution_submission_record_from_execution_package_v1,
)
from constellation_2.common.paper_session_ledger_v1 import assert_paper_session_ledger_granted_v1
from constellation_2.common.runtime_contract_v1 import (
    resolve_pointer_index_root_for_truth_root,
)
from constellation_2.common.runtime_authority_bridge_v1 import (
    resolve_canonical_truth_root_bridge_v1,
    resolve_truth_root_bridge_v1,
    resolve_truth_sleeves_root_bridge_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.evidence_writer_v1 import EvidenceWriteError, write_phased_veto_only_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry

SLEEVE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
GATE_HIERARCHY_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
ENGINE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()

POINTER_INDEX_NAME = "canonical_pointer_index.v1.jsonl"
POINTER_LOCK_NAME = ".canonical_pointer_index.v1.lock"
REFRESHABLE_GATE_STAGE_IDS = {
    "A3_LIQUIDITY_SLIPPAGE_GATE_V1",
    "A5A_OPERATOR_DAILY_GATE_V3",
    "A5AAA_FEED_ATTESTATION_GATE_V1",
    "A5B_HEARTBEAT_GATE_V1",
    "A5C_CORRELATION_ENVELOPE_GATE_V1",
}
DEFAULT_GOVERNED_SUBMIT_DRY_RUN = "YES"
DEFAULT_STRUCTURAL_ACTIVITY_MODE = "OFF"
ADMITTED_EXECUTION_STAGE_IDS = {
    "A0_ENFORCE_SINGLE_ACCOUNT_TOPOLOGY",
    "B0_CASH_LEDGER_SNAPSHOT_V1",
    "B0_POSITIONS_SNAPSHOT_V5",
    "B0B_POSITIONS_SNAPSHOT_V2_COMPAT",
    "B0_ENSURE_EXECUTION_SUBMISSIONS_DIR_V1",
    "B0_ACCOUNTING_NAV_V2",
    "B0_ACCOUNTING_NAV_COMPAT_BRIDGE_V1",
    "A3_LIQUIDITY_SLIPPAGE_GATE_V1",
    "A4A_ALLOCATION_DAY_V2",
    "A5_CAPITAL_RISK_ENVELOPE_GATE_V2",
    "A5AA_ENGINE_CORRELATION_MATRIX_DAY_V1",
    "A5AAA_FEED_ATTESTATION_GATE_V1",
    "A5AB_RECONCILIATION_REPORT_V3",
    "A5AC_EXIT_RECONCILIATION_DAY_V1",
    "A5A_OPERATOR_DAILY_GATE_V3",
    "A5B_HEARTBEAT_GATE_V1",
    "A5C_CORRELATION_ENVELOPE_GATE_V1",
    "A5D_REPLAY_CERTIFICATION_GATE_V1",
    "A6_GATE_STACK_VERDICT_V1",
    "A7_GLOBAL_KILL_SWITCH_V1",
    "A6A_RUN_POINTER_APPEND_V1",
    "A6A_POINTER_HEADS_MATERIALIZE_V1",
    "A6A_EXPOSURE_NET_DAY_V1",
    "A6A_SLEEVE_EDGE_MEASUREMENT_V1",
    "A6A1_GOVERNED_EVALUATION_DAY_V1",
    "A6AA_CAPITAL_AUTHORITY_ALLOCATION_DAY_V1",
    "A6B_AUTHORIZATION_ARTIFACTS_DAY_V1",
    "A6C_OPTIONS_CHAIN_CAPTURE_IB_DAY_V1",
    "A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_DAY_V1",
    "A6E_MARKET_DATA_SNAPSHOT_REFRESH_V1",
    "A7_PHASEC_IDENTITY_MATERIALIZER_DAY_V1",
    "A7A_GOVERNED_SUBMIT_V5",
    "B0A_EXECUTION_EVIDENCE_TRUTH_V1",
    "B0C_POSITIONS_EFFECTIVE_POINTER_V1",
    "B0_EXECUTION_STREAM_SNAPSHOT_V1",
    "B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1",
    "B1_FILL_LEDGER_V1",
    "B2_EXECUTION_RECONCILIATION_V1",
}
SLEEVE_EDGE_PUBLICATION_STAGE_ID = "A6A_SLEEVE_EDGE_MEASUREMENT_V1"
GOVERNED_EVALUATION_STAGE_ID = "A6A1_GOVERNED_EVALUATION_DAY_V1"
CAPITAL_AUTHORITY_STAGE_ID = "A6AA_CAPITAL_AUTHORITY_ALLOCATION_DAY_V1"


def _governed_evaluation_stage_cmd(*, day: str, truth: Path) -> list[str]:
    cmd = [
        "python3",
        "ops/tools/run_governed_evaluation_day_v1.py",
        "--day_utc",
        day,
        "--truth_root",
        str(truth),
        "--execution_sleeve_id",
        "PRIMARY",
        "--mode",
        "PAPER",
    ]
    for row in resolve_adopted_governed_sleeve_bindings_v1(execution_sleeve_id="PRIMARY", mode="PAPER"):
        cmd.extend(["--sleeve_id", row["sleeve_id"]])
    return cmd


def _json_dumps(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _sha256_file(p: Path) -> str:
    return _sha256_bytes(p.read_bytes())


def _git_sha() -> str:
    try:
        return (
            subprocess.check_output(
                ["/usr/bin/git", "rev-parse", "--short=7", "HEAD"],
                cwd=str(REPO_ROOT),
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
            or ("0" * 7)
        )
    except Exception:
        return "0" * 7


def _require_repo_root_cwd() -> None:
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT:
        raise SystemExit(f"FATAL: must run with cwd={REPO_ROOT} got={cwd}")


def _require_truth_root(p: str) -> Path:
    s = (p or "").strip()
    if not s:
        raise SystemExit("FAIL: --truth_root resolved to empty string")
    pr = Path(s).expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: --truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {pr}")
    return pr

def _resolve_truth_root_or_default(p: Optional[str]) -> Path:
    s = ("" if p is None else str(p)).strip()
    if s:
        return _require_truth_root(s)

    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root(env_root)

    return resolve_truth_root_bridge_v1(
        repo_root=REPO_ROOT,
        caller="ops/tools/run_c2_paper_day_orchestrator_v2.py",
    ).resolve()


def _load_sleeve_registry() -> Dict[str, Any]:
    if not SLEEVE_REGISTRY_PATH.exists() or not SLEEVE_REGISTRY_PATH.is_file():
        raise SystemExit(f"FAIL: sleeve registry missing: {SLEEVE_REGISTRY_PATH}")
    try:
        obj = json.loads(SLEEVE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: sleeve registry parse error path={SLEEVE_REGISTRY_PATH} err={type(e).__name__}:{e}")
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: sleeve registry must be object path={SLEEVE_REGISTRY_PATH}")
    if str(obj.get("schema_id") or "") != "c2_sleeve_registry" or str(obj.get("schema_version") or "") != "v1":
        raise SystemExit(
            f"FAIL: sleeve registry schema mismatch path={SLEEVE_REGISTRY_PATH} "
            f"schema_id={obj.get('schema_id')!r} schema_version={obj.get('schema_version')!r}"
        )
    return obj


def _resolve_expected_sleeve_account(*, truth_root: Path, mode: str) -> Tuple[str, str]:
    truth_sleeves_root = resolve_truth_sleeves_root_bridge_v1(
        caller="ops/tools/run_c2_paper_day_orchestrator_v2.py"
    )
    try:
        rel_parts = truth_root.relative_to(truth_sleeves_root).parts
    except ValueError:
        raise SystemExit(
            f"FAIL: truth_root_outside_runtime_root truth_root={truth_root} runtime_root={truth_sleeves_root}"
        )
    if len(rel_parts) != 2:
        raise SystemExit(
            f"FAIL: truth_root_not_sleeve_partition truth_root={truth_root} expected=.../truth_sleeves/<SLEEVE>/<MODE>"
        )
    sleeve_id = str(rel_parts[0]).strip()
    partition_mode = str(rel_parts[1]).strip().upper()
    if partition_mode != mode:
        raise SystemExit(
            f"FAIL: truth_root_mode_mismatch truth_root_mode={partition_mode} requested_mode={mode} truth_root={truth_root}"
        )

    reg = _load_sleeve_registry()
    sleeves = reg.get("sleeves")
    if not isinstance(sleeves, list):
        raise SystemExit(f"FAIL: sleeve registry invalid sleeves list path={SLEEVE_REGISTRY_PATH}")

    found: Optional[Dict[str, Any]] = None
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("sleeve_id") or "").strip()
        smode = str(s.get("mode") or "").strip().upper()
        enabled = bool(s.get("enabled"))
        if sid == sleeve_id and smode == mode and enabled:
            found = s
            break
    if found is None:
        raise SystemExit(
            f"FAIL: no_enabled_sleeve_binding_for_truth_root sleeve_id={sleeve_id} mode={mode} registry={SLEEVE_REGISTRY_PATH}"
        )

    exp_partition = f"truth_sleeves/{sleeve_id}/{mode}"
    got_partition = str(found.get("truth_partition") or "").strip()
    if got_partition != exp_partition:
        raise SystemExit(
            f"FAIL: sleeve_truth_partition_mismatch sleeve_id={sleeve_id} expected={exp_partition} got={got_partition}"
        )

    exp_acct = str(found.get("ib_account") or "").strip()
    if not exp_acct:
        raise SystemExit(f"FAIL: sleeve_registry_missing_ib_account sleeve_id={sleeve_id} mode={mode}")
    return sleeve_id, exp_acct

def _require_day(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad day key (expected YYYY-MM-DD): {d!r}")
    return d


def _require_mode(mode: str) -> str:
    m = (mode or "").strip().upper()
    if m not in ("PAPER", "LIVE"):
        raise SystemExit(f"FAIL: bad --mode (expected PAPER|LIVE): {m!r}")
    return m


def _require_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        raise SystemExit("FAIL: --symbol resolved to empty string")
    return s


def _validate_produced_utc_isoz(s: str) -> str:
    v = (s or "").strip()
    if len(v) < 11 or not v.endswith("Z") or "T" not in v:
        raise SystemExit(f"FAIL: --produced_utc must look like ISO-8601 UTC Z timestamp: {v!r}")
    return v


def _intent_simulator_produced_utc_for_day(day: str) -> str:
    d = date.fromisoformat(day)
    local = datetime(d.year, d.month, d.day, 10, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_structural_activity_mode(env: Dict[str, str]) -> str:
    raw = str(env.get("C2_STRUCTURAL_ACTIVITY_MODE") or DEFAULT_STRUCTURAL_ACTIVITY_MODE).strip().upper()
    if raw not in {"OFF", "SIMULATOR"}:
        return DEFAULT_STRUCTURAL_ACTIVITY_MODE
    return raw


def _load_engine_registry() -> Dict[str, Any]:
    if not ENGINE_REGISTRY_PATH.exists() or not ENGINE_REGISTRY_PATH.is_file():
        raise SystemExit(f"FAIL: engine registry missing: {ENGINE_REGISTRY_PATH}")
    try:
        obj = json.loads(ENGINE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(
            f"FAIL: engine registry parse error path={ENGINE_REGISTRY_PATH} err={type(e).__name__}:{e}"
        )
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: engine registry must be object path={ENGINE_REGISTRY_PATH}")
    return obj


def _active_engines_sorted(reg: Dict[str, Any]) -> List[Dict[str, str]]:
    engines = reg.get("engines") or []
    if not isinstance(engines, list):
        raise SystemExit("FAIL: engine registry engines not list")

    active: List[Dict[str, str]] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        if str(e.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue

        engine_id = str(e.get("engine_id") or "").strip()
        runner_mod = str(e.get("runner_path") or "").strip()
        runner_file = str(e.get("engine_runner_path") or "").strip()
        runner_sha = str(e.get("engine_runner_sha256") or "").strip().lower()

        if not engine_id:
            raise SystemExit("FAIL: ACTIVE engine missing engine_id")
        if not runner_mod:
            raise SystemExit(f"FAIL: ACTIVE engine missing runner_path: engine_id={engine_id}")
        if not runner_file:
            raise SystemExit(f"FAIL: ACTIVE engine missing engine_runner_path: engine_id={engine_id}")
        if len(runner_sha) != 64 or any(c not in "0123456789abcdef" for c in runner_sha):
            raise SystemExit(
                f"FAIL: ACTIVE engine missing/invalid engine_runner_sha256: engine_id={engine_id} sha={runner_sha!r}"
            )

        active.append(
            {
                "engine_id": engine_id,
                "runner_path": runner_mod,
                "engine_runner_path": runner_file,
                "engine_runner_sha256": runner_sha,
            }
        )

    return sorted(active, key=lambda row: row["engine_id"])


def _build_allowed_symbols_map_fail_closed(reg: Dict[str, Any]) -> Dict[str, Optional[List[str]]]:
    engines = reg.get("engines") or []
    if not isinstance(engines, list):
        raise SystemExit("FAIL: engine registry engines not list")

    out: Dict[str, Optional[List[str]]] = {}
    for e in engines:
        if not isinstance(e, dict):
            continue
        engine_id = str(e.get("engine_id") or "").strip()
        if not engine_id:
            raise SystemExit("FAIL: engine registry entry missing engine_id")
        if "allowed_symbols" not in e:
            raise SystemExit(f"FAIL: engine registry entry missing allowed_symbols: engine_id={engine_id}")
        allowed = e.get("allowed_symbols")
        if allowed is None:
            out[engine_id] = None
            continue
        if not isinstance(allowed, list):
            raise SystemExit(f"FAIL: allowed_symbols not list|null: engine_id={engine_id}")
        vals: List[str] = []
        for raw in allowed:
            sym = str(raw or "").strip().upper()
            if not sym:
                raise SystemExit(f"FAIL: allowed_symbols contains empty symbol: engine_id={engine_id}")
            vals.append(sym)
        out[engine_id] = sorted(set(vals))
    return out


def _emit_missing_engine_heartbeat(
    *,
    truth_root: Path,
    day: str,
    engine: Dict[str, str],
    status: str,
    reason_code: str,
    produced_utc: str,
    registry_sha: str,
    git_sha: str,
    env: Dict[str, str],
) -> None:
    hb_path = (
        truth_root
        / "monitoring_v1"
        / "engine_heartbeat_v1"
        / day
        / str(engine["engine_id"])
        / "engine_heartbeat.v1.json"
    ).resolve()
    if hb_path.exists() and hb_path.is_file():
        return

    rc = _run_cmd(
        f"A0_STRUCTURAL_HEARTBEAT_{engine['engine_id']}",
        [
            "python3",
            "ops/tools/run_engine_heartbeat_emit_v1.py",
            "--day_utc",
            day,
            "--engine_id",
            str(engine["engine_id"]),
            "--status",
            status,
            "--reason_code",
            reason_code,
            "--last_run_utc",
            produced_utc,
            "--expected_period_seconds",
            "86400",
            "--stale_after_seconds",
            "172800",
            "--fingerprint",
            f"engine_registry|governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json|{registry_sha}|true",
            "--fingerprint",
            f"engine_runner|{engine['engine_runner_path']}|{engine['engine_runner_sha256']}|true",
            "--truth_root",
            str(truth_root),
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_module",
            "ops/tools/run_engine_heartbeat_emit_v1.py",
            "--producer_git_sha",
            git_sha,
        ],
        env=env,
    )
    if rc != 0:
        raise SystemExit(f"FAIL: structural heartbeat emit failed engine_id={engine['engine_id']} rc={rc}")


def _run_structural_pre_activity_producers(
    *,
    truth_root: Path,
    day: str,
    mode: str,
    symbol: str,
    produced_utc: str,
    git_sha: str,
    env: Dict[str, str],
) -> None:
    reg = _load_engine_registry()
    active_engines = _active_engines_sorted(reg)
    allowed_map = _build_allowed_symbols_map_fail_closed(reg)
    registry_sha = _sha256_file(ENGINE_REGISTRY_PATH)
    structural_activity_mode = _resolve_structural_activity_mode(env)

    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    producer_specs: List[Tuple[str, str, List[str]]] = [
        (
            "C2_VOL_INCOME_DEFINED_RISK_V1",
            "A0A_VOL_INCOME_DEFINED_RISK_INTENTS_DAY_V1",
            [
                "python3",
                "constellation_2/phaseI/vol_income_defined_risk/run/run_vol_income_defined_risk_intents_day_v1.py",
                "--day_utc",
                day,
                "--mode",
                mode,
                "--truth_root",
                str(truth_root),
                "--target_notional_pct",
                "0.01",
            ],
        ),
        (
            "C2_TREND_EQ_PRIMARY_V1",
            "A0B_TREND_EQ_PRIMARY_INTENTS_DAY_V1",
            [
                "python3",
                "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
                "--day_utc",
                day,
                "--mode",
                mode,
                "--truth_root",
                str(truth_root),
            ],
        ),
        (
            "C2_MEAN_REVERSION_EQ_V1",
            "A0C_MEAN_REVERSION_INTENTS_DAY_V1",
            [
                "python3",
                "constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py",
                "--day_utc",
                day,
                "--mode",
                mode,
                "--truth_root",
                str(truth_root),
            ],
        ),
    ]
    if structural_activity_mode == "SIMULATOR":
        producer_specs.append(
            (
                "C2_INTENT_SIMULATOR_V1",
                "A0S_INTENT_SIMULATOR_DAY_V1",
                [
                    "python3",
                    "constellation_2/phaseH/intent_simulator/run/run_intent_simulator_day_v1.py",
                    "--produced_utc",
                    _intent_simulator_produced_utc_for_day(day),
                    "--engine_registry_sha256",
                    registry_sha,
                ],
            )
        )
    producer_status: Dict[str, Tuple[str, str]] = {}

    if not (intents_day.exists() and intents_day.is_dir()):
        ranked_rc = _run_cmd(
            "A0R_RANKED_SYMBOL_UNIVERSE_DAY_V1",
            [
                "python3",
                "ops/tools/run_ranked_symbol_universe_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth_root),
                "--produced_utc",
                produced_utc,
            ],
            env=env,
        )
        if ranked_rc != 0:
            raise SystemExit(f"FAIL: ranked symbol universe producer failed stage=A0R_RANKED_SYMBOL_UNIVERSE_DAY_V1 rc={ranked_rc}")

        structural_symbols: List[str] = []
        for engine_id, _, _ in producer_specs:
            if engine_id == "C2_INTENT_SIMULATOR_V1":
                continue
            allowed = allowed_map.get(engine_id)
            if allowed is None:
                structural_symbols.append(symbol)
                continue
            if isinstance(allowed, list):
                allowed_symbols = {str(sym).strip().upper() for sym in allowed if str(sym).strip()}
                if symbol in allowed_symbols:
                    structural_symbols.append(symbol)
        md_executed, md_rc, md_reason_codes, _ = _run_market_data_refresh_for_symbols(
            truth_root=truth_root,
            day=day,
            produced_utc=produced_utc,
            env=env,
            symbols=sorted(set(structural_symbols)),
            empty_reason="SKIP_NO_STRUCTURAL_ENGINE_SYMBOLS",
        )
        md_refresh_failed = (md_rc != 0)
        for engine_id, stage_id, cmd in producer_specs:
            engine = next((row for row in active_engines if row["engine_id"] == engine_id), None)
            if engine is None:
                producer_status[engine_id] = ("WARN", "ENGINE_NOT_ACTIVE_IN_REGISTRY")
                continue
            allowed = allowed_map.get(engine_id)
            symbols = [symbol]
            if engine_id == "C2_INTENT_SIMULATOR_V1":
                symbols = []
            if isinstance(allowed, list) and allowed:
                allowed_symbols = {str(sym).strip().upper() for sym in allowed if str(sym).strip()}
                if engine_id == "C2_INTENT_SIMULATOR_V1":
                    if symbol not in allowed_symbols:
                        producer_status[engine_id] = ("WARN", "ENGINE_SKIPPED_SYMBOL_UNIVERSE")
                        continue
                    symbols = []
                else:
                    symbols = [symbol] if symbol in allowed_symbols else []
            if not symbols:
                if engine_id != "C2_INTENT_SIMULATOR_V1":
                    producer_status[engine_id] = ("WARN", "ENGINE_SKIPPED_SYMBOL_UNIVERSE")
                    continue
            engine_executed = False
            if engine_id == "C2_INTENT_SIMULATOR_V1":
                rc = _run_cmd(stage_id, cmd, env=env)
                if rc != 0:
                    raise SystemExit(f"FAIL: structural intent producer failed stage={stage_id} rc={rc}")
                engine_executed = True
            else:
                for producer_symbol in symbols:
                    rc = _run_cmd(stage_id + "_" + producer_symbol, cmd + ["--symbol", producer_symbol], env=env)
                    if rc != 0:
                        raise SystemExit(
                            f"FAIL: structural intent producer failed stage={stage_id}_{producer_symbol} rc={rc}"
                        )
                    engine_executed = True
            if engine_executed:
                hb_reason = "ENGINE_DIRECT_RUN_PRESENT_V2_STRUCTURAL_PREPASS"
                if md_executed:
                    hb_reason = "ENGINE_DIRECT_RUN_PRESENT_V2_STRUCTURAL_PREPASS_WITH_MD_REFRESH"
                elif md_refresh_failed:
                    hb_reason = "ENGINE_DIRECT_RUN_PRESENT_V2_STRUCTURAL_PREPASS_MD_REFRESH_FAILED"
                producer_status[engine_id] = ("OK", hb_reason)

    for engine in active_engines:
        engine_id = str(engine["engine_id"])
        if engine_id in producer_status:
            hb_status, hb_reason = producer_status[engine_id]
        elif engine_id == "C2_INTENT_SIMULATOR_V1" and structural_activity_mode != "SIMULATOR":
            hb_status = "WARN"
            hb_reason = "ENGINE_DIRECT_RUN_NOT_PRESENT_V2_STRUCTURAL_PREPASS"
        else:
            allowed = allowed_map.get(engine_id)
            if allowed is None or (isinstance(allowed, list) and symbol in allowed):
                hb_status = "WARN"
                hb_reason = "ENGINE_DIRECT_RUN_NOT_PRESENT_V2_STRUCTURAL_PREPASS"
            else:
                hb_status = "WARN"
                hb_reason = "ENGINE_SKIPPED_SYMBOL_UNIVERSE"

        _emit_missing_engine_heartbeat(
            truth_root=truth_root,
            day=day,
            engine=engine,
            status=hb_status,
            reason_code=hb_reason,
            produced_utc=produced_utc,
            registry_sha=registry_sha,
            git_sha=git_sha,
            env=env,
        )


@dataclass(frozen=True)
class StageDef:
    stage_id: str
    cmd: List[str]
    required_for_paper: bool
    required_for_live: bool
    required_if_activity: bool
    blocking: bool
    skip_if_exists_paths: List[str]


@dataclass
class StageResult:
    stage_id: str
    classification: Dict[str, Any]
    executed: bool
    rc: int
    status: str
    reason_codes: List[str]
    outputs_present: List[str]


def _validate_sleeve_edge_publication_order(stage_defs: List[StageDef]) -> None:
    stage_ids = [stage.stage_id for stage in stage_defs]
    try:
        publication_index = stage_ids.index(SLEEVE_EDGE_PUBLICATION_STAGE_ID)
        governed_evaluation_index = stage_ids.index(GOVERNED_EVALUATION_STAGE_ID)
        allocation_index = stage_ids.index(CAPITAL_AUTHORITY_STAGE_ID)
    except ValueError as exc:
        raise SystemExit("FAIL: SLEEVE_EDGE_PUBLICATION_STAGE_MISSING_FROM_ORCHESTRATOR") from exc
    if publication_index >= governed_evaluation_index:
        raise SystemExit("FAIL: SLEEVE_EDGE_PUBLICATION_MUST_PRECEDE_GOVERNED_EVALUATION")
    if governed_evaluation_index >= allocation_index:
        raise SystemExit("FAIL: GOVERNED_EVALUATION_MUST_PRECEDE_CAPITAL_AUTHORITY_ALLOCATION")
    if not bool(stage_defs[publication_index].blocking):
        raise SystemExit("FAIL: SLEEVE_EDGE_PUBLICATION_STAGE_MUST_BE_BLOCKING")


def _should_stop_after_sleeve_edge_publication_failure(stage_results: List[Dict[str, Any]]) -> bool:
    for row in stage_results:
        if str(row.get("stage_id") or "") != SLEEVE_EDGE_PUBLICATION_STAGE_ID:
            continue
        return str(row.get("status") or "") == "FAIL"
    return False


def _path_exists(p: str) -> bool:
    try:
        s = str(p)
        if any(ch in s for ch in "*?[]"):
            import glob
            return len(glob.glob(s)) > 0
        return Path(s).resolve().exists()
    except Exception:
        return False


def _detect_activity(truth_root: Path, day: str) -> Dict[str, Any]:
    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    exec_sub_day = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    fills_day = (truth_root / "fill_ledger_v1" / day).resolve()

    intents_n = 0
    if intents_day.exists() and intents_day.is_dir():
        intents_n = len(
            [
                p
                for p in intents_day.iterdir()
                if p.is_file() and p.name.endswith(".json") and p.name != "no_intents_day.v1.json"
            ]
        )

    exec_n = 0
    if exec_sub_day.exists() and exec_sub_day.is_dir():
        exec_n = len(
            list(exec_sub_day.glob("*/broker_submission_record.v2.json"))
        )

    fills_n = 0
    if fills_day.exists() and fills_day.is_dir():
        fills_n = len([p for p in fills_day.glob("*.fill_ledger.v1.json") if p.is_file()])

    activity = (intents_n > 0) or (exec_n > 0) or (fills_n > 0)

    return {
        "activity": bool(activity),
        "intents_json_count": int(intents_n),
        "exec_submission_json_count": int(exec_n),
        "fill_ledger_json_count": int(fills_n),
        "paths": {
            "intents_day": str(intents_day),
            "exec_submissions_day": str(exec_sub_day),
            "fills_day": str(fills_day),
        },
    }


def _sleeve_edge_core2_summary_ready(truth_root: Path, day: str) -> Tuple[bool, str]:
    summary_root = (truth_root / "reports" / "reconciled_trade_state_summary_v1" / day).resolve()
    if not summary_root.exists() or not summary_root.is_dir():
        return (False, "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_MISSING")

    summary_paths = sorted(summary_root.glob("*/reconciled_trade_state_summary.v1.json"))
    if not summary_paths:
        return (False, "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_MISSING")

    saw_parsed_summary = False
    for summary_path in summary_paths:
        try:
            summary_obj = _read_json_obj(summary_path)
        except Exception:
            # Preserve fail-closed behavior for malformed summary artifacts.
            return (True, "")
        saw_parsed_summary = True
        trade_refs = summary_obj.get("trade_refs")
        if isinstance(trade_refs, list) and len(trade_refs) > 0:
            return (True, "")

    if saw_parsed_summary:
        return (False, "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_EMPTY")
    return (False, "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_MISSING")


def _resolve_session_state(day: str) -> Dict[str, Any]:
    cal_root = (
        resolve_canonical_truth_root_bridge_v1(
            caller="ops/tools/run_c2_paper_day_orchestrator_v2.py"
        )
        / "market_calendar_v1"
    ).resolve()
    manifest = (cal_root / "dataset_manifest.json").resolve()
    info: Dict[str, Any] = {
        "session_state": "UNKNOWN_SESSION",
        "source": {
            "path": str(manifest),
            "sha256": "",
            "resolution_reason": "",
        },
    }

    try:
        wk = date.fromisoformat(day).weekday()  # Mon=0 ... Sun=6
    except Exception:
        wk = -1

    is_weekend = wk in (5, 6)
    if manifest.exists() and manifest.is_file():
        info["source"]["sha256"] = _sha256_file(manifest)
        try:
            obj = json.loads(manifest.read_text(encoding="utf-8"))
            files = obj.get("files")
            if isinstance(files, list):
                year = int(day[0:4])
                for ent in files:
                    if not isinstance(ent, dict):
                        continue
                    if int(ent.get("year", -1)) != year:
                        continue
                    rel = str(ent.get("file") or "").strip()
                    if not rel:
                        continue
                    fp = (cal_root / rel).resolve()
                    if not fp.exists() or not fp.is_file():
                        continue
                    for line in fp.read_text(encoding="utf-8").splitlines():
                        if not line.strip():
                            continue
                        rec = json.loads(line)
                        if str(rec.get("day_utc") or "").strip() != day:
                            continue
                        if bool(rec.get("is_trading_session")):
                            info["session_state"] = "TRADING_SESSION"
                            info["source"]["resolution_reason"] = "CALENDAR_EXPLICIT_TRUE"
                        else:
                            info["session_state"] = "NON_TRADING_SESSION"
                            info["source"]["resolution_reason"] = "CALENDAR_EXPLICIT_FALSE"
                        info["source"]["path"] = str(fp)
                        info["source"]["sha256"] = _sha256_file(fp)
                        return info
        except Exception:
            pass

    if is_weekend:
        info["session_state"] = "NON_TRADING_SESSION"
        info["source"]["resolution_reason"] = "WEEKEND_FALLBACK_NO_CALENDAR_RECORD"
        return info

    info["source"]["resolution_reason"] = "CALENDAR_RECORD_MISSING"
    return info


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _should_skip_existing_outputs(sd: "StageDef", outputs_present: List[str]) -> bool:
    if not outputs_present:
        return False
    if sd.stage_id not in REFRESHABLE_GATE_STAGE_IDS:
        return True
    if len(outputs_present) != 1:
        return False
    path = Path(outputs_present[0]).resolve()
    if not path.exists() or not path.is_file():
        return False
    try:
        obj = _read_json_obj(path)
    except Exception:
        return False
    status = str(obj.get("status") or "").strip().upper()
    return status in ("PASS", "OK")


def _discover_short_vol_symbols(truth_root: Path, day: str) -> List[str]:
    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    if not intents_day.exists() or not intents_day.is_dir():
        return []

    out: List[str] = []
    for p in sorted(intents_day.iterdir()):
        if not p.is_file() or not p.name.endswith(".json"):
            continue
        try:
            obj = _read_json_obj(p)
        except Exception:
            continue
        if str(obj.get("exposure_type") or "").strip().upper() != "SHORT_VOL_DEFINED":
            continue
        underlying = obj.get("underlying") if isinstance(obj.get("underlying"), dict) else {}
        symbol = str(underlying.get("symbol") or "").strip().upper()
        if symbol:
            out.append(symbol)
    return sorted(set(out))


def _discover_equity_entry_symbols(truth_root: Path, day: str) -> List[str]:
    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    if not intents_day.exists() or not intents_day.is_dir():
        return []

    out: List[str] = []
    for p in sorted(intents_day.iterdir()):
        if not p.is_file() or not p.name.endswith(".json"):
            continue
        try:
            obj = _read_json_obj(p)
        except Exception:
            continue
        if str(obj.get("exposure_type") or "").strip().upper() != "LONG_EQUITY":
            continue
        try:
            target_pct = Decimal(str(obj.get("target_notional_pct") or "").strip())
        except (InvalidOperation, ValueError):
            continue
        if target_pct <= Decimal("0"):
            continue
        underlying = obj.get("underlying") if isinstance(obj.get("underlying"), dict) else {}
        symbol = str(underlying.get("symbol") or "").strip().upper()
        if symbol:
            out.append(symbol)
    return sorted(set(out))


def _options_raw_exists_for_symbol(truth_root: Path, day: str, symbol: str) -> bool:
    root = (truth_root / "options_chain_raw_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return False
    for d in sorted(root.iterdir()):
        raw = (d / "raw_chain.json").resolve()
        if not raw.exists() or not raw.is_file():
            continue
        try:
            obj = _read_json_obj(raw)
        except Exception:
            continue
        underlying = obj.get("underlying") if isinstance(obj.get("underlying"), dict) else {}
        if str(underlying.get("symbol") or "").strip().upper() == symbol.upper():
            return True
    return False


def _options_snapshot_exists_for_symbol(truth_root: Path, day: str, symbol: str) -> bool:
    root = (truth_root / "options_chain_snapshot_v1" / day).resolve()
    if not root.exists() or not root.is_dir():
        return False
    for d in sorted(root.iterdir()):
        snap = (d / "options_chain_snapshot.v1.json").resolve()
        cert = (d / "freshness_certificate.v1.json").resolve()
        if not snap.exists() or not snap.is_file() or not cert.exists() or not cert.is_file():
            continue
        try:
            obj = _read_json_obj(snap)
        except Exception:
            continue
        underlying = obj.get("underlying") if isinstance(obj.get("underlying"), dict) else {}
        if str(underlying.get("symbol") or "").strip().upper() == symbol.upper():
            return True
    return False


def _market_data_close_for_same_day(truth_root: Path, day: str, symbol: str) -> Optional[str]:
    md_root = (truth_root / "market_data_snapshot_v1").resolve()
    manifest = (md_root / "dataset_manifest.json").resolve()
    if not manifest.exists() or not manifest.is_file():
        return None

    try:
        manifest_obj = _read_json_obj(manifest)
    except Exception:
        return None

    files = manifest_obj.get("files")
    if not isinstance(files, list):
        return None

    year = int(day[0:4])
    target_rel: Optional[str] = None
    target_sha: Optional[str] = None
    for entry in files:
        if not isinstance(entry, dict):
            continue
        if int(entry.get("year", -1)) != year:
            continue
        if str(entry.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        target_rel = str(entry.get("file") or "").strip()
        target_sha = str(entry.get("sha256") or "").strip().lower()
        break
    if not target_rel or not target_sha:
        return None

    year_path = (md_root / target_rel).resolve()
    if not year_path.exists() or not year_path.is_file():
        return None
    if _sha256_file(year_path).lower() != target_sha:
        return None

    for line in year_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            row = json.loads(s)
        except Exception:
            return None
        if not isinstance(row, dict):
            return None
        ts = str(row.get("timestamp_utc") or "").strip()
        if ts[:10] != day:
            continue
        raw_close = row.get("close")
        try:
            close = Decimal(str(raw_close).strip())
        except (InvalidOperation, ValueError):
            return None
        if not close.is_finite() or close <= Decimal("0"):
            return None
        return format(close.normalize(), "f")
    return None


def _liquidity_gate_reference_price(truth_root: Path, day: str, symbol: str) -> Optional[str]:
    gate_path = (truth_root / "reports" / "liquidity_slippage_gate_v1" / day / "liquidity_slippage_gate.v1.json").resolve()
    if not gate_path.exists() or not gate_path.is_file():
        return None
    try:
        gate_obj = _read_json_obj(gate_path)
    except Exception:
        return None
    if str(gate_obj.get("status") or "").strip().upper() not in {"PASS", "OK"}:
        return None
    results = gate_obj.get("results")
    per_intent = results.get("per_intent") if isinstance(results, dict) else None
    if not isinstance(per_intent, list):
        return None
    symbol_upper = str(symbol or "").strip().upper()
    for row in per_intent:
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol") or "").strip().upper() != symbol_upper:
            continue
        metrics = row.get("metrics")
        if not isinstance(metrics, dict):
            continue
        raw_close = metrics.get("close")
        try:
            close = Decimal(str(raw_close).strip())
        except (InvalidOperation, ValueError):
            continue
        if not close.is_finite() or close <= Decimal("0"):
            continue
        return format(close.normalize(), "f")
    return None


def _build_phasec_materializer_cmd(*, truth_root: Path, day: str, produced_utc: str, ib_account: str) -> Tuple[List[str], List[str]]:
    execution_truth_root = resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    ).execution_root_path
    cmd = [
        "python3",
        "ops/tools/run_phasec_identity_materializer_day_v1.py",
        "--day_utc",
        day,
        "--eval_time_utc",
        f"{day}T00:00:00Z",
        "--truth_root",
        str(truth_root),
        "--execution_truth_root",
        str(execution_truth_root),
    ]
    reason_codes: List[str] = []
    equity_symbols = _discover_equity_entry_symbols(truth_root, day)
    if len(equity_symbols) == 1:
        price_symbol = equity_symbols[0]
        price = _market_data_close_for_same_day(truth_root, day, price_symbol)
        source = "MARKET_DATA"
        if not price:
            price = _liquidity_gate_reference_price(truth_root, day, price_symbol)
            source = "LIQUIDITY_GATE"
        if price:
            cmd.extend(["--default_equity_reference_price", price])
            reason_codes.append(f"DEFAULT_EQUITY_REFERENCE_PRICE_FROM_{source}:{price_symbol}:{price}")
        else:
            reason_codes.append(f"DEFAULT_EQUITY_REFERENCE_PRICE_ABSENT_PRESERVE_VETO:{price_symbol}")
    elif len(equity_symbols) > 1:
        reason_codes.append(
            "DEFAULT_EQUITY_REFERENCE_PRICE_MULTI_SYMBOL_PRESERVE_VETO:" + ",".join(sorted(equity_symbols))
        )
    else:
        reason_codes.append("DEFAULT_EQUITY_REFERENCE_PRICE_NO_EQUITY_ENTRY_INTENTS")
    return cmd, reason_codes


def _run_options_capture_stage(*, truth_root: Path, day: str, produced_utc: str, env: Dict[str, str]) -> Tuple[bool, int, List[str], List[str]]:
    option_symbols = _discover_short_vol_symbols(truth_root, day)
    if not option_symbols:
        return (False, 0, ["SKIP_NO_SHORT_VOL_INTENTS"], [])

    outputs: List[str] = []
    reason_codes: List[str] = []
    executed = False
    for symbol in option_symbols:
        raw_root = str((truth_root / "options_chain_raw_v1" / day).resolve())
        outputs.append(raw_root)
        if _options_snapshot_exists_for_symbol(truth_root, day, symbol):
            reason_codes.append(f"SKIP_OPTIONS_SNAPSHOT_ALREADY_PRESENT:{symbol}")
            continue
        if _options_raw_exists_for_symbol(truth_root, day, symbol):
            reason_codes.append(f"SKIP_OPTIONS_RAW_ALREADY_PRESENT:{symbol}")
            continue
        executed = True
        rc = _run_cmd(
            f"A6C_OPTIONS_CHAIN_CAPTURE_IB_DAY_V1_{symbol}",
            [
                "python3",
                "ops/tools/run_options_chain_capture_ib_day_v1.py",
                "--day_utc",
                day,
                "--eval_time_utc",
                produced_utc,
                "--symbol",
                symbol,
                "--truth_root",
                str(truth_root),
                "--ib_host",
                str(env.get("C2_IB_HOST") or "127.0.0.1").strip(),
                "--ib_port",
                str(env.get("C2_IB_PORT") or "4002").strip(),
                "--ib_client_id",
                str(env.get("C2_IB_CLIENT_ID") or "7").strip(),
            ],
            env=env,
        )
        if rc != 0:
            return (True, int(rc), [f"OPTIONS_CAPTURE_FAILED:{symbol}"], outputs)
        reason_codes.append(f"OPTIONS_CAPTURE_OK:{symbol}")
    return (executed, 0, reason_codes or ["SKIP_NO_OPTIONS_CAPTURE_NEEDED"], outputs)


def _run_options_promotion_stage(*, truth_root: Path, day: str, produced_utc: str, env: Dict[str, str]) -> Tuple[bool, int, List[str], List[str]]:
    option_symbols = _discover_short_vol_symbols(truth_root, day)
    if not option_symbols:
        return (False, 0, ["SKIP_NO_SHORT_VOL_INTENTS"], [])

    outputs: List[str] = []
    reason_codes: List[str] = []
    executed = False
    for symbol in option_symbols:
        snap_root = str((truth_root / "options_chain_snapshot_v1" / day).resolve())
        outputs.append(snap_root)
        if _options_snapshot_exists_for_symbol(truth_root, day, symbol):
            reason_codes.append(f"SKIP_OPTIONS_PROMOTION_ALREADY_PRESENT:{symbol}")
            continue
        if not _options_raw_exists_for_symbol(truth_root, day, symbol):
            reason_codes.append(f"SKIP_OPTIONS_PROMOTION_RAW_MISSING:{symbol}")
            continue
        executed = True
        rc = _run_cmd(
            f"A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_DAY_V1_{symbol}",
            [
                "python3",
                "ops/tools/run_options_chain_truth_promotion_day_v1.py",
                "--day_utc",
                day,
                "--eval_time_utc",
                produced_utc,
                "--symbol",
                symbol,
                "--truth_root",
                str(truth_root),
            ],
            env=env,
        )
        if rc != 0:
            return (True, int(rc), [f"OPTIONS_PROMOTION_FAILED:{symbol}"], outputs)
        reason_codes.append(f"OPTIONS_PROMOTION_OK:{symbol}")
    return (executed, 0, reason_codes or ["SKIP_NO_OPTIONS_PROMOTION_NEEDED"], outputs)


def _run_market_data_refresh_stage(*, truth_root: Path, day: str, produced_utc: str, env: Dict[str, str]) -> Tuple[bool, int, List[str], List[str]]:
    equity_symbols = _discover_equity_entry_symbols(truth_root, day)
    if not equity_symbols:
        return (False, 0, ["SKIP_NO_EQUITY_ENTRY_INTENTS"], [])

    return _run_market_data_refresh_for_symbols(
        truth_root=truth_root,
        day=day,
        produced_utc=produced_utc,
        env=env,
        symbols=equity_symbols,
        empty_reason="SKIP_NO_EQUITY_ENTRY_INTENTS",
    )


def _run_market_data_refresh_for_symbols(
    *,
    truth_root: Path,
    day: str,
    produced_utc: str,
    env: Dict[str, str],
    symbols: List[str],
    empty_reason: str,
) -> Tuple[bool, int, List[str], List[str]]:
    if not symbols:
        return (False, 0, [empty_reason], [])

    outputs: List[str] = []
    reason_codes: List[str] = []
    executed = False
    for symbol in sorted(set(symbols)):
        md_year = str((truth_root / "market_data_snapshot_v1" / symbol.upper() / f"{day[:4]}.jsonl").resolve())
        outputs.append(md_year)
        if _market_data_close_for_same_day(truth_root, day, symbol):
            reason_codes.append(f"SKIP_MARKET_DATA_SAME_DAY_ALREADY_PRESENT:{symbol}")
            continue
        executed = True
        rc = _run_cmd(
            f"A6E_MARKET_DATA_SNAPSHOT_REFRESH_V1_{symbol}",
            [
                ".venv_c2/bin/python",
                "constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py",
                "--run_utc",
                produced_utc,
                "--dataset_version",
                "v1",
                "--symbol",
                symbol,
                "--start_year",
                day[:4],
                "--end_year",
                day[:4],
                "--host",
                str(env.get("C2_IB_HOST") or "127.0.0.1").strip(),
                "--port",
                str(env.get("C2_IB_PORT") or "4002").strip(),
                "--client_id",
                str(env.get("C2_IB_CLIENT_ID") or "7").strip(),
                "--sleep_sec",
                str(env.get("C2_IB_SLEEP_SEC") or "0.1").strip(),
                "--use_rth",
                "1",
            ],
            env=env,
        )
        if rc != 0:
            return (True, int(rc), [f"MARKET_DATA_REFRESH_FAILED:{symbol}"], outputs)
        if _market_data_close_for_same_day(truth_root, day, symbol):
            reason_codes.append(f"MARKET_DATA_SAME_DAY_OK:{symbol}")
        else:
            return (True, 2, [f"MARKET_DATA_SAME_DAY_STILL_ABSENT:{symbol}"], outputs)
    return (executed, 0, reason_codes or ["SKIP_NO_MARKET_DATA_REFRESH_NEEDED"], outputs)

def _positions_snapshot_v5_skip_safe(truth_root: Path, day: str) -> bool:
    snap_path = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json").resolve()
    if not snap_path.exists() or not snap_path.is_file():
        return False
    try:
        snap_obj = _read_json_obj(snap_path)
    except Exception:
        return False
    if str(snap_obj.get("schema_id") or "").strip() != "C2_POSITIONS_SNAPSHOT_V5":
        return False
    if str(snap_obj.get("day_utc") or "").strip() != day:
        return False
    items = snap_obj.get("items") if isinstance(snap_obj.get("items"), list) else []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "").strip().upper() != "OPEN":
                continue
            try:
                qty = int(item.get("qty") or 0)
            except Exception:
                return False
            if qty <= 0:
                return False
    return True


def _positions_snapshot_v2_skip_safe(truth_root: Path, day: str) -> bool:
    snap_path = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json").resolve()
    latest_ptr = (truth_root / "positions_v1" / "latest_pointer.v2.json").resolve()
    canonical_v5_path = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json").resolve()

    if not canonical_v5_path.exists() or not canonical_v5_path.is_file():
        return False
    if not snap_path.exists() or not snap_path.is_file():
        return False
    try:
        snap_obj = _read_json_obj(snap_path)
    except Exception:
        return False
    if str(snap_obj.get("schema_id") or "").strip() != "C2_POSITIONS_SNAPSHOT_V2":
        return False
    if str(snap_obj.get("day_utc") or "").strip() != day:
        return False
    reason_codes = snap_obj.get("reason_codes")
    if not isinstance(reason_codes, list) or "COMPAT_BRIDGE_FROM_POSITIONS_V5" not in reason_codes:
        return False
    positions = snap_obj.get("positions") if isinstance(snap_obj.get("positions"), dict) else {}
    items = positions.get("items") if isinstance(positions, dict) else []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "").strip().upper() != "OPEN":
                continue
            try:
                qty = int(item.get("qty") or 0)
            except Exception:
                return False
            if qty <= 0:
                return False

    # Optional strengthening: if latest pointer currently references this same snapshot,
    # require its embedded sha256 to match; otherwise still allow skip on valid same-day snapshot.
    if latest_ptr.exists() and latest_ptr.is_file():
        try:
            latest_obj = _read_json_obj(latest_ptr)
            pointers = latest_obj.get("pointers")
            if isinstance(pointers, dict):
                pointer_snap_path = str(pointers.get("snapshot_path") or "").strip()
                pointer_snap_sha = str(pointers.get("snapshot_sha256") or "").strip().lower()
                if pointer_snap_path and len(pointer_snap_sha) == 64:
                    resolved_pointer_snap_path = Path(pointer_snap_path).expanduser().resolve()
                    if resolved_pointer_snap_path == snap_path:
                        return _sha256_file(snap_path) == pointer_snap_sha
        except Exception:
            pass
    return True


def _seed_sleeve_positions_snapshot_from_canonical(truth_root: Path, day: str) -> dict:
    global_truth = (truth_root / "global").resolve()
    canonical_path = (
        global_truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
    ).resolve()
    if not canonical_path.exists() or not canonical_path.is_file():
        return {"seeded_count": 0}

    canonical_payload = _read_json_obj(canonical_path)
    sleeves_root = (truth_root / "sleeves").resolve()
    if not sleeves_root.exists() or not sleeves_root.is_dir():
        return {"seeded_count": 0}

    seeded_count = 0
    canonical_text = canonical_path.read_text(encoding="utf-8")
    for sleeve_dir in sorted(sleeves_root.iterdir(), key=lambda p: p.name):
        if not sleeve_dir.is_dir():
            continue
        sleeve_snapshot = (
            sleeve_dir / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
        ).resolve()
        if sleeve_snapshot.exists():
            continue
        sleeve_snapshot.parent.mkdir(parents=True, exist_ok=True)
        sleeve_snapshot.write_text(canonical_text, encoding="utf-8")
        written_payload = _read_json_obj(sleeve_snapshot)
        if written_payload != canonical_payload:
            raise RuntimeError(f"SLEEVE_POSITIONS_SNAPSHOT_SEED_MISMATCH:{sleeve_dir.name}:{day}")
        seeded_count += 1
    return {"seeded_count": seeded_count}

def _count_broker_submission_records(truth_root: Path, day: str) -> int:
    d = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not d.exists() or not d.is_dir():
        return 0
    return len(list(d.glob("*/broker_submission_record.v2.json")))


def _count_submission_dirs(truth_root: Path, day: str) -> int:
    d = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not d.exists() or not d.is_dir():
        return 0
    return len([p for p in d.iterdir() if p.is_dir()])


def _submission_dirs_for_day(
    truth_root: Path,
    day: str,
    *,
    require_broker_submission_record: bool = False,
    require_lifecycle_ready_identity_set: bool = False,
) -> List[Path]:
    d = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not d.exists() or not d.is_dir():
        return []
    out: List[Path] = []
    for path in sorted((p.resolve() for p in d.iterdir() if p.is_dir()), key=lambda item: item.name):
        if path.name.startswith("__"):
            continue
        if require_broker_submission_record:
            bsr_path = (path / "broker_submission_record.v2.json").resolve()
            if not bsr_path.exists() or not bsr_path.is_file():
                continue
        if require_lifecycle_ready_identity_set:
            plan_v2 = (path / "equity_order_plan.v2.json").resolve()
            if plan_v2.exists() and plan_v2.is_file():
                out.append(path)
                continue
            options_plan = (path / "order_plan.v1.json").resolve()
            if options_plan.exists() and options_plan.is_file():
                out.append(path)
                continue
            plan_v1 = (path / "equity_order_plan.v1.json").resolve()
            if not plan_v1.exists() or not plan_v1.is_file():
                continue
            try:
                plan_obj = _read_json_obj(plan_v1)
            except Exception:
                continue
            if str(plan_obj.get("schema_id") or "").strip() != "equity_order_plan":
                continue
            if str(plan_obj.get("schema_version") or "").strip() not in {"v1", "v2"}:
                continue
        out.append(path)
    return out


def _run_submission_lifecycle_refresh_stage(*, truth_root: Path, day: str, env: Dict[str, str]) -> Tuple[bool, int, List[str], List[str]]:
    submission_dirs = _submission_dirs_for_day(
        truth_root,
        day,
        require_broker_submission_record=True,
        require_lifecycle_ready_identity_set=True,
    )
    if not submission_dirs:
        return (False, 0, ["SKIP_NO_LIFECYCLE_ELIGIBLE_SUBMISSIONS"], [])

    def _stream_record_is_dry_run_snapshot(stream_obj: Dict[str, Any]) -> bool:
        reason_codes = stream_obj.get("reason_codes")
        if not isinstance(reason_codes, list):
            return False
        return any(str(code).strip().upper() == "DRY_RUN_SUBMISSION_SNAPSHOT" for code in reason_codes)

    def _submission_stream_is_dry_run_snapshot(submission_id: str) -> bool:
        stream_dir = (truth_root / "execution_stream_v1" / day).resolve()
        if not stream_dir.exists() or not stream_dir.is_dir():
            return False
        stream_rows: List[Dict[str, Any]] = []
        for stream_path in sorted(stream_dir.glob("*.execution_event_stream_record.v1.json")):
            if not stream_path.is_file():
                continue
            try:
                payload = _read_json_obj(stream_path)
            except Exception:
                continue
            if str(payload.get("submission_id") or "").strip() != submission_id:
                continue
            stream_rows.append(payload)
        if not stream_rows:
            return False
        return all(_stream_record_is_dry_run_snapshot(row) for row in stream_rows)

    outputs_present: List[str] = []
    reason_codes: List[str] = []
    for submission_dir in submission_dirs:
        submission_id = submission_dir.name
        rc = _run_cmd(
            f"B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1_{submission_id}",
            [
                "python3",
                "ops/tools/run_submission_lifecycle_refresh_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth_root),
                "--submission_id",
                submission_id,
            ],
            env=env,
        )
        if rc != 0:
            return (True, int(rc), [f"SUBMISSION_LIFECYCLE_REFRESH_FAILED:{submission_id}:rc={int(rc)}"], outputs_present)
        execution_event_path = (submission_dir / "execution_event_record.v1.json").resolve()
        if execution_event_path.exists() and execution_event_path.is_file():
            outputs_present.append(str(execution_event_path))
            reason_codes.append(f"SUBMISSION_LIFECYCLE_REFRESH_OK:{submission_id}")
            continue
        broker_submission_path = (submission_dir / "broker_submission_record.v2.json").resolve()
        if broker_submission_path.exists() and broker_submission_path.is_file():
            outputs_present.append(str(broker_submission_path))
            try:
                broker_submission_obj = _read_json_obj(broker_submission_path)
            except Exception:
                broker_submission_obj = {}
            broker_error = (
                broker_submission_obj.get("error")
                if isinstance(broker_submission_obj.get("error"), dict)
                else {}
            )
            broker_error_code = str(broker_error.get("code") or "").strip().upper()
            if broker_error_code == "DRY_RUN_NO_BROKER_ID" or _submission_stream_is_dry_run_snapshot(submission_id):
                reason_codes.append(f"SUBMISSION_LIFECYCLE_REFRESH_DRY_RUN_NO_BROKER_ID:{submission_id}")
                continue
        reason_codes.append(f"SUBMISSION_LIFECYCLE_REFRESH_PENDING_NO_STREAM:{submission_id}")
    return (True, 0, reason_codes, outputs_present)


def _execution_stream_snapshot_skip_safe(truth_root: Path, day: str) -> bool:
    stream_day = (truth_root / "execution_stream_v1" / day).resolve()
    if not stream_day.exists() or not stream_day.is_dir():
        return False

    submission_dirs = _submission_dirs_for_day(
        truth_root,
        day,
        require_broker_submission_record=True,
        require_lifecycle_ready_identity_set=True,
    )
    if not submission_dirs:
        return False

    covered_submission_ids: set[str] = set()
    for stream_path in sorted(stream_day.glob("*.execution_event_stream_record.v1.json")):
        if not stream_path.is_file():
            continue
        try:
            payload = _read_json_obj(stream_path)
        except Exception:
            return False
        submission_id = str(payload.get("submission_id") or "").strip()
        if submission_id:
            covered_submission_ids.add(submission_id)

    if not covered_submission_ids:
        return False

    for submission_dir in submission_dirs:
        if submission_dir.name not in covered_submission_ids:
            return False
    return True


def _fill_ledger_skip_safe(truth_root: Path, day: str) -> bool:
    submissions_day = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    ledgers_day = (truth_root / "fill_ledger_v1" / day).resolve()
    if not submissions_day.exists() or not submissions_day.is_dir():
        return False
    if not ledgers_day.exists() or not ledgers_day.is_dir():
        return False
    submission_dirs = sorted([p for p in submissions_day.iterdir() if p.is_dir()])
    if not submission_dirs:
        return False
    for subdir in submission_dirs:
        bsr = (subdir / "broker_submission_record.v2.json").resolve()
        if not bsr.exists() or not bsr.is_file():
            continue
        ledger = (ledgers_day / f"{subdir.name}.fill_ledger.v1.json").resolve()
        if not ledger.exists() or not ledger.is_file():
            return False
    return True


def _allocation_summary_v1_skip_safe(path: Path, day: str) -> bool:
    p = path.resolve()
    if not p.exists() or not p.is_file():
        return False
    try:
        obj = _read_json_obj(p)
        if str(obj.get("day_utc") or "").strip() != day:
            raise RuntimeError(f"DAY_MISMATCH:{p}")
        from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

        validate_against_repo_schema_v1(
            obj,
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json",
        )
        return True
    except Exception:
        stale_sha = _sha256_file(p) if p.exists() and p.is_file() else "missing"
        quarantine = p.with_name(f"{p.name}.INVALID_{stale_sha}.json")
        if quarantine.exists():
            quarantine = p.with_name(f"{p.name}.INVALID_{stale_sha}.{os.getpid()}.json")
        os.replace(str(p), str(quarantine))
        print(
            f"WARN: QUARANTINED_STALE_ALLOCATION_SUMMARY day_utc={day} "
            f"old_path={p} quarantined_path={quarantine} sha256={stale_sha}"
        )
        return False


def _capital_risk_envelope_v2_skip_safe(path: Path, day: str) -> bool:
    p = path.resolve()
    if not p.exists() or not p.is_file():
        return False
    try:
        obj = _read_json_obj(p)
        if str(obj.get("day_utc") or "").strip() != day:
            raise RuntimeError(f"DAY_MISMATCH:{p}")
        status = str(obj.get("status") or "").strip().upper()
        if status not in ("PASS", "OK"):
            raise RuntimeError(f"STATUS_NOT_PASS:{status}")
        from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

        validate_against_repo_schema_v1(
            obj,
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json",
        )
        return True
    except Exception:
        stale_sha = _sha256_file(p) if p.exists() and p.is_file() else "missing"
        quarantine = p.with_name(f"{p.name}.INVALID_{stale_sha}.json")
        if quarantine.exists():
            quarantine = p.with_name(f"{p.name}.INVALID_{stale_sha}.{os.getpid()}.json")
        os.replace(str(p), str(quarantine))
        print(
            f"WARN: QUARANTINED_STALE_CAPITAL_RISK_ENVELOPE day_utc={day} "
            f"old_path={p} quarantined_path={quarantine} sha256={stale_sha}"
        )
        return False


def _identity_dir_has_supported_set(d: Path) -> bool:
    if not d.exists() or not d.is_dir():
        return False

    if (
        (d / "equity_order_plan.v2.json").exists()
        and (d / "mapping_ledger_record.v2.json").exists()
        and (d / "binding_record.v2.json").exists()
    ):
        return True

    if (
        (d / "equity_order_plan.v1.json").exists()
        and (d / "mapping_ledger_record.v2.json").exists()
        and (d / "binding_record.v2.json").exists()
    ):
        return True

    if (
        (d / "order_plan.v1.json").exists()
        and (d / "mapping_ledger_record.v1.json").exists()
        and (d / "binding_record.v1.json").exists()
    ):
        return True

    return False


def _resolve_governed_phasec_day_root(*, day: str, ib_account: str = "") -> Path:
    account = str(ib_account or "").strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_root = resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=account,
        sleeve_id="PRIMARY",
    ).execution_root_path
    return (execution_root / "phaseC_preflight_v1" / day).resolve()


def _discover_same_day_identity_dirs(truth_root: Path, day: str, ib_account: str = "") -> List[Path]:
    root = _resolve_governed_phasec_day_root(day=day, ib_account=ib_account)
    if not root.exists() or not root.is_dir():
        return []

    latest_active_pointer = (root / "latest_active_attempt.v1.json").resolve()
    if latest_active_pointer.exists() and latest_active_pointer.is_file():
        try:
            pointer_obj = _read_json_obj(latest_active_pointer)
        except Exception:
            pointer_obj = {}
        attempt_dir_value = str(pointer_obj.get("attempt_dir") or "").strip()
        if attempt_dir_value:
            attempt_dir = Path(attempt_dir_value).resolve()
            if attempt_dir.exists() and attempt_dir.is_dir():
                latest_dirs = [child.resolve() for child in sorted(attempt_dir.iterdir()) if child.is_dir() and _identity_dir_has_supported_set(child)]
                if latest_dirs:
                    return latest_dirs

    latest_active = resolve_latest_active_attempt(
        truth_root=root.parent.parent.resolve(),
        invoked_day_utc=day,
        scope_key="PRIMARY::PAPER",
    )
    if isinstance(latest_active, dict):
        attempt_id = str(latest_active.get("attempt_id") or "").strip()
        if attempt_id:
            attempt_dir = (root / f"attempt_{attempt_id}").resolve()
            if attempt_dir.exists() and attempt_dir.is_dir():
                latest_dirs = [child.resolve() for child in sorted(attempt_dir.iterdir()) if child.is_dir() and _identity_dir_has_supported_set(child)]
                if latest_dirs:
                    return latest_dirs

    out: List[Path] = []
    for p in sorted(root.iterdir()):
        if not p.is_dir():
            continue
        if _identity_dir_has_supported_set(p):
            out.append(p.resolve())
            continue
        if not p.name.startswith("attempt_A"):
            continue
        for child in sorted(p.iterdir()):
            if child.is_dir() and _identity_dir_has_supported_set(child):
                out.append(child.resolve())
    return out


def _phasec_veto_only_summary(truth_root: Path, day: str, ib_account: str = "") -> Dict[str, Any]:
    root = _resolve_governed_phasec_day_root(day=day, ib_account=ib_account)
    if not root.exists() or not root.is_dir():
        return {
            "present": False,
            "identity_dir_count": 0,
            "veto_count": 0,
            "allow_count": 0,
            "detail_codes": [],
        }

    identity_dirs = _discover_same_day_identity_dirs(truth_root, day, ib_account)
    veto_files = sorted(root.glob("*.veto_record.v1.json"))
    allow_files = sorted(root.glob("*.submit_preflight_decision.v1.json"))
    detail_codes: List[str] = []

    for veto_path in veto_files:
        try:
            obj = _read_json_obj(veto_path)
        except Exception:
            continue
        reason_code = str(obj.get("reason_code") or "").strip()
        if reason_code:
            detail_codes.append(f"PHASEC_VETO_REASON_CODE:{reason_code}")
        reason_detail = str(obj.get("reason_detail") or "").strip()
        if reason_detail:
            detail_codes.append(f"PHASEC_VETO_DETAIL:{reason_detail.split(':', 1)[0]}")

    return {
        "present": True,
        "identity_dir_count": len(identity_dirs),
        "veto_count": len(veto_files),
        "allow_count": len(allow_files),
        "detail_codes": sorted(set(detail_codes)),
    }


def _is_64hex(s: str) -> bool:
    t = str(s or "").strip().lower()
    return len(t) == 64 and all(c in "0123456789abcdef" for c in t)


def _intent_file_sha_by_intent_id(truth_root: Path, day: str, intent_id: str) -> str:
    iid = str(intent_id or "").strip()
    if not iid:
        return ""
    intents_dir = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    if not intents_dir.exists() or not intents_dir.is_dir():
        return ""
    for p in sorted(intents_dir.glob("*.exposure_intent.v1.json")):
        if not p.is_file():
            continue
        try:
            o = _read_json_obj(p)
        except Exception:
            continue
        if str(o.get("intent_id") or "").strip() == iid:
            return _sha256_file(p)
    return ""


def _extract_identity_set_intent_sha(identity_dir: Path, truth_root: Path, day: str) -> str:
    adapter_record = (identity_dir / "exposure_to_options_adapter_record.v1.json").resolve()
    if adapter_record.exists() and adapter_record.is_file():
        try:
            o = _read_json_obj(adapter_record)
            ie = o.get("input_exposure_intent")
            if isinstance(ie, dict):
                v = str(ie.get("sha256") or "").strip()
                if v:
                    return v
        except Exception:
            pass

    for name in ("equity_order_plan.v2.json", "equity_order_plan.v1.json", "order_plan.v1.json"):
        p = (identity_dir / name).resolve()
        if not p.exists() or not p.is_file():
            continue
        o = _read_json_obj(p)
        # Prefer explicit lineage hashes carried by the released identity set.
        # Same-day intent ids may be reused across blocked/released snapshots.
        for k in ("intent_sha256", "intent_hash"):
            v = str(o.get(k) or "").strip()
            if v:
                return v
        # If the identity set only carries source intent id, resolve the day intent file
        # and hash it to align with the authorization writer key.
        intent_file_sha = _intent_file_sha_by_intent_id(truth_root, day, str(o.get("source_intent_id") or ""))
        if intent_file_sha:
            return intent_file_sha

    # Equity identity sets may only carry intent_id on equity_intent.v1.json.
    eq_intent = (identity_dir / "equity_intent.v1.json").resolve()
    if eq_intent.exists() and eq_intent.is_file():
        try:
            o = _read_json_obj(eq_intent)
            intent_file_sha = _intent_file_sha_by_intent_id(truth_root, day, str(o.get("intent_id") or ""))
            if intent_file_sha:
                return intent_file_sha
        except Exception:
            pass
    # Last-resort fallback for existing day structures where directory name is intent file sha.
    if _is_64hex(identity_dir.name):
        return identity_dir.name
    return ""


def _has_real_broker_id_value(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 0
    text = str(value or "").strip()
    if not text:
        return False
    try:
        return int(text) > 0
    except Exception:
        return False


def _broker_submission_has_real_broker_ids(bsr_obj: Dict[str, Any]) -> bool:
    broker_ids = bsr_obj.get("broker_ids")
    if not isinstance(broker_ids, dict):
        return False
    return _has_real_broker_id_value(broker_ids.get("order_id")) or _has_real_broker_id_value(broker_ids.get("perm_id"))


def _has_submission_evidence_for_intent_sha(
    *,
    truth_root: Path,
    day: str,
    intent_sha: str,
    require_real_broker_ids: bool = False,
) -> bool:
    target = str(intent_sha or "").strip().lower()
    if not _is_64hex(target):
        return False
    submissions_day = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not submissions_day.exists() or not submissions_day.is_dir():
        return False
    for subdir in sorted([p for p in submissions_day.iterdir() if p.is_dir()]):
        # Idempotency must only trigger after an actual submit attempt.
        # Veto-only dirs are historical failures and must not suppress a new
        # authorized attempt for the same intent.
        bsr_path = (subdir / "broker_submission_record.v2.json").resolve()
        if not bsr_path.exists() or not bsr_path.is_file():
            continue
        if require_real_broker_ids:
            try:
                bsr_obj = _read_json_obj(bsr_path)
            except Exception:
                continue
            if not _broker_submission_has_real_broker_ids(bsr_obj):
                continue
        plan_obj: Optional[Dict[str, Any]] = None
        for plan_name in ("equity_order_plan.v2.json", "equity_order_plan.v1.json", "order_plan.v1.json"):
            plan_path = (subdir / plan_name).resolve()
            if not plan_path.exists() or not plan_path.is_file():
                continue
            try:
                plan_obj = _read_json_obj(plan_path)
            except Exception:
                plan_obj = None
            if plan_obj is not None:
                break
        if plan_obj is None:
            continue
        for key in ("intent_sha256", "intent_hash"):
            candidate = str(plan_obj.get(key) or "").strip().lower()
            if candidate == target:
                return True
    return False


def _read_authorization_state(truth_root: Path, day: str, intent_sha: str) -> Dict[str, Any]:
    p = (truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_sha}.authorization.v1.json").resolve()
    if not p.exists() or not p.is_file():
        return {
            "ok": False,
            "reason_code": "GOV_SUBMIT_AUTHZ_MISSING",
            "status": "",
            "decision": "",
            "authorized_quantity": 0,
            "path": str(p),
        }

    try:
        obj = _read_json_obj(p)
    except Exception:
        return {
            "ok": False,
            "reason_code": "GOV_SUBMIT_AUTHZ_PARSE_ERROR",
            "status": "",
            "decision": "",
            "authorized_quantity": 0,
            "path": str(p),
        }

    auth = obj.get("authorization") if isinstance(obj.get("authorization"), dict) else {}
    try:
        qty = int(auth.get("authorized_quantity") or 0)
    except Exception:
        qty = 0

    return {
        "ok": True,
        "reason_code": "",
        "status": str(obj.get("status") or "").strip().upper(),
        "decision": str(auth.get("decision") or "").strip().upper(),
        "authorized_quantity": int(qty),
        "path": str(p),
    }


def _load_identity_submit_payload(identity_dir: Path) -> Dict[str, Any]:
    plan_v2 = (identity_dir / "equity_order_plan.v2.json").resolve()
    plan_v1 = (identity_dir / "equity_order_plan.v1.json").resolve()
    plan_opt = (identity_dir / "order_plan.v1.json").resolve()
    if plan_v2.exists() and plan_v2.is_file():
        plan_path = plan_v2
        mapping_path = (identity_dir / "mapping_ledger_record.v2.json").resolve()
        binding_path = (identity_dir / "binding_record.v2.json").resolve()
    elif plan_v1.exists() and plan_v1.is_file():
        plan_path = plan_v1
        mapping_path = (identity_dir / "mapping_ledger_record.v2.json").resolve()
        binding_path = (identity_dir / "binding_record.v2.json").resolve()
    elif plan_opt.exists() and plan_opt.is_file():
        plan_path = plan_opt
        mapping_path = (identity_dir / "mapping_ledger_record.v1.json").resolve()
        binding_path = (identity_dir / "binding_record.v1.json").resolve()
    else:
        raise RuntimeError(f"GOV_SUBMIT_IDENTITY_SET_MISSING_PLAN:{identity_dir}")
    if not mapping_path.exists() or not mapping_path.is_file():
        raise RuntimeError(f"GOV_SUBMIT_IDENTITY_SET_MISSING_MAPPING:{identity_dir}")
    if not binding_path.exists() or not binding_path.is_file():
        raise RuntimeError(f"GOV_SUBMIT_IDENTITY_SET_MISSING_BINDING:{identity_dir}")
    plan_obj = _read_json_obj(plan_path)
    mapping_obj = _read_json_obj(mapping_path)
    binding_obj = _read_json_obj(binding_path)
    submission_id = str(binding_obj.get("submission_id") or "").strip().lower()
    if not _is_64hex(submission_id):
        identity_path = (identity_dir / "execution_identity_record.v1.json").resolve()
        if identity_path.exists() and identity_path.is_file():
            identity_obj = _read_json_obj(identity_path)
            submission_id = str(identity_obj.get("submission_id") or "").strip().lower()
            if _is_64hex(submission_id):
                binding_obj = dict(binding_obj)
                binding_obj.setdefault("submission_id", submission_id)
    if not _is_64hex(submission_id):
        raise RuntimeError(f"GOV_SUBMIT_IDENTITY_SET_MISSING_SUBMISSION_ID:{identity_dir}")
    return {
        "plan_path": plan_path,
        "plan_obj": plan_obj,
        "mapping_path": mapping_path,
        "mapping_obj": mapping_obj,
        "binding_path": binding_path,
        "binding_obj": binding_obj,
        "submission_id": submission_id,
    }


def _execution_package_path_for_submission(*, truth_root: Path, day: str, submission_id: str) -> Path:
    return (truth_root / "execution_package_v1" / day / submission_id / "execution_package.v1.json").resolve()


def _execution_submission_record_path_for_submission(*, truth_root: Path, day: str, submission_id: str) -> Path:
    return (truth_root / "execution_kernel_v1" / "submission_records" / day / submission_id / "submission_record.v1.json").resolve()


def _materialize_submission_record_for_identity(
    *,
    truth_root: Path,
    day: str,
    identity_dir: Path,
    execution_package_path: Path,
    payload: Dict[str, Any],
) -> Tuple[bool, str]:
    try:
        _, _, write_action = write_execution_submission_record_from_execution_package_v1(
            execution_package_path=execution_package_path.resolve(),
            day_utc=day,
            produced_utc=f"{day}T00:00:00Z",
            truth_root=truth_root.resolve(),
            plan_obj=dict(payload.get("plan_obj") or {}),
            binding_obj=dict(payload.get("binding_obj") or {}),
        )
    except Exception as exc:  # noqa: BLE001
        return (
            False,
            (
                f"GOV_SUBMIT_SUBMISSION_RECORD_MATERIALIZE_FAILED:"
                f"{identity_dir.name}:{type(exc).__name__}:{exc}"
            ),
        )
    submission_id = str(payload.get("submission_id") or "").strip().lower()
    return (True, f"GOV_SUBMIT_SUBMISSION_RECORD_{write_action}:{identity_dir.name}:{submission_id}")


def _run_execution_build_authority_for_identity(
    *,
    identity_dir: Path,
    env: Dict[str, str],
) -> int:
    return _run_cmd(
        "A7AA_EXECUTION_BUILD_AUTHORITY_V1",
        [
            sys.executable,
            str((REPO_ROOT / "ops/tools/run_execution_build_authority_v1.py").resolve()),
            "--operation_type",
            "fresh_paper_entry_v1",
            "--candidate_path",
            str(identity_dir.resolve()),
            "--materialize",
            "YES",
            "--emit_package",
            "YES",
        ],
        env=env,
    )


def _ensure_governed_submit_inputs(
    *,
    truth_root: Path,
    day: str,
    identity_dir: Path,
    env: Dict[str, str],
) -> Dict[str, Any]:
    payload = _load_identity_submit_payload(identity_dir)
    submission_id = str(payload["submission_id"])
    package_path = _execution_package_path_for_submission(
        truth_root=truth_root,
        day=day,
        submission_id=submission_id,
    )
    submission_record_path = _execution_submission_record_path_for_submission(
        truth_root=truth_root,
        day=day,
        submission_id=submission_id,
    )
    if package_path.exists() and package_path.is_file() and submission_record_path.exists() and submission_record_path.is_file():
        return {
            "ok": True,
            "reason_code": "",
            "submission_id": submission_id,
            "execution_package_path": package_path,
            "submission_record_path": submission_record_path,
        }
    rc = _run_execution_build_authority_for_identity(identity_dir=identity_dir, env=env)
    if rc != 0:
        return {
            "ok": False,
            "reason_code": f"GOV_SUBMIT_EXECUTION_BUILD_NONZERO_RC:{identity_dir.name}:rc={rc}",
            "submission_id": submission_id,
            "execution_package_path": package_path,
            "submission_record_path": submission_record_path,
        }
    if package_path.exists() and package_path.is_file() and (not submission_record_path.exists() or not submission_record_path.is_file()):
        materialized_ok, materialize_code = _materialize_submission_record_for_identity(
            truth_root=truth_root,
            day=day,
            identity_dir=identity_dir,
            execution_package_path=package_path,
            payload=payload,
        )
        if not materialized_ok:
            return {
                "ok": False,
                "reason_code": materialize_code,
                "submission_id": submission_id,
                "execution_package_path": package_path,
                "submission_record_path": submission_record_path,
            }
    missing: List[str] = []
    if not package_path.exists() or not package_path.is_file():
        missing.append(f"execution_package={package_path}")
    if not submission_record_path.exists() or not submission_record_path.is_file():
        missing.append(f"submission_record={submission_record_path}")
    if missing:
        return {
            "ok": False,
            "reason_code": f"GOV_SUBMIT_EXECUTION_INPUTS_MISSING_AFTER_BUILD:{identity_dir.name}:{'|'.join(missing)}",
            "submission_id": submission_id,
            "execution_package_path": package_path,
            "submission_record_path": submission_record_path,
        }
    return {
        "ok": True,
        "reason_code": "",
        "submission_id": submission_id,
        "execution_package_path": package_path,
        "submission_record_path": submission_record_path,
    }


def _emit_governed_submit_skip_veto(
    *,
    truth_root: Path,
    day: str,
    eval_time_utc: str,
    identity_dir: Path,
    reason_code: str,
    reason_detail: str,
    pointer_paths: List[str],
) -> Tuple[bool, str]:
    try:
        payload = _load_identity_submit_payload(identity_dir)
    except Exception as exc:  # noqa: BLE001
        return (False, f"GOV_SUBMIT_SKIP_EVIDENCE_INPUT_ERROR:{identity_dir.name}:{type(exc).__name__}")
    submission_id = str(payload["submission_id"])
    submission_dir = (truth_root / "execution_evidence_v1" / "submissions" / day / submission_id).resolve()
    evidence_files = (
        (submission_dir / "broker_submission_record.v2.json").exists()
        or (submission_dir / "execution_event_record.v1.json").exists()
        or (submission_dir / "veto_record.v1.json").exists()
    )
    if evidence_files:
        return (True, f"GOV_SUBMIT_SKIP_EVIDENCE_ALREADY_PRESENT:{submission_id}")
    veto_obj: Dict[str, Any] = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "boundary": "SUBMIT",
        "reason_code": str(reason_code).strip().upper() or "GOV_SUBMIT_SKIPPED",
        "reason_detail": str(reason_detail).strip(),
        "inputs": {
            "intent_hash": str(payload["plan_obj"].get("intent_hash") or "").strip() or str(payload["binding_obj"].get("intent_hash") or "").strip(),
            "plan_hash": str(payload["plan_obj"].get("plan_hash") or "").strip(),
            "chain_snapshot_hash": None,
            "freshness_cert_hash": None,
        },
        "pointers": list(pointer_paths) if pointer_paths else [str(identity_dir.resolve())],
        "canonical_json_hash": None,
        "upstream_hash": canonical_hash_for_c2_artifact_v1(payload["binding_obj"]),
    }
    veto_obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto_obj)
    try:
        write_phased_veto_only_v1(
            submission_dir,
            veto_record=veto_obj,
            order_plan=payload["plan_obj"],
            binding_record=payload["binding_obj"],
            mapping_ledger_record=payload["mapping_obj"],
        )
        return (True, f"GOV_SUBMIT_SKIP_EVIDENCE_WRITTEN:{submission_id}")
    except EvidenceWriteError as exc:
        if "OUT_DIR_NOT_EMPTY" in str(exc):
            evidence_now = (
                (submission_dir / "broker_submission_record.v2.json").exists()
                or (submission_dir / "execution_event_record.v1.json").exists()
                or (submission_dir / "veto_record.v1.json").exists()
            )
            if evidence_now:
                return (True, f"GOV_SUBMIT_SKIP_EVIDENCE_ALREADY_PRESENT:{submission_id}")
        return (False, f"GOV_SUBMIT_SKIP_EVIDENCE_WRITE_FAILED:{submission_id}:{type(exc).__name__}")


def _build_governed_submit_cmd(
    *,
    execution_package_path: Path,
    submission_record_path: Path,
    day: str,
    produced_utc: str,
    ib_account: str,
    env: Dict[str, str],
) -> List[str]:
    submit_tool = str((REPO_ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py").resolve())
    risk_budget = str((REPO_ROOT / "constellation_2" / "phaseD" / "inputs" / "sample_risk_budget.v1.json").resolve())
    execution_profile = resolve_governed_paper_execution_profile(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    )
    dry_run = _resolve_governed_submit_dry_run(env)
    eval_time_utc = f"{day}T00:00:00Z"

    return [
        sys.executable,
        submit_tool,
        "--eval_time_utc",
        eval_time_utc,
        "--execution_package_path",
        str(execution_package_path.resolve()),
        "--submission_record_path",
        str(submission_record_path.resolve()),
        "--risk_budget",
        risk_budget,
        "--ib_host",
        execution_profile.host,
        "--ib_port",
        str(execution_profile.port),
        "--ib_client_id",
        str(execution_profile.client_id_orders),
        "--ib_account",
        ib_account,
        "--dry_run",
        dry_run,
    ]


def _resolve_governed_submit_dry_run(env: Dict[str, str]) -> str:
    dry_run = str(env.get("C2_GOVERNED_SUBMIT_DRY_RUN") or DEFAULT_GOVERNED_SUBMIT_DRY_RUN).strip().upper()
    if dry_run not in ("YES", "NO"):
        return DEFAULT_GOVERNED_SUBMIT_DRY_RUN
    return dry_run


def _run_governed_submit_stage(
    *,
    truth_root: Path,
    day: str,
    produced_utc: str,
    ib_account: str,
    env: Dict[str, str],
) -> Tuple[int, List[str]]:
    identity_dirs = _discover_same_day_identity_dirs(truth_root, day, ib_account)
    if not identity_dirs:
        return (1, ["GOV_SUBMIT_NO_SAME_DAY_IDENTITY_SET"])

    authorized_dirs: List[Path] = []
    reason_codes: List[str] = []

    for identity_dir in identity_dirs:
        intent_sha = _extract_identity_set_intent_sha(identity_dir, truth_root, day)
        if not intent_sha:
            reason_codes.append(f"GOV_SUBMIT_IDENTITY_SET_MISSING_INTENT_SHA:{identity_dir}")
            continue

        az = _read_authorization_state(truth_root, day, intent_sha)
        if not az["ok"]:
            reason_code = f"{az['reason_code']}:{identity_dir.name}:{intent_sha}"
            reason_codes.append(reason_code)
            emitted, emitted_code = _emit_governed_submit_skip_veto(
                truth_root=truth_root,
                day=day,
                eval_time_utc=f"{day}T00:00:00Z",
                identity_dir=identity_dir,
                reason_code=az["reason_code"],
                reason_detail=f"AUTHORIZATION_LOOKUP_FAILED:path={az['path']}",
                pointer_paths=[az["path"], str(identity_dir.resolve())],
            )
            if emitted:
                reason_codes.append(emitted_code)
            continue

        if az["status"] != "AUTHORIZED" or az["decision"] != "AUTHORIZED" or int(az["authorized_quantity"]) <= 0:
            reason_code = (
                f"GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED:{identity_dir.name}:{intent_sha}:"
                f"status={az['status']}:decision={az['decision']}:authorized_quantity={az['authorized_quantity']}"
            )
            reason_codes.append(reason_code)
            emitted, emitted_code = _emit_governed_submit_skip_veto(
                truth_root=truth_root,
                day=day,
                eval_time_utc=f"{day}T00:00:00Z",
                identity_dir=identity_dir,
                reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
                reason_detail=reason_code,
                pointer_paths=[az["path"], str(identity_dir.resolve())],
            )
            if emitted:
                reason_codes.append(emitted_code)
            continue
        authorized_dirs.append(identity_dir)

    if not authorized_dirs:
        reason_codes.append("GOV_SUBMIT_NO_AUTHORIZED_IDENTITY_SETS")
        return (1, reason_codes)

    require_real_broker_ids = _resolve_governed_submit_dry_run(env) == "NO"
    submit_failures: List[str] = []
    for identity_dir in authorized_dirs:
        intent_sha = _extract_identity_set_intent_sha(identity_dir, truth_root, day)
        if intent_sha and _has_submission_evidence_for_intent_sha(
            truth_root=truth_root,
            day=day,
            intent_sha=intent_sha,
            require_real_broker_ids=require_real_broker_ids,
        ):
            reason_codes.append(f"GOV_SUBMIT_IDEMPOTENT_ALREADY_SUBMITTED:{identity_dir.name}:{intent_sha}")
            continue
        submit_inputs = _ensure_governed_submit_inputs(
            truth_root=truth_root,
            day=day,
            identity_dir=identity_dir,
            env=env,
        )
        if not bool(submit_inputs.get("ok")):
            submit_failures.append(str(submit_inputs.get("reason_code") or f"GOV_SUBMIT_INPUT_PREP_FAILED:{identity_dir.name}"))
            continue
        rc = _run_cmd(
            "A7A_GOVERNED_SUBMIT_V5",
            _build_governed_submit_cmd(
                execution_package_path=Path(str(submit_inputs["execution_package_path"])).resolve(),
                submission_record_path=Path(str(submit_inputs["submission_record_path"])).resolve(),
                day=day,
                produced_utc=produced_utc,
                ib_account=ib_account,
                env=env,
            ),
            env=env,
        )
        if rc != 0:
            submit_failures.append(f"GOV_SUBMIT_WRAPPER_NONZERO_RC:{identity_dir.name}:rc={rc}")

    if submit_failures:
        return (1, submit_failures)

    sub_count = _count_broker_submission_records(truth_root, day)
    if sub_count <= 0:
        return (1, ["GOV_SUBMIT_NO_SUBMISSION_EVIDENCE"])

    return (0, [f"GOV_SUBMIT_SUBMISSION_COUNT={sub_count}"])


def _governed_abort_reason_codes(*, truth_root: Path, day: str, gov_reason_codes: List[str]) -> List[str]:
    codes = [str(x).strip() for x in gov_reason_codes if str(x).strip()]
    if codes != ["GOV_SUBMIT_NO_SAME_DAY_IDENTITY_SET"]:
        return []
    summary = _phasec_veto_only_summary(truth_root, day, ib_account)
    if not summary.get("present"):
        return []
    if int(summary.get("identity_dir_count") or 0) != 0:
        return []
    if int(summary.get("allow_count") or 0) != 0:
        return []
    veto_count = int(summary.get("veto_count") or 0)
    if veto_count <= 0:
        return []
    out = [
        "GOVERNED_ABORT_NO_IDENTITY",
        "PHASEC_VETO_ONLY",
        f"PHASEC_VETO_COUNT:{veto_count}",
    ]
    out.extend(summary.get("detail_codes") or [])
    return out


def _stage_required_for_mode(sd: StageDef, mode: str, has_activity: bool) -> Tuple[bool, bool]:
    base_required = bool(sd.required_for_paper) if mode == "PAPER" else bool(sd.required_for_live)
    if sd.required_if_activity and not has_activity:
        return (False, False)
    return (bool(base_required), bool(sd.blocking))


def _run_cmd(stage_id: str, cmd: List[str], env: Dict[str, str]) -> int:
    try:
        real_cmd = list(cmd)
        if real_cmd:
            exe = str(real_cmd[0]).strip()
            if exe == "python3":
                real_cmd[0] = sys.executable
            elif exe.endswith(".venv_c2/bin/python") or exe == ".venv_c2/bin/python":
                candidate = (REPO_ROOT / exe).resolve() if not Path(exe).is_absolute() else Path(exe).resolve()
                real_cmd[0] = str(candidate) if candidate.exists() else sys.executable
        return subprocess.call(real_cmd, env=env)
    except Exception as e:
        print(f"STAGE_EXCEPTION stage_id={stage_id} err={e!r}", file=sys.stderr)
        return 99


def _pointer_lock_acquire(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: pointer lock busy: {lock_path}")
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def _pointer_lock_release(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass


def _read_last_pointer_seq(idx_path: Path, mode: str) -> int:
    if not idx_path.exists():
        return 0
    last = 0
    for line in idx_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        obj = json.loads(s)
        if not isinstance(obj, dict):
            continue
        if str(obj.get("mode") or "").strip().upper() != mode:
            continue
        try:
            ps = int(obj.get("pointer_seq"))
        except Exception:
            continue
        if ps > last:
            last = ps
    return last


def _read_last_pointer_seq_unfiltered(idx_path: Path) -> int:
    if not idx_path.exists():
        return 0
    last = 0
    for line in idx_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        obj = json.loads(s)
        if not isinstance(obj, dict):
            continue
        try:
            ps = int(obj.get("pointer_seq"))
        except Exception:
            continue
        if ps > last:
            last = ps
    return last


def _atomic_append_jsonl(path: Path, obj: Dict[str, Any]) -> Tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    line_sha = _sha256_bytes(line)

    fd = os.open(str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)

    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return (line_sha, str(path))


def _read_status_upper(path: Path, key: str) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        o = _read_json_obj(path)
    except Exception:
        return ""
    return str(o.get(key) or "").strip().upper()


def _append_run_pointer_v1(
    *,
    truth_root: Path,
    day: str,
    mode: str,
    attempt_id: str,
    attempt_seq: int,
    cfg_hash: str,
    git_sha: str,
) -> Tuple[int, List[str]]:
    if not GATE_HIERARCHY_POLICY_PATH.exists() or not GATE_HIERARCHY_POLICY_PATH.is_file():
        return (1, [f"RUN_POINTER_POLICY_MISSING:{GATE_HIERARCHY_POLICY_PATH}"])

    hb_path = (truth_root / "reports" / "heartbeat_gate_v1" / day / "heartbeat_gate.v1.json").resolve()
    gs_path = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()
    if not gs_path.exists() or not gs_path.is_file():
        return (1, [f"RUN_POINTER_GATE_STACK_MISSING:{gs_path}"])

    hb_status = _read_status_upper(hb_path, "status")
    gs_status = _read_status_upper(gs_path, "status")
    kill_result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=day)
    ks_path = kill_result.canonical_path
    ks_state = kill_result.state if kill_result.status == KILL_SWITCH_STATUS_PASS else "FAIL_CLOSED"
    ok_authoritative = (hb_status == "PASS") and (gs_status == "PASS") and (kill_result.status == KILL_SWITCH_STATUS_PASS) and (ks_state == "INACTIVE")

    status = "PASS" if ok_authoritative else "FAIL"
    policy_hash = _sha256_file(GATE_HIERARCHY_POLICY_PATH)

    try:
        runptr_root = resolve_pointer_index_root_for_truth_root(truth_root, family="run_pointer_v1")
    except Exception:
        runptr_root = (truth_root / "run_pointer_v1").resolve()
    idx_path = (runptr_root / "canonical_pointer_index.v1.jsonl").resolve()
    lock_path = (runptr_root / ".canonical_pointer_index.v1.lock").resolve()

    lock_fd = _pointer_lock_acquire(lock_path)
    try:
        last_seq = _read_last_pointer_seq_unfiltered(idx_path)
        pointer_seq = last_seq + 1
        entry = {
            "schema_id": "C2_RUN_POINTER_CANONICAL_POINTER_INDEX_V1",
            "pointer_seq": int(pointer_seq),
            "day_utc": day,
            "attempt_id": attempt_id,
            "attempt_seq": int(attempt_seq),
            "mode": mode,
            "status": status,
            "authoritative": bool(ok_authoritative),
            "policy_hash": policy_hash,
            "orchestrator_config_hash": cfg_hash,
            "produced_utc": f"{day}T00:00:00Z",
            "producer_git_sha": git_sha,
            "points_to": str(gs_path),
        }
        line_sha, _ = _atomic_append_jsonl(idx_path, entry)
    finally:
        _pointer_lock_release(lock_fd, lock_path)

    return (
        0,
        [
            f"RUN_POINTER_SEQ={pointer_seq}",
            f"RUN_POINTER_STATUS={status}",
            f"RUN_POINTER_AUTHORITATIVE={'YES' if ok_authoritative else 'NO'}",
            f"RUN_POINTER_APPEND_LINE_SHA256={line_sha}",
        ],
    )


def _write_attempt_file(path: Path, data: bytes) -> Dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    cand_sha = _sha256_bytes(data)

    if path.exists():
        existing = path.read_bytes()
        ex_sha = _sha256_bytes(existing)
        if ex_sha == cand_sha:
            return {"ok": True, "action": "SKIP_IDENTICAL", "path": str(path), "sha256": cand_sha}
        raise SystemExit(
            f"FATAL: ATTEMPTED_REWRITE_ATTEMPT_SCOPED path={path} existing_sha={ex_sha} candidate_sha={cand_sha}"
        )

    tmp = path.parent / f".{path.name}.tmp.{os.getpid()}"
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))
    return {"ok": True, "action": "WROTE", "path": str(path), "sha256": cand_sha}


def _build_stage_defs(*, truth: Path, day: str, input_day: str, ib_account: str, git_sha: str, attempt_id: str) -> List[StageDef]:
    feed_gate_out = str(truth / "reports" / "feed_attestation_gate_v1" / day / "feed_attestation_gate.v1.json")
    liq_gate_out = str(truth / "reports" / "liquidity_slippage_gate_v1" / day / "liquidity_slippage_gate.v1.json")
    sys_gate_out = str(truth / "reports" / "systemic_risk_gate_v3" / day / "systemic_risk_gate.v3.json")
    cap_gate_out = str(truth / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json")
    op_daily_out = str(truth / "reports" / "operator_daily_gate_v3" / day / "operator_daily_gate.v3.json")
    hb_gate_out = str(truth / "reports" / "heartbeat_gate_v1" / day / "heartbeat_gate.v1.json")
    corr_gate_out = str(truth / "reports" / "correlation_envelope_gate_v1" / day / "correlation_envelope_gate.v1.json")
    replay_gate_out = str(truth / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.v1.json")
    gate_stack_out = str(truth / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json")
    kill_switch_out = str(truth / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json")
    broker_recon_out = str(truth / "reports" / "broker_reconciliation_v2" / day / "broker_reconciliation.v2.json")
    broker_recon_check_out = str(
        truth
        / "reports"
        / "orchestrator_run_verdict_v2"
        / day
        / attempt_id
        / "checks"
        / "A1_BROKER_RECONCILIATION_GATE_V2_CHECK"
        / "broker_reconciliation.v2.check.json"
    )
    exposure_net_out = str(truth / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json")
    execution_stream_day_dir = str(truth / "execution_stream_v1" / day)
    execution_stream_records_glob = str(truth / "execution_stream_v1" / day / "*.execution_event_stream_record.v1.json")
    exec_recon_out = str(truth / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json")
    fills_day_dir = str(truth / "fill_ledger_v1" / day)
    nav_out = str(truth / "accounting_v2" / "nav" / day / "nav.v2.json")
    nav_compat_out = str(truth / "accounting_compat_v1" / "nav" / day / "nav_snapshot.v1.json")
    exec_submissions_day_dir = str(truth / "execution_evidence_v1" / "submissions" / day)
    pos_v5_out = str(truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json")
    pos_out = str(truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json")
    cash_out = str(truth / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json")
    alloc_summary_out = str(truth / "allocation_v1" / "summary" / day / "summary.json")
    corr_matrix_out = str(truth / "monitoring_v1" / "engine_correlation_matrix" / day / "engine_correlation_matrix.v1.json")
    recon_report_v3_out = str(truth / "reports" / "reconciliation_report_v3" / day / "reconciliation_report.v3.json")
    exit_recon_out = str(truth / "exit_reconciliation_v1" / day / "exit_reconciliation.v1.json")
    op_stmt = str(
        REPO_ROOT / "constellation_2" / "operator_inputs" / "cash_ledger_operator_statements" / day / "operator_statement.v1.json"
    )
    governed_submit_tool = str((REPO_ROOT / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py").resolve())
    execution_truth_root = resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    ).execution_root_path

    all_stage_defs = [
        StageDef(
            stage_id="A0_ENFORCE_SINGLE_ACCOUNT_TOPOLOGY",
            cmd=["/bin/true"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0_POSITIONS_SNAPSHOT_V5",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
                "--ib_account",
                ib_account,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[pos_v5_out],
        ),
        StageDef(
            stage_id="B0B_POSITIONS_SNAPSHOT_V2_COMPAT",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[pos_out],
        ),
        StageDef(
            stage_id="B0_CASH_LEDGER_SNAPSHOT_V1",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
                "--day_utc",
                day,
                "--operator_statement_json",
                op_stmt,
                "--producer_repo",
                REPO_ROOT.name,
                "--producer_git_sha",
                git_sha,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[cash_out],
        ),
        StageDef(
            stage_id="B0_ENSURE_EXECUTION_SUBMISSIONS_DIR_V1",
            cmd=["/usr/bin/bash", "-lc", f"set -euo pipefail; mkdir -p '{exec_submissions_day_dir}'"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[exec_submissions_day_dir],
        ),
        StageDef(
            stage_id="A1_BROKER_RECONCILIATION_GATE_V2_CHECK",
            cmd=[
                "python3",
                "ops/tools/run_broker_reconciliation_day_v2.py",
                "--day_utc",
                day,
                "--ib_account",
                ib_account,
                "--mode",
                "CHECK",
                "--emit_check_artifact",
                "YES",
                "--check_artifact_path",
                broker_recon_check_out,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0_ACCOUNTING_NAV_V2",
            cmd=[
                "python3",
                "ops/tools/run_accounting_nav_v2_day_v1.py",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[nav_out],
        ),
        StageDef(
            stage_id="A3_LIQUIDITY_SLIPPAGE_GATE_V1",
            cmd=["python3", "ops/tools/run_liquidity_slippage_gate_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[liq_gate_out],
        ),
        StageDef(
            stage_id="A4_SYSTEMIC_RISK_GATE_V3",
            cmd=["python3", "ops/tools/run_systemic_risk_gate_v3.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[sys_gate_out],
        ),
        StageDef(
            stage_id="B0_ACCOUNTING_NAV_COMPAT_BRIDGE_V1",
            cmd=[
                "python3",
                "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[nav_compat_out],
        ),
        StageDef(
            stage_id="A4A_ALLOCATION_DAY_V2",
            cmd=[
                "python3",
                "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[alloc_summary_out],
        ),
        StageDef(
            stage_id="A5_CAPITAL_RISK_ENVELOPE_GATE_V2",
            cmd=[
                "python3",
                "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
                "--out_day_utc",
                day,
                "--input_day_utc",
                input_day,
                "--truth_root",
                str(truth),
                "--produced_utc",
                f"{day}T00:00:00Z",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[cap_gate_out],
        ),
        StageDef(
            stage_id="A5AA_ENGINE_CORRELATION_MATRIX_DAY_V1",
            cmd=[
                "python3",
                "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[corr_matrix_out],
        ),
        StageDef(
            stage_id="A5AAA_FEED_ATTESTATION_GATE_V1",
            cmd=[
                "python3",
                "ops/tools/run_feed_attestation_gate_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[feed_gate_out],
        ),
        StageDef(
            stage_id="A5AB_RECONCILIATION_REPORT_V3",
            cmd=[
                "python3",
                "ops/tools/run_reconciliation_report_v3.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[recon_report_v3_out],
        ),
        StageDef(
            stage_id="A5AC_EXIT_RECONCILIATION_DAY_V1",
            cmd=[
                "python3",
                "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[exit_recon_out],
        ),
        StageDef(
            stage_id="A5A_OPERATOR_DAILY_GATE_V3",
            cmd=[
                "python3",
                "ops/tools/run_operator_daily_gate_v3.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
                "--produced_utc",
                f"{day}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[op_daily_out],
        ),
        StageDef(
            stage_id="A5B_HEARTBEAT_GATE_V1",
            cmd=["python3", "ops/tools/run_heartbeat_gate_v1.py", "--day_utc", day, "--truth_root", str(truth)],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[hb_gate_out],
        ),
        StageDef(
            stage_id="A5C_CORRELATION_ENVELOPE_GATE_V1",
            cmd=[
                "python3",
                "ops/tools/run_correlation_envelope_gate_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
                "--produced_utc",
                f"{day}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[corr_gate_out],
        ),
        StageDef(
            stage_id="A5D_REPLAY_CERTIFICATION_GATE_V1",
            cmd=[
                "python3",
                "ops/tools/run_replay_certification_gate_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
                "--produced_utc",
                f"{day}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[replay_gate_out],
        ),
        StageDef(
            stage_id="A5X_GATE_COMPLETENESS_GATE_V1",
            cmd=["python3", "ops/tools/run_gate_completeness_gate_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[str(truth / "reports" / "gate_completeness_gate_v1" / day / "gate_completeness_gate.v1.json")],
        ),
        StageDef(
            stage_id="A6_GATE_STACK_VERDICT_V1",
            cmd=[
                "python3",
                "ops/tools/run_gate_stack_verdict_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
                "--produced_utc",
                f"{day}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A7_GLOBAL_KILL_SWITCH_V1",
            cmd=["python3", "ops/tools/run_global_kill_switch_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A6A_RUN_POINTER_APPEND_V1",
            cmd=["python3", "ops/tools/run_pointer_append_v1.py"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A6A_POINTER_HEADS_MATERIALIZE_V1",
            cmd=[
                "python3",
                "ops/tools/run_pointer_heads_materialize_v1.py",
                "--fail_if_no_authority_head",
                "YES",
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A6A_EXPOSURE_NET_DAY_V1",
            cmd=["python3", "ops/tools/run_exposure_net_day_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[exposure_net_out],
        ),
        StageDef(
            stage_id=SLEEVE_EDGE_PUBLICATION_STAGE_ID,
            cmd=[
                "python3",
                "ops/tools/run_sleeve_edge_measurement_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id=GOVERNED_EVALUATION_STAGE_ID,
            cmd=_governed_evaluation_stage_cmd(day=day, truth=truth),
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id=CAPITAL_AUTHORITY_STAGE_ID,
            cmd=[
                "python3",
                "ops/tools/run_capital_authority_allocation_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
                "--canonical_sequence_owner",
                "ops/tools/run_c2_paper_day_orchestrator_v2.py",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[
                str(truth / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json")
            ],
        ),
        StageDef(
            stage_id="A6B_AUTHORIZATION_ARTIFACTS_DAY_V1",
            cmd=[
                "python3",
                "ops/tools/run_authorization_artifacts_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A6C_OPTIONS_CHAIN_CAPTURE_IB_DAY_V1",
            cmd=[
                "python3",
                "ops/tools/run_options_chain_capture_ib_day_v1.py",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_DAY_V1",
            cmd=[
                "python3",
                "ops/tools/run_options_chain_truth_promotion_day_v1.py",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A6E_MARKET_DATA_SNAPSHOT_REFRESH_V1",
            cmd=[
                "python3",
                "ops/tools/run_market_data_snapshot_day_v1.py",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A7_PHASEC_IDENTITY_MATERIALIZER_DAY_V1",
            cmd=[
                "python3",
                "ops/tools/run_phasec_identity_materializer_day_v1.py",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A7A_GOVERNED_SUBMIT_V5",
            cmd=[governed_submit_tool],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0A_EXECUTION_EVIDENCE_TRUTH_V1",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.execution_evidence.run.run_execution_evidence_truth_day_v1",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
                "--truth_root",
                str(truth),
                "--source_truth_root",
                str(execution_truth_root),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0_EXECUTION_STREAM_SNAPSHOT_V1",
            cmd=[
                "python3",
                "ops/tools/run_execution_stream_snapshot_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[execution_stream_records_glob],
        ),
        StageDef(
            stage_id="B0AA_ORPHAN_SUBMISSION_BACKFILL_SLEEVE_V1",
            cmd=[
                "python3",
                "ops/tools/run_orphan_submission_backfill_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(execution_truth_root),
                "--stream_truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0AB_ORPHAN_SUBMISSION_BACKFILL_CANONICAL_V1",
            cmd=[
                "python3",
                "ops/tools/run_orphan_submission_backfill_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
                "--stream_truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1",
            cmd=["python3", "ops/tools/run_submission_lifecycle_refresh_v1.py"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B1_FILL_LEDGER_V1",
            cmd=[
                "python3",
                "ops/tools/run_fill_ledger_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0B_POSITIONS_SNAPSHOT_V4",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v4",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0C_POSITIONS_EFFECTIVE_POINTER_V1",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_effective_pointer_day_v1",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B2_EXECUTION_RECONCILIATION_V1",
            cmd=[
                "python3",
                "ops/tools/run_execution_reconciliation_day_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B3_LIFECYCLE_MONITOR_V1",
            cmd=[
                "python3",
                "ops/tools/run_lifecycle_monitor_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=False,
            required_for_live=False,
            required_if_activity=False,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B4_PAPER_READINESS_MONITOR_V2",
            cmd=[
                "python3",
                "ops/tools/run_paper_readiness_monitor_v2.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth),
            ],
            required_for_paper=False,
            required_for_live=False,
            required_if_activity=False,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B5_SLEEVE_LIVE_READINESS_V1",
            cmd=[
                "python3",
                "ops/tools/run_sleeve_live_readiness_v1.py",
                "--day_utc",
                day,
                "--sleeve_id",
                "PRIMARY",
                "--mode",
                "PAPER",
                "--truth_root",
                str(truth),
            ],
            required_for_paper=False,
            required_for_live=False,
            required_if_activity=False,
            blocking=False,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="A8_EXECUTION_READINESS_GATE_V1",
            cmd=["python3", "ops/tools/run_execution_readiness_gate_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=False,
            skip_if_exists_paths=[str(truth / "reports" / "execution_readiness_gate_v1" / day / "execution_readiness_gate.v1.json")],
        ),
    ]
    return [stage for stage in all_stage_defs if stage.stage_id in ADMITTED_EXECUTION_STAGE_IDS]


def _publish_pipeline_manifest_v3(
    *,
    day: str,
    mode: str,
    attempt_id: str,
    attempt_seq: int,
    attempt_manifest_path: Path,
    env: Dict[str, str],
) -> Tuple[bool, int]:
    cmd = [
        "python3",
        "ops/tools/run_pipeline_manifest_v3_mode_aware.py",
        "--day_utc",
        day,
        "--mode",
        mode,
        "--attempt_id",
        attempt_id,
        "--attempt_seq",
        str(int(attempt_seq)),
        "--attempt_manifest_path",
        str(attempt_manifest_path),
    ]
    rc = _run_cmd("PUBLISH_PIPELINE_MANIFEST_V3", cmd, env=env)
    return (rc == 0, int(rc))


def _publish_pipeline_manifest_v2_compat(*, day: str, attempt_manifest_path: Path, truth_root: Path, env: Dict[str, str]) -> Tuple[bool, int]:
    cmd = [
        "python3",
        "ops/tools/run_pipeline_manifest_v2_compat_from_attempt_v1.py",
        "--day_utc",
        day,
        "--attempt_manifest_path",
        str(attempt_manifest_path),
        "--truth_root",
        str(truth_root),
    ]
    rc = _run_cmd("PUBLISH_PIPELINE_MANIFEST_V2_COMPAT", cmd, env=env)
    return (rc == 0, int(rc))


def _publish_pipeline_manifest_v1_compat(*, day: str, attempt_manifest_path: Path, truth_root: Path, env: Dict[str, str]) -> Tuple[bool, int]:
    cmd = [
        "python3",
        "ops/tools/run_pipeline_manifest_v1_compat_from_attempt_v1.py",
        "--day_utc",
        day,
        "--attempt_manifest_path",
        str(attempt_manifest_path),
        "--truth_root",
        str(truth_root),
    ]
    rc = _run_cmd("PUBLISH_PIPELINE_MANIFEST_V1_COMPAT", cmd, env=env)
    return (rc == 0, int(rc))


def _enforce_gate_stack_verdict(*, truth_root: Path, day: str, current_status: str, reason_codes: List[str]) -> str:
    p = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()

    if not p.exists():
        if "GATE_STACK_VERDICT_MISSING" not in reason_codes:
            reason_codes.append("GATE_STACK_VERDICT_MISSING")
        return "FAIL" if current_status != "ABORTED" else current_status

    try:
        o = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        if "GATE_STACK_VERDICT_PARSE_ERROR" not in reason_codes:
            reason_codes.append("GATE_STACK_VERDICT_PARSE_ERROR")
        return "ABORTED"

    status = str(o.get("status") or "").strip().upper()
    blocking = str(o.get("blocking_class") or "").strip().upper()

    if blocking == "CLASS1_SYSTEM_HARD_STOP":
        if "CLASS1_SYSTEM_HARD_STOP" not in reason_codes:
            reason_codes.append("CLASS1_SYSTEM_HARD_STOP")
        return "ABORTED"

    if status != "PASS":
        if "GATE_STACK_VERDICT_NOT_PASS" not in reason_codes:
            reason_codes.append("GATE_STACK_VERDICT_NOT_PASS")
        return "FAIL" if current_status != "ABORTED" else current_status

    return current_status


def main() -> int:
    from constellation_2.common.paper_day_orchestrator_pipeline_v1 import (
        run_paper_day_orchestrator_obligation_pipeline_v1,
    )

    ap = argparse.ArgumentParser(prog="run_c2_paper_day_orchestrator_v2")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--input_day_utc", default="")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--ib_account", required=True)
    ap.add_argument("--produced_utc", required=True)
    ap.add_argument("--truth_root", default=None)
    ap.add_argument("--paper_session_ledger_path", required=True)
    ap.add_argument(
        "--pipeline_mode",
        default="normal",
        choices=["normal", "exact_ref_replay", "bounded_recompute", "forensic_replay"],
    )
    ap.add_argument("--replay_attempt_manifest_path", default="")
    ap.add_argument("--budget_profile", default="contract_default")
    ap.add_argument("--emit_pipeline_report", action="store_true")
    args = ap.parse_args()

    report = run_paper_day_orchestrator_obligation_pipeline_v1(args)
    if args.emit_pipeline_report:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    return int(report["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
