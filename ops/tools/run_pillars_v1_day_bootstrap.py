#!/usr/bin/env python3
"""
run_pillars_v1_day_bootstrap.py

C2 Pillars v1 — bootstrap writer (compile-from-existing truth).

Writes (immutable, day-keyed):
- constellation_2/runtime/truth/pillars_v1/<DAY>/inputs_frozen.v1.json
- constellation_2/runtime/truth/pillars_v1/<DAY>/event_ledger.v1.jsonl
- constellation_2/runtime/truth/pillars_v1/<DAY>/decisions/<DECISION_ID>.submission_decision_record.v1.json
- constellation_2/runtime/truth/pillars_v1/<DAY>/daily_execution_state.v1.json
- constellation_2/runtime/truth/pillars_v1/<DAY>/day_root_anchor.v1.json
- constellation_2/runtime/truth/pillars_v1/<DAY>/bundles/{determinism,model_governance,execution_quality}_bundle.v1.json

Fail-closed:
- refuses future day (C2_TEST_DAY_QUARANTINE_POLICY_V1 via enforce_operational_day_key_invariant_v1)
- refuses template day keys via strict numeric day parser
- refuses missing required upstream inputs (writes nothing if cannot prove inputs)

Runnability:
  cd /home/node/constellation_2_runtime
  python3 ops/tools/run_pillars_v1_day_bootstrap.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

# --- Import bootstrap (audit-grade, deterministic, fail-closed) ---
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
# -------------------------------------------------------------------

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.enforce_operational_day_invariant_v1 import enforce_operational_day_key_invariant_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = _REPO_ROOT_FROM_FILE
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

PILLARS_ROOT = (TRUTH / "pillars_v1").resolve()

SCHEMA_INPUTS_FROZEN = "governance/04_DATA/SCHEMAS/C2/PILLARS/inputs_frozen.v1.schema.json"
SCHEMA_DECISION = "governance/04_DATA/SCHEMAS/C2/PILLARS/submission_decision_record.v1.schema.json"
SCHEMA_DAILY_STATE = "governance/04_DATA/SCHEMAS/C2/PILLARS/daily_execution_state.v1.schema.json"
SCHEMA_DAY_ANCHOR = "governance/04_DATA/SCHEMAS/C2/PILLARS/day_root_anchor.v1.schema.json"
SCHEMA_BUNDLE_DET = "governance/04_DATA/SCHEMAS/C2/PILLARS/determinism_bundle.v1.schema.json"
SCHEMA_BUNDLE_MODEL = "governance/04_DATA/SCHEMAS/C2/PILLARS/model_governance_bundle.v1.schema.json"
SCHEMA_BUNDLE_EXEC = "governance/04_DATA/SCHEMAS/C2/PILLARS/execution_quality_bundle.v1.schema.json"

# upstream paths we compile from
PATH_POS_V3 = TRUTH / "positions_v1/snapshots"
PATH_POS_V2 = TRUTH / "positions_v1/snapshots"
PATH_NAV = TRUTH / "accounting_v1/nav"
PATH_ALLOC = TRUTH / "allocation_v1/summary"
PATH_RISK = TRUTH / "risk_v1/engine_budget"
PATH_CAP = TRUTH / "reports/capital_risk_envelope_v1"
PATH_REGIME = TRUTH / "monitoring_v1/regime_snapshot_v2"
PATH_PIPE = TRUTH / "reports/pipeline_manifest_v1"
PATH_OPVER = TRUTH / "reports/operator_gate_verdict_v1"

PATH_EXEC_MANIFESTS = TRUTH / "execution_evidence_v1/manifests"
PATH_BROKER_LOG = TRUTH / "execution_evidence_v1/broker_events"

REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
REG_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json"


def _parse_day_utc_strict(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    if (not d[0:4].isdigit()) or (not d[5:7].isdigit()) or (not d[8:10].isdigit()):
        raise ValueError(f"BAD_DAY_UTC_NOT_NUMERIC_YYYY_MM_DD: {d!r}")
    return d


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canon(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _self_sha(obj: Dict[str, Any], field: str) -> str:
    tmp = dict(obj)
    tmp[field] = None
    return _sha256_bytes(_canon(tmp))


def _must_exist_file(p: Path, code: str) -> None:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: {code}: {p}")


def _pick_positions(day: str) -> Path:
    p3 = (PATH_POS_V3 / day / "positions_snapshot.v3.json").resolve()
    if p3.exists():
        return p3
    p2 = (PATH_POS_V2 / day / "positions_snapshot.v2.json").resolve()
    if p2.exists():
        return p2
    raise SystemExit(f"FAIL: MISSING_POSITIONS_SNAPSHOT_V2_OR_V3 day={day}")


def _read_json_obj(p: Path) -> Dict[str, Any]:
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: JSON_NOT_OBJECT: {p}")
    return obj


def _write_json_immutable(path: Path, obj: Dict[str, Any]) -> Tuple[str, str]:
    payload = _canon(obj)
    try:
        wr = write_file_immutable_v1(path=path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e
    return (wr.path, wr.sha256)


def _write_bytes_immutable(path: Path, b: bytes) -> Tuple[str, str]:
    try:
        wr = write_file_immutable_v1(path=path, data=b, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e
    return (wr.path, wr.sha256)


def _load_engine_registry() -> Tuple[Dict[str, Any], str]:
    _must_exist_file(REG_PATH, "MISSING_ENGINE_MODEL_REGISTRY")
    reg_sha = _sha256_file(REG_PATH)
    reg = _read_json_obj(REG_PATH)
    validate_against_repo_schema_v1(reg, REPO_ROOT, REG_SCHEMA_RELPATH)
    return (reg, reg_sha)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pillars_v1_day_bootstrap")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc_strict(args.day_utc)

    # policy enforcement: refuse future-day operational truth writes
    enforce_operational_day_key_invariant_v1(day)

    git_sha = _git_sha()
    produced_utc = f"{day}T23:59:59Z"

    out_day_dir = (PILLARS_ROOT / day).resolve()
    out_decisions = (out_day_dir / "decisions").resolve()
    out_bundles = (out_day_dir / "bundles").resolve()

    # --- Resolve required upstream inputs (fail-closed) ---
    pos_path = _pick_positions(day)
    nav_path = (PATH_NAV / day / "nav.json").resolve()
    alloc_path = (PATH_ALLOC / day / "summary.json").resolve()
    risk_path = (PATH_RISK / day / "engine_risk_budget_ledger.v1.json").resolve()
    cap_path = (PATH_CAP / day / "capital_risk_envelope.v1.json").resolve()
    regime_path = (PATH_REGIME / day / "regime_snapshot.v2.json").resolve()
    pipe_path = (PATH_PIPE / day / "pipeline_manifest.v1.json").resolve()
    opver_path = (PATH_OPVER / day / "operator_gate_verdict.v1.json").resolve()

    for p, code in [
        (pos_path, "MISSING_POSITIONS_SNAPSHOT"),
        (nav_path, "MISSING_ACCOUNTING_NAV"),
        (alloc_path, "MISSING_ALLOCATION_SUMMARY"),
        (risk_path, "MISSING_ENGINE_RISK_BUDGET_LEDGER"),
        (cap_path, "MISSING_CAPITAL_RISK_ENVELOPE"),
        (regime_path, "MISSING_REGIME_SNAPSHOT_V2"),
        (pipe_path, "MISSING_PIPELINE_MANIFEST"),
        (opver_path, "MISSING_OPERATOR_GATE_VERDICT"),
    ]:
        _must_exist_file(p, code)

    # --- inputs_frozen ---
    inputs_manifest: List[Dict[str, str]] = []

    def add_input(t: str, p: Path) -> None:
        inputs_manifest.append({"type": t, "path": str(p), "sha256": _sha256_file(p)})

    add_input("positions_snapshot", pos_path)
    add_input("accounting_nav", nav_path)
    add_input("allocation_summary", alloc_path)
    add_input("engine_risk_budget_ledger_v1", risk_path)
    add_input("capital_risk_envelope_v1", cap_path)
    add_input("regime_snapshot_v2", regime_path)
    add_input("pipeline_manifest_v1", pipe_path)
    add_input("operator_gate_verdict_v1", opver_path)

    inputs_obj: Dict[str, Any] = {
        "schema_id": "inputs_frozen",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
        "status": "OK",
        "reason_codes": [],
        "input_manifest": inputs_manifest,
        "inputs_frozen_sha256": None,
    }
    inputs_obj["inputs_frozen_sha256"] = _self_sha(inputs_obj, "inputs_frozen_sha256")
    validate_against_repo_schema_v1(inputs_obj, REPO_ROOT, SCHEMA_INPUTS_FROZEN)
    inputs_path = (out_day_dir / "inputs_frozen.v1.json").resolve()
    inputs_written_path, inputs_sha = _write_json_immutable(inputs_path, inputs_obj)

    # --- decision records (compiled from execution evidence manifests for that day) ---
    decisions_written: List[Tuple[str, str]] = []
    decision_record_sha256s: List[str] = []

    manifests_day = (PATH_EXEC_MANIFESTS / day).resolve()
    if manifests_day.exists() and manifests_day.is_dir():
        for mf in sorted([p for p in manifests_day.iterdir() if p.is_file() and p.name.endswith(".manifest.json")], key=lambda p: p.name):
            submission_id = mf.name.split(".manifest.json", 1)[0].strip()
            if len(submission_id) != 64:
                continue
            msha = _sha256_file(mf)
            dec_obj: Dict[str, Any] = {
                "schema_id": "submission_decision_record",
                "schema_version": "v1",
                "day_utc": day,
                "decision_id": submission_id,
                "produced_utc": produced_utc,
                "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
                "status": "OK",
                "decision": {"disposition": "ALLOW", "detail": None},
                "reason_codes": ["BOOTSTRAP_FROM_EXECUTION_EVIDENCE_MANIFEST_V1"],
                "input_manifest": [{"type": "execution_evidence_submission_manifest", "path": str(mf), "sha256": msha}],
                "decision_sha256": None,
            }
            dec_obj["decision_sha256"] = _self_sha(dec_obj, "decision_sha256")
            validate_against_repo_schema_v1(dec_obj, REPO_ROOT, SCHEMA_DECISION)
            out_dec_path = (out_decisions / f"{submission_id}.submission_decision_record.v1.json").resolve()
            p_written, p_sha = _write_json_immutable(out_dec_path, dec_obj)
            decisions_written.append((p_written, p_sha))
            decision_record_sha256s.append(p_sha)

    decision_count = len(decisions_written)

    # --- event ledger (minimal bootstrap) ---
    ledger_lines: List[bytes] = []
    for p_written, p_sha in decisions_written:
        ev = {
            "event_id": _sha256_bytes((day + "|" + p_written + "|" + p_sha).encode("utf-8")),
            "event_type": "submission_decision_written",
            "day_utc": day,
            "event_time_utc": produced_utc,
            "ref_path": p_written,
            "ref_sha256": p_sha,
        }
        ledger_lines.append(_canon(ev))
    ledger_bytes = b"".join(ledger_lines)
    ledger_path = (out_day_dir / "event_ledger.v1.jsonl").resolve()
    ledger_written_path, ledger_sha = _write_bytes_immutable(ledger_path, ledger_bytes)

    # --- daily execution state ---
    daily_obj: Dict[str, Any] = {
        "schema_id": "daily_execution_state",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
        "status": "OK",
        "reason_codes": [],
        "input_manifest": [
            {"type": "inputs_frozen_v1", "path": str(inputs_written_path), "sha256": inputs_sha},
            {"type": "event_ledger_v1", "path": str(ledger_written_path), "sha256": ledger_sha},
        ],
        "summary": {
            "positions_snapshot": {"path": str(pos_path), "sha256": _sha256_file(pos_path)},
            "accounting_nav": {"path": str(nav_path), "sha256": _sha256_file(nav_path)},
            "allocation_summary": {"path": str(alloc_path), "sha256": _sha256_file(alloc_path)},
            "engine_risk_budget_ledger": {"path": str(risk_path), "sha256": _sha256_file(risk_path)},
            "capital_risk_envelope": {"path": str(cap_path), "sha256": _sha256_file(cap_path)},
            "regime_snapshot_v2": {"path": str(regime_path), "sha256": _sha256_file(regime_path)},
            "pipeline_manifest": {"path": str(pipe_path), "sha256": _sha256_file(pipe_path)},
            "operator_gate_verdict": {"path": str(opver_path), "sha256": _sha256_file(opver_path)},
            "decision_count": int(decision_count),
        },
        "daily_state_sha256": None,
    }
    daily_obj["daily_state_sha256"] = _self_sha(daily_obj, "daily_state_sha256")
    validate_against_repo_schema_v1(daily_obj, REPO_ROOT, SCHEMA_DAILY_STATE)
    daily_path = (out_day_dir / "daily_execution_state.v1.json").resolve()
    daily_written_path, daily_sha = _write_json_immutable(daily_path, daily_obj)

    # --- bundles ---
    det_obj: Dict[str, Any] = {
        "schema_id": "determinism_bundle",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
        "status": "DEGRADED",
        "reason_codes": ["REPLAY_PROOF_NOT_IMPLEMENTED_V1"],
        "input_manifest": [
            {"type": "inputs_frozen_v1", "path": str(inputs_written_path), "sha256": inputs_sha},
            {"type": "daily_execution_state_v1", "path": str(daily_written_path), "sha256": daily_sha},
            {"type": "event_ledger_v1", "path": str(ledger_written_path), "sha256": ledger_sha},
        ],
        "anchors": {
            "inputs_frozen_sha256": inputs_sha,
            "daily_execution_state_sha256": daily_sha,
            "event_ledger_sha256": ledger_sha,
            "replay_proof_mode": "NOT_IMPLEMENTED",
            "replay_output_sha256": None,
        },
    }
    validate_against_repo_schema_v1(det_obj, REPO_ROOT, SCHEMA_BUNDLE_DET)
    det_path = (out_bundles / "determinism_bundle.v1.json").resolve()
    _write_json_immutable(det_path, det_obj)

    reg, reg_sha = _load_engine_registry()
    engines = reg.get("engines") or []
    engine_items: List[Dict[str, Any]] = []
    mg_inputs: List[Dict[str, str]] = [{"type": "engine_model_registry_v1", "path": str(REG_PATH), "sha256": reg_sha}]
    for e in engines:
        engine_id = str(e.get("engine_id") or "").strip()
        activation = str(e.get("activation_status") or "").strip()
        runner_rel = str(e.get("engine_runner_path") or "").strip()
        runner_path = (REPO_ROOT / runner_rel).resolve()
        runner_sha = _sha256_file(runner_path) if runner_path.exists() else ("0" * 64)
        mg_inputs.append({"type": f"engine_runner:{engine_id}", "path": str(runner_path), "sha256": runner_sha})
        engine_items.append(
            {
                "engine_id": engine_id,
                "activation_status": activation,
                "runner_path": str(runner_path),
                "runner_sha256_actual": runner_sha,
                "ok": None,
                "reason_codes": [],
            }
        )

    mg_obj: Dict[str, Any] = {
        "schema_id": "model_governance_bundle",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
        "status": "OK",
        "reason_codes": ["BOOTSTRAP_FROM_ENGINE_MODEL_REGISTRY_V1"],
        "input_manifest": mg_inputs,
        "engines": engine_items,
    }
    validate_against_repo_schema_v1(mg_obj, REPO_ROOT, SCHEMA_BUNDLE_MODEL)
    mg_path = (out_bundles / "model_governance_bundle.v1.json").resolve()
    _write_json_immutable(mg_path, mg_obj)

    broker_log = (PATH_BROKER_LOG / day / "broker_event_log.v1.jsonl").resolve()
    broker_count = 0
    eq_inputs: List[Dict[str, str]] = []
    if broker_log.exists():
        broker_sha = _sha256_file(broker_log)
        try:
            broker_count = len([ln for ln in broker_log.read_text(encoding="utf-8").splitlines() if ln.strip() != ""])
        except Exception:
            broker_count = 0
        eq_inputs.append({"type": "broker_event_log_v1_jsonl", "path": str(broker_log), "sha256": broker_sha})
        eq_status = "OK"
        eq_reason_codes: List[str] = []
        eq_manifest = eq_inputs
        eq_notes = ["latency metrics not implemented in v1 bootstrap"]
    else:
        eq_status = "DEGRADED"
        eq_reason_codes = ["BROKER_EVENT_LOG_MISSING_V1"]
        eq_manifest = [{"type": "broker_event_log_missing", "path": str(broker_log), "sha256": _sha256_bytes(b"")}]
        eq_notes = ["no broker log; metrics limited"]

    eq_obj: Dict[str, Any] = {
        "schema_id": "execution_quality_bundle",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
        "status": eq_status,
        "reason_codes": eq_reason_codes,
        "input_manifest": eq_manifest,
        "metrics": {
            "submissions_total": int(decision_count),
            "broker_events_total": int(broker_count),
            "notes": eq_notes,
        },
    }
    validate_against_repo_schema_v1(eq_obj, REPO_ROOT, SCHEMA_BUNDLE_EXEC)
    eq_path = (out_bundles / "execution_quality_bundle.v1.json").resolve()
    _write_json_immutable(eq_path, eq_obj)

    anchor_obj: Dict[str, Any] = {
        "schema_id": "day_root_anchor",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pillars_v1_day_bootstrap.py", "git_sha": git_sha},
        "status": "OK",
        "reason_codes": [],
        "anchors": {
            "inputs_frozen_sha256": inputs_sha,
            "event_ledger_sha256": ledger_sha,
            "daily_execution_state_sha256": daily_sha,
            "decision_record_sha256s": sorted(decision_record_sha256s),
        },
        "anchor_sha256": None,
    }
    anchor_obj["anchor_sha256"] = _self_sha(anchor_obj, "anchor_sha256")
    validate_against_repo_schema_v1(anchor_obj, REPO_ROOT, SCHEMA_DAY_ANCHOR)
    anchor_path = (out_day_dir / "day_root_anchor.v1.json").resolve()
    _write_json_immutable(anchor_path, anchor_obj)

    print(f"OK: PILLARS_V1_BOOTSTRAP_WRITTEN day_utc={day} pillars_dir={out_day_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
