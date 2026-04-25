from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Mapping

from constellation_2.common.bounded_obligation_pipeline_v1 import (
    PipelineBlockedError,
    blocked_pipeline_report_v1,
    execute_bounded_obligation_pipeline_v1,
)
from constellation_2.common.paper_session_ledger_v1 import (
    assert_paper_session_ledger_granted_v1,
    assert_paper_session_ledger_open_ready_v1,
)


PIPELINE_ID = "paper_day_orchestrator_v2"
TARGET_PATH_FAMILY = "ops/tools/run_c2_paper_day_orchestrator_v2.py"


def _module():
    import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_tool

    return orchestrator_tool


def _require_replay_manifest(path_text: str) -> Path:
    path = Path(str(path_text or "")).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise PipelineBlockedError(f"REPLAY_ATTEMPT_MANIFEST_MISSING:{path}")
    return path


def _manifest_governing_refs(manifest_path: Path, verdict_path: Path | None) -> list[dict[str, str]]:
    rows = [
        {
            "artifact_id": "orchestrator_attempt_manifest_v2",
            "artifact_path": str(manifest_path.resolve()),
            "artifact_sha256": "",
        }
    ]
    if verdict_path is not None and verdict_path.exists():
        rows.append(
            {
                "artifact_id": "orchestrator_run_verdict_v2",
                "artifact_path": str(verdict_path.resolve()),
                "artifact_sha256": "",
            }
        )
    return rows


def resolve_paper_day_orchestrator_pipeline_inputs_v1(args: Any) -> Dict[str, Any]:
    orch = _module()
    orch._require_repo_root_cwd()

    pipeline_mode = str(getattr(args, "pipeline_mode", "") or "normal").strip()
    truth_root = orch._resolve_truth_root_or_default(getattr(args, "truth_root", None))
    verdict_root = (truth_root / "reports" / "orchestrator_run_verdict_v2").resolve()

    if pipeline_mode == "forensic_replay":
        raise PipelineBlockedError("PIPELINE_MODE_UNSUPPORTED:paper_day_orchestrator_v2:forensic_replay")

    if pipeline_mode in {"exact_ref_replay", "bounded_recompute"}:
        manifest_path = _require_replay_manifest(getattr(args, "replay_attempt_manifest_path", None))
        manifest_payload = orch._read_json_obj(manifest_path)
        day = orch._require_day(str(manifest_payload.get("day_utc") or getattr(args, "day_utc", "")))
        if str(getattr(args, "day_utc", "")).strip() and str(getattr(args, "day_utc")).strip() != day:
            raise PipelineBlockedError(f"REPLAY_DAY_MISMATCH:{getattr(args, 'day_utc')}:{day}")
        attempt_id = str(manifest_payload.get("attempt_id") or "").strip()
        attempt_seq = int(manifest_payload.get("attempt_seq") or 0)
        mode = orch._require_mode(str(manifest_payload.get("mode") or getattr(args, "mode", "")))
        stage_env = dict(os.environ)
        stage_env["PYTHONPATH"] = str(orch.REPO_ROOT)
        stage_env["C2_TRUTH_ROOT"] = str(truth_root)
        stage_env["C2_AUTHORITY_MODE"] = "governance_primary"
        verdict_path = (manifest_path.parent / "orchestrator_run_verdict.v2.json").resolve()
        return {
            "pipeline_mode": pipeline_mode,
            "truth_root": truth_root,
            "verdict_root": verdict_root,
            "day": day,
            "mode": mode,
            "attempt_id": attempt_id,
            "attempt_seq": attempt_seq,
            "attempt_manifest_path": manifest_path,
            "attempt_manifest": manifest_payload,
            "verdict_path": verdict_path if verdict_path.exists() else None,
            "stage_env": stage_env,
            "governing_refs": _manifest_governing_refs(manifest_path, verdict_path if verdict_path.exists() else None),
        }

    day = orch._require_day(getattr(args, "day_utc"))
    input_day = orch._require_day((getattr(args, "input_day_utc", "") or "").strip() or day)
    mode = orch._require_mode(getattr(args, "mode"))
    symbol = orch._require_symbol(getattr(args, "symbol"))
    produced_utc = orch._validate_produced_utc_isoz(getattr(args, "produced_utc"))
    if day != input_day:
        raise PipelineBlockedError(f"FUTURE_DAY_OR_SPLIT_DAY_RUN_NOT_ALLOWED:{day}:{input_day}")
    ib_account = str(getattr(args, "ib_account", "") or "").strip()
    if not ib_account:
        raise PipelineBlockedError("ORCHESTRATOR_IB_ACCOUNT_REQUIRED")

    paper_session_ledger_path = Path(str(getattr(args, "paper_session_ledger_path", ""))).resolve()
    if mode == "PAPER":
        ledger = assert_paper_session_ledger_open_ready_v1(path=paper_session_ledger_path, day_utc=day)
    else:
        ledger = assert_paper_session_ledger_granted_v1(path=paper_session_ledger_path, day_utc=day)
    git_sha = orch._git_sha()

    sleeve_id, expected_ib_account = orch._resolve_expected_sleeve_account(truth_root=truth_root, mode=mode)
    if ib_account != expected_ib_account:
        attempt_id = f"{day}__{produced_utc}__{git_sha}__{mode}__{symbol}__{ib_account}"
        return {
            "pipeline_mode": pipeline_mode,
            "truth_root": truth_root,
            "verdict_root": verdict_root,
            "day": day,
            "input_day": input_day,
            "mode": mode,
            "symbol": symbol,
            "ib_account": ib_account,
            "produced_utc": produced_utc,
            "paper_session_ledger_path": paper_session_ledger_path,
            "ledger": ledger,
            "git_sha": git_sha,
            "account_binding_failure": {
                "sleeve_id": sleeve_id,
                "expected_ib_account": expected_ib_account,
            },
            "attempt_id": attempt_id,
            "governing_refs": [],
        }

    cfg_hash = (os.environ.get("C2_ORCHESTRATOR_CONFIG_HASH") or "").strip().lower()
    if len(cfg_hash) != 64 or any(c not in "0123456789abcdef" for c in cfg_hash):
        cfg_hash = "0" * 64

    alloc_raw = orch.subprocess.check_output(
        [
            "python3",
            "ops/tools/run_pointer_attempt_alloc_v1.py",
            "--day_utc",
            day,
            "--mode",
            mode,
            "--orchestrator_config_hash",
            cfg_hash,
            "--git_sha",
            git_sha,
            "--truth_root",
            str(truth_root),
        ],
        text=True,
    ).strip()
    alloc = json.loads(alloc_raw)
    attempt_id = str(alloc.get("attempt_id") or "").strip()
    attempt_seq = int(alloc.get("attempt_seq") or 0)
    if not attempt_id or attempt_seq <= 0:
        raise PipelineBlockedError(f"RUN_POINTER_ATTEMPT_ALLOC_INVALID:{alloc_raw}")

    session_info = orch._resolve_session_state(day)
    stage_env = dict(os.environ)
    stage_env["PYTHONPATH"] = str(orch.REPO_ROOT)
    stage_env["C2_TRUTH_ROOT"] = str(truth_root)
    stage_env["C2_AUTHORITY_MODE"] = "governance_primary"
    stage_env["C2_PRODUCED_UTC"] = produced_utc

    return {
        "pipeline_mode": pipeline_mode,
        "truth_root": truth_root,
        "verdict_root": verdict_root,
        "paper_session_ledger_path": paper_session_ledger_path,
        "ledger": ledger,
        "day": day,
        "input_day": input_day,
        "mode": mode,
        "symbol": symbol,
        "ib_account": ib_account,
        "produced_utc": produced_utc,
        "git_sha": git_sha,
        "cfg_hash": cfg_hash,
        "attempt_id": attempt_id,
        "attempt_seq": attempt_seq,
        "session_info": session_info,
        "stage_env": stage_env,
        "governing_refs": [],
    }


def _evaluate_normal_pipeline_v1(resolved: Mapping[str, Any]) -> Dict[str, Any]:
    orch = _module()
    if resolved.get("account_binding_failure"):
        return {
            "account_binding_failure": True,
            "status": "ABORTED",
            "reason_codes": [],
            "safety_breaches": ["IB_ACCOUNT_MISMATCH_SLEEVE_BINDING"],
            "governing_refs": list(resolved.get("governing_refs") or []),
        }

    truth_root = Path(str(resolved["truth_root"]))
    day = str(resolved["day"])
    input_day = str(resolved["input_day"])
    mode = str(resolved["mode"])
    symbol = str(resolved["symbol"])
    ib_account = str(resolved["ib_account"])
    produced_utc = str(resolved["produced_utc"])
    git_sha = str(resolved["git_sha"])
    attempt_id = str(resolved["attempt_id"])
    attempt_seq = int(resolved["attempt_seq"])
    session_info = dict(resolved["session_info"])
    stage_env = dict(resolved["stage_env"])
    cfg_hash = str(resolved["cfg_hash"])
    ledger = resolved["ledger"]
    execution_truth_root = orch.resolve_governed_paper_execution_roots(
        repo_root=orch.REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    ).execution_root_path

    if mode == "PAPER":
        orch._run_structural_pre_activity_producers(
            truth_root=truth_root,
            day=day,
            mode=mode,
            symbol=symbol,
            produced_utc=produced_utc,
            git_sha=git_sha,
            env=stage_env,
        )

    act = orch._detect_activity(truth_root, day)
    has_activity = bool(act["activity"])
    session_state = str(session_info.get("session_state") or "UNKNOWN_SESSION").strip().upper()
    effective_activity = bool(has_activity and session_state == "TRADING_SESSION")

    stages = orch._build_stage_defs(
        truth=truth_root,
        day=day,
        input_day=input_day,
        ib_account=ib_account,
        git_sha=git_sha,
        attempt_id=attempt_id,
    )
    orch._validate_sleeve_edge_publication_order(stages)

    stage_results: list[dict[str, Any]] = []
    safety_breaches: list[str] = []
    governed_abort_reasons: list[str] = []
    reason_codes: list[str] = []
    any_required_fail = False
    any_optional_fail = False

    attempt_manifest: Dict[str, Any] = {
        "schema_id": "C2_ORCHESTRATOR_ATTEMPT_MANIFEST_V2",
        "day_utc": day,
        "input_day_utc": input_day,
        "mode": mode,
        "symbol": symbol,
        "ib_account": ib_account,
        "attempt_id": attempt_id,
        "attempt_seq": int(attempt_seq),
        "produced_utc": produced_utc,
        "producer": {"repo": orch.REPO_ROOT.name, "module": "ops/tools/run_c2_paper_day_orchestrator_v2.py", "git_sha": git_sha},
        "paper_session_ledger_path": str(resolved["paper_session_ledger_path"]),
        "ledger_id": ledger.ledger_id,
        "session_id": ledger.session_id,
        "activity": act,
        "effective_activity": bool(effective_activity),
        "session": session_info,
        "stages": [],
        "outputs": [],
    }

    for sd in stages:
        if orch._should_stop_after_sleeve_edge_publication_failure(stage_results):
            break
        required, blocking = orch._stage_required_for_mode(sd, mode, effective_activity)
        classification = {
            "required_for_mode": {"PAPER": bool(sd.required_for_paper), "LIVE": bool(sd.required_for_live)},
            "required_if_activity": bool(sd.required_if_activity),
            "blocking": bool(sd.blocking),
            "effective_required": bool(required),
            "effective_blocking": bool(blocking),
        }

        if sd.stage_id == "A0_ENFORCE_SINGLE_ACCOUNT_TOPOLOGY":
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=False,
                rc=0,
                status="OK",
                reason_codes=["SLEEVE_ACCOUNT_BINDING_ENFORCED"],
                outputs_present=[],
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            continue

        if mode == "PAPER" and sd.stage_id == orch.SLEEVE_EDGE_PUBLICATION_STAGE_ID and required:
            sleeve_edge_ready, sleeve_edge_reason = orch._sleeve_edge_core2_summary_ready(truth_root, day)
            if not sleeve_edge_ready:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=[sleeve_edge_reason],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue

        if sd.stage_id == "A7A_GOVERNED_SUBMIT_V5":
            if session_state != "TRADING_SESSION":
                reason = "SKIP_NON_TRADING_SESSION" if session_state == "NON_TRADING_SESSION" else "SKIP_UNKNOWN_SESSION_FAIL_CLOSED"
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=[reason],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            gov_rc, gov_reason_codes = orch._run_governed_submit_stage(
                truth_root=truth_root,
                day=day,
                produced_utc=produced_utc,
                ib_account=ib_account,
                env=stage_env,
            )
            gov_ok = gov_rc == 0
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=True,
                rc=int(gov_rc),
                status="OK" if gov_ok else "FAIL",
                reason_codes=list(gov_reason_codes),
                outputs_present=[],
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if not gov_ok:
                governed_abort_codes = orch._governed_abort_reason_codes(
                    truth_root=truth_root,
                    day=day,
                    gov_reason_codes=gov_reason_codes,
                )
                if governed_abort_codes:
                    governed_abort_reasons.extend(governed_abort_codes)
                else:
                    safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            continue

        if sd.stage_id == "A6B_AUTHORIZATION_ARTIFACTS_DAY_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        if sd.stage_id == "A6C_OPTIONS_CHAIN_CAPTURE_IB_DAY_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            executed, rc, stage_reason_codes, outputs_present = orch._run_options_capture_stage(
                truth_root=truth_root,
                day=day,
                produced_utc=produced_utc,
                env=stage_env,
            )
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=executed,
                rc=int(rc),
                status="SKIP" if (not executed and rc == 0) else ("OK" if rc == 0 else "FAIL"),
                reason_codes=list(stage_reason_codes),
                outputs_present=outputs_present,
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if rc != 0:
                safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            continue

        if sd.stage_id == "A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_DAY_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            executed, rc, stage_reason_codes, outputs_present = orch._run_options_promotion_stage(
                truth_root=truth_root,
                day=day,
                produced_utc=produced_utc,
                env=stage_env,
            )
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=executed,
                rc=int(rc),
                status="SKIP" if (not executed and rc == 0) else ("OK" if rc == 0 else "FAIL"),
                reason_codes=list(stage_reason_codes),
                outputs_present=outputs_present,
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if rc != 0:
                safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            continue

        if sd.stage_id == "A6E_MARKET_DATA_SNAPSHOT_REFRESH_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            executed, rc, stage_reason_codes, outputs_present = orch._run_market_data_refresh_stage(
                truth_root=truth_root,
                day=day,
                produced_utc=produced_utc,
                env=stage_env,
            )
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=executed,
                rc=int(rc),
                status="SKIP" if (not executed and rc == 0) else ("OK" if rc == 0 else "FAIL"),
                reason_codes=list(stage_reason_codes),
                outputs_present=outputs_present,
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if rc != 0:
                safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            continue

        if sd.stage_id == "A6A_RUN_POINTER_APPEND_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            rp_rc, rp_reason_codes = orch._append_run_pointer_v1(
                truth_root=truth_root,
                day=day,
                mode=mode,
                attempt_id=attempt_id,
                attempt_seq=int(attempt_seq),
                cfg_hash=cfg_hash,
                git_sha=git_sha,
            )
            rp_ok = rp_rc == 0
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=True,
                rc=int(rp_rc),
                status="OK" if rp_ok else "FAIL",
                reason_codes=list(rp_reason_codes),
                outputs_present=[],
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if not rp_ok:
                safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            continue

        if sd.stage_id == "A7_PHASEC_IDENTITY_MATERIALIZER_DAY_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            cmd, stage_reason_codes = orch._build_phasec_materializer_cmd(
                truth_root=truth_root,
                day=day,
                produced_utc=produced_utc,
                ib_account=ib_account,
            )
            rc = orch._run_cmd(sd.stage_id, cmd, env=stage_env)
            ok = rc == 0
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=True,
                rc=int(rc),
                status="OK" if ok else "FAIL",
                reason_codes=list(stage_reason_codes),
                outputs_present=[],
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if not ok:
                safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            continue

        if sd.stage_id == "B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1":
            if not required:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
            executed, rc, stage_reason_codes, outputs_present = orch._run_submission_lifecycle_refresh_stage(
                truth_root=truth_root,
                day=day,
                env=stage_env,
            )
            sr = orch.StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=executed,
                rc=int(rc),
                status="SKIP" if (not executed and rc == 0) else ("OK" if rc == 0 else "FAIL"),
                reason_codes=list(stage_reason_codes),
                outputs_present=outputs_present,
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            if rc != 0:
                any_required_fail = True
            continue

        if sd.stage_id == "B0A_EXECUTION_EVIDENCE_TRUTH_V1":
            governed_submission_count = orch._count_submission_dirs(execution_truth_root, day)
            if governed_submission_count <= 0:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=[
                        "SKIP_SAFE_IDLE_NO_SLEEVE_GOVERNED_SUBMISSIONS"
                        if not effective_activity
                        else "SKIP_NO_SLEEVE_GOVERNED_SUBMISSIONS"
                    ],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue

        if sd.stage_id in (
            "B0AA_ORPHAN_SUBMISSION_BACKFILL_SLEEVE_V1",
            "B0AB_ORPHAN_SUBMISSION_BACKFILL_CANONICAL_V1",
            "B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1",
            "B0B_POSITIONS_SNAPSHOT_V4",
            "B0C_POSITIONS_EFFECTIVE_POINTER_V1",
            "B0_EXECUTION_STREAM_SNAPSHOT_V1",
            "B1_FILL_LEDGER_V1",
            "B2_EXECUTION_RECONCILIATION_V1",
        ):
            governed_submission_count = orch._count_submission_dirs(truth_root, day)
            if governed_submission_count <= 0:
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=[
                        "SKIP_SAFE_IDLE_NO_GOVERNED_SUBMISSIONS"
                        if not effective_activity
                        else "SKIP_NO_GOVERNED_SUBMISSIONS"
                    ],
                    outputs_present=[],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue

        outputs_present: list[str] = []
        if sd.stage_id == "B0_POSITIONS_SNAPSHOT_V5":
            outputs_present = [p for p in sd.skip_if_exists_paths if orch._path_exists(p)]
            if outputs_present and orch._positions_snapshot_v5_skip_safe(truth_root, day):
                sr = orch.StageResult(sd.stage_id, classification, False, 0, "SKIP", ["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"], outputs_present)
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        elif sd.stage_id == "B0B_POSITIONS_SNAPSHOT_V2_COMPAT":
            outputs_present = [p for p in sd.skip_if_exists_paths if orch._path_exists(p)]
            if outputs_present and orch._positions_snapshot_v2_skip_safe(truth_root, day):
                sr = orch.StageResult(sd.stage_id, classification, False, 0, "SKIP", ["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"], outputs_present)
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        elif sd.stage_id == "A4A_ALLOCATION_DAY_V2":
            outputs_present = [p for p in sd.skip_if_exists_paths if orch._path_exists(p)]
            if outputs_present and orch._allocation_summary_v1_skip_safe(Path(outputs_present[0]), day):
                sr = orch.StageResult(sd.stage_id, classification, False, 0, "SKIP", ["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"], outputs_present)
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        elif sd.stage_id == "A5_CAPITAL_RISK_ENVELOPE_GATE_V2":
            outputs_present = [p for p in sd.skip_if_exists_paths if orch._path_exists(p)]
            if outputs_present and orch._capital_risk_envelope_v2_skip_safe(Path(outputs_present[0]), day):
                sr = orch.StageResult(sd.stage_id, classification, False, 0, "SKIP", ["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"], outputs_present)
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        elif sd.stage_id == "B0_EXECUTION_STREAM_SNAPSHOT_V1":
            outputs_present = [p for p in sd.skip_if_exists_paths if orch._path_exists(p)]
            if outputs_present and orch._execution_stream_snapshot_skip_safe(truth_root, day):
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"],
                    outputs_present=outputs_present,
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        elif sd.stage_id == "B1_FILL_LEDGER_V1":
            if orch._fill_ledger_skip_safe(truth_root, day):
                sr = orch.StageResult(
                    stage_id=sd.stage_id,
                    classification=classification,
                    executed=False,
                    rc=0,
                    status="SKIP",
                    reason_codes=["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"],
                    outputs_present=[str((truth_root / "fill_ledger_v1" / day).resolve())],
                )
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue
        else:
            outputs_present = [p for p in sd.skip_if_exists_paths if orch._path_exists(p)]
            if orch._should_skip_existing_outputs(sd, outputs_present):
                sr = orch.StageResult(sd.stage_id, classification, False, 0, "SKIP", ["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"], outputs_present)
                stage_results.append(sr.__dict__)
                attempt_manifest["stages"].append(sr.__dict__)
                continue

        rc = orch._run_cmd(sd.stage_id, sd.cmd, env=stage_env)
        ok = rc == 0
        if ok:
            sr = orch.StageResult(sd.stage_id, classification, True, int(rc), "OK", [], [])
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            continue

        if blocking:
            safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            fail_code = "SAFETY_BREACH_BLOCKING_STAGE_FAIL"
        else:
            if required:
                any_required_fail = True
                fail_code = "REQUIRED_STAGE_FAIL"
            else:
                any_optional_fail = True
                fail_code = "OPTIONAL_STAGE_FAIL"

        sr = orch.StageResult(
            stage_id=sd.stage_id,
            classification=classification,
            executed=True,
            rc=int(rc),
            status="FAIL",
            reason_codes=[fail_code, "SLEEVE_EDGE_PUBLICATION_FAILED"] if sd.stage_id == orch.SLEEVE_EDGE_PUBLICATION_STAGE_ID else [fail_code],
            outputs_present=[],
        )
        stage_results.append(sr.__dict__)
        attempt_manifest["stages"].append(sr.__dict__)

    if safety_breaches:
        status = "ABORTED"
        reason_codes.append("SAFETY_BREACH")
    elif governed_abort_reasons:
        status = "ABORTED"
        reason_codes.extend(governed_abort_reasons)
    else:
        if session_state == "UNKNOWN_SESSION":
            status = "FAIL" if effective_activity else "DEGRADED"
            if "UNKNOWN_SESSION" not in reason_codes:
                reason_codes.append("UNKNOWN_SESSION")
        elif session_state == "NON_TRADING_SESSION":
            status = "DEGRADED"
            if "NON_TRADING_SESSION" not in reason_codes:
                reason_codes.append("NON_TRADING_SESSION")
        elif any_required_fail:
            status = "FAIL"
            reason_codes.append("REQUIRED_FAILURE")
        elif any_optional_fail or not effective_activity:
            status = "DEGRADED"
            if not effective_activity:
                reason_codes.append("NO_ACTIVITY_DAY")
        else:
            status = "PASS"

    return {
        "status": status,
        "reason_codes": list(reason_codes),
        "safety_breaches": list(safety_breaches),
        "stage_results": stage_results,
        "attempt_manifest": attempt_manifest,
        "governing_refs": list(resolved.get("governing_refs") or []),
    }


def evaluate_paper_day_orchestrator_pipeline_v1(resolved: Mapping[str, Any]) -> Dict[str, Any]:
    pipeline_mode = str(resolved["pipeline_mode"])
    if pipeline_mode == "normal":
        return _evaluate_normal_pipeline_v1(resolved)
    if pipeline_mode in {"exact_ref_replay", "bounded_recompute"}:
        verdict_payload = None
        verdict_path = resolved.get("verdict_path")
        if isinstance(verdict_path, Path) and verdict_path.exists():
            verdict_payload = _module()._read_json_obj(verdict_path)
        return {
            "status": str((verdict_payload or {}).get("status") or "REPLAY_ONLY"),
            "attempt_manifest": dict(resolved["attempt_manifest"]),
            "verdict": verdict_payload,
            "reason_codes": list((verdict_payload or {}).get("reason_codes") or []),
            "governing_refs": list(resolved.get("governing_refs") or []),
        }
    raise PipelineBlockedError(f"PIPELINE_MODE_UNSUPPORTED:{PIPELINE_ID}:{pipeline_mode}")


def persist_paper_day_orchestrator_pipeline_outputs_v1(
    resolved: Mapping[str, Any],
    evaluated: Mapping[str, Any],
) -> Dict[str, Any]:
    orch = _module()
    pipeline_mode = str(resolved["pipeline_mode"])
    truth_root = Path(str(resolved["truth_root"]))
    day = str(resolved["day"])
    verdict_root = Path(str(resolved["verdict_root"]))
    attempt_id = str(resolved["attempt_id"])
    out_dir = (verdict_root / day / attempt_id).resolve()

    if pipeline_mode == "normal" and evaluated.get("account_binding_failure"):
        verdict = {
            "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2",
            "day_utc": day,
            "input_day_utc": str(resolved["input_day"]),
            "mode": str(resolved["mode"]),
            "symbol": str(resolved["symbol"]),
            "ib_account": str(resolved["ib_account"]),
            "produced_utc": str(resolved["produced_utc"]),
            "status": "ABORTED",
            "safety_breaches": ["IB_ACCOUNT_MISMATCH_SLEEVE_BINDING"],
            "reason_codes": [
                f"SLEEVE_ID={resolved['account_binding_failure']['sleeve_id']}",
                f"EXPECTED_SLEEVE_ACCOUNT={resolved['account_binding_failure']['expected_ib_account']}",
            ],
            "stages": [],
            "replay": {"derived_from_attempt_manifest": True, "hashes": {}},
            "producer": {"repo": orch.REPO_ROOT.name, "module": "ops/tools/run_c2_paper_day_orchestrator_v2.py", "git_sha": str(resolved["git_sha"])},
            "paper_session_ledger_path": str(resolved["paper_session_ledger_path"]),
            "ledger_id": resolved["ledger"].ledger_id,
        }
        verdict_path = out_dir / "orchestrator_run_verdict.v2.json"
        orch._write_attempt_file(verdict_path, orch._json_dumps(verdict))
        return {
            "status": "ABORTED",
            "exit_code": 7,
            "verdict_path": str(verdict_path),
            "governing_refs": [
                {
                    "artifact_id": "orchestrator_run_verdict_v2",
                    "artifact_path": str(verdict_path.resolve()),
                    "artifact_sha256": "",
                }
            ],
        }

    if pipeline_mode == "normal":
        attempt_manifest = dict(evaluated["attempt_manifest"])
        man_wr = orch._write_attempt_file(out_dir / "orchestrator_attempt_manifest.v2.json", orch._json_dumps(attempt_manifest))
        attempt_manifest_path = (out_dir / "orchestrator_attempt_manifest.v2.json").resolve()
        pub3_ok, pub3_rc = orch._publish_pipeline_manifest_v3(
            day=day,
            mode=str(resolved["mode"]),
            attempt_id=attempt_id,
            attempt_seq=int(resolved["attempt_seq"]),
            attempt_manifest_path=attempt_manifest_path,
            env=dict(resolved["stage_env"]),
        )
        reason_codes = list(evaluated["reason_codes"])
        status = str(evaluated["status"])
        if not pub3_ok:
            reason_codes.extend(["PIPELINE_MANIFEST_V3_PUBLISH_FAIL", f"PIPELINE_MANIFEST_V3_PUBLISH_RC={pub3_rc}"])
            if status == "PASS":
                status = "DEGRADED"
        pub2_ok, pub2_rc = orch._publish_pipeline_manifest_v2_compat(
            day=day,
            attempt_manifest_path=attempt_manifest_path,
            truth_root=truth_root,
            env=dict(resolved["stage_env"]),
        )
        if not pub2_ok:
            reason_codes.extend(["PIPELINE_MANIFEST_V2_COMPAT_PUBLISH_FAIL", f"PIPELINE_MANIFEST_V2_COMPAT_PUBLISH_RC={pub2_rc}"])
            if status == "PASS":
                status = "DEGRADED"
        pub1_ok, pub1_rc = orch._publish_pipeline_manifest_v1_compat(
            day=day,
            attempt_manifest_path=attempt_manifest_path,
            truth_root=truth_root,
            env=dict(resolved["stage_env"]),
        )
        if not pub1_ok:
            reason_codes.extend(["PIPELINE_MANIFEST_V1_COMPAT_PUBLISH_FAIL", f"PIPELINE_MANIFEST_V1_COMPAT_PUBLISH_RC={pub1_rc}"])
            if status == "PASS":
                status = "DEGRADED"
        verdict = {
            "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2",
            "day_utc": day,
            "input_day_utc": str(resolved["input_day"]),
            "mode": str(resolved["mode"]),
            "symbol": str(resolved["symbol"]),
            "ib_account": str(resolved["ib_account"]),
            "attempt_id": attempt_id,
            "attempt_seq": int(resolved["attempt_seq"]),
            "produced_utc": str(resolved["produced_utc"]),
            "status": status,
            "safety_breaches": list(evaluated["safety_breaches"]),
            "reason_codes": reason_codes,
            "stages": list(evaluated["stage_results"]),
            "replay": {
                "derived_from_attempt_manifest": True,
                "hashes": {"attempt_manifest_sha256": str(man_wr["sha256"])},
            },
            "producer": {"repo": orch.REPO_ROOT.name, "module": "ops/tools/run_c2_paper_day_orchestrator_v2.py", "git_sha": str(resolved["git_sha"])},
        }
        verdict_path = (out_dir / "orchestrator_run_verdict.v2.json").resolve()
        orch._write_attempt_file(verdict_path, orch._json_dumps(verdict))
        day_root = (verdict_root / day).resolve()
        idx_path = (day_root / orch.POINTER_INDEX_NAME).resolve()
        lock_path = (day_root / orch.POINTER_LOCK_NAME).resolve()
        lock_fd = orch._pointer_lock_acquire(lock_path)
        try:
            last_seq = orch._read_last_pointer_seq(idx_path, str(resolved["mode"]))
            pointer_seq = last_seq + 1
            entry = {
                "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2_POINTER_INDEX_V1",
                "pointer_seq": int(pointer_seq),
                "day_utc": day,
                "mode": str(resolved["mode"]),
                "attempt_id": attempt_id,
                "attempt_seq": int(resolved["attempt_seq"]),
                "status": status,
                "authoritative": bool(status == "PASS"),
                "producer_git_sha": str(resolved["git_sha"]),
                "produced_utc": f"{day}T00:00:00Z",
                "points_to": str(verdict_path),
                "attempt_manifest_path": str(attempt_manifest_path),
            }
            line_sha, _ = orch._atomic_append_jsonl(idx_path, entry)
        finally:
            orch._pointer_lock_release(lock_fd, lock_path)
        orch._write_attempt_file(
            out_dir / "orchestrator_run_verdict.v2.pointer_append.sha256.json",
            orch._json_dumps({"append_line_sha256": line_sha, "index_path": str(idx_path)}),
        )
        return {
            "status": status,
            "exit_code": 9 if status == "ABORTED" else 0,
            "attempt_manifest_path": str(attempt_manifest_path),
            "verdict_path": str(verdict_path),
            "governing_refs": [
                {"artifact_id": "orchestrator_attempt_manifest_v2", "artifact_path": str(attempt_manifest_path), "artifact_sha256": str(man_wr["sha256"])},
                {"artifact_id": "orchestrator_run_verdict_v2", "artifact_path": str(verdict_path), "artifact_sha256": ""},
            ],
        }

    if pipeline_mode == "exact_ref_replay":
        return {
            "status": str(evaluated["status"]),
            "exit_code": 0,
            "attempt_manifest_path": str(resolved["attempt_manifest_path"]),
            "verdict_path": str(resolved["verdict_path"]) if resolved.get("verdict_path") else None,
            "governing_refs": list(resolved.get("governing_refs") or []),
        }

    if pipeline_mode == "bounded_recompute":
        manifest_payload = dict(resolved["attempt_manifest"])
        manifest_path = Path(str(resolved["attempt_manifest_path"])).resolve()
        mode = str(manifest_payload.get("mode") or resolved.get("mode") or "")
        attempt_seq = int(manifest_payload.get("attempt_seq") or resolved.get("attempt_seq") or 0)
        pub3_ok, pub3_rc = orch._publish_pipeline_manifest_v3(
            day=day,
            mode=mode,
            attempt_id=attempt_id,
            attempt_seq=attempt_seq,
            attempt_manifest_path=manifest_path,
            env=dict(resolved["stage_env"]),
        )
        pub2_ok, pub2_rc = orch._publish_pipeline_manifest_v2_compat(
            day=day,
            attempt_manifest_path=manifest_path,
            truth_root=truth_root,
            env=dict(resolved["stage_env"]),
        )
        pub1_ok, pub1_rc = orch._publish_pipeline_manifest_v1_compat(
            day=day,
            attempt_manifest_path=manifest_path,
            truth_root=truth_root,
            env=dict(resolved["stage_env"]),
        )
        if not (pub3_ok and pub2_ok and pub1_ok):
            raise PipelineBlockedError(
                "BOUNDED_RECOMPUTE_PUBLISH_FAIL:"
                f"v3={pub3_rc}:v2={pub2_rc}:v1={pub1_rc}"
            )
        return {
            "status": str(evaluated["status"]),
            "exit_code": 0,
            "attempt_manifest_path": str(manifest_path),
            "verdict_path": str(resolved["verdict_path"]) if resolved.get("verdict_path") else None,
            "governing_refs": list(resolved.get("governing_refs") or []),
        }

    raise PipelineBlockedError(f"PIPELINE_MODE_UNSUPPORTED:{PIPELINE_ID}:{pipeline_mode}")


def project_emit_paper_day_orchestrator_pipeline_v1(
    resolved: Mapping[str, Any],
    evaluated: Mapping[str, Any],
    persisted: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "pipeline_mode": str(resolved["pipeline_mode"]),
        "status": str(persisted.get("status") or evaluated.get("status") or "UNKNOWN"),
        "attempt_id": str(resolved.get("attempt_id") or ""),
        "attempt_manifest_path": persisted.get("attempt_manifest_path"),
        "verdict_path": persisted.get("verdict_path"),
        "exit_code": int(persisted.get("exit_code") or 0),
        "governing_refs": list(persisted.get("governing_refs") or evaluated.get("governing_refs") or resolved.get("governing_refs") or []),
    }


def run_paper_day_orchestrator_obligation_pipeline_v1(args: Any) -> Dict[str, Any]:
    pipeline_mode = str(getattr(args, "pipeline_mode", "") or "normal")
    budget_profile = str(getattr(args, "budget_profile", "") or "contract_default")
    if pipeline_mode == "forensic_replay":
        blocked = blocked_pipeline_report_v1(
            pipeline_id=PIPELINE_ID,
            pipeline_mode=pipeline_mode,
            target_path_family=TARGET_PATH_FAMILY,
            blocked_reason="PIPELINE_MODE_UNSUPPORTED:paper_day_orchestrator_v2:forensic_replay",
            budget_profile=budget_profile,
        )
        return {
            "ok": False,
            "exit_code": 9,
            "result": {},
            "pipeline_proof": dict(blocked["proof"]),
        }
    report = execute_bounded_obligation_pipeline_v1(
        pipeline_id=PIPELINE_ID,
        pipeline_mode=pipeline_mode,
        target_path_family=TARGET_PATH_FAMILY,
        budget_profile=budget_profile,
        resolve_inputs=lambda: resolve_paper_day_orchestrator_pipeline_inputs_v1(args),
        evaluate=evaluate_paper_day_orchestrator_pipeline_v1,
        persist_outputs=persist_paper_day_orchestrator_pipeline_outputs_v1,
        project_emit=project_emit_paper_day_orchestrator_pipeline_v1,
    )
    return {
        "ok": bool(report["ok"]),
        "exit_code": int((report["projected"] or {}).get("exit_code") or (9 if not report["ok"] else 0)),
        "result": dict(report["projected"]),
        "pipeline_proof": dict(report["proof"]),
    }
