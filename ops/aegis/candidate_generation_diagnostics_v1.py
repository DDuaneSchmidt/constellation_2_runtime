from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_candidate_generation_diagnostics_v1"
ENGINE_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json")
LEGACY_EXECUTION_SLEEVE_REGISTRY_RELPATH = Path("governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json")
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
REPAIR_COMMAND = "npm run aegis:repair-candidate-readiness"
SLEEVE_EVALUATION_FAMILY = "sleeve_evaluation_kernel_v1"
DATA_BLOCK_REASONS = {
    "EVENT_OR_REGIME_EVIDENCE_MISSING_OR_STALE",
    "RUNTIME_TRUTH_NOT_TARGET_MODE_READY",
    "MISSING_RUNTIME_TRUTH",
    "MISSING_MARKET_DATA",
    "STALE_MARKET_DATA",
}


def build_candidate_generation_diagnostics_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    trigger_path, trigger_payload = latest_json_v1(root, "aegis_event_regime_trigger_evaluator_v1", day_utc, "trigger_evaluation.v1.json")
    runs_path, runs_payload = latest_json_v1(root, "aegis_triggered_sleeve_runs_v1", day_utc, "triggered_sleeve_runs.v1.json")
    lifecycle_path, lifecycle_payload = latest_json_v1(root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json")
    ranking_path, _ranking_payload = latest_json_v1(root, "aegis_candidate_ranking_v1", day_utc, "candidate_ranking.v1.json")
    candidate_manifest_path, candidate_manifest_payload = latest_json_v1(root, "candidate_generation_manifest_v1", day_utc, "candidate_generation_manifest.v1.json")
    candidate_lineage_path, candidate_lineage_payload = latest_json_v1(root, "candidate_lineage_v1", day_utc, "candidate_lineage.v1.json")
    intent_arbitration_path, intent_arbitration_payload = latest_json_v1(root, "intent_arbitration_v1", day_utc, "intent_arbitration.v1.json")
    selected_intent_promotion_path, selected_intent_promotion_payload = latest_json_v1(root, "aegis_selected_intent_promotion_v1", day_utc, "selected_intent_promotion.v1.json")
    runtime_path, runtime_payload = latest_json_v1(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    market_data_path, market_data_payload = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    data_registry_path, data_registry_payload = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    contracts_path, contracts_payload = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    readiness_path, readiness_payload = latest_json_v1(root, "aegis_sleeve_readiness_v1", day_utc, "sleeve_readiness.v1.json")
    if not lifecycle_payload and runs_payload:
        lifecycle_payload = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)

    inventory = _authoritative_sleeve_inventory(repo_root=repo)
    expected_sleeves = [row["sleeve_id"] for row in inventory["expected_sleeves"]]
    sleeve_eval_path, sleeve_eval_payload = latest_json_v1(root, SLEEVE_EVALUATION_FAMILY, day_utc, "sleeve_evaluation_rollup.v1.json")
    sleeve_eval_by_id = _sleeve_evaluation_outcomes_by_id(
        truth_root=root,
        day_utc=day_utc,
        rollup_payload=sleeve_eval_payload,
        expected_sleeves=expected_sleeves,
    )
    decisions = trigger_payload.get("decisions") if isinstance(trigger_payload.get("decisions"), list) else []
    runs = runs_payload.get("runs") if isinstance(runs_payload.get("runs"), list) else []
    candidates = lifecycle_payload.get("candidates") if isinstance(lifecycle_payload.get("candidates"), list) else []
    candidate_counts = _candidate_counts_by_sleeve(candidates)
    run_sleeves = _run_sleeves(runs)
    rejected_by_sleeve = _rejection_reasons_by_sleeve(runs)
    data_status = _data_status(trigger_payload=trigger_payload, runtime_payload=runtime_payload, runtime_path=runtime_path)
    readiness_by_id = _readiness_by_id(readiness_payload)
    raw_signal_rejections = _raw_signal_rejections(
        candidate_manifest_payload=candidate_manifest_payload,
        candidate_lineage_payload=candidate_lineage_payload,
        intent_arbitration_payload=intent_arbitration_payload,
        selected_intent_promotion_payload=selected_intent_promotion_payload,
    )
    raw_signal_rejections_by_sleeve = _raw_signal_rejections_by_sleeve(raw_signal_rejections)
    sleeves = _sleeve_rows(
        expected_sleeves=expected_sleeves,
        run_sleeves=run_sleeves,
        candidate_counts=candidate_counts,
        rejected_by_sleeve=rejected_by_sleeve,
        decisions=decisions,
        data_status=data_status,
        inventory=inventory,
        sleeve_eval_by_id=sleeve_eval_by_id,
        readiness_by_id=readiness_by_id,
        raw_signal_rejections_by_sleeve=raw_signal_rejections_by_sleeve,
    )
    total_raw_signals = sum(int(row.get("raw_signal_count") or 0) for row in sleeves)
    total_candidates = sum(int(row.get("candidate_count") or 0) for row in sleeves)
    if candidates and total_candidates == 0:
        total_candidates = len(candidates)
    total_rejected = sum(int(row.get("rejected_count") or 0) for row in sleeves)
    total_run = sum(1 for row in sleeves if row.get("run_status") == "RAN")
    status = _candidate_generation_status(
        trigger_path=trigger_path,
        runs_path=runs_path,
        sleeve_eval_path=sleeve_eval_path,
        expected_count=len(expected_sleeves),
        run_count=total_run,
        runs=runs,
        candidates=candidates,
        sleeves=sleeves,
    )
    trigger_evaluation = _trigger_evaluation(trigger_path=trigger_path, trigger_payload=trigger_payload)
    interpretation = _operator_interpretation(
        status=status,
        total_candidates=total_candidates,
        total_sleeves_expected=len(expected_sleeves),
        total_sleeves_run=total_run,
        data_status=data_status,
        sleeves=sleeves,
        readiness_payload=readiness_payload,
    )
    explanation = _zero_candidate_explanation(
        total_candidates=total_candidates,
        total_rejected=total_rejected,
        status=status,
        interpretation=interpretation,
        total_sleeves_run=total_run,
        data_status=data_status,
        raw_signal_rejections=raw_signal_rejections,
    )
    return {
        "schema_id": "aegis_candidate_generation_diagnostics",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_generation_diagnostics_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "candidate_generation_status": status,
        "authoritative_sleeve_registry_path": str(inventory["registry_path"]),
        "legacy_execution_sleeve_registry_path": str(inventory["legacy_registry_path"]),
        "registry_mismatches": inventory["registry_mismatches"],
        "total_sleeves_registered": inventory["total_sleeves_registered"],
        "total_sleeves_enabled": inventory["total_sleeves_enabled"],
        "total_sleeves_expected_today": len(expected_sleeves),
        "total_sleeves_expected": len(expected_sleeves),
        "total_sleeves_run": total_run,
        "total_sleeves_ready": int(readiness_payload.get("ready_count") or 0) if readiness_payload else 0,
        "total_sleeves_ready_with_warnings": int(readiness_payload.get("ready_with_warnings_count") or 0) if readiness_payload else 0,
        "total_sleeves_skipped": sum(1 for row in sleeves if row.get("run_status") == "SKIPPED"),
        "total_sleeves_failed": sum(1 for row in sleeves if row.get("run_status") == "FAILED"),
        "total_sleeves_blocked": sum(1 for row in sleeves if row.get("run_status") == "BLOCKED"),
        "global_data_status": _global_data_status(data_registry_payload),
        "market_data_summary": _market_data_summary(market_data_path=market_data_path, market_data_payload=market_data_payload),
        "per_sleeve_readiness": readiness_payload.get("sleeves") if isinstance(readiness_payload.get("sleeves"), list) else [],
        "global_context_warnings": _global_context_warnings(data_registry_payload),
        "blocking_policy": "PER_SLEEVE_INPUT_CONTRACTS",
        "expected_sleeves": expected_sleeves,
        "expected_sleeve_ids": expected_sleeves,
        "enabled_sleeve_ids": [row["sleeve_id"] for row in inventory["enabled_sleeves"]],
        "sleeve_run_commands": _sleeve_run_commands(sleeves),
        "producer_commands": _producer_commands(sleeves=sleeves, runtime_payload=runtime_payload),
        "missing_input_artifacts": _missing_input_artifacts(runtime_payload=runtime_payload, sleeves=sleeves),
        "stale_input_artifacts": _stale_input_artifacts(runtime_payload=runtime_payload, sleeves=sleeves),
        "failed_producers": _failed_producers(sleeves),
        "exact_blocker": _exact_blocker(
            sleeve_eval_path=sleeve_eval_path,
            sleeves=sleeves,
            runtime_payload=runtime_payload,
            inventory=inventory,
        ),
        "repair_command": REPAIR_COMMAND,
        "cannot_repair_reason": _cannot_repair_reason(runtime_payload=runtime_payload, sleeves=sleeves),
        "total_raw_signals": total_raw_signals,
        "total_candidates_generated": total_candidates,
        "total_candidates_rejected": total_rejected,
        "raw_signal_rejections": raw_signal_rejections,
        "rejected_raw_signal_details": raw_signal_rejections,
        "selected_intent_promotion": _selected_intent_promotion_summary(selected_intent_promotion_payload),
        "zero_candidate_explanation": explanation,
        "operator_interpretation": interpretation,
        "sleeves": sleeves,
        "trigger_evaluation": trigger_evaluation,
        "recommended_next_steps": _recommended_next_steps(
            interpretation=interpretation,
            data_status=data_status,
            status=status,
            total_candidates=total_candidates,
            total_rejected=total_rejected,
        ),
        "input_artifacts": {
            "trigger_evaluation": str(trigger_path or ""),
            "triggered_sleeve_runs": str(runs_path or ""),
            "candidate_lifecycle": str(lifecycle_path or ""),
            "candidate_ranking": str(ranking_path or ""),
            "candidate_generation_manifest": str(candidate_manifest_path or ""),
            "candidate_lineage": str(candidate_lineage_path or ""),
            "intent_arbitration": str(intent_arbitration_path or ""),
            "selected_intent_promotion": str(selected_intent_promotion_path or ""),
            "runtime_truth": str(runtime_path or ""),
            "market_data": str(market_data_path or ""),
            "data_registry": str(data_registry_path or ""),
            "sleeve_input_contracts": str(contracts_path or ""),
            "sleeve_readiness": str(readiness_path or ""),
            "sleeve_evaluation_rollup": str(sleeve_eval_path or ""),
            "authoritative_sleeve_registry": str(inventory["registry_path"]),
        },
        "safety": {
            "advisory_only": True,
            "read_only": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "broker_submit_transmit_called": False,
        },
    }


def write_candidate_generation_diagnostics_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "candidate_generation_diagnostics.v1.json", payload)
    summary_path = out_dir / "candidate_generation_diagnostics.summary.txt"
    matrix_path = out_dir / "candidate_generation_diagnostics.matrix.csv"
    summary_path.write_text(render_candidate_generation_diagnostics_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_candidate_generation_diagnostics_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_candidate_generation_diagnostics_summary_v1(payload: dict[str, Any]) -> str:
    market_data = payload.get("market_data_summary") if isinstance(payload.get("market_data_summary"), dict) else {}
    market_config = market_data.get("provider_config") if isinstance(market_data.get("provider_config"), dict) else {}
    lines = [
        "AEGIS CANDIDATE GENERATION DIAGNOSTICS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"candidate_generation_status: {payload.get('candidate_generation_status')}",
        f"operator_interpretation: {payload.get('operator_interpretation')}",
        f"authoritative_sleeve_registry_path: {payload.get('authoritative_sleeve_registry_path')}",
        f"total_sleeves_registered: {payload.get('total_sleeves_registered')}",
        f"total_sleeves_enabled: {payload.get('total_sleeves_enabled')}",
        f"total_sleeves_expected_today: {payload.get('total_sleeves_expected_today')}",
        f"total_sleeves_expected: {payload.get('total_sleeves_expected')}",
        f"total_sleeves_run: {payload.get('total_sleeves_run')}",
        f"total_sleeves_skipped: {payload.get('total_sleeves_skipped')}",
        f"total_sleeves_failed: {payload.get('total_sleeves_failed')}",
        f"total_sleeves_blocked: {payload.get('total_sleeves_blocked')}",
        f"total_raw_signals: {payload.get('total_raw_signals')}",
        f"total_candidates_generated: {payload.get('total_candidates_generated')}",
        f"total_candidates_rejected: {payload.get('total_candidates_rejected')}",
        f"exact_blocker: {payload.get('exact_blocker')}",
        f"market_data_status: {market_data.get('status') or 'UNKNOWN'}",
        f"market_data_failure_reason: {market_data.get('failure_reason') or ''}",
        f"market_data_provider_primary: {market_config.get('primary') or ''}",
        f"market_data_provider_fallback: {market_config.get('fallback') or ''}",
        f"missing_symbols: {', '.join(market_data.get('missing_symbols') or [])}",
        f"stale_symbols: {', '.join(market_data.get('stale_symbols') or [])}",
        f"repair_command: {payload.get('repair_command')}",
        f"zero_candidate_explanation: {payload.get('zero_candidate_explanation')}",
        "",
        "sleeves:",
    ]
    explanations = market_data.get("missing_symbol_explanations") if isinstance(market_data.get("missing_symbol_explanations"), dict) else {}
    if explanations:
        lines.extend(["", "missing_symbol_explanations:"])
        for symbol, row in sorted(explanations.items()):
            if isinstance(row, dict):
                lines.append(f"- {symbol}: {row.get('operator_message') or row.get('blocker_reason') or ''}")
    for row in payload.get("sleeves") or []:
        lines.append(
            "- "
            f"{row.get('sleeve_id')}: status={row.get('run_status')} raw={row.get('raw_signal_count')} "
            f"candidates={row.get('candidate_count')} rejected={row.get('rejected_count')} reason={row.get('reason_no_candidate')}"
        )
    raw_rejections = payload.get("raw_signal_rejections") if isinstance(payload.get("raw_signal_rejections"), list) else []
    lines.extend(["", "raw_signal_rejections:"])
    if raw_rejections:
        for row in raw_rejections:
            lines.append(
                "- "
                f"{row.get('raw_signal_id')}: sleeve={row.get('sleeve_id')} stage={row.get('rejection_stage')} "
                f"reason={row.get('rejection_reason')} explanation={row.get('human_readable_explanation')}"
            )
    else:
        lines.append("- none")
    trigger = payload.get("trigger_evaluation") if isinstance(payload.get("trigger_evaluation"), dict) else {}
    lines.extend(
        [
            "",
            "trigger_evaluation:",
            f"- ran: {str(trigger.get('ran') is True).lower()}",
            f"- trigger_count: {trigger.get('trigger_count', 0)}",
            f"- activated_count: {trigger.get('activated_count', 0)}",
            f"- skipped_count: {trigger.get('skipped_count', 0)}",
            f"- reason: {trigger.get('reason', '')}",
            "",
            "recommended_next_steps:",
        ]
    )
    for step in payload.get("recommended_next_steps") or []:
        lines.append(f"- {step}")
    lines.extend(["", "broker_execution_allowed: false", "autonomous_execution_allowed: false", "automatic_sleeve_mutation_allowed: false", ""])
    return "\n".join(lines)


def render_candidate_generation_diagnostics_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=[
            "sleeve_id",
            "expected_today",
            "run_status",
            "runner_command",
            "required_inputs",
            "missing_inputs",
            "stale_inputs",
            "raw_signal_count",
            "candidate_count",
            "rejected_count",
            "rejection_reasons",
            "raw_signal_rejections",
            "data_status",
            "regime_filter_status",
            "thresholds_applied",
            "reason_no_candidate",
            "next_repair_action",
        ],
    )
    writer.writeheader()
    for row in payload.get("sleeves") or []:
        writer.writerow(
            {
                "sleeve_id": row.get("sleeve_id", ""),
                "expected_today": row.get("expected_today", False),
                "run_status": row.get("run_status", ""),
                "runner_command": row.get("runner_command", ""),
                "required_inputs": "|".join(row.get("required_inputs") or []),
                "missing_inputs": "|".join(row.get("missing_inputs") or []),
                "stale_inputs": "|".join(row.get("stale_inputs") or []),
                "raw_signal_count": row.get("raw_signal_count", 0),
                "candidate_count": row.get("candidate_count", 0),
                "rejected_count": row.get("rejected_count", 0),
                "rejection_reasons": "|".join(row.get("rejection_reasons") or []),
                "raw_signal_rejections": "|".join(str(item.get("rejection_reason") or "") for item in row.get("raw_signal_rejections") or [] if isinstance(item, dict)),
                "data_status": row.get("data_status", ""),
                "regime_filter_status": row.get("regime_filter_status", ""),
                "thresholds_applied": "|".join(row.get("thresholds_applied") or []),
                "reason_no_candidate": row.get("reason_no_candidate", ""),
                "next_repair_action": row.get("next_repair_action", ""),
            }
        )
    return out.getvalue()


def _authoritative_sleeve_inventory(*, repo_root: Path) -> dict[str, Any]:
    registry_path = (Path(repo_root).resolve() / ENGINE_REGISTRY_RELPATH).resolve()
    registry = _read_json(registry_path)
    raw_rows = registry.get("engines") if isinstance(registry.get("engines"), list) else []
    registered: list[dict[str, Any]] = []
    excluded: list[str] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        sleeve_id = str(raw.get("engine_id") or "").strip().upper()
        if not sleeve_id:
            continue
        if sleeve_id == SIMULATOR_ENGINE_ID:
            excluded.append(sleeve_id)
            continue
        activation = str(raw.get("activation_status") or "").strip().upper()
        runner_rel = str(raw.get("engine_runner_path") or "").strip()
        allowed_symbols = [str(item).strip().upper() for item in raw.get("allowed_symbols") or [] if str(item).strip()]
        row = {
            "sleeve_id": sleeve_id,
            "registry_path": str(registry_path),
            "definition_path": str((Path(repo_root).resolve() / runner_rel).resolve()) if runner_rel else "",
            "activation_status": activation or "UNKNOWN",
            "enabled": activation == "ACTIVE",
            "expected_today": activation == "ACTIVE",
            "expected_cadence": "DAILY_ADVISORY_CANDIDATE_GENERATION",
            "required_inputs": [
                f"{ENGINE_REGISTRY_RELPATH}",
                runner_rel or "engine_runner_path",
                "market_data_snapshot_v1:" + ",".join(allowed_symbols),
                f"reports/{SLEEVE_EVALUATION_FAMILY}/<day>/<sleeve_id>/sleeve_evaluation.v1.json",
            ],
            "allowed_symbols": allowed_symbols,
            "runner_command": _registry_runner_command(repo_root=repo_root, runner_rel=runner_rel, allowed_symbols=allowed_symbols),
        }
        registered.append(row)
    enabled = [row for row in registered if row["enabled"]]
    expected = [row for row in enabled if row["expected_today"]]
    legacy_path = (Path(repo_root).resolve() / LEGACY_EXECUTION_SLEEVE_REGISTRY_RELPATH).resolve()
    legacy_sleeves = _legacy_execution_sleeves(legacy_path)
    mismatches = []
    if legacy_sleeves and sorted(legacy_sleeves) != sorted(row["sleeve_id"] for row in registered):
        mismatches.append(
            {
                "registry_path": str(legacy_path),
                "registry_role": "execution_account_registry",
                "registered_sleeve_count": len(legacy_sleeves),
                "registered_sleeve_ids": legacy_sleeves,
                "ignored_for_candidate_diagnostics": True,
                "reason": "C2_SLEEVE_REGISTRY_V1 lists execution sleeves/accounts and is not the advisory candidate sleeve authority.",
            }
        )
    return {
        "registry_path": registry_path,
        "legacy_registry_path": legacy_path,
        "registry_mismatches": mismatches,
        "excluded_engine_ids": excluded,
        "registered_sleeves": registered,
        "enabled_sleeves": enabled,
        "expected_sleeves": expected,
        "total_sleeves_registered": len(registered),
        "total_sleeves_enabled": len(enabled),
    }


def _candidate_counts_by_sleeve(candidates: list[Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in candidates:
        if not isinstance(row, dict):
            continue
        sleeve = str(row.get("sleeve_id") or "UNKNOWN").strip().upper() or "UNKNOWN"
        out[sleeve] = out.get(sleeve, 0) + 1
    return out


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _legacy_execution_sleeves(path: Path) -> list[str]:
    payload = _read_json(path)
    sleeves = payload.get("sleeves") if isinstance(payload.get("sleeves"), list) else []
    out = []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip().upper()
        if sleeve_id:
            out.append(sleeve_id)
    return sorted(set(out))


def _registry_runner_command(*, repo_root: Path, runner_rel: str, allowed_symbols: list[str]) -> str:
    if not runner_rel:
        return ""
    base = [
        "python3",
        str((Path(repo_root).resolve() / runner_rel).resolve()),
        "--day_utc",
        "<DAY_UTC>",
        "--mode",
        "PAPER",
        "--truth_root",
        "/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER",
    ]
    if len(allowed_symbols) == 1:
        base.extend(["--symbol", allowed_symbols[0]])
    elif allowed_symbols:
        base.extend(["--symbols", ",".join(allowed_symbols)])
    return " ".join(base)


def _sleeve_evaluation_outcomes_by_id(
    *,
    truth_root: Path,
    day_utc: str,
    rollup_payload: dict[str, Any],
    expected_sleeves: list[str],
) -> dict[str, dict[str, Any]]:
    outcomes = rollup_payload.get("outcomes") if isinstance(rollup_payload.get("outcomes"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in outcomes:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or row.get("engine_id") or "").strip().upper()
        if sleeve_id:
            out[sleeve_id] = row
    for sleeve_id in expected_sleeves:
        if sleeve_id in out:
            continue
        path = (
            Path(truth_root).resolve()
            / "reports"
            / SLEEVE_EVALUATION_FAMILY
            / day_utc
            / sleeve_id
            / "sleeve_evaluation.v1.json"
        )
        payload = _read_json(path)
        if payload:
            out[sleeve_id] = payload
    return out


def _run_sleeves(runs: list[Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for run in runs:
        if not isinstance(run, dict):
            continue
        status = str(run.get("status") or "UNKNOWN").strip().upper()
        for sleeve in _strings(run.get("selected_sleeve_ids")):
            row = out.setdefault(sleeve, {"run_count": 0, "statuses": set(), "raw_signal_count": 0})
            row["run_count"] += 1
            row["statuses"].add(status)
            row["raw_signal_count"] += 1
    return out


def _rejection_reasons_by_sleeve(runs: list[Any]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for run in runs:
        if not isinstance(run, dict):
            continue
        selected = _strings(run.get("selected_sleeve_ids"))
        for warning in _strings(run.get("warnings"), preserve_case=True):
            sleeve, reason = _split_sleeve_reason(warning, selected)
            if not reason:
                continue
            out.setdefault(sleeve, [])
            if reason not in out[sleeve]:
                out[sleeve].append(reason)
    return out


def _split_sleeve_reason(warning: str, selected: list[str]) -> tuple[str, str]:
    text = str(warning or "").strip()
    if not text:
        return "", ""
    if ":" in text:
        sleeve, reason = text.split(":", 1)
        return (sleeve.strip().upper() or "UNKNOWN", reason.strip() or text)
    if len(selected) == 1:
        return selected[0], text
    return "UNKNOWN", text


def _data_status(*, trigger_payload: dict[str, Any], runtime_payload: dict[str, Any], runtime_path: Path | None) -> str:
    runtime_missing = not runtime_path or not runtime_payload
    runtime_stale_count = int(runtime_payload.get("missing_or_stale_source_count") or 0) if runtime_payload else 0
    decisions = trigger_payload.get("decisions") if isinstance(trigger_payload.get("decisions"), list) else []
    reasons = {
        str(row.get("reason_skipped") or "").strip().upper()
        for row in decisions
        if isinstance(row, dict) and str(row.get("reason_skipped") or "").strip()
    }
    if "STALE_MARKET_DATA" in reasons:
        return "STALE"
    if runtime_missing or runtime_stale_count > 0 or reasons.intersection(DATA_BLOCK_REASONS):
        return "MISSING"
    if not trigger_payload:
        return "UNKNOWN"
    return "OK"


def _readiness_by_id(readiness_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = readiness_payload.get("sleeves") if isinstance(readiness_payload.get("sleeves"), list) else []
    return {str(row.get("sleeve_id") or "").strip().upper(): row for row in rows if isinstance(row, dict) and row.get("sleeve_id")}


def _global_data_status(data_registry_payload: dict[str, Any]) -> str:
    if not data_registry_payload:
        return "UNKNOWN"
    if data_registry_payload.get("stale_items"):
        return "STALE"
    if data_registry_payload.get("missing_items"):
        return "MISSING"
    return "OK"


def _global_context_warnings(data_registry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not data_registry_payload:
        return []
    ids = [*(data_registry_payload.get("missing_items") or []), *(data_registry_payload.get("stale_items") or [])]
    return [{"data_item_id": str(item), "message": "Global context item is unavailable; only sleeves whose contracts require it are blocked."} for item in sorted(set(ids))]


def _market_data_summary(*, market_data_path: Path | None, market_data_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(market_data_payload, dict) or not market_data_payload:
        return {
            "available": False,
            "path": "",
            "status": "MISSING",
            "provider_config": {"configured": False, "primary": "", "fallback": ""},
            "provider_results": [],
            "requested_symbols": [],
            "runtime_universe_mode": "",
            "production_scan_dataset_id": "",
            "dataset_snapshot_id": "",
            "production_scan_universe_count": 0,
            "sleeve_required_symbol_count": 0,
            "total_requested_symbol_count": 0,
            "requested_symbols_source": "",
            "minimum_viable_runtime_reference_removed": False,
            "fetched_symbols": [],
            "missing_symbols": [],
            "stale_symbols": [],
            "mapping_missing_symbols": [],
            "provider_failed_symbols": [],
            "missing_fields": [],
            "stale_fields": [],
        "failure_reason": "MARKET_DATA_REPORT_MISSING",
        "usable_for_candidate_generation": False,
        "symbol_resolution": {},
        "missing_symbol_explanations": {},
    }
    config = market_data_payload.get("provider_config") if isinstance(market_data_payload.get("provider_config"), dict) else {}
    return {
        "available": True,
        "path": str(market_data_path or ""),
        "status": str(market_data_payload.get("status") or "UNKNOWN").upper(),
        "provider_config": {
            "configured": bool(config.get("configured")),
            "primary": str(config.get("primary") or ""),
            "fallback": str(config.get("fallback") or ""),
            "allow_delayed": bool(config.get("allow_delayed")),
            "require_current_session": bool(config.get("require_current_session")),
            "require_breadth": bool(config.get("require_breadth")),
        },
        "provider_results": market_data_payload.get("provider_results") if isinstance(market_data_payload.get("provider_results"), list) else [],
        "requested_symbols": market_data_payload.get("requested_symbols") if isinstance(market_data_payload.get("requested_symbols"), list) else [],
        "runtime_universe_mode": str(market_data_payload.get("runtime_universe_mode") or ""),
        "production_scan_dataset_id": str(market_data_payload.get("production_scan_dataset_id") or ""),
        "dataset_snapshot_id": str(market_data_payload.get("dataset_snapshot_id") or market_data_payload.get("production_scan_dataset_id") or ""),
        "production_scan_universe_count": int(market_data_payload.get("production_scan_universe_count") or 0),
        "sleeve_required_symbol_count": int(market_data_payload.get("sleeve_required_symbol_count") or 0),
        "total_requested_symbol_count": int(market_data_payload.get("total_requested_symbol_count") or len(market_data_payload.get("requested_symbols") or [])),
        "requested_symbols_source": str(market_data_payload.get("requested_symbols_source") or ""),
        "minimum_viable_runtime_reference_removed": bool(market_data_payload.get("minimum_viable_runtime_reference_removed")),
        "fetched_symbols": market_data_payload.get("fetched_symbols") if isinstance(market_data_payload.get("fetched_symbols"), list) else [],
        "missing_symbols": market_data_payload.get("missing_symbols") if isinstance(market_data_payload.get("missing_symbols"), list) else [],
        "stale_symbols": market_data_payload.get("stale_symbols") if isinstance(market_data_payload.get("stale_symbols"), list) else [],
        "mapping_missing_symbols": market_data_payload.get("mapping_missing_symbols") if isinstance(market_data_payload.get("mapping_missing_symbols"), list) else [],
        "provider_failed_symbols": market_data_payload.get("provider_failed_symbols") if isinstance(market_data_payload.get("provider_failed_symbols"), list) else [],
        "missing_fields": market_data_payload.get("missing_fields") if isinstance(market_data_payload.get("missing_fields"), list) else [],
        "stale_fields": market_data_payload.get("stale_fields") if isinstance(market_data_payload.get("stale_fields"), list) else [],
        "failure_reason": market_data_payload.get("failure_reason"),
        "usable_for_candidate_generation": bool(market_data_payload.get("usable_for_candidate_generation")),
        "symbol_resolution": market_data_payload.get("symbol_resolution") if isinstance(market_data_payload.get("symbol_resolution"), dict) else {},
        "missing_symbol_explanations": market_data_payload.get("missing_symbol_explanations") if isinstance(market_data_payload.get("missing_symbol_explanations"), dict) else {},
    }


def _raw_signal_rejections(
    *,
    candidate_manifest_payload: dict[str, Any],
    candidate_lineage_payload: dict[str, Any],
    intent_arbitration_payload: dict[str, Any],
    selected_intent_promotion_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    manifest_rows = candidate_manifest_payload.get("candidate_rows") if isinstance(candidate_manifest_payload.get("candidate_rows"), list) else []
    if not manifest_rows:
        return []
    lineage_rows = candidate_lineage_payload.get("lineage_rows") if isinstance(candidate_lineage_payload.get("lineage_rows"), list) else []
    arbitration_rejections = intent_arbitration_payload.get("rejected_or_filtered_intents") if isinstance(intent_arbitration_payload.get("rejected_or_filtered_intents"), list) else []
    selected_intent = intent_arbitration_payload.get("selected_intent") if isinstance(intent_arbitration_payload.get("selected_intent"), dict) else {}
    promoted_intent_ids = _promoted_intent_ids(selected_intent_promotion_payload)
    lineage_by_candidate_id = {
        str(row.get("candidate_id") or ""): row
        for row in lineage_rows
        if isinstance(row, dict) and str(row.get("candidate_id") or "")
    }
    arbitration_by_intent_id = {
        str(row.get("intent_id") or ""): row
        for row in arbitration_rejections
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    }
    selected_intent_id = str(selected_intent.get("intent_id") or "").strip()
    if selected_intent_id:
        arbitration_by_intent_id[selected_intent_id] = selected_intent
    out: list[dict[str, Any]] = []
    for row in manifest_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").strip().upper() != "CANDIDATE_CREATED":
            continue
        raw_signal_id = str(row.get("raw_intent_id") or row.get("raw_intent_hash") or row.get("candidate_id") or "").strip()
        if not raw_signal_id:
            continue
        candidate_id = str(row.get("candidate_id") or "").strip()
        sleeve_id = str(row.get("engine_id") or row.get("sleeve_id") or "UNKNOWN").strip().upper()
        symbol = str(row.get("symbol_or_pair") or row.get("symbol") or "").strip().upper()
        lineage = lineage_by_candidate_id.get(candidate_id, {})
        arbitration = arbitration_by_intent_id.get(str(row.get("raw_intent_id") or ""), {})
        if str(row.get("raw_intent_id") or "").strip() in promoted_intent_ids:
            continue
        if _raw_signal_was_promoted(lineage= lineage, arbitration=arbitration):
            continue
        rejection_reason = _raw_signal_rejection_reason(row=row, lineage=lineage, arbitration=arbitration)
        rejection_stage = _raw_signal_rejection_stage(rejection_reason)
        source_paths = _merge_unique(
            [str(row.get("raw_intent_path") or ""), str(arbitration.get("intent_path") or "")],
            [str(path) for path in row.get("output_artifact_paths") or []],
        )
        out.append(
            {
                "raw_signal_id": raw_signal_id,
                "candidate_id": candidate_id,
                "sleeve_id": sleeve_id,
                "symbol": symbol,
                "rejection_stage": rejection_stage,
                "rejection_reason": rejection_reason,
                "human_readable_explanation": _raw_signal_human_explanation(
                    sleeve_id=sleeve_id,
                    symbol=symbol,
                    rejection_reason=rejection_reason,
                    lineage=lineage,
                    arbitration=arbitration,
                ),
                "required_next_action": _raw_signal_next_action(rejection_reason),
                "rejection_classification": _raw_signal_rejection_classification(rejection_reason),
                "expected_rejection": _raw_signal_rejection_classification(rejection_reason) == "EXPECTED",
                "safety_related": _raw_signal_rejection_classification(rejection_reason) == "SAFETY_RELATED",
                "error": _raw_signal_rejection_classification(rejection_reason) == "ERROR",
                "lifecycle_decision": str(row.get("lifecycle_decision") or arbitration.get("lifecycle_decision") or ""),
                "lifecycle_reason_codes": row.get("lifecycle_reason_codes") if isinstance(row.get("lifecycle_reason_codes"), list) else arbitration.get("lifecycle_reason_codes") if isinstance(arbitration.get("lifecycle_reason_codes"), list) else [],
                "portfolio_scoring_status": str(arbitration.get("portfolio_scoring_status") or ""),
                "portfolio_score_total": _optional_text(arbitration.get("portfolio_score_total")),
                "portfolio_score_rank": arbitration.get("portfolio_score_rank"),
                "portfolio_scoring_path": str(arbitration.get("portfolio_scoring_path") or intent_arbitration_payload.get("portfolio_scoring_path") or ""),
                "intent_arbitration_status": str(intent_arbitration_payload.get("status") or ""),
                "selected_by_intent_arbitration": selected_intent_id == str(row.get("raw_intent_id") or "").strip() and bool(selected_intent_id),
                "portfolio_gate_decision": str(row.get("portfolio_gate_decision") or arbitration.get("portfolio_gate_decision") or ""),
                "allowed_by_portfolio_gate": bool(row.get("allowed_by_portfolio_gate")),
                "promotion_status": str(lineage.get("promotion_status") or ""),
                "final_state": str(lineage.get("final_state") or ""),
                "block_reasons": lineage.get("block_reasons") if isinstance(lineage.get("block_reasons"), list) else [],
                "source_artifact_paths": [path for path in source_paths if path],
            }
        )
    return out


def _promoted_intent_ids(payload: dict[str, Any]) -> set[str]:
    if not isinstance(payload, dict) or str(payload.get("status") or "") != "PROMOTED_TO_OPERATOR_REVIEW":
        return set()
    ids = {str(payload.get("selected_intent_id") or "").strip()}
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    ids.add(str(candidate.get("intent_id") or candidate.get("raw_signal_id") or "").strip())
    return {item for item in ids if item}


def _selected_intent_promotion_summary(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        return {"available": False, "status": "MISSING", "promoted_candidate_count": 0, "missing_contract_fields": []}
    return {
        "available": True,
        "status": str(payload.get("status") or "UNKNOWN"),
        "promotion_status": str(payload.get("promotion_status") or ""),
        "selected_intent_id": str(payload.get("selected_intent_id") or ""),
        "sleeve_id": str(payload.get("sleeve_id") or ""),
        "symbol": str(payload.get("symbol") or ""),
        "promoted_candidate_count": int(payload.get("promoted_candidate_count") or 0),
        "missing_contract_fields": payload.get("missing_contract_fields") if isinstance(payload.get("missing_contract_fields"), list) else [],
        "promoted_candidate_set_path": str(payload.get("promoted_candidate_set_path") or ""),
        "operator_review_required": bool(payload.get("operator_review_required", True)),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
    }


def _raw_signal_was_promoted(*, lineage: dict[str, Any], arbitration: dict[str, Any]) -> bool:
    if lineage and (lineage.get("consumed_by_eod") is True or str(lineage.get("promotion_status") or "").upper() in {"PROMOTED", "CONSUMED"}):
        return True
    if arbitration and str(arbitration.get("rejection_reason") or "").strip():
        return False
    return False


def _raw_signal_rejection_reason(*, row: dict[str, Any], lineage: dict[str, Any], arbitration: dict[str, Any]) -> str:
    if str(arbitration.get("portfolio_scoring_status") or "").strip().upper() == "SCORED" and int(arbitration.get("portfolio_score_rank") or 0) > 0:
        if not lineage or lineage.get("consumed_by_eod") is not True:
            return "SELECTED_INTENT_NOT_PROMOTED_TO_OPPORTUNITY"
    for value in (
        arbitration.get("rejection_reason"),
        row.get("rejection_reason"),
        lineage.get("promotion_reason"),
    ):
        text = str(value or "").strip().upper()
        if text:
            return text
    block_reasons = [str(code).strip().upper() for code in lineage.get("block_reasons") or [] if str(code).strip()]
    if "RAW_CANDIDATES_EXISTED_NOT_CONSUMED" in block_reasons:
        return "RAW_CANDIDATES_EXISTED_NOT_CONSUMED"
    if "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE" in block_reasons:
        return "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE"
    if "MARKET_SNAPSHOT_PARTIAL" in block_reasons:
        return "MARKET_SNAPSHOT_PARTIAL"
    return "RAW_SIGNAL_NOT_PROMOTED"


def _raw_signal_rejection_stage(rejection_reason: str) -> str:
    reason = str(rejection_reason or "").upper()
    if reason.startswith("PORTFOLIO_SCORING"):
        return "PORTFOLIO_SCORING"
    if reason.startswith("PORTFOLIO_GATE"):
        return "PORTFOLIO_GATE"
    if "PROMOTED" in reason or "NOT_CONSUMED" in reason:
        return "PROMOTION"
    if "MARKET_SNAPSHOT" in reason:
        return "MARKET_CONTEXT_VALIDATION"
    return "CANDIDATE_LIFECYCLE"


def _raw_signal_human_explanation(*, sleeve_id: str, symbol: str, rejection_reason: str, lineage: dict[str, Any], arbitration: dict[str, Any]) -> str:
    reason = str(rejection_reason or "").upper()
    label = f"{sleeve_id} produced a raw {symbol or 'UNKNOWN'} signal"
    if reason == "PORTFOLIO_SCORING_MISSING_INTENT_SCORE":
        return f"{label}, but it was not promoted because portfolio scoring did not produce a score for that intent. Aegis failed closed instead of showing an unscored opportunity."
    if reason == "RAW_CANDIDATES_EXISTED_NOT_CONSUMED":
        return f"{label}, but the EOD/opportunity pipeline did not consume raw candidates into the promoted candidate set."
    if reason == "SELECTED_INTENT_NOT_PROMOTED_TO_OPPORTUNITY":
        score = arbitration.get("portfolio_score_total")
        rank = arbitration.get("portfolio_score_rank")
        score_text = f" (rank {rank}, score {score})" if rank is not None and score is not None else ""
        return f"{label}, and portfolio scoring selected it{score_text}, but no promoted-sleeve/opportunity contract converted the selected intent into an operator-reviewable opportunity. Aegis failed closed instead of fabricating an opportunity."
    if reason == "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE":
        return f"{label}, but the promoted sleeve manifest did not authorize it for the opportunity lifecycle."
    if reason == "MARKET_SNAPSHOT_PARTIAL":
        return f"{label}, but the market snapshot was partial, so the candidate was not surfaced as an opportunity."
    if reason.startswith("PORTFOLIO_GATE"):
        return f"{label}, but portfolio gate policy marked it {reason.replace('_', ' ').lower()}."
    final_state = str(lineage.get("final_state") or "").strip()
    scoring_status = str(arbitration.get("portfolio_scoring_status") or "").strip()
    suffix = f" final_state={final_state}" if final_state else ""
    suffix += f" portfolio_scoring_status={scoring_status}" if scoring_status else ""
    return f"{label}, but it did not pass candidate promotion. reason={reason or 'UNKNOWN'}{suffix}."


def _raw_signal_next_action(rejection_reason: str) -> str:
    reason = str(rejection_reason or "").upper()
    if reason == "PORTFOLIO_SCORING_MISSING_INTENT_SCORE":
        return "Regenerate portfolio scoring/intent arbitration, then rerun candidate diagnostics; do not approve this raw signal manually."
    if reason == "SELECTED_INTENT_NOT_PROMOTED_TO_OPPORTUNITY":
        return "Define or run the governed selected-intent-to-opportunity promotion path; do not manually approve or fabricate this raw signal."
    if reason in {"RAW_CANDIDATES_EXISTED_NOT_CONSUMED", "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE"}:
        return "Regenerate promoted candidate set and candidate lifecycle, then rerun candidate diagnostics."
    if reason == "MARKET_SNAPSHOT_PARTIAL":
        return "Refresh market snapshot inputs, then rerun candidate readiness."
    return "Inspect candidate generation manifest, lineage, and intent arbitration artifacts."


def _raw_signal_rejection_classification(rejection_reason: str) -> str:
    reason = str(rejection_reason or "").upper()
    if reason in {"PORTFOLIO_SCORING_MISSING_INTENT_SCORE", "MARKET_SNAPSHOT_PARTIAL", "PROMOTED_SLEEVE_MANIFEST_INEFFECTIVE", "SELECTED_INTENT_NOT_PROMOTED_TO_OPPORTUNITY"}:
        return "SAFETY_RELATED"
    if reason in {"RAW_CANDIDATES_EXISTED_NOT_CONSUMED", "RAW_SIGNAL_NOT_PROMOTED"}:
        return "ERROR"
    if reason.startswith("PORTFOLIO_GATE"):
        return "EXPECTED"
    return "ERROR"


def _optional_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _raw_signal_rejections_by_sleeve(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sleeve_id = str(row.get("sleeve_id") or "UNKNOWN").strip().upper() or "UNKNOWN"
        out.setdefault(sleeve_id, []).append(row)
    return out


def _sleeve_rows(
    *,
    expected_sleeves: list[str],
    run_sleeves: dict[str, dict[str, Any]],
    candidate_counts: dict[str, int],
    rejected_by_sleeve: dict[str, list[str]],
    decisions: list[Any],
    data_status: str,
    inventory: dict[str, Any],
    sleeve_eval_by_id: dict[str, dict[str, Any]],
    readiness_by_id: dict[str, dict[str, Any]],
    raw_signal_rejections_by_sleeve: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    inventory_by_id = {row["sleeve_id"]: row for row in inventory.get("registered_sleeves") or [] if isinstance(row, dict)}
    all_sleeves = sorted(set(expected_sleeves) | set(run_sleeves) | set(candidate_counts) | set(sleeve_eval_by_id) | {key for key in rejected_by_sleeve if key != "UNKNOWN"})
    all_sleeves = [sleeve for sleeve in all_sleeves if sleeve != SIMULATOR_ENGINE_ID]
    if not all_sleeves and rejected_by_sleeve.get("UNKNOWN"):
        all_sleeves = ["UNKNOWN"]
    rows = []
    for sleeve in all_sleeves:
        inventory_row = inventory_by_id.get(sleeve, {})
        eval_row = sleeve_eval_by_id.get(sleeve, {})
        readiness = readiness_by_id.get(sleeve, {})
        run_info = run_sleeves.get(sleeve, {})
        statuses = {str(item) for item in run_info.get("statuses", set())}
        run_status = _sleeve_run_status(sleeve=sleeve, statuses=statuses, expected_sleeves=expected_sleeves, eval_row=eval_row)
        if readiness and readiness.get("can_run_candidate_generation") is False:
            run_status = "BLOCKED"
        candidate_count = int(candidate_counts.get(sleeve, 0))
        raw_signal_count = _raw_signal_count(run_info=run_info, eval_row=eval_row)
        raw_signal_rejections = raw_signal_rejections_by_sleeve.get(sleeve, [])
        reasons = rejected_by_sleeve.get(sleeve, [])
        reasons = _merge_unique(reasons, [str(row.get("rejection_reason") or "") for row in raw_signal_rejections])
        reasons = _merge_unique(
            reasons,
            [] if raw_signal_rejections else _evaluation_rejection_reasons(eval_row=eval_row, run_status=run_status, candidate_count=candidate_count, raw_signal_count=raw_signal_count),
        )
        rejected_count = max(len(reasons), len(raw_signal_rejections))
        if raw_signal_count > candidate_count and not reasons and candidate_count == 0 and run_status == "RAN":
            reasons = ["NO_QUALIFYING_SETUP"]
            rejected_count = raw_signal_count
        regime_status = _regime_filter_status(sleeve=sleeve, decisions=decisions, run_status=run_status)
        missing_inputs = _merge_unique(_sleeve_missing_inputs(inventory_row=inventory_row, eval_row=eval_row), [str(item) for item in readiness.get("blocking_inputs") or []])
        stale_inputs = _sleeve_stale_inputs(eval_row=eval_row)
        sleeve_data_status = _sleeve_data_status(data_status=data_status, eval_row=eval_row, missing_inputs=missing_inputs, stale_inputs=stale_inputs)
        rows.append(
            {
                "sleeve_id": sleeve,
                "expected_today": sleeve in expected_sleeves,
                "enabled": bool(inventory_row.get("enabled", sleeve in expected_sleeves)),
                "activation_status": inventory_row.get("activation_status", ""),
                "expected_cadence": inventory_row.get("expected_cadence", ""),
                "run_status": run_status,
                "readiness": readiness.get("readiness") or "",
                "contract_status": readiness.get("contract_status") or "",
                "can_run_candidate_generation": readiness.get("can_run_candidate_generation") if readiness else None,
                "required_inputs_status": readiness.get("required_inputs_status") if isinstance(readiness.get("required_inputs_status"), list) else [],
                "optional_inputs_status": readiness.get("optional_inputs_status") if isinstance(readiness.get("optional_inputs_status"), list) else [],
                "blocking_inputs": readiness.get("blocking_inputs") if isinstance(readiness.get("blocking_inputs"), list) else [],
                "warning_inputs": readiness.get("warning_inputs") if isinstance(readiness.get("warning_inputs"), list) else [],
                "runner_command": str(eval_row.get("producer_command") or inventory_row.get("runner_command") or ""),
                "producer_command": str(eval_row.get("producer_command") or inventory_row.get("runner_command") or ""),
                "definition_path": inventory_row.get("definition_path", ""),
                "registry_path": inventory_row.get("registry_path", ""),
                "required_inputs": inventory_row.get("required_inputs") or [],
                "missing_inputs": missing_inputs,
                "stale_inputs": stale_inputs,
                "raw_signal_count": raw_signal_count,
                "raw_signal_rejections": raw_signal_rejections,
                "candidate_count": candidate_count,
                "rejected_count": rejected_count,
                "rejection_reasons": reasons,
                "data_status": sleeve_data_status,
                "regime_filter_status": regime_status,
                "thresholds_applied": ["runtime_ready", "trigger_detected", "evidence_quality", "regime_filter", "generator_available"],
                "reason_no_candidate": _reason_no_candidate(
                    run_status=run_status,
                    candidate_count=candidate_count,
                    rejection_reasons=reasons,
                    data_status=sleeve_data_status,
                    regime_status=regime_status,
                    eval_row=eval_row,
                    readiness=readiness,
                ),
                "next_repair_action": _next_repair_action(run_status=run_status, missing_inputs=missing_inputs, stale_inputs=stale_inputs, eval_row=eval_row),
                "evaluation_status": str(eval_row.get("status") or eval_row.get("current_status") or ""),
                "evaluation_artifact_path": str(eval_row.get("artifact_path") or ""),
                "canonical_blocker": str(eval_row.get("canonical_blocker") or ""),
                "reason_codes": eval_row.get("reason_codes") if isinstance(eval_row.get("reason_codes"), list) else [],
                "allowed_symbols": inventory_row.get("allowed_symbols") or eval_row.get("allowed_symbols") or [],
            }
        )
    return rows


def _sleeve_run_status(*, sleeve: str, statuses: set[str], expected_sleeves: list[str], eval_row: dict[str, Any]) -> str:
    eval_status = str(eval_row.get("status") or eval_row.get("current_status") or "").strip().upper()
    if eval_status == "BLOCKED":
        return "BLOCKED"
    if eval_status in {"NO_INTENT", "INTENT_CREATED", "FILTERED_OUT"}:
        return "RAN"
    if eval_status == "DISABLED":
        return "SKIPPED"
    if "FAILED" in statuses:
        return "FAILED"
    if statuses.intersection({"SUCCESS", "RAN", "COMPLETED"}):
        return "RAN"
    if statuses.intersection({"SKIPPED"}):
        return "SKIPPED"
    if sleeve in expected_sleeves:
        return "NOT_RUN"
    return "UNKNOWN"


def _regime_filter_status(*, sleeve: str, decisions: list[Any], run_status: str) -> str:
    if run_status == "RAN":
        return "PASS"
    reasons = []
    for row in decisions:
        if not isinstance(row, dict):
            continue
        skipped = _strings(row.get("skipped_sleeve_ids"))
        selected = _strings(row.get("selected_sleeve_ids"))
        if sleeve in skipped or sleeve in selected or not selected:
            reason = str(row.get("reason_skipped") or row.get("decision") or "").strip()
            if reason and reason not in reasons:
                reasons.append(reason)
    return ", ".join(reasons[:3]) or "UNKNOWN"


def _raw_signal_count(*, run_info: dict[str, Any], eval_row: dict[str, Any]) -> int:
    explicit = int(run_info.get("raw_signal_count") or 0)
    if explicit:
        return explicit
    outputs = eval_row.get("output_intents") if isinstance(eval_row.get("output_intents"), list) else []
    if outputs:
        return len(outputs)
    if str(eval_row.get("status") or "").strip().upper() == "INTENT_CREATED":
        return 1
    return 0


def _merge_unique(left: list[str], right: list[str]) -> list[str]:
    out = []
    for item in left + right:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _evaluation_rejection_reasons(*, eval_row: dict[str, Any], run_status: str, candidate_count: int, raw_signal_count: int) -> list[str]:
    if not eval_row:
        return []
    status = str(eval_row.get("status") or "").strip().upper()
    blocker = str(eval_row.get("canonical_blocker") or "").strip()
    if run_status == "BLOCKED" and blocker:
        return [blocker]
    if raw_signal_count > 0 and candidate_count == 0:
        return ["candidate lifecycle/ranking did not produce an opportunity candidate"]
    if status == "NO_INTENT":
        return []
    return [str(code) for code in eval_row.get("reason_codes") or [] if str(code).strip() and str(code).strip() != "NO_INTENT_DECLARED"][:3]


def _sleeve_missing_inputs(*, inventory_row: dict[str, Any], eval_row: dict[str, Any]) -> list[str]:
    missing = []
    if inventory_row and not inventory_row.get("definition_path"):
        missing.append("engine_runner_path")
    if not eval_row:
        missing.append("sleeve_evaluation_artifact")
    manifest = eval_row.get("market_data_manifest_check") if isinstance(eval_row.get("market_data_manifest_check"), dict) else {}
    for symbol in manifest.get("missing_in_both_truth_roots") or []:
        missing.append(f"market_data_snapshot_v1:{symbol}")
    for symbol in manifest.get("missing_in_local_truth_root") or []:
        if symbol in (manifest.get("present_in_canonical_truth_root") or []):
            missing.append(f"local_market_data_snapshot_v1:{symbol}")
    blocker = str(eval_row.get("canonical_blocker") or "").strip()
    if blocker and "MISSING" in blocker and blocker not in missing:
        missing.append(blocker)
    return sorted(set(str(item) for item in missing if str(item)))


def _sleeve_stale_inputs(*, eval_row: dict[str, Any]) -> list[str]:
    stale = []
    if eval_row.get("stale_artifact_detected") is True:
        artifact_symbol = str(eval_row.get("artifact_symbol") or "").strip()
        stale.append(f"stale_intent_artifact:{artifact_symbol}" if artifact_symbol else "stale_intent_artifact")
    return stale


def _sleeve_data_status(*, data_status: str, eval_row: dict[str, Any], missing_inputs: list[str], stale_inputs: list[str]) -> str:
    if stale_inputs:
        return "STALE"
    if missing_inputs:
        return "MISSING"
    manifest = eval_row.get("market_data_manifest_check") if isinstance(eval_row.get("market_data_manifest_check"), dict) else {}
    if manifest and str(manifest.get("status") or "").upper() == "PASS":
        return "OK"
    if data_status in {"MISSING", "STALE"}:
        return data_status
    return "UNKNOWN" if not eval_row else "OK"


def _next_repair_action(*, run_status: str, missing_inputs: list[str], stale_inputs: list[str], eval_row: dict[str, Any]) -> str:
    if missing_inputs or stale_inputs:
        return REPAIR_COMMAND
    if run_status == "BLOCKED":
        blocker = str(eval_row.get("canonical_blocker") or "sleeve producer").strip()
        return f"Investigate {blocker} and rerun {REPAIR_COMMAND}."
    if run_status == "NOT_RUN":
        return REPAIR_COMMAND
    if run_status == "RAN":
        return "No action needed."
    return "Investigate sleeve generator."


def _reason_no_candidate(
    *,
    run_status: str,
    candidate_count: int,
    rejection_reasons: list[str],
    data_status: str,
    regime_status: str,
    eval_row: dict[str, Any],
    readiness: dict[str, Any] | None = None,
) -> str:
    if candidate_count > 0:
        return ""
    readiness = readiness or {}
    if readiness and readiness.get("can_run_candidate_generation") is False:
        blockers = ", ".join(str(item) for item in readiness.get("blocking_inputs") or [])
        return f"Blocked by sleeve input contract: {blockers or readiness.get('reason') or 'required input unavailable'}."
    if data_status in {"MISSING", "STALE"}:
        return "Required market/runtime data was missing or stale."
    if run_status == "BLOCKED":
        return str(eval_row.get("canonical_blocker") or "Sleeve producer was blocked before candidate generation completed.")
    if run_status in {"NOT_RUN", "SKIPPED"}:
        return regime_status if regime_status != "UNKNOWN" else "Sleeve did not run."
    if run_status == "FAILED":
        return "Sleeve run failed before candidate generation completed."
    if rejection_reasons:
        return "; ".join(rejection_reasons)
    if run_status == "RAN":
        return "Sleeve ran and produced no qualifying setup."
    return "Candidate generation state is unknown."


def _candidate_generation_status(
    *,
    trigger_path: Path | None,
    runs_path: Path | None,
    sleeve_eval_path: Path | None,
    expected_count: int,
    run_count: int,
    runs: list[Any],
    candidates: list[Any],
    sleeves: list[dict[str, Any]],
) -> str:
    if candidates:
        return "RAN"
    if not trigger_path and not runs_path and not sleeve_eval_path:
        return "UNKNOWN"
    if run_count == 0:
        return "NOT_RUN"
    if any(row.get("run_status") in {"BLOCKED", "FAILED", "NOT_RUN", "UNKNOWN"} for row in sleeves):
        return "PARTIAL"
    if any(isinstance(row, dict) and str(row.get("status") or "").upper() == "FAILED" for row in runs):
        return "PARTIAL"
    if expected_count and run_count < expected_count:
        return "PARTIAL"
    return "RAN"


def _trigger_evaluation(*, trigger_path: Path | None, trigger_payload: dict[str, Any]) -> dict[str, Any]:
    decisions = trigger_payload.get("decisions") if isinstance(trigger_payload.get("decisions"), list) else []
    activated = [row for row in decisions if isinstance(row, dict) and row.get("decision") == "RUN_SLEEVES"]
    skipped = [row for row in decisions if isinstance(row, dict) and row.get("decision") != "RUN_SLEEVES"]
    reason = ""
    if not trigger_path:
        reason = "Trigger/event evaluation artifact is missing."
    elif activated:
        reason = "Event/regime triggers were evaluated and at least one trigger activated sleeve evaluation."
    else:
        reason = "Event/regime triggers were evaluated; no trigger activated sleeve evaluation."
    return {
        "ran": bool(trigger_path and trigger_payload),
        "trigger_count": len(decisions),
        "activated_count": len(activated),
        "skipped_count": len(skipped),
        "reason": reason,
    }


def _operator_interpretation(
    *,
    status: str,
    total_candidates: int,
    total_sleeves_expected: int,
    total_sleeves_run: int,
    data_status: str,
    sleeves: list[dict[str, Any]],
    readiness_payload: dict[str, Any] | None = None,
) -> str:
    readiness_payload = readiness_payload or {}
    ready_total = int(readiness_payload.get("ready_count") or 0) + int(readiness_payload.get("ready_with_warnings_count") or 0)
    blocked_total = int(readiness_payload.get("blocked_count") or 0)
    unknown_total = int(readiness_payload.get("unknown_count") or 0)
    contract_missing = any(str(row.get("contract_status") or "").upper() == "CONTRACT_MISSING" for row in readiness_payload.get("sleeves") or [] if isinstance(row, dict))
    if contract_missing:
        return "CONTRACT_BLOCKED"
    if readiness_payload and ready_total == 0 and blocked_total > 0:
        return "DATA_BLOCKED"
    if total_candidates > 0:
        return "PARTIAL_RUN" if blocked_total else "NORMAL_NO_SIGNAL"
    if blocked_total and total_sleeves_run > 0:
        return "PARTIAL_RUN"
    if not readiness_payload and data_status in {"MISSING", "STALE"}:
        return "DATA_BLOCKED"
    if total_sleeves_expected > 0 and total_sleeves_run == 0:
        return "ENGINE_NOT_RUN"
    if unknown_total:
        return "PARTIAL_CONTEXT"
    if status == "PARTIAL" or any(row.get("run_status") in {"FAILED", "UNKNOWN"} for row in sleeves):
        return "PARTIAL_CONTEXT"
    if status == "RAN":
        return "NORMAL_NO_SIGNAL"
    if status == "NOT_RUN":
        return "ENGINE_NOT_RUN"
    return "SUSPICIOUS_NO_SIGNAL"


def _zero_candidate_explanation(
    *,
    total_candidates: int,
    total_rejected: int,
    status: str,
    interpretation: str,
    total_sleeves_run: int,
    data_status: str,
    raw_signal_rejections: list[dict[str, Any]] | None = None,
) -> str:
    if total_candidates > 0:
        return "Candidates were generated; no zero-candidate explanation is required."
    raw_signal_rejections = raw_signal_rejections or []
    if interpretation == "PARTIAL_RUN" and raw_signal_rejections:
        return f"Partial run completed: {total_sleeves_run} sleeves ran, 0 opportunities were generated, and {total_rejected} raw signal(s) were rejected before opportunity promotion."
    if interpretation == "CONTRACT_BLOCKED":
        return "No candidates found because one or more sleeve input contracts are missing."
    if interpretation == "PARTIAL_RUN":
        return f"No candidates after {total_sleeves_run} sleeves ran; some sleeves were blocked by their own input contracts."
    if interpretation == "DATA_BLOCKED" or data_status in {"MISSING", "STALE"}:
        return "No candidates found because required data was missing/stale."
    if interpretation == "ENGINE_NOT_RUN" or total_sleeves_run == 0:
        return "No candidates found because candidate generation did not run sleeves."
    if interpretation == "PARTIAL_CONTEXT" or status == "PARTIAL":
        return "Only part of the candidate pipeline ran."
    if interpretation == "NORMAL_NO_SIGNAL":
        return "Candidate generation ran. No sleeves produced qualifying candidates."
    return "No candidates found, but candidate generation did not fully run."


def _recommended_next_steps(*, interpretation: str, data_status: str, status: str, total_candidates: int, total_rejected: int) -> list[str]:
    if total_candidates > 0:
        return ["Review generated candidates."]
    if interpretation == "NORMAL_NO_SIGNAL":
        steps = ["No action needed."]
        if total_rejected:
            steps.append("Review rejected raw-signal reasons.")
        return steps
    if interpretation == "CONTRACT_BLOCKED":
        return ["Build sleeve input contracts.", "Run sleeve readiness.", "Run candidate diagnostics."]
    if interpretation == "PARTIAL_RUN":
        return ["Review blocked sleeve input contracts.", "Run sleeve readiness.", "No action needed for sleeves that ran cleanly."]
    if interpretation == "DATA_BLOCKED" or data_status in {"MISSING", "STALE"}:
        return ["Refresh runtime evidence.", "Check missing data feed.", "Run candidate diagnostics."]
    if interpretation == "ENGINE_NOT_RUN":
        return ["Run candidate diagnostics.", "Investigate sleeve generator."]
    if status == "PARTIAL":
        return ["Run candidate diagnostics.", "Investigate sleeve generator.", "Refresh runtime evidence."]
    return ["Run candidate diagnostics."]


def _sleeve_run_commands(sleeves: list[dict[str, Any]]) -> dict[str, str]:
    return {str(row.get("sleeve_id")): str(row.get("runner_command") or "") for row in sleeves if row.get("sleeve_id")}


def _producer_commands(*, sleeves: list[dict[str, Any]], runtime_payload: dict[str, Any]) -> list[str]:
    commands = []
    for row in sleeves:
        command = str(row.get("producer_command") or row.get("runner_command") or "").strip()
        if command:
            commands.append(command)
    for source in _runtime_problem_sources(runtime_payload):
        command = str(source.get("generated_by_command") or "").strip()
        if command:
            commands.append(command)
    commands.extend(
        [
            "python3 ops/tools/run_sleeve_evaluation_kernel_v1.py --day_utc <DAY_UTC> --truth_root /home/node/constellation_runtime_data/truth",
            "npm run aegis:candidate-diagnostics",
        ]
    )
    return _merge_unique([], commands)


def _runtime_problem_sources(runtime_payload: dict[str, Any]) -> list[dict[str, Any]]:
    sources = runtime_payload.get("missing_or_stale_sources") if isinstance(runtime_payload.get("missing_or_stale_sources"), list) else []
    return [row for row in sources if isinstance(row, dict) and str(row.get("status") or "").strip().upper() in {"MISSING", "STALE", "INVALID"}]


def _missing_input_artifacts(*, runtime_payload: dict[str, Any], sleeves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for source in _runtime_problem_sources(runtime_payload):
        if str(source.get("status") or "").strip().upper() != "MISSING":
            continue
        artifacts.append(
            {
                "artifact_id": source.get("artifact_id") or "",
                "expected_path": source.get("expected_path") or "",
                "producer_command": source.get("generated_by_command") or "",
                "reason": source.get("reason") or "",
                "required": bool(source.get("required")),
            }
        )
    for row in sleeves:
        for item in row.get("missing_inputs") or []:
            artifacts.append({"sleeve_id": row.get("sleeve_id"), "artifact_id": item, "expected_path": "", "producer_command": row.get("producer_command") or ""})
    return artifacts


def _stale_input_artifacts(*, runtime_payload: dict[str, Any], sleeves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for source in _runtime_problem_sources(runtime_payload):
        status = str(source.get("status") or "").strip().upper()
        if status not in {"STALE", "INVALID"}:
            continue
        artifacts.append(
            {
                "artifact_id": source.get("artifact_id") or "",
                "status": status,
                "expected_path": source.get("expected_path") or "",
                "path": source.get("path") or "",
                "generated_at": source.get("last_modified_at") or "",
                "producer_command": source.get("generated_by_command") or "",
                "reason": source.get("reason") or "",
            }
        )
    for row in sleeves:
        for item in row.get("stale_inputs") or []:
            artifacts.append({"sleeve_id": row.get("sleeve_id"), "artifact_id": item, "path": row.get("evaluation_artifact_path") or ""})
    return artifacts


def _failed_producers(sleeves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failed = []
    for row in sleeves:
        if row.get("run_status") not in {"BLOCKED", "FAILED"}:
            continue
        failed.append(
            {
                "sleeve_id": row.get("sleeve_id"),
                "run_status": row.get("run_status"),
                "producer_command": row.get("producer_command") or row.get("runner_command") or "",
                "canonical_blocker": row.get("canonical_blocker") or "",
                "reason_codes": row.get("reason_codes") or [],
                "next_repair_action": row.get("next_repair_action") or REPAIR_COMMAND,
            }
        )
    return failed


def _exact_blocker(*, sleeve_eval_path: Path | None, sleeves: list[dict[str, Any]], runtime_payload: dict[str, Any], inventory: dict[str, Any]) -> str:
    if inventory.get("total_sleeves_enabled") and inventory.get("total_sleeves_enabled") != len(inventory.get("expected_sleeves") or []):
        return "SLEEVE_REGISTRY_MISMATCH"
    if not sleeve_eval_path:
        return "SLEEVE_RUNNER_NOT_CALLED"
    readiness_rows = [row for row in sleeves if row.get("readiness")]
    if readiness_rows:
        contract_missing = any(str(row.get("contract_status") or "").upper() == "CONTRACT_MISSING" for row in readiness_rows)
        blocked = [row for row in readiness_rows if str(row.get("readiness") or "").upper() == "BLOCKED"]
        ran = [row for row in readiness_rows if row.get("run_status") == "RAN"]
        if contract_missing:
            return "CONTRACT_BLOCKED"
        if blocked and ran:
            return "SLEEVE_INPUT_REQUIREMENT_BLOCKED"
        if blocked and not ran:
            return _blocker_from_inputs([item for row in blocked for item in row.get("blocking_inputs") or []])
    for source in _runtime_problem_sources(runtime_payload):
        artifact_id = str(source.get("artifact_id") or "").strip()
        if artifact_id == "event_market_snapshot":
            status = str(source.get("status") or "").upper()
            reason = str(source.get("reason") or "").upper()
            if status == "MISSING":
                return "MARKET_CONTEXT_MISSING"
            if "MISSING_INPUT" in reason:
                return "MARKET_DATA_MISSING"
            return "STALE_DAY_MISMATCH"
    for source in _runtime_problem_sources(runtime_payload):
        artifact_id = str(source.get("artifact_id") or "").strip()
        if artifact_id in {"event_rules_registry", "event_monitoring_status", "event_validity_gate"}:
            return "EVENT_PACKET_MISSING"
    if any(row.get("run_status") == "BLOCKED" for row in sleeves):
        return "PRODUCER_FAILED"
    if any(row.get("missing_inputs") for row in sleeves):
        return "MARKET_DATA_MISSING"
    return ""


def _blocker_from_inputs(inputs: list[str]) -> str:
    normalized = [str(item).upper() for item in inputs]
    if any("MARKET.PRICE" in item for item in normalized):
        return "SYMBOL_DATA_MISSING"
    if any("VOLATILITY.VIX" in item or item.endswith(".VIX") for item in normalized):
        return "VIX_DATA_MISSING"
    if any("BREADTH" in item for item in normalized):
        return "BREADTH_DATA_MISSING"
    return "SLEEVE_INPUT_REQUIREMENT_BLOCKED"


def _cannot_repair_reason(*, runtime_payload: dict[str, Any], sleeves: list[dict[str, Any]]) -> str:
    for source in _runtime_problem_sources(runtime_payload):
        artifact_id = str(source.get("artifact_id") or "").strip()
        if artifact_id == "event_market_snapshot":
            return "DATA_REQUIRED: event_market_snapshot_v1 requires current SPY, QQQ, VIX, breadth, and trend inputs; repair will not fabricate them."
    for row in sleeves:
        if row.get("missing_inputs"):
            return f"DATA_REQUIRED: {row.get('sleeve_id')} missing {', '.join(row.get('missing_inputs') or [])}."
    return ""


def _strings(value: Any, *, preserve_case: bool = False) -> list[str]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = str(item).strip()
        if text:
            out.append(text if preserve_case else text.upper())
    return out
