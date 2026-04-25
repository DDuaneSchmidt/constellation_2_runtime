from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any, Callable, Dict, Mapping


PIPELINE_PHASES: tuple[str, ...] = (
    "resolve_inputs",
    "evaluate",
    "persist_outputs",
    "project_emit",
    "measure_phase_boundaries",
)

PIPELINE_AUTHORITY_LABEL = "governed_obligation_pipeline"
MEASUREMENT_VERSION = "constellation_2.common.bounded_obligation_pipeline_v1"

_BUDGET_TABLE: dict[str, dict[str, dict[str, int]]] = {
    "contract_default": {
        "paper_day_orchestrator_v2:normal": {"soft_total_ms": 5000, "hard_total_ms": 60000},
        "paper_day_orchestrator_v2:exact_ref_replay": {"soft_total_ms": 500, "hard_total_ms": 1500},
        "paper_day_orchestrator_v2:bounded_recompute": {"soft_total_ms": 1000, "hard_total_ms": 2500},
        "c2_ops_cockpit_status_v2_collector_v1:normal": {"soft_total_ms": 800, "hard_total_ms": 2000},
    },
    "strict_validation": {
        "paper_day_orchestrator_v2:normal": {"soft_total_ms": 1, "hard_total_ms": 1},
        "paper_day_orchestrator_v2:exact_ref_replay": {"soft_total_ms": 1, "hard_total_ms": 1},
        "paper_day_orchestrator_v2:bounded_recompute": {"soft_total_ms": 1, "hard_total_ms": 1},
        "c2_ops_cockpit_status_v2_collector_v1:normal": {"soft_total_ms": 1, "hard_total_ms": 1},
    },
}


class PipelineBlockedError(RuntimeError):
    pass


@dataclass(frozen=True)
class PipelineBudgetV1:
    profile: str
    soft_total_ms: int
    hard_total_ms: int


def pipeline_budget_v1(*, pipeline_id: str, pipeline_mode: str, profile: str) -> PipelineBudgetV1:
    profile_key = str(profile or "").strip() or "contract_default"
    profile_table = _BUDGET_TABLE.get(profile_key)
    if profile_table is None:
        raise PipelineBlockedError(f"PIPELINE_BUDGET_PROFILE_UNKNOWN:{profile_key}")
    mode_key = f"{str(pipeline_id).strip()}:{str(pipeline_mode).strip()}"
    budget = profile_table.get(mode_key)
    if budget is None:
        raise PipelineBlockedError(f"PIPELINE_BUDGET_UNDEFINED:{mode_key}:{profile_key}")
    return PipelineBudgetV1(
        profile=profile_key,
        soft_total_ms=int(budget["soft_total_ms"]),
        hard_total_ms=int(budget["hard_total_ms"]),
    )


def _phase_row(*, phase_name: str, status: str, elapsed_ms: int, detail: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "phase_name": phase_name,
        "status": status,
        "elapsed_ms": int(elapsed_ms),
        "detail": dict(detail or {}),
    }


def blocked_pipeline_report_v1(
    *,
    pipeline_id: str,
    pipeline_mode: str,
    target_path_family: str,
    blocked_reason: str,
    budget_profile: str,
) -> Dict[str, Any]:
    phase_rows = [
        _phase_row(
            phase_name="resolve_inputs",
            status="BLOCKED",
            elapsed_ms=0,
            detail={"blocked_reason": str(blocked_reason)},
        ),
        _phase_row(
            phase_name="measure_phase_boundaries",
            status="BLOCKED",
            elapsed_ms=0,
            detail={
                "budget_profile": str(budget_profile).strip() or "contract_default",
                "soft_total_ms": None,
                "hard_total_ms": None,
                "total_elapsed_ms": 0,
            },
        ),
    ]
    proof = {
        "pipeline_id": str(pipeline_id).strip(),
        "pipeline_mode": str(pipeline_mode).strip(),
        "target_path_family": str(target_path_family).strip(),
        "phase_results": phase_rows,
        "phase_timings": {phase: 0 for phase in PIPELINE_PHASES},
        "governing_refs": [],
        "status": "BLOCKED",
        "blocked_reason": str(blocked_reason),
        "warning_codes": [],
        "authority_label": PIPELINE_AUTHORITY_LABEL,
        "measurement_version": MEASUREMENT_VERSION,
        "budget_profile": str(budget_profile).strip() or "contract_default",
        "budget_summary": {
            "budget_profile": str(budget_profile).strip() or "contract_default",
            "soft_total_ms": None,
            "hard_total_ms": None,
            "total_elapsed_ms": 0,
        },
    }
    return {
        "ok": False,
        "proof": proof,
        "resolved": {},
        "evaluated": {},
        "persisted": {},
        "projected": {},
    }


def execute_bounded_obligation_pipeline_v1(
    *,
    pipeline_id: str,
    pipeline_mode: str,
    target_path_family: str,
    budget_profile: str,
    resolve_inputs: Callable[[], Mapping[str, Any]],
    evaluate: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    persist_outputs: Callable[[Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]],
    project_emit: Callable[[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]],
) -> Dict[str, Any]:
    budget = pipeline_budget_v1(pipeline_id=pipeline_id, pipeline_mode=pipeline_mode, profile=budget_profile)
    phase_rows: list[dict[str, Any]] = []
    timings: dict[str, int] = {}

    resolved: Mapping[str, Any] = {}
    evaluated: Mapping[str, Any] = {}
    persisted: Mapping[str, Any] = {}
    projected: Mapping[str, Any] = {}
    blocked_reason: str | None = None
    warning_codes: list[str] = []

    total_started = perf_counter()
    try:
        for phase_name, callback in (
            ("resolve_inputs", lambda: resolve_inputs()),
            ("evaluate", lambda: evaluate(resolved)),
            ("persist_outputs", lambda: persist_outputs(resolved, evaluated)),
            ("project_emit", lambda: project_emit(resolved, evaluated, persisted)),
        ):
            phase_started = perf_counter()
            try:
                result = callback()
            except PipelineBlockedError as exc:
                elapsed_ms = int((perf_counter() - phase_started) * 1000)
                timings[phase_name] = elapsed_ms
                phase_rows.append(
                    _phase_row(
                        phase_name=phase_name,
                        status="BLOCKED",
                        elapsed_ms=elapsed_ms,
                        detail={"blocked_reason": str(exc)},
                    )
                )
                blocked_reason = str(exc)
                break
            except Exception as exc:
                elapsed_ms = int((perf_counter() - phase_started) * 1000)
                timings[phase_name] = elapsed_ms
                blocked_reason = f"PIPELINE_PHASE_EXCEPTION:{phase_name}:{type(exc).__name__}:{exc}"
                phase_rows.append(
                    _phase_row(
                        phase_name=phase_name,
                        status="BLOCKED",
                        elapsed_ms=elapsed_ms,
                        detail={"blocked_reason": blocked_reason},
                    )
                )
                break
            elapsed_ms = int((perf_counter() - phase_started) * 1000)
            timings[phase_name] = elapsed_ms
            phase_rows.append(_phase_row(phase_name=phase_name, status="OK", elapsed_ms=elapsed_ms))
            if phase_name == "resolve_inputs":
                resolved = dict(result)
            elif phase_name == "evaluate":
                evaluated = dict(result)
            elif phase_name == "persist_outputs":
                persisted = dict(result)
            elif phase_name == "project_emit":
                projected = dict(result)
    finally:
        total_elapsed_ms = int((perf_counter() - total_started) * 1000)

    measure_detail = {
        "budget_profile": budget.profile,
        "soft_total_ms": budget.soft_total_ms,
        "hard_total_ms": budget.hard_total_ms,
        "total_elapsed_ms": total_elapsed_ms,
    }
    if blocked_reason is None and total_elapsed_ms > budget.hard_total_ms:
        blocked_reason = f"PIPELINE_PERFORMANCE_HARD_FAIL:{pipeline_id}:{pipeline_mode}:{total_elapsed_ms}>{budget.hard_total_ms}"
    elif total_elapsed_ms > budget.soft_total_ms:
        warning_codes.append(f"PIPELINE_PERFORMANCE_SOFT_WARN:{pipeline_id}:{pipeline_mode}:{total_elapsed_ms}>{budget.soft_total_ms}")
    timings["measure_phase_boundaries"] = 0
    phase_rows.append(
        _phase_row(
            phase_name="measure_phase_boundaries",
            status="OK" if blocked_reason is None else "BLOCKED",
            elapsed_ms=0,
            detail=measure_detail,
        )
    )

    proof = {
        "pipeline_id": str(pipeline_id).strip(),
        "pipeline_mode": str(pipeline_mode).strip(),
        "target_path_family": str(target_path_family).strip(),
        "phase_results": phase_rows,
        "phase_timings": {phase: int(timings.get(phase, 0)) for phase in PIPELINE_PHASES},
        "governing_refs": list(projected.get("governing_refs") or persisted.get("governing_refs") or evaluated.get("governing_refs") or resolved.get("governing_refs") or []),
        "status": "BLOCKED" if blocked_reason else "OK",
        "blocked_reason": blocked_reason,
        "warning_codes": warning_codes,
        "authority_label": PIPELINE_AUTHORITY_LABEL,
        "measurement_version": MEASUREMENT_VERSION,
        "budget_profile": budget.profile,
        "budget_summary": measure_detail,
    }
    return {
        "ok": blocked_reason is None,
        "proof": proof,
        "resolved": dict(resolved),
        "evaluated": dict(evaluated),
        "persisted": dict(persisted),
        "projected": dict(projected),
    }
