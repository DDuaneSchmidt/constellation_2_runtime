from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .research_event_bus_v1 import log_event_transition_v1, resolve_runtime_root
from .research_event_v1 import utc_now_iso


AI_HYPOTHESIS_BATCH_SCHEMA_VERSION = "ai_hypothesis_batch.v1"
EDGE_HYPOTHESIS_SCHEMA_VERSION = "edge_hypothesis.v1"
SANDBOX_TEST_PLAN_SCHEMA_VERSION = "sandbox_test_plan.v1"

REPO_ROOT = Path(__file__).resolve().parents[2]

IDEA_ID_RE = re.compile(r"^EDGE-\d{4}-\d{4}$")
_ALLOWED_MARKETS = {"US_EQUITIES", "US_OPTIONS", "INDEX_OPTIONS", "CROSS_ASSET"}
_ALLOWED_TEST_TYPES = {"BACKTEST", "WALK_FORWARD", "SANDBOX_REPLAY"}

_DIRECT_TRADE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bbuy\s+[A-Z]{1,6}\b",
        r"\bsell\s+[A-Z]{1,6}\b",
        r"\bgo\s+long\b",
        r"\bgo\s+short\b",
        r"\bplace\s+(a\s+)?(trade|order)\b",
        r"\bsubmit\s+(a\s+)?(trade|order)\b",
        r"\benter\s+(a\s+)?position\b",
        r"\bexit\s+(a\s+)?position\b",
        r"\bopen\s+(a\s+)?position\b",
    )
]

_FORBIDDEN_CONTROL_MARKERS = {
    "enable paper",
    "paper promotion",
    "promote live",
    "live promotion",
    "place live trade",
    "override gate",
    "bypass gate",
    "disable gate",
    "modify aegis core",
    "change risk",
    "change capital",
    "risk override",
    "capital override",
}

_FORBIDDEN_DATA_MARKERS = {
    "mnpi",
    "material non-public",
    "insider",
    "private orderflow",
    "customer order flow",
    "non-public",
    "forbidden data",
}

_UNAVAILABLE_DATA_MARKERS = {
    "unavailable_data",
    "external paid alt data unavailable",
    "not available in aegis",
    "requires unavailable",
}


class AIHypothesisBatchValidationError(ValueError):
    pass


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _guard_runtime_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if _is_under(resolved, REPO_ROOT):
        raise ValueError(f"RESEARCH_RUNTIME_WRITE_UNDER_REPO_FORBIDDEN:{resolved}")
    runtime_root = resolve_runtime_root()
    if not _is_under(resolved, runtime_root):
        raise ValueError(f"RESEARCH_RUNTIME_WRITE_OUTSIDE_ROOT_FORBIDDEN:{resolved}")
    return resolved


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    target = _guard_runtime_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return target


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_NOT_OBJECT:{path}")
    return payload


def runtime_ideas_validated_root() -> Path:
    return _guard_runtime_path((resolve_runtime_root() / "ideas" / "validated").resolve())


def runtime_ideas_rejected_root() -> Path:
    return _guard_runtime_path((resolve_runtime_root() / "ideas" / "rejected").resolve())


def runtime_tests_queued_root() -> Path:
    return _guard_runtime_path((resolve_runtime_root() / "tests" / "queued").resolve())


def runtime_tests_root() -> Path:
    return _guard_runtime_path((resolve_runtime_root() / "tests").resolve())


def edge_hypothesis_validated_path(idea_id: str) -> Path:
    return (runtime_ideas_validated_root() / f"{idea_id}.edge_hypothesis.v1.json").resolve()


def edge_hypothesis_rejected_path(idea_id: str) -> Path:
    return (runtime_ideas_rejected_root() / f"{idea_id}.edge_hypothesis.v1.json").resolve()


def sandbox_test_plan_path(test_id: str) -> Path:
    return (runtime_tests_queued_root() / f"{test_id}.sandbox_test_plan.v1.json").resolve()


def validate_ai_hypothesis_batch_v1(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise AIHypothesisBatchValidationError("AI_HYPOTHESIS_BATCH_TOP_LEVEL_NOT_OBJECT")
    required = {"schema_version", "source_packet_path", "generated_utc", "hypotheses"}
    missing = sorted([field for field in required if field not in payload])
    if missing:
        raise AIHypothesisBatchValidationError(f"AI_HYPOTHESIS_BATCH_MISSING_FIELDS:{','.join(missing)}")
    if payload.get("schema_version") != AI_HYPOTHESIS_BATCH_SCHEMA_VERSION:
        raise AIHypothesisBatchValidationError("AI_HYPOTHESIS_BATCH_SCHEMA_VERSION_MISMATCH")
    if not isinstance(payload.get("source_packet_path"), str) or not str(payload.get("source_packet_path")).strip():
        raise AIHypothesisBatchValidationError("AI_HYPOTHESIS_BATCH_SOURCE_PACKET_PATH_INVALID")
    if not isinstance(payload.get("generated_utc"), str) or not str(payload.get("generated_utc")).strip():
        raise AIHypothesisBatchValidationError("AI_HYPOTHESIS_BATCH_GENERATED_UTC_INVALID")
    hypotheses = payload.get("hypotheses")
    if not isinstance(hypotheses, list) or not hypotheses:
        raise AIHypothesisBatchValidationError("AI_HYPOTHESIS_BATCH_HYPOTHESES_INVALID")
    if any(not isinstance(item, dict) for item in hypotheses):
        raise AIHypothesisBatchValidationError("AI_HYPOTHESIS_BATCH_HYPOTHESIS_NOT_OBJECT")
    return dict(payload)


def _contains_direct_trade_instruction(text: str) -> bool:
    value = str(text or "")
    return any(pattern.search(value) is not None for pattern in _DIRECT_TRADE_PATTERNS)


def _contains_marker(text: str, markers: set[str]) -> bool:
    lowered = str(text or "").strip().lower()
    return any(marker in lowered for marker in markers)


def _collect_text_values(hypothesis: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field in (
        "hypothesis",
        "expected_edge_mechanism",
        "edge_type",
        "market",
    ):
        values.append(str(hypothesis.get(field) or ""))
    for field in ("features_required", "data_required", "rejection_criteria", "risk_notes"):
        raw = hypothesis.get(field)
        if isinstance(raw, list):
            values.extend([str(item or "") for item in raw])
    test_design = hypothesis.get("test_design") if isinstance(hypothesis.get("test_design"), dict) else {}
    values.extend([f"{k}:{v}" for k, v in test_design.items()])
    return values


def _required_hypothesis_fields() -> set[str]:
    return {
        "idea_id",
        "hypothesis",
        "expected_edge_mechanism",
        "market",
        "edge_type",
        "instruments",
        "features_required",
        "data_required",
        "test_design",
        "success_criteria",
        "rejection_criteria",
        "risk_notes",
        "forbidden_if",
    }


def evaluate_hypothesis_record_v1(hypothesis: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    normalized = dict(hypothesis)
    reasons: list[str] = []

    missing = sorted([field for field in _required_hypothesis_fields() if field not in normalized])
    if missing:
        reasons.append(f"MISSING_REQUIRED_FIELDS:{','.join(missing)}")

    idea_id = str(normalized.get("idea_id") or "").strip()
    if not IDEA_ID_RE.match(idea_id):
        reasons.append("IDEA_ID_FORMAT_INVALID")

    for field in ("hypothesis", "expected_edge_mechanism", "edge_type"):
        if not isinstance(normalized.get(field), str) or not str(normalized.get(field)).strip():
            reasons.append(f"FIELD_EMPTY:{field}")

    market = str(normalized.get("market") or "").strip().upper()
    if market not in _ALLOWED_MARKETS:
        reasons.append("MARKET_UNSUPPORTED")

    for field in ("instruments", "features_required", "data_required", "risk_notes", "forbidden_if"):
        raw = normalized.get(field)
        if not isinstance(raw, list):
            reasons.append(f"FIELD_NOT_LIST:{field}")
        elif field == "instruments" and not raw:
            reasons.append("INSTRUMENTS_EMPTY")

    success_criteria = normalized.get("success_criteria")
    if not isinstance(success_criteria, dict) or not success_criteria:
        reasons.append("SUCCESS_CRITERIA_MISSING")
    else:
        required_success_fields = {
            "min_trade_count",
            "min_expectancy_after_costs",
            "max_drawdown_limit",
            "min_generalization_ratio",
        }
        if any(field not in success_criteria for field in required_success_fields):
            reasons.append("SUCCESS_CRITERIA_INCOMPLETE")

    rejection_criteria = normalized.get("rejection_criteria")
    if not isinstance(rejection_criteria, list) or not rejection_criteria:
        reasons.append("REJECTION_CRITERIA_MISSING")

    test_design = normalized.get("test_design") if isinstance(normalized.get("test_design"), dict) else None
    if test_design is None:
        reasons.append("TEST_DESIGN_MISSING")
    else:
        test_type = str(test_design.get("test_type") or "").strip().upper()
        if test_type not in _ALLOWED_TEST_TYPES:
            reasons.append("TEST_TYPE_UNSUPPORTED")
        if not isinstance(test_design.get("in_sample_period"), str) or not str(test_design.get("in_sample_period")).strip():
            reasons.append("IN_SAMPLE_PERIOD_MISSING")
        if not isinstance(test_design.get("out_of_sample_period"), str) or not str(test_design.get("out_of_sample_period")).strip():
            reasons.append("OUT_OF_SAMPLE_PERIOD_MISSING")
        if test_design.get("cost_model_required") is not True:
            reasons.append("COST_MODEL_NOT_REQUIRED")
        if test_design.get("slippage_model_required") is not True:
            reasons.append("SLIPPAGE_MODEL_NOT_REQUIRED")
        if test_design.get("walk_forward_required") is not True:
            reasons.append("WALK_FORWARD_NOT_REQUIRED")

    collected_text = _collect_text_values(normalized)
    if any(_contains_direct_trade_instruction(text) for text in collected_text):
        reasons.append("DIRECT_TRADE_INSTRUCTION_FORBIDDEN")
    if any(_contains_marker(text, _FORBIDDEN_CONTROL_MARKERS) for text in collected_text):
        reasons.append("GATE_RISK_CORE_OVERRIDE_FORBIDDEN")
    if any(_contains_marker(text, _FORBIDDEN_DATA_MARKERS) for text in collected_text):
        reasons.append("FORBIDDEN_DATA_REQUIRED")
    if any(_contains_marker(text, _UNAVAILABLE_DATA_MARKERS) for text in collected_text):
        reasons.append("UNAVAILABLE_DATA_REQUIRED")

    normalized["idea_id"] = idea_id
    normalized["market"] = market
    if isinstance(normalized.get("test_design"), dict):
        normalized["test_design"] = dict(normalized["test_design"])
        normalized["test_design"]["test_type"] = str(normalized["test_design"].get("test_type") or "").strip().upper()
    return normalized, sorted(set(reasons))


def evaluate_ai_hypothesis_batch_v1(payload: dict[str, Any]) -> dict[str, Any]:
    batch = validate_ai_hypothesis_batch_v1(payload)
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for index, hypothesis in enumerate(batch["hypotheses"]):
        normalized, reasons = evaluate_hypothesis_record_v1(dict(hypothesis))
        idea_id = str(normalized.get("idea_id") or f"EDGE-0000-{index:04d}")
        if reasons:
            rejected.append(
                {
                    "idea_id": idea_id,
                    "reasons": reasons,
                    "hypothesis": normalized,
                }
            )
        else:
            accepted.append(normalized)

    return {
        "batch": batch,
        "accepted": accepted,
        "rejected": rejected,
    }


def to_edge_hypothesis_v1(*, hypothesis: dict[str, Any], source_packet_path: str, generated_utc: str) -> dict[str, Any]:
    return {
        "schema_version": EDGE_HYPOTHESIS_SCHEMA_VERSION,
        "idea_id": str(hypothesis["idea_id"]),
        "created_utc": str(generated_utc),
        "source": "AI_RESEARCH",
        "status": "VALIDATED",
        "hypothesis": str(hypothesis["hypothesis"]),
        "expected_edge_mechanism": str(hypothesis["expected_edge_mechanism"]),
        "market": str(hypothesis["market"]),
        "instruments": [str(item) for item in (hypothesis.get("instruments") or [])],
        "edge_type": str(hypothesis["edge_type"]),
        "features_required": [str(item) for item in (hypothesis.get("features_required") or [])],
        "data_required": [str(item) for item in (hypothesis.get("data_required") or [])],
        "test_design": dict(hypothesis.get("test_design") or {}),
        "success_criteria": dict(hypothesis.get("success_criteria") or {}),
        "rejection_criteria": list(hypothesis.get("rejection_criteria") or []),
        "risk_notes": {
            "notes": list(hypothesis.get("risk_notes") or []),
            "source_packet_path": str(source_packet_path),
            "forbidden_if": list(hypothesis.get("forbidden_if") or []),
        },
        "human_approval_required_for_paper": True,
    }


def rejected_edge_hypothesis_v1(
    *,
    hypothesis: dict[str, Any],
    source_packet_path: str,
    generated_utc: str,
    reasons: list[str],
) -> dict[str, Any]:
    idea_id = str(hypothesis.get("idea_id") or "EDGE-0000-0000")
    return {
        "schema_version": EDGE_HYPOTHESIS_SCHEMA_VERSION,
        "idea_id": idea_id,
        "created_utc": str(generated_utc),
        "source": "AI_RESEARCH",
        "status": "AUTO_REJECTED",
        "hypothesis": str(hypothesis.get("hypothesis") or ""),
        "expected_edge_mechanism": str(hypothesis.get("expected_edge_mechanism") or ""),
        "market": str(hypothesis.get("market") or "UNKNOWN"),
        "instruments": [str(item) for item in (hypothesis.get("instruments") or [])],
        "edge_type": str(hypothesis.get("edge_type") or "UNKNOWN"),
        "features_required": [str(item) for item in (hypothesis.get("features_required") or [])],
        "data_required": [str(item) for item in (hypothesis.get("data_required") or [])],
        "test_design": dict(hypothesis.get("test_design") or {}),
        "success_criteria": dict(hypothesis.get("success_criteria") or {}),
        "rejection_criteria": list(hypothesis.get("rejection_criteria") or []),
        "risk_notes": {
            "notes": list(hypothesis.get("risk_notes") or []),
            "source_packet_path": str(source_packet_path),
            "rejected_reasons": sorted(set([str(reason) for reason in reasons])),
        },
        "human_approval_required_for_paper": True,
    }


def write_validated_edge_hypothesis_v1(edge_payload: dict[str, Any]) -> Path:
    idea_id = str(edge_payload["idea_id"])
    return _write_json(edge_hypothesis_validated_path(idea_id), edge_payload)


def write_rejected_edge_hypothesis_v1(edge_payload: dict[str, Any]) -> Path:
    idea_id = str(edge_payload["idea_id"])
    return _write_json(edge_hypothesis_rejected_path(idea_id), edge_payload)


def _stable_test_id(idea_id: str) -> str:
    digest = hashlib.sha256(str(idea_id).encode("utf-8")).hexdigest()[:12]
    return f"test_{idea_id}_{digest}"


def build_sandbox_test_plan_v1(edge_payload: dict[str, Any]) -> dict[str, Any]:
    idea_id = str(edge_payload.get("idea_id") or "")
    test_design = edge_payload.get("test_design") if isinstance(edge_payload.get("test_design"), dict) else {}
    success_criteria = edge_payload.get("success_criteria") if isinstance(edge_payload.get("success_criteria"), dict) else {}
    return {
        "schema_version": SANDBOX_TEST_PLAN_SCHEMA_VERSION,
        "test_id": _stable_test_id(idea_id),
        "idea_id": idea_id,
        "status": "TEST_QUEUED",
        "test_type": str(test_design.get("test_type") or "BACKTEST"),
        "data_inputs": [str(item) for item in (edge_payload.get("data_required") or [])],
        "in_sample_period": str(test_design.get("in_sample_period") or "INFER_FROM_DATA"),
        "out_of_sample_period": str(test_design.get("out_of_sample_period") or ""),
        "walk_forward_required": bool(test_design.get("walk_forward_required", True)),
        "cost_model_required": bool(test_design.get("cost_model_required", True)),
        "slippage_model_required": bool(test_design.get("slippage_model_required", True)),
        "no_lookahead_check_required": True,
        "survivorship_bias_check_required": True,
        "metrics_required": sorted([str(key) for key in success_criteria.keys()])
        if success_criteria
        else ["min_trade_count", "min_expectancy_after_costs", "max_drawdown_limit", "min_generalization_ratio"],
        "promotion_allowed": False,
    }


def write_sandbox_test_plan_v1(plan_payload: dict[str, Any]) -> Path:
    test_id = str(plan_payload["test_id"])
    return _write_json(sandbox_test_plan_path(test_id), plan_payload)


def _idea_has_any_test_plan_v1(idea_id: str) -> bool:
    root = runtime_tests_root()
    for bucket in ("queued", "running", "completed", "failed"):
        bucket_root = (root / bucket).resolve()
        if not bucket_root.exists() or not bucket_root.is_dir():
            continue
        for path in bucket_root.rglob("*.sandbox_test_plan.v1.json"):
            try:
                payload = _read_json(path)
            except Exception:
                continue
            if str(payload.get("idea_id") or "") == str(idea_id):
                return True
    return False


def queue_edge_hypothesis_v1(edge_payload: dict[str, Any]) -> dict[str, Any]:
    idea_id = str(edge_payload.get("idea_id") or "")
    if _idea_has_any_test_plan_v1(idea_id):
        return {
            "queued": False,
            "reason": "TEST_PLAN_ALREADY_EXISTS",
            "idea_id": idea_id,
        }
    plan = build_sandbox_test_plan_v1(edge_payload)
    path = write_sandbox_test_plan_v1(plan)
    log_event_transition_v1(
        event_id=plan["test_id"],
        event_type="RESEARCH_TEST_QUEUE",
        day_utc=str(edge_payload.get("created_utc") or utc_now_iso())[:10],
        from_status="VALIDATED",
        to_status="TEST_QUEUED",
        reason="AI_HYPOTHESIS_ACCEPTED_AND_QUEUED",
        details={"idea_id": idea_id, "test_plan_path": str(path)},
    )
    return {
        "queued": True,
        "idea_id": idea_id,
        "test_id": plan["test_id"],
        "test_plan_path": str(path),
    }


def list_validated_edge_hypotheses_v1() -> list[dict[str, Any]]:
    root = runtime_ideas_validated_root()
    if not root.exists() or not root.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.edge_hypothesis.v1.json")):
        try:
            rows.append(_read_json(path))
        except Exception:
            continue
    return rows


def find_validated_edge_hypothesis_by_id_v1(idea_id: str) -> dict[str, Any] | None:
    path = edge_hypothesis_validated_path(str(idea_id))
    if not path.exists() or not path.is_file():
        return None
    try:
        return _read_json(path)
    except Exception:
        return None


def intake_ai_hypothesis_batch_v1(
    batch_payload: dict[str, Any],
    *,
    queue_tests: bool = False,
) -> dict[str, Any]:
    evaluated = evaluate_ai_hypothesis_batch_v1(batch_payload)
    batch = evaluated["batch"]
    generated_utc = str(batch["generated_utc"])
    source_packet_path = str(batch["source_packet_path"])

    accepted_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    queue_rows: list[dict[str, Any]] = []
    rejection_reason_counts: dict[str, int] = {}

    for hypothesis in evaluated["accepted"]:
        edge_payload = to_edge_hypothesis_v1(
            hypothesis=hypothesis,
            source_packet_path=source_packet_path,
            generated_utc=generated_utc,
        )
        idea_path = write_validated_edge_hypothesis_v1(edge_payload)
        log_event_transition_v1(
            event_id=str(edge_payload["idea_id"]),
            event_type="AI_HYPOTHESIS_INTAKE",
            day_utc=str(generated_utc)[:10],
            from_status="AI_SUBMITTED",
            to_status="VALIDATED",
            reason="HYPOTHESIS_ACCEPTED",
            details={"idea_path": str(idea_path)},
        )
        accepted_rows.append({"idea_id": edge_payload["idea_id"], "idea_path": str(idea_path)})
        if queue_tests:
            queue_rows.append(queue_edge_hypothesis_v1(edge_payload))

    for rejected in evaluated["rejected"]:
        reasons = list(rejected["reasons"])
        edge_payload = rejected_edge_hypothesis_v1(
            hypothesis=dict(rejected["hypothesis"]),
            source_packet_path=source_packet_path,
            generated_utc=generated_utc,
            reasons=reasons,
        )
        idea_path = write_rejected_edge_hypothesis_v1(edge_payload)
        log_event_transition_v1(
            event_id=str(edge_payload["idea_id"]),
            event_type="AI_HYPOTHESIS_INTAKE",
            day_utc=str(generated_utc)[:10],
            from_status="AI_SUBMITTED",
            to_status="AUTO_REJECTED",
            reason="HYPOTHESIS_REJECTED",
            details={"idea_path": str(idea_path), "reasons": reasons},
        )
        rejected_rows.append({"idea_id": edge_payload["idea_id"], "idea_path": str(idea_path), "reasons": reasons})
        for reason in reasons:
            rejection_reason_counts[reason] = int(rejection_reason_counts.get(reason, 0)) + 1

    return {
        "accepted_count": len(accepted_rows),
        "rejected_count": len(rejected_rows),
        "accepted": accepted_rows,
        "rejected": rejected_rows,
        "rejected_reasons": dict(sorted(rejection_reason_counts.items())),
        "queue_tests": bool(queue_tests),
        "queued": queue_rows,
    }


def queue_all_validated_without_test_v1() -> dict[str, Any]:
    queued: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for edge_payload in list_validated_edge_hypotheses_v1():
        result = queue_edge_hypothesis_v1(edge_payload)
        if result.get("queued") is True:
            queued.append(result)
        else:
            skipped.append(result)
    return {
        "queued_count": len(queued),
        "skipped_count": len(skipped),
        "queued": queued,
        "skipped": skipped,
    }
