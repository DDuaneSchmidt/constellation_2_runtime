from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "validation_vocabulary_bridge"
STATUS_GENERATED_ONLY = "GENERATED_ONLY"

BridgeMappingType = Literal["EXACT_MATCH", "PARTIAL_MATCH", "UNMAPPABLE", "UNKNOWN"]

VALIDATOR_REGIMES = {"HIGH_VOLATILITY", "TRENDING", "RANGE_BOUND", "LOW_VOLATILITY", "UNKNOWN"}
EXPLICIT_MAPPINGS: tuple[tuple[str, str, BridgeMappingType, float, str], ...] = (
    ("TREND", "TRENDING", "EXACT_MATCH", 0.9, "Candidate alias maps to validator TRENDING."),
    ("CHOP", "RANGE_BOUND", "PARTIAL_MATCH", 0.65, "CHOP can overlap daily RANGE_BOUND, but is not equivalent."),
    ("CHOP", "LOW_VOLATILITY", "PARTIAL_MATCH", 0.35, "Some CHOP contexts can be quiet, but this is weak evidence."),
)

AUTHORITY_BOUNDARY = {
    "evaluation_overlay_only": True,
    "generated_only": True,
    "read_only_inputs": True,
    "replay_behavior_changed": False,
    "qualification_changed": False,
    "candidate_changed": False,
    "governance_changed": False,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
}


@dataclass(frozen=True)
class BridgeMapping:
    source_regime: str
    validator_regime: str
    mapping_type: BridgeMappingType
    confidence: float
    notes: str


@dataclass(frozen=True)
class VocabularyBridgeMatch:
    candidate_id: str
    candidate_regime: str
    validator_regime: str
    mapping_type: BridgeMappingType
    confidence: float
    matched: bool
    notes: str


@dataclass(frozen=True)
class BridgeEvaluationSummary:
    candidates_reviewed: int
    candidate_regime_count: int
    exact_match_count: int
    partial_match_count: int
    unmappable_count: int
    unknown_count: int
    generated_only_status: str


@dataclass(frozen=True)
class VocabularyBridgeResult:
    schema_id: str
    schema_version: str
    report_type: str
    status: str
    created_at: str
    day: str
    mappings: list[BridgeMapping]
    matches: list[VocabularyBridgeMatch]
    summary: BridgeEvaluationSummary
    authority_boundary: dict[str, bool]
    guardrails: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "report_type": self.report_type,
            "status": self.status,
            "created_at": self.created_at,
            "day": self.day,
            "mappings": [asdict(row) for row in self.mappings],
            "matches": [asdict(row) for row in self.matches],
            "summary": asdict(self.summary),
            "authority_boundary": dict(self.authority_boundary),
            "guardrails": list(self.guardrails),
        }


def run_validation_vocabulary_bridge(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    result = build_validation_vocabulary_bridge(root=root, created_at=created_at)
    write_validation_vocabulary_bridge(result, root=root)
    return result.to_dict()


def build_validation_vocabulary_bridge(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> VocabularyBridgeResult:
    root_path = Path(root)
    created = created_at or _now()
    candidates = _candidate_rows(root_path)
    mappings = _bridge_mappings()
    matches = [_match_for_candidate(candidate, mappings) for candidate in candidates]
    summary = BridgeEvaluationSummary(
        candidates_reviewed=len(candidates),
        candidate_regime_count=len({row.candidate_regime for row in matches}),
        exact_match_count=sum(row.mapping_type == "EXACT_MATCH" for row in matches),
        partial_match_count=sum(row.mapping_type == "PARTIAL_MATCH" for row in matches),
        unmappable_count=sum(row.mapping_type == "UNMAPPABLE" for row in matches),
        unknown_count=sum(row.mapping_type == "UNKNOWN" for row in matches),
        generated_only_status=STATUS_GENERATED_ONLY,
    )
    return VocabularyBridgeResult(
        schema_id="atlas_v2_research_os_validation_vocabulary_bridge",
        schema_version="1.0",
        report_type="VALIDATION_VOCABULARY_BRIDGE",
        status=STATUS_GENERATED_ONLY,
        created_at=created,
        day=created[:10],
        mappings=mappings,
        matches=matches,
        summary=summary,
        authority_boundary=dict(AUTHORITY_BOUNDARY),
        guardrails=[
            "Evaluation overlay only.",
            "Generated-only output.",
            "No replay behavior changes.",
            "No qualification changes.",
            "No candidate changes.",
            "No governance changes.",
            "No trading, broker, capital, sizing, or paper-placement authority.",
        ],
    )


def write_validation_vocabulary_bridge(result: VocabularyBridgeResult | dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    payload = result.to_dict() if isinstance(result, VocabularyBridgeResult) else dict(result)
    root_path = Path(root) / REPORT_DIRNAME
    day = str(payload.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "bridge_result.json"
    md_path = out_dir / "bridge_result.md"
    latest_json = root_path / "bridge_result.json"
    latest_md = root_path / "bridge_result.md"
    json_payload = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    md_payload = render_validation_vocabulary_bridge_markdown(payload)
    for path in [json_path, latest_json]:
        path.write_text(json_payload, encoding="utf-8")
    for path in [md_path, latest_md]:
        path.write_text(md_payload, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_validation_vocabulary_bridge_markdown(result: dict[str, Any]) -> str:
    summary = result.get("summary", {})
    lines = [
        "# Validation Vocabulary Bridge",
        "",
        f"Status: {result.get('status')}",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Exact matches: {summary.get('exact_match_count')}",
        f"Partial matches: {summary.get('partial_match_count')}",
        f"Unmappable: {summary.get('unmappable_count')}",
        f"Unknown: {summary.get('unknown_count')}",
        "",
        "## Matches",
        "",
    ]
    for row in result.get("matches", []):
        lines.append(
            "- {candidate_id}: {candidate_regime} -> {validator_regime}; {mapping_type}; confidence={confidence}".format(
                candidate_id=row.get("candidate_id"),
                candidate_regime=row.get("candidate_regime"),
                validator_regime=row.get("validator_regime"),
                mapping_type=row.get("mapping_type"),
                confidence=row.get("confidence"),
            )
        )
    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            "Generated-only evaluation overlay. No replay behavior changes, qualification changes, candidate changes, governance changes, trading recommendations, broker execution, capital allocation, position sizing, or paper placement.",
            "",
        ]
    )
    return "\n".join(lines)


def _bridge_mappings() -> list[BridgeMapping]:
    mappings = [BridgeMapping(source, target, mapping_type, confidence, notes) for source, target, mapping_type, confidence, notes in EXPLICIT_MAPPINGS]
    for regime in sorted(VALIDATOR_REGIMES):
        mappings.append(BridgeMapping(regime, regime, "EXACT_MATCH", 1.0, "Candidate label already matches validator vocabulary."))
    return mappings


def _match_for_candidate(candidate: dict[str, Any], mappings: list[BridgeMapping]) -> VocabularyBridgeMatch:
    candidate_id = str(candidate.get("candidate_id") or "UNKNOWN_CANDIDATE")
    regime = _normalize(candidate.get("regime") or candidate.get("candidate_regime") or candidate.get("primary_regime"))
    if not regime:
        return VocabularyBridgeMatch(candidate_id, "UNKNOWN", "UNKNOWN", "UNKNOWN", 0.0, False, "Candidate has no regime label.")
    candidates = [row for row in mappings if row.source_regime == regime]
    if not candidates:
        return VocabularyBridgeMatch(candidate_id, regime, "UNMAPPABLE", "UNMAPPABLE", 0.0, False, "No evaluation bridge mapping exists for this label.")
    best = sorted(candidates, key=lambda row: (_mapping_rank(row.mapping_type), row.confidence), reverse=True)[0]
    return VocabularyBridgeMatch(candidate_id, regime, best.validator_regime, best.mapping_type, best.confidence, True, best.notes)


def _candidate_rows(root: Path) -> list[dict[str, Any]]:
    validation = _read_json(root / "direct_candidate_data_validation" / "latest.json", {})
    rows = list(validation.get("candidate_validations") or [])
    if rows:
        return rows
    plan = _read_json(root / "candidate_data_validation_plan" / "latest.json", {})
    return list(plan.get("candidate_data_validation_plans") or [])


def _mapping_rank(mapping_type: str) -> int:
    return {"EXACT_MATCH": 4, "PARTIAL_MATCH": 3, "UNKNOWN": 2, "UNMAPPABLE": 1}.get(mapping_type, 0)


def _normalize(value: Any) -> str:
    return str(value or "").strip().upper()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
