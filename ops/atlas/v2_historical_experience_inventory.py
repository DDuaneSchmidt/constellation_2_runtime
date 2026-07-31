from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

INVENTORY_VERSION = "atlas_v2_historical_experience_inventory_v1"
DEFAULT_DAY = "2026-06-04"
SUPPORTED_SOURCE_TYPES = {
    "FAILURE",
    "OBSERVATION",
    "KNOWLEDGE",
    "DECISION",
    "CONTRADICTION",
    "RESEARCH_OUTCOME",
}
KNOWN_SOURCE_LOCATIONS = (
    Path("research_journal/failures"),
    Path("research_journal/observations"),
    Path("research_journal/knowledge"),
    Path("research_journal/reports"),
    Path("docs/aegis"),
    Path("reports"),
)
TEXT_SUFFIXES = {".md", ".txt", ".yaml", ".yml", ".json", ".jsonl", ".csv"}
YAML_SUFFIXES = {".yaml", ".yml"}
UNKNOWN_VALUES = {"", "unknown", "none", "n/a", "null"}
EXPECTATION_KEYS = {
    "expected",
    "expectation",
    "expected_outcome",
    "what_we_expected",
    "hypothesis",
    "claim",
    "thesis",
    "decision",
    "statement",
}
OUTCOME_KEYS = {
    "actual",
    "actual_outcome",
    "what_failed",
    "outcome",
    "result",
    "evidence",
    "observed",
    "observation",
    "conclusion",
}
EXPECTATION_PATTERNS = (
    "expected",
    "expectation",
    "hypothesis",
    "claim",
    "thesis",
    "decision",
    "intended",
    "supporting evidence",
)
OUTCOME_PATTERNS = (
    "actual",
    "outcome",
    "result",
    "failed",
    "failure",
    "contradictory evidence",
    "observed",
    "conclusion",
    "review result",
)


@dataclass(frozen=True)
class HistoricalInventoryCandidate:
    source_path: str
    source_type: str
    has_expectation: bool
    has_outcome: bool
    has_provenance: bool
    conversion_eligibility: str
    estimated_experience_quality_score: float
    reason: str


def build_historical_inventory(repo_root: Path, *, source_locations: list[Path] | None = None, day: str = DEFAULT_DAY) -> dict[str, Any]:
    repo_root = Path(repo_root)
    locations = source_locations or list(KNOWN_SOURCE_LOCATIONS)
    candidates = [_classify_path(repo_root, path) for path in _iter_known_source_files(repo_root, locations)]
    candidates.sort(key=lambda item: (-item.estimated_experience_quality_score, item.source_path))
    eligible = [item for item in candidates if item.conversion_eligibility == "ELIGIBLE"]
    incomplete = [item for item in candidates if item.conversion_eligibility.startswith("INCOMPLETE_")]
    unsupported = [item for item in candidates if item.conversion_eligibility == "UNSUPPORTED"]
    scanned = [str(location) for location in locations if _resolve(repo_root, location).exists()]
    payload = {
        "schema_id": INVENTORY_VERSION,
        "schema_version": "v1",
        "day_utc": day,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "authority_boundary": {
            "inventory_only": True,
            "conversion_performed": False,
            "experience_events_created": 0,
            "trading_authority": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "sleeve_authority": False,
            "candidate_authority": False,
            "paper_position_authority": False,
            "capital_allocation_authority": False,
            "recommendation_authority": False,
            "validation_authority": False,
        },
        "directories_scanned": scanned,
        "historical_records_discovered": len(candidates),
        "eligible_count": len(eligible),
        "incomplete_count": len(incomplete),
        "unsupported_count": len(unsupported),
        "top_20_eligible_artifacts": [asdict(item) for item in eligible[:20]],
        "quality_score_distribution": _quality_distribution(candidates),
        "source_coverage_by_directory": _source_coverage(candidates, scanned),
        "candidate_artifacts": [asdict(item) for item in candidates],
        "incomplete_reason_distribution": _reason_distribution(incomplete),
    }
    return payload


def write_historical_inventory_report(repo_root: Path, *, output_root: Path | None = None, day: str = DEFAULT_DAY, source_locations: list[Path] | None = None) -> dict[str, Path]:
    repo_root = Path(repo_root)
    payload = build_historical_inventory(repo_root, source_locations=source_locations, day=day)
    root = Path(output_root) if output_root is not None else repo_root / "reports" / "atlas_v2_historical_inventory" / day
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "historical_inventory.v1.json"
    summary_path = root / "historical_inventory_summary.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_historical_inventory_summary(payload), encoding="utf-8")
    return {"json": json_path, "summary": summary_path}


def render_historical_inventory_summary(payload: dict[str, Any]) -> str:
    lines = [
        "# Atlas V2 Historical Experience Inventory",
        "",
        f"day_utc: {payload['day_utc']}",
        f"historical_records_discovered: {payload['historical_records_discovered']}",
        f"eligible_count: {payload['eligible_count']}",
        f"incomplete_count: {payload['incomplete_count']}",
        f"unsupported_count: {payload['unsupported_count']}",
        "conversion_performed: false",
        "experience_events_created: 0",
        "",
        "## Directories Scanned",
    ]
    for directory in payload["directories_scanned"]:
        coverage = payload["source_coverage_by_directory"].get(directory, {})
        lines.append(f"- {directory}: {coverage.get('candidate_count', 0)} candidates, {coverage.get('eligible_count', 0)} eligible")
    lines.extend(["", "## Quality Distribution"])
    for bucket, count in payload["quality_score_distribution"].items():
        lines.append(f"- {bucket}: {count}")
    lines.extend(["", "## Top Eligible Artifacts"])
    for item in payload["top_20_eligible_artifacts"]:
        lines.append(f"- {item['source_path']} ({item['source_type']}, score={item['estimated_experience_quality_score']}) - {item['reason']}")
    if not payload["top_20_eligible_artifacts"]:
        lines.append("- none")
    lines.extend(["", "## Incomplete Reasons"])
    for reason, count in payload["incomplete_reason_distribution"].items():
        lines.append(f"- {reason}: {count}")
    lines.append("")
    return "\n".join(lines)


def _iter_known_source_files(repo_root: Path, locations: list[Path]):
    for location in locations:
        root = _resolve(repo_root, location)
        if not root.exists():
            continue
        if root.is_file():
            yield root
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.name != ".gitkeep":
                yield path


def _classify_path(repo_root: Path, path: Path) -> HistoricalInventoryCandidate:
    rel = _relative(repo_root, path)
    suffix = path.suffix.lower()
    has_provenance = path.exists() and path.is_file() and path.stat().st_size > 0
    if suffix and suffix not in TEXT_SUFFIXES:
        return HistoricalInventoryCandidate(rel, "UNKNOWN", False, False, has_provenance, "UNSUPPORTED", 0.2 if has_provenance else 0.0, "Unsupported non-text artifact for historical experience conversion.")
    data = _load_structured(path)
    text = _read_text(path)
    source_type = _classify_source_type(rel, text, data)
    has_expectation = _has_expectation(data, text, source_type)
    has_outcome = _has_outcome(data, text, source_type)
    eligibility = _eligibility(source_type, has_expectation, has_outcome, has_provenance)
    score = _quality_score(has_expectation=has_expectation, has_outcome=has_outcome, has_provenance=has_provenance, source_type=source_type, text=text, data=data)
    reason = _reason(source_type, eligibility, has_expectation, has_outcome, has_provenance)
    return HistoricalInventoryCandidate(rel, source_type, has_expectation, has_outcome, has_provenance, eligibility, score, reason)


def _classify_source_type(rel: str, text: str, data: dict[str, Any]) -> str:
    lowered = rel.lower()
    text_lower = text.lower()
    if "/failures/" in f"/{lowered}":
        return "FAILURE"
    if "/observations/" in f"/{lowered}":
        return "OBSERVATION"
    if "/knowledge/" in f"/{lowered}":
        return "KNOWLEDGE"
    if "contradict" in text_lower or "misdiagnosis" in lowered or "hostile_review" in lowered:
        return "CONTRADICTION"
    if "decision" in lowered or re.search(r"\bdecision\b", text_lower):
        return "DECISION"
    if "/reports/" in f"/{lowered}" or lowered.startswith("reports/"):
        if any(word in text_lower for word in ("outcome", "result", "review", "evidence", "retrospective")):
            return "RESEARCH_OUTCOME"
    if lowered.startswith("docs/aegis/"):
        if any(word in text_lower for word in ("adr", "decision", "standard", "protocol", "review", "invariant")):
            return "KNOWLEDGE"
    if any(key in data for key in ("what_we_expected", "what_failed")):
        return "FAILURE"
    return "UNKNOWN"


def _has_expectation(data: dict[str, Any], text: str, source_type: str) -> bool:
    if source_type == "OBSERVATION" and not any(key in data for key in EXPECTATION_KEYS):
        return False
    if _has_meaningful_key(data, EXPECTATION_KEYS):
        return True
    lowered = text.lower()
    return any(pattern in lowered for pattern in EXPECTATION_PATTERNS)


def _has_outcome(data: dict[str, Any], text: str, source_type: str) -> bool:
    if data and source_type == "KNOWLEDGE":
        return _has_meaningful_key(data, {"evidence", "actual_outcome", "outcome", "result"})
    if data and source_type == "OBSERVATION":
        return _has_meaningful_key(data, {"observation", "actual_outcome", "outcome", "result"})
    if _has_meaningful_key(data, OUTCOME_KEYS):
        return True
    lowered = text.lower()
    return any(pattern in lowered for pattern in OUTCOME_PATTERNS)


def _eligibility(source_type: str, has_expectation: bool, has_outcome: bool, has_provenance: bool) -> str:
    if source_type not in SUPPORTED_SOURCE_TYPES:
        return "UNSUPPORTED"
    if not has_provenance:
        return "INCOMPLETE_PROVENANCE"
    if not has_expectation:
        return "INCOMPLETE_EXPECTATION"
    if not has_outcome:
        return "INCOMPLETE_OUTCOME"
    return "ELIGIBLE"


def _quality_score(*, has_expectation: bool, has_outcome: bool, has_provenance: bool, source_type: str, text: str, data: dict[str, Any]) -> float:
    components = [
        1.0 if has_provenance else 0.0,
        1.0 if has_expectation else 0.0,
        1.0 if has_outcome else 0.0,
        1.0 if source_type in SUPPORTED_SOURCE_TYPES else 0.0,
        1.0 if len(text.strip()) > 80 or len(data) >= 3 else 0.4,
    ]
    return round(sum(components) / len(components), 6)


def _reason(source_type: str, eligibility: str, has_expectation: bool, has_outcome: bool, has_provenance: bool) -> str:
    if eligibility == "ELIGIBLE":
        return "Artifact has supported source type, expectation evidence, outcome evidence, and local provenance."
    if eligibility == "UNSUPPORTED":
        return "Artifact does not map to a supported historical experience source type."
    missing = []
    if not has_expectation:
        missing.append("expectation")
    if not has_outcome:
        missing.append("outcome")
    if not has_provenance:
        missing.append("provenance")
    return f"Artifact is {source_type} but lacks explicit {', '.join(missing)} evidence."


def _load_structured(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    try:
        if suffix in YAML_SUFFIXES and yaml is not None:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
    return {}


def _read_text(path: Path) -> str:
    try:
        if path.suffix.lower() not in TEXT_SUFFIXES:
            return ""
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _has_meaningful_key(data: dict[str, Any], keys: set[str]) -> bool:
    for key, value in data.items():
        if str(key).lower() in keys and _meaningful(value):
            return True
    return False


def _meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return str(value).strip().lower() not in UNKNOWN_VALUES


def _quality_distribution(candidates: list[HistoricalInventoryCandidate]) -> dict[str, int]:
    distribution = {"0.0-0.25": 0, "0.25-0.5": 0, "0.5-0.75": 0, "0.75-1.0": 0}
    for item in candidates:
        score = item.estimated_experience_quality_score
        if score < 0.25:
            distribution["0.0-0.25"] += 1
        elif score < 0.5:
            distribution["0.25-0.5"] += 1
        elif score < 0.75:
            distribution["0.5-0.75"] += 1
        else:
            distribution["0.75-1.0"] += 1
    return distribution


def _source_coverage(candidates: list[HistoricalInventoryCandidate], directories: list[str]) -> dict[str, dict[str, int]]:
    coverage = {directory: {"candidate_count": 0, "eligible_count": 0, "incomplete_count": 0, "unsupported_count": 0} for directory in directories}
    for item in candidates:
        directory = next((candidate_dir for candidate_dir in directories if item.source_path.startswith(candidate_dir.rstrip("/") + "/") or item.source_path == candidate_dir), "")
        if not directory:
            continue
        coverage[directory]["candidate_count"] += 1
        if item.conversion_eligibility == "ELIGIBLE":
            coverage[directory]["eligible_count"] += 1
        elif item.conversion_eligibility == "UNSUPPORTED":
            coverage[directory]["unsupported_count"] += 1
        else:
            coverage[directory]["incomplete_count"] += 1
    return coverage


def _reason_distribution(candidates: list[HistoricalInventoryCandidate]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for item in candidates:
        distribution[item.conversion_eligibility] = distribution.get(item.conversion_eligibility, 0) + 1
    return distribution


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _relative(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inventory known Atlas/AEGIS historical artifacts without conversion.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--day", default=DEFAULT_DAY)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--source-root", action="append", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    paths = write_historical_inventory_report(
        args.repo_root,
        output_root=args.output_root,
        day=args.day,
        source_locations=args.source_root,
    )
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
