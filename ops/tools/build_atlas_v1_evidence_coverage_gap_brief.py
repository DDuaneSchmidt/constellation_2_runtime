from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        _relative_source_path,
        search_by_query,
    )
    from ops.tools.build_atlas_v1_research_synthesis_brief import SUPPORTED_QUERIES
except ModuleNotFoundError:
    from build_atlas_v1_evidence_librarian_brief import DEFAULT_JOURNAL_ROOT, _relative_source_path, search_by_query
    from build_atlas_v1_research_synthesis_brief import SUPPORTED_QUERIES


MAX_ITEMS_PER_SECTION = 64
OBJECT_ID_RE = re.compile(r"\b(?:OBS|KNW|FAIL)_\d{4}\b")
REPORT_NAME_RE = re.compile(r"\b[a-z0-9][a-z0-9_]*_001\.md\b|\bweekly_learning_report\.md\b")
JOURNAL_PATH_RE = re.compile(
    r"research_journal/(?:observations|knowledge|failures)/(?:OBS|KNW|FAIL)_\d{4}\.yaml|"
    r"research_journal/reports/[a-z0-9][a-z0-9_]*\.md"
)


@dataclass(frozen=True)
class ReferencedSource:
    source_path: str
    referenced_by: str
    reference_text: str


@dataclass(frozen=True)
class CoverageGroups:
    retrieved_source_paths: tuple[str, ...]
    referenced_not_retrieved: tuple[ReferencedSource, ...]
    possible_gaps: tuple[str, ...]
    noisy_sources: tuple[str, ...]
    source_paths: tuple[str, ...]


def _path_for_object_id(journal_root: Path, object_id: str) -> Path | None:
    prefix = object_id.split("_", 1)[0]
    directory = {"OBS": "observations", "KNW": "knowledge", "FAIL": "failures"}.get(prefix)
    if directory is None:
        return None
    path = journal_root / directory / f"{object_id}.yaml"
    if path.exists():
        return path
    return None


def _path_for_report_name(journal_root: Path, report_name: str) -> Path | None:
    path = journal_root / "reports" / report_name
    if path.exists():
        return path
    return None


def _relative_or_literal(path: Path | str) -> str:
    if isinstance(path, Path):
        return _relative_source_path(path)
    return path


def _dedupe_append(items: list[str], item: str) -> None:
    if item not in items:
        items.append(item)


def _dedupe_ref_append(items: list[ReferencedSource], item: ReferencedSource) -> None:
    if item not in items:
        items.append(item)


def _retrieved_paths(matches: list) -> tuple[str, ...]:
    paths: list[str] = []
    for match in matches:
        _dedupe_append(paths, _relative_source_path(match.source_path))
    return tuple(paths)


def _text_for(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _reference_context(text: str, needle: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if needle in stripped:
            return stripped
    return needle


def _referenced_sources(journal_root: Path, matches: list, retrieved: set[str]) -> tuple[ReferencedSource, ...]:
    refs: list[ReferencedSource] = []
    for match in matches:
        referring_source = _relative_source_path(match.source_path)
        text = _text_for(match.source_path)
        if not text:
            continue

        for path_text in sorted(set(JOURNAL_PATH_RE.findall(text))):
            if path_text not in retrieved and (journal_root.parent / path_text).exists():
                _dedupe_ref_append(
                    refs,
                    ReferencedSource(
                        source_path=path_text,
                        referenced_by=referring_source,
                        reference_text=_reference_context(text, path_text),
                    ),
                )

        for object_id in sorted(set(OBJECT_ID_RE.findall(text))):
            path = _path_for_object_id(journal_root, object_id)
            if path is None:
                continue
            source_path = _relative_source_path(path)
            if source_path not in retrieved:
                _dedupe_ref_append(
                    refs,
                    ReferencedSource(
                        source_path=source_path,
                        referenced_by=referring_source,
                        reference_text=_reference_context(text, object_id),
                    ),
                )

        for report_name in sorted(set(REPORT_NAME_RE.findall(text))):
            path = _path_for_report_name(journal_root, report_name)
            if path is None:
                continue
            source_path = _relative_source_path(path)
            if source_path not in retrieved:
                _dedupe_ref_append(
                    refs,
                    ReferencedSource(
                        source_path=source_path,
                        referenced_by=referring_source,
                        reference_text=_reference_context(text, report_name),
                    ),
                )
    return tuple(sorted(refs, key=lambda ref: (ref.source_path, ref.referenced_by))[:MAX_ITEMS_PER_SECTION])


def _possible_gaps(matches: list, refs: tuple[ReferencedSource, ...]) -> tuple[str, ...]:
    gaps: list[str] = []
    retrieved_paths = _retrieved_paths(matches)
    direct_objects = [path for path in retrieved_paths if "/observations/" in path or "/knowledge/" in path or "/failures/" in path]
    failures = [path for path in retrieved_paths if "/failures/" in path]
    reports = [path for path in retrieved_paths if "/reports/" in path]
    if not matches:
        gaps.append("No sources retrieved for the query.")
    if refs:
        gaps.append("Retrieved sources reference journal objects or reports that were not directly retrieved.")
    if not direct_objects and matches:
        gaps.append("No direct observation, knowledge, or failure object was retrieved.")
    if not failures and matches:
        gaps.append("No direct failure object was retrieved; negative-knowledge coverage may be indirect.")
    if len(reports) > len(direct_objects) and reports:
        gaps.append("Report coverage exceeds direct object coverage; reviewer may need to separate primary objects from review material.")
    return tuple(gaps[:MAX_ITEMS_PER_SECTION])


def _noise_sources(matches: list) -> tuple[str, ...]:
    noisy: list[str] = []
    for match in matches:
        source = _relative_source_path(match.source_path)
        lowered = source.casefold()
        if "/reports/atlas_" in lowered:
            _dedupe_append(noisy, f"{source}: Atlas meta-review source; useful context but may duplicate primary evidence.")
        elif "/reports/" in lowered and "review" in lowered:
            _dedupe_append(noisy, f"{source}: review report source; may summarize rather than provide primary journal object context.")
    return tuple(noisy[:MAX_ITEMS_PER_SECTION])


def analyze_coverage(journal_root: Path, query: str) -> CoverageGroups:
    if query not in SUPPORTED_QUERIES:
        supported = ", ".join(SUPPORTED_QUERIES)
        raise ValueError(f"Unsupported query: {query}. Supported queries: {supported}")
    matches = search_by_query(journal_root, query)
    retrieved = _retrieved_paths(matches)
    refs = _referenced_sources(journal_root, matches, set(retrieved))
    source_paths = list(retrieved)
    coverage_review = journal_root / "reports" / "atlas_evidence_coverage_review_001.md"
    if coverage_review.exists():
        _dedupe_append(source_paths, _relative_source_path(coverage_review))
    return CoverageGroups(
        retrieved_source_paths=retrieved,
        referenced_not_retrieved=refs,
        possible_gaps=_possible_gaps(matches, refs),
        noisy_sources=_noise_sources(matches),
        source_paths=tuple(source_paths),
    )


def render_markdown(query: str, groups: CoverageGroups) -> str:
    lines = [
        "# Atlas V1 Evidence Coverage Gap Brief",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_COVERAGE_GAP_BRIEF",
        "",
        "This brief checks whether a query may have weak, missing, indirect, or noisy Research Journal coverage before synthesis.",
        "",
        "It does not validate truth, rank evidence strength, infer readiness, recommend trades, allocate capital, mutate candidates, modify rules, or create journal objects.",
        "",
        "Referenced-but-not-retrieved sources are coverage-gap candidates only. This brief does not claim they are relevant evidence.",
        "",
        f"Query: {query}",
        "",
        "# Coverage Summary",
        "",
        f"- Retrieved source count: {len(groups.retrieved_source_paths)}",
        f"- Referenced-but-not-retrieved candidate count: {len(groups.referenced_not_retrieved)}",
        f"- Possible coverage gap count: {len(groups.possible_gaps)}",
        f"- Noise / duplicative source count: {len(groups.noisy_sources)}",
        "",
        "# Retrieved Source Paths",
        "",
    ]
    if groups.retrieved_source_paths:
        for source_path in groups.retrieved_source_paths:
            lines.append(f"- `{source_path}`")
    else:
        lines.append("None.")
    lines.extend(["", "# Referenced But Not Retrieved", ""])
    if groups.referenced_not_retrieved:
        for ref in groups.referenced_not_retrieved:
            lines.append(
                f"- `{ref.source_path}` referenced by `{ref.referenced_by}`: {ref.reference_text}"
            )
    else:
        lines.append("None found by the deterministic reference scan.")
    lines.extend(["", "# Possible Coverage Gaps", ""])
    if groups.possible_gaps:
        for gap in groups.possible_gaps:
            lines.append(f"- {gap}")
    else:
        lines.append("None found by the deterministic coverage rules.")
    lines.extend(["", "# Noise / Duplicative Sources", ""])
    if groups.noisy_sources:
        for source in groups.noisy_sources:
            lines.append(f"- {source}")
    else:
        lines.append("None found by the deterministic noise rules.")
    lines.extend(["", "# Source Paths", ""])
    if groups.source_paths:
        for source_path in groups.source_paths:
            lines.append(f"- `{source_path}`")
    else:
        lines.append("None.")
    lines.append("")
    return "\n".join(lines)


def build_coverage_gap_brief(journal_root: Path, query: str) -> str:
    groups = analyze_coverage(journal_root, query)
    return render_markdown(query, groups)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Evidence Coverage Gap brief.")
    parser.add_argument(
        "--query",
        required=True,
        choices=SUPPORTED_QUERIES,
        help="Research topic query to check for coverage gaps.",
    )
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    print(build_coverage_gap_brief(Path(args.journal_root), args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
