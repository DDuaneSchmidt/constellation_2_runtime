from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        EvidenceMatch,
        _iter_files,
        _relative_source_path,
        search_with_referenced_follow_through,
    )
    from ops.tools.build_atlas_v1_negative_knowledge_brief import retrieve_negative_knowledge
except ModuleNotFoundError:
    from build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        EvidenceMatch,
        _iter_files,
        _relative_source_path,
        search_with_referenced_follow_through,
    )
    from build_atlas_v1_negative_knowledge_brief import retrieve_negative_knowledge


OBJECT_ID_RE = re.compile(r"\b(?:OBS|KNW|FAIL)_\d{4}\b")
JOURNAL_PATH_RE = re.compile(
    r"(?:\./)?(?:research_journal/)?(?:observations|knowledge|failures|reports)/"
    r"[A-Za-z0-9_.\-/]+(?:\.yaml|\.md)"
)
REPORT_FILENAME_RE = re.compile(r"\b[A-Za-z0-9_]+\.md\b")


@dataclass(frozen=True)
class ReferencedSource:
    source_path: str
    referenced_by: tuple[str, ...]


@dataclass(frozen=True)
class CoverageCheck:
    query: str
    retrieved_paths: tuple[str, ...]
    referenced_paths: tuple[ReferencedSource, ...]
    referenced_but_not_retrieved: tuple[ReferencedSource, ...]


def _dedupe(items: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return tuple(deduped)


def _source_path(journal_root: Path, path: Path) -> str:
    try:
        return f"research_journal/{path.relative_to(journal_root).as_posix()}"
    except ValueError:
        return _relative_source_path(path)


def _merge_matches(journal_root: Path, matches: list[EvidenceMatch]) -> tuple[str, ...]:
    return _dedupe([_source_path(journal_root, match.source_path) for match in matches])


def _known_source_index(journal_root: Path) -> dict[str, str]:
    index: dict[str, str] = {}
    for path in _iter_files(journal_root, ("observations", "knowledge", "failures", "reports")):
        source_path = _source_path(journal_root, path)
        index[source_path] = source_path
        index[path.name] = source_path
        if path.suffix == ".yaml":
            index[path.stem] = source_path
    return index


def _normalize_reference(reference: str, known_sources: dict[str, str]) -> str | None:
    candidate = reference.strip().strip("`.,;:)]}")
    candidate = candidate.lstrip("./")
    if candidate in known_sources:
        return known_sources[candidate]
    if not candidate.startswith("research_journal/"):
        prefixed = f"research_journal/{candidate}"
        if prefixed in known_sources:
            return known_sources[prefixed]
    return None


def _references_in_text(text: str, known_sources: dict[str, str]) -> tuple[str, ...]:
    references: list[str] = []
    for pattern in (JOURNAL_PATH_RE, OBJECT_ID_RE, REPORT_FILENAME_RE):
        for match in pattern.finditer(text):
            source_path = _normalize_reference(match.group(0), known_sources)
            if source_path is not None:
                references.append(source_path)
    return _dedupe(references)


def _path_for_source(journal_root: Path, source_path: str) -> Path:
    prefix = "research_journal/"
    if source_path.startswith(prefix):
        return journal_root / source_path[len(prefix) :]
    return journal_root / source_path


def _collect_referenced_sources(journal_root: Path, retrieved_paths: tuple[str, ...]) -> tuple[ReferencedSource, ...]:
    known_sources = _known_source_index(journal_root)
    referrers_by_source: dict[str, list[str]] = {}
    for retrieved_path in retrieved_paths:
        path = _path_for_source(journal_root, retrieved_path)
        if not path.exists() or not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for referenced_path in _references_in_text(text, known_sources):
            referrers_by_source.setdefault(referenced_path, [])
            if retrieved_path not in referrers_by_source[referenced_path]:
                referrers_by_source[referenced_path].append(retrieved_path)

    return tuple(
        ReferencedSource(source_path=source_path, referenced_by=tuple(sorted(referrers)))
        for source_path, referrers in sorted(referrers_by_source.items())
    )


def build_coverage_check(journal_root: Path, query: str) -> CoverageCheck:
    librarian_matches = search_with_referenced_follow_through(journal_root, query)
    negative_matches = retrieve_negative_knowledge(journal_root, query)
    retrieved_paths = _merge_matches(journal_root, librarian_matches + negative_matches)
    referenced_paths = _collect_referenced_sources(journal_root, retrieved_paths)
    retrieved_set = set(retrieved_paths)
    referenced_but_not_retrieved = tuple(
        reference for reference in referenced_paths if reference.source_path not in retrieved_set
    )
    return CoverageCheck(
        query=query,
        retrieved_paths=retrieved_paths,
        referenced_paths=referenced_paths,
        referenced_but_not_retrieved=referenced_but_not_retrieved,
    )


def _render_path_list(lines: list[str], paths: tuple[str, ...], empty_text: str) -> None:
    if not paths:
        lines.append(empty_text)
    else:
        for path in paths:
            lines.append(f"- `{path}`")
    lines.append("")


def _render_referenced_list(lines: list[str], references: tuple[ReferencedSource, ...], empty_text: str) -> None:
    if not references:
        lines.append(empty_text)
    else:
        for reference in references:
            referrers = ", ".join(f"`{path}`" for path in reference.referenced_by)
            lines.append(f"- `{reference.source_path}` (referenced by: {referrers})")
    lines.append("")


def render_markdown(check: CoverageCheck) -> str:
    lines = [
        "# Atlas V1 Evidence Coverage Check",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_COVERAGE_CHECK",
        "",
        "This check compares source paths retrieved for a query with known Research Journal source paths referenced inside those retrieved sources. A referenced-but-not-retrieved path is only a potential coverage gap; this output does not determine relevance, truth, readiness, capital allocation, trade quality, or evidence strength.",
        "",
        f"Query: {check.query}",
        "Ordering: deterministic source-path order from existing Atlas retrieval, followed by referenced source paths sorted lexically. No relevance ranking is applied.",
        "",
        "## Retrieved Source Paths",
        "",
    ]
    _render_path_list(lines, check.retrieved_paths, "None. No matching retrieved sources found.")

    lines.extend(["## Source Paths Referenced Inside Retrieved Sources", ""])
    _render_referenced_list(
        lines,
        check.referenced_paths,
        "None. Retrieved sources did not reference additional known Research Journal source paths.",
    )

    lines.extend(["## Referenced-But-Not-Retrieved Source Paths", ""])
    _render_referenced_list(lines, check.referenced_but_not_retrieved, "None.")

    lines.extend(["## Potential Coverage Gaps", ""])
    if not check.retrieved_paths:
        lines.append("No matching retrieved sources found. Coverage cannot be checked for this query.")
    elif not check.referenced_but_not_retrieved:
        lines.append(
            "None. Retrieved sources did not reference additional known Research Journal source paths that were absent from the retrieval set."
        )
    else:
        for reference in check.referenced_but_not_retrieved:
            referrers = ", ".join(f"`{path}`" for path in reference.referenced_by)
            lines.append(
                f"- Potential gap only: `{reference.source_path}` was referenced by {referrers} but was not retrieved by the query."
            )
    lines.append("")

    return "\n".join(lines)


def build_coverage_brief(journal_root: Path, query: str) -> str:
    return render_markdown(build_coverage_check(journal_root, query))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only Atlas V1 evidence coverage check.")
    parser.add_argument("--query", required=True, help="Keyword query to check for coverage gaps.")
    parser.add_argument(
        "--journal-root",
        type=Path,
        default=DEFAULT_JOURNAL_ROOT,
        help="Research Journal root directory. Defaults to research_journal/.",
    )
    args = parser.parse_args()

    print(build_coverage_brief(args.journal_root, args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
