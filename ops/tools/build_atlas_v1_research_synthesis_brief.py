from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        EvidenceMatch,
        _relative_source_path,
        search_with_referenced_follow_through,
    )
except ModuleNotFoundError:
    from build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        EvidenceMatch,
        _relative_source_path,
        search_with_referenced_follow_through,
    )


LIMITING_TERMS = (
    "blocked",
    "bottleneck",
    "contradict",
    "failed",
    "failure",
    "gap",
    "limitation",
    "limitations",
    "missing",
    "not ",
    "underpowered",
    "unresolved",
    "zero",
)
MAX_ITEMS_PER_SECTION = 12
SUPPORTED_QUERIES = (
    "outcome maturity",
    "candidate volume",
    "capital review readiness",
    "technical strategy evidence",
    "evidence concentration",
)


@dataclass(frozen=True)
class CitedLine:
    source_path: str
    text: str


@dataclass(frozen=True)
class SynthesisGroups:
    direct_evidence: tuple[CitedLine, ...]
    referenced_follow_through: tuple[CitedLine, ...]
    limiting_sources: tuple[CitedLine, ...]
    unresolved_gaps: tuple[CitedLine, ...]
    source_paths: tuple[str, ...]


def _is_failure_source(match: EvidenceMatch) -> bool:
    return "/failures/" in _relative_source_path(match.source_path)


def _contains_limiting_language(text: str) -> bool:
    lowered = text.casefold()
    return any(term in lowered for term in LIMITING_TERMS)


def _dedupe_append(items: list[CitedLine], item: CitedLine) -> None:
    if item not in items:
        items.append(item)


def _cited_lines(match: EvidenceMatch) -> tuple[CitedLine, ...]:
    source = _relative_source_path(match.source_path)
    return tuple(CitedLine(source_path=source, text=snippet) for snippet in match.snippets)


def synthesize_matches(matches: list[EvidenceMatch]) -> SynthesisGroups:
    direct_evidence: list[CitedLine] = []
    referenced_follow_through: list[CitedLine] = []
    limiting_sources: list[CitedLine] = []
    unresolved_gaps: list[CitedLine] = []
    source_paths: list[str] = []

    for match in matches:
        source = _relative_source_path(match.source_path)
        if source not in source_paths:
            source_paths.append(source)

        cited_lines = _cited_lines(match)
        tier_items = referenced_follow_through if match.match_type == "referenced_follow_through" else direct_evidence
        for line in cited_lines:
            _dedupe_append(tier_items, line)
            if _is_failure_source(match) or _contains_limiting_language(line.text):
                _dedupe_append(limiting_sources, line)
            if _contains_limiting_language(line.text):
                _dedupe_append(unresolved_gaps, line)

    return SynthesisGroups(
        direct_evidence=tuple(direct_evidence[:MAX_ITEMS_PER_SECTION]),
        referenced_follow_through=tuple(referenced_follow_through[:MAX_ITEMS_PER_SECTION]),
        limiting_sources=tuple(limiting_sources[:MAX_ITEMS_PER_SECTION]),
        unresolved_gaps=tuple(unresolved_gaps[:MAX_ITEMS_PER_SECTION]),
        source_paths=tuple(source_paths),
    )


def _render_cited_section(lines: list[str], items: tuple[CitedLine, ...], empty_text: str) -> None:
    if not items:
        lines.append(empty_text)
        lines.append("")
        return
    for item in items:
        lines.append(f"- `{item.source_path}`: {item.text}")
    lines.append("")


def render_markdown(query: str, matches: list[EvidenceMatch], groups: SynthesisGroups) -> str:
    lines = [
        "# Evidence Summary",
        "",
        "- Status: NON_AUTHORITATIVE_READ_ONLY_SYNTHESIS",
        f"- Query: {query}",
        "- Retrieval layer: Atlas V1 Research Evidence Librarian.",
        "- Boundary: this brief compresses retrieved Research Journal content only. It does not validate truth, infer readiness, rank evidence, recommend trades, allocate capital, mutate candidates, modify rules, or create journal objects.",
        "- Ordering: deterministic source-path order from the Research Evidence Librarian; no relevance ranking is applied.",
        "",
    ]

    if not matches:
        lines.extend(
            [
                "No matching evidence found. No synthesis is produced beyond this deterministic no-match result.",
                "",
                "## Direct Evidence",
                "",
                "None.",
                "",
                "## Referenced Follow-Through",
                "",
                "None.",
                "",
                "## Contradictory / Limiting Evidence",
                "",
                "None.",
                "",
                "## Open Questions",
                "",
                "None.",
                "",
                "## Source Paths",
                "",
                "None.",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(["## Direct Evidence", ""])
    _render_cited_section(
        lines,
        groups.direct_evidence,
        "None directly retrieved by the query. Review the source paths directly.",
    )
    lines.extend(["## Referenced Follow-Through", ""])
    _render_cited_section(
        lines,
        groups.referenced_follow_through,
        "None surfaced through one-hop referenced object follow-through.",
    )
    lines.extend(
        [
            "## Contradictory / Limiting Evidence",
            "",
            "This section lists retrieved failure sources and retrieved lines with explicit limiting language. It is not an independent contradiction finding.",
            "",
        ]
    )
    _render_cited_section(
        lines,
        groups.limiting_sources,
        "None separated by the deterministic grouping rule. Review the source paths directly.",
    )
    lines.extend(
        [
            "## Open Questions",
            "",
            "This section lists retrieved lines containing explicit gap, blocker, missing, underpowered, unresolved, or limitation language. It is not a new gap claim.",
            "",
        ]
    )
    _render_cited_section(
        lines,
        groups.unresolved_gaps,
        "None separated by the deterministic grouping rule. Review the source paths directly.",
    )
    lines.extend(["## Source Paths", ""])
    for source_path in groups.source_paths:
        lines.append(f"- `{source_path}`")
    lines.append("")
    return "\n".join(lines)


def build_synthesis_brief(journal_root: Path, query: str) -> str:
    if query not in SUPPORTED_QUERIES:
        supported = ", ".join(SUPPORTED_QUERIES)
        raise ValueError(f"Unsupported query: {query}. Supported queries: {supported}")
    matches = search_with_referenced_follow_through(journal_root, query)
    groups = synthesize_matches(matches)
    return render_markdown(query=query, matches=matches, groups=groups)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Research Synthesis brief.")
    parser.add_argument(
        "--query",
        required=True,
        choices=SUPPORTED_QUERIES,
        help="Research topic query to synthesize from existing Research Journal content.",
    )
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    print(build_synthesis_brief(Path(args.journal_root), args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
