from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_coverage_gap_brief import analyze_coverage
    from ops.tools.build_atlas_v1_evidence_librarian_brief import DEFAULT_JOURNAL_ROOT, _relative_source_path, search_by_query
    from ops.tools.build_atlas_v1_negative_knowledge_brief import (
        group_negative_knowledge,
        retrieve_negative_knowledge,
    )
    from ops.tools.build_atlas_v1_research_contrarian_brief import build_contrarian_brief
    from ops.tools.build_atlas_v1_research_synthesis_brief import (
        SUPPORTED_QUERIES,
        CitedLine,
        synthesize_matches,
    )
except ModuleNotFoundError:
    from build_atlas_v1_evidence_coverage_gap_brief import analyze_coverage
    from build_atlas_v1_evidence_librarian_brief import DEFAULT_JOURNAL_ROOT, _relative_source_path, search_by_query
    from build_atlas_v1_negative_knowledge_brief import group_negative_knowledge, retrieve_negative_knowledge
    from build_atlas_v1_research_contrarian_brief import build_contrarian_brief
    from build_atlas_v1_research_synthesis_brief import SUPPORTED_QUERIES, CitedLine, synthesize_matches


MAX_ITEMS_PER_SECTION = 10


@dataclass(frozen=True)
class WorkflowPacket:
    query: str
    coverage_classification: str
    retrieved_evidence: tuple[CitedLine, ...]
    synthesized_support: tuple[CitedLine, ...]
    synthesized_contradictions: tuple[CitedLine, ...]
    negative_knowledge: tuple[CitedLine, ...]
    contrarian_lines: tuple[str, ...]
    coverage_gaps: tuple[str, ...]
    referenced_not_retrieved: tuple[str, ...]
    open_questions: tuple[CitedLine, ...]
    source_paths: tuple[str, ...]


def _dedupe_cited(items: tuple[CitedLine, ...]) -> tuple[CitedLine, ...]:
    seen: set[CitedLine] = set()
    deduped: list[CitedLine] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return tuple(deduped)


def _limit_cited(items: tuple[CitedLine, ...]) -> tuple[CitedLine, ...]:
    return _dedupe_cited(items)[:MAX_ITEMS_PER_SECTION]


def _dedupe_strings(items: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    deduped: list[str] = []
    for item in items:
        if item not in deduped:
            deduped.append(item)
    return tuple(deduped)


def _source_paths_from_matches(matches: list) -> tuple[str, ...]:
    return _dedupe_strings(tuple(_relative_source_path(match.source_path) for match in matches))


def classify_coverage(*, retrieved_count: int, referenced_gap_count: int, possible_gap_count: int, noise_count: int) -> str:
    if retrieved_count == 0:
        return "LOW_COVERAGE"
    if possible_gap_count == 0 and referenced_gap_count == 0 and noise_count <= retrieved_count:
        return "HIGH_COVERAGE"
    return "MODERATE_COVERAGE"


def _match_cited_lines(matches: list) -> tuple[CitedLine, ...]:
    lines: list[CitedLine] = []
    for match in matches:
        source = _relative_source_path(match.source_path)
        for snippet in match.snippets:
            item = CitedLine(source_path=source, text=snippet)
            if item not in lines:
                lines.append(item)
    return tuple(lines[:MAX_ITEMS_PER_SECTION])


def _contrarian_excerpt(brief: str) -> tuple[str, ...]:
    wanted_sections = {"# Strongest Contradictory Evidence", "# Alternative Explanations", "# What Would Falsify This?"}
    current = ""
    lines: list[str] = []
    for raw_line in brief.splitlines():
        line = raw_line.strip()
        if line.startswith("# "):
            current = line
            continue
        if current in wanted_sections and line.startswith("- "):
            if line not in lines:
                lines.append(line)
        if len(lines) >= MAX_ITEMS_PER_SECTION:
            break
    return tuple(lines)


def build_workflow_packet(journal_root: Path, query: str) -> WorkflowPacket:
    if query not in SUPPORTED_QUERIES:
        supported = ", ".join(SUPPORTED_QUERIES)
        raise ValueError(f"Unsupported query: {query}. Supported queries: {supported}")

    evidence_matches = search_by_query(journal_root, query)
    synthesis = synthesize_matches(evidence_matches)
    negative_matches = retrieve_negative_knowledge(journal_root, query)
    negative = group_negative_knowledge(negative_matches)
    coverage = analyze_coverage(journal_root, query)
    contrarian = build_contrarian_brief(journal_root, query)
    coverage_classification = classify_coverage(
        retrieved_count=len(coverage.retrieved_source_paths),
        referenced_gap_count=len(coverage.referenced_not_retrieved),
        possible_gap_count=len(coverage.possible_gaps),
        noise_count=len(coverage.noisy_sources),
    )
    referenced = tuple(
        f"`{ref.source_path}` referenced by `{ref.referenced_by}`" for ref in coverage.referenced_not_retrieved[:MAX_ITEMS_PER_SECTION]
    )
    source_paths = _dedupe_strings(
        _source_paths_from_matches(evidence_matches)
        + tuple(negative.source_paths)
        + tuple(coverage.source_paths)
    )
    return WorkflowPacket(
        query=query,
        coverage_classification=coverage_classification,
        retrieved_evidence=_limit_cited(_match_cited_lines(evidence_matches)),
        synthesized_support=_limit_cited(synthesis.direct_evidence + synthesis.referenced_follow_through),
        synthesized_contradictions=_limit_cited(synthesis.limiting_sources),
        negative_knowledge=_limit_cited(negative.summary + negative.failures + negative.cautions),
        contrarian_lines=_dedupe_strings(_contrarian_excerpt(contrarian)),
        coverage_gaps=coverage.possible_gaps,
        referenced_not_retrieved=referenced,
        open_questions=_limit_cited(synthesis.unresolved_gaps),
        source_paths=source_paths,
    )


def _render_cited(lines: list[str], items: tuple[CitedLine, ...], empty: str) -> None:
    if not items:
        lines.extend([empty, ""])
        return
    for item in items:
        lines.append(f"- `{item.source_path}`: {item.text}")
    lines.append("")


def _render_strings(lines: list[str], items: tuple[str, ...], empty: str) -> None:
    if not items:
        lines.extend([empty, ""])
        return
    for item in items:
        lines.append(f"- {item}")
    lines.append("")


def render_markdown(packet: WorkflowPacket) -> str:
    lines = [
        "# Atlas V1 Workflow Evidence Packet",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_WORKFLOW_PACKET",
        "",
        "This packet consolidates existing Atlas V1 read-only retrieval, synthesis, negative-knowledge, contrarian, and coverage-gap outputs for one query.",
        "",
        "It does not validate truth, infer readiness, recommend trades, allocate capital, mutate candidates, modify rules, or create journal objects.",
        "",
        "# Evidence Packet Summary",
        "",
        f"- Query: {packet.query}",
        f"- Coverage classification: {packet.coverage_classification}",
        f"- Retrieved evidence lines: {len(packet.retrieved_evidence)}",
        f"- Supporting synthesis lines: {len(packet.synthesized_support)}",
        f"- Contradictory synthesis lines: {len(packet.synthesized_contradictions)}",
        f"- Referenced-but-not-retrieved candidates shown: {len(packet.referenced_not_retrieved)}",
        "",
        "# Retrieved Evidence",
        "",
    ]
    _render_cited(lines, packet.retrieved_evidence, "None.")
    lines.extend(["# Synthesized Evidence", "", "Supporting evidence:", ""])
    _render_cited(lines, packet.synthesized_support, "None.")
    lines.extend(["Contradictions / limitations:", ""])
    _render_cited(lines, packet.synthesized_contradictions, "None.")
    lines.extend(["# Negative Knowledge", ""])
    _render_cited(lines, packet.negative_knowledge, "None.")
    lines.extend(["# Contrarian Review", ""])
    _render_strings(lines, packet.contrarian_lines, "None.")
    lines.extend(["# Coverage Gaps", ""])
    _render_strings(lines, packet.coverage_gaps, "None found by deterministic coverage rules.")
    lines.extend(["Referenced-but-not-retrieved coverage-gap candidates only. This packet does not claim they are relevant evidence.", ""])
    _render_strings(lines, packet.referenced_not_retrieved, "None found by deterministic reference scan.")
    lines.extend(["# Open Questions", ""])
    _render_cited(lines, packet.open_questions, "None.")
    lines.extend(["# Source Paths", ""])
    _render_strings(lines, tuple(f"`{source_path}`" for source_path in packet.source_paths), "None.")
    return "\n".join(lines)


def build_workflow_brief(journal_root: Path, query: str) -> str:
    return render_markdown(build_workflow_packet(journal_root, query))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only workflow evidence packet.")
    parser.add_argument(
        "--query",
        required=True,
        choices=SUPPORTED_QUERIES,
        help="Research topic query to run through the Atlas V1 workflow.",
    )
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    print(build_workflow_brief(Path(args.journal_root), args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
