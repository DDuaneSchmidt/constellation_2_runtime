from __future__ import annotations

import argparse
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_librarian_brief import DEFAULT_JOURNAL_ROOT, search_by_query
    from ops.tools.build_atlas_v1_negative_knowledge_brief import (
        group_negative_knowledge,
        retrieve_negative_knowledge,
    )
    from ops.tools.build_atlas_v1_research_synthesis_brief import (
        SUPPORTED_QUERIES,
        CitedLine,
        synthesize_matches,
    )
except ModuleNotFoundError:
    from build_atlas_v1_evidence_librarian_brief import DEFAULT_JOURNAL_ROOT, search_by_query
    from build_atlas_v1_negative_knowledge_brief import group_negative_knowledge, retrieve_negative_knowledge
    from build_atlas_v1_research_synthesis_brief import SUPPORTED_QUERIES, CitedLine, synthesize_matches


MAX_ITEMS_PER_SECTION = 8


def _dedupe(items: tuple[CitedLine, ...]) -> tuple[CitedLine, ...]:
    seen: set[CitedLine] = set()
    deduped: list[CitedLine] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return tuple(deduped)


def _limit(items: tuple[CitedLine, ...]) -> tuple[CitedLine, ...]:
    return _dedupe(items)[:MAX_ITEMS_PER_SECTION]


def _render_cited_section(lines: list[str], items: tuple[CitedLine, ...], empty_text: str) -> None:
    if not items:
        lines.append(empty_text)
        lines.append("")
        return
    for item in items:
        lines.append(f"- `{item.source_path}`: {item.text}")
    lines.append("")


def _source_paths(*groups: tuple[str, ...]) -> tuple[str, ...]:
    paths: list[str] = []
    for group in groups:
        for source_path in group:
            if source_path not in paths:
                paths.append(source_path)
    return tuple(paths)


def render_markdown(
    *,
    query: str,
    current_conclusion: tuple[CitedLine, ...],
    supporting_evidence: tuple[CitedLine, ...],
    contradictory_evidence: tuple[CitedLine, ...],
    alternative_explanations: tuple[CitedLine, ...],
    falsifiers: tuple[CitedLine, ...],
    source_paths: tuple[str, ...],
) -> str:
    lines = [
        "# Atlas V1 Research Contrarian Brief",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_CONTRARIAN_BRIEF",
        "",
        "This brief stress-tests retrieved Research Journal conclusions using existing synthesis and negative-knowledge outputs. It challenges conclusions; it does not replace them.",
        "",
        "It does not validate truth, infer readiness, recommend trades, allocate capital, mutate candidates, modify sleeves, modify rules, or create journal objects.",
        "",
        f"Query: {query}",
        "",
        "# Current Conclusion",
        "",
    ]

    _render_cited_section(
        lines,
        current_conclusion,
        "No matching conclusion was retrieved. No contrarian conclusion is produced beyond this deterministic no-match result.",
    )
    lines.extend(["# Supporting Evidence", ""])
    _render_cited_section(lines, supporting_evidence, "None retrieved by the deterministic grouping rule.")
    lines.extend(["# Strongest Contradictory Evidence", ""])
    _render_cited_section(
        lines,
        contradictory_evidence,
        "None retrieved by the deterministic grouping rule. This is not evidence that contradictions do not exist.",
    )
    lines.extend(["# Alternative Explanations", ""])
    _render_cited_section(
        lines,
        alternative_explanations,
        "None retrieved by the deterministic grouping rule. Reviewers should not infer that alternatives are exhausted.",
    )
    lines.extend(["# What Would Falsify This?", ""])
    _render_cited_section(
        lines,
        falsifiers,
        "No source-bound recheck or falsification lines were retrieved. Re-review should still use existing evidence standards.",
    )
    lines.extend(["# Source Paths", ""])
    if not source_paths:
        lines.append("None.")
    else:
        for source_path in source_paths:
            lines.append(f"- `{source_path}`")
    lines.append("")
    return "\n".join(lines)


def build_contrarian_brief(journal_root: Path, query: str) -> str:
    if query not in SUPPORTED_QUERIES:
        supported = ", ".join(SUPPORTED_QUERIES)
        raise ValueError(f"Unsupported query: {query}. Supported queries: {supported}")

    evidence_matches = search_by_query(journal_root, query)
    synthesis = synthesize_matches(evidence_matches)
    negative_matches = retrieve_negative_knowledge(journal_root, query)
    negative = group_negative_knowledge(negative_matches)

    current_conclusion = _limit(synthesis.direct_evidence[:3])
    supporting_evidence = _limit(synthesis.direct_evidence + synthesis.referenced_follow_through)
    contradictory_evidence = _limit(synthesis.limiting_sources + negative.failures + negative.cautions)
    alternative_explanations = _limit(negative.misdiagnoses + negative.summary + synthesis.unresolved_gaps)
    falsifiers = _limit(negative.recheck_conditions + synthesis.unresolved_gaps)
    paths = _source_paths(synthesis.source_paths, negative.source_paths)

    return render_markdown(
        query=query,
        current_conclusion=current_conclusion,
        supporting_evidence=supporting_evidence,
        contradictory_evidence=contradictory_evidence,
        alternative_explanations=alternative_explanations,
        falsifiers=falsifiers,
        source_paths=paths,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Research Contrarian brief.")
    parser.add_argument(
        "--query",
        required=True,
        choices=SUPPORTED_QUERIES,
        help="Research topic query to stress-test from existing Research Journal content.",
    )
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    print(build_contrarian_brief(Path(args.journal_root), args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
