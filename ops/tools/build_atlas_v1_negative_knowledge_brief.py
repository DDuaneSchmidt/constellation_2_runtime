from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        EvidenceMatch,
        _iter_files,
        _load_yaml,
        _matches_query,
        _relative_source_path,
        _snippets_for_query,
        _yaml_summary_lines,
    )
    from ops.tools.build_atlas_v1_research_synthesis_brief import CitedLine
except ModuleNotFoundError:
    from build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        EvidenceMatch,
        _iter_files,
        _load_yaml,
        _matches_query,
        _relative_source_path,
        _snippets_for_query,
        _yaml_summary_lines,
    )
    from build_atlas_v1_research_synthesis_brief import CitedLine


NEGATIVE_TERMS = (
    "blocked",
    "bottleneck",
    "caution",
    "contradict",
    "defect",
    "failed",
    "failure",
    "gap",
    "limitation",
    "limitations",
    "misdiagnosis",
    "misread",
    "missing",
    "not ",
    "not-ready",
    "rejected",
    "unsupported",
    "underpowered",
    "weakness",
)
RECHECK_TERMS = (
    "next",
    "recheck",
    "retry",
    "try",
    "unless",
    "until",
    "when",
)
MAX_ITEMS_PER_SECTION = 12


@dataclass(frozen=True)
class NegativeKnowledgeGroups:
    summary: tuple[CitedLine, ...]
    failures: tuple[CitedLine, ...]
    misdiagnoses: tuple[CitedLine, ...]
    cautions: tuple[CitedLine, ...]
    recheck_conditions: tuple[CitedLine, ...]
    source_paths: tuple[str, ...]


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    lowered = text.casefold()
    return any(term in lowered for term in terms)


def _dedupe_append(items: list[CitedLine], item: CitedLine) -> None:
    if item not in items:
        items.append(item)


def _negative_snippets(text: str) -> tuple[str, ...]:
    snippets: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _contains_any(stripped, NEGATIVE_TERMS):
            snippets.append(stripped)
        if len(snippets) >= 3:
            break
    return tuple(snippets)


def _failure_match(path: Path, query: str) -> EvidenceMatch | None:
    text = path.read_text(encoding="utf-8")
    matched, reason = _matches_query(text, query)
    if not matched:
        return None
    payload = _load_yaml(path)
    snippets = _yaml_summary_lines(payload)
    return EvidenceMatch(
        source_path=path,
        match_type="failure",
        identifier=str(payload.get("id") or ""),
        reason=reason,
        snippets=snippets,
    )


def _related_match(path: Path, query: str) -> EvidenceMatch | None:
    text = path.read_text(encoding="utf-8")
    matched, reason = _matches_query(text, query)
    if not matched or not _contains_any(text, NEGATIVE_TERMS):
        return None
    snippets = tuple(dict.fromkeys(_snippets_for_query(text, query) + _negative_snippets(text)))
    identifier = ""
    if path.suffix == ".yaml":
        identifier = str(_load_yaml(path).get("id") or "")
    return EvidenceMatch(
        source_path=path,
        match_type="related_negative_context",
        identifier=identifier,
        reason=reason + "; contains explicit negative-knowledge language",
        snippets=snippets,
    )


def retrieve_negative_knowledge(journal_root: Path, query: str) -> list[EvidenceMatch]:
    matches: list[EvidenceMatch] = []
    for path in _iter_files(journal_root, ("failures",)):
        match = _failure_match(path, query)
        if match is not None:
            matches.append(match)

    for path in _iter_files(journal_root, ("observations", "knowledge", "reports")):
        match = _related_match(path, query)
        if match is not None:
            matches.append(match)
    return matches


def _cited_lines(match: EvidenceMatch) -> tuple[CitedLine, ...]:
    source = _relative_source_path(match.source_path)
    return tuple(CitedLine(source_path=source, text=snippet) for snippet in match.snippets)


def group_negative_knowledge(matches: list[EvidenceMatch]) -> NegativeKnowledgeGroups:
    summary: list[CitedLine] = []
    failures: list[CitedLine] = []
    misdiagnoses: list[CitedLine] = []
    cautions: list[CitedLine] = []
    recheck_conditions: list[CitedLine] = []
    source_paths: list[str] = []

    for match in matches:
        source = _relative_source_path(match.source_path)
        if source not in source_paths:
            source_paths.append(source)

        for line in _cited_lines(match):
            if _contains_any(line.text, NEGATIVE_TERMS):
                _dedupe_append(summary, line)
                _dedupe_append(cautions, line)
            if match.match_type == "failure":
                _dedupe_append(failures, line)
                if line.text.startswith(("what_we_expected:", "what_failed:", "why_failed:")):
                    _dedupe_append(misdiagnoses, line)
                if line.text.startswith("what_to_try_next:") or _contains_any(line.text, RECHECK_TERMS):
                    _dedupe_append(recheck_conditions, line)
            elif _contains_any(line.text, ("misdiagnosis", "misread", "expected", "failed")):
                _dedupe_append(misdiagnoses, line)
            elif _contains_any(line.text, RECHECK_TERMS):
                _dedupe_append(recheck_conditions, line)

    return NegativeKnowledgeGroups(
        summary=tuple(summary[:MAX_ITEMS_PER_SECTION]),
        failures=tuple(failures[:MAX_ITEMS_PER_SECTION]),
        misdiagnoses=tuple(misdiagnoses[:MAX_ITEMS_PER_SECTION]),
        cautions=tuple(cautions[:MAX_ITEMS_PER_SECTION]),
        recheck_conditions=tuple(recheck_conditions[:MAX_ITEMS_PER_SECTION]),
        source_paths=tuple(source_paths),
    )


def _render_section(lines: list[str], title: str, items: tuple[CitedLine, ...], empty_text: str) -> None:
    lines.extend([f"## {title}", ""])
    if not items:
        lines.extend([empty_text, ""])
        return
    for item in items:
        lines.append(f"- `{item.source_path}`: {item.text}")
    lines.append("")


def render_markdown(query: str, matches: list[EvidenceMatch], groups: NegativeKnowledgeGroups) -> str:
    lines = [
        "# Atlas V1 Negative Knowledge Brief",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_NEGATIVE_KNOWLEDGE_BRIEF",
        "",
        "This brief is advisory and source-bound. It retrieves prior failures, limitations, and cautions from existing Research Journal files only. It does not infer permanent rejection unless a cited source says so, and it must not suppress exploration.",
        "",
        "It does not validate truth, infer readiness, recommend trades, allocate capital, mutate candidates, modify sleeves, modify rules, create journal objects, or create reports.",
        "",
        f"Query: {query}",
        "Ordering: matching failure entries first, then related negative-context observations, knowledge, and reports in deterministic source-path order. No relevance ranking is applied.",
        "",
    ]

    if not matches:
        lines.extend(
            [
                "## Negative Knowledge Summary",
                "",
                "No matches found. No negative-knowledge reuse brief is produced beyond this deterministic no-match result.",
                "",
                "## Relevant Failure Entries",
                "",
                "None.",
                "",
                "## Corrected Misdiagnoses",
                "",
                "None.",
                "",
                "## Do-Not-Repeat Cautions",
                "",
                "None.",
                "",
                "## Recheck Conditions",
                "",
                "None.",
                "",
                "## Source File Paths",
                "",
                "None.",
                "",
            ]
        )
        return "\n".join(lines)

    _render_section(lines, "Negative Knowledge Summary", groups.summary, "No source-bound negative-knowledge summary lines retrieved.")
    _render_section(lines, "Relevant Failure Entries", groups.failures, "No matching failure entries retrieved.")
    _render_section(
        lines,
        "Corrected Misdiagnoses",
        groups.misdiagnoses,
        "No source-bound corrected-misdiagnosis lines retrieved.",
    )
    _render_section(
        lines,
        "Do-Not-Repeat Cautions",
        groups.cautions,
        "No source-bound do-not-repeat caution lines retrieved.",
    )
    _render_section(
        lines,
        "Recheck Conditions",
        groups.recheck_conditions,
        "No source-bound recheck conditions retrieved.",
    )
    lines.extend(["## Source File Paths", ""])
    for source_path in groups.source_paths:
        lines.append(f"- `{source_path}`")
    lines.append("")
    return "\n".join(lines)


def build_negative_knowledge_brief(journal_root: Path, query: str) -> str:
    matches = retrieve_negative_knowledge(journal_root, query)
    groups = group_negative_knowledge(matches)
    return render_markdown(query=query, matches=matches, groups=groups)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Negative Knowledge brief.")
    parser.add_argument("--query", required=True, help="Research topic query to check against prior failures and cautions.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    print(build_negative_knowledge_brief(Path(args.journal_root), args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
