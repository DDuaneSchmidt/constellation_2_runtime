from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

try:
    from ops.tools.build_atlas_v1_evidence_librarian_brief import (
        DEFAULT_JOURNAL_ROOT,
        STOPWORDS,
        _relative_source_path,
        search_by_query,
    )
except ModuleNotFoundError:
    from build_atlas_v1_evidence_librarian_brief import DEFAULT_JOURNAL_ROOT, STOPWORDS, _relative_source_path, search_by_query


MAX_ADJACENT_PHRASES = 24
PHRASE_LENGTHS = (2, 3)
PHRASE_STOPWORDS = STOPWORDS | {"be", "been", "being", "do", "does", "did", "would", "could", "should"}


@dataclass(frozen=True)
class AdjacentPhrase:
    phrase: str
    source_path: str
    source_excerpt: str


@dataclass(frozen=True)
class QueryAdjacency:
    query: str
    retrieved_source_paths: tuple[str, ...]
    adjacent_phrases: tuple[AdjacentPhrase, ...]


def _tokens(text: str) -> tuple[str, ...]:
    return tuple(re.findall(r"[A-Za-z0-9_]+", text.casefold()))


def _query_terms(query: str) -> set[str]:
    return {token for token in _tokens(query) if len(token) > 2 and token not in STOPWORDS}


def _normalized_phrase(text: str) -> str:
    return " ".join(_tokens(text))


def _source_observed_phrases(text: str, query: str) -> tuple[str, ...]:
    if ":" in text:
        text = text.split(":", 1)[1]
    tokens = _tokens(text)
    terms = _query_terms(query)
    original_query = _normalized_phrase(query)
    phrases: list[str] = []

    if not terms:
        return tuple()

    for length in PHRASE_LENGTHS:
        if len(tokens) < length:
            continue
        for index in range(0, len(tokens) - length + 1):
            phrase_tokens = tokens[index : index + length]
            if phrase_tokens[0] in PHRASE_STOPWORDS or phrase_tokens[-1] in PHRASE_STOPWORDS:
                continue
            if phrase_tokens[0].isdigit() or phrase_tokens[-1].isdigit():
                continue
            if not terms.intersection(phrase_tokens):
                continue
            if all(token in PHRASE_STOPWORDS for token in phrase_tokens):
                continue
            phrase = " ".join(phrase_tokens)
            if phrase == original_query:
                continue
            if phrase not in phrases:
                phrases.append(phrase)

    return tuple(phrases)


def build_query_adjacency(journal_root: Path, query: str) -> QueryAdjacency:
    matches = search_by_query(journal_root, query)
    retrieved_paths = tuple(dict.fromkeys(_relative_source_path(match.source_path) for match in matches))
    phrase_by_text: dict[str, AdjacentPhrase] = {}

    for match in matches:
        source_path = _relative_source_path(match.source_path)
        for snippet in match.snippets:
            for phrase in _source_observed_phrases(snippet, query):
                if phrase in phrase_by_text:
                    continue
                phrase_by_text[phrase] = AdjacentPhrase(
                    phrase=phrase,
                    source_path=source_path,
                    source_excerpt=snippet,
                )

    adjacent = tuple(phrase_by_text.values())[:MAX_ADJACENT_PHRASES]
    return QueryAdjacency(query=query, retrieved_source_paths=retrieved_paths, adjacent_phrases=adjacent)


def _render_adjacent_phrases(lines: list[str], phrases: tuple[AdjacentPhrase, ...]) -> None:
    if not phrases:
        lines.extend(["None.", ""])
        return
    for item in phrases:
        lines.append(f"- `{item.phrase}`")
        lines.append(f"  - Source path: `{item.source_path}`")
    lines.append("")


def _render_source_evidence(lines: list[str], phrases: tuple[AdjacentPhrase, ...]) -> None:
    if not phrases:
        lines.extend(["None.", ""])
        return
    for item in phrases:
        lines.append(f"- `{item.phrase}` observed in `{item.source_path}`: {item.source_excerpt}")
    lines.append("")


def _render_source_paths(lines: list[str], paths: tuple[str, ...]) -> None:
    if not paths:
        lines.extend(["None.", ""])
        return
    for source_path in paths:
        lines.append(f"- `{source_path}`")
    lines.append("")


def render_markdown(adjacency: QueryAdjacency) -> str:
    lines = [
        "# Atlas V1 Query Adjacency Brief",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_QUERY_ADJACENCY_BRIEF",
        "",
        "This brief reports source-observed adjacent phrases from retrieved Research Journal content only. It does not infer semantic fit, validate truth, rank evidence, infer readiness, recommend trades, allocate capital, mutate candidates, or create journal objects.",
        "",
        "# Query Adjacency Summary",
        "",
        f"- Retrieved source count: {len(adjacency.retrieved_source_paths)}",
        f"- Source-observed adjacent phrase count: {len(adjacency.adjacent_phrases)}",
        "- Ordering: deterministic retrieval and source-occurrence order only.",
        "- Boundary: suggestions are source-observed adjacent phrases, not fit claims.",
        "",
        "# Original Query",
        "",
        adjacency.query,
        "",
        "# Adjacent Phrases Found",
        "",
    ]
    _render_adjacent_phrases(lines, adjacency.adjacent_phrases)
    lines.extend(["# Source Evidence", ""])
    _render_source_evidence(lines, adjacency.adjacent_phrases)
    lines.extend(["# Suggested Follow-Up Queries", ""])
    if adjacency.adjacent_phrases:
        for item in adjacency.adjacent_phrases:
            lines.append(f"- `{item.phrase}` (source-observed adjacent phrase; cited above)")
        lines.append("")
    else:
        lines.extend(["None.", ""])
    lines.extend(["# Source Paths", ""])
    _render_source_paths(lines, adjacency.retrieved_source_paths)
    return "\n".join(lines)


def build_query_adjacency_brief(journal_root: Path, query: str) -> str:
    return render_markdown(build_query_adjacency(journal_root, query))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Query Adjacency brief.")
    parser.add_argument("--query", required=True, help="Keyword query to search across Research Journal objects and reports.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    print(build_query_adjacency_brief(Path(args.journal_root), args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
