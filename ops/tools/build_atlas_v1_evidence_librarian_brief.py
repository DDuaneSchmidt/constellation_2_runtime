from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_JOURNAL_ROOT = REPO_ROOT / "research_journal"
OBJECT_DIRECTORIES = ("observations", "knowledge", "failures")
SEARCH_DIRECTORIES = ("observations", "knowledge", "failures", "reports")
SEARCH_SUFFIXES = (".yaml", ".md")
MAX_SNIPPETS_PER_RESULT = 3
MIN_RELATED_REPORT_TERM_MATCHES = 2
OBJECT_ID_RE = re.compile(r"\b(?:OBS|KNW|FAIL)_\d{4}\b")

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "in",
    "is",
    "it",
    "not",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "were",
    "with",
}


@dataclass(frozen=True)
class EvidenceMatch:
    source_path: Path
    match_type: str
    identifier: str
    reason: str
    snippets: tuple[str, ...]


def _relative_source_path(path: Path, repo_root: Path = REPO_ROOT) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(_read_text(path))
    if not isinstance(payload, dict):
        return {}
    return payload


def _iter_files(journal_root: Path, directories: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for directory_name in directories:
        directory = journal_root / directory_name
        if not directory.exists():
            continue
        for path in sorted(directory.iterdir()):
            if path.is_file() and path.suffix in SEARCH_SUFFIXES:
                files.append(path)
    return files


def _yaml_summary_lines(payload: dict[str, Any]) -> tuple[str, ...]:
    lines: list[str] = []
    for key in sorted(payload):
        value = payload[key]
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"- {item}")
        else:
            lines.append(f"{key}: {value}")
    return tuple(lines)


def _query_terms(query: str) -> tuple[str, ...]:
    return tuple(term for term in query.casefold().split() if term)


def _tokenize(text: str) -> tuple[str, ...]:
    token = ""
    tokens: list[str] = []
    for char in text.casefold():
        if char.isalnum() or char == "_":
            token += char
            continue
        if token:
            tokens.append(token)
            token = ""
    if token:
        tokens.append(token)
    return tuple(token for token in tokens if len(token) > 2 and token not in STOPWORDS)


def _payload_search_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for value in payload.values():
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        else:
            parts.append(str(value))
    return " ".join(parts)


def _matches_query(text: str, query: str) -> tuple[bool, str]:
    normalized_text = text.casefold()
    normalized_query = query.casefold().strip()
    if not normalized_query:
        return False, "empty query"
    if normalized_query in normalized_text:
        return True, "contains query phrase"
    terms = _query_terms(query)
    if terms and all(term in normalized_text for term in terms):
        return True, "contains query terms: " + ", ".join(terms)
    return False, "query not found"


def _snippets_for_query(text: str, query: str) -> tuple[str, ...]:
    normalized_query = query.casefold().strip()
    terms = _query_terms(query)
    snippets: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.casefold()
        if normalized_query in lowered or any(term in lowered for term in terms):
            snippets.append(stripped)
        if len(snippets) >= MAX_SNIPPETS_PER_RESULT:
            break
    return tuple(snippets)


def _snippets_for_terms(text: str, terms: tuple[str, ...]) -> tuple[str, ...]:
    snippets: list[str] = []
    term_set = set(terms)
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        line_terms = set(_tokenize(stripped))
        if stripped.casefold() in term_set or line_terms & term_set:
            snippets.append(stripped)
        if len(snippets) >= MAX_SNIPPETS_PER_RESULT:
            break
    return tuple(snippets)


def find_by_id(journal_root: Path, object_id: str) -> list[EvidenceMatch]:
    matches: list[EvidenceMatch] = []
    exact_payloads: list[dict[str, Any]] = []
    for path in _iter_files(journal_root, OBJECT_DIRECTORIES):
        if path.suffix != ".yaml":
            continue
        payload = _load_yaml(path)
        if str(payload.get("id") or "") != object_id:
            continue
        exact_payloads.append(payload)
        matches.append(
            EvidenceMatch(
                source_path=path,
                match_type="exact_id",
                identifier=object_id,
                reason=f"YAML id exactly equals {object_id}",
                snippets=_yaml_summary_lines(payload),
            )
        )
    if exact_payloads:
        matches.extend(_related_report_matches(journal_root, object_id, exact_payloads))
    return matches


def _related_report_matches(journal_root: Path, object_id: str, payloads: list[dict[str, Any]]) -> list[EvidenceMatch]:
    terms = tuple(dict.fromkeys(_tokenize(" ".join(_payload_search_text(payload) for payload in payloads))))
    related: list[EvidenceMatch] = []
    for path in _iter_files(journal_root, ("reports",)):
        text = _read_text(path)
        text_terms = set(_tokenize(text))
        matched_terms = tuple(term for term in terms if term in text_terms)
        object_id_in_report = object_id.casefold() in text.casefold()
        if not object_id_in_report and len(matched_terms) < MIN_RELATED_REPORT_TERM_MATCHES:
            continue
        reason = f"report references {object_id}" if object_id_in_report else (
            "report shares retrieved object terms: " + ", ".join(matched_terms[:5])
        )
        related.append(
            EvidenceMatch(
                source_path=path,
                match_type="related_report",
                identifier=path.stem,
                reason=reason,
                snippets=_snippets_for_terms(text, (object_id.casefold(),) + matched_terms),
            )
        )
    return related


def search_by_query(journal_root: Path, query: str) -> list[EvidenceMatch]:
    matches: list[EvidenceMatch] = []
    for path in _iter_files(journal_root, SEARCH_DIRECTORIES):
        text = _read_text(path)
        matched, reason = _matches_query(text, query)
        if not matched:
            continue
        identifier = ""
        if path.suffix == ".yaml":
            payload = _load_yaml(path)
            identifier = str(payload.get("id") or "")
        matches.append(
            EvidenceMatch(
                source_path=path,
                match_type="keyword",
                identifier=identifier,
                reason=reason,
                snippets=_snippets_for_query(text, query),
            )
        )
    return matches


def _object_path_by_id(journal_root: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for path in _iter_files(journal_root, OBJECT_DIRECTORIES):
        if path.suffix != ".yaml":
            continue
        object_id = path.stem
        if OBJECT_ID_RE.fullmatch(object_id):
            paths[object_id] = path
    return paths


def _referenced_object_ids_from_reports(matches: list[EvidenceMatch]) -> dict[str, list[str]]:
    referrers_by_id: dict[str, list[str]] = {}
    for match in matches:
        if match.source_path.suffix != ".md" or match.source_path.parent.name != "reports":
            continue
        text = _read_text(match.source_path)
        for found in OBJECT_ID_RE.findall(text):
            referrers_by_id.setdefault(found, [])
            source = _relative_source_path(match.source_path)
            if source not in referrers_by_id[found]:
                referrers_by_id[found].append(source)
    return referrers_by_id


def add_referenced_object_follow_through(journal_root: Path, matches: list[EvidenceMatch]) -> list[EvidenceMatch]:
    if not matches:
        return matches

    existing_sources = {_relative_source_path(match.source_path) for match in matches}
    object_paths = _object_path_by_id(journal_root)
    referrers_by_id = _referenced_object_ids_from_reports(matches)
    follow_through: list[EvidenceMatch] = []

    for object_id in sorted(referrers_by_id):
        path = object_paths.get(object_id)
        if path is None:
            continue
        source = _relative_source_path(path)
        if source in existing_sources:
            continue
        payload = _load_yaml(path)
        referrers = tuple(sorted(referrers_by_id[object_id]))
        follow_through.append(
            EvidenceMatch(
                source_path=path,
                match_type="referenced_follow_through",
                identifier=object_id,
                reason="referenced by retrieved source: " + ", ".join(referrers),
                snippets=_yaml_summary_lines(payload),
            )
        )

    return matches + sorted(follow_through, key=lambda match: _relative_source_path(match.source_path))


def search_with_referenced_follow_through(journal_root: Path, query: str) -> list[EvidenceMatch]:
    return add_referenced_object_follow_through(journal_root, search_by_query(journal_root, query))


def render_markdown(*, mode: str, value: str, matches: list[EvidenceMatch]) -> str:
    lines = [
        "# Atlas V1 Research Evidence Librarian Brief",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_BRIEF",
        "",
        "This brief only reports retrieved Research Journal content. It does not validate truth, infer readiness, rank evidence, recommend trades, allocate capital, mutate candidates, or create journal objects.",
        "",
        f"Lookup mode: {mode}",
        f"Lookup value: {value}",
        "Ordering: deterministic source-path order only. No relevance ranking is applied.",
        "",
        "## Results",
        "",
    ]

    if not matches:
        lines.extend(
            [
                "No matching evidence found.",
                "",
                "## Source Paths",
                "",
                "None.",
                "",
            ]
        )
        return "\n".join(lines)

    for match in matches:
        source = _relative_source_path(match.source_path)
        heading = match.identifier or source
        lines.extend(
            [
                f"### Match: {heading}",
                "",
                f"- Source path: `{source}`",
                f"- Match type: `{match.match_type}`",
                f"- Match reason: {match.reason}",
                "- Retrieved content:",
            ]
        )
        for snippet in match.snippets:
            lines.append(f"  - {snippet}")
        if not match.snippets:
            lines.append("  - No non-empty snippet available.")
        lines.append("")

    lines.extend(["## Source Paths", ""])
    for match in matches:
        lines.append(f"- `{_relative_source_path(match.source_path)}`")
    lines.append("")
    return "\n".join(lines)


def build_brief(journal_root: Path, *, object_id: str | None = None, query: str | None = None) -> str:
    if object_id:
        matches = find_by_id(journal_root, object_id)
        return render_markdown(mode="exact_id", value=object_id, matches=matches)
    if query:
        matches = search_with_referenced_follow_through(journal_root, query)
        return render_markdown(mode="keyword", value=query, matches=matches)
    raise ValueError("Either object_id or query is required.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Research Evidence Librarian brief.")
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--id", dest="object_id", help="Exact journal object ID, such as OBS_0001, KNW_0017, or FAIL_0016.")
    selector.add_argument("--query", help="Keyword query to search across Research Journal objects and reports.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    args = parser.parse_args()

    brief = build_brief(Path(args.journal_root), object_id=args.object_id, query=args.query)
    print(brief)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
