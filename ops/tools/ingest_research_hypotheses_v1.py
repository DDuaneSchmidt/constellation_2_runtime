#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_research_hypothesis_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%d")


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned.strip("_-.").lower() or "research_hypothesis"


def _load_seed_rows(path: Path | None, seed_text: str, title: str) -> list[dict[str, Any]]:
    if path:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [dict(row) for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("hypotheses"), list):
            return [dict(row) for row in payload["hypotheses"] if isinstance(row, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("seeds"), list):
            return [dict(row) for row in payload["seeds"] if isinstance(row, dict)]
        if isinstance(payload, dict):
            return [dict(payload)]
        raise ValueError("SEED_JSON_MUST_BE_OBJECT_OR_ARRAY")
    if seed_text:
        if not title:
            raise ValueError("SEED_TEXT_REQUIRES_TITLE")
        return [{"title": title, "hypothesis_summary": seed_text}]
    raise ValueError("NO_EXPLICIT_SEED_ITEMS_PROVIDED")


def _hypothesis_from_row(row: dict[str, Any], *, created_at_utc: str, ordinal: int, default_source: str) -> dict[str, Any]:
    title = str(row.get("title") or "").strip()
    summary = str(row.get("hypothesis_summary") or row.get("summary") or "").strip()
    if not title or not summary:
        raise ValueError("EACH_SEED_REQUIRES_TITLE_AND_HYPOTHESIS_SUMMARY")
    hypothesis_id = str(row.get("hypothesis_id") or f"rh-{created_at_utc[:10].replace('-', '')}-{ordinal:04d}-{_safe_id(title)[:40]}")
    return build_research_hypothesis_v1(
        hypothesis_id=hypothesis_id,
        created_at_utc=str(row.get("created_at_utc") or created_at_utc),
        title=title,
        hypothesis_summary=summary,
        market_thesis=str(row.get("market_thesis") or ""),
        edge_family=str(row.get("edge_family") or ""),
        behavioral_state=str(row.get("behavioral_state") or ""),
        expected_regime=str(row.get("expected_regime") or ""),
        expected_direction=str(row.get("expected_direction") or ""),
        expected_holding_period=str(row.get("expected_holding_period") or ""),
        instruments=_strings(row.get("instruments")),
        rationale=str(row.get("rationale") or ""),
        expected_behavior=str(row.get("expected_behavior") or ""),
        failure_conditions=_strings(row.get("failure_conditions")),
        invalidation_conditions=_strings(row.get("invalidation_conditions")),
        related_sleeves=_strings(row.get("related_sleeves")),
        related_research_refs=_strings(row.get("related_research_refs")),
        confidence_level=str(row.get("confidence_level") or "LOW"),
        status=str(row.get("status") or "IDEA"),
        source=str(row.get("source") or default_source),
        notes=str(row.get("notes") or ""),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingest_research_hypotheses_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--created_at_utc", default="")
    parser.add_argument("--seed_json", default="")
    parser.add_argument("--seed_text", default="")
    parser.add_argument("--title", default="")
    parser.add_argument("--source", default="MANUAL", choices=["CHATGPT_SEED", "MANUAL", "RESEARCH_LAB"])
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = _day(args.day_utc)
    created_at = args.created_at_utc or _now()
    seed_path = Path(args.seed_json).expanduser().resolve() if args.seed_json else None
    rows = _load_seed_rows(seed_path, args.seed_text, args.title)
    written: list[str] = []
    for index, row in enumerate(rows, start=1):
        hypothesis = _hypothesis_from_row(row, created_at_utc=created_at, ordinal=index, default_source=args.source)
        validate_research_lab_artifact_v1(hypothesis)
        path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=hypothesis)
        written.append(str(path))
    print(
        json.dumps(
            {
                "ingested_count": len(written),
                "hypothesis_paths": written,
                "research_lab_only": True,
                "runtime_mutation_allowed": False,
                "broker_submit_required": False,
                "trade_authorization_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
