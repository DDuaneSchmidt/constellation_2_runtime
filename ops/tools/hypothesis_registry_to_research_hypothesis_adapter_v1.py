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


def _status_from_legacy(value: str) -> str:
    mapped = {
        "proposed": "IDEA",
        "definition_ready": "UNDER_INVESTIGATION",
        "duplicate_checked": "UNDER_INVESTIGATION",
        "exploratory_testing": "BACKTESTING",
        "regime_testing": "REPLAY_REVIEW",
        "robustness_testing": "BACKTESTING",
        "friction_testing": "BACKTESTING",
        "out_of_sample_testing": "BACKTESTING",
        "failure_mode_review": "UNDER_INVESTIGATION",
        "edge_overlap_review": "UNDER_INVESTIGATION",
        "validated_candidate": "VALIDATED_RESEARCH",
        "promotion_review": "PROMOTION_CANDIDATE",
        "promoted": "APPROVED_FOR_LITE",
        "rejected": "REJECTED",
        "insufficient_data": "UNDER_INVESTIGATION",
        "regime_dependent": "UNDER_INVESTIGATION",
        "duplicate_of_existing_edge": "ARCHIVED",
        "retired": "ARCHIVED",
    }
    return mapped.get(str(value or "").strip().lower(), "IDEA")


def _confidence_from_legacy(row: dict[str, Any]) -> str:
    status = str(row.get("latest_result_status") or "").strip().lower()
    if status in {"validated_candidate", "promising"}:
        return "MEDIUM"
    if status in {"rejected", "insufficient_data", "weak"}:
        return "LOW"
    return "LOW"


def _legacy_to_research(row: dict[str, Any], *, created_at_utc: str) -> dict[str, Any]:
    return build_research_hypothesis_v1(
        hypothesis_id=str(row.get("hypothesis_id") or ""),
        created_at_utc=str(row.get("created_at") or created_at_utc),
        title=str(row.get("title") or ""),
        hypothesis_summary=str(row.get("behavioral_thesis") or ""),
        market_thesis=str(row.get("behavioral_thesis") or ""),
        edge_family=str(row.get("edge_family") or ""),
        behavioral_state=str(row.get("market_regime") or ""),
        expected_regime=str(row.get("market_regime") or ""),
        expected_direction="",
        expected_holding_period=str(row.get("time_horizon") or ""),
        instruments=row.get("instrument_universe") if isinstance(row.get("instrument_universe"), list) else [],
        rationale=str(row.get("trigger_conditions") or ""),
        expected_behavior=str(row.get("expected_outcome") or ""),
        failure_conditions=row.get("failure_modes") if isinstance(row.get("failure_modes"), list) else [],
        invalidation_conditions=row.get("failure_modes") if isinstance(row.get("failure_modes"), list) else [],
        related_sleeves=[],
        related_research_refs=row.get("related_hypotheses") if isinstance(row.get("related_hypotheses"), list) else [],
        confidence_level=_confidence_from_legacy(row),
        status=_status_from_legacy(str(row.get("lifecycle_state") or "")),
        source="RESEARCH_LAB",
        notes=f"Adapted one-way from legacy hypothesis_registry.v1. {row.get('notes') or ''}".strip(),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hypothesis_registry_to_research_hypothesis_adapter_v1")
    parser.add_argument("--registry_json", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--created_at_utc", default="")
    args = parser.parse_args(argv)

    registry = json.loads(Path(args.registry_json).expanduser().resolve().read_text(encoding="utf-8"))
    created_at = args.created_at_utc or _now()
    truth_root = Path(args.truth_root).expanduser().resolve()
    paths: list[str] = []
    for row in registry.get("hypotheses", []):
        if not isinstance(row, dict):
            continue
        payload = _legacy_to_research(row, created_at_utc=created_at)
        validate_research_lab_artifact_v1(payload)
        path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=args.day_utc, payload=payload)
        paths.append(str(path))
    print(
        json.dumps(
            {
                "adapted_count": len(paths),
                "research_hypothesis_paths": paths,
                "source_of_truth": "research_hypothesis.v1",
                "legacy_registry_authoritative": False,
                "runtime_mutation_allowed": False,
                "broker_submit_required": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
