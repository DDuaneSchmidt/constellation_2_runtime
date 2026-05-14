#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_hypothesis_registry_v1,
    register_hypothesis_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _registry_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "hypothesis_registry_v1" / day_utc / "index" / "hypothesis_registry.v1.json"


def _queue_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "research_task_queue_v1" / day_utc / "index" / "research_task_queue.v1.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="research_lab_register_hypothesis_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--created_at_utc", default="")
    parser.add_argument("--title", required=True)
    parser.add_argument("--edge_family", required=True)
    parser.add_argument("--behavioral_thesis", required=True)
    parser.add_argument("--market_regime", required=True)
    parser.add_argument("--trigger_conditions", required=True)
    parser.add_argument("--expected_outcome", required=True)
    parser.add_argument("--failure_modes", required=True)
    parser.add_argument("--instrument_universe", default="")
    parser.add_argument("--time_horizon", default="")
    parser.add_argument("--overlap_tags", default="")
    parser.add_argument("--source_type", default="manual")
    parser.add_argument("--source_reference", default="")
    parser.add_argument("--priority", default="normal")
    parser.add_argument("--notes", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day_utc or _day()
    created_at = args.created_at_utc or _now()
    registry = _load_json(_registry_path(truth_root, day_utc)) or build_hypothesis_registry_v1(
        generated_at_utc=created_at,
        hypotheses=[],
    )
    task_queue = _load_json(_queue_path(truth_root, day_utc))
    result = register_hypothesis_v1(
        registry=registry,
        task_queue=task_queue,
        title=args.title,
        edge_family=args.edge_family,
        behavioral_thesis=args.behavioral_thesis,
        market_regime=args.market_regime,
        trigger_conditions=args.trigger_conditions,
        expected_outcome=args.expected_outcome,
        failure_modes=args.failure_modes,
        instrument_universe=[item.strip() for item in args.instrument_universe.split(",") if item.strip()],
        time_horizon=args.time_horizon,
        created_at_utc=created_at,
        overlap_tags=[item.strip() for item in args.overlap_tags.split(",") if item.strip()],
        source_type=args.source_type,
        source_reference=args.source_reference,
        priority=args.priority,
        notes=args.notes,
    )
    for payload in (result["registry"], result["task_queue"], result["test_plan"]):
        validate_research_lab_artifact_v1(payload)
        write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    print(
        json.dumps(
            {
                "hypothesis_id": result["hypothesis"]["hypothesis_id"],
                "duplicate_warning": result["duplicate_warning"],
                "related_hypothesis_ids": result["related_hypothesis_ids"],
                "overlap_reason_codes": result["overlap_reason_codes"],
                "research_lab_only": True,
                "executable_trade_created": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
