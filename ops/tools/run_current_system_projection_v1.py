#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.current_system_projection_v1 import (
    build_current_system_projection_v1,
    write_current_system_projection_v1,
)
from constellation_2.common.execution_journal_v1 import read_execution_journal_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    now_utc_iso_v1,
    read_json_object_v1,
    read_paper_session_ledger_ref_v1,
    read_startup_materialization_ref_v1,
    read_startup_proof_validation_ref_v1,
    read_trading_day_state_machine_ref_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_current_system_projection_path,
    resolve_deployment_state_machine_path,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DEPLOYMENT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"


def _source_artifact_row(*, logical_name: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "logical_name": logical_name,
        "path": str(path),
        "generated_at_utc": str(
            payload.get("evaluated_at_utc") or payload.get("produced_at_utc") or payload.get("generated_at_utc") or ""
        ).strip(),
        "sha256": sha256_file_v1(path),
        "status": "PRESENT",
        "producer_git_sha": str(dict(payload.get("producer") or {}).get("git_sha") or "").strip(),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_current_system_projection_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default=str((REPO_ROOT / "constellation_2/runtime/truth").resolve()),
    )
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()

    journal_ref = read_execution_journal_v1(truth_root=truth_root, day_utc=day_utc)

    deployment_path = resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    deployment_payload = read_json_object_v1(deployment_path)
    validate_against_repo_schema_v1(deployment_payload, REPO_ROOT, DEPLOYMENT_SCHEMA_RELPATH)

    startup_ref = read_startup_materialization_ref_v1(truth_root=truth_root, day_utc=day_utc)
    startup_proof_ref = read_startup_proof_validation_ref_v1(truth_root=truth_root, day_utc=day_utc)
    ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day_utc)
    trading_day_ref = read_trading_day_state_machine_ref_v1(truth_root=truth_root, day_utc=day_utc)

    source_artifacts = [
        _source_artifact_row(logical_name="execution_journal_v1", path=journal_ref.path, payload=dict(journal_ref.payload)),
        _source_artifact_row(logical_name="deployment_state_machine_v1", path=deployment_path, payload=deployment_payload),
        _source_artifact_row(logical_name="startup_materialization_v1", path=startup_ref.path, payload=dict(startup_ref.payload)),
        _source_artifact_row(logical_name="startup_proof_validation_v1", path=startup_proof_ref.path, payload=dict(startup_proof_ref.payload)),
        _source_artifact_row(logical_name="paper_session_ledger_v1", path=ledger_ref.path, payload=dict(ledger_ref.payload)),
        _source_artifact_row(logical_name="trading_day_state_machine_v1", path=trading_day_ref.path, payload=dict(trading_day_ref.payload)),
    ]

    payload = build_current_system_projection_v1(
        day_utc=day_utc,
        journal_payload=dict(journal_ref.payload),
        source_artifacts=source_artifacts,
        deployment_payload=deployment_payload,
        startup_materialization_payload=dict(startup_ref.payload),
        startup_proof_payload=dict(startup_proof_ref.payload),
        ledger_payload=dict(ledger_ref.payload),
        trading_day_payload=dict(trading_day_ref.payload),
        generated_at_utc=now_utc_iso_v1(),
        producer_module="ops/tools/run_current_system_projection_v1.py",
    )
    ref = write_current_system_projection_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "report_path": str(ref.path),
                "projection_id": str(payload.get("projection_id") or ""),
                "first_true_blocker_code": str(payload.get("first_true_blocker_code") or ""),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
