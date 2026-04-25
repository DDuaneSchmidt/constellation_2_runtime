#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_intents_day_completeness_path
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


OUTPUT_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/intents_day_completeness.v1.schema.json"
NO_INTENTS_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"
EXPOSURE_INTENT_V1_SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v1.schema.json"
EXPOSURE_INTENT_V2_SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v2.schema.json"


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _day_intents_dir(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / parse_day_utc_v1(day_utc)).resolve()


def _validate_intent_file(path: Path) -> None:
    payload = read_json_object_v1(path)
    schema_id = str(payload.get("schema_id") or "").strip()
    schema_version = str(payload.get("schema_version") or "").strip()
    if schema_id != "exposure_intent":
        raise ValueError(f"INTENTS_DAY_COMPLETENESS_UNSUPPORTED_SCHEMA_ID:path={path}")
    if schema_version == "v1":
        schema_relpath = EXPOSURE_INTENT_V1_SCHEMA_RELPATH
    elif schema_version == "v2":
        schema_relpath = EXPOSURE_INTENT_V2_SCHEMA_RELPATH
    else:
        raise ValueError(f"INTENTS_DAY_COMPLETENESS_UNSUPPORTED_SCHEMA_VERSION:path={path}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    expected_prefix = path.name.split(".", 1)[0].strip().lower()
    actual_sha = sha256_file_v1(path).lower()
    if expected_prefix != actual_sha:
        raise ValueError(f"INTENTS_DAY_COMPLETENESS_FILENAME_HASH_MISMATCH:path={path}")


def _validate_no_intents_marker(path: Path, *, day_utc: str) -> None:
    payload = read_json_object_v1(path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, NO_INTENTS_SCHEMA_RELPATH_V1)
    if str(payload.get("day_utc") or "").strip() != parse_day_utc_v1(day_utc):
        raise ValueError(f"INTENTS_DAY_COMPLETENESS_NO_INTENTS_DAY_MISMATCH:path={path}")


def _build_payload(*, day_utc: str, truth_root: Path) -> dict[str, Any]:
    day = parse_day_utc_v1(day_utc)
    intents_dir = _day_intents_dir(truth_root=truth_root, day_utc=day)
    session_id = canonical_paper_session_id_v1(day)
    produced_at_utc = now_utc_iso_v1()
    checked: list[dict[str, Any]] = []
    missing_inputs: list[str] = []
    blocking_codes: set[str] = set()
    completeness_status = "UNKNOWN"
    freshness_verdict = "UNKNOWN"
    linkage_verdict = "UNLINKED"

    if not intents_dir.exists() or not intents_dir.is_dir():
        checked.append(
            build_fact_dependency_row_v1(
                logical_name="intents_day_directory",
                absolute_path=intents_dir,
                status="MISSING",
                reason_codes=["INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR"],
                day_utc=day,
            )
        )
        missing_inputs.append(str(intents_dir))
        blocking_codes.add("INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR")
        completeness_status = "INCOMPLETE"
    else:
        freshness_verdict = "CURRENT"
        linkage_verdict = "LINKED"
        all_json = sorted(path.resolve() for path in intents_dir.iterdir() if path.is_file() and path.name.endswith(".json"))
        marker_path = (intents_dir / "no_intents_day.v1.json").resolve()
        intent_files = [path for path in all_json if path.name != "no_intents_day.v1.json"]
        checked.append(
            build_fact_dependency_row_v1(
                logical_name="intents_day_directory",
                absolute_path=intents_dir,
                status="PRESENT",
                reason_codes=[],
                day_utc=day,
            )
        )
        if marker_path.exists():
            try:
                _validate_no_intents_marker(marker_path, day_utc=day)
                checked.append(
                    build_fact_dependency_row_v1(
                        logical_name="no_intents_day_marker",
                        absolute_path=marker_path,
                        status="PRESENT",
                        reason_codes=[],
                        day_utc=day,
                    )
                )
            except Exception as exc:
                checked.append(
                    build_fact_dependency_row_v1(
                        logical_name="no_intents_day_marker",
                        absolute_path=marker_path,
                        status="INVALID",
                        reason_codes=[str(exc)],
                        day_utc=day,
                    )
                )
                blocking_codes.add("INTENTS_DAY_COMPLETENESS_INVALID_NO_INTENTS_MARKER")
                completeness_status = "MALFORMED"
                linkage_verdict = "UNLINKED"

        for path in intent_files:
            try:
                _validate_intent_file(path)
                checked.append(
                    build_fact_dependency_row_v1(
                        logical_name=f"intent_snapshot:{path.name}",
                        absolute_path=path,
                        status="PRESENT",
                        reason_codes=[],
                        day_utc=day,
                    )
                )
            except Exception as exc:
                checked.append(
                    build_fact_dependency_row_v1(
                        logical_name=f"intent_snapshot:{path.name}",
                        absolute_path=path,
                        status="INVALID",
                        reason_codes=[str(exc)],
                        day_utc=day,
                    )
                )
                blocking_codes.add("INTENTS_DAY_COMPLETENESS_INVALID_INTENT_FILE")
                completeness_status = "MALFORMED"
                linkage_verdict = "UNLINKED"

        if not all_json:
            blocking_codes.add("INTENTS_DAY_COMPLETENESS_EMPTY_DAY_DIR")
            completeness_status = "INCOMPLETE"
            missing_inputs.append(str(intents_dir))
        elif marker_path.exists() and intent_files:
            blocking_codes.add("INTENTS_DAY_COMPLETENESS_CONTRADICTORY_MARKER_AND_INTENTS")
            completeness_status = "INCOMPLETE"
            linkage_verdict = "UNLINKED"
        elif completeness_status == "MALFORMED":
            pass
        elif marker_path.exists():
            blocking_codes.add("INTENTS_DAY_COMPLETENESS_NO_INTENTS_DECLARED")
            completeness_status = "NO_INTENTS_DECLARED"
        elif intent_files:
            completeness_status = "COMPLETE"
        else:
            blocking_codes.add("INTENTS_DAY_COMPLETENESS_NO_SUPPORTED_INPUTS")
            completeness_status = "UNKNOWN"
            linkage_verdict = "UNLINKED"

    return {
        "schema_id": "intents_day_completeness",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
        "day_utc": day,
        "session_id": session_id,
        "completeness_status": completeness_status,
        "required_inputs_checked": checked,
        "missing_inputs": sorted(set(missing_inputs)),
        "freshness_verdict": freshness_verdict,
        "linkage_verdict": linkage_verdict,
        "blocking_codes": sorted(blocking_codes),
        "producer": producer_block_v1(module="ops/tools/run_intents_day_completeness_v1.py"),
        "produced_at_utc": produced_at_utc,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_intents_day_completeness_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    decision_truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    intent_truth_root = resolve_paper_intent_truth_root_v1(
        truth_root=decision_truth_root,
        repo_root=REPO_ROOT,
    )
    payload = _build_payload(day_utc=args.day_utc, truth_root=intent_truth_root)
    ref = atomic_write_validated_json_v1(
        path=resolve_intents_day_completeness_path(truth_root=decision_truth_root, day_utc=args.day_utc),
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA_RELPATH_V1,
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "completeness_status": str(payload.get("completeness_status") or "").strip(),
                "session_id": str(payload.get("session_id") or "").strip(),
            },
            sort_keys=True,
        )
    )
    return 0 if str(payload.get("completeness_status") or "").strip() == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
