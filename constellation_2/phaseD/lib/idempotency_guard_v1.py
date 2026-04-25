"""
idempotency_guard_v1.py

Constellation 2.0 Phase D
Idempotent submission control (HARD FAIL on duplicates).

Authority:
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md
  - C2_IDEMPOTENCY_DUPLICATE_SUBMISSION (HARD FAIL)
- constellation_2/governance/C2_DETERMINISM_STANDARD.md

Rule:
- submission_id MUST be derived from binding_hash (deterministic)
- Prior evidence must be classified before retry:
  - brokered evidence (real broker identity) => HARD FAIL
  - malformed/ambiguous evidence => HARD FAIL (fail closed)
  - veto/dry-run/failed-before-broker evidence => controlled retry allowed
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class IdempotencyError(Exception):
    pass


def derive_submission_id_from_binding_hash_v1(binding_hash: str) -> str:
    """
    Deterministic: submission_id is the binding_hash itself (64 hex chars).
    This is stable, reproducible, and meets schema constraints (minLength 16).
    """
    if not isinstance(binding_hash, str) or len(binding_hash) != 64:
        raise IdempotencyError("BINDING_HASH_INVALID_FOR_SUBMISSION_ID")
    return binding_hash


@dataclass(frozen=True)
class IdempotencyCheckV1:
    submission_id: str
    already_exists: bool
    existing_path: Optional[str]
    classification: str
    retry_allowed: bool
    diagnostic: str


def _has_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _coerce_bool(value: object) -> tuple[Optional[bool], bool]:
    if value is None:
        return None, False
    if isinstance(value, bool):
        return value, True
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "1"}:
            return True, True
        if normalized in {"false", "no", "n", "0"}:
            return False, True
    if isinstance(value, int) and value in {0, 1}:
        return bool(value), True
    return None, False


def classify_existing_submission_evidence_v1(*, submissions_root: Path, submission_id: str) -> IdempotencyCheckV1:
    if not submissions_root.exists():
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=False,
            existing_path=None,
            classification="NONE",
            retry_allowed=True,
            diagnostic="SUBMISSION_DIR_ABSENT",
        )
    if not submissions_root.is_dir():
        raise IdempotencyError(f"SUBMISSIONS_ROOT_NOT_DIRECTORY: {str(submissions_root)}")

    submission_dir = (submissions_root / submission_id).resolve()
    if not submission_dir.exists():
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=False,
            existing_path=None,
            classification="NONE",
            retry_allowed=True,
            diagnostic="SUBMISSION_DIR_ABSENT",
        )
    if not submission_dir.is_dir():
        raise IdempotencyError(f"SUBMISSION_PATH_NOT_DIRECTORY: {str(submission_dir)}")

    json_files = sorted(path for path in submission_dir.rglob("*.json") if path.is_file())
    broker_files = [path for path in json_files if path.name.startswith("broker_submission_record.")]
    veto_files = [path for path in json_files if path.name.startswith("veto_record.")]
    malformed_reasons: list[str] = []
    has_broker_ids = False
    has_broker_transmitted_true = False
    has_broker_transmitted_false = False
    has_dry_run_true = False
    has_dry_run_false = False
    has_broker_error = False
    has_dry_run_marker_code = False

    for broker_path in broker_files:
        try:
            broker_obj = json.loads(broker_path.read_text(encoding="utf-8"))
        except Exception as exc:
            malformed_reasons.append(f"MALFORMED_JSON:{broker_path}:{type(exc).__name__}")
            continue
        if not isinstance(broker_obj, dict):
            malformed_reasons.append(f"MALFORMED_OBJECT:{broker_path}")
            continue

        schema_id = str(broker_obj.get("schema_id") or "").strip()
        if schema_id and schema_id != "broker_submission_record":
            malformed_reasons.append(f"SCHEMA_ID_INVALID:{broker_path}:{schema_id}")

        broker_ids_obj = broker_obj.get("broker_ids")
        if broker_ids_obj is None:
            broker_ids: dict[str, object] = {}
        elif isinstance(broker_ids_obj, dict):
            broker_ids = broker_ids_obj
        else:
            malformed_reasons.append(f"BROKER_IDS_MALFORMED:{broker_path}")
            broker_ids = {}

        if _has_value(broker_ids.get("order_id")) or _has_value(broker_ids.get("perm_id")):
            has_broker_ids = True

        broker_transmitted, broker_transmitted_ok = _coerce_bool(broker_obj.get("broker_transmitted"))
        if broker_obj.get("broker_transmitted") is not None and not broker_transmitted_ok:
            malformed_reasons.append(f"BROKER_TRANSMITTED_INVALID:{broker_path}")
        elif broker_transmitted is True:
            has_broker_transmitted_true = True
        elif broker_transmitted is False:
            has_broker_transmitted_false = True

        dry_run, dry_run_ok = _coerce_bool(broker_obj.get("dry_run"))
        if broker_obj.get("dry_run") is not None and not dry_run_ok:
            malformed_reasons.append(f"DRY_RUN_INVALID:{broker_path}")
        elif dry_run is True:
            has_dry_run_true = True
        elif dry_run is False:
            has_dry_run_false = True

        error_obj = broker_obj.get("error")
        if error_obj not in (None, "", {}, []):
            has_broker_error = True
            if isinstance(error_obj, dict):
                error_code = str(error_obj.get("code") or "").strip().upper()
                if error_code == "DRY_RUN_NO_BROKER_ID":
                    has_dry_run_marker_code = True

    if malformed_reasons:
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=True,
            existing_path=str(submission_dir),
            classification="E",
            retry_allowed=False,
            diagnostic="AMBIGUOUS_OR_MALFORMED_EVIDENCE:" + ";".join(malformed_reasons),
        )

    if has_broker_ids or has_broker_transmitted_true:
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=True,
            existing_path=str(submission_dir),
            classification="A",
            retry_allowed=False,
            diagnostic="BROKERED_EVIDENCE_PRESENT",
        )

    if not broker_files:
        if veto_files:
            return IdempotencyCheckV1(
                submission_id=submission_id,
                already_exists=True,
                existing_path=str(submission_dir),
                classification="C",
                retry_allowed=True,
                diagnostic="VETO_ONLY_NO_BROKER_RECORD",
            )
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=True,
            existing_path=str(submission_dir),
            classification="E",
            retry_allowed=False,
            diagnostic="AMBIGUOUS_NO_BROKER_RECORD_AND_NO_VETO",
        )

    if (has_dry_run_true or has_dry_run_marker_code) and not has_dry_run_false:
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=True,
            existing_path=str(submission_dir),
            classification="B",
            retry_allowed=True,
            diagnostic="DRY_RUN_ONLY_NO_BROKER_IDS",
        )

    if has_broker_error or has_broker_transmitted_false or veto_files:
        return IdempotencyCheckV1(
            submission_id=submission_id,
            already_exists=True,
            existing_path=str(submission_dir),
            classification="D",
            retry_allowed=True,
            diagnostic="FAILED_BEFORE_BROKER_IDENTITY",
        )

    return IdempotencyCheckV1(
        submission_id=submission_id,
        already_exists=True,
        existing_path=str(submission_dir),
        classification="E",
        retry_allowed=False,
        diagnostic="AMBIGUOUS_BROKER_EVIDENCE_WITHOUT_BROKER_IDS",
    )


def assert_idempotent_or_raise_v1(*, submissions_root: Path, submission_id: str) -> IdempotencyCheckV1:
    """
    Return idempotency classification and fail closed when retry is not safe.
    """
    check = classify_existing_submission_evidence_v1(
        submissions_root=submissions_root,
        submission_id=submission_id,
    )
    if check.already_exists and not check.retry_allowed:
        raise IdempotencyError(
            "C2_IDEMPOTENCY_DUPLICATE_SUBMISSION: "
            f"{submission_id} exists at {check.existing_path}; "
            f"classification={check.classification}; diagnostic={check.diagnostic}"
        )
    return check
