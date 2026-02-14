# constellation_2/phaseB/lib/build_freshness_certificate_v1.py
#
# Constellation 2.0 — Phase B
# Build FreshnessCertificate v1 bound to an OptionsChainSnapshot v1.
#
# Determinism:
# - No use of system time.
# - issued_at_utc == snapshot.as_of_utc
# - valid_from_utc == snapshot.as_of_utc
# - valid_until_utc == snapshot.as_of_utc + max_age_seconds
#
# Fail-closed:
# - Any invalid input => raise
# - Any schema validation failure => raise

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseB.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, _walk_assert_no_floats
from constellation_2.phaseB.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class FreshnessBuildError(Exception):
    pass


def _parse_utc_z(ts: Any, field_name: str) -> datetime:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise FreshnessBuildError(f"TIMESTAMP_NOT_Z_UTC: {field_name}")
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:  # noqa: BLE001
        raise FreshnessBuildError(f"TIMESTAMP_INVALID: {field_name}: {e}") from e
    if dt.tzinfo is None:
        raise FreshnessBuildError(f"TIMESTAMP_MISSING_TZ: {field_name}")
    return dt.astimezone(timezone.utc)


def _fmt_utc_z(dt: datetime) -> str:
    dt2 = dt.astimezone(timezone.utc)
    return dt2.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def build_freshness_certificate_v1(
    snapshot: Dict[str, Any],
    repo_root: Path,
    max_age_seconds: int,
    clock_skew_tolerance_seconds: int,
) -> Dict[str, Any]:
    # Determinism guard: forbid floats anywhere in snapshot.
    try:
        _walk_assert_no_floats(snapshot, "$")
    except CanonicalizationError as e:
        raise FreshnessBuildError(f"SNAPSHOT_FLOAT_FORBIDDEN: {e}") from e

    if not isinstance(max_age_seconds, int) or max_age_seconds < 1 or max_age_seconds > 86400:
        raise FreshnessBuildError("POLICY_MAX_AGE_SECONDS_INVALID")
    if not isinstance(clock_skew_tolerance_seconds, int) or clock_skew_tolerance_seconds < 0 or clock_skew_tolerance_seconds > 3600:
        raise FreshnessBuildError("POLICY_CLOCK_SKEW_TOLERANCE_INVALID")

    as_of_utc_str = snapshot.get("as_of_utc")
    as_of_dt = _parse_utc_z(as_of_utc_str, "snapshot.as_of_utc")

    # Deterministic issued time: equals snapshot time (no "now").
    issued_at = as_of_dt
    valid_from = as_of_dt
    valid_until = as_of_dt + timedelta(seconds=max_age_seconds)

    prov = snapshot.get("provenance")
    if not isinstance(prov, dict):
        raise FreshnessBuildError("SNAPSHOT_PROVENANCE_MISSING_OR_INVALID")
    source = prov.get("source")
    capture_method = prov.get("capture_method")
    if not isinstance(source, str) or not source.strip():
        raise FreshnessBuildError("SNAPSHOT_PROVENANCE_SOURCE_INVALID")
    if not isinstance(capture_method, str) or not capture_method.strip():
        raise FreshnessBuildError("SNAPSHOT_PROVENANCE_CAPTURE_METHOD_INVALID")

    # Hash of canonical snapshot form with canonical_json_hash forced to null
    snapshot_hash = canonical_hash_for_c2_artifact_v1(snapshot)

    cert = {
        "schema_id": "freshness_certificate",
        "schema_version": "v1",
        "issued_at_utc": _fmt_utc_z(issued_at),
        "valid_from_utc": _fmt_utc_z(valid_from),
        "valid_until_utc": _fmt_utc_z(valid_until),
        "snapshot_hash": snapshot_hash,
        "snapshot_as_of_utc": _fmt_utc_z(as_of_dt),
        "source": str(source).strip(),
        "capture_method": str(capture_method).strip(),
        "policy": {
            "max_age_seconds": int(max_age_seconds),
            "clock_skew_tolerance_seconds": int(clock_skew_tolerance_seconds),
        },
        "canonical_json_hash": None,
    }

    # Schema validate (fail-closed)
    try:
        validate_against_repo_schema_v1(
            cert,
            repo_root=repo_root,
            schema_relpath="constellation_2/schemas/freshness_certificate.v1.schema.json",
        )
    except SchemaValidationError as e:
        raise FreshnessBuildError(f"CERT_SCHEMA_INVALID: {e}") from e

    return cert
