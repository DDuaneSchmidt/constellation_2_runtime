from __future__ import annotations

from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    canonical_hash_v1,
    now_utc_iso_v1,
    require_list_of_strings_v1,
    require_nonempty_str_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.constants_v1 import TAX_SCOPE_TYPES_V1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


def _validate_scope_type_v1(scope_type: str) -> str:
    normalized = require_nonempty_str_v1(scope_type, "scope_type")
    if normalized not in TAX_SCOPE_TYPES_V1:
        raise ValueError(f"TAX_SCOPE_TYPE_INVALID:{normalized}")
    return normalized


def build_tax_scope_registry_v1(*, scope_records: Iterable[dict[str, Any]], produced_utc: str | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for raw in scope_records:
        row = {
            "scope_id": require_nonempty_str_v1(raw.get("scope_id"), "scope_id"),
            "scope_type": _validate_scope_type_v1(str(raw.get("scope_type") or "")),
            "parent_scope_ids": list(require_list_of_strings_v1(raw.get("parent_scope_ids") or ())),
            "member_refs": list(require_list_of_strings_v1(raw.get("member_refs") or ())),
            "notes": list(require_list_of_strings_v1(raw.get("notes") or ())),
        }
        rows.append(row)
    payload = {
        "schema_id": "tax_scope_registry",
        "schema_version": "v1",
        "registry_id": canonical_hash_v1(rows),
        "produced_utc": produced_utc or now_utc_iso_v1(),
        "scope_records": sorted(rows, key=lambda row: (row["scope_type"], row["scope_id"])),
    }
    return validate_tax_payload_v1(payload)


def build_tax_scope_membership_v1(
    *,
    scope_id: str,
    member_ref: str,
    member_type: str,
    produced_utc: str | None = None,
    reason_codes: Iterable[str] = (),
) -> dict[str, Any]:
    payload = {
        "schema_id": "tax_scope_membership",
        "schema_version": "v1",
        "membership_id": canonical_hash_v1({"scope_id": scope_id, "member_ref": member_ref, "member_type": member_type}),
        "scope_id": require_nonempty_str_v1(scope_id, "scope_id"),
        "member_ref": require_nonempty_str_v1(member_ref, "member_ref"),
        "member_type": require_nonempty_str_v1(member_type, "member_type"),
        "produced_utc": produced_utc or now_utc_iso_v1(),
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(payload)


def build_wash_enforcement_scope_v1(
    *,
    scope_id: str,
    filing_scope_ids: Iterable[str],
    legal_owner_scope_ids: Iterable[str],
    produced_utc: str | None = None,
    reason_codes: Iterable[str] = (),
) -> dict[str, Any]:
    payload = {
        "schema_id": "wash_enforcement_scope",
        "schema_version": "v1",
        "wash_enforcement_scope_id": canonical_hash_v1(
            {
                "scope_id": scope_id,
                "filing_scope_ids": tuple(sorted(filing_scope_ids)),
                "legal_owner_scope_ids": tuple(sorted(legal_owner_scope_ids)),
            }
        ),
        "scope_id": require_nonempty_str_v1(scope_id, "scope_id"),
        "filing_scope_ids": list(require_list_of_strings_v1(filing_scope_ids)),
        "legal_owner_scope_ids": list(require_list_of_strings_v1(legal_owner_scope_ids)),
        "produced_utc": produced_utc or now_utc_iso_v1(),
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(payload)

