from __future__ import annotations

from typing import Any

from constellation_2.common.constitutional_runtime_v1 import validate_read_model_payload_v1


REQUIRED_ENVELOPE_FIELDS = (
    "view_name",
    "surface_kind",
    "entity_scope",
    "truth_state",
    "as_of_utc",
    "freshness_state",
    "source_authority",
    "contract_id",
    "contract_version",
    "provenance_refs",
    "degradation_codes",
)

RECOMMENDED_ITEM_FIELDS = (
    "entity_id",
    "truth_state",
    "as_of_utc",
    "freshness_state",
    "source_authority",
    "provenance_refs",
    "degradation_codes",
)

ALLOWED_SURFACE_KINDS = (
    "projection",
    "composition",
)


def _presence_map(data: dict[str, Any], fields: tuple[str, ...]) -> dict[str, bool]:
    return {field: field in data for field in fields}


def validate_projection_envelope(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {
            "ok": False,
            "kind": "projection_envelope",
            "errors": ["envelope is not a dict"],
            "missing_fields": list(REQUIRED_ENVELOPE_FIELDS),
            "field_presence": {},
            "surface_kind_valid": False,
        }

    missing_fields = [field for field in REQUIRED_ENVELOPE_FIELDS if field not in data]
    surface_kind = data.get("surface_kind")
    surface_kind_valid = surface_kind in ALLOWED_SURFACE_KINDS
    errors: list[str] = []
    if not surface_kind_valid:
        errors.append("surface_kind is missing or invalid")

    constitutional = validate_read_model_payload_v1(data)
    errors.extend(list(constitutional.get("errors") or []))

    return {
        "ok": not missing_fields and surface_kind_valid and bool(constitutional.get("ok")),
        "kind": "projection_envelope",
        "errors": errors,
        "missing_fields": missing_fields,
        "field_presence": _presence_map(data, REQUIRED_ENVELOPE_FIELDS),
        "surface_kind_valid": surface_kind_valid,
        "surface_kind": surface_kind,
        "constitutional_non_authority_ok": bool(constitutional.get("ok")),
    }


def validate_item_metadata(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return {
            "ok": False,
            "kind": "item_metadata",
            "errors": ["item is not a dict"],
            "missing_required_fields": [],
            "missing_recommended_fields": list(RECOMMENDED_ITEM_FIELDS),
            "field_presence": {},
        }

    missing_required_fields: list[str] = []
    missing_recommended_fields = [
        field for field in RECOMMENDED_ITEM_FIELDS if field not in item
    ]
    return {
        "ok": not missing_required_fields,
        "kind": "item_metadata",
        "errors": [],
        "missing_required_fields": missing_required_fields,
        "missing_recommended_fields": missing_recommended_fields,
        "field_presence": _presence_map(item, RECOMMENDED_ITEM_FIELDS),
    }
