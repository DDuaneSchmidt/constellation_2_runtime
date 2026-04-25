from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.post_entry_core4_shared_v1 import REPO_ROOT, canonical_hash_v1, coerce_utc_v1, freeze_json_v1, thaw_json_v1


REQUEST_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_action_request.v1.schema.json"
ACTION_CLOSE_TRADE = "CLOSE_TRADE"
ACTION_REDUCE_TRADE = "REDUCE_TRADE"
ACTION_AMEND_PROTECTION = "AMEND_PROTECTION"
ACTION_CANCEL_ORDER = "CANCEL_ORDER"
SUPPORTED_ACTIONS = {
    ACTION_CLOSE_TRADE,
    ACTION_REDUCE_TRADE,
    ACTION_AMEND_PROTECTION,
    ACTION_CANCEL_ORDER,
}
ACTIONS_REQUIRING_QUANTITY = {ACTION_CLOSE_TRADE, ACTION_REDUCE_TRADE, ACTION_AMEND_PROTECTION}
ACTIONS_REQUIRING_ORDER_PARAMETERS = {ACTION_CLOSE_TRADE, ACTION_REDUCE_TRADE, ACTION_AMEND_PROTECTION}
ACTIONS_REQUIRING_LINKAGE = {ACTION_AMEND_PROTECTION, ACTION_CANCEL_ORDER}


@dataclass(frozen=True)
class PostEntryActionRequestV1:
    request_id: str
    request_seal_id: str
    action_class: str
    trade_identity_id: str
    requested_quantity: str | None
    order_parameters_json: str | None
    target_order_linkage_json: str | None
    execution_identity_json: str
    created_at_utc: str

    def order_parameters(self) -> dict[str, Any] | None:
        value = thaw_json_v1(self.order_parameters_json)
        return value if isinstance(value, dict) else None

    def target_order_linkage(self) -> dict[str, Any] | None:
        value = thaw_json_v1(self.target_order_linkage_json)
        return value if isinstance(value, dict) else None

    def execution_identity(self) -> dict[str, Any]:
        value = thaw_json_v1(self.execution_identity_json)
        if not isinstance(value, dict):
            raise ValueError("REQUEST_EXECUTION_IDENTITY_INVALID")
        return value

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_id": "post_entry_action_request",
            "schema_version": "v1",
            "request_id": self.request_id,
            "request_seal_id": self.request_seal_id,
            "action_class": self.action_class,
            "trade_identity_id": self.trade_identity_id,
            "requested_quantity": self.requested_quantity,
            "order_parameters": self.order_parameters(),
            "target_order_linkage": self.target_order_linkage(),
            "execution_identity": self.execution_identity(),
            "created_at_utc": self.created_at_utc,
        }


def _positive_decimal_text(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"REQUEST_MISSING_REQUIRED_FIELD:{field_name}")
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"REQUEST_QUANTITY_INVALID:{field_name}") from exc
    if parsed <= 0:
        raise ValueError(f"REQUEST_QUANTITY_INVALID:{field_name}")
    return format(parsed.normalize(), "f")


def _require_dict(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"REQUEST_MISSING_REQUIRED_FIELD:{field_name}")
    return {str(key): value[key] for key in sorted(value)}


def _normalize_execution_identity(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity")
    environment = str(value.get("environment") or "").strip().upper()
    sleeve_id = str(value.get("sleeve_id") or "").strip().upper()
    account_id = str(value.get("account_id") or "").strip()
    execution_root_ref = str(value.get("execution_root_ref") or "").strip()
    raw_client_id_orders = value.get("client_id_orders")
    if not environment:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity.environment")
    if not sleeve_id:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity.sleeve_id")
    if not account_id:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity.account_id")
    if not execution_root_ref:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity.execution_root_ref")
    try:
        client_id_orders = int(raw_client_id_orders)
    except Exception as exc:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity.client_id_orders") from exc
    if client_id_orders <= 0:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:execution_identity.client_id_orders")
    return {
        "environment": environment,
        "sleeve_id": sleeve_id,
        "account_id": account_id,
        "client_id_orders": client_id_orders,
        "execution_root_ref": execution_root_ref,
    }


def seal_post_entry_action_request_v1(payload: Mapping[str, Any]) -> PostEntryActionRequestV1:
    raw = dict(payload)
    schema_id = str(raw.get("schema_id") or "post_entry_action_request").strip()
    schema_version = str(raw.get("schema_version") or "v1").strip()
    if schema_id != "post_entry_action_request":
        raise ValueError(f"REQUEST_SCHEMA_ID_UNSUPPORTED:{schema_id}")
    if schema_version != "v1":
        raise ValueError(f"REQUEST_SCHEMA_VERSION_UNSUPPORTED:{schema_version}")

    action_class = str(raw.get("action_class") or "").strip().upper()
    if action_class not in SUPPORTED_ACTIONS:
        raise ValueError(f"REQUEST_ACTION_CLASS_UNSUPPORTED:{action_class or 'MISSING'}")

    request_id = str(raw.get("request_id") or "").strip()
    trade_identity_id = str(raw.get("trade_identity_id") or "").strip().lower()
    if not request_id:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:request_id")
    if not trade_identity_id:
        raise ValueError("REQUEST_MISSING_REQUIRED_FIELD:trade_identity_id")

    requested_quantity: str | None = None
    if action_class in ACTIONS_REQUIRING_QUANTITY:
        requested_quantity = _positive_decimal_text(raw.get("requested_quantity"), field_name="requested_quantity")
    elif raw.get("requested_quantity") not in (None, ""):
        raise ValueError("REQUEST_PARAMETERS_INVALID:requested_quantity_not_allowed")

    order_parameters: dict[str, Any] | None = None
    if action_class in ACTIONS_REQUIRING_ORDER_PARAMETERS:
        order_parameters = _require_dict(raw.get("order_parameters"), field_name="order_parameters")
    elif raw.get("order_parameters") not in (None, {}):
        raise ValueError("REQUEST_PARAMETERS_INVALID:order_parameters_not_allowed")

    target_order_linkage: dict[str, Any] | None = None
    if action_class in ACTIONS_REQUIRING_LINKAGE:
        target_order_linkage = _require_dict(raw.get("target_order_linkage"), field_name="target_order_linkage")
    elif raw.get("target_order_linkage") not in (None, {}):
        raise ValueError("REQUEST_PARAMETERS_INVALID:target_order_linkage_not_allowed")

    execution_identity = _normalize_execution_identity(raw.get("execution_identity"))
    created_at_utc = coerce_utc_v1(str(raw.get("created_at_utc") or ""))

    final_payload = {
        "schema_id": "post_entry_action_request",
        "schema_version": "v1",
        "request_id": request_id,
        "action_class": action_class,
        "trade_identity_id": trade_identity_id,
        "requested_quantity": requested_quantity,
        "order_parameters": order_parameters,
        "target_order_linkage": target_order_linkage,
        "execution_identity": execution_identity,
        "created_at_utc": created_at_utc,
    }
    final_payload["request_seal_id"] = canonical_hash_v1(final_payload)
    validate_against_repo_schema_v1(final_payload, REPO_ROOT, REQUEST_SCHEMA_RELPATH)
    return PostEntryActionRequestV1(
        request_id=request_id,
        request_seal_id=str(final_payload["request_seal_id"]),
        action_class=action_class,
        trade_identity_id=trade_identity_id,
        requested_quantity=requested_quantity,
        order_parameters_json=freeze_json_v1(order_parameters),
        target_order_linkage_json=freeze_json_v1(target_order_linkage),
        execution_identity_json=freeze_json_v1(execution_identity) or "{}",
        created_at_utc=created_at_utc,
    )
