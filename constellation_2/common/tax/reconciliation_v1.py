from __future__ import annotations

from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    algorithm_version_v1,
    canonical_hash_v1,
    decimal_from_value_v1,
    quantize_basis_v1,
    quantize_gain_loss_v1,
    quantize_money_v1,
    quantize_quantity_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.constants_v1 import TAX_RECONCILIATION_MISMATCH_CODES_V1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1
from constellation_2.common.tax.truth_v1 import effective_accepted_facts_v1


def _ordered_mismatch_codes_v1(codes: Iterable[str]) -> list[str]:
    rank = {code: index for index, code in enumerate(TAX_RECONCILIATION_MISMATCH_CODES_V1)}
    return sorted({str(code) for code in codes if str(code).strip()}, key=lambda code: (rank.get(code, 999), code))


def _current_effective_facts_v1(accepted_facts: Iterable[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    facts = tuple(dict(item) for item in accepted_facts)
    if not facts:
        return ()
    cutoff = max(str(item["recorded_at"]) for item in facts)
    return effective_accepted_facts_v1(accepted_facts=facts, recorded_at_cutoff=cutoff)


def _lot_open_dates_v1(accepted_facts: Iterable[dict[str, Any]]) -> dict[str, str]:
    lot_dates: dict[str, str] = {}
    for fact in _current_effective_facts_v1(accepted_facts):
        family = str(fact.get("fact_family") or "")
        payload = dict(fact.get("payload") or {})
        lot_id = str(payload.get("lot_id") or "").strip()
        if not lot_id:
            continue
        if family in {"lot_opened", "lot_imported"}:
            lot_dates[lot_id] = str(payload.get("holding_period_start_at") or "")
        elif family == "buy_execution_posted" and lot_id not in lot_dates:
            lot_dates[lot_id] = str(payload.get("holding_period_start_at") or payload.get("executed_at") or "")
    return lot_dates


def _lot_row_map_v1(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        lot_id = str(row.get("lot_id") or "").strip()
        if lot_id:
            result[lot_id] = dict(row)
    return result


def build_lot_reconciliation_report_v1(
    *,
    snapshot: dict[str, Any],
    accepted_facts: Iterable[dict[str, Any]],
    broker_lot_view: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    algorithm_version_v1("reconciliation")
    constellation_rows = _lot_row_map_v1(snapshot.get("lot_states") or ())
    broker_rows = _lot_row_map_v1(broker_lot_view)
    lot_open_dates = _lot_open_dates_v1(accepted_facts)
    matched_items: list[dict[str, Any]] = []
    unmatched_items: list[dict[str, Any]] = []
    summary = {code: 0 for code in TAX_RECONCILIATION_MISMATCH_CODES_V1}
    for lot_id in sorted(set(constellation_rows) | set(broker_rows)):
        constellation_row = constellation_rows.get(lot_id)
        broker_row = broker_rows.get(lot_id)
        if constellation_row is None:
            summary["missing_in_constellation"] += 1
            unmatched_items.append({"lot_id": lot_id, "mismatch_codes": ["missing_in_constellation"], "broker_view": broker_row})
            continue
        if broker_row is None:
            summary["missing_in_broker"] += 1
            unmatched_items.append({"lot_id": lot_id, "mismatch_codes": ["missing_in_broker"], "constellation_view": constellation_row})
            continue
        mismatch_codes: list[str] = []
        if quantize_quantity_v1(constellation_row.get("remaining_quantity")) != quantize_quantity_v1(broker_row.get("remaining_quantity")):
            mismatch_codes.append("quantity_mismatch")
        if quantize_basis_v1(constellation_row.get("basis_total")) != quantize_basis_v1(broker_row.get("basis_total")):
            mismatch_codes.append("basis_mismatch")
        constellation_acquired_at = str(lot_open_dates.get(lot_id) or "")
        broker_acquired_at = str(broker_row.get("acquisition_date") or "")
        if constellation_acquired_at != broker_acquired_at:
            mismatch_codes.append("acquisition_date_mismatch")
        broker_holding_period = str(broker_row.get("holding_period_state") or "")
        if broker_holding_period and broker_holding_period != str(constellation_row.get("holding_period_state") or ""):
            mismatch_codes.append("holding_period_mismatch")
        broker_corp_refs = sorted(str(item) for item in (broker_row.get("corporate_action_refs") or ()) if str(item).strip())
        constellation_corp_refs = sorted(str(item) for item in (constellation_row.get("corporate_action_refs") or ()) if str(item).strip())
        if broker_corp_refs != constellation_corp_refs:
            mismatch_codes.append("corporate_action_mismatch")
        if mismatch_codes:
            ordered_codes = _ordered_mismatch_codes_v1(mismatch_codes)
            for code in ordered_codes:
                summary[code] += 1
            unmatched_items.append(
                {
                    "lot_id": lot_id,
                    "mismatch_codes": ordered_codes,
                    "constellation_view": constellation_row,
                    "broker_view": broker_row,
                }
            )
            continue
        matched_items.append(
            {
                "lot_id": lot_id,
                "account_id": constellation_row["account_id"],
                "security_id": constellation_row["security_id"],
            }
        )
    payload = {
        "schema_id": "lot_reconciliation_report",
        "schema_version": "v1",
        "report_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "broker_lot_view": list(sorted(broker_rows)), "summary": summary}),
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "matched_items": matched_items,
        "unmatched_items": unmatched_items,
        "mismatch_summary": summary,
        "reason_codes": ["TAX_REPORT_GENERATED"],
    }
    return validate_tax_payload_v1(payload)


def _realized_key_v1(row: dict[str, Any]) -> str:
    return canonical_hash_v1(
        {
            "lot_id": row.get("lot_id"),
            "security_id": row.get("security_id"),
            "account_id": row.get("account_id"),
            "realized_at": row.get("realized_at"),
        }
    )


def _accepted_realized_rows_v1(accepted_facts: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for fact in _current_effective_facts_v1(accepted_facts):
        if str(fact.get("fact_family") or "") != "realization_recorded":
            continue
        payload = dict(fact.get("payload") or {})
        row = {
            "lot_id": payload.get("lot_id"),
            "security_id": payload.get("security_id"),
            "account_id": payload.get("account_id"),
            "realized_at": payload.get("realized_at"),
            "realized_gain_loss": quantize_gain_loss_v1(payload.get("realized_gain_loss")),
            "realized_proceeds": quantize_money_v1(payload.get("realized_proceeds") or "0.00"),
        }
        rows[_realized_key_v1(row)] = row
    return rows


def build_broker_tax_reconciliation_report_v1(
    *,
    snapshot: dict[str, Any],
    accepted_facts: Iterable[dict[str, Any]],
    broker_lot_view: Iterable[dict[str, Any]],
    broker_realized_view: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    algorithm_version_v1("reconciliation")
    lot_report = build_lot_reconciliation_report_v1(
        snapshot=snapshot,
        accepted_facts=accepted_facts,
        broker_lot_view=broker_lot_view,
    )
    accepted_realized = _accepted_realized_rows_v1(accepted_facts)
    broker_realized = {
        _realized_key_v1(row): {
            "lot_id": row.get("lot_id"),
            "security_id": row.get("security_id"),
            "account_id": row.get("account_id"),
            "realized_at": row.get("realized_at"),
            "realized_gain_loss": quantize_gain_loss_v1(row.get("realized_gain_loss")),
            "realized_proceeds": quantize_money_v1(row.get("realized_proceeds") or "0.00"),
        }
        for row in broker_realized_view
    }
    matched_items = [{"item_type": "lot", **item} for item in lot_report["matched_items"]]
    unmatched_items = [{"item_type": "lot", **item} for item in lot_report["unmatched_items"]]
    summary = dict(lot_report["mismatch_summary"])
    realized_items: list[dict[str, Any]] = []
    for key in sorted(set(accepted_realized) | set(broker_realized)):
        constellation_row = accepted_realized.get(key)
        broker_row = broker_realized.get(key)
        if constellation_row is None:
            summary["missing_in_constellation"] += 1
            unmatched_items.append({"item_type": "realized", "mismatch_codes": ["missing_in_constellation"], "broker_view": broker_row})
            realized_items.append({"comparison_key": key, "status": "missing_in_constellation", "broker_view": broker_row})
            continue
        if broker_row is None:
            summary["missing_in_broker"] += 1
            unmatched_items.append({"item_type": "realized", "mismatch_codes": ["missing_in_broker"], "constellation_view": constellation_row})
            realized_items.append({"comparison_key": key, "status": "missing_in_broker", "constellation_view": constellation_row})
            continue
        mismatch_codes: list[str] = []
        if quantize_money_v1(constellation_row["realized_proceeds"]) != quantize_money_v1(broker_row["realized_proceeds"]):
            mismatch_codes.append("realized_proceeds_mismatch")
        if quantize_gain_loss_v1(constellation_row["realized_gain_loss"]) != quantize_gain_loss_v1(broker_row["realized_gain_loss"]):
            mismatch_codes.append("realized_gain_loss_mismatch")
        if mismatch_codes:
            ordered_codes = _ordered_mismatch_codes_v1(mismatch_codes)
            for code in ordered_codes:
                summary[code] += 1
            unmatched_items.append(
                {
                    "item_type": "realized",
                    "comparison_key": key,
                    "mismatch_codes": ordered_codes,
                    "constellation_view": constellation_row,
                    "broker_view": broker_row,
                }
            )
            realized_items.append({"comparison_key": key, "status": "mismatch", "mismatch_codes": ordered_codes})
            continue
        matched_items.append({"item_type": "realized", "comparison_key": key})
        realized_items.append({"comparison_key": key, "status": "matched"})
    payload = {
        "schema_id": "broker_tax_reconciliation_report",
        "schema_version": "v1",
        "report_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "lot_report_id": lot_report["report_id"], "summary": summary}),
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "status": "matched" if not unmatched_items else "unmatched",
        "lot_report_id": lot_report["report_id"],
        "realized_items": realized_items,
        "matched_items": matched_items,
        "unmatched_items": unmatched_items,
        "mismatch_summary": summary,
        "reason_codes": list(require_reason_codes_v1(["TAX_REPORT_GENERATED"])),
    }
    return validate_tax_payload_v1(payload)
