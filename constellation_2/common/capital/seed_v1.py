from __future__ import annotations

from typing import Any

from .constants_v1 import (
    ACCOUNT_SEED_ROWS_V1,
    BALANCE_SEED_DATE_V1,
    BALANCE_SEED_ROWS_V1,
    CASHFLOW_EVENT_SEED_ROWS_V1,
    CONFIDENCE_BAND_HIGH_MIN_V1,
    CONFIDENCE_BAND_MEDIUM_MIN_V1,
)
from .service_v1 import CapitalDomainServiceV1


def _band_from_level(level: float) -> str:
    if level >= CONFIDENCE_BAND_HIGH_MIN_V1:
        return "high"
    if level >= CONFIDENCE_BAND_MEDIUM_MIN_V1:
        return "medium"
    return "low"


def seed_capital_reference_dataset_v1(
    *,
    service: CapitalDomainServiceV1,
    changed_by: str = "capital_seed_v1",
    reason: str = "initial_reference_seed",
) -> dict[str, Any]:
    created_accounts = 0
    appended_classifications = 0
    appended_snapshots = 0
    appended_cashflow_events = 0

    for row in ACCOUNT_SEED_ROWS_V1:
        account_id = str(row["account_id"])
        account_name = str(row["account_name"])
        try:
            service.get_account(account_id=account_id)
        except ValueError:
            service.create_account(
                account_id=account_id,
                account_name=account_name,
                notes=str(row.get("notes") or ""),
                is_active=True,
                reason=reason,
            )
            created_accounts += 1

        existing_classifications = service.list_classifications(account_id=account_id)
        exact_match = False
        target_band = str(row.get("confidence_band") or _band_from_level(float(row["confidence_level"])))
        for existing in existing_classifications:
            if (
                str(existing.get("effective_from") or "") == BALANCE_SEED_DATE_V1
                and (existing.get("effective_to") is None)
                and str(existing.get("capital_type") or "") == str(row["capital_type"])
                and str(existing.get("control_type") or "") == str(row["control_type"])
                and str(existing.get("bucket_type") or "") == str(row["bucket_type"])
                and bool(int(existing.get("include_in_allocation") or 0)) == bool(row["include_in_allocation"])
                and float(existing.get("confidence_level") or 0.0) == float(row["confidence_level"])
                and str(existing.get("confidence_band") or "") == target_band
                and str(existing.get("notes") or "") == str(row.get("notes") or "")
            ):
                exact_match = True
                break
        if not exact_match:
            service.append_classification(
                account_id=account_id,
                effective_from=BALANCE_SEED_DATE_V1,
                effective_to=None,
                capital_type=str(row["capital_type"]),
                control_type=str(row["control_type"]),
                bucket_type=str(row["bucket_type"]),
                include_in_allocation=bool(row["include_in_allocation"]),
                confidence_level=float(row["confidence_level"]),
                confidence_band=target_band,
                notes=str(row.get("notes") or ""),
                changed_by=changed_by,
                reason=reason,
            )
            appended_classifications += 1

    for row in BALANCE_SEED_ROWS_V1:
        account_id = str(row["account_id"])
        has_seed_snapshot = any(
            str(existing.get("as_of_date") or "") == BALANCE_SEED_DATE_V1
            and str(existing.get("input_source") or "") == "seed_capture"
            for existing in service.latest_balance_per_account(as_of_date=BALANCE_SEED_DATE_V1)["rows"]
            if str(existing.get("account_id") or "") == account_id
        )
        if has_seed_snapshot:
            continue
        service.append_balance_snapshot(
            account_id=account_id,
            as_of_date=BALANCE_SEED_DATE_V1,
            balance=float(row["balance"]),
            input_source="seed_capture",
            notes="Reference import seed balance",
            reason=reason,
        )
        appended_snapshots += 1

    existing_events = service.list_cashflow_events()
    for row in CASHFLOW_EVENT_SEED_ROWS_V1:
        event_name = str(row["event_name"])
        event_type = str(row["event_type"])
        scenario = str(row["scenario"])
        start_date = str(row["start_date"])
        end_date = None if row.get("end_date") is None else str(row.get("end_date"))
        frequency = str(row["frequency"])
        amount = float(row["amount"])
        confidence = float(row["confidence"])
        is_deterministic = bool(row["is_deterministic"])
        notes = str(row.get("notes") or "")
        match = next(
            (
                existing
                for existing in existing_events
                if str(existing.get("event_name") or "") == event_name
                and str(existing.get("event_type") or "") == event_type
                and str(existing.get("scenario") or "") == scenario
                and str(existing.get("start_date") or "") == start_date
                and (None if existing.get("end_date") in {None, ""} else str(existing.get("end_date"))) == end_date
                and str(existing.get("frequency") or "") == frequency
                and float(existing.get("amount") or 0.0) == amount
                and float(existing.get("confidence") or 0.0) == confidence
                and bool(int(existing.get("is_deterministic") or 0)) == is_deterministic
            ),
            None,
        )
        if match is not None:
            continue
        created = service.append_cashflow_event(
            event_name=event_name,
            event_type=event_type,
            scenario=scenario,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            amount=amount,
            confidence=confidence,
            is_deterministic=is_deterministic,
            notes=notes,
            reason=reason,
        )
        appended_cashflow_events += 1
        existing_events.append(created)

    overview = service.overview_surface(as_of_date=BALANCE_SEED_DATE_V1)
    return {
        "created_accounts": created_accounts,
        "appended_classifications": appended_classifications,
        "appended_snapshots": appended_snapshots,
        "appended_cashflow_events": appended_cashflow_events,
        "as_of_date": BALANCE_SEED_DATE_V1,
        "investable_total": overview["investable_total"],
        "included_account_count": overview["included_account_count"],
        "excluded_account_count": overview["excluded_account_count"],
    }
