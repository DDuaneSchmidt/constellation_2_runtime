from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .constants_v1 import (
    CASHFLOW_EVENT_TYPES_V1,
    CASHFLOW_FREQUENCIES_V1,
    CASHFLOW_SCENARIOS_V1,
    BUCKET_TYPES_V1,
    CAPITAL_TYPES_V1,
    CONFIDENCE_BANDS_V1,
    CONFIDENCE_BAND_HIGH_MIN_V1,
    CONFIDENCE_BAND_MEDIUM_MIN_V1,
    CONFIDENCE_MAX_V1,
    CONFIDENCE_MIN_V1,
    CONTROL_TYPES_V1,
    FLOW_TRUTH_OWNER_DESCRIPTION_V1,
    FLOW_TRUTH_OWNER_V1,
    FRESHNESS_POLICY_DESCRIPTION_V1,
    FRESHNESS_POLICY_ID_V1,
    FLOW_TYPES_V1,
    INPUT_SOURCES_V1,
)
from .storage_v1 import (
    capital_connection_v1,
    ensure_capital_schema_v1,
    json_text_or_none_v1,
    resolve_capital_db_path_v1,
    row_dicts_v1,
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_text(name: str, value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"CAPITAL_{name}_REQUIRED")
    return text


def _require_iso_day(name: str, value: Any) -> str:
    text = _require_text(name, value)
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"CAPITAL_{name}_INVALID_DAY") from exc
    return text


def _require_bool(name: str, value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    raise ValueError(f"CAPITAL_{name}_MUST_BE_BOOL")


def _require_enum(name: str, value: Any, allowed: tuple[str, ...]) -> str:
    text = _require_text(name, value)
    if text not in allowed:
        raise ValueError(f"CAPITAL_{name}_INVALID")
    return text


def _require_float(name: str, value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError(f"CAPITAL_{name}_MUST_BE_NUMERIC")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"CAPITAL_{name}_MUST_BE_NUMERIC")
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"CAPITAL_{name}_MUST_BE_NUMERIC") from exc


def _require_reason(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("CAPITAL_REASON_REQUIRED")
    return text


def _confidence_band_from_level(level: float) -> str:
    if level >= CONFIDENCE_BAND_HIGH_MIN_V1:
        return "high"
    if level >= CONFIDENCE_BAND_MEDIUM_MIN_V1:
        return "medium"
    return "low"


def _date_from_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _month_floor(value: datetime) -> datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(value: datetime) -> datetime:
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1, day=1)
    return value.replace(month=value.month + 1, day=1)


def _month_key(value: datetime) -> str:
    return value.strftime("%Y-%m")


@dataclass(frozen=True)
class CapitalFactRowV1:
    account_id: str
    account_name: str
    is_active: bool
    account_notes: str
    as_of_date: str | None
    balance: float | None
    input_source: str | None
    capital_type: str | None
    control_type: str | None
    bucket_type: str | None
    include_in_allocation: bool | None
    confidence_level: float | None
    confidence_band: str | None
    classification_notes: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "is_active": self.is_active,
            "account_notes": self.account_notes,
            "as_of_date": self.as_of_date,
            "balance": self.balance,
            "input_source": self.input_source,
            "capital_type": self.capital_type,
            "control_type": self.control_type,
            "bucket_type": self.bucket_type,
            "include_in_allocation": self.include_in_allocation,
            "confidence_level": self.confidence_level,
            "confidence_band": self.confidence_band,
            "classification_notes": self.classification_notes,
        }


class CapitalDomainServiceV1:
    def __init__(
        self,
        *,
        db_path: Path | None = None,
        actor: str = "capital_service_v1",
        ensure_schema: bool = True,
    ) -> None:
        resolved = (db_path or resolve_capital_db_path_v1()).resolve()
        if ensure_schema:
            self.db_path = ensure_capital_schema_v1(resolved)
        else:
            if not resolved.exists() or not resolved.is_file():
                raise ValueError(f"CAPITAL_DB_MISSING:{resolved}")
            self.db_path = resolved
        self.actor = _require_text("ACTOR", actor)

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}:{uuid.uuid4().hex}"

    def _append_audit_entry(
        self,
        *,
        entity_type: str,
        entity_id: str,
        action: str,
        before_payload: dict[str, Any] | None,
        after_payload: dict[str, Any] | None,
        reason: str,
    ) -> None:
        now = _utc_now_iso()
        with capital_connection_v1(self.db_path) as connection:
            connection.execute(
                """
INSERT INTO capital_audit_log_v1 (
    audit_id,
    entity_type,
    entity_id,
    action,
    before_json,
    after_json,
    reason,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
                (
                    self._new_id("capital_audit"),
                    _require_text("ENTITY_TYPE", entity_type),
                    _require_text("ENTITY_ID", entity_id),
                    _require_text("ACTION", action),
                    json_text_or_none_v1(before_payload),
                    json_text_or_none_v1(after_payload),
                    str(reason or "").strip(),
                    now,
                ),
            )

    def create_account(
        self,
        *,
        account_id: str,
        account_name: str,
        notes: str = "",
        is_active: bool = True,
        reason: str = "",
    ) -> dict[str, Any]:
        normalized_id = _require_text("ACCOUNT_ID", account_id)
        normalized_name = _require_text("ACCOUNT_NAME", account_name)
        now = _utc_now_iso()
        with capital_connection_v1(self.db_path) as connection:
            existing = connection.execute(
                "SELECT * FROM capital_accounts_v1 WHERE account_id = ?",
                (normalized_id,),
            ).fetchone()
            if existing is not None:
                raise ValueError("CAPITAL_ACCOUNT_ALREADY_EXISTS")
            connection.execute(
                """
INSERT INTO capital_accounts_v1 (
    account_id,
    account_name,
    is_active,
    notes,
    created_at
) VALUES (?, ?, ?, ?, ?)
""".strip(),
                (
                    normalized_id,
                    normalized_name,
                    _require_bool("IS_ACTIVE", is_active),
                    str(notes or "").strip(),
                    now,
                ),
            )
        after = self.get_account(account_id=normalized_id)
        self._append_audit_entry(
            entity_type="account",
            entity_id=normalized_id,
            action="create_account",
            before_payload=None,
            after_payload=after,
            reason=reason,
        )
        return after

    def get_account(self, *, account_id: str) -> dict[str, Any]:
        normalized_id = _require_text("ACCOUNT_ID", account_id)
        with capital_connection_v1(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM capital_accounts_v1 WHERE account_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None:
                raise ValueError("CAPITAL_ACCOUNT_NOT_FOUND")
            return dict(row)

    def list_accounts(self) -> list[dict[str, Any]]:
        with capital_connection_v1(self.db_path) as connection:
            rows = connection.execute(
                "SELECT * FROM capital_accounts_v1 ORDER BY account_name ASC"
            ).fetchall()
            return row_dicts_v1(rows)

    def append_classification(
        self,
        *,
        account_id: str,
        effective_from: str,
        effective_to: str | None,
        capital_type: str,
        control_type: str,
        bucket_type: str,
        include_in_allocation: bool,
        confidence_level: float,
        confidence_band: str | None = None,
        notes: str = "",
        reason: str = "",
        changed_by: str | None = None,
    ) -> dict[str, Any]:
        normalized_account_id = _require_text("ACCOUNT_ID", account_id)
        reason_text = _require_reason(reason)
        start_day = _require_iso_day("EFFECTIVE_FROM", effective_from)
        end_day = None if effective_to is None else _require_iso_day("EFFECTIVE_TO", effective_to)
        if end_day is not None and end_day < start_day:
            raise ValueError("CAPITAL_EFFECTIVE_TO_BEFORE_EFFECTIVE_FROM")
        confidence = _require_float("CONFIDENCE_LEVEL", confidence_level)
        if confidence < CONFIDENCE_MIN_V1 or confidence > CONFIDENCE_MAX_V1:
            raise ValueError("CAPITAL_CONFIDENCE_OUT_OF_RANGE")
        normalized_confidence_band = (
            _confidence_band_from_level(confidence)
            if confidence_band is None
            else _require_enum("CONFIDENCE_BAND", confidence_band, CONFIDENCE_BANDS_V1)
        )
        normalized_capital_type = _require_enum("CAPITAL_TYPE", capital_type, CAPITAL_TYPES_V1)
        normalized_control_type = _require_enum("CONTROL_TYPE", control_type, CONTROL_TYPES_V1)
        normalized_bucket_type = _require_enum("BUCKET_TYPE", bucket_type, BUCKET_TYPES_V1)
        classification_id = self._new_id("capital_classification")
        now = _utc_now_iso()
        before_payload: dict[str, Any] | None = None
        with capital_connection_v1(self.db_path) as connection:
            account_row = connection.execute(
                "SELECT account_id FROM capital_accounts_v1 WHERE account_id = ?",
                (normalized_account_id,),
            ).fetchone()
            if account_row is None:
                raise ValueError("CAPITAL_ACCOUNT_NOT_FOUND")
            before_payload = self._classification_governance_before_payload(
                connection=connection,
                account_id=normalized_account_id,
                effective_day=start_day,
            )
            connection.execute(
                """
INSERT INTO capital_account_classifications_v1 (
    classification_id,
    account_id,
    effective_from,
    effective_to,
    capital_type,
    control_type,
    bucket_type,
    include_in_allocation,
    confidence_level,
    confidence_band,
    notes,
    changed_at,
    changed_by,
    change_reason
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
                (
                    classification_id,
                    normalized_account_id,
                    start_day,
                    end_day,
                    normalized_capital_type,
                    normalized_control_type,
                    normalized_bucket_type,
                    _require_bool("INCLUDE_IN_ALLOCATION", include_in_allocation),
                    confidence,
                    normalized_confidence_band,
                    str(notes or "").strip(),
                    now,
                    _require_text("CHANGED_BY", changed_by or self.actor),
                    reason_text,
                ),
            )
        row = self.get_classification(classification_id=classification_id)
        self._append_audit_entry(
            entity_type="classification",
            entity_id=classification_id,
            action="append_classification",
            before_payload=before_payload,
            after_payload=row,
            reason=reason_text,
        )
        return row

    def get_classification(self, *, classification_id: str) -> dict[str, Any]:
        normalized_id = _require_text("CLASSIFICATION_ID", classification_id)
        with capital_connection_v1(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM capital_account_classifications_v1 WHERE classification_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None:
                raise ValueError("CAPITAL_CLASSIFICATION_NOT_FOUND")
            return dict(row)

    def list_classifications(self, *, account_id: str | None = None) -> list[dict[str, Any]]:
        with capital_connection_v1(self.db_path) as connection:
            if account_id is None:
                rows = connection.execute(
                    """
SELECT *
FROM capital_account_classifications_v1
ORDER BY account_id ASC, effective_from ASC, changed_at ASC
""".strip()
                ).fetchall()
                return row_dicts_v1(rows)
            normalized_id = _require_text("ACCOUNT_ID", account_id)
            rows = connection.execute(
                """
SELECT *
FROM capital_account_classifications_v1
WHERE account_id = ?
ORDER BY effective_from ASC, changed_at ASC
""".strip(),
                (normalized_id,),
            ).fetchall()
            return row_dicts_v1(rows)

    def _classification_governance_before_payload(
        self,
        *,
        connection: Any,
        account_id: str,
        effective_day: str,
    ) -> dict[str, Any]:
        effective_row = connection.execute(
            """
SELECT *
FROM capital_account_classifications_v1
WHERE account_id = ?
  AND effective_from <= ?
  AND (effective_to IS NULL OR effective_to >= ?)
ORDER BY effective_from DESC, changed_at DESC, classification_id DESC
LIMIT 1
""".strip(),
            (account_id, effective_day, effective_day),
        ).fetchone()
        latest_row = connection.execute(
            """
SELECT *
FROM capital_account_classifications_v1
WHERE account_id = ?
ORDER BY effective_from DESC, changed_at DESC, classification_id DESC
LIMIT 1
""".strip(),
            (account_id,),
        ).fetchone()
        return {
            "account_id": account_id,
            "effective_day": effective_day,
            "prior_effective_classification": None if effective_row is None else dict(effective_row),
            "prior_latest_classification": None if latest_row is None else dict(latest_row),
        }

    def append_balance_snapshot(
        self,
        *,
        account_id: str,
        as_of_date: str,
        balance: float,
        input_source: str,
        notes: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        normalized_account_id = _require_text("ACCOUNT_ID", account_id)
        day = _require_iso_day("AS_OF_DATE", as_of_date)
        normalized_source = _require_enum("INPUT_SOURCE", input_source, INPUT_SOURCES_V1)
        balance_value = _require_float("BALANCE", balance)
        snapshot_id = self._new_id("capital_snapshot")
        now = _utc_now_iso()
        with capital_connection_v1(self.db_path) as connection:
            account_row = connection.execute(
                "SELECT account_id FROM capital_accounts_v1 WHERE account_id = ?",
                (normalized_account_id,),
            ).fetchone()
            if account_row is None:
                raise ValueError("CAPITAL_ACCOUNT_NOT_FOUND")
            duplicate = connection.execute(
                """
SELECT snapshot_id
FROM capital_balance_snapshots_v1
WHERE account_id = ? AND as_of_date = ? AND input_source = ?
""".strip(),
                (normalized_account_id, day, normalized_source),
            ).fetchone()
            if duplicate is not None:
                raise ValueError("CAPITAL_SNAPSHOT_DUPLICATE")
            connection.execute(
                """
INSERT INTO capital_balance_snapshots_v1 (
    snapshot_id,
    account_id,
    as_of_date,
    balance,
    input_source,
    notes,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
""".strip(),
                (
                    snapshot_id,
                    normalized_account_id,
                    day,
                    balance_value,
                    normalized_source,
                    str(notes or "").strip(),
                    now,
                ),
            )
        row = self.get_balance_snapshot(snapshot_id=snapshot_id)
        self._append_audit_entry(
            entity_type="balance_snapshot",
            entity_id=snapshot_id,
            action="append_balance_snapshot",
            before_payload=None,
            after_payload=row,
            reason=reason,
        )
        return row

    def get_balance_snapshot(self, *, snapshot_id: str) -> dict[str, Any]:
        normalized_id = _require_text("SNAPSHOT_ID", snapshot_id)
        with capital_connection_v1(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM capital_balance_snapshots_v1 WHERE snapshot_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None:
                raise ValueError("CAPITAL_SNAPSHOT_NOT_FOUND")
            return dict(row)

    def append_cash_flow(
        self,
        *,
        account_id: str,
        flow_date: str,
        flow_amount: float,
        flow_type: str,
        notes: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        normalized_account_id = _require_text("ACCOUNT_ID", account_id)
        day = _require_iso_day("FLOW_DATE", flow_date)
        amount = _require_float("FLOW_AMOUNT", flow_amount)
        normalized_type = _require_enum("FLOW_TYPE", flow_type, FLOW_TYPES_V1)
        flow_id = self._new_id("capital_flow")
        now = _utc_now_iso()
        with capital_connection_v1(self.db_path) as connection:
            account_row = connection.execute(
                "SELECT account_id FROM capital_accounts_v1 WHERE account_id = ?",
                (normalized_account_id,),
            ).fetchone()
            if account_row is None:
                raise ValueError("CAPITAL_ACCOUNT_NOT_FOUND")
            connection.execute(
                """
INSERT INTO capital_cash_flows_v1 (
    flow_id,
    account_id,
    flow_date,
    flow_amount,
    flow_type,
    notes,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
""".strip(),
                (
                    flow_id,
                    normalized_account_id,
                    day,
                    amount,
                    normalized_type,
                    str(notes or "").strip(),
                    now,
                ),
            )
        row = self.get_cash_flow(flow_id=flow_id)
        self._append_audit_entry(
            entity_type="cash_flow",
            entity_id=flow_id,
            action="append_cash_flow",
            before_payload=None,
            after_payload=row,
            reason=reason,
        )
        return row

    def get_cash_flow(self, *, flow_id: str) -> dict[str, Any]:
        normalized_id = _require_text("FLOW_ID", flow_id)
        with capital_connection_v1(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM capital_cash_flows_v1 WHERE flow_id = ?",
                (normalized_id,),
            ).fetchone()
            if row is None:
                raise ValueError("CAPITAL_FLOW_NOT_FOUND")
            return dict(row)

    def list_cash_flows(self) -> list[dict[str, Any]]:
        with capital_connection_v1(self.db_path) as connection:
            rows = connection.execute(
                """
SELECT *
FROM capital_cash_flows_v1
ORDER BY flow_date DESC, created_at DESC, flow_id DESC
""".strip()
            ).fetchall()
            return row_dicts_v1(rows)

    def append_cashflow_event(
        self,
        *,
        event_name: str,
        event_type: str,
        scenario: str,
        start_date: str,
        end_date: str | None,
        frequency: str,
        amount: float,
        confidence: float,
        is_deterministic: bool,
        notes: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        normalized_name = _require_text("EVENT_NAME", event_name)
        normalized_type = _require_enum("EVENT_TYPE", event_type, CASHFLOW_EVENT_TYPES_V1)
        normalized_scenario = _require_enum("SCENARIO", scenario, CASHFLOW_SCENARIOS_V1)
        start_day = _require_iso_day("START_DATE", start_date)
        end_day = None if end_date is None else _require_iso_day("END_DATE", end_date)
        if end_day is not None and end_day < start_day:
            raise ValueError("CAPITAL_CASHFLOW_END_BEFORE_START")
        normalized_frequency = _require_enum("FREQUENCY", frequency, CASHFLOW_FREQUENCIES_V1)
        amount_value = _require_float("AMOUNT", amount)
        if amount_value < 0:
            raise ValueError("CAPITAL_CASHFLOW_AMOUNT_NEGATIVE")
        confidence_value = _require_float("CONFIDENCE", confidence)
        if confidence_value < CONFIDENCE_MIN_V1 or confidence_value > CONFIDENCE_MAX_V1:
            raise ValueError("CAPITAL_CASHFLOW_CONFIDENCE_OUT_OF_RANGE")
        deterministic_flag = _require_bool("IS_DETERMINISTIC", is_deterministic)
        now = _utc_now_iso()
        with capital_connection_v1(self.db_path) as connection:
            connection.execute(
                """
INSERT INTO capital_cashflow_events_v1 (
    event_name,
    event_type,
    scenario,
    start_date,
    end_date,
    frequency,
    amount,
    confidence,
    is_deterministic,
    notes,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
                (
                    normalized_name,
                    normalized_type,
                    normalized_scenario,
                    start_day,
                    end_day,
                    normalized_frequency,
                    amount_value,
                    confidence_value,
                    deterministic_flag,
                    str(notes or "").strip(),
                    now,
                ),
            )
            event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        row = self.get_cashflow_event(event_id=event_id)
        self._append_audit_entry(
            entity_type="cashflow_event",
            entity_id=str(event_id),
            action="append_cashflow_event",
            before_payload=None,
            after_payload=row,
            reason=reason,
        )
        return row

    def get_cashflow_event(self, *, event_id: int) -> dict[str, Any]:
        with capital_connection_v1(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM capital_cashflow_events_v1 WHERE event_id = ?",
                (int(event_id),),
            ).fetchone()
            if row is None:
                raise ValueError("CAPITAL_CASHFLOW_EVENT_NOT_FOUND")
            return dict(row)

    def list_cashflow_events(self, *, scenario: str | None = None) -> list[dict[str, Any]]:
        with capital_connection_v1(self.db_path) as connection:
            if scenario is None:
                rows = connection.execute(
                    """
SELECT *
FROM capital_cashflow_events_v1
ORDER BY start_date ASC, event_name ASC, event_id ASC
""".strip()
                ).fetchall()
                return row_dicts_v1(rows)
            normalized_scenario = _require_enum("SCENARIO", scenario, CASHFLOW_SCENARIOS_V1)
            rows = connection.execute(
                """
SELECT *
FROM capital_cashflow_events_v1
WHERE scenario = ?
ORDER BY start_date ASC, event_name ASC, event_id ASC
""".strip(),
                (normalized_scenario,),
            ).fetchall()
            return row_dicts_v1(rows)

    def list_audit_entries(self, *, limit: int = 200) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 5000))
        with capital_connection_v1(self.db_path) as connection:
            rows = connection.execute(
                """
SELECT *
FROM capital_audit_log_v1
ORDER BY created_at DESC, audit_id DESC
LIMIT ?
""".strip(),
                (safe_limit,),
            ).fetchall()
            return row_dicts_v1(rows)

    def latest_snapshot_day(self) -> str | None:
        with capital_connection_v1(self.db_path) as connection:
            row = connection.execute(
                "SELECT MAX(as_of_date) AS latest_day FROM capital_balance_snapshots_v1"
            ).fetchone()
            value = None if row is None else row["latest_day"]
            if not isinstance(value, str) or not value:
                return None
            return value

    def _latest_balance_rows(self, *, as_of_date: str | None) -> list[dict[str, Any]]:
        with capital_connection_v1(self.db_path) as connection:
            rows = connection.execute(
                """
WITH ranked AS (
    SELECT
        snapshot_id,
        account_id,
        as_of_date,
        balance,
        input_source,
        notes,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY as_of_date DESC, created_at DESC, snapshot_id DESC
        ) AS rn
    FROM capital_balance_snapshots_v1
    WHERE (? IS NULL OR as_of_date <= ?)
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY account_id ASC
""".strip(),
                (as_of_date, as_of_date),
            ).fetchall()
            return row_dicts_v1(rows)

    def _current_classification_rows(self, *, as_of_date: str) -> list[dict[str, Any]]:
        with capital_connection_v1(self.db_path) as connection:
            rows = connection.execute(
                """
WITH ranked AS (
    SELECT
        classification_id,
        account_id,
        effective_from,
        effective_to,
        capital_type,
        control_type,
        bucket_type,
        include_in_allocation,
        confidence_level,
        confidence_band,
        notes,
        changed_at,
        changed_by,
        change_reason,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY effective_from DESC, changed_at DESC, classification_id DESC
        ) AS rn
    FROM capital_account_classifications_v1
    WHERE effective_from <= ?
      AND (effective_to IS NULL OR effective_to >= ?)
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY account_id ASC
""".strip(),
                (as_of_date, as_of_date),
            ).fetchall()
            return row_dicts_v1(rows)

    def _account_facts(self, *, as_of_date: str | None = None) -> tuple[str | None, list[CapitalFactRowV1]]:
        resolved_day = as_of_date or self.latest_snapshot_day()
        if resolved_day is None:
            return None, []
        day = _require_iso_day("AS_OF_DATE", resolved_day)
        balances = {row["account_id"]: row for row in self._latest_balance_rows(as_of_date=day)}
        classifications = {row["account_id"]: row for row in self._current_classification_rows(as_of_date=day)}
        accounts = self.list_accounts()
        facts: list[CapitalFactRowV1] = []
        for account in accounts:
            account_id = str(account["account_id"])
            balance_row = balances.get(account_id)
            classification_row = classifications.get(account_id)
            include_flag = None
            if classification_row is not None:
                include_flag = bool(int(classification_row["include_in_allocation"]))
            facts.append(
                CapitalFactRowV1(
                    account_id=account_id,
                    account_name=str(account["account_name"]),
                    is_active=bool(int(account["is_active"])),
                    account_notes=str(account.get("notes") or ""),
                    as_of_date=None if balance_row is None else str(balance_row["as_of_date"]),
                    balance=None if balance_row is None else float(balance_row["balance"]),
                    input_source=None if balance_row is None else str(balance_row["input_source"]),
                    capital_type=None if classification_row is None else str(classification_row["capital_type"]),
                    control_type=None if classification_row is None else str(classification_row["control_type"]),
                    bucket_type=None if classification_row is None else str(classification_row["bucket_type"]),
                    include_in_allocation=include_flag,
                    confidence_level=None if classification_row is None else float(classification_row["confidence_level"]),
                    confidence_band=None if classification_row is None else str(classification_row["confidence_band"]),
                    classification_notes=None if classification_row is None else str(classification_row.get("notes") or ""),
                )
            )
        return day, facts

    def latest_balance_per_account(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, facts = self._account_facts(as_of_date=as_of_date)
        return {
            "as_of_date": resolved_day,
            "basis": "latest_per_account",
            "rows": [
                {
                    "account_id": row.account_id,
                    "account_name": row.account_name,
                    "as_of_date": row.as_of_date,
                    "balance": row.balance,
                    "input_source": row.input_source,
                }
                for row in facts
            ],
        }

    def current_classification_per_account(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, facts = self._account_facts(as_of_date=as_of_date)
        return {
            "as_of_date": resolved_day,
            "rows": [
                {
                    "account_id": row.account_id,
                    "account_name": row.account_name,
                    "capital_type": row.capital_type,
                    "control_type": row.control_type,
                    "bucket_type": row.bucket_type,
                    "include_in_allocation": row.include_in_allocation,
                    "confidence_level": row.confidence_level,
                    "confidence_band": row.confidence_band,
                    "notes": row.classification_notes,
                }
                for row in facts
            ],
        }

    def _included_rows(self, *, as_of_date: str | None = None) -> tuple[str | None, list[CapitalFactRowV1], list[CapitalFactRowV1]]:
        resolved_day, facts = self._account_facts(as_of_date=as_of_date)
        included: list[CapitalFactRowV1] = []
        excluded: list[CapitalFactRowV1] = []
        for row in facts:
            if row.balance is None or row.include_in_allocation is None:
                excluded.append(row)
                continue
            if row.include_in_allocation:
                included.append(row)
            else:
                excluded.append(row)
        return resolved_day, included, excluded

    def _flow_truth_descriptor(self) -> dict[str, Any]:
        return {
            "truth_owner": FLOW_TRUTH_OWNER_V1,
            "description": FLOW_TRUTH_OWNER_DESCRIPTION_V1,
            "authoritative_inputs": ["capital_cash_flows_v1.flow_amount", "capital_cash_flows_v1.flow_date"],
            "excluded_inputs": ["capital_balance_snapshots_v1.net_flow"],
        }

    def _freshness_completeness_summary(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day = as_of_date or self.latest_snapshot_day()
        if resolved_day is None:
            return {
                "policy_id": FRESHNESS_POLICY_ID_V1,
                "policy_description": FRESHNESS_POLICY_DESCRIPTION_V1,
                "as_of_date": None,
                "global_latest_snapshot_day": None,
                "included_account_count": 0,
                "missing_account_count": 0,
                "stale_account_count": 0,
                "missing_accounts": [],
                "stale_accounts": [],
                "status": "INVALID",
                "reason_codes": ["CAPITAL_REPORT_BASIS_NO_SNAPSHOTS"],
            }
        day = _require_iso_day("AS_OF_DATE", resolved_day)
        with capital_connection_v1(self.db_path) as connection:
            latest_row = connection.execute(
                """
SELECT MAX(as_of_date) AS latest_global_snapshot_day
FROM capital_balance_snapshots_v1
WHERE as_of_date <= ?
""".strip(),
                (day,),
            ).fetchone()
            global_latest = None if latest_row is None else latest_row["latest_global_snapshot_day"]
            included_rows = connection.execute(
                """
WITH current_cls AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.account_id
            ORDER BY c.effective_from DESC, c.changed_at DESC, c.classification_id DESC
        ) AS rn
    FROM capital_account_classifications_v1 AS c
    JOIN capital_accounts_v1 AS a ON a.account_id = c.account_id
    WHERE a.is_active = 1
      AND c.effective_from <= ?
      AND (c.effective_to IS NULL OR c.effective_to >= ?)
)
SELECT account_id
FROM current_cls
WHERE rn = 1
  AND include_in_allocation = 1
ORDER BY account_id ASC
""".strip(),
                (day, day),
            ).fetchall()
            included_account_ids = [str(row["account_id"]) for row in included_rows]
            missing_accounts: list[dict[str, Any]] = []
            stale_accounts: list[dict[str, Any]] = []
            for account_id in included_account_ids:
                row = connection.execute(
                    """
SELECT MAX(as_of_date) AS latest_account_day
FROM capital_balance_snapshots_v1
WHERE account_id = ?
  AND as_of_date <= ?
""".strip(),
                    (account_id, day),
                ).fetchone()
                account_day = None if row is None else row["latest_account_day"]
                if not isinstance(account_day, str) or not account_day:
                    missing_accounts.append(
                        {
                            "account_id": account_id,
                            "latest_snapshot_day": None,
                        }
                    )
                    continue
                if isinstance(global_latest, str) and global_latest and account_day < global_latest:
                    stale_accounts.append(
                        {
                            "account_id": account_id,
                            "latest_snapshot_day": account_day,
                            "latest_global_snapshot_day": global_latest,
                        }
                    )
        status = "HEALTHY"
        reason_codes: list[str] = []
        if missing_accounts:
            status = "INVALID"
            reason_codes.append("CAPITAL_INCLUDED_ACCOUNT_SNAPSHOT_MISSING")
        elif stale_accounts:
            status = "DEGRADED"
            reason_codes.append("CAPITAL_INCLUDED_ACCOUNT_SNAPSHOT_STALE")
        return {
            "policy_id": FRESHNESS_POLICY_ID_V1,
            "policy_description": FRESHNESS_POLICY_DESCRIPTION_V1,
            "as_of_date": day,
            "global_latest_snapshot_day": global_latest,
            "included_account_count": len(included_account_ids),
            "missing_account_count": len(missing_accounts),
            "stale_account_count": len(stale_accounts),
            "missing_accounts": missing_accounts,
            "stale_accounts": stale_accounts,
            "status": status,
            "reason_codes": reason_codes,
        }

    def _report_basis_payload(
        self,
        *,
        report_basis: str,
        basis_description: str,
        as_of_date: str | None,
        included_count: int,
        excluded_count: int,
        freshness: dict[str, Any] | None = None,
        validation: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        freshness_payload = freshness or self._freshness_completeness_summary(as_of_date=as_of_date)
        validation_payload = validation or self.validation_errors(as_of_date=as_of_date)
        as_of_utc = None if not as_of_date else f"{as_of_date}T00:00:00Z"
        return {
            "report_basis": report_basis,
            "basis_description": basis_description,
            "as_of_date": as_of_date,
            "as_of_utc": as_of_utc,
            "included_account_count": included_count,
            "excluded_account_count": excluded_count,
            "stale_account_count": int(freshness_payload.get("stale_account_count") or 0),
            "stale_account_ids": [
                str(item["account_id"]) for item in freshness_payload.get("stale_accounts", []) if "account_id" in item
            ],
            "freshness_status": freshness_payload.get("status", "UNKNOWN"),
            "validation_status": validation_payload.get("status", "UNKNOWN"),
            "validation_error_count": int(validation_payload.get("error_count") or 0),
        }

    def _contributors_payload(self, rows: list[CapitalFactRowV1]) -> list[dict[str, Any]]:
        return [
            {
                "account_id": row.account_id,
                "account_name": row.account_name,
                "balance": round(float(row.balance or 0.0), 2),
                "capital_type": row.capital_type,
                "control_type": row.control_type,
                "bucket_type": row.bucket_type,
                "include_in_allocation": bool(row.include_in_allocation),
            }
            for row in sorted(rows, key=lambda item: item.account_id)
        ]

    def _exclusion_reason_code(self, row: CapitalFactRowV1) -> str:
        if row.balance is None:
            return "CAPITAL_EXCLUDED_MISSING_BALANCE"
        if row.include_in_allocation is None:
            return "CAPITAL_EXCLUDED_MISSING_CLASSIFICATION"
        return "CAPITAL_EXCLUDED_BY_CLASSIFICATION"

    def current_investable_capital(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, included, excluded = self._included_rows(as_of_date=as_of_date)
        investable_total = round(sum(float(row.balance or 0.0) for row in included), 2)
        freshness = self._freshness_completeness_summary(as_of_date=resolved_day)
        validations = self.validation_errors(as_of_date=resolved_day)
        contributors = self._contributors_payload(included)
        return {
            "as_of_date": resolved_day,
            "basis": "latest_per_account",
            "included_account_count": len(included),
            "excluded_account_count": len(excluded),
            "investable_total": investable_total,
            "contributing_accounts": [row.account_id for row in included],
            "contributing_account_details": contributors,
            "denominator": "sum_of_included_account_balances",
            "denominator_value": investable_total,
            "report_basis_metadata": self._report_basis_payload(
                report_basis="latest_per_account",
                basis_description=(
                    "For each account, use latest snapshot on or before report day with current effective "
                    "classification for include/exclude semantics."
                ),
                as_of_date=resolved_day,
                included_count=len(included),
                excluded_count=len(excluded),
                freshness=freshness,
                validation=validations,
            ),
            "freshness_completeness": freshness,
            "validation_status_summary": {
                "status": validations["status"],
                "error_count": validations["error_count"],
                "severity_counts": validations["severity_counts"],
            },
        }

    def allocation_by_control(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, included, excluded = self._included_rows(as_of_date=as_of_date)
        total = sum(float(row.balance or 0.0) for row in included)
        by_control: dict[str, dict[str, Any]] = {}
        for row in included:
            key = str(row.control_type)
            bucket = by_control.setdefault(
                key,
                {"control_type": key, "balance_total": 0.0, "account_ids": [], "contributors": []},
            )
            bucket["balance_total"] += float(row.balance or 0.0)
            bucket["account_ids"].append(row.account_id)
            bucket["contributors"].append(
                {
                    "account_id": row.account_id,
                    "account_name": row.account_name,
                    "balance": round(float(row.balance or 0.0), 2),
                }
            )
        rows: list[dict[str, Any]] = []
        for key in sorted(by_control.keys()):
            balance_total = round(float(by_control[key]["balance_total"]), 2)
            pct = None if total <= 0 else round(balance_total / total, 8)
            rows.append(
                {
                    "control_type": key,
                    "balance_total": balance_total,
                    "allocation_pct": pct,
                    "account_ids": sorted(by_control[key]["account_ids"]),
                    "contributors": sorted(by_control[key]["contributors"], key=lambda item: str(item["account_id"])),
                    "denominator_value": round(total, 2),
                }
            )
        freshness = self._freshness_completeness_summary(as_of_date=resolved_day)
        validations = self.validation_errors(as_of_date=resolved_day)
        return {
            "as_of_date": resolved_day,
            "basis": "latest_per_account",
            "numerator": "included_balance_by_control",
            "denominator": "total_included_balance",
            "total_included_balance": round(total, 2),
            "included_account_count": len(included),
            "excluded_account_count": len(excluded),
            "rows": rows,
            "report_basis_metadata": self._report_basis_payload(
                report_basis="latest_per_account",
                basis_description=(
                    "Group included latest-per-account balances by current effective control_type."
                ),
                as_of_date=resolved_day,
                included_count=len(included),
                excluded_count=len(excluded),
                freshness=freshness,
                validation=validations,
            ),
            "freshness_completeness": freshness,
            "validation_status_summary": {
                "status": validations["status"],
                "error_count": validations["error_count"],
                "severity_counts": validations["severity_counts"],
            },
        }

    def allocation_by_bucket(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, included, excluded = self._included_rows(as_of_date=as_of_date)
        total = sum(float(row.balance or 0.0) for row in included)
        by_bucket: dict[str, dict[str, Any]] = {}
        for row in included:
            key = str(row.bucket_type)
            bucket = by_bucket.setdefault(
                key,
                {"bucket_type": key, "balance_total": 0.0, "account_ids": [], "contributors": []},
            )
            bucket["balance_total"] += float(row.balance or 0.0)
            bucket["account_ids"].append(row.account_id)
            bucket["contributors"].append(
                {
                    "account_id": row.account_id,
                    "account_name": row.account_name,
                    "balance": round(float(row.balance or 0.0), 2),
                }
            )
        rows: list[dict[str, Any]] = []
        for key in sorted(by_bucket.keys()):
            balance_total = round(float(by_bucket[key]["balance_total"]), 2)
            pct = None if total <= 0 else round(balance_total / total, 8)
            rows.append(
                {
                    "bucket_type": key,
                    "balance_total": balance_total,
                    "allocation_pct": pct,
                    "account_ids": sorted(by_bucket[key]["account_ids"]),
                    "contributors": sorted(by_bucket[key]["contributors"], key=lambda item: str(item["account_id"])),
                    "denominator_value": round(total, 2),
                }
            )
        freshness = self._freshness_completeness_summary(as_of_date=resolved_day)
        validations = self.validation_errors(as_of_date=resolved_day)
        return {
            "as_of_date": resolved_day,
            "basis": "latest_per_account",
            "numerator": "included_balance_by_bucket",
            "denominator": "total_included_balance",
            "total_included_balance": round(total, 2),
            "included_account_count": len(included),
            "excluded_account_count": len(excluded),
            "rows": rows,
            "report_basis_metadata": self._report_basis_payload(
                report_basis="latest_per_account",
                basis_description=(
                    "Group included latest-per-account balances by current effective bucket_type."
                ),
                as_of_date=resolved_day,
                included_count=len(included),
                excluded_count=len(excluded),
                freshness=freshness,
                validation=validations,
            ),
            "freshness_completeness": freshness,
            "validation_status_summary": {
                "status": validations["status"],
                "error_count": validations["error_count"],
                "severity_counts": validations["severity_counts"],
            },
        }

    def bucket_control_matrix(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, included, excluded = self._included_rows(as_of_date=as_of_date)
        total = sum(float(row.balance or 0.0) for row in included)
        matrix: dict[tuple[str, str], dict[str, Any]] = {}
        for row in included:
            key = (str(row.bucket_type), str(row.control_type))
            cell = matrix.setdefault(
                key,
                {
                    "bucket_type": key[0],
                    "control_type": key[1],
                    "balance_total": 0.0,
                    "account_ids": [],
                    "contributors": [],
                },
            )
            cell["balance_total"] += float(row.balance or 0.0)
            cell["account_ids"].append(row.account_id)
            cell["contributors"].append(
                {
                    "account_id": row.account_id,
                    "account_name": row.account_name,
                    "balance": round(float(row.balance or 0.0), 2),
                }
            )
        rows: list[dict[str, Any]] = []
        for key in sorted(matrix.keys()):
            cell = matrix[key]
            balance_total = round(float(cell["balance_total"]), 2)
            rows.append(
                {
                    "bucket_type": cell["bucket_type"],
                    "control_type": cell["control_type"],
                    "balance_total": balance_total,
                    "allocation_pct": None if total <= 0 else round(balance_total / total, 8),
                    "account_ids": sorted(cell["account_ids"]),
                    "contributors": sorted(cell["contributors"], key=lambda item: str(item["account_id"])),
                    "denominator_value": round(total, 2),
                }
            )
        freshness = self._freshness_completeness_summary(as_of_date=resolved_day)
        validations = self.validation_errors(as_of_date=resolved_day)
        return {
            "as_of_date": resolved_day,
            "basis": "latest_per_account",
            "total_included_balance": round(total, 2),
            "rows": rows,
            "included_account_count": len(included),
            "excluded_account_count": len(excluded),
            "report_basis_metadata": self._report_basis_payload(
                report_basis="latest_per_account",
                basis_description=(
                    "Group included latest-per-account balances by bucket_type x control_type."
                ),
                as_of_date=resolved_day,
                included_count=len(included),
                excluded_count=len(excluded),
                freshness=freshness,
                validation=validations,
            ),
            "freshness_completeness": freshness,
            "validation_status_summary": {
                "status": validations["status"],
                "error_count": validations["error_count"],
                "severity_counts": validations["severity_counts"],
            },
        }

    def included_excluded_summary(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day, included, excluded = self._included_rows(as_of_date=as_of_date)
        included_total = round(sum(float(row.balance or 0.0) for row in included), 2)
        excluded_total = round(sum(float(row.balance or 0.0) for row in excluded if row.balance is not None), 2)
        freshness = self._freshness_completeness_summary(as_of_date=resolved_day)
        validations = self.validation_errors(as_of_date=resolved_day)
        return {
            "as_of_date": resolved_day,
            "basis": "latest_per_account",
            "included_total": included_total,
            "excluded_total": excluded_total,
            "included_account_count": len(included),
            "excluded_account_count": len(excluded),
            "included_accounts": [row.account_id for row in included],
            "excluded_accounts": [row.account_id for row in excluded],
            "included_account_details": self._contributors_payload(included),
            "excluded_account_details": [
                {
                    "account_id": row.account_id,
                    "account_name": row.account_name,
                    "balance": None if row.balance is None else round(float(row.balance), 2),
                    "exclude_reason_code": self._exclusion_reason_code(row),
                }
                for row in sorted(excluded, key=lambda item: item.account_id)
            ],
            "report_basis_metadata": self._report_basis_payload(
                report_basis="latest_per_account",
                basis_description="Partition latest-per-account rows into included versus excluded populations.",
                as_of_date=resolved_day,
                included_count=len(included),
                excluded_count=len(excluded),
                freshness=freshness,
                validation=validations,
            ),
            "freshness_completeness": freshness,
            "validation_status_summary": {
                "status": validations["status"],
                "error_count": validations["error_count"],
                "severity_counts": validations["severity_counts"],
            },
        }

    def validation_errors(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        resolved_day = as_of_date or self.latest_snapshot_day() or datetime.now(UTC).strftime("%Y-%m-%d")
        day = _require_iso_day("AS_OF_DATE", resolved_day)
        findings: list[dict[str, Any]] = []

        def add_finding(
            *,
            code: str,
            severity: str,
            message: str,
            account_ids: list[str] | None = None,
            details: dict[str, Any] | None = None,
        ) -> None:
            findings.append(
                {
                    "code": code,
                    "severity": severity,
                    "message": message,
                    "account_ids": sorted(set(account_ids or [])),
                    "details": details or {},
                }
            )

        with capital_connection_v1(self.db_path) as connection:
            missing_rows = connection.execute(
                """
SELECT a.account_id
FROM capital_accounts_v1 AS a
LEFT JOIN capital_account_classifications_v1 AS c
  ON c.account_id = a.account_id
 AND c.effective_from <= ?
 AND (c.effective_to IS NULL OR c.effective_to >= ?)
WHERE a.is_active = 1
  AND c.classification_id IS NULL
ORDER BY a.account_id ASC
""".strip(),
                (day, day),
            ).fetchall()
            if missing_rows:
                add_finding(
                    code="CAPITAL_MISSING_CLASSIFICATION",
                    severity="CRITICAL",
                    message="Active accounts are missing a current effective classification.",
                    account_ids=[str(row["account_id"]) for row in missing_rows],
                )

            overlap_rows = connection.execute(
                """
SELECT c1.account_id
FROM capital_account_classifications_v1 AS c1
JOIN capital_account_classifications_v1 AS c2
  ON c1.account_id = c2.account_id
 AND c1.classification_id < c2.classification_id
 AND c1.effective_from <= COALESCE(c2.effective_to, '9999-12-31')
 AND c2.effective_from <= COALESCE(c1.effective_to, '9999-12-31')
GROUP BY c1.account_id
ORDER BY c1.account_id ASC
""".strip()
            ).fetchall()
            if overlap_rows:
                add_finding(
                    code="CAPITAL_CLASSIFICATION_OVERLAP",
                    severity="CRITICAL",
                    message="One or more accounts have overlapping effective-dated classifications.",
                    account_ids=[str(row["account_id"]) for row in overlap_rows],
                )

            duplicate_rows = connection.execute(
                """
SELECT account_id, as_of_date, COUNT(*) AS row_count
FROM capital_balance_snapshots_v1
GROUP BY account_id, as_of_date
HAVING COUNT(*) > 1
ORDER BY account_id ASC, as_of_date ASC
""".strip()
            ).fetchall()
            if duplicate_rows:
                add_finding(
                    code="CAPITAL_DUPLICATE_SNAPSHOT_ACCOUNT_DAY",
                    severity="WARNING",
                    message="Multiple snapshots exist for the same account/day (different input sources).",
                    account_ids=[str(row["account_id"]) for row in duplicate_rows],
                    details={
                        "rows": [
                            {
                                "account_id": str(row["account_id"]),
                                "as_of_date": str(row["as_of_date"]),
                                "row_count": int(row["row_count"]),
                            }
                            for row in duplicate_rows
                        ]
                    },
                )

            invalid_enum_rows = connection.execute(
                """
SELECT account_id
FROM capital_account_classifications_v1
WHERE capital_type NOT IN ('owned','future','speculative','real_estate','simulated')
   OR control_type NOT IN ('advisor','aegis','passive','self','other')
   OR bucket_type NOT IN ('income','growth','safety','future_income','speculative','residence')
   OR confidence_band NOT IN ('high','medium','low')
GROUP BY account_id
ORDER BY account_id ASC
""".strip()
            ).fetchall()
            if invalid_enum_rows:
                add_finding(
                    code="CAPITAL_INVALID_CLASSIFICATION_ENUM",
                    severity="CRITICAL",
                    message="Classification rows contain invalid enum values.",
                    account_ids=[str(row["account_id"]) for row in invalid_enum_rows],
                )

            empty_reason_rows = connection.execute(
                """
SELECT account_id
FROM capital_account_classifications_v1
WHERE TRIM(COALESCE(change_reason, '')) = ''
GROUP BY account_id
ORDER BY account_id ASC
""".strip()
            ).fetchall()
            if empty_reason_rows:
                add_finding(
                    code="CAPITAL_CLASSIFICATION_REASON_MISSING",
                    severity="CRITICAL",
                    message="Classification rows exist without a non-empty governance reason.",
                    account_ids=[str(row["account_id"]) for row in empty_reason_rows],
                )

            snapshot_table_columns = connection.execute(
                "PRAGMA table_info(capital_balance_snapshots_v1)"
            ).fetchall()
            if any(str(row["name"]) == "net_flow" for row in snapshot_table_columns):
                add_finding(
                    code="CAPITAL_SNAPSHOT_NET_FLOW_DEPRECATED_PRESENT",
                    severity="WARNING",
                    message=(
                        "Snapshot-level net_flow compatibility column exists; canonical external flow truth remains cash_flows."
                    ),
                    details=self._flow_truth_descriptor(),
                )

            included_semantic_rows = connection.execute(
                """
WITH current_cls AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.account_id
            ORDER BY c.effective_from DESC, c.changed_at DESC, c.classification_id DESC
        ) AS rn
    FROM capital_account_classifications_v1 AS c
    WHERE c.effective_from <= ?
      AND (c.effective_to IS NULL OR c.effective_to >= ?)
)
SELECT account_id, capital_type, bucket_type
FROM current_cls
WHERE rn = 1
  AND include_in_allocation = 1
ORDER BY account_id ASC
""".strip(),
                (day, day),
            ).fetchall()
            disallowed_rows = [
                str(row["account_id"])
                for row in included_semantic_rows
                if str(row["capital_type"]) not in {"owned"}
            ]
            future_rows = [str(row["account_id"]) for row in included_semantic_rows if str(row["capital_type"]) == "future"]
            simulated_rows = [str(row["account_id"]) for row in included_semantic_rows if str(row["capital_type"]) == "simulated"]
            residence_rows = [str(row["account_id"]) for row in included_semantic_rows if str(row["bucket_type"]) == "residence"]
            if future_rows:
                add_finding(
                    code="CAPITAL_INCLUDED_FUTURE_DISALLOWED",
                    severity="CRITICAL",
                    message="Future capital cannot be included in investable allocation.",
                    account_ids=future_rows,
                )
            if simulated_rows:
                add_finding(
                    code="CAPITAL_INCLUDED_SIMULATED_DISALLOWED",
                    severity="CRITICAL",
                    message="Simulated capital cannot be included in investable allocation.",
                    account_ids=simulated_rows,
                )
            if residence_rows:
                add_finding(
                    code="CAPITAL_INCLUDED_RESIDENCE_BUCKET_DISALLOWED",
                    severity="CRITICAL",
                    message="Residence bucket cannot be included in investable allocation.",
                    account_ids=residence_rows,
                )
            if disallowed_rows:
                add_finding(
                    code="CAPITAL_INCLUDED_ACCOUNT_DISALLOWED_TYPE",
                    severity="CRITICAL",
                    message="Included accounts use a disallowed capital_type for investable allocation.",
                    account_ids=disallowed_rows,
                )

            suspicious_cash_growth_rows = connection.execute(
                """
WITH current_cls AS (
    SELECT
        c.*,
        a.account_name,
        ROW_NUMBER() OVER (
            PARTITION BY c.account_id
            ORDER BY c.effective_from DESC, c.changed_at DESC, c.classification_id DESC
        ) AS rn
    FROM capital_account_classifications_v1 AS c
    JOIN capital_accounts_v1 AS a ON a.account_id = c.account_id
    WHERE c.effective_from <= ?
      AND (c.effective_to IS NULL OR c.effective_to >= ?)
)
SELECT account_id
FROM current_cls
WHERE rn = 1
  AND bucket_type = 'growth'
  AND (
    LOWER(account_name) LIKE '%cash%'
    OR LOWER(account_name) LIKE '%savings%'
  )
ORDER BY account_id ASC
""".strip(),
                (day, day),
            ).fetchall()
            if suspicious_cash_growth_rows:
                add_finding(
                    code="CAPITAL_SUSPICIOUS_CASH_LIKE_GROWTH_BUCKET",
                    severity="WARNING",
                    message="Cash-like account names are classified as growth; verify semantic intent.",
                    account_ids=[str(row["account_id"]) for row in suspicious_cash_growth_rows],
                    details={"heuristic_basis": "account_name contains cash/savings and bucket_type=growth"},
                )

            suspicious_annuity_bucket_rows = connection.execute(
                """
WITH current_cls AS (
    SELECT
        c.*,
        a.account_name,
        ROW_NUMBER() OVER (
            PARTITION BY c.account_id
            ORDER BY c.effective_from DESC, c.changed_at DESC, c.classification_id DESC
        ) AS rn
    FROM capital_account_classifications_v1 AS c
    JOIN capital_accounts_v1 AS a ON a.account_id = c.account_id
    WHERE c.effective_from <= ?
      AND (c.effective_to IS NULL OR c.effective_to >= ?)
)
SELECT account_id
FROM current_cls
WHERE rn = 1
  AND LOWER(account_name) LIKE '%annuity%'
  AND bucket_type NOT IN ('income', 'future_income')
ORDER BY account_id ASC
""".strip(),
                (day, day),
            ).fetchall()
            if suspicious_annuity_bucket_rows:
                add_finding(
                    code="CAPITAL_SUSPICIOUS_ANNUITY_BUCKET",
                    severity="WARNING",
                    message="Annuity account is classified outside expected income-oriented buckets.",
                    account_ids=[str(row["account_id"]) for row in suspicious_annuity_bucket_rows],
                    details={"heuristic_basis": "account_name contains annuity and bucket_type not income/future_income"},
                )

            orphan_snapshot_rows = connection.execute(
                """
SELECT s.account_id
FROM capital_balance_snapshots_v1 AS s
LEFT JOIN capital_accounts_v1 AS a ON a.account_id = s.account_id
WHERE a.account_id IS NULL
GROUP BY s.account_id
ORDER BY s.account_id ASC
""".strip()
            ).fetchall()
            if orphan_snapshot_rows:
                add_finding(
                    code="CAPITAL_ORPHAN_BALANCE_SNAPSHOT",
                    severity="CRITICAL",
                    message="Balance snapshots exist for missing account rows.",
                    account_ids=[str(row["account_id"]) for row in orphan_snapshot_rows],
                )

            orphan_flow_rows = connection.execute(
                """
SELECT f.account_id
FROM capital_cash_flows_v1 AS f
LEFT JOIN capital_accounts_v1 AS a ON a.account_id = f.account_id
WHERE a.account_id IS NULL
GROUP BY f.account_id
ORDER BY f.account_id ASC
""".strip()
            ).fetchall()
            if orphan_flow_rows:
                add_finding(
                    code="CAPITAL_ORPHAN_CASH_FLOW",
                    severity="CRITICAL",
                    message="Cash flows exist for missing account rows.",
                    account_ids=[str(row["account_id"]) for row in orphan_flow_rows],
                )

            freshness = self._freshness_completeness_summary(as_of_date=day)
            if freshness["missing_accounts"]:
                add_finding(
                    code="CAPITAL_INCLUDED_ACCOUNT_SNAPSHOT_MISSING",
                    severity="CRITICAL",
                    message="Included active accounts are missing snapshots.",
                    account_ids=[str(item["account_id"]) for item in freshness["missing_accounts"]],
                    details={
                        "latest_global_snapshot_day": freshness["global_latest_snapshot_day"],
                        "missing_accounts": freshness["missing_accounts"],
                        "policy_id": freshness["policy_id"],
                    },
                )
            if freshness["stale_accounts"]:
                add_finding(
                    code="CAPITAL_INCLUDED_ACCOUNT_SNAPSHOT_STALE",
                    severity="WARNING",
                    message="Included active accounts are stale relative to the latest snapshot day.",
                    account_ids=[str(item["account_id"]) for item in freshness["stale_accounts"]],
                    details={
                        "latest_global_snapshot_day": freshness["global_latest_snapshot_day"],
                        "stale_accounts": freshness["stale_accounts"],
                        "policy_id": freshness["policy_id"],
                    },
                )

        classifications_by_account: dict[str, list[dict[str, Any]]] = {}
        for row in self.list_classifications():
            classifications_by_account.setdefault(str(row["account_id"]), []).append(row)
        for account_id, rows in classifications_by_account.items():
            sorted_rows = sorted(rows, key=lambda item: (str(item["effective_from"]), str(item["changed_at"]), str(item["classification_id"])))
            for index in range(1, len(sorted_rows)):
                prev = sorted_rows[index - 1]
                current = sorted_rows[index]
                prev_to = prev.get("effective_to")
                if isinstance(prev_to, str) and prev_to:
                    prev_end = _date_from_day(prev_to)
                    current_start = _date_from_day(str(current["effective_from"]))
                    if current_start > (prev_end + timedelta(days=1)):
                        add_finding(
                            code="CAPITAL_CLASSIFICATION_GAP",
                            severity="WARNING",
                            message="Classification timeline has a date gap between effective periods.",
                            account_ids=[account_id],
                            details={
                                "previous_effective_to": str(prev_to),
                                "next_effective_from": str(current["effective_from"]),
                            },
                        )

        severity_counts: dict[str, int] = {}
        for finding in findings:
            severity = str(finding["severity"])
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        if not findings:
            status = "HEALTHY"
        elif severity_counts.get("CRITICAL", 0) > 0:
            status = "INVALID"
        else:
            status = "DEGRADED"
        return {
            "as_of_date": day,
            "status": status,
            "error_count": len(findings),
            "severity_counts": severity_counts,
            "freshness_policy_id": FRESHNESS_POLICY_ID_V1,
            "freshness_policy_description": FRESHNESS_POLICY_DESCRIPTION_V1,
            "findings": findings,
        }

    def history_investable_total(self) -> dict[str, Any]:
        with capital_connection_v1(self.db_path) as connection:
            days = [
                str(row["as_of_date"])
                for row in connection.execute(
                    """
SELECT DISTINCT as_of_date
FROM capital_balance_snapshots_v1
ORDER BY as_of_date ASC
""".strip()
                ).fetchall()
            ]
        points: list[dict[str, Any]] = []
        for day in days:
            metric = self.current_investable_capital(as_of_date=day)
            points.append(
                {
                    "day": day,
                    "investable_total": metric["investable_total"],
                    "included_account_count": metric["included_account_count"],
                }
            )
        return {
            "basis": "latest_per_account_on_or_before_day",
            "points": points,
        }

    def history_aegis_allocation_pct(self) -> dict[str, Any]:
        with capital_connection_v1(self.db_path) as connection:
            days = [
                str(row["as_of_date"])
                for row in connection.execute(
                    """
SELECT DISTINCT as_of_date
FROM capital_balance_snapshots_v1
ORDER BY as_of_date ASC
""".strip()
                ).fetchall()
            ]
        points: list[dict[str, Any]] = []
        for day in days:
            allocation = self.allocation_by_control(as_of_date=day)
            total = float(allocation.get("total_included_balance") or 0.0)
            aegis_row = next((row for row in allocation["rows"] if row["control_type"] == "aegis"), None)
            aegis_total = 0.0 if aegis_row is None else float(aegis_row["balance_total"])
            pct = None if total <= 0 else round(aegis_total / total, 8)
            points.append(
                {
                    "day": day,
                    "aegis_balance": round(aegis_total, 2),
                    "investable_total": round(total, 2),
                    "aegis_allocation_pct": pct,
                }
            )
        return {
            "basis": "latest_per_account_on_or_before_day",
            "points": points,
        }

    def flow_summary_by_period(self) -> dict[str, Any]:
        with capital_connection_v1(self.db_path) as connection:
            rows = connection.execute(
                """
SELECT
    SUBSTR(flow_date, 1, 7) AS period,
    SUM(flow_amount) AS net_flow_amount,
    COUNT(*) AS flow_count
FROM capital_cash_flows_v1
GROUP BY SUBSTR(flow_date, 1, 7)
ORDER BY period ASC
""".strip()
            ).fetchall()
            return {
                "period_granularity": "month",
                "flow_truth_owner": self._flow_truth_descriptor(),
                "rows": [
                    {
                        "period": str(row["period"]),
                        "net_flow_amount": round(float(row["net_flow_amount"] or 0.0), 2),
                        "flow_count": int(row["flow_count"]),
                    }
                    for row in rows
                ],
            }

    def _cashflow_event_active_for_month(self, *, event: dict[str, Any], month_start: datetime) -> bool:
        start_day = _require_iso_day("START_DATE", event.get("start_date"))
        start_dt = _date_from_day(start_day)
        month_floor = _month_floor(month_start)
        month_end = _next_month(month_floor) - timedelta(days=1)
        end_raw = event.get("end_date")
        end_dt = None if end_raw in {None, ""} else _date_from_day(_require_iso_day("END_DATE", end_raw))
        if start_dt > month_end:
            return False
        if end_dt is not None and end_dt < month_floor:
            return False
        frequency = str(event.get("frequency") or "")
        if frequency == "monthly":
            return True
        if frequency == "annual":
            return month_floor.month == start_dt.month
        if frequency == "one_time":
            return month_floor.year == start_dt.year and month_floor.month == start_dt.month
        return False

    def cashflow_projection(
        self,
        *,
        scenario: str = "florida",
        include_nondeterministic: bool = False,
        horizon_months: int = 24,
        start_month: str | None = None,
    ) -> dict[str, Any]:
        normalized_scenario = _require_enum("SCENARIO", scenario, CASHFLOW_SCENARIOS_V1)
        safe_horizon = max(1, min(int(horizon_months), 120))
        scenario_scope = {"base", normalized_scenario}
        all_events = [
            row
            for row in self.list_cashflow_events()
            if str(row.get("scenario") or "") in scenario_scope
        ]
        deterministic_events = [row for row in all_events if bool(int(row.get("is_deterministic") or 0))]
        projection_events = list(all_events) if include_nondeterministic else list(deterministic_events)

        findings: list[dict[str, Any]] = []

        def add_finding(*, code: str, severity: str, message: str, details: dict[str, Any] | None = None) -> None:
            findings.append(
                {
                    "code": code,
                    "severity": severity,
                    "message": message,
                    "details": details or {},
                }
            )

        invalid_frequency_event_ids = [
            int(row["event_id"])
            for row in projection_events
            if str(row.get("frequency") or "") not in CASHFLOW_FREQUENCIES_V1
        ]
        if invalid_frequency_event_ids:
            add_finding(
                code="CAPITAL_CASHFLOW_INVALID_FREQUENCY",
                severity="CRITICAL",
                message="One or more cashflow events have invalid frequency values.",
                details={"event_ids": invalid_frequency_event_ids},
            )

        if normalized_scenario in {"florida", "chile"}:
            has_scenario_expense = any(
                str(row.get("event_type") or "") == "expense"
                and str(row.get("scenario") or "") == normalized_scenario
                for row in projection_events
            )
            if not has_scenario_expense:
                add_finding(
                    code="CAPITAL_CASHFLOW_SCENARIO_EXPENSE_MISSING",
                    severity="WARNING",
                    message="Scenario-specific expense event is missing for selected scenario.",
                    details={"scenario": normalized_scenario},
                )

        has_income_event = any(
            str(row.get("event_type") or "") in {"invest_income", "social_security"}
            for row in projection_events
        )
        if not has_income_event:
            add_finding(
                code="CAPITAL_CASHFLOW_INCOME_EVENT_MISSING",
                severity="CRITICAL",
                message="No deterministic income events are available for projection.",
                details={"scenario": normalized_scenario},
            )

        by_overlap_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in projection_events:
            by_overlap_key.setdefault((str(row.get("event_name") or ""), str(row.get("scenario") or "")), []).append(row)
        overlapping: list[dict[str, Any]] = []
        for (event_name, scenario_name), rows in by_overlap_key.items():
            sorted_rows = sorted(rows, key=lambda item: str(item.get("start_date") or ""))
            for idx in range(1, len(sorted_rows)):
                prev = sorted_rows[idx - 1]
                current = sorted_rows[idx]
                prev_end_raw = prev.get("end_date")
                prev_end = (
                    datetime.strptime("9999-12-31", "%Y-%m-%d")
                    if prev_end_raw in {None, ""}
                    else _date_from_day(_require_iso_day("END_DATE", prev_end_raw))
                )
                current_start = _date_from_day(_require_iso_day("START_DATE", current.get("start_date")))
                if current_start <= prev_end:
                    overlapping.append(
                        {
                            "event_name": event_name,
                            "scenario": scenario_name,
                            "previous_event_id": int(prev["event_id"]),
                            "current_event_id": int(current["event_id"]),
                        }
                    )
        if overlapping:
            add_finding(
                code="CAPITAL_CASHFLOW_EVENT_OVERLAP",
                severity="WARNING",
                message="Overlapping cashflow event windows were detected for identical event_name/scenario keys.",
                details={"overlaps": overlapping},
            )

        projection_seed_events = projection_events
        if not projection_seed_events:
            add_finding(
                code="CAPITAL_CASHFLOW_EVENTS_MISSING",
                severity="CRITICAL",
                message="No cashflow events were available for projection.",
                details={"scenario": normalized_scenario},
            )

        if isinstance(start_month, str) and start_month.strip():
            try:
                projection_start = datetime.strptime(f"{start_month.strip()}-01", "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("CAPITAL_START_MONTH_INVALID") from exc
        elif projection_seed_events:
            min_start = min(
                _date_from_day(_require_iso_day("START_DATE", row.get("start_date")))
                for row in projection_seed_events
            )
            projection_start = _month_floor(min_start)
        else:
            projection_start = _month_floor(datetime.now(UTC))

        months: list[datetime] = []
        cursor = _month_floor(projection_start)
        for _ in range(safe_horizon):
            months.append(cursor)
            cursor = _next_month(cursor)

        projection_rows: list[dict[str, Any]] = []
        cumulative = 0.0
        for month_start in months:
            income_total = 0.0
            expense_total = 0.0
            contributors: list[dict[str, Any]] = []
            for event in projection_events:
                if not self._cashflow_event_active_for_month(event=event, month_start=month_start):
                    continue
                amount = round(float(event.get("amount") or 0.0), 2)
                event_type = str(event.get("event_type") or "")
                if event_type == "expense":
                    expense_total += amount
                else:
                    income_total += amount
                contributors.append(
                    {
                        "event_id": int(event["event_id"]),
                        "event_name": str(event["event_name"]),
                        "event_type": event_type,
                        "scenario": str(event["scenario"]),
                        "amount": amount,
                        "frequency": str(event["frequency"]),
                        "is_deterministic": bool(int(event.get("is_deterministic") or 0)),
                    }
                )
            net = round(income_total - expense_total, 2)
            cumulative = round(cumulative + net, 2)
            projection_rows.append(
                {
                    "month": _month_key(month_start),
                    "income": round(income_total, 2),
                    "expenses": round(expense_total, 2),
                    "net": net,
                    "cumulative": cumulative,
                    "contributors": contributors,
                }
            )

        negative_streak = 0
        max_negative_streak = 0
        negative_months: list[str] = []
        for row in projection_rows:
            if float(row["net"]) < 0:
                negative_streak += 1
                max_negative_streak = max(max_negative_streak, negative_streak)
                negative_months.append(str(row["month"]))
            else:
                negative_streak = 0
        if max_negative_streak >= 3:
            add_finding(
                code="CAPITAL_CASHFLOW_NEGATIVE_STREAK_AT_RISK",
                severity="CRITICAL",
                message="Projected net cashflow is negative for at least three consecutive months.",
                details={"max_negative_streak": max_negative_streak, "negative_months": negative_months},
            )
        elif negative_months:
            add_finding(
                code="CAPITAL_CASHFLOW_NEGATIVE_MONTH_DETECTED",
                severity="WARNING",
                message="Projected net cashflow is negative in one or more months.",
                details={"negative_months": negative_months},
            )

        if not include_nondeterministic and any(
            str(row.get("event_type") or "") == "inheritance"
            for row in all_events
        ):
            add_finding(
                code="CAPITAL_CASHFLOW_NONDETERMINISTIC_EXCLUDED",
                severity="INFO",
                message="Non-deterministic inheritance input is excluded from deterministic projection basis.",
                details={
                    "excluded_event_ids": [
                        int(row["event_id"])
                        for row in all_events
                        if str(row.get("event_type") or "") == "inheritance"
                        and not bool(int(row.get("is_deterministic") or 0))
                    ]
                },
            )

        severity_counts: dict[str, int] = {}
        for finding in findings:
            severity = str(finding["severity"])
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        if severity_counts.get("CRITICAL", 0) > 0:
            status = "AT_RISK"
        elif severity_counts.get("WARNING", 0) > 0:
            status = "DEGRADED"
        else:
            status = "HEALTHY"

        min_net = None if not projection_rows else min(float(row["net"]) for row in projection_rows)
        operator_status = "HEALTHY"
        if status == "AT_RISK":
            operator_status = "AT_RISK"
        elif status == "DEGRADED":
            operator_status = "TIGHT"

        weakest_month = None
        if projection_rows:
            weakest_source = min(
                projection_rows,
                key=lambda row: (
                    float(row.get("net") or 0.0),
                    str(row.get("month") or ""),
                ),
            )
            weakest_month = {
                "month": str(weakest_source["month"]),
                "income": round(float(weakest_source["income"]), 2),
                "expenses": round(float(weakest_source["expenses"]), 2),
                "net": round(float(weakest_source["net"]), 2),
                "cumulative": round(float(weakest_source["cumulative"]), 2),
            }

        failure_month = None
        running_negative_streak = 0
        for row in projection_rows:
            if float(row["net"]) < 0:
                running_negative_streak += 1
                if running_negative_streak >= 3:
                    failure_month = str(row["month"])
                    break
            else:
                running_negative_streak = 0

        critical_codes = {
            str(finding.get("code") or "")
            for finding in findings
            if str(finding.get("severity") or "") == "CRITICAL"
        }
        month_specific_failure = "CAPITAL_CASHFLOW_NEGATIVE_STREAK_AT_RISK" in critical_codes
        non_month_specific_critical = bool(critical_codes - {"CAPITAL_CASHFLOW_NEGATIVE_STREAK_AT_RISK"})
        if non_month_specific_critical:
            failure = {
                "status": "FAIL_CLOSED",
                "month": None,
                "condition": "critical_projection_validation",
                "message": "Critical projection validation prevents a trusted month-specific failure answer.",
            }
        elif month_specific_failure and failure_month is not None:
            failure = {
                "status": "FAILS_WITHIN_HORIZON",
                "month": failure_month,
                "condition": "three_consecutive_negative_net_months",
                "message": "Kernel threshold is breached when projected net cashflow is negative for three consecutive months.",
            }
        else:
            failure = {
                "status": "NO_FAILURE_WITHIN_HORIZON",
                "month": None,
                "condition": "three_consecutive_negative_net_months",
                "message": "Kernel failure threshold is not breached within the projection horizon.",
            }

        driver_contributors: list[dict[str, Any]] = []
        if weakest_month is not None:
            weakest_row = next(
                (row for row in projection_rows if str(row.get("month") or "") == weakest_month["month"]),
                None,
            )
            if weakest_row is not None:
                driver_contributors = sorted(
                    [
                        {
                            "event_id": int(item["event_id"]),
                            "event_name": str(item["event_name"]),
                            "event_type": str(item["event_type"]),
                            "scenario": str(item["scenario"]),
                            "amount": round(float(item["amount"]), 2),
                            "frequency": str(item["frequency"]),
                            "is_deterministic": bool(item["is_deterministic"]),
                        }
                        for item in weakest_row.get("contributors", [])
                    ],
                    key=lambda item: (
                        0 if item["event_type"] == "expense" else 1,
                        -abs(float(item["amount"])),
                        str(item["event_name"]),
                    ),
                )

        operator_console = {
            "safety": {
                "answer": (
                    "SAFE_WITHIN_HORIZON"
                    if status == "HEALTHY"
                    else "SAFE_BUT_DEGRADED"
                    if status == "DEGRADED"
                    else "NOT_SAFE"
                ),
                "status": status,
                "operator_status": operator_status,
                "reason_codes": [
                    str(finding.get("code") or "")
                    for finding in findings
                    if str(finding.get("severity") or "") in {"CRITICAL", "WARNING"}
                ],
            },
            "weakest_month": weakest_month,
            "failure": failure,
            "drivers": {
                "basis_month": weakest_month["month"] if weakest_month is not None else None,
                "contributors": driver_contributors,
                "expense_contributors": [
                    item for item in driver_contributors if item["event_type"] == "expense"
                ],
                "income_contributors": [
                    item for item in driver_contributors if item["event_type"] != "expense"
                ],
            },
            "trust": {
                "calculation_authority": "CapitalDomainServiceV1.cashflow_projection",
                "projection_view": "v_capital_cashflow_projection_v1",
                "source_table": "capital_cashflow_events_v1",
                "deterministic_only": not include_nondeterministic,
                "inheritance_excluded": not include_nondeterministic,
                "scenario_scope": sorted(scenario_scope),
                "horizon_months": safe_horizon,
                "start_month": _month_key(_month_floor(projection_start)),
                "event_count": len(projection_events),
                "validation_status": status,
                "severity_counts": severity_counts,
                "ui_calculation_policy": "UI renders projection-kernel fields and does not calculate financial truth.",
            },
        }

        return {
            "projection_view": "v_capital_cashflow_projection_v1",
            "scenario": normalized_scenario,
            "status": status,
            "operator_status": operator_status,
            "monthly_projection": projection_rows,
            "min_net": min_net,
            "max_negative_streak": max_negative_streak,
            "operator_console": operator_console,
            "validation": {
                "status": status,
                "severity_counts": severity_counts,
                "findings": findings,
            },
            "basis": {
                "report_basis": "v_capital_cashflow_projection_v1",
                "basis_description": (
                    "Month-by-month deterministic projection from capital_cashflow_events_v1; "
                    "net equals deterministic income minus deterministic expenses."
                ),
                "deterministic_only": not include_nondeterministic,
                "inheritance_excluded": not include_nondeterministic,
                "scenario_scope": sorted(scenario_scope),
                "horizon_months": safe_horizon,
                "start_month": _month_key(_month_floor(projection_start)),
                "event_count": len(projection_events),
            },
        }

    def overview_surface(self, *, as_of_date: str | None = None) -> dict[str, Any]:
        investable = self.current_investable_capital(as_of_date=as_of_date)
        allocation_control = self.allocation_by_control(as_of_date=investable["as_of_date"])
        allocation_bucket = self.allocation_by_bucket(as_of_date=investable["as_of_date"])
        matrix = self.bucket_control_matrix(as_of_date=investable["as_of_date"])
        included_excluded = self.included_excluded_summary(as_of_date=investable["as_of_date"])
        validations = self.validation_errors(as_of_date=investable["as_of_date"])
        facts_day, facts = self._account_facts(as_of_date=investable["as_of_date"])
        by_control = {row["control_type"]: row["balance_total"] for row in allocation_control["rows"]}
        report_basis_metadata = investable.get("report_basis_metadata") or self._report_basis_payload(
            report_basis="latest_per_account",
            basis_description=(
                "For each account, use latest snapshot on or before report day with current effective "
                "classification for include/exclude semantics."
            ),
            as_of_date=facts_day,
            included_count=int(investable["included_account_count"]),
            excluded_count=int(investable["excluded_account_count"]),
            freshness=investable.get("freshness_completeness"),
            validation=validations,
        )
        return {
            "as_of_date": facts_day,
            "basis": "latest_per_account",
            "investable_total": investable["investable_total"],
            "advisor_controlled_capital": round(float(by_control.get("advisor") or 0.0), 2),
            "aegis_controlled_capital": round(float(by_control.get("aegis") or 0.0), 2),
            "passive_controlled_capital": round(float(by_control.get("passive") or 0.0), 2),
            "included_account_count": investable["included_account_count"],
            "excluded_account_count": investable["excluded_account_count"],
            "allocation_by_control": allocation_control,
            "allocation_by_bucket": allocation_bucket,
            "bucket_control_matrix": matrix,
            "included_excluded_summary": included_excluded,
            "validation_status": validations["status"],
            "validation_error_count": validations["error_count"],
            "active_validation_findings": validations["findings"],
            "contributing_accounts": [row.to_dict() for row in facts if row.include_in_allocation and row.balance is not None],
            "investable_explainability": {
                "numerator": "sum_of_included_account_balances",
                "denominator": "sum_of_included_account_balances",
                "denominator_value": investable["investable_total"],
                "contributors": investable.get("contributing_account_details", []),
                "exclusions": included_excluded.get("excluded_account_details", []),
            },
            "report_basis_metadata": report_basis_metadata,
            "freshness_completeness": investable.get("freshness_completeness"),
            "validation_status_summary": {
                "status": validations["status"],
                "error_count": validations["error_count"],
                "severity_counts": validations["severity_counts"],
            },
        }
