from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from constellation_2.common.truth_root_v1 import resolve_runtime_root

from .constants_v1 import (
    CASHFLOW_EVENT_TYPES_V1,
    CASHFLOW_FREQUENCIES_V1,
    CASHFLOW_SCENARIOS_V1,
    BUCKET_TYPES_V1,
    CAPITAL_DB_ENV_VAR_V1,
    CAPITAL_DB_RELATIVE_DEFAULT_V1,
    CAPITAL_TYPES_V1,
    CONFIDENCE_BANDS_V1,
    CONFIDENCE_BAND_HIGH_MIN_V1,
    CONFIDENCE_BAND_MEDIUM_MIN_V1,
    CONFIDENCE_MAX_V1,
    CONFIDENCE_MIN_V1,
    CONTROL_TYPES_V1,
    FLOW_TYPES_V1,
    INPUT_SOURCES_V1,
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sql_literal_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def resolve_capital_db_path_v1() -> Path:
    raw = (os.environ.get(CAPITAL_DB_ENV_VAR_V1) or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    runtime_root = resolve_runtime_root().resolve()
    path = runtime_root
    for part in CAPITAL_DB_RELATIVE_DEFAULT_V1:
        path = path / part
    return path.resolve()


@dataclass(frozen=True)
class MigrationSpecV1:
    migration_id: str
    sql: str


MIGRATIONS_V1: tuple[MigrationSpecV1, ...] = (
    MigrationSpecV1(
        migration_id="001_initial_capital_schema_v1",
        sql=f"""
CREATE TABLE IF NOT EXISTS capital_accounts_v1 (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL UNIQUE,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capital_account_classifications_v1 (
    classification_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    capital_type TEXT NOT NULL CHECK (capital_type IN ({_sql_literal_list(CAPITAL_TYPES_V1)})),
    control_type TEXT NOT NULL CHECK (control_type IN ({_sql_literal_list(CONTROL_TYPES_V1)})),
    bucket_type TEXT NOT NULL CHECK (bucket_type IN ({_sql_literal_list(BUCKET_TYPES_V1)})),
    include_in_allocation INTEGER NOT NULL CHECK (include_in_allocation IN (0, 1)),
    confidence_level REAL NOT NULL CHECK (confidence_level >= {CONFIDENCE_MIN_V1} AND confidence_level <= {CONFIDENCE_MAX_V1}),
    confidence_band TEXT NOT NULL CHECK (confidence_band IN ({_sql_literal_list(CONFIDENCE_BANDS_V1)})),
    notes TEXT NOT NULL DEFAULT '',
    changed_at TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    change_reason TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (account_id) REFERENCES capital_accounts_v1 (account_id)
);

CREATE INDEX IF NOT EXISTS idx_capital_classification_account_v1
ON capital_account_classifications_v1 (account_id, effective_from, effective_to);

CREATE TABLE IF NOT EXISTS capital_balance_snapshots_v1 (
    snapshot_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    balance REAL NOT NULL,
    input_source TEXT NOT NULL CHECK (input_source IN ({_sql_literal_list(INPUT_SOURCES_V1)})),
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    FOREIGN KEY (account_id) REFERENCES capital_accounts_v1 (account_id),
    UNIQUE (account_id, as_of_date, input_source)
);

CREATE INDEX IF NOT EXISTS idx_capital_snapshot_account_day_v1
ON capital_balance_snapshots_v1 (account_id, as_of_date);

CREATE TABLE IF NOT EXISTS capital_cash_flows_v1 (
    flow_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    flow_date TEXT NOT NULL,
    flow_amount REAL NOT NULL,
    flow_type TEXT NOT NULL CHECK (flow_type IN ({_sql_literal_list(FLOW_TYPES_V1)})),
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    FOREIGN KEY (account_id) REFERENCES capital_accounts_v1 (account_id)
);

CREATE INDEX IF NOT EXISTS idx_capital_flow_account_day_v1
ON capital_cash_flows_v1 (account_id, flow_date);

CREATE TABLE IF NOT EXISTS capital_audit_log_v1 (
    audit_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_capital_audit_entity_v1
ON capital_audit_log_v1 (entity_type, entity_id, created_at);
""".strip(),
    ),
    MigrationSpecV1(
        migration_id="002_capital_cashflow_events_v1",
        sql=f"""
CREATE TABLE IF NOT EXISTS capital_cashflow_events_v1 (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ({_sql_literal_list(CASHFLOW_EVENT_TYPES_V1)})),
    scenario TEXT NOT NULL CHECK (scenario IN ({_sql_literal_list(CASHFLOW_SCENARIOS_V1)})),
    start_date TEXT NOT NULL,
    end_date TEXT,
    frequency TEXT NOT NULL CHECK (frequency IN ({_sql_literal_list(CASHFLOW_FREQUENCIES_V1)})),
    amount REAL NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= {CONFIDENCE_MIN_V1} AND confidence <= {CONFIDENCE_MAX_V1}),
    is_deterministic INTEGER NOT NULL CHECK (is_deterministic IN (0, 1)),
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_capital_cashflow_events_scenario_v1
ON capital_cashflow_events_v1 (scenario, start_date, event_type);

CREATE INDEX IF NOT EXISTS idx_capital_cashflow_events_name_v1
ON capital_cashflow_events_v1 (event_name, scenario, start_date);
""".strip(),
    ),
)


def _open_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


@contextmanager
def capital_connection_v1(path: Path | None = None) -> Iterator[sqlite3.Connection]:
    resolved = (path or resolve_capital_db_path_v1()).resolve()
    conn = _open_connection(resolved)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_capital_schema_v1(path: Path | None = None) -> Path:
    resolved = (path or resolve_capital_db_path_v1()).resolve()
    with capital_connection_v1(resolved) as connection:
        connection.execute(
            """
CREATE TABLE IF NOT EXISTS capital_schema_migrations_v1 (
    migration_id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
)
""".strip()
        )
        for migration in MIGRATIONS_V1:
            row = connection.execute(
                "SELECT migration_id FROM capital_schema_migrations_v1 WHERE migration_id = ?",
                (migration.migration_id,),
            ).fetchone()
            if row is not None:
                continue
            connection.executescript(migration.sql)
            connection.execute(
                "INSERT INTO capital_schema_migrations_v1 (migration_id, applied_at) VALUES (?, ?)",
                (migration.migration_id, _utc_now_iso()),
            )
        _ensure_classification_confidence_band_v1(connection)
    return resolved


def _ensure_classification_confidence_band_v1(connection: sqlite3.Connection) -> None:
    table_rows = connection.execute("PRAGMA table_info(capital_account_classifications_v1)").fetchall()
    columns = {str(row["name"]) for row in table_rows}
    if "confidence_band" not in columns:
        connection.execute(
            f"""
ALTER TABLE capital_account_classifications_v1
ADD COLUMN confidence_band TEXT NOT NULL DEFAULT 'high' CHECK (confidence_band IN ({_sql_literal_list(CONFIDENCE_BANDS_V1)}))
""".strip()
        )
    connection.execute(
        """
UPDATE capital_account_classifications_v1
SET confidence_band = CASE
    WHEN confidence_level >= ? THEN 'high'
    WHEN confidence_level >= ? THEN 'medium'
    ELSE 'low'
END
WHERE confidence_band IS NULL
   OR confidence_band NOT IN ('high', 'medium', 'low')
""".strip(),
        (
            CONFIDENCE_BAND_HIGH_MIN_V1,
            CONFIDENCE_BAND_MEDIUM_MIN_V1,
        ),
    )


def row_dicts_v1(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def json_text_or_none_v1(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
