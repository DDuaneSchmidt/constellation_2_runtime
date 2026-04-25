#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from constellation_2.common.capital.service_v1 import CapitalDomainServiceV1
from constellation_2.common.capital.seed_v1 import seed_capital_reference_dataset_v1
from constellation_2.common.capital.storage_v1 import ensure_capital_schema_v1


def _build_service(db_path: str | None) -> CapitalDomainServiceV1:
    resolved = None if db_path is None else Path(db_path).expanduser().resolve()
    return CapitalDomainServiceV1(db_path=resolved, actor="ops.tools.run_capital_domain_v1")


def _print(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Capital bounded-domain toolchain v1.")
    parser.add_argument("--db_path", default=None, help="Optional absolute/relative path to the Capital sqlite database.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Initialize Capital schema/migrations.")
    sub.add_parser("seed", help="Load the canonical reference Capital seed dataset.")
    sub.add_parser("overview", help="Print current Capital overview surface.")
    sub.add_parser("accounts", help="Print account + latest-balance surface.")
    sub.add_parser("allocation", help="Print allocation and matrix surfaces.")
    sub.add_parser("history", help="Print investable and Aegis allocation time series.")
    sub.add_parser("flows", help="Print flow records and monthly flow summary.")
    cashflow = sub.add_parser("cashflow", help="Print scenario cashflow projection timeline.")
    cashflow.add_argument("--scenario", choices=["base", "florida", "chile"], default="florida")
    cashflow.add_argument("--include_nondeterministic", choices=["true", "false"], default="false")
    cashflow.add_argument("--horizon_months", type=int, default=24)
    cashflow.add_argument("--start_month", default=None, help="Optional projection start month in YYYY-MM.")
    sub.add_parser("validation", help="Print current Capital validation findings.")

    create_account = sub.add_parser("create-account", help="Create a new Capital account.")
    create_account.add_argument("--account_id", required=True)
    create_account.add_argument("--account_name", required=True)
    create_account.add_argument("--notes", default="")
    create_account.add_argument("--inactive", action="store_true")
    create_account.add_argument("--reason", default="")

    classification = sub.add_parser("append-classification", help="Append an effective-dated account classification.")
    classification.add_argument("--account_id", required=True)
    classification.add_argument("--effective_from", required=True)
    classification.add_argument("--effective_to", default=None)
    classification.add_argument("--capital_type", required=True)
    classification.add_argument("--control_type", required=True)
    classification.add_argument("--bucket_type", required=True)
    classification.add_argument("--include_in_allocation", choices=["true", "false"], required=True)
    classification.add_argument("--confidence_level", type=float, required=True)
    classification.add_argument("--confidence_band", choices=["high", "medium", "low"], default=None)
    classification.add_argument("--notes", default="")
    classification.add_argument("--reason", required=True)

    snapshot = sub.add_parser("append-snapshot", help="Append a balance snapshot.")
    snapshot.add_argument("--account_id", required=True)
    snapshot.add_argument("--as_of_date", required=True)
    snapshot.add_argument("--balance", type=float, required=True)
    snapshot.add_argument("--input_source", required=True)
    snapshot.add_argument("--notes", default="")
    snapshot.add_argument("--reason", default="")

    flow = sub.add_parser("append-flow", help="Append a cash flow.")
    flow.add_argument("--account_id", required=True)
    flow.add_argument("--flow_date", required=True)
    flow.add_argument("--flow_amount", type=float, required=True)
    flow.add_argument("--flow_type", required=True)
    flow.add_argument("--notes", default="")
    flow.add_argument("--reason", default="")

    cashflow_event = sub.add_parser("append-cashflow-event", help="Append a cashflow timeline event.")
    cashflow_event.add_argument("--event_name", required=True)
    cashflow_event.add_argument("--event_type", choices=["invest_income", "social_security", "expense", "inheritance"], required=True)
    cashflow_event.add_argument("--scenario", choices=["base", "florida", "chile"], required=True)
    cashflow_event.add_argument("--start_date", required=True)
    cashflow_event.add_argument("--end_date", default=None)
    cashflow_event.add_argument("--frequency", choices=["monthly", "annual", "one_time"], required=True)
    cashflow_event.add_argument("--amount", type=float, required=True)
    cashflow_event.add_argument("--confidence", type=float, required=True)
    cashflow_event.add_argument("--is_deterministic", choices=["true", "false"], required=True)
    cashflow_event.add_argument("--notes", default="")
    cashflow_event.add_argument("--reason", default="")

    args = parser.parse_args(argv)

    if args.command == "init":
        db = ensure_capital_schema_v1(None if args.db_path is None else Path(args.db_path).expanduser().resolve())
        return _print({"ok": True, "db_path": str(db), "command": "init"})

    service = _build_service(args.db_path)
    if args.command == "seed":
        return _print({"ok": True, "command": "seed", "result": seed_capital_reference_dataset_v1(service=service)})
    if args.command == "overview":
        return _print({"ok": True, "command": "overview", "result": service.overview_surface()})
    if args.command == "accounts":
        return _print(
            {
                "ok": True,
                "command": "accounts",
                "result": {
                    "accounts": service.list_accounts(),
                    "latest_balances": service.latest_balance_per_account(),
                    "current_classifications": service.current_classification_per_account(),
                },
            }
        )
    if args.command == "allocation":
        return _print(
            {
                "ok": True,
                "command": "allocation",
                "result": {
                    "current_investable": service.current_investable_capital(),
                    "allocation_by_control": service.allocation_by_control(),
                    "allocation_by_bucket": service.allocation_by_bucket(),
                    "bucket_control_matrix": service.bucket_control_matrix(),
                    "included_excluded": service.included_excluded_summary(),
                },
            }
        )
    if args.command == "history":
        return _print(
            {
                "ok": True,
                "command": "history",
                "result": {
                    "investable_time_series": service.history_investable_total(),
                    "aegis_allocation_pct_time_series": service.history_aegis_allocation_pct(),
                },
            }
        )
    if args.command == "flows":
        return _print(
            {
                "ok": True,
                "command": "flows",
                "result": {
                    "rows": service.list_cash_flows(),
                    "summary_by_period": service.flow_summary_by_period(),
                },
            }
        )
    if args.command == "cashflow":
        return _print(
            {
                "ok": True,
                "command": "cashflow",
                "result": service.cashflow_projection(
                    scenario=args.scenario,
                    include_nondeterministic=args.include_nondeterministic == "true",
                    horizon_months=args.horizon_months,
                    start_month=args.start_month,
                ),
            }
        )
    if args.command == "validation":
        return _print({"ok": True, "command": "validation", "result": service.validation_errors()})
    if args.command == "create-account":
        created = service.create_account(
            account_id=args.account_id,
            account_name=args.account_name,
            notes=args.notes,
            is_active=not bool(args.inactive),
            reason=args.reason,
        )
        return _print({"ok": True, "command": "create-account", "result": created})
    if args.command == "append-classification":
        created = service.append_classification(
            account_id=args.account_id,
            effective_from=args.effective_from,
            effective_to=args.effective_to,
            capital_type=args.capital_type,
            control_type=args.control_type,
            bucket_type=args.bucket_type,
            include_in_allocation=args.include_in_allocation == "true",
            confidence_level=args.confidence_level,
            confidence_band=args.confidence_band,
            notes=args.notes,
            reason=args.reason,
        )
        return _print({"ok": True, "command": "append-classification", "result": created})
    if args.command == "append-snapshot":
        created = service.append_balance_snapshot(
            account_id=args.account_id,
            as_of_date=args.as_of_date,
            balance=args.balance,
            input_source=args.input_source,
            notes=args.notes,
            reason=args.reason,
        )
        return _print({"ok": True, "command": "append-snapshot", "result": created})
    if args.command == "append-flow":
        created = service.append_cash_flow(
            account_id=args.account_id,
            flow_date=args.flow_date,
            flow_amount=args.flow_amount,
            flow_type=args.flow_type,
            notes=args.notes,
            reason=args.reason,
        )
        return _print({"ok": True, "command": "append-flow", "result": created})
    if args.command == "append-cashflow-event":
        created = service.append_cashflow_event(
            event_name=args.event_name,
            event_type=args.event_type,
            scenario=args.scenario,
            start_date=args.start_date,
            end_date=args.end_date,
            frequency=args.frequency,
            amount=args.amount,
            confidence=args.confidence,
            is_deterministic=args.is_deterministic == "true",
            notes=args.notes,
            reason=args.reason,
        )
        return _print({"ok": True, "command": "append-cashflow-event", "result": created})

    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
