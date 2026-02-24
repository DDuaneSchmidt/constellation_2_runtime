#!/usr/bin/env python3
"""
probe_ib_accounts_v1.py

Fail-closed probe: connect to IB Gateway/TWS and list accounts seen via accountSummary.

Purpose:
- Discover which ports are active and which accounts are available (PAPER/LIVE).
- No secrets. No writes. No truth mutation.

Run (use .venv_c2 python):
  .venv_c2/bin/python ops/tools/probe_ib_accounts_v1.py --host 127.0.0.1 --port 7497 --client_id 7
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))


def main() -> int:
    ap = argparse.ArgumentParser(prog="probe_ib_accounts_v1")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--client_id", type=int, required=True)
    ap.add_argument("--timeout_seconds", type=int, default=4)
    args = ap.parse_args()

    try:
        from ib_insync import IB  # type: ignore
    except Exception as e:
        print(f"FATAL: ib_insync_import_failed: {e!r}", file=sys.stderr)
        return 2

    ib = IB()
    try:
        ok = ib.connect(str(args.host), int(args.port), clientId=int(args.client_id), timeout=float(int(args.timeout_seconds)))
        if not ok:
            print("FAIL: connect_returned_false", file=sys.stderr)
            return 3

        avs = ib.accountSummary()
        accounts: List[str] = []
        for a in avs:
            acct = str(getattr(a, "account", "") or "").strip()
            if acct:
                accounts.append(acct)

        uniq = sorted(set(accounts))
        if not uniq:
            print(f"OK: connected host={args.host} port={args.port} client_id={args.client_id} accounts=[]")
            return 0

        print(f"OK: connected host={args.host} port={args.port} client_id={args.client_id} accounts={uniq}")
        return 0
    except Exception as e:
        print(f"FAIL: connect_or_summary_error: {e!r}", file=sys.stderr)
        return 4
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
