from __future__ import annotations

import argparse
from pathlib import Path

from constellation_2.phaseF.cash_ledger.validators._validate_common_v1 import (
    validate_file_against_repo_schema_or_exit,
)

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate C2 cash ledger snapshot v1 against governed schema.")
    ap.add_argument("--path", required=True, help="Path to cash_ledger_snapshot.v1.json")
    args = ap.parse_args()

    validate_file_against_repo_schema_or_exit(Path(args.path).resolve(), SCHEMA_RELPATH)


if __name__ == "__main__":
    main()
