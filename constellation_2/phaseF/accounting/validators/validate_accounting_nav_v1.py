from __future__ import annotations

import argparse
import sys
from pathlib import Path

from constellation_2.phaseF.accounting.validators._validate_common_v1 import (
    validate_instance_against_schema,
)

SCHEMA_FILENAME = "accounting_nav.v1.schema.json"


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate C2 accounting NAV artifact v1 against governed schema.")
    ap.add_argument("--path", required=True, help="Path to nav.json")
    args = ap.parse_args()

    p = Path(args.path).resolve()
    if not p.is_file():
        print(f"FAIL: missing file: {p}", file=sys.stderr)
        sys.exit(2)

    res = validate_instance_against_schema(p, SCHEMA_FILENAME)
    if res.ok:
        print("OK")
        sys.exit(0)

    print(f"{res.code}: {res.message}", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
