#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

from constellation_2.common.lifecycle_action_authority_v1 import (
    materialize_lifecycle_action_authority_set_v1,
)


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Materialize Core 3 lifecycle action authority artifacts from a Core 2 trade-state materialization.")
    parser.add_argument("--core2_materialization_dir", required=True)
    parser.add_argument("--execution_root", required=True)
    parser.add_argument("--evaluated_at_utc", default="")
    args = parser.parse_args(argv)

    evaluated_at_utc = str(args.evaluated_at_utc or "").strip() or _utc_now()
    result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=Path(args.core2_materialization_dir).resolve(),
        execution_root=Path(args.execution_root).resolve(),
        evaluated_at_utc=evaluated_at_utc,
    )
    print(
        "OK: LIFECYCLE_ACTION_AUTHORITY_V1_WRITTEN "
        f"day_utc={result['day_utc']} "
        f"materialization_set_id={result['materialization_set_id']} "
        f"trades_processed={result['trades_processed']} "
        f"operator_surface_path={result['operator_surface'].path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
