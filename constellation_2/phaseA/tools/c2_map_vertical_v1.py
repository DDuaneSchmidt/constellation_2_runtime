"""
c2_map_vertical_v1.py

Constellation 2.0 Phase A
Offline CLI: deterministic vertical spread mapping.

Writes outputs under:
  constellation_2/phaseA/outputs/<run_id>/

Single-writer:
- Refuses to overwrite an existing run_id directory.

Usage (module form required):
  python3 -m constellation_2.phaseA.tools.c2_map_vertical_v1 \
    --intent <path> --chain <path> --freshness <path> \
    --now-utc <ISO_Z> --tick-size <decimal> --run-id <id>

Example:
  python3 -m constellation_2.phaseA.tools.c2_map_vertical_v1 \
    --intent constellation_2/acceptance/samples/sample_options_intent.v2.json \
    --chain constellation_2/acceptance/samples/sample_chain_snapshot.v1.json \
    --freshness constellation_2/acceptance/samples/sample_freshness_certificate.v1.json \
    --now-utc 2026-02-13T21:52:00Z \
    --tick-size 0.01 \
    --run-id sample_run_20260213T215200Z
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.phaseA.lib.canon_json_v1 import CanonJsonError, canonicalize_json_obj, inject_canonical_hash_field, load_json_file
from constellation_2.phaseA.lib.map_vertical_spread_v1 import MapResult, map_vertical_spread_offline
from constellation_2.phaseA.lib.validate_json_against_schema_v1 import (
    JsonSchemaValidationBoundaryError,
    validate_obj_against_schema,
)


class CliError(Exception):
    pass


def _must_file(p: Path) -> Path:
    if not p.exists() or not p.is_file():
        raise CliError(f"Missing file: {p}")
    return p


def _write_canonical_json(path: Path, obj: Dict[str, Any]) -> None:
    # Write canonical JSON string (no newline). Fail-closed on error.
    try:
        canon = canonicalize_json_obj(obj)
    except CanonJsonError as e:
        raise CliError(f"Canonicalization failed for write {path}: {e}") from e
    try:
        path.write_text(canon, encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        raise CliError(f"Write failed {path}: {e}") from e


def _validate_or_raise(schema_name: str, obj: Dict[str, Any]) -> None:
    try:
        r = validate_obj_against_schema(schema_name, obj)
    except JsonSchemaValidationBoundaryError as e:
        raise CliError(f"Schema boundary error for {schema_name}: {e}") from e
    if not r.ok:
        raise CliError(f"Schema validation failed for {schema_name}: {r.error}")


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--intent", required=True)
    ap.add_argument("--chain", required=True)
    ap.add_argument("--freshness", required=True)
    ap.add_argument("--now-utc", required=True)
    ap.add_argument("--tick-size", required=True)
    ap.add_argument("--run-id", required=True)

    args = ap.parse_args()

    root = Path.cwd().resolve()
    out_base = root / "constellation_2" / "phaseA" / "outputs"
    if not out_base.exists() or not out_base.is_dir():
        raise CliError(f"outputs directory missing: {out_base}")

    intent_p = _must_file(Path(args.intent).expanduser().resolve())
    chain_p = _must_file(Path(args.chain).expanduser().resolve())
    fresh_p = _must_file(Path(args.freshness).expanduser().resolve())

    run_id = (args.run_id or "").strip()
    if not run_id:
        raise CliError("run-id must be non-empty")
    out_dir = out_base / run_id

    # Single-writer: refuse overwrite
    if out_dir.exists():
        raise CliError(f"Refusing overwrite: output directory already exists: {out_dir}")

    # Create output directory
    out_dir.mkdir(parents=False, exist_ok=False)

    pointers = [str(intent_p), str(chain_p), str(fresh_p)]

    try:
        intent = load_json_file(intent_p)
        chain = load_json_file(chain_p)
        cert = load_json_file(fresh_p)
    except CanonJsonError as e:
        raise CliError(f"Input JSON load failed: {e}") from e

    # Defensive: inputs must validate
    for schema_name, obj in [
        ("options_intent.v2", intent),
        ("options_chain_snapshot.v1", chain),
        ("freshness_certificate.v1", cert),
    ]:
        _validate_or_raise(schema_name, obj)

    # Run mapping
    res: MapResult = map_vertical_spread_offline(
        intent=intent,
        chain=chain,
        cert=cert,
        now_utc=args.now_utc,
        tick_size=args.tick_size,
        pointers=pointers,
    )

    if res.ok:
        assert res.order_plan and res.mapping_ledger_record and res.binding_record
        # Validate outputs again (belt + suspenders)
        _validate_or_raise("order_plan.v1", res.order_plan)
        _validate_or_raise("mapping_ledger_record.v1", res.mapping_ledger_record)
        _validate_or_raise("binding_record.v1", res.binding_record)

        _write_canonical_json(out_dir / "order_plan.v1.json", res.order_plan)
        _write_canonical_json(out_dir / "mapping_ledger_record.v1.json", res.mapping_ledger_record)
        _write_canonical_json(out_dir / "binding_record.v1.json", res.binding_record)

        print(f"OK: wrote outputs to {out_dir}")
        print(f"ORDER_PLAN_HASH={res.order_plan['canonical_json_hash']}")
        print(f"MAPPING_LEDGER_HASH={res.mapping_ledger_record['canonical_json_hash']}")
        print(f"BINDING_RECORD_HASH={res.binding_record['canonical_json_hash']}")
        return 0

    assert res.veto_record is not None
    _validate_or_raise("veto_record.v1", res.veto_record)
    _write_canonical_json(out_dir / "veto_record.v1.json", res.veto_record)
    print(f"VETO: wrote veto_record to {out_dir}")
    print(f"VETO_HASH={res.veto_record['canonical_json_hash']}")
    print(f"REASON_CODE={res.veto_record['reason_code']}")
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"ERR: {e}", file=sys.stderr)
        return_code = 2
        raise SystemExit(return_code)
