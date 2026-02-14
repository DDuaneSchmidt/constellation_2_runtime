"""
c2_build_chain_truth_v1.py

Constellation 2.0 — Phase B
Offline CLI: build OptionsChainSnapshot v1 + FreshnessCertificate v1 from a single raw input JSON.

Single-writer:
- Refuses to overwrite an existing output directory.

Fail-closed:
- On any failure, writes NOTHING (no output dir, no partial files).

Output directory rule:
- The parent directory of --out_dir MUST already exist and be a directory.
  (Matches Phase A operational style: required output base must exist.)

Usage (module form required):
  python3 -m constellation_2.phaseB.tools.c2_build_chain_truth_v1 \
    --raw_input <path> \
    --out_dir <path> \
    --max_age_seconds <int> \
    --clock_skew_tolerance_seconds <int>

Outputs (only if all steps succeed):
- <out_dir>/options_chain_snapshot.v1.json
- <out_dir>/freshness_certificate.v1.json
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseB.lib.build_freshness_certificate_v1 import FreshnessBuildError, build_freshness_certificate_v1
from constellation_2.phaseB.lib.build_options_chain_snapshot_v1 import RawInputError, build_options_chain_snapshot_v1
from constellation_2.phaseB.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseB.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class CliError(Exception):
    pass


def _must_file(p: Path) -> Path:
    if not p.exists() or not p.is_file():
        raise CliError(f"Missing file: {p}")
    return p


def _write_canonical_json(path: Path, obj: Dict[str, Any]) -> None:
    try:
        b = canonical_json_bytes_v1(obj)
        s = b.decode("utf-8")
    except (CanonicalizationError, UnicodeDecodeError) as e:
        raise CliError(f"Canonicalization failed for write {path}: {e}") from e
    try:
        path.write_text(s, encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        raise CliError(f"Write failed {path}: {e}") from e


def _inject_canonical_hash_field(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise CliError("Expected object for hash injection")
    h = canonical_hash_for_c2_artifact_v1(obj)
    out: Dict[str, Any] = dict(obj)
    out["canonical_json_hash"] = h
    return out


def _validate_snapshot_or_raise(repo_root: Path, snapshot: Dict[str, Any]) -> None:
    try:
        validate_against_repo_schema_v1(snapshot, repo_root, "constellation_2/schemas/options_chain_snapshot.v1.schema.json")
    except SchemaValidationError as e:
        raise CliError(f"Snapshot schema invalid: {e}") from e


def _validate_cert_or_raise(repo_root: Path, cert: Dict[str, Any]) -> None:
    try:
        validate_against_repo_schema_v1(cert, repo_root, "constellation_2/schemas/freshness_certificate.v1.schema.json")
    except SchemaValidationError as e:
        raise CliError(f"Certificate schema invalid: {e}") from e


def _load_json_file(p: Path) -> Any:
    try:
        txt = p.read_text(encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        raise CliError(f"Read failed: {p}: {e}") from e

    import json  # local import

    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        raise CliError(f"JSON parse failed: {p}: {e}") from e


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--raw_input", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--max_age_seconds", required=True, type=int)
    ap.add_argument("--clock_skew_tolerance_seconds", required=True, type=int)
    args = ap.parse_args()

    repo_root = Path.cwd().resolve()

    raw_p = _must_file(Path(args.raw_input).expanduser().resolve())
    out_dir = Path(args.out_dir).expanduser().resolve()

    # Parent directory must exist (fail-closed, no implicit directory creation beyond out_dir itself)
    out_parent = out_dir.parent
    if not out_parent.exists() or not out_parent.is_dir():
        raise CliError(f"Output parent directory missing: {out_parent}")

    # Single-writer: refuse overwrite
    if out_dir.exists():
        raise CliError(f"Refusing overwrite: output directory already exists: {out_dir}")

    # Load + validate/build everything BEFORE creating any outputs (fail-closed)
    raw_obj = _load_json_file(raw_p)
    if not isinstance(raw_obj, dict):
        raise CliError("raw_input must be a JSON object")

    try:
        snapshot0 = build_options_chain_snapshot_v1(raw=raw_obj, repo_root=repo_root)
    except RawInputError as e:
        raise CliError(f"Raw input invalid: {e}") from e

    snapshot = _inject_canonical_hash_field(snapshot0)
    _validate_snapshot_or_raise(repo_root, snapshot)

    try:
        cert0 = build_freshness_certificate_v1(
            snapshot=snapshot,
            repo_root=repo_root,
            max_age_seconds=int(args.max_age_seconds),
            clock_skew_tolerance_seconds=int(args.clock_skew_tolerance_seconds),
        )
    except FreshnessBuildError as e:
        raise CliError(f"Certificate build failed: {e}") from e

    cert = _inject_canonical_hash_field(cert0)
    _validate_cert_or_raise(repo_root, cert)

    # Only now create output directory and write files
    out_dir.mkdir(parents=False, exist_ok=False)

    snap_path = out_dir / "options_chain_snapshot.v1.json"
    cert_path = out_dir / "freshness_certificate.v1.json"
    _write_canonical_json(snap_path, snapshot)
    _write_canonical_json(cert_path, cert)

    print(f"OK: wrote outputs to {out_dir}")
    print(f"SNAPSHOT_HASH={snapshot['canonical_json_hash']}")
    print(f"CERT_HASH={cert['canonical_json_hash']}")
    print(f"CERT_SNAPSHOT_HASH={cert['snapshot_hash']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"ERR: {e}", file=sys.stderr)
        raise SystemExit(2)
