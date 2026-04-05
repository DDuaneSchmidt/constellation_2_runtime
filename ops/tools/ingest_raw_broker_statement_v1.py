#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.exists() or not p.is_dir():
        raise SystemExit(f"FATAL: invalid --truth_root: {p}")
    return p


def _require_json_object(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FATAL: cannot parse input_json: {path}: {exc!r}") from exc
    if not isinstance(obj, dict):
        raise SystemExit(f"FATAL: input_json top-level not object: {path}")
    return obj


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('wb') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _canonical_write(path: Path, content: bytes) -> None:
    if path.exists():
        existing_obj = _require_json_object(path)
        existing_content = _json_bytes(existing_obj)
        if _sha256_bytes(existing_content) != _sha256_bytes(content):
            raise SystemExit(
                f"FATAL: raw broker statement already exists with different content: {path} existing_sha={_sha256_bytes(existing_content)} candidate_sha={_sha256_bytes(content)}"
            )
        return
    _atomic_write(path, content)


def main() -> int:
    ap = argparse.ArgumentParser(prog='ingest_raw_broker_statement_v1')
    ap.add_argument('--day_utc', required=True, help='YYYY-MM-DD')
    ap.add_argument('--truth_root', required=True, help='Authoritative runtime truth root')
    ap.add_argument('--input_json', required=True, help='Raw broker statement JSON to ingest')
    args = ap.parse_args()

    truth_root = _require_truth_root(args.truth_root)
    day = str(args.day_utc).strip()
    input_path = Path(args.input_json).expanduser().resolve()
    if not input_path.exists() or not input_path.is_file():
        raise SystemExit(f"FATAL: input_json missing: {input_path}")

    obj = _require_json_object(input_path)
    content = _json_bytes(obj)
    out_path = (truth_root / 'operator_inputs' / 'raw_broker_statements' / day / 'broker_statement_raw.v1.json').resolve()
    _canonical_write(out_path, content)
    print(f"OK: wrote {out_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
