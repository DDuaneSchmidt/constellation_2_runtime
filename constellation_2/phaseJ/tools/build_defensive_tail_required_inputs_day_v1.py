#!/usr/bin/env python3
"""
Constellation 2.0 — Phase J

Build Defensive Tail Required Inputs (Bridge) v1

Creates (immutably) the three files required by:
  constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py

Outputs (under truth root):
- market_data_snapshot_v1/snapshots/<DAY>/<SYMBOL>.market_data_snapshot.v1.json
- accounting_v1/nav/<DAY>/nav_snapshot.v1.json
- positions_snapshot_v2/snapshots/<DAY>/positions_snapshot.v2.json

Truth root:
- If C2_TRUTH_ROOT is set: must be absolute existing dir
- Else: canonical constellation_2/runtime/truth

FAIL-CLOSED:
- Missing required source => exit nonzero
- Target exists with DIFFERENT bytes => exit nonzero (immutable rewrite attempt)
- Target exists with IDENTICAL bytes => OK (idempotent)
- Deterministic JSON bytes (sorted keys, minified, trailing newline)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional


ISO_DAY = "%Y-%m-%d"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_json_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _require_abs_existing_dir(p: str, name: str) -> Path:
    if not p:
        raise SystemExit(f"FAIL: missing required {name}")
    path = Path(p).expanduser()
    if not path.is_absolute():
        raise SystemExit(f"FAIL: {name} must be absolute: {p!r}")
    if not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: {name} must exist and be a directory: {p!r}")
    return path.resolve()


def _select_truth_root(repo_root: Path) -> Path:
    env = os.environ.get("C2_TRUTH_ROOT", "").strip()
    if env:
        return _require_abs_existing_dir(env, "C2_TRUTH_ROOT")
    return (repo_root / "constellation_2" / "runtime" / "truth").resolve()


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")
    return s


def _atomic_write_idempotent(path: Path, data: bytes) -> str:
    """
    Immutable + idempotent write rule:
      - If absent: write atomically => action=WROTE
      - If present and identical bytes => action=EXISTS_IDENTICAL
      - If present and different bytes => FAIL (ATTEMPTED_REWRITE)
    """
    cand_sha = _sha256_bytes(data)

    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: target exists but is not a file: {path}")
        existing = path.read_bytes()
        ex_sha = _sha256_bytes(existing)
        if ex_sha == cand_sha:
            return "EXISTS_IDENTICAL"
        raise SystemExit(
            f"FAIL: ATTEMPTED_REWRITE: {path} existing_sha={ex_sha} candidate_sha={cand_sha}"
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(str(tmp), str(path))
    return "WROTE"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: cannot parse json: {path}: {e!r}") from e


def _read_jsonl_objects(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            text = line.strip()
            if not text:
                continue
            obj = json.loads(text)
            if not isinstance(obj, dict):
                raise SystemExit(f"FAIL: jsonl row not object: {path}:{idx}")
            rows.append(obj)
    except Exception as e:
        if isinstance(e, SystemExit):
            raise
        raise SystemExit(f"FAIL: cannot parse jsonl: {path}: {e!r}") from e
    return rows


def _dec_str(v: Any, field: str) -> str:
    try:
        d = Decimal(str(v).strip())
    except (InvalidOperation, ValueError) as e:
        raise SystemExit(f"FAIL: decimal_parse_failed field={field} value={v!r}") from e
    return format(d, "f")


def _runtime_evaluation_ref(truth_root: Path, day: str) -> Dict[str, str]:
    path = (truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_evaluation.v1.json").resolve()
    if not path.exists():
        return {"path": "", "sha256": "", "runtime_evaluation_hash": ""}
    payload = _read_json_obj(path)
    return {
        "path": str(path),
        "sha256": _sha256_file(path),
        "runtime_evaluation_hash": str(payload.get("deterministic_output_hash") or ""),
    }


def _market_inputs_event_ref(truth_root: Path, day: str, market_inputs_path: Path) -> Dict[str, str]:
    events_path = truth_root / "events" / "aegis_evidence_events_v1" / day / "events.jsonl"
    if not events_path.exists():
        return {"event_id": "", "event_hash": ""}
    target = str(market_inputs_path.resolve())
    for line in reversed(events_path.read_text(encoding="utf-8").splitlines()):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except Exception:
            continue
        paths = [str(Path(path).expanduser().resolve()) for path in event.get("artifact_paths", []) if str(path)]
        if target in paths and str(event.get("event_type") or "") == "EvidenceValidated":
            return {"event_id": str(event.get("event_id") or ""), "event_hash": str(event.get("event_hash") or "")}
    return {"event_id": "", "event_hash": ""}


def _canonical_market_inputs_path(truth_root: Path, day: str) -> Path:
    local = (truth_root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json").resolve()
    if local.exists():
        return local
    canonical_raw = os.environ.get("C2_CANONICAL_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth").strip()
    if canonical_raw:
        canonical = Path(canonical_raw).expanduser().resolve()
        candidate = (canonical / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json").resolve()
        if candidate.exists():
            return candidate
    return local


def _canonical_spy_market_snapshot(truth_root: Path, day: str, sym: str) -> Dict[str, Any]:
    if sym != "SPY":
        return {}
    src = _canonical_market_inputs_path(truth_root, day)
    source_root = src.parents[3] if len(src.parents) > 3 else truth_root
    if not src.exists():
        raise SystemExit(f"FAIL: missing canonical market_data_inputs_v1 for SPY: {src}")
    payload = _read_json_obj(src)
    item_id = "market.price.SPY"
    record: Dict[str, Any] = {}
    for row in payload.get("input_records", []) if isinstance(payload.get("input_records"), list) else []:
        if isinstance(row, dict) and str(row.get("data_item_id") or "") == item_id:
            record = row
            break
    if not record:
        raise SystemExit(f"FAIL: canonical market input missing: {item_id}")
    validation = str(record.get("validation_status") or "").upper()
    source_timestamp = str(record.get("source_timestamp_utc") or "")
    if validation != "VALID":
        raise SystemExit(f"FAIL: canonical market input not valid: {item_id} status={validation or 'UNKNOWN'} reason={record.get('reason') or ''}")
    if source_timestamp[:10] != day:
        raise SystemExit(f"FAIL: canonical market input not current: {item_id} source_day={source_timestamp[:10] or 'UNKNOWN'} day={day}")
    raw_hash = str(record.get("raw_source_hash") or "")
    price_hash = str(record.get("transformed_value_hash") or "")
    if len(raw_hash) != 64 or len(price_hash) != 64:
        raise SystemExit(f"FAIL: canonical market input hash missing: {item_id}")
    price = _dec_str(record.get("value"), "market_data_inputs.market.price.SPY.value")
    runtime_ref = _runtime_evaluation_ref(source_root, day)
    event_ref = _market_inputs_event_ref(source_root, day, src)
    bar = {
        "close": price,
        "market_session_date": day,
        "source": "market_data_inputs_v1",
        "symbol": sym,
        "timestamp_utc": source_timestamp,
    }
    return {
        "bars": [bar],
        "day_utc": day,
        "freshness_status": "CURRENT",
        "runtime_evaluation_hash": runtime_ref.get("runtime_evaluation_hash", ""),
        "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
        "schema_version": "v1",
        "source_canonical_market_data_inputs_event_id": event_ref.get("event_id", ""),
        "source_timestamp_utc": source_timestamp,
        "source_provenance": {
            "canonical_market_data_inputs_v1": {
                "path": str(src),
                "sha256": _sha256_file(src),
                "event_id": event_ref.get("event_id", ""),
                "event_hash": event_ref.get("event_hash", ""),
                "data_item_id": item_id,
                "raw_source_hash": raw_hash,
                "transformed_value_hash": price_hash,
                "source_timestamp_utc": source_timestamp,
                "retrieval_timestamp_utc": str(record.get("retrieval_timestamp_utc") or record.get("retrieved_at_utc") or ""),
                "freshness_status": "CURRENT",
                "validation_status": validation,
            },
            "runtime_evaluation_v1": runtime_ref,
        },
        "spy_price_hash": price_hash,
        "symbol": sym,
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="build_defensive_tail_required_inputs_day_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--symbol", required=True, help="Underlying symbol from ENGINE_MODEL_REGISTRY_V1.allowed_symbols")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    truth_root = _select_truth_root(repo_root)

    day = _parse_day(args.day_utc)
    sym = str(args.symbol).strip().upper()
    if not sym:
        raise SystemExit("FAIL: --symbol must be non-empty")

    # Sources
    src_pos = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json").resolve()
    src_reg = (truth_root / "monitoring_v1" / "regime_snapshot_v2" / day / "regime_snapshot.v2.json").resolve()
    src_md = (truth_root / "market_data_snapshot_v1" / sym / f"{day[:4]}.jsonl").resolve()

    if not src_pos.exists():
        raise SystemExit(f"FAIL: missing required source positions snapshot: {src_pos}")
    if not src_reg.exists():
        raise SystemExit(f"FAIL: missing required source regime snapshot: {src_reg}")
    canonical_md_obj = _canonical_spy_market_snapshot(truth_root, day, sym)
    if not canonical_md_obj and not src_md.exists():
        raise SystemExit(f"FAIL: missing required source market data jsonl: {src_md}")

    # Targets
    out_md = (truth_root / "market_data_snapshot_v1" / "snapshots" / day / f"{sym}.market_data_snapshot.v1.json").resolve()
    out_nav = (truth_root / "accounting_v1" / "nav" / day / "nav_snapshot.v1.json").resolve()
    out_pos = (truth_root / "positions_snapshot_v2" / "snapshots" / day / "positions_snapshot.v2.json").resolve()

    print(f"OK: truth_root={truth_root}")
    print(f"OK: day_utc={day}")
    print(f"OK: symbol={sym}")

    # --- positions_snapshot_v2: byte-for-byte copy from positions_v1 ---
    pos_bytes = src_pos.read_bytes()
    act_pos = _atomic_write_idempotent(out_pos, pos_bytes)
    print(
        f"OK: positions_snapshot_v2 action={act_pos} path={out_pos} sha256={_sha256_file(out_pos) if out_pos.exists() else _sha256_bytes(pos_bytes)} "
        f"source={src_pos} source_sha256={_sha256_bytes(pos_bytes)}"
    )

    # --- nav_snapshot.v1: minimal schema surface with drawdown_pct ---
    reg = _read_json_obj(src_reg)
    dd_raw: Optional[Any] = None
    ev = reg.get("evidence")
    if isinstance(ev, dict) and "drawdown_pct" in ev:
        dd_raw = ev.get("drawdown_pct")
    if dd_raw is None:
        dd_raw = "0.000000"
    dd = _dec_str(dd_raw, "regime_snapshot_v2.evidence.drawdown_pct")

    nav_obj = {
        "day_utc": day,
        "history": {"drawdown_pct": dd},
        "schema_id": "C2_NAV_SNAPSHOT_V1",
        "schema_version": "v1",
    }
    nav_bytes = _stable_json_bytes(nav_obj)
    act_nav = _atomic_write_idempotent(out_nav, nav_bytes)
    nav_sha = _sha256_file(out_nav) if out_nav.exists() else _sha256_bytes(nav_bytes)
    print(
        f"OK: nav_snapshot_v1 action={act_nav} path={out_nav} sha256={nav_sha} drawdown_pct={dd} "
        f"source_regime={src_reg} source_regime_sha256={_sha256_file(src_reg)}"
    )

    # --- market_data_snapshot_v1 daily snapshot wrapper ---
    if canonical_md_obj:
        md_obj = canonical_md_obj
        source_market_data = md_obj["source_provenance"]["canonical_market_data_inputs_v1"]["path"]
        source_market_data_sha = md_obj["source_provenance"]["canonical_market_data_inputs_v1"]["sha256"]
        matching_day_rows = len(md_obj.get("bars") or [])
    else:
        md_rows = _read_jsonl_objects(src_md)
        day_rows = [
            row
            for row in md_rows
            if str(row.get("symbol") or "").strip().upper() == sym
            and str(row.get("timestamp_utc") or "").startswith(day)
        ]
        md_obj = {
            "bars": day_rows,
            "day_utc": day,
            "source_provenance": {
                "market_data_yearly_jsonl": {
                    "path": str(src_md),
                    "sha256": _sha256_file(src_md),
                    "row_count": len(md_rows),
                    "matching_day_row_count": len(day_rows),
                }
            },
            "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
            "schema_version": "v1",
            "symbol": sym,
        }
        source_market_data = str(src_md)
        source_market_data_sha = _sha256_file(src_md)
        matching_day_rows = len(day_rows)
    md_bytes = _stable_json_bytes(md_obj)
    act_md = _atomic_write_idempotent(out_md, md_bytes)
    md_sha = _sha256_file(out_md) if out_md.exists() else _sha256_bytes(md_bytes)
    print(
        f"OK: market_data_snapshot_v1_snapshot action={act_md} path={out_md} sha256={md_sha} "
        f"source_market_data={source_market_data} source_market_data_sha256={source_market_data_sha} matching_day_rows={matching_day_rows}"
    )

    print("OK: done=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
