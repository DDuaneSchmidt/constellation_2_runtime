#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1  # noqa: E402
from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()


def _require_truth_root_under_repo(truth_root: Path) -> Path:
    pr = truth_root.expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {pr}")
    try:
        pr.relative_to(REPO_ROOT)
    except ValueError as e:
        raise SystemExit(
            f"FAIL: truth_root not under repo_root: truth_root={pr} repo_root={REPO_ROOT}"
        ) from e
    return pr


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        return _require_truth_root_under_repo(Path(arg))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_under_repo(Path(env_root))
    return _require_truth_root_under_repo(resolve_truth_root(repo_root=REPO_ROOT))

MAP_VERTICAL_V1 = (REPO_ROOT / "constellation_2" / "phaseA" / "tools" / "c2_map_vertical_v1.py").resolve()
PREFLIGHT_OPTIONS_V1 = (REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v1.py").resolve()

SOURCE_REASON_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"


class OptionsIdentityError(Exception):
    pass


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise OptionsIdentityError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise OptionsIdentityError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _immutable_write_json(path: Path, obj: Dict[str, Any]) -> None:
    data = canonical_json_bytes_v1(obj) + b"\n"
    write_file_immutable_v1(path=path, data=data, create_dirs=True)


def _immutable_copy(src: Path, dst: Path) -> None:
    if not src.exists() or not src.is_file():
        raise OptionsIdentityError(f"SOURCE_FILE_MISSING: {src}")
    write_file_immutable_v1(path=dst, data=src.read_bytes(), create_dirs=True)


def _write_failclosed_veto(
    *,
    out_day_dir: Path,
    day_utc: str,
    eval_time_utc: str,
    intent_hash: str,
    intent_path: Path,
    reason_detail: str,
) -> Path:
    veto_path = (out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
    veto_obj = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "boundary": "SUBMIT",
        "observed_at_utc": eval_time_utc,
        "reason_code": SOURCE_REASON_FAIL_CLOSED,
        "reason_detail": reason_detail,
        "inputs": {
            "intent_hash": intent_hash,
            "plan_hash": None,
            "chain_snapshot_hash": None,
            "freshness_cert_hash": None,
        },
        "input_manifest": [
            {
                "type": "intent",
                "path": str(intent_path.resolve()),
                "sha256": _sha256_file(intent_path),
                "day_utc": day_utc,
                "producer": "intents_v1",
            }
        ],
        "pointers": [],
        "upstream_hash": intent_hash,
        "canonical_json_hash": None,
    }
    _immutable_write_json(veto_path, veto_obj)
    return veto_path


def _find_latest_snapshot_and_cert(truth_root: Path, day_utc: str, symbol: str) -> Tuple[Path, Path]:
    root = (truth_root / "options_chain_snapshot_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        raise OptionsIdentityError(f"OPTIONS_SNAPSHOT_ROOT_MISSING: {root}")

    candidates = []
    for d in sorted(root.iterdir()):
        snap = (d / "options_chain_snapshot.v1.json").resolve()
        cert = (d / "freshness_certificate.v1.json").resolve()
        if not snap.exists() or not cert.exists():
            continue
        try:
            snap_obj = _read_json_obj(snap)
        except Exception:
            continue
        underlying = snap_obj.get("underlying")
        if not isinstance(underlying, dict):
            continue
        if str(underlying.get("symbol") or "").strip().upper() == symbol.upper():
            candidates.append((snap, cert))

    if not candidates:
        raise OptionsIdentityError(f"SAME_DAY_OPTIONS_TRUTH_MISSING: day={day_utc} symbol={symbol}")
    return candidates[-1]


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def _parse_decimal_str(value: str, *, label: str) -> Decimal:
    s = str(value or "").strip()
    if not s:
        raise OptionsIdentityError(f"DECIMAL_STRING_REQUIRED: {label}")
    try:
        return Decimal(s)
    except InvalidOperation as e:
        raise OptionsIdentityError(f"DECIMAL_PARSE_FAILED: {label}={value!r}") from e


def _decimal_quantum_str(value: str, *, label: str) -> str:
    d = _parse_decimal_str(value, label=label)
    if d <= Decimal("0"):
        raise OptionsIdentityError(f"DECIMAL_MUST_BE_POSITIVE: {label}={value!r}")
    normalized = d.normalize()
    exponent = normalized.as_tuple().exponent
    if exponent >= 0:
        return "1"
    return format(Decimal(1).scaleb(exponent), "f")


def _iter_price_strings_from_snapshot(snap_obj: Dict[str, Any]) -> Iterable[str]:
    contracts = snap_obj.get("contracts")
    if not isinstance(contracts, list):
        raise OptionsIdentityError("SNAPSHOT_CONTRACTS_NOT_LIST")

    found = 0
    for idx, contract in enumerate(contracts):
        if not isinstance(contract, dict):
            continue
        for field in ("bid", "ask"):
            raw = contract.get(field)
            if raw is None:
                continue
            s = str(raw).strip()
            if not s:
                continue
            found += 1
            yield s

    if found <= 0:
        raise OptionsIdentityError("NO_PRICE_FIELDS_AVAILABLE_TO_DERIVE_TICK_SIZE")


def _derive_tick_size_from_snapshot(snap_obj: Dict[str, Any]) -> str:
    tick_sizes = set()
    for idx, price_str in enumerate(_iter_price_strings_from_snapshot(snap_obj)):
        tick_sizes.add(_decimal_quantum_str(price_str, label=f"snapshot_price[{idx}]"))

    if not tick_sizes:
        raise OptionsIdentityError("TICK_SIZE_DERIVATION_EMPTY")

    ordered = sorted(tick_sizes, key=lambda s: _parse_decimal_str(s, label=f"tick_size_candidate:{s}"))
    return ordered[0]

def _materialize(
    *,
    truth_root: Path,
    day_utc: str,
    eval_time_utc: str,
    intent_path: Path,
    out_day_dir: Path,
) -> Tuple[str, str]:

    intent_obj = _read_json_obj(intent_path)
    intent_hash = _sha256_file(intent_path)

    exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
    if exposure_type != "SHORT_VOL_DEFINED":
        raise OptionsIdentityError(f"UNSUPPORTED_OPTIONS_EXPOSURE_TYPE: {exposure_type}")

    underlying = intent_obj.get("underlying")
    if not isinstance(underlying, dict):
        raise OptionsIdentityError("INTENT_UNDERLYING_MISSING")
    symbol = str(underlying.get("symbol") or "").strip().upper()
    if not symbol:
        raise OptionsIdentityError("INTENT_SYMBOL_MISSING")

    try:
        snap_path, cert_path = _find_latest_snapshot_and_cert(truth_root, day_utc, symbol)
    except Exception as e:
        veto_path = _write_failclosed_veto(
            out_day_dir=out_day_dir,
            day_utc=day_utc,
            eval_time_utc=eval_time_utc,
            intent_hash=intent_hash,
            intent_path=intent_path,
            reason_detail=str(e),
        )
        return ("BLOCKED", str(veto_path))

    snap_obj = _read_json_obj(snap_path)
    cert_obj = _read_json_obj(cert_path)
    snap_hash = str(snap_obj.get("canonical_json_hash") or "").strip()
    cert_hash = str(cert_obj.get("canonical_json_hash") or "").strip()
    if not snap_hash:
        raise OptionsIdentityError(f"SNAPSHOT_CANONICAL_HASH_MISSING: {snap_path}")
    if not cert_hash:
        raise OptionsIdentityError(f"FRESHNESS_CANONICAL_HASH_MISSING: {cert_path}")

    tick_size = _derive_tick_size_from_snapshot(snap_obj)

    with tempfile.TemporaryDirectory(prefix=f"phasec_options_{day_utc}_{intent_hash[:12]}_") as td:
        temp_root = Path(td).resolve()
        phasea_root = (temp_root / "phasea_cwd").resolve()
        map_out = (phasea_root / "constellation_2" / "phaseA" / "outputs").resolve()
        run_id = f"phasec_options_{day_utc}_{intent_hash}"
        run_dir = (map_out / run_id).resolve()
        preflight_out = (temp_root / "preflight_out").resolve()

        map_out.mkdir(parents=True, exist_ok=True)

        map_cmd = [
            "python3",
            "-m",
            "constellation_2.phaseA.tools.c2_map_vertical_v1",
            "--intent",
            str(intent_path.resolve()),
            "--chain",
            str(snap_path.resolve()),
            "--freshness",
            str(cert_path.resolve()),
            "--now-utc",
            str(eval_time_utc),
            "--tick-size",
            tick_size,
            "--run-id",
            run_id,
        ]
        res_map = _run(map_cmd, cwd=phasea_root)
        if res_map.returncode != 0:
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail=(
                    "OPTIONS_MAP_FAILED:"
                    f"rc={res_map.returncode};"
                    f"tick_size={tick_size!r};"
                    f"stdout={res_map.stdout.strip()!r};"
                    f"stderr={res_map.stderr.strip()!r}"
                ),
            )
            return ("BLOCKED", str(veto_path))

        order_plan = (run_dir / "order_plan.v1.json").resolve()
        mapping = (run_dir / "mapping_ledger_record.v1.json").resolve()
        if not order_plan.exists() or not mapping.exists():
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail="OPTIONS_MAP_OUTPUT_MISSING",
            )
            return ("BLOCKED", str(veto_path))

        preflight_cmd = [
            "python3",
            str(PREFLIGHT_OPTIONS_V1),
            "--intent",
            str(intent_path.resolve()),
            "--order_plan",
            str(order_plan),
            "--mapping_ledger_record",
            str(mapping),
            "--chain_snapshot",
            str(snap_path.resolve()),
            "--freshness_cert",
            str(cert_path.resolve()),
            "--eval_time_utc",
            str(eval_time_utc),
            "--out_dir",
            str(preflight_out),
        ]
        res_pf = _run(preflight_cmd, cwd=REPO_ROOT)
        if res_pf.returncode != 0:
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail=(
                    "OPTIONS_PREFLIGHT_FAILED:"
                    f"rc={res_pf.returncode};"
                    f"stdout={res_pf.stdout.strip()!r};"
                    f"stderr={res_pf.stderr.strip()!r}"
                ),
            )
            return ("BLOCKED", str(veto_path))

        veto = (preflight_out / "veto_record.v1.json").resolve()
        allow = (preflight_out / "submit_preflight_decision.v1.json").resolve()
        binding_v1 = (preflight_out / "binding_record.v1.json").resolve()

        if veto.exists():
            veto_dst = (out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
            _immutable_copy(veto, veto_dst)
            return ("BLOCKED", str(veto_dst))

        if not allow.exists() or not binding_v1.exists():
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                reason_detail="OPTIONS_PREFLIGHT_OUTPUT_MISSING",
            )
            return ("BLOCKED", str(veto_path))

        final_identity_dir = (out_day_dir / intent_hash).resolve()
        final_identity_dir.mkdir(parents=True, exist_ok=True)

        _immutable_copy(order_plan, final_identity_dir / "order_plan.v1.json")
        _immutable_copy(mapping, final_identity_dir / "mapping_ledger_record.v1.json")
        _immutable_copy(binding_v1, final_identity_dir / "binding_record.v1.json")
        _immutable_copy(allow, final_identity_dir / "submit_preflight_decision.v1.json")

        allow_dst = (out_day_dir / f"{intent_hash}.submit_preflight_decision.v1.json").resolve()
        _immutable_copy(allow, allow_dst)

        return ("RELEASED", str(final_identity_dir))


def main() -> int:
    ap = argparse.ArgumentParser(description="Materialize same-day Phase C options identities from governed truth.")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--intent_path", required=True)
    ap.add_argument("--out_day_dir", required=True)
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")
    args = ap.parse_args()

    truth_root = _resolve_truth_root(args.truth_root)

    try:
        status, path = _materialize(
            truth_root=truth_root,
            day_utc=str(args.day_utc).strip(),
            eval_time_utc=str(args.eval_time_utc).strip(),
            intent_path=Path(str(args.intent_path)).expanduser().resolve(),
            out_day_dir=Path(str(args.out_day_dir)).expanduser().resolve(),
        )
        print(f"{status}: {path}")
        return 0 if status == "RELEASED" else 2
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE: {e}", file=sys.stderr)
        return 2
    except OptionsIdentityError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
