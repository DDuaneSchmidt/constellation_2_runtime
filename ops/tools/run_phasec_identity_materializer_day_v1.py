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
from typing import Any, Dict, List, Optional, Tuple
from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402

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
        raise SystemExit(f"FAIL: truth_root not under repo_root: truth_root={pr} repo_root={REPO_ROOT}") from e
    return pr


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        return _require_truth_root_under_repo(Path(arg))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_under_repo(Path(env_root))
    return _require_truth_root_under_repo(resolve_truth_root(repo_root=REPO_ROOT))


def _intents_root(truth_root: Path) -> Path:
    return (truth_root / "intents_v1" / "snapshots").resolve()


def _phasec_root(truth_root: Path) -> Path:
    return (truth_root / "phaseC_preflight_v1").resolve()

ENTRY_TRANSFORMER_V2 = (REPO_ROOT / "constellation_2" / "phaseH" / "tools" / "c2_risk_transformer_offline_v2.py").resolve()
EXIT_TRANSFORMER_V3 = (REPO_ROOT / "constellation_2" / "phaseH" / "tools" / "c2_risk_transformer_exit_offline_v3.py").resolve()
PREFLIGHT_WRITER_V2 = (REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v2.py").resolve()

OPTIONS_IDENTITY_HELPER_V1 = (REPO_ROOT / "ops" / "tools" / "run_phasec_options_identity_from_truth_day_v1.py").resolve()

EXPECTED_IDENTITY_FILES = [
    "equity_intent.v1.json",
    "equity_order_plan.v2.json",
    "mapping_ledger_record.v2.json",
    "binding_record.v2.json",
]

SOURCE_REASON_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"


class MaterializerError(Exception):
    pass


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise MaterializerError(f"INPUT_FILE_MISSING: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise MaterializerError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _parse_day(day_utc: str) -> str:
    d = str(day_utc or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise MaterializerError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _dec_strict(value: str, name: str) -> Decimal:
    if not isinstance(value, str) or not value.strip():
        raise MaterializerError(f"DECIMAL_STRING_REQUIRED: {name}")
    try:
        return Decimal(value.strip())
    except InvalidOperation as e:
        raise MaterializerError(f"DECIMAL_PARSE_FAILED: {name}={value!r}") from e


def _list_intent_files(day_utc: str, truth_root: Path) -> List[Path]:
    day_dir = (_intents_root(truth_root) / day_utc).resolve()
    if not day_dir.exists() or not day_dir.is_dir():
        raise MaterializerError(f"INTENTS_DAY_DIR_MISSING: {day_dir}")
    files = sorted(p for p in day_dir.iterdir() if p.is_file() and p.name.endswith(".json"))
    if not files:
        raise MaterializerError(f"INTENTS_DAY_DIR_EMPTY: {day_dir}")
    return files


def _intent_hash_from_path(intent_path: Path) -> str:
    return _sha256_file(intent_path)


def _immutable_write_json(path: Path, obj: Dict[str, Any]) -> None:
    data = canonical_json_bytes_v1(obj) + b"\n"
    write_file_immutable_v1(path=path, data=data, create_dirs=True)


def _immutable_copy_file(src: Path, dst: Path) -> None:
    if not src.exists() or not src.is_file():
        raise MaterializerError(f"SOURCE_FILE_MISSING: {src}")
    write_file_immutable_v1(path=dst, data=src.read_bytes(), create_dirs=True)


def _write_failclosed_veto(
    *,
    out_day_dir: Path,
    day_utc: str,
    eval_time_utc: str,
    intent_hash: str,
    intent_path: Path,
    intent_sha256: str,
    reason_detail: str,
) -> Path:
    veto_path = (out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
    veto_obj: Dict[str, Any] = {
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
                "sha256": intent_sha256,
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


def _run(cmd: List[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def _materialize_equity_intent(
    *,
    truth_root: Path,
    day_utc: str,
    eval_time_utc: str,
    out_day_dir: Path,
    intent_path: Path,
    intent_obj: Dict[str, Any],
    intent_hash: str,
    intent_sha256: str,
    default_equity_reference_price: str,
) -> Tuple[str, str]:
    target_pct = _dec_strict(str(intent_obj.get("target_notional_pct") or ""), "target_notional_pct")

    with tempfile.TemporaryDirectory(prefix=f"phasec_materializer_{day_utc}_{intent_hash[:12]}_") as td:
        temp_root = Path(td).resolve()
        transformer_out = (temp_root / "transformer_out").resolve()
        preflight_out = (temp_root / "preflight_out").resolve()

        if target_pct == Decimal("0"):
            cmd = [
                "python3",
                str(EXIT_TRANSFORMER_V3),
                "--exposure_intent",
                str(intent_path),
                "--day_utc",
                day_utc,
                "--eval_time_utc",
                eval_time_utc,
                "--out_dir",
                str(transformer_out),
                "--truth_root",
                str(truth_root),
            ]
        else:
            if not str(default_equity_reference_price or "").strip():
                raise MaterializerError("DEFAULT_EQUITY_REFERENCE_PRICE_REQUIRED_FOR_ENTRY_INTENTS")
            cmd = [
                "python3",
                str(ENTRY_TRANSFORMER_V2),
                "--exposure_intent",
                str(intent_path),
                "--day_utc",
                day_utc,
                "--eval_time_utc",
                eval_time_utc,
                "--out_dir",
                str(transformer_out),
                "--equity_reference_price",
                str(default_equity_reference_price).strip(),
                "--truth_root",
                str(truth_root),
            ]

        res_transform = _run(cmd, cwd=REPO_ROOT)
        if res_transform.returncode != 0:
            detail = (
                "TRANSFORMER_NONZERO_RC:"
                f"rc={res_transform.returncode};"
                f"stdout={res_transform.stdout.strip()!r};"
                f"stderr={res_transform.stderr.strip()!r}"
            )
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        eq_intent_path = (transformer_out / "equity_intent.v1.json").resolve()
        eq_plan_path = (transformer_out / "equity_order_plan.v2.json").resolve()

        if not eq_intent_path.exists():
            detail = "TRANSFORMER_OUTPUT_MISSING: equity_intent.v1.json"
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        if not eq_plan_path.exists():
            detail = "TRANSFORMER_OUTPUT_MISSING: equity_order_plan.v2.json"
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        res_preflight = _run(
            [
                "python3",
                str(PREFLIGHT_WRITER_V2),
                "--intent",
                str(eq_intent_path),
                "--equity_order_plan",
                str(eq_plan_path),
                "--eval_time_utc",
                eval_time_utc,
                "--out_dir",
                str(preflight_out),
            ],
            cwd=REPO_ROOT,
        )
        if res_preflight.returncode != 0:
            detail = (
                "PREFLIGHT_NONZERO_RC:"
                f"rc={res_preflight.returncode};"
                f"stdout={res_preflight.stdout.strip()!r};"
                f"stderr={res_preflight.stderr.strip()!r}"
            )
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        preflight_allow = (preflight_out / "submit_preflight_decision.v1.json").resolve()
        preflight_veto = (preflight_out / "veto_record.v1.json").resolve()

        if preflight_veto.exists():
            veto_dst = (out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
            _immutable_copy_file(preflight_veto, veto_dst)
            return ("BLOCKED", str(veto_dst))

        if not preflight_allow.exists():
            detail = "PREFLIGHT_OUTPUT_MISSING: submit_preflight_decision.v1.json"
            veto_path = _write_failclosed_veto(
                out_day_dir=out_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        final_identity_dir = (out_day_dir / intent_hash).resolve()
        final_identity_dir.mkdir(parents=True, exist_ok=True)

        _immutable_copy_file(eq_intent_path, final_identity_dir / "equity_intent.v1.json")
        _immutable_copy_file(preflight_out / "equity_order_plan.v2.json", final_identity_dir / "equity_order_plan.v2.json")
        _immutable_copy_file(preflight_out / "mapping_ledger_record.v2.json", final_identity_dir / "mapping_ledger_record.v2.json")
        _immutable_copy_file(preflight_out / "binding_record.v2.json", final_identity_dir / "binding_record.v2.json")
        _immutable_copy_file(preflight_allow, final_identity_dir / "submit_preflight_decision.v1.json")

        allow_dst = (out_day_dir / f"{intent_hash}.submit_preflight_decision.v1.json").resolve()
        _immutable_copy_file(preflight_allow, allow_dst)

        return ("RELEASED", str(final_identity_dir))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_phasec_identity_materializer_day_v1",
        description="Materialize same-day Phase C identity directories and flat preflight decisions from daily intents.",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--eval_time_utc", required=True, help="Deterministic evaluation timestamp with Z suffix")
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")    
    ap.add_argument(
        "--default_equity_reference_price",
        default="",
        help="Required for LONG_EQUITY entry intents (target_notional_pct > 0). Applied to all equity entry intents for the run.",
    )
    args = ap.parse_args(argv)

    truth_root = _resolve_truth_root(args.truth_root)
    day_utc = _parse_day(str(args.day_utc))
    eval_time_utc = str(args.eval_time_utc or "").strip()
    if not eval_time_utc.endswith("Z"):
        raise SystemExit("FAIL: EVAL_TIME_UTC_MUST_END_WITH_Z")

    out_day_dir = (_phasec_root(truth_root) / day_utc).resolve()
    out_day_dir.mkdir(parents=True, exist_ok=True)

    try:
        intent_files = _list_intent_files(day_utc, truth_root)
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2

    released = 0
    blocked = 0
    failed = 0

    for intent_path in intent_files:
        try:
            intent_obj = _read_json_obj(intent_path)
            intent_hash = _intent_hash_from_path(intent_path)
            intent_sha256 = intent_hash

            schema_id = str(intent_obj.get("schema_id") or "").strip()
            schema_version = str(intent_obj.get("schema_version") or "").strip()
            if schema_id != "exposure_intent" or schema_version != "v1":
                veto_path = _write_failclosed_veto(
                    out_day_dir=out_day_dir,
                    day_utc=day_utc,
                    eval_time_utc=eval_time_utc,
                    intent_hash=intent_hash,
                    intent_path=intent_path,
                    intent_sha256=intent_sha256,
                    reason_detail=f"UNSUPPORTED_INTENT_SCHEMA_FOR_PHASEC_IDENTITY_MATERIALIZER: schema_id={schema_id} schema_version={schema_version}",
                )
                status = "BLOCKED"
                out_path = str(veto_path)
            else:
                exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
                if exposure_type == "LONG_EQUITY":
                    status, out_path = _materialize_equity_intent(
                        truth_root=truth_root,
                        day_utc=day_utc,
                        eval_time_utc=eval_time_utc,
                        out_day_dir=out_day_dir,
                        intent_path=intent_path,
                        intent_obj=intent_obj,
                        intent_hash=intent_hash,
                        intent_sha256=intent_sha256,
                        default_equity_reference_price=str(args.default_equity_reference_price or ""),
                    )
                elif exposure_type == "SHORT_VOL_DEFINED":
                    res = _run(
                        [
                            "python3",
                            str(OPTIONS_IDENTITY_HELPER_V1),
                            "--day_utc",
                            day_utc,
                            "--eval_time_utc",
                            eval_time_utc,
                            "--intent_path",
                            str(intent_path),
                            "--out_day_dir",
                            str(out_day_dir),
                            "--truth_root",
                            str(truth_root),
                        ],
                        cwd=REPO_ROOT,
                    )
                    if res.returncode == 0:
                        status = "RELEASED"
                        out_path = str((out_day_dir / intent_hash).resolve())
                    else:
                        status = "BLOCKED"
                        out_path = str((out_day_dir / f"{intent_hash}.veto_record.v1.json").resolve())
                else:
                    veto_path = _write_failclosed_veto(
                        out_day_dir=out_day_dir,
                        day_utc=day_utc,
                        eval_time_utc=eval_time_utc,
                        intent_hash=intent_hash,
                        intent_path=intent_path,
                        intent_sha256=intent_sha256,
                        reason_detail=f"UNSUPPORTED_EXPOSURE_TYPE_FOR_PHASEC_IDENTITY_MATERIALIZER: {exposure_type}",
                    )
                    status = "BLOCKED"
                    out_path = str(veto_path)

            if status == "RELEASED":
                released += 1
                print(f"RELEASED: intent_hash={intent_hash} path={out_path}")
            else:
                blocked += 1
                print(f"BLOCKED: intent_hash={intent_hash} path={out_path}")

        except MaterializerError as e:
            try:
                veto_path = _write_failclosed_veto(
                    out_day_dir=out_day_dir,
                    day_utc=day_utc,
                    eval_time_utc=eval_time_utc,
                    intent_hash=intent_hash,
                    intent_path=intent_path,
                    intent_sha256=intent_sha256,
                    reason_detail=str(e),
                )
                blocked += 1
                print(f"BLOCKED: intent_hash={intent_hash} path={str(veto_path)}")
            except Exception as veto_err:
                failed += 1
                print(
                    f"FAIL: MATERIALIZER_VETO_WRITE_FAILED intent_file={intent_path} "
                    f"err={e} veto_err={veto_err}",
                    file=sys.stderr,
                )
        except ImmutableWriteError as e:
            failed += 1
            print(f"FAIL: IMMUTABLE_WRITE intent_file={intent_path} err={e}", file=sys.stderr)
        except Exception as e:
            failed += 1
            print(f"FAIL: INTENT_PROCESSING_FAILED intent_file={intent_path} err={e}", file=sys.stderr)

    if failed > 0:
        print(
            f"STATUS=FAIL day_utc={day_utc} released={released} blocked={blocked} failed={failed}",
            file=sys.stderr,
        )
        return 5

    print(f"OK: PHASEC_IDENTITY_MATERIALIZER_DAY_V1 day_utc={day_utc} released={released} blocked={blocked} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
