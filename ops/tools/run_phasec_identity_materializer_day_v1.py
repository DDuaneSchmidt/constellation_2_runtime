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
from constellation_2.common.canonical_fact_store_v1 import (  # noqa: E402
    capture_executed_code_identity,
    finalize_run_ledger,
    mark_attempt_state,
    open_or_create_run_ledger,
    resolve_latest_active_attempt,
    write_invariant_result,
)
from constellation_2.common.c2_risk_policy_loader_v1 import get_per_trade_notional_pct_max_or_fail  # noqa: E402

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()

def _require_truth_root_allowed(truth_root: Path) -> Path:
    pr = truth_root.expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {pr}")
    try:
        pr.relative_to(REPO_ROOT)
        return pr
    except ValueError:
        pass
    try:
        pr.relative_to(RUNTIME_DATA_ROOT)
        return pr
    except ValueError as e:
        raise SystemExit(
            f"FAIL: truth_root not under allowed roots: truth_root={pr} repo_root={REPO_ROOT} runtime_data_root={RUNTIME_DATA_ROOT}"
        ) from e
    return pr


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        return _require_truth_root_allowed(Path(arg))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_allowed(Path(env_root))
    return _require_truth_root_allowed(resolve_truth_root(repo_root=REPO_ROOT))


def _intents_root(truth_root: Path) -> Path:
    return (truth_root / "intents_v1" / "snapshots").resolve()


def _phasec_root(truth_root: Path) -> Path:
    return (truth_root / "phaseC_preflight_v1").resolve()

ENTRY_TRANSFORMER_V1 = (REPO_ROOT / "constellation_2" / "phaseH" / "tools" / "c2_risk_transformer_offline_v1.py").resolve()
EXIT_TRANSFORMER_V2 = (REPO_ROOT / "constellation_2" / "phaseH" / "tools" / "c2_risk_transformer_exit_offline_v2.py").resolve()
PREFLIGHT_WRITER_V2 = (REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v2.py").resolve()
PREFLIGHT_WRITER_V1 = (REPO_ROOT / "constellation_2" / "phaseC" / "tools" / "c2_submit_preflight_offline_v1.py").resolve()

OPTIONS_IDENTITY_HELPER_V1 = (REPO_ROOT / "ops" / "tools" / "run_phasec_options_identity_from_truth_day_v1.py").resolve()

EXPECTED_IDENTITY_FILES = [
    "equity_intent.v1.json",
    "lineage_envelope.v1.json",
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


def _atomic_write_json_replace(path: Path, obj: Dict[str, Any]) -> None:
    data = canonical_json_bytes_v1(obj) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(data)
    os.replace(str(tmp), str(path))


def _ledger_scope_context(truth_root: Path) -> Dict[str, Any]:
    context: Dict[str, Any] = {"sleeve": "GLOBAL", "mode": "GLOBAL", "symbol": "GLOBAL"}
    parts = truth_root.resolve().parts
    for idx, part in enumerate(parts):
        if part != "truth_sleeves":
            continue
        if idx + 2 >= len(parts):
            break
        context["sleeve"] = str(parts[idx + 1])
        context["mode"] = str(parts[idx + 2])
        break
    return context


def _ledger_scope_key(context: Dict[str, Any]) -> str:
    return f"{context.get('sleeve', 'GLOBAL')}__{context.get('mode', 'GLOBAL')}__{context.get('symbol', 'GLOBAL')}"


def _append_fact_ref(ledger: Dict[str, Any], *, fact_type: str, path: Path) -> Dict[str, str]:
    ref = {
        "fact_type": fact_type,
        "path": str(path.resolve()),
        "sha256": _sha256_file(path.resolve()),
    }
    fact_refs = ledger.setdefault("fact_refs", [])
    if not any(item.get("path") == ref["path"] and item.get("sha256") == ref["sha256"] for item in fact_refs):
        fact_refs.append(ref)
    return ref


def _source_ref(*, ref_type: str, path: Path) -> Dict[str, str]:
    return {
        "ref_type": ref_type,
        "path": str(path.resolve()),
        "sha256": _sha256_file(path.resolve()),
    }


def _append_invariant_result(
    *,
    ledger: Dict[str, Any],
    truth_root: Path,
    attempt_id: str,
    invoked_day_utc: str,
    invariant_name: str,
    passed: bool,
    actual: Any,
    expected: Any,
    source_refs: List[Dict[str, str]],
) -> Dict[str, Any]:
    existing_items = ledger.setdefault("invariant_results", [])
    existing_idx: Optional[int] = None
    existing_payload: Optional[Dict[str, Any]] = None
    for idx, item in enumerate(existing_items):
        if str(item.get("invariant_name") or "") != invariant_name:
            continue
        existing_idx = idx
        existing_path = Path(str(item["path"])).resolve()
        existing_payload = _read_json_obj(existing_path)
        break
    merged_passed = bool(passed)
    merged_actual = actual
    merged_expected = expected
    merged_source_refs = list(source_refs)
    if existing_payload is not None:
        existing_passed = bool(existing_payload.get("passed"))
        merged_passed = existing_passed and bool(passed)
        if not existing_passed:
            merged_actual = existing_payload.get("actual")
            merged_expected = existing_payload.get("expected")
        elif not passed:
            merged_actual = actual
            merged_expected = expected
        else:
            merged_actual = existing_payload.get("actual")
            merged_expected = existing_payload.get("expected")
        seen = {(str(ref.get("path") or ""), str(ref.get("sha256") or "")) for ref in merged_source_refs}
        for ref in existing_payload.get("source_refs", []):
            key = (str(ref.get("path") or ""), str(ref.get("sha256") or ""))
            if key in seen:
                continue
            merged_source_refs.append(ref)
            seen.add(key)
    inv_ref = write_invariant_result(
        invariant_payload={
            "schema_id": "invariant_result.v1",
            "schema_version": "v1",
            "invariant_name": invariant_name,
            "invoked_day_utc": invoked_day_utc,
            "passed": merged_passed,
            "actual": merged_actual,
            "expected": merged_expected,
            "source_refs": merged_source_refs,
            "generated_at_utc": str(ledger["generated_at_utc"]),
        },
        truth_root=truth_root,
        attempt_id=attempt_id,
        repo_root=REPO_ROOT,
    )
    if existing_idx is None:
        existing_items.append(inv_ref)
    else:
        existing_items[existing_idx] = inv_ref
    return inv_ref


def _same_day_market_close_sources(
    *,
    truth_root: Path,
    day_utc: str,
    symbol: str,
) -> Tuple[Optional[str], List[Path]]:
    md_root = (truth_root / "market_data_snapshot_v1").resolve()
    manifest = (md_root / "dataset_manifest.json").resolve()
    if not manifest.exists() or not manifest.is_file():
        return None, []
    try:
        manifest_obj = _read_json_obj(manifest)
    except Exception:
        return None, []
    files = manifest_obj.get("files")
    if not isinstance(files, list):
        return None, [manifest]
    year = int(day_utc[0:4])
    target_rel: Optional[str] = None
    target_sha: Optional[str] = None
    for entry in files:
        if not isinstance(entry, dict):
            continue
        if int(entry.get("year", -1)) != year:
            continue
        if str(entry.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        target_rel = str(entry.get("file") or "").strip()
        target_sha = str(entry.get("sha256") or "").strip().lower()
        break
    if not target_rel or not target_sha:
        return None, [manifest]
    year_path = (md_root / target_rel).resolve()
    if not year_path.exists() or not year_path.is_file():
        return None, [manifest]
    if _sha256_file(year_path).lower() != target_sha:
        return None, [manifest, year_path]
    for line in year_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            row = json.loads(s)
        except Exception:
            return None, [manifest, year_path]
        if not isinstance(row, dict):
            return None, [manifest, year_path]
        if str(row.get("timestamp_utc") or "")[:10] != day_utc:
            continue
        close = row.get("close")
        if isinstance(close, (int, float)):
            return str(close), [manifest, year_path]
        if isinstance(close, str) and close.strip():
            return close.strip(), [manifest, year_path]
        return None, [manifest, year_path]
    return None, [manifest, year_path]


def _read_reason_detail_from_veto(veto_path: Path) -> str:
    try:
        veto_obj = _read_json_obj(veto_path)
    except Exception:
        return ""
    return str(veto_obj.get("reason_detail") or "").strip()


def _existing_attempt_ids(out_day_dir: Path) -> List[str]:
    ids: List[str] = []
    if not out_day_dir.exists():
        return ids
    for child in sorted(out_day_dir.iterdir()):
        if not child.is_dir():
            continue
        if not child.name.startswith("attempt_A"):
            continue
        attempt_id = child.name[len("attempt_") :]
        if len(attempt_id) == 5 and attempt_id.startswith("A") and attempt_id[1:].isdigit():
            ids.append(attempt_id)
    return ids


def _next_attempt_id(out_day_dir: Path) -> str:
    existing = _existing_attempt_ids(out_day_dir)
    if not existing:
        return "A0001"
    max_n = max(int(v[1:]) for v in existing)
    return f"A{max_n + 1:04d}"


def _attempt_dir(out_day_dir: Path, attempt_id: str) -> Path:
    return (out_day_dir / f"attempt_{attempt_id}").resolve()


def _attempt_state_path(out_day_dir: Path, attempt_id: str) -> Path:
    return (_attempt_dir(out_day_dir, attempt_id) / "attempt_state.v1.json").resolve()


def _latest_active_attempt_pointer_path(out_day_dir: Path) -> Path:
    return (out_day_dir / "latest_active_attempt.v1.json").resolve()


def _write_attempt_state(
    *,
    out_day_dir: Path,
    attempt_id: str,
    day_utc: str,
    status: str,
    supersedes_attempt_id: Optional[str] = None,
) -> Path:
    state_path = _attempt_state_path(out_day_dir, attempt_id)
    payload: Dict[str, Any] = {
        "schema_id": "phasec_attempt_state.v1",
        "schema_version": "v1",
        "day_utc": day_utc,
        "attempt_id": attempt_id,
        "status": status,
        "attempt_dir": str(_attempt_dir(out_day_dir, attempt_id)),
        "supersedes_attempt_id": supersedes_attempt_id,
    }
    _atomic_write_json_replace(state_path, payload)
    return state_path


def _write_latest_active_attempt_pointer(*, out_day_dir: Path, day_utc: str, attempt_id: str) -> Path:
    pointer_path = _latest_active_attempt_pointer_path(out_day_dir)
    payload: Dict[str, Any] = {
        "schema_id": "phasec_latest_active_attempt.v1",
        "schema_version": "v1",
        "day_utc": day_utc,
        "attempt_id": attempt_id,
        "attempt_dir": str(_attempt_dir(out_day_dir, attempt_id)),
    }
    _atomic_write_json_replace(pointer_path, payload)
    return pointer_path


def _supersede_prior_attempts(*, out_day_dir: Path, day_utc: str, active_attempt_id: str) -> None:
    for prior_attempt_id in _existing_attempt_ids(out_day_dir):
        if prior_attempt_id == active_attempt_id:
            continue
        _write_attempt_state(
            out_day_dir=out_day_dir,
            attempt_id=prior_attempt_id,
            day_utc=day_utc,
            status="SUPERSEDED",
            supersedes_attempt_id=active_attempt_id,
        )


def _write_failclosed_veto(
    *,
    attempt_day_dir: Path,
    day_utc: str,
    eval_time_utc: str,
    intent_hash: str,
    intent_path: Path,
    intent_sha256: str,
    reason_detail: str,
) -> Path:
    veto_path = (attempt_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
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
    candidate_bytes = canonical_json_bytes_v1(veto_obj) + b"\n"
    if veto_path.exists():
        existing_bytes = veto_path.read_bytes()
        if existing_bytes == candidate_bytes:
            return veto_path
        existing_sha = _sha256_bytes(existing_bytes)
        quarantine = veto_path.with_name(f"{veto_path.name}.INVALID_{existing_sha}.json")
        if quarantine.exists():
            quarantine = veto_path.with_name(f"{veto_path.name}.INVALID_{existing_sha}.{os.getpid()}.json")
        os.replace(str(veto_path), str(quarantine))
        print(
            f"WARN: PHASEC_VETO_REFRESHED_STALE day_utc={day_utc} "
            f"old_path={veto_path} quarantined_path={quarantine} sha256={existing_sha}"
        )
    write_file_immutable_v1(path=veto_path, data=candidate_bytes, create_dirs=True)
    return veto_path


def _run(cmd: List[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)


def _materialize_equity_intent(
    *,
    truth_root: Path,
    day_utc: str,
    eval_time_utc: str,
    attempt_day_dir: Path,
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
                str(EXIT_TRANSFORMER_V2),
                "--exposure_intent",
                str(intent_path),
                "--day_utc",
                day_utc,
                "--eval_time_utc",
                eval_time_utc,
                "--out_dir",
                str(transformer_out),
                "--positions_snapshot_path",
                str((truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json").resolve()),
            ]
        else:
            if not str(default_equity_reference_price or "").strip():
                raise MaterializerError("DEFAULT_EQUITY_REFERENCE_PRICE_REQUIRED_FOR_ENTRY_INTENTS")
            cmd = [
                "python3",
                str(ENTRY_TRANSFORMER_V1),
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
                attempt_day_dir=attempt_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        eq_intent_path = (transformer_out / "equity_intent.v1.json").resolve()
        lineage_envelope_path = (transformer_out / "lineage_envelope.v1.json").resolve()
        eq_plan_v2_path = (transformer_out / "equity_order_plan.v2.json").resolve()
        eq_plan_v1_path = (transformer_out / "equity_order_plan.v1.json").resolve()

        if not eq_intent_path.exists():
            detail = "TRANSFORMER_OUTPUT_MISSING: equity_intent.v1.json"
            veto_path = _write_failclosed_veto(
                attempt_day_dir=attempt_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))
        if not lineage_envelope_path.exists():
            detail = "TRANSFORMER_OUTPUT_MISSING: lineage_envelope.v1.json"
            veto_path = _write_failclosed_veto(
                attempt_day_dir=attempt_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        eq_plan_path: Optional[Path] = None
        preflight_cmd: List[str]
        if eq_plan_v2_path.exists():
            eq_plan_path = eq_plan_v2_path
            preflight_cmd = [
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
            ]
        elif eq_plan_v1_path.exists():
            eq_plan_path = eq_plan_v1_path
            preflight_cmd = [
                "python3",
                str(PREFLIGHT_WRITER_V1),
                "--intent",
                str(eq_intent_path),
                "--equity_order_plan",
                str(eq_plan_path),
                "--eval_time_utc",
                eval_time_utc,
                "--out_dir",
                str(preflight_out),
            ]
        else:
            detail = "TRANSFORMER_OUTPUT_MISSING: equity_order_plan.v1.json|equity_order_plan.v2.json"
            veto_path = _write_failclosed_veto(
                attempt_day_dir=attempt_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        res_preflight = _run(preflight_cmd, cwd=REPO_ROOT)
        if res_preflight.returncode != 0:
            detail = (
                "PREFLIGHT_NONZERO_RC:"
                f"rc={res_preflight.returncode};"
                f"stdout={res_preflight.stdout.strip()!r};"
                f"stderr={res_preflight.stderr.strip()!r}"
            )
            veto_path = _write_failclosed_veto(
                attempt_day_dir=attempt_day_dir,
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
            veto_dst = (attempt_day_dir / f"{intent_hash}.veto_record.v1.json").resolve()
            _immutable_copy_file(preflight_veto, veto_dst)
            return ("BLOCKED", str(veto_dst))

        if not preflight_allow.exists():
            detail = "PREFLIGHT_OUTPUT_MISSING: submit_preflight_decision.v1.json"
            veto_path = _write_failclosed_veto(
                attempt_day_dir=attempt_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))

        final_identity_dir = (attempt_day_dir / intent_hash).resolve()
        final_identity_dir.mkdir(parents=True, exist_ok=True)

        _immutable_copy_file(eq_intent_path, final_identity_dir / "equity_intent.v1.json")
        _immutable_copy_file(lineage_envelope_path, final_identity_dir / "lineage_envelope.v1.json")
        preflight_plan_v2 = (preflight_out / "equity_order_plan.v2.json").resolve()
        preflight_plan_v1 = (preflight_out / "equity_order_plan.v1.json").resolve()
        if preflight_plan_v2.exists():
            _immutable_copy_file(preflight_plan_v2, final_identity_dir / "equity_order_plan.v2.json")
        elif preflight_plan_v1.exists():
            _immutable_copy_file(preflight_plan_v1, final_identity_dir / "equity_order_plan.v1.json")
        else:
            detail = "PREFLIGHT_OUTPUT_MISSING: equity_order_plan.v1.json|equity_order_plan.v2.json"
            veto_path = _write_failclosed_veto(
                attempt_day_dir=attempt_day_dir,
                day_utc=day_utc,
                eval_time_utc=eval_time_utc,
                intent_hash=intent_hash,
                intent_path=intent_path,
                intent_sha256=intent_sha256,
                reason_detail=detail,
            )
            return ("BLOCKED", str(veto_path))
        _immutable_copy_file(preflight_out / "mapping_ledger_record.v2.json", final_identity_dir / "mapping_ledger_record.v2.json")
        _immutable_copy_file(preflight_out / "binding_record.v2.json", final_identity_dir / "binding_record.v2.json")
        _immutable_copy_file(preflight_allow, final_identity_dir / "submit_preflight_decision.v1.json")

        allow_dst = (attempt_day_dir / f"{intent_hash}.submit_preflight_decision.v1.json").resolve()
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
    attempt_id = _next_attempt_id(out_day_dir)
    attempt_day_dir = _attempt_dir(out_day_dir, attempt_id)
    attempt_day_dir.mkdir(parents=True, exist_ok=True)
    _write_attempt_state(out_day_dir=out_day_dir, attempt_id=attempt_id, day_utc=day_utc, status="ABORTED")
    ledger_context = _ledger_scope_context(truth_root)
    prior_active = resolve_latest_active_attempt(
        truth_root=truth_root,
        invoked_day_utc=day_utc,
        scope_key=_ledger_scope_key(ledger_context),
    )
    prior_active_attempt_id = None if prior_active is None else str(prior_active.get("attempt_id") or "").strip() or None
    run_id = f"phasec_identity_materializer_day_v1:{day_utc}:{attempt_id}"
    ledger = open_or_create_run_ledger(
        truth_root=truth_root,
        run_id=run_id,
        attempt_id=attempt_id,
        invoked_day_utc=day_utc,
        effective_day_utc=day_utc,
        context=ledger_context,
        executed_code_identity=capture_executed_code_identity(repo_root=REPO_ROOT),
        supersedes_attempt_id=prior_active_attempt_id,
        repo_root=REPO_ROOT,
    )

    try:
        intent_files = _list_intent_files(day_utc, truth_root)
    except Exception as e:
        _append_invariant_result(
            ledger=ledger,
            truth_root=truth_root,
            attempt_id=attempt_id,
            invoked_day_utc=day_utc,
            invariant_name="submission_allowed",
            passed=False,
            actual={"intent_dir_error": str(e)},
            expected={"intent_files_present": True},
            source_refs=[],
        )
        finalize_run_ledger(truth_root=truth_root, ledger_payload=ledger, repo_root=REPO_ROOT)
        mark_attempt_state(
            truth_root=truth_root,
            invoked_day_utc=day_utc,
            attempt_id=attempt_id,
            context=ledger_context,
            status="ABORTED",
            repo_root=REPO_ROOT,
        )
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
            intent_fact_ref = _append_fact_ref(ledger, fact_type="intent_source_ref", path=intent_path)
            intent_source_ref = {
                "ref_type": "intent_source_ref",
                "path": intent_fact_ref["path"],
                "sha256": intent_fact_ref["sha256"],
            }

            schema_id = str(intent_obj.get("schema_id") or "").strip()
            schema_version = str(intent_obj.get("schema_version") or "").strip()
            if schema_id != "exposure_intent" or schema_version != "v1":
                veto_path = _write_failclosed_veto(
                    attempt_day_dir=attempt_day_dir,
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
                    target_pct = _dec_strict(str(intent_obj.get("target_notional_pct") or ""), "target_notional_pct")
                    symbol = str(((intent_obj.get("underlying") or {}) if isinstance(intent_obj.get("underlying"), dict) else {}).get("symbol") or "").strip().upper()
                    same_day_close, close_source_paths = _same_day_market_close_sources(
                        truth_root=truth_root,
                        day_utc=day_utc,
                        symbol=symbol,
                    )
                    close_source_refs: List[Dict[str, str]] = [intent_source_ref]
                    for source_path in close_source_paths:
                        market_fact_ref = _append_fact_ref(ledger, fact_type="market_close_source_ref", path=source_path)
                        close_source_refs.append(
                            {
                                "ref_type": "market_close_source_ref",
                                "path": market_fact_ref["path"],
                                "sha256": market_fact_ref["sha256"],
                            }
                        )
                    same_day_market_close_passed = False
                    actual_close: Dict[str, Any] = {
                        "symbol": symbol,
                        "resolved_close": same_day_close,
                        "default_equity_reference_price": str(args.default_equity_reference_price or "").strip() or None,
                    }
                    if target_pct > Decimal("0") and same_day_close is not None and str(args.default_equity_reference_price or "").strip():
                        try:
                            same_day_market_close_passed = (
                                _dec_strict(str(same_day_close), "same_day_close")
                                == _dec_strict(str(args.default_equity_reference_price).strip(), "default_equity_reference_price")
                            )
                        except MaterializerError:
                            same_day_market_close_passed = False
                    _append_invariant_result(
                        ledger=ledger,
                        truth_root=truth_root,
                        attempt_id=attempt_id,
                        invoked_day_utc=day_utc,
                        invariant_name="same_day_market_close_exists",
                        passed=(same_day_market_close_passed if target_pct > Decimal("0") else True),
                        actual=actual_close,
                        expected={"day_utc": day_utc, "symbol": symbol, "same_day_close_required": bool(target_pct > Decimal("0"))},
                        source_refs=close_source_refs,
                    )
                    status, out_path = _materialize_equity_intent(
                        truth_root=truth_root,
                        day_utc=day_utc,
                        eval_time_utc=eval_time_utc,
                        attempt_day_dir=attempt_day_dir,
                        intent_path=intent_path,
                        intent_obj=intent_obj,
                        intent_hash=intent_hash,
                        intent_sha256=intent_sha256,
                        default_equity_reference_price=str(args.default_equity_reference_price or ""),
                    )
                    if status == "RELEASED":
                        identity_dir = Path(out_path).resolve()
                        lineage_path = (identity_dir / "lineage_envelope.v1.json").resolve()
                        plan_path = (identity_dir / "equity_order_plan.v2.json").resolve()
                        decision_path = (identity_dir / "submit_preflight_decision.v1.json").resolve()
                        lineage_ref = _append_fact_ref(ledger, fact_type="lineage_envelope_ref", path=lineage_path)
                        plan_ref = _append_fact_ref(ledger, fact_type="equity_order_plan_ref", path=plan_path)
                        decision_ref = _append_fact_ref(ledger, fact_type="submission_decision_ref", path=decision_path)
                        lineage_source_refs = [intent_source_ref, _source_ref(ref_type="lineage_envelope_ref", path=lineage_path)]
                        _append_invariant_result(
                            ledger=ledger,
                            truth_root=truth_root,
                            attempt_id=attempt_id,
                            invoked_day_utc=day_utc,
                            invariant_name="lineage_complete",
                            passed=True,
                            actual={"lineage_envelope_path": lineage_ref["path"]},
                            expected={"engine_id": True, "source_intent_id": True, "intent_sha256": True},
                            source_refs=lineage_source_refs,
                        )
                        engine = intent_obj.get("engine") if isinstance(intent_obj.get("engine"), dict) else {}
                        engine_id = str(engine.get("engine_id") or "").strip()
                        cap_value = get_per_trade_notional_pct_max_or_fail(engine_id)
                        _append_invariant_result(
                            ledger=ledger,
                            truth_root=truth_root,
                            attempt_id=attempt_id,
                            invoked_day_utc=day_utc,
                            invariant_name="intent_within_per_trade_cap",
                            passed=True,
                            actual={"target_notional_pct": str(intent_obj.get("target_notional_pct") or "")},
                            expected={"per_trade_notional_pct_max": cap_value},
                            source_refs=[intent_source_ref, _source_ref(ref_type="equity_order_plan_ref", path=plan_path)],
                        )
                        _append_invariant_result(
                            ledger=ledger,
                            truth_root=truth_root,
                            attempt_id=attempt_id,
                            invoked_day_utc=day_utc,
                            invariant_name="submission_allowed",
                            passed=True,
                            actual={"decision": "ALLOW", "path": decision_ref["path"]},
                            expected={"decision": "ALLOW"},
                            source_refs=[_source_ref(ref_type="submission_decision_ref", path=decision_path)],
                        )
                    else:
                        veto_path = Path(out_path).resolve()
                        veto_ref = _append_fact_ref(ledger, fact_type="submission_decision_ref", path=veto_path)
                        reason_detail = _read_reason_detail_from_veto(veto_path)
                        if "PER_TRADE_NOTIONAL_CAP_EXCEEDED" in reason_detail:
                            engine = intent_obj.get("engine") if isinstance(intent_obj.get("engine"), dict) else {}
                            engine_id = str(engine.get("engine_id") or "").strip()
                            cap_value = get_per_trade_notional_pct_max_or_fail(engine_id)
                            _append_invariant_result(
                                ledger=ledger,
                                truth_root=truth_root,
                                attempt_id=attempt_id,
                                invoked_day_utc=day_utc,
                                invariant_name="intent_within_per_trade_cap",
                                passed=False,
                                actual={"target_notional_pct": str(intent_obj.get("target_notional_pct") or ""), "reason_detail": reason_detail},
                                expected={"per_trade_notional_pct_max": cap_value},
                                source_refs=[intent_source_ref, _source_ref(ref_type="submission_decision_ref", path=veto_path)],
                            )
                        if "lineage_envelope.v1.json" in reason_detail or "LINEAGE_" in reason_detail:
                            _append_invariant_result(
                                ledger=ledger,
                                truth_root=truth_root,
                                attempt_id=attempt_id,
                                invoked_day_utc=day_utc,
                                invariant_name="lineage_complete",
                                passed=False,
                                actual={"reason_detail": reason_detail},
                                expected={"lineage_envelope_present": True},
                                source_refs=[intent_source_ref, _source_ref(ref_type="submission_decision_ref", path=veto_path)],
                            )
                        _append_invariant_result(
                            ledger=ledger,
                            truth_root=truth_root,
                            attempt_id=attempt_id,
                            invoked_day_utc=day_utc,
                            invariant_name="submission_allowed",
                            passed=False,
                            actual={"decision": "VETO", "path": veto_ref["path"], "reason_detail": reason_detail},
                            expected={"decision": "ALLOW"},
                            source_refs=[_source_ref(ref_type="submission_decision_ref", path=veto_path)],
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
                            str(attempt_day_dir),
                            "--truth_root",
                            str(truth_root),
                        ],
                        cwd=REPO_ROOT,
                    )
                    if res.returncode == 0:
                        status = "RELEASED"
                        out_path = str((attempt_day_dir / intent_hash).resolve())
                    else:
                        status = "BLOCKED"
                        out_path = str((attempt_day_dir / f"{intent_hash}.veto_record.v1.json").resolve())
                else:
                    veto_path = _write_failclosed_veto(
                        attempt_day_dir=attempt_day_dir,
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
                    attempt_day_dir=attempt_day_dir,
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
        finalize_run_ledger(truth_root=truth_root, ledger_payload=ledger, repo_root=REPO_ROOT)
        mark_attempt_state(
            truth_root=truth_root,
            invoked_day_utc=day_utc,
            attempt_id=attempt_id,
            context=ledger_context,
            status="ABORTED",
            repo_root=REPO_ROOT,
        )
        _write_attempt_state(out_day_dir=out_day_dir, attempt_id=attempt_id, day_utc=day_utc, status="ABORTED")
        print(
            f"STATUS=FAIL day_utc={day_utc} attempt_id={attempt_id} released={released} blocked={blocked} failed={failed}",
            file=sys.stderr,
        )
        return 5

    finalize_run_ledger(truth_root=truth_root, ledger_payload=ledger, repo_root=REPO_ROOT)
    if released > 0:
        if prior_active_attempt_id and prior_active_attempt_id != attempt_id:
            mark_attempt_state(
                truth_root=truth_root,
                invoked_day_utc=day_utc,
                attempt_id=prior_active_attempt_id,
                context=ledger_context,
                status="SUPERSEDED",
                repo_root=REPO_ROOT,
            )
        mark_attempt_state(
            truth_root=truth_root,
            invoked_day_utc=day_utc,
            attempt_id=attempt_id,
            context=ledger_context,
            status="ACTIVE",
            repo_root=REPO_ROOT,
        )
        _supersede_prior_attempts(out_day_dir=out_day_dir, day_utc=day_utc, active_attempt_id=attempt_id)
        _write_attempt_state(out_day_dir=out_day_dir, attempt_id=attempt_id, day_utc=day_utc, status="ACTIVE")
        _write_latest_active_attempt_pointer(out_day_dir=out_day_dir, day_utc=day_utc, attempt_id=attempt_id)
    else:
        mark_attempt_state(
            truth_root=truth_root,
            invoked_day_utc=day_utc,
            attempt_id=attempt_id,
            context=ledger_context,
            status="ABORTED",
            repo_root=REPO_ROOT,
        )
        _write_attempt_state(out_day_dir=out_day_dir, attempt_id=attempt_id, day_utc=day_utc, status="ABORTED")
    print(
        f"OK: PHASEC_IDENTITY_MATERIALIZER_DAY_V1 day_utc={day_utc} attempt_id={attempt_id} "
        f"released={released} blocked={blocked} failed={failed} out_dir={attempt_day_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
