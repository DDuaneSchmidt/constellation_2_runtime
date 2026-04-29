#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_execution_authority_v1 import resolve_governed_paper_execution_profile
from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerConnectionSpec, BrokerSubmitResult
from constellation_2.phaseD.adapters.ib_paper_adapter_v2 import IBPaperAdapterV2
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DEFAULT_EXECUTION_ROOT = Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER")
DEFAULT_CANONICAL_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
EXPECTED_ENVIRONMENT = "PAPER"
EXPECTED_ACCOUNT = "DUO847203"


class PaperCancelError(Exception):
    pass


@dataclass(frozen=True)
class CancelContext:
    day_utc: str
    submission_id: str
    execution_root: Path
    canonical_truth_root: Path
    produced_utc: str


CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]
AdapterFactory = Callable[[BrokerConnectionSpec], Any]


def _utc_now_isoz() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PaperCancelError(f"ARTIFACT_MISSING:path={path}") from exc
    except Exception as exc:
        raise PaperCancelError(f"ARTIFACT_READ_FAILED:path={path}:error={type(exc).__name__}") from exc
    if not isinstance(obj, dict):
        raise PaperCancelError(f"ARTIFACT_NOT_OBJECT:path={path}")
    return obj


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _submission_dir(ctx: CancelContext) -> Path:
    return (ctx.execution_root / "execution_evidence_v1" / "submissions" / ctx.day_utc / ctx.submission_id).resolve()


def _load_broker_submission(ctx: CancelContext) -> tuple[Path, dict[str, Any]]:
    path = (_submission_dir(ctx) / "broker_submission_record.v2.json").resolve()
    obj = _read_json(path)
    validate_against_repo_schema_v1(obj, REPO_ROOT, "constellation_2/schemas/broker_submission_record.v2.schema.json")
    return path, obj


def _derive_account(ctx: CancelContext, submission_dir: Path, broker_record: dict[str, Any]) -> tuple[str, str]:
    direct = str(broker_record.get("ib_account") or broker_record.get("account") or "").strip()
    if direct:
        return direct, str((submission_dir / "broker_submission_record.v2.json").resolve())
    attempt_path = (submission_dir / "broker_submit_attempt_v1.json").resolve()
    attempt = _read_json(attempt_path)
    if str(attempt.get("submission_id") or "").strip() != ctx.submission_id:
        raise PaperCancelError(f"SUBMIT_ATTEMPT_SUBMISSION_ID_MISMATCH:path={attempt_path}")
    if str(attempt.get("day_utc") or "").strip() != ctx.day_utc:
        raise PaperCancelError(f"SUBMIT_ATTEMPT_DAY_MISMATCH:path={attempt_path}")
    if str(attempt.get("environment") or "").strip().upper() != EXPECTED_ENVIRONMENT:
        raise PaperCancelError(f"SUBMIT_ATTEMPT_ENVIRONMENT_NOT_PAPER:path={attempt_path}")
    account = str(attempt.get("ib_account") or "").strip()
    if not account:
        raise PaperCancelError(f"ACCOUNT_EVIDENCE_MISSING:path={attempt_path}")
    return account, str(attempt_path)


def _derive_cancel_identity(ctx: CancelContext) -> dict[str, Any]:
    broker_path, broker_record = _load_broker_submission(ctx)
    broker = broker_record.get("broker") if isinstance(broker_record.get("broker"), dict) else {}
    environment = str(broker.get("environment") or "").strip().upper()
    if environment != EXPECTED_ENVIRONMENT:
        raise PaperCancelError(f"CANCEL_ENVIRONMENT_NOT_PAPER:environment={environment or 'MISSING'}")
    submission_id = str(broker_record.get("submission_id") or "").strip()
    if submission_id != ctx.submission_id:
        raise PaperCancelError(f"SUBMISSION_ID_MISMATCH:expected={ctx.submission_id}:actual={submission_id}")

    account, account_source_path = _derive_account(ctx, broker_path.parent, broker_record)
    if account != EXPECTED_ACCOUNT:
        raise PaperCancelError(f"CANCEL_ACCOUNT_NOT_ALLOWED:expected={EXPECTED_ACCOUNT}:actual={account or 'MISSING'}")

    broker_ids = broker_record.get("broker_ids") if isinstance(broker_record.get("broker_ids"), dict) else {}
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    if not isinstance(order_id, int) or order_id <= 0:
        raise PaperCancelError("BROKER_ORDER_ID_MISSING")
    if not isinstance(perm_id, int) or perm_id <= 0:
        raise PaperCancelError("BROKER_PERM_ID_MISSING")

    return {
        "broker_submission_record_path": str(broker_path),
        "submission_dir": str(broker_path.parent),
        "submission_id": submission_id,
        "environment": environment,
        "account": account,
        "account_source_path": account_source_path,
        "order_id": int(order_id),
        "perm_id": int(perm_id),
        "broker_status": str(broker_record.get("status") or "").strip().upper(),
    }


def _existing_outcome_classification(ctx: CancelContext) -> str:
    subdir = _submission_dir(ctx)
    outcome_path = subdir / "broker_order_outcome_v1.json"
    if outcome_path.exists():
        outcome = _read_json(outcome_path)
        outcome_state = str(outcome.get("outcome_state") or "").strip().upper()
        status = str(outcome.get("status") or "").strip().upper()
        if outcome_state == "FILLED" or status == "FILLED":
            return "FILLED_BEFORE_CANCEL"
        if outcome_state == "PARTIALLY_FILLED" or status == "PARTIALLY_FILLED":
            return "PARTIALLY_FILLED"
        if outcome_state == "BROKER_CANCELLED" or status in {"CANCELLED", "CANCELED", "APICANCELLED"}:
            return "CANCELLED"
    fill_path = ctx.execution_root / "fill_ledger_v1" / ctx.day_utc / f"{ctx.submission_id}.fill_ledger.v1.json"
    if fill_path.exists():
        fill = _read_json(fill_path)
        lifecycle = str(fill.get("lifecycle_status") or "").strip().upper()
        if lifecycle == "FILLED":
            return "FILLED_BEFORE_CANCEL"
        if lifecycle == "PARTIALLY_FILLED":
            return "PARTIALLY_FILLED"
    return ""


def _default_runner(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=60)


def _default_adapter_factory(profile: BrokerConnectionSpec) -> IBPaperAdapterV2:
    return IBPaperAdapterV2(conn=profile, env="PAPER")


def _write_cancel_request(ctx: CancelContext, identity: dict[str, Any], *, status: str) -> Path:
    payload = {
        "schema_id": "aegis_paper_cancel_request",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "produced_utc": ctx.produced_utc,
        "status": status,
        "environment": identity["environment"],
        "account": identity["account"],
        "account_source_path": identity["account_source_path"],
        "submission_id": identity["submission_id"],
        "order_id": identity["order_id"],
        "perm_id": identity["perm_id"],
        "broker_submission_record_path": identity["broker_submission_record_path"],
        "operator_command": "ops/tools/run_aegis_paper_cancel_v1.py --day_utc <DAY> --submission_id <SUBMISSION_ID>",
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    path = _submission_dir(ctx) / "aegis_paper_cancel_request.v1.json"
    _write_json(path, payload)
    return path.resolve()


def _refresh_commands(ctx: CancelContext) -> list[list[str]]:
    return [
        [
            sys.executable,
            "ops/tools/run_submission_lifecycle_refresh_v1.py",
            "--day_utc",
            ctx.day_utc,
            "--truth_root",
            str(ctx.execution_root),
            "--submission_id",
            ctx.submission_id,
        ],
        [
            sys.executable,
            "ops/tools/run_execution_lifecycle_authority_v1.py",
            "--day_utc",
            ctx.day_utc,
            "--truth_root",
            str(ctx.canonical_truth_root),
            "--execution_root",
            str(ctx.execution_root),
            "--environment",
            "PAPER",
        ],
    ]


def _write_cancel_result(
    ctx: CancelContext,
    identity: dict[str, Any],
    *,
    request_path: Path,
    classification: str,
    cancel_result: BrokerSubmitResult | None,
    refresh_results: list[dict[str, Any]],
    error: str = "",
) -> dict[str, Any]:
    payload = {
        "schema_id": "aegis_paper_cancel_result",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "produced_utc": ctx.produced_utc,
        "classification": classification,
        "environment": identity["environment"],
        "account": identity["account"],
        "account_source_path": identity["account_source_path"],
        "submission_id": identity["submission_id"],
        "order_id": identity["order_id"],
        "perm_id": identity["perm_id"],
        "cancel_request_path": str(request_path),
        "broker_submission_record_path": identity["broker_submission_record_path"],
        "broker_cancel_result": None
        if cancel_result is None
        else {
            "ok": cancel_result.ok,
            "status": cancel_result.status,
            "order_id": cancel_result.order_id,
            "perm_id": cancel_result.perm_id,
            "error_code": cancel_result.error_code,
            "error_message": cancel_result.error_message,
            "raw": cancel_result.raw,
        },
        "refresh_results": refresh_results,
        "error": error,
        "evidence_artifacts": [
            identity["broker_submission_record_path"],
            str(_submission_dir(ctx) / "broker_order_outcome_v1.json"),
            str(_submission_dir(ctx) / "broker_acknowledgement_v1.json"),
            str(ctx.execution_root / "fill_ledger_v1" / ctx.day_utc / f"{ctx.submission_id}.fill_ledger.v1.json"),
        ],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    _write_json(_submission_dir(ctx) / "aegis_paper_cancel_result.v1.json", payload)
    return payload


def run_aegis_paper_cancel_v1(
    ctx: CancelContext,
    *,
    adapter_factory: AdapterFactory = _default_adapter_factory,
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    identity = _derive_cancel_identity(ctx)
    pre_classification = _existing_outcome_classification(ctx)
    request_path = _write_cancel_request(ctx, identity, status="CANCEL_REQUESTED")
    refresh_results: list[dict[str, Any]] = []
    cancel_result: BrokerSubmitResult | None = None

    if pre_classification == "FILLED_BEFORE_CANCEL":
        return _write_cancel_result(
            ctx,
            identity,
            request_path=request_path,
            classification="FILLED_BEFORE_CANCEL",
            cancel_result=None,
            refresh_results=[],
        )

    profile = resolve_governed_paper_execution_profile(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=identity["account"],
        sleeve_id="PRIMARY",
    )
    adapter = adapter_factory(
        BrokerConnectionSpec(host=profile.host, port=profile.port, client_id=profile.client_id_orders)
    )
    try:
        adapter.connect()
        cancel_result = adapter.cancel_order(order_id=identity["order_id"])
    except Exception as exc:  # noqa: BLE001
        return _write_cancel_result(
            ctx,
            identity,
            request_path=request_path,
            classification="CANCEL_FAILED",
            cancel_result=cancel_result,
            refresh_results=[],
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass

    for cmd in _refresh_commands(ctx):
        completed = runner(cmd)
        refresh_results.append(
            {
                "command": cmd,
                "return_code": completed.returncode,
                "stdout_tail": (completed.stdout or "")[-2000:],
                "stderr_tail": (completed.stderr or "")[-2000:],
            }
        )

    post_classification = _existing_outcome_classification(ctx)
    if post_classification in {"CANCELLED", "FILLED_BEFORE_CANCEL", "PARTIALLY_FILLED"}:
        classification = post_classification
    elif cancel_result.ok:
        classification = "CANCEL_REQUESTED"
    else:
        classification = "CANCEL_FAILED"
    return _write_cancel_result(
        ctx,
        identity,
        request_path=request_path,
        classification=classification,
        cancel_result=cancel_result,
        refresh_results=refresh_results,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_paper_cancel_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--submission_id", required=True)
    parser.add_argument("--execution_root", default=str(DEFAULT_EXECUTION_ROOT))
    parser.add_argument("--truth_root", default=str(DEFAULT_CANONICAL_TRUTH_ROOT))
    parser.add_argument("--produced_utc", default="")
    args = parser.parse_args(argv)

    ctx = CancelContext(
        day_utc=str(args.day_utc).strip(),
        submission_id=str(args.submission_id).strip(),
        execution_root=Path(args.execution_root).expanduser().resolve(),
        canonical_truth_root=Path(args.truth_root).expanduser().resolve(),
        produced_utc=str(args.produced_utc or "").strip() or _utc_now_isoz(),
    )
    try:
        result = run_aegis_paper_cancel_v1(ctx)
    except PaperCancelError as exc:
        print(json.dumps({"status": "BLOCKED", "blocker": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("classification") in {"CANCEL_REQUESTED", "CANCELLED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
