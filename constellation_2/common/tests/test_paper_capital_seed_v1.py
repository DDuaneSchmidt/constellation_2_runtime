from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SEED_TOOL = (REPO_ROOT / "ops" / "tools" / "ensure_paper_capital_seed_v1.py").resolve()
OPERATOR_STATEMENT_TOOL = (REPO_ROOT / "ops" / "tools" / "ensure_cash_ledger_operator_statement_v1.py").resolve()
POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_PAPER_CAPITAL_SEED_POLICY_V1.json").resolve()


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_governed_seed_accepts_arbitrary_usd_and_renders_operator_statement(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)
    day = "2026-04-20"

    result = _run(
        [
            sys.executable,
            str(SEED_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--seed_usd",
            "1234567.89",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "GOVERNED_SEED",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    operator_statement_path = truth_root / "operator_inputs" / "cash_ledger_operator_statements" / day / "operator_statement.v1.json"
    operator_statement = _load_json(operator_statement_path)
    assert operator_statement["cash_total"] == "1234567.89"
    assert operator_statement["nlv_total"] == "1234567.89"
    assert operator_statement["account_id"] == "DUO847203"
    assert operator_statement["notes"][0] == "CAPITAL_SEED_V2: governed paper capital seed"


def test_legacy_zero_still_works(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)
    day = "2026-04-21"

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "ZERO",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    operator_statement_path = truth_root / "operator_inputs" / "cash_ledger_operator_statements" / day / "operator_statement.v1.json"
    operator_statement = _load_json(operator_statement_path)
    assert operator_statement["cash_total"] == "0.00"
    assert operator_statement["nlv_total"] == "0.00"


def test_legacy_seed_100k_still_works(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)
    day = "2026-04-22"

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "SEED_100K",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    operator_statement_path = truth_root / "operator_inputs" / "cash_ledger_operator_statements" / day / "operator_statement.v1.json"
    operator_statement = _load_json(operator_statement_path)
    assert operator_statement["cash_total"] == "100000.00"
    assert operator_statement["nlv_total"] == "100000.00"


def test_governed_seed_missing_input_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            "2026-04-23",
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "GOVERNED_SEED",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode != 0
    assert "GOVERNED_SEED_INPUT_MISSING" in (result.stderr or result.stdout)


def test_invalid_governed_seed_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)
    day = "2026-04-24"
    seed_path = truth_root / "operator_inputs" / "paper_capital_seed_v1" / day / "paper_capital_seed.v1.json"
    seed_path.parent.mkdir(parents=True)
    seed_path.write_text(
        json.dumps(
            {
                "schema_id": "C2_PAPER_CAPITAL_SEED",
                "schema_version": 1,
                "day_utc": day,
                "environment": "PAPER",
                "currency": "USD",
                "cash_total": "1.00",
                "nlv_total": "2.00",
                "notes": ["bad"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "GOVERNED_SEED",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode != 0
    assert "GOVERNED_SEED_CASH_NLV_MISMATCH" in (result.stderr or result.stdout)


def test_existing_no_overwrite_behavior_is_preserved(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)
    day = "2026-04-25"

    result = _run(
        [
            sys.executable,
            str(SEED_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--seed_usd",
            "5000000.00",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout
    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "GOVERNED_SEED",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    operator_statement_path = truth_root / "operator_inputs" / "cash_ledger_operator_statements" / day / "operator_statement.v1.json"
    first = operator_statement_path.read_text(encoding="utf-8")

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "SEED_100K",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert "OPERATOR_STATEMENT_EXISTS" in result.stdout
    assert operator_statement_path.read_text(encoding="utf-8") == first


def test_fresh_future_day_can_be_initialized_from_specified_seed(tmp_path: Path) -> None:
    truth_root = tmp_path / "constellation_2"
    truth_root.mkdir(parents=True)
    day = "2026-04-26"

    result = _run(
        [
            sys.executable,
            str(SEED_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--seed_usd",
            "5000000.00",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    seed_path = truth_root / "operator_inputs" / "paper_capital_seed_v1" / day / "paper_capital_seed.v1.json"
    seed_obj = _load_json(seed_path)
    assert seed_obj["cash_total"] == "5000000.00"
    assert seed_obj["nlv_total"] == "5000000.00"
    assert seed_obj["policy_ref"]["path"] == str(POLICY_PATH)

    result = _run(
        [
            sys.executable,
            str(OPERATOR_STATEMENT_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--ib_account",
            "DUO847203",
            "--mode",
            "GOVERNED_SEED",
            "--allow_create",
            "YES",
        ]
    )
    assert result.returncode == 0, result.stderr or result.stdout

    operator_statement_path = truth_root / "operator_inputs" / "cash_ledger_operator_statements" / day / "operator_statement.v1.json"
    operator_statement = _load_json(operator_statement_path)
    assert operator_statement["cash_total"] == "5000000.00"
    assert operator_statement["nlv_total"] == "5000000.00"
