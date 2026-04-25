from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.ensure_paper_submit_smoke_test_request_v1 as request_module  # noqa: E402
import ops.tools.run_paper_submit_smoke_test_v1 as smoke_module  # noqa: E402
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_submit_smoke_test_request_path,
)


DAY = "2026-04-15"
ACCOUNT = "DUO847203"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")


def test_request_builder_writes_explicit_test_metadata(tmp_path: Path) -> None:
    policy = request_module._load_policy()
    payload = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="", policy=policy)
    assert payload["environment"] == "PAPER"
    assert payload["test_only"] is True
    assert payload["engine_id"] == "C2_INTENT_SIMULATOR_V1"
    assert payload["instrument"]["symbol"] == "SPY"
    assert payload["quantity_shares"] == 1
    assert "TEST_ONLY" in payload["notes"]
    assert "request_nonce" not in payload


def test_smoke_tool_fails_closed_outside_paper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    operator_root = tmp_path / "operator_root"
    operator_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_paper_submit_smoke_test_v1.py",
            "--day_utc",
            DAY,
            "--eval_time_utc",
            f"{DAY}T14:35:00Z",
            "--operator_input_root",
            str(operator_root),
            "--ib_account",
            ACCOUNT,
            "--environment",
            "LIVE",
            "--allow_broker_transmit",
            "YES",
        ],
    )
    with pytest.raises(SystemExit, match="PAPER_ONLY_TOOL"):
        smoke_module.main()


def test_build_execution_intent_emits_smoke_metadata(tmp_path: Path) -> None:
    operator_root = tmp_path / "operator_root"
    operator_root.mkdir(parents=True, exist_ok=True)
    policy = request_module._load_policy()
    request_payload = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="", policy=policy)
    request_path = operator_root / "request.json"
    _write_json(request_path, request_payload)
    intent = smoke_module.build_smoke_execution_intent_v1(
        day_utc=DAY,
        eval_time_utc=f"{DAY}T14:35:00Z",
        ib_account=ACCOUNT,
        request_path=request_path,
        request_obj=request_payload,
        policy=policy,
    )
    assert intent.environment == "PAPER"
    assert intent.actor_source == "paper_submit_smoke_test_v1"
    assert "paper_submit_smoke_test_marker:TEST_ONLY" in intent.source_artifact_refs
    assert any("paper_submit_smoke_test_request_path:" in item for item in intent.source_artifact_refs)


def test_smoke_auth_contains_explicit_reason_codes(tmp_path: Path) -> None:
    operator_root = tmp_path / "operator_root"
    operator_root.mkdir(parents=True, exist_ok=True)
    policy = request_module._load_policy()
    request_payload = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="", policy=policy)
    request_path = operator_root / "request.json"
    _write_json(request_path, request_payload)
    intent = smoke_module.build_smoke_execution_intent_v1(
        day_utc=DAY,
        eval_time_utc=f"{DAY}T14:35:00Z",
        ib_account=ACCOUNT,
        request_path=request_path,
        request_obj=request_payload,
        policy=policy,
    )
    auth_obj = smoke_module.build_smoke_authorization_obj(
        day_utc=DAY,
        produced_utc=f"{DAY}T14:35:00Z",
        intent=intent,
        request_path=request_path,
        request_obj=request_payload,
        policy=policy,
    )
    assert auth_obj["status"] == "AUTHORIZED"
    assert "PAPER_SUBMIT_SMOKE_TEST_AUTHORIZED" in auth_obj["reason_codes"]
    assert "TEST_ONLY" in auth_obj["reason_codes"]
    assert auth_obj["authorization"]["authorized_quantity"] == 1


def test_missing_request_fails_closed_and_writes_nothing(tmp_path: Path) -> None:
    operator_root = tmp_path / "operator_root"
    operator_root.mkdir(parents=True, exist_ok=True)
    with pytest.raises(SystemExit, match="SMOKE_REQUEST_MISSING"):
        smoke_module._load_request(operator_input_root=operator_root, day_utc=DAY, ib_account=ACCOUNT, request_nonce="")
    assert list(operator_root.rglob("*")) == []


def test_request_id_unchanged_without_nonce() -> None:
    policy = request_module._load_policy()
    payload = request_module._build_request_payload(day_utc="2026-04-14", ib_account=ACCOUNT, request_nonce="", policy=policy)
    assert payload["request_id"] == "7b13f2425b9515a01e3f3920c63e1d955ffd7ce9e921437f9dd137f953a6ba2b"


def test_same_day_different_nonce_produces_different_request_id() -> None:
    policy = request_module._load_policy()
    first = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="retry-1", policy=policy)
    second = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="retry-2", policy=policy)
    assert first["request_id"] != second["request_id"]
    assert first["request_nonce"] == "retry-1"
    assert second["request_nonce"] == "retry-2"


def test_same_nonce_produces_same_request_id_and_path(tmp_path: Path) -> None:
    policy = request_module._load_policy()
    first = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="retry-2", policy=policy)
    second = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="retry-2", policy=policy)
    path_first = resolve_paper_submit_smoke_test_request_path(
        operator_input_root=tmp_path,
        day_utc=DAY,
        request_nonce="retry-2",
    )
    path_second = resolve_paper_submit_smoke_test_request_path(
        operator_input_root=tmp_path,
        day_utc=DAY,
        request_nonce="retry-2",
    )
    assert first["request_id"] == second["request_id"]
    assert path_first == path_second
    assert path_first.name == "paper_submit_smoke_test_request.retry-2.v1.json"


def test_non_smoke_paths_unchanged_when_nonce_added(tmp_path: Path) -> None:
    operator_input_root = tmp_path / "operator_root"
    expected = operator_input_root / "operator_inputs" / "cash_ledger_operator_statements" / DAY / "operator_statement.v1.json"
    assert resolve_operator_statement_path(operator_input_root=operator_input_root, day_utc=DAY) == expected.resolve()


def test_execution_intent_emits_nonce_metadata_when_present(tmp_path: Path) -> None:
    operator_root = tmp_path / "operator_root"
    operator_root.mkdir(parents=True, exist_ok=True)
    policy = request_module._load_policy()
    request_payload = request_module._build_request_payload(day_utc=DAY, ib_account=ACCOUNT, request_nonce="retry-2", policy=policy)
    request_path = operator_root / "request.retry-2.json"
    _write_json(request_path, request_payload)
    intent = smoke_module.build_smoke_execution_intent_v1(
        day_utc=DAY,
        eval_time_utc=f"{DAY}T14:35:00Z",
        ib_account=ACCOUNT,
        request_path=request_path,
        request_obj=request_payload,
        policy=policy,
    )
    assert "paper_submit_smoke_test_request_nonce:retry-2" in intent.source_artifact_refs
    assert "paper_submit_smoke_test_request_nonce:retry-2" in intent.parent_lineage_refs
