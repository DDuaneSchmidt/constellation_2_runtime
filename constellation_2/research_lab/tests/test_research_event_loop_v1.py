from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from constellation_2.research_lab.research_event_bus_v1 import (
    ensure_event_runtime_tree,
    list_events_in_bucket_v1,
    publish_event_v1,
    resolve_runtime_root,
)
from constellation_2.research_lab.research_event_v1 import build_research_event_v1
from constellation_2.research_lab.research_trigger_evaluator_v1 import (
    build_sandbox_result_completed_event_v1,
    evaluate_trading_day_closed_trigger_v1,
    resolve_execution_root,
    resolve_truth_root,
)


@pytest.fixture()
def env_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    runtime_root = (tmp_path / "runtime_root").resolve()
    truth_root = (tmp_path / "truth_root").resolve()
    execution_root = (tmp_path / "execution_root").resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    truth_root.mkdir(parents=True, exist_ok=True)
    execution_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("CONSTELLATION_RESEARCH_LAB_RUNTIME_ROOT", str(runtime_root))
    monkeypatch.setenv("CONSTELLATION_TRUTH_ROOT", str(truth_root))
    monkeypatch.setenv("CONSTELLATION_EXECUTION_ROOT", str(execution_root))

    ensure_event_runtime_tree()
    return {
        "runtime_root": runtime_root,
        "truth_root": truth_root,
        "execution_root": execution_root,
    }


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _seed_trading_day_artifacts(day_utc: str, *, truth_root: Path, execution_root: Path, packet_path: Path) -> None:
    _write_json(
        truth_root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl",
        {
            "day_utc": day_utc,
            "is_trading_session": True,
        },
    )
    # convert JSON object line file for test calendar
    calendar_file = truth_root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    calendar_file.write_text(json.dumps({"day_utc": day_utc, "is_trading_session": True}) + "\n", encoding="utf-8")

    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text("# Aegis packet\n", encoding="utf-8")

    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        {
            "boundary_status": "AUTHORIZED",
            "blocking_codes": [],
        },
    )
    _write_json(
        truth_root / "reports" / "aegis_day_closure_authority_v1" / day_utc / "aegis_day_closure_authority.v1.json",
        {
            "schema_version": "aegis_day_closure_authority.v1",
            "day": day_utc,
            "status": "PASS",
            "canonical_blocker": "",
            "source_surfaces": {
                "execution_current_head": {
                    "path": str(
                        execution_root
                        / "execution_evidence_v1"
                        / "current_head"
                        / day_utc
                        / "current_head.v1.json"
                    )
                },
                "submission_index": {
                    "path": str(
                        execution_root
                        / "submission_index_v1"
                        / day_utc
                        / "submission_index.v1.json"
                    )
                },
            },
            "blocking_evidence": [],
        },
    )
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        {"status": "PASS", "day_utc": day_utc},
    )
    _write_json(
        execution_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json",
        {"status": "PASS", "day_utc": day_utc},
    )
    _write_json(
        execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json",
        {"status": "PASS", "day_utc": day_utc, "submission_count": 1},
    )


def _sandbox_result_payload(*, day_utc: str, test_id: str = "test_001", idea_id: str = "idea_001") -> dict:
    return {
        "schema_version": "sandbox_result.v1",
        "test_id": test_id,
        "idea_id": idea_id,
        "status": "PASS",
        "metrics": {
            "walk_forward_passed": True,
            "walk_forward_inconclusive": False,
            "overfitting_risk": "LOW",
            "data_quality_risk": "LOW",
        },
        "in_sample_metrics": {"edge_score": 1.0},
        "out_of_sample_metrics": {"edge_score": 0.8},
        "cost_model_status": "PRESENT",
        "slippage_model_status": "PRESENT",
        "sample_size": 200,
        "warnings": [],
        "generated_utc": f"{day_utc}T20:00:00Z",
    }


def _run_sweep(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> dict:
    from ops.tools import run_research_event_sweep_v1 as sweep_tool

    monkeypatch.setattr(sys, "argv", ["run_research_event_sweep_v1.py"])
    rc = sweep_tool.main()
    assert rc == 0
    out = capsys.readouterr().out.strip().splitlines()[-1]
    return json.loads(out)


def test_valid_trading_day_closed_event_created_with_required_artifacts(env_roots: dict[str, Path]) -> None:
    day_utc = "2026-04-24"
    packet_path = env_roots["runtime_root"] / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    _seed_trading_day_artifacts(day_utc, truth_root=env_roots["truth_root"], execution_root=env_roots["execution_root"], packet_path=packet_path)

    evaluated = evaluate_trading_day_closed_trigger_v1(
        day_utc=day_utc,
        truth_root=resolve_truth_root(),
        execution_root=resolve_execution_root(),
        aegis_packet_path=packet_path,
    )
    assert evaluated["status"] == "READY"
    published = publish_event_v1(dict(evaluated["event"]))
    assert published["status"] == "NEW"
    assert len(list_events_in_bucket_v1("inbox")) == 1


def test_no_trading_day_event_created_for_non_trading_day(env_roots: dict[str, Path]) -> None:
    day_utc = "2026-04-26"  # Sunday
    packet_path = env_roots["runtime_root"] / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    evaluated = evaluate_trading_day_closed_trigger_v1(
        day_utc=day_utc,
        truth_root=resolve_truth_root(),
        execution_root=resolve_execution_root(),
        aegis_packet_path=packet_path,
    )
    assert evaluated["status"] == "SKIPPED"
    assert evaluated["reason"] == "NON_TRADING_DAY"
    assert len(list_events_in_bucket_v1("inbox")) == 0


def test_duplicate_idempotency_key_does_not_create_duplicate_event(env_roots: dict[str, Path]) -> None:
    event = build_research_event_v1(
        event_type="SANDBOX_RESULT_COMPLETED",
        day_utc="2026-04-24",
        source="RESEARCH_LAB",
        source_artifacts=["/tmp/a.json"],
        requires_ai_review=True,
    )
    first = publish_event_v1(event)
    second = publish_event_v1(event)
    assert first["status"] == "NEW"
    assert second["status"] == "DUPLICATE"
    assert len(list_events_in_bucket_v1("inbox")) == 1


def test_invalid_event_rejected(env_roots: dict[str, Path]) -> None:
    bad = {
        "schema_version": "research_event.v1",
        "event_type": "TRADING_DAY_CLOSED",
    }
    result = publish_event_v1(bad)
    assert result["accepted"] is False
    assert result["status"] == "REJECTED"
    assert len(list_events_in_bucket_v1("rejected")) == 0  # rejected records use distinct schema
    rejected_files = list((env_roots["runtime_root"] / "events" / "rejected").rglob("*.json"))
    assert rejected_files


def test_sandbox_result_completed_event_created_from_valid_result(env_roots: dict[str, Path]) -> None:
    day_utc = "2026-04-24"
    result = _sandbox_result_payload(day_utc=day_utc)
    result_path = _write_json(env_roots["runtime_root"] / "tmp" / "result.sandbox_result.v1.json", result)
    event = build_sandbox_result_completed_event_v1(result_payload=result, result_json_path=result_path)
    published = publish_event_v1(event)
    assert published["status"] == "NEW"
    inbox = list_events_in_bucket_v1("inbox")
    assert any(row["event_type"] == "SANDBOX_RESULT_COMPLETED" for row in inbox)


def test_event_sweep_processes_inbox_event_exactly_once(
    env_roots: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    day_utc = "2026-04-24"
    packet_path = env_roots["runtime_root"] / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    _seed_trading_day_artifacts(day_utc, truth_root=env_roots["truth_root"], execution_root=env_roots["execution_root"], packet_path=packet_path)
    evaluated = evaluate_trading_day_closed_trigger_v1(
        day_utc=day_utc,
        truth_root=resolve_truth_root(),
        execution_root=resolve_execution_root(),
        aegis_packet_path=packet_path,
    )
    publish_event_v1(dict(evaluated["event"]))

    first = _run_sweep(monkeypatch, capsys)
    second = _run_sweep(monkeypatch, capsys)

    assert first["processed_count"] == 1
    assert second["processed_count"] == 0
    assert len(list_events_in_bucket_v1("processed")) == 1


def test_failed_event_processing_preserves_event_in_failed_folder(
    env_roots: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    event = build_research_event_v1(
        event_type="SANDBOX_RESULT_COMPLETED",
        day_utc="2026-04-24",
        source="RESEARCH_LAB",
        source_artifacts=["/tmp/does_not_exist.sandbox_result.v1.json"],
        requires_ai_review=True,
    )
    publish_event_v1(event)
    result = _run_sweep(monkeypatch, capsys)
    assert result["failed_count"] == 1
    assert len(list_events_in_bucket_v1("failed")) == 1


def test_ai_packet_created_for_trading_day_event(
    env_roots: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    day_utc = "2026-04-24"
    packet_path = env_roots["runtime_root"] / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    _seed_trading_day_artifacts(day_utc, truth_root=env_roots["truth_root"], execution_root=env_roots["execution_root"], packet_path=packet_path)
    event_payload = evaluate_trading_day_closed_trigger_v1(
        day_utc=day_utc,
        truth_root=resolve_truth_root(),
        execution_root=resolve_execution_root(),
        aegis_packet_path=packet_path,
    )["event"]
    publish_event_v1(event_payload)

    _run_sweep(monkeypatch, capsys)

    packet_json = env_roots["runtime_root"] / "reviews" / "ai_edge_reviews" / day_utc / "research_ai_packet.v1.json"
    packet_md = env_roots["runtime_root"] / "reviews" / "ai_edge_reviews" / day_utc / "research_ai_packet.md"
    assert packet_json.exists()
    assert packet_md.exists()


def test_research_event_does_not_write_inside_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_runtime = Path(__file__).resolve().parents[3] / "runtime" / "research_event_bad_root"
    monkeypatch.setenv("CONSTELLATION_RESEARCH_LAB_RUNTIME_ROOT", str(repo_runtime))
    with pytest.raises(ValueError):
        resolve_runtime_root()


def test_event_sweep_cannot_promote_to_paper_or_live(
    env_roots: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    day_utc = "2026-04-24"
    result = _sandbox_result_payload(day_utc=day_utc, test_id="test_no_promo", idea_id="idea_no_promo")
    result_path = _write_json(env_roots["runtime_root"] / "tmp" / "test_no_promo.sandbox_result.v1.json", result)
    event = build_sandbox_result_completed_event_v1(result_payload=result, result_json_path=result_path)
    publish_event_v1(event)

    sweep_out = _run_sweep(monkeypatch, capsys)
    assert sweep_out["processed_count"] == 1
    details = sweep_out["processed"][0]["details"]
    assert details["promoted_to_paper"] is False
    assert details["promoted_to_live"] is False
    promotion_dir = env_roots["runtime_root"] / "promotion_queue"
    assert not promotion_dir.exists() or not any(promotion_dir.rglob("*.json"))
