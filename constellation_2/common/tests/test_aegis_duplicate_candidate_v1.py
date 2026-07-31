from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.duplicate_candidate_v1 import (  # noqa: E402
    DUPLICATE_OPEN_POSITION,
    DUPLICATE_RECENT_CAPTURE,
    DUPLICATE_SAME_DAY,
    DUPLICATE_SAME_SESSION,
    IMPROVED_SIGNAL,
    IMPROVED_SIGNAL_ADD_ON,
    NEW_DISTINCT_SETUP,
    SIGNAL_REFRESH_NO_ACTION,
    build_duplicate_candidate_v1,
    render_duplicate_candidate_v1,
)

DAY = "2026-05-29"
SESSION = "PAPER-2026-05-29-0950"
NOW = "2026-05-29T16:00:00Z"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _candidate(symbol: str, candidate_id: str, **extra: object) -> dict:
    row = {
        "symbol": symbol,
        "direction": "LONG",
        "candidate_id": candidate_id,
        "candidate_contract_id": candidate_id,
        "paper_session_id": SESSION,
        "entry_price": "100.00",
        "stop_price": "95.00",
        "quantity": "1",
        "score": "1.00",
        "reward_risk_ratio": "2.0",
    }
    row.update(extra)
    return row


def _seed(root: Path, candidates: list[dict], *, open_positions: list[dict] | None = None, carry: list[dict] | None = None) -> None:
    _write(
        root / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_session_candidates": candidates,
            "open_paper_positions": open_positions or [],
            "closed_paper_positions": [],
            "carry_forward_context": carry or [],
        },
    )
    _write(
        root / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "signal_evidence_present_count": len(candidates),
            "boundary_rows": [
                {
                    "symbol": row["symbol"],
                    "candidate_id": row["candidate_id"],
                    "candidate_contract_id": row["candidate_contract_id"],
                    "paper_session_id": row["paper_session_id"],
                    "boundary_status": "SIGNAL_EVIDENCE_PRESENT",
                }
                for row in candidates
            ],
        },
    )


def _by_symbol(payload: dict, symbol: str) -> dict:
    return next(row for row in payload["duplicate_rows"] if row["symbol"] == symbol)


def test_duplicate_candidate_classifies_open_position_and_blocks_normal_capture(tmp_path: Path) -> None:
    qqq = _candidate("QQQ", "candidate-qqq")
    _seed(tmp_path, [qqq], open_positions=[{**qqq, "candidate_id": "candidate-prior-qqq", "position_id": "paper-position-qqq", "candidate_lifecycle_state": "PAPER_POSITION_OPEN"}])

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    row = _by_symbol(payload, "QQQ")

    assert row["duplicate_classification"] == DUPLICATE_OPEN_POSITION
    assert row["suppressed"] is True
    assert "CONFIRM_CAPTURED" in row["blocked_actions"]
    assert "View existing position" in row["operator_message"] or "view existing position" in row["operator_message"]


def test_duplicate_candidate_same_session_duplicate_is_suppressed(tmp_path: Path) -> None:
    _seed(tmp_path, [_candidate("QQQ", "candidate-1"), _candidate("QQQ", "candidate-2")])

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert _by_symbol(payload, "QQQ")["duplicate_classification"] == NEW_DISTINCT_SETUP
    assert [row["duplicate_classification"] for row in payload["duplicate_rows"]].count(DUPLICATE_SAME_SESSION) == 1
    assert payload["suppressed_duplicate_count"] == 1


def test_duplicate_candidate_same_day_duplicate_is_classified(tmp_path: Path) -> None:
    qqq = _candidate("QQQ", "candidate-qqq")
    _seed(tmp_path, [qqq])
    _write(
        tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json",
        {"paper_session_id": SESSION, "rows": [{**qqq, "candidate_id": "candidate-qqq-1450", "paper_session_id": "PAPER-2026-05-29-1450"}]},
    )

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert _by_symbol(payload, "QQQ")["duplicate_classification"] == DUPLICATE_SAME_DAY


def test_duplicate_candidate_recent_capture_is_classified(tmp_path: Path) -> None:
    qqq = _candidate("QQQ", "candidate-qqq")
    _seed(tmp_path, [qqq])
    _write(
        tmp_path / "reports" / "aegis_paper_entry_receipts_v1" / DAY / "paper_entry_receipts.v1.json",
        {"receipts": [{**qqq, "candidate_id": "candidate-prior", "receipt_id": "ENTRY-1"}]},
    )

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert _by_symbol(payload, "QQQ")["duplicate_classification"] == DUPLICATE_RECENT_CAPTURE


def test_duplicate_candidate_materially_improved_duplicate_is_reviewable(tmp_path: Path) -> None:
    qqq = _candidate("QQQ", "candidate-qqq", score="1.30")
    _seed(tmp_path, [qqq])
    _write(
        tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json",
        {"paper_session_id": SESSION, "rows": [{**qqq, "candidate_id": "candidate-prior", "paper_session_id": "PAPER-2026-05-29-1450", "score": "1.00"}]},
    )

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    row = _by_symbol(payload, "QQQ")

    assert row["duplicate_classification"] == IMPROVED_SIGNAL
    assert row["reviewable"] is True
    assert "CONFIRM_CAPTURED" in row["allowed_actions"]


def test_duplicate_candidate_improved_open_position_becomes_add_on(tmp_path: Path) -> None:
    qqq = _candidate("QQQ", "candidate-qqq", score="1.30")
    _seed(tmp_path, [qqq], open_positions=[{**qqq, "candidate_id": "candidate-prior", "position_id": "paper-position-qqq", "score": "1.00"}])

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    row = _by_symbol(payload, "QQQ")

    assert row["duplicate_classification"] == IMPROVED_SIGNAL_ADD_ON
    assert row["reviewable"] is True
    assert "REVIEW_ADD_ON" in row["allowed_actions"]
    assert "CONFIRM_CAPTURED" in row["blocked_actions"]


def test_duplicate_candidate_repeated_non_improved_carry_forward_is_suppressed(tmp_path: Path) -> None:
    qqq = _candidate("QQQ", "candidate-qqq")
    _seed(tmp_path, [qqq], carry=[{**qqq, "candidate_id": "candidate-prior", "paper_session_id": "PAPER-2026-05-28-0950"}])

    payload = build_duplicate_candidate_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert _by_symbol(payload, "QQQ")["duplicate_classification"] == SIGNAL_REFRESH_NO_ACTION
    assert payload["suppressed_duplicate_count"] == 1


def test_duplicate_candidate_status_render_and_package_script() -> None:
    package = json.loads(Path("package.json").read_text(encoding="utf-8"))
    assert "aegis:duplicate-candidate-status" in package["scripts"]
    assert "build_aegis_duplicate_candidate_v1.py" in package["scripts"]["aegis:duplicate-candidate-status"]
    rendered = render_duplicate_candidate_v1({"current_output_candidate_count": 2, "new_distinct_setup_count": 1, "suppressed_duplicate_count": 1})
    assert "Duplicate candidate status:" in rendered
    assert "- current output candidates: 2" in rendered
    assert "- suppressed duplicates: 1" in rendered
