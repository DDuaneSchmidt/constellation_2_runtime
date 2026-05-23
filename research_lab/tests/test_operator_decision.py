from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.candidates.operator_decision import append_operator_decision, build_operator_decision, load_operator_decisions


def test_operator_decision_is_append_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    first = build_operator_decision(
        candidate_id="cand_fixture",
        candidate_batch_id="cb_fixture",
        decision="ignore",
        decision_reason="research observation only",
        decided_by="operator",
        decided_at="2024-01-01T00:00:00Z",
    )
    second = build_operator_decision(
        candidate_id="cand_fixture",
        candidate_batch_id="cb_fixture",
        decision="defer",
        decision_reason="wait for confirmation",
        decided_by="operator",
        decided_at="2024-01-02T00:00:00Z",
    )

    append_operator_decision(first, store_root=store, actor="operator")
    append_operator_decision(second, store_root=store, actor="operator")

    assert [row["decision"] for row in load_operator_decisions(store_root=store)] == ["ignore", "defer"]


def test_invalid_decision_fails() -> None:
    with pytest.raises(RuntimeError, match="Unsupported operator decision"):
        build_operator_decision(
            candidate_id="cand_fixture",
            candidate_batch_id="cb_fixture",
            decision="trade",
            decision_reason="bad",
            decided_by="operator",
        )

