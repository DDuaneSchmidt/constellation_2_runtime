from __future__ import annotations

from pathlib import Path

from research_lab.storage.hashing import canonical_json


def test_event_study_hash_inputs_are_canonicalized() -> None:
    first = canonical_json({"rows": [{"symbol": "SPY", "event_date": "2024-01-02"}]})
    second = canonical_json({"rows": [{"event_date": "2024-01-02", "symbol": "SPY"}]})

    assert first == second

