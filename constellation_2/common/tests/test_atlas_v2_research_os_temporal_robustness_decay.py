from __future__ import annotations

from constellation_2.common.atlas_v2_research_os.temporal_robustness_decay import (
    _classify_temporal,
    _concentration_rows,
    _decay_rows,
    _monthly_rows,
)


def test_builds_165_166_monthly_buckets_compute_net_metrics() -> None:
    samples = [
        {"candidate_id": "a", "month": "2023-01", "return": 0.003},
        {"candidate_id": "b", "month": "2023-01", "return": 0.002},
        {"candidate_id": "a", "month": "2023-02", "return": -0.001},
        {"candidate_id": "b", "month": "2023-02", "return": -0.002},
    ]

    rows = _monthly_rows(samples)

    assert [row["month"] for row in rows] == ["2023-01", "2023-02"]
    assert rows[0]["signal_count"] == 2
    assert rows[0]["expectancy"] == 0.0025
    assert rows[0]["net_expectancy_10bps"] == 0.0015
    assert rows[1]["net_expectancy_10bps"] == -0.0025


def test_builds_165_166_detects_decay_before_concentration() -> None:
    monthly = []
    samples = []
    for month_index in range(8):
        month = f"2023-{month_index + 1:02d}"
        value = 0.004 if month_index < 4 else -0.001
        for event in range(15):
            samples.append({"candidate_id": f"c{event % 3}", "month": month, "return": value})
    monthly = _monthly_rows(samples)
    decay = _decay_rows(monthly, samples)
    concentration = _concentration_rows(monthly, decay)

    assert _classify_temporal(monthly, decay, concentration) == "DECAYING"
