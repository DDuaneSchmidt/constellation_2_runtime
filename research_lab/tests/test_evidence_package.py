from __future__ import annotations

from research_lab.evidence.evidence_package import evidence_quality_for_event_count


def test_evidence_quality_uses_packet_3_thresholds() -> None:
    assert evidence_quality_for_event_count(29) == "insufficient_sample"
    assert evidence_quality_for_event_count(30) == "preliminary"
    assert evidence_quality_for_event_count(100) == "moderate"

