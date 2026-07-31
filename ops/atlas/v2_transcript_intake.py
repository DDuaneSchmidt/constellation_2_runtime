from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_external_strategy_claim_extractor import (
    DEFAULT_CREATED_AT,
    DEFAULT_LEDGER_ROOT,
    AtlasV2ExternalStrategyClaimExtractor,
    ExternalStrategyExtractionResult,
    sanitize_claim_text,
    sha256_text,
    short_hash,
)

INTAKE_VERSION = "atlas_v2_transcript_intake_v1"
RULE_SEGMENT_TYPES = {"ENTRY_RULE", "EXIT_RULE", "FILTER_RULE", "RISK_RULE", "MARKET_CONTEXT"}


@dataclass(frozen=True)
class TranscriptIntakeResult:
    request: dict[str, Any]
    segments: list[dict[str, Any]]
    candidates: list[dict[str, Any]]
    external_results: list[ExternalStrategyExtractionResult]

    def summary(self) -> dict[str, Any]:
        return {
            "request_id": self.request["request_id"],
            "request_status": self.request["status"],
            "segments": len(self.segments),
            "candidates": len(self.candidates),
            "candidate_statuses": [candidate["status"] for candidate in self.candidates],
            "segment_types": [segment["segment_type"] for segment in self.segments],
            "external_claims": sum(len(result.claims) for result in self.external_results),
            "mechanism_families": [mechanism["mechanism_family"] for result in self.external_results for mechanism in result.mechanisms],
            "contrarian_statuses": [theory["status"] for result in self.external_results for theory in result.contrarian_theories],
            "handoff_statuses": [handoff["status"] for result in self.external_results for handoff in result.handoffs],
        }


class AtlasV2TranscriptIntake:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger
        self.extractor = AtlasV2ExternalStrategyClaimExtractor(ledger)

    def intake(
        self,
        *,
        source_title: str,
        transcript_text: str,
        source_url: str | None = None,
        source_channel: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
        request_id: str | None = None,
    ) -> TranscriptIntakeResult:
        text = str(transcript_text or "").strip()
        if not text:
            raise AtlasV2ValidationError("Transcript Intake V1 accepts manually supplied transcript_text only")
        transcript_hash = sha256_text(text)
        request_record_id = request_id or f"tir-{short_hash('|'.join([source_title or '', source_url or '', transcript_hash]))}"
        request_payload: dict[str, Any] = {
            "request_id": request_record_id,
            "created_at": created_at,
            "source_title": source_title or "untitled manually supplied transcript",
            "transcript_text": text,
            "transcript_hash": transcript_hash,
            "status": "RECEIVED",
        }
        if source_url:
            request_payload["source_url"] = source_url
        if source_channel:
            request_payload["source_channel"] = source_channel
        request = self.ledger.create_record(
            "TranscriptIntakeRequest",
            request_payload,
            reason="manual transcript text received for Atlas V2 transcript intake",
            triggering_object=INTAKE_VERSION,
        )

        segments = [
            self.ledger.create_record(
                "TranscriptSegment",
                payload,
                reason="manual transcript text segmented into rule-like chunks",
                triggering_object=f"TranscriptIntakeRequest:{request_record_id}",
            )
            for payload in segment_transcript(request_record_id, text, created_at=created_at)
        ]
        candidates = [
            self.ledger.create_record(
                "TranscriptClaimCandidate",
                payload,
                reason="transcript segments converted to extraction candidates without performance evidence",
                triggering_object=f"TranscriptIntakeRequest:{request_record_id}",
            )
            for payload in claim_candidates_for_segments(request_record_id, segments, created_at=created_at)
        ]
        request = self.ledger.transition_object(
            "TranscriptIntakeRequest",
            request_record_id,
            to_status="PARSED",
            reason="manual transcript parsed into segments and claim candidates",
            triggering_object=f"TranscriptClaimCandidate:{','.join(candidate['candidate_id'] for candidate in candidates) or 'none'}",
            transitioned_at=created_at,
        )

        external_results: list[ExternalStrategyExtractionResult] = []
        for candidate in candidates:
            if candidate["status"] == "DUPLICATE_SEGMENT":
                continue
            external_results.append(
                self.extractor.extract(
                    source_title=source_title or "untitled manually supplied transcript",
                    source_type="YOUTUBE_TRANSCRIPT",
                    source_url=source_url,
                    source_channel=source_channel,
                    input_text=candidate["candidate_text"],
                    created_at=created_at,
                    source_id=f"ess-{short_hash(candidate['candidate_id'])}",
                )
            )
        return TranscriptIntakeResult(request=request, segments=segments, candidates=candidates, external_results=external_results)


def segment_transcript(request_id: str, transcript_text: str, *, created_at: str) -> list[dict[str, Any]]:
    chunks = split_transcript_chunks(transcript_text)
    payloads: list[dict[str, Any]] = []
    for index, (start, end, chunk) in enumerate(chunks, start=1):
        segment_type = classify_segment_type(chunk)
        payloads.append(
            {
                "segment_id": f"tis-{short_hash('|'.join([request_id, str(index), chunk]))}",
                "request_id": request_id,
                "created_at": created_at,
                "start_offset": start,
                "end_offset": end,
                "segment_text": chunk,
                "segment_type": segment_type,
                "status": "PARSED",
            }
        )
    if not payloads:
        payloads.append(
            {
                "segment_id": f"tis-{short_hash(request_id + ':empty')}",
                "request_id": request_id,
                "created_at": created_at,
                "start_offset": 0,
                "end_offset": max(1, len(transcript_text)),
                "segment_text": transcript_text.strip() or "No transcript detail supplied.",
                "segment_type": "OTHER",
                "status": "PARSED",
            }
        )
    return payloads


def split_transcript_chunks(text: str) -> list[tuple[int, int, str]]:
    chunks: list[tuple[int, int, str]] = []
    pattern = re.compile(r"[^.!?;]+(?:[.!?;]|$)")
    for line_match in re.finditer(r"[^\n]+", text):
        raw_line = line_match.group(0)
        stripped_line = raw_line.strip(" \t\r-•")
        if not stripped_line:
            continue
        line_start = line_match.start() + raw_line.find(stripped_line)
        list_prefix = re.match(r"(?:\d+|[A-Za-z])[\.)]\s+", stripped_line)
        if list_prefix:
            line_start += list_prefix.end()
            stripped_line = stripped_line[list_prefix.end() :].strip(" \t\r-•")
        for match in pattern.finditer(stripped_line):
            raw = match.group(0)
            chunk = raw.strip(" \t\r\n-•")
            if not chunk:
                continue
            start = line_start + match.start() + len(raw) - len(raw.lstrip(" \t\r\n-•"))
            end = start + len(chunk)
            chunks.append((start, end, chunk))
    return chunks


def classify_segment_type(text: str) -> str:
    lowered = text.lower()
    if is_performance_claim(lowered):
        return "PERFORMANCE_CLAIM"
    if any(term in lowered for term in ("stop", "risk", "loss", "invalid", "invalidation", "position size")):
        return "RISK_RULE"
    if any(term in lowered for term in ("exit", "target", "take profit", "close the trade", "trail", "scale out")):
        return "EXIT_RULE"
    if any(term in lowered for term in ("enter", "entry", "buy", "long", "short", "sell at", "sell short", "break above", "break below", "breakout", "reclaim")):
        return "ENTRY_RULE"
    if any(term in lowered for term in ("filter", "only if", "only trade", "chart only", "framework", "confirm", "confirmation", "vwap", "ema", "sma", "rsi", "atr")):
        return "FILTER_RULE"
    if any(term in lowered for term in ("opening range", "orb", "market open", "new york open", "london open", "session", "first 5", "first 15", "trend", "range")):
        return "MARKET_CONTEXT"
    return "OTHER"


def is_performance_claim(lowered_text: str) -> bool:
    return bool(
        re.search(r"\$\s?\d", lowered_text)
        or re.search(r"\b\d+(?:\.\d+)?\s?%", lowered_text)
        or any(term in lowered_text for term in ("per month", "a month", "monthly", "win rate", "profit", "pnl"))
    )


def claim_candidates_for_segments(request_id: str, segments: list[dict[str, Any]], *, created_at: str) -> list[dict[str, Any]]:
    duplicate_payloads = duplicate_segment_candidates(request_id, segments, created_at=created_at)
    rule_segments = [segment for segment in segments if segment["segment_type"] in RULE_SEGMENT_TYPES]
    candidate_segments = [segment for segment in rule_segments if normalized_segment_key(segment["segment_text"])]
    if not candidate_segments:
        detail_segments = [segment for segment in segments if segment["segment_type"] != "PERFORMANCE_CLAIM"] or segments[:1]
        return [insufficient_candidate_payload(request_id, detail_segments, created_at=created_at), *duplicate_payloads]

    segment_types = {segment["segment_type"] for segment in candidate_segments}
    has_entry = "ENTRY_RULE" in segment_types
    has_supporting_rule = bool(segment_types & {"EXIT_RULE", "FILTER_RULE", "RISK_RULE"})
    status = "READY_FOR_EXTRACTION" if has_entry and has_supporting_rule else "INSUFFICIENT_DETAIL"
    confidence = 0.74 if status == "READY_FOR_EXTRACTION" else 0.28
    candidate_text = " ".join(segment["segment_text"] for segment in candidate_segments)
    payload = {
        "candidate_id": f"tic-{short_hash('|'.join([request_id, candidate_text, status]))}",
        "request_id": request_id,
        "created_at": created_at,
        "segment_ids": [segment["segment_id"] for segment in candidate_segments],
        "candidate_text": sanitize_claim_text(candidate_text),
        "confidence": confidence,
        "status": status,
    }
    return [payload, *duplicate_payloads]


def duplicate_segment_candidates(request_id: str, segments: list[dict[str, Any]], *, created_at: str) -> list[dict[str, Any]]:
    seen: dict[str, str] = {}
    duplicates: list[dict[str, Any]] = []
    for segment in segments:
        if segment["segment_type"] == "PERFORMANCE_CLAIM":
            continue
        key = normalized_segment_key(segment["segment_text"])
        if not key:
            continue
        if key not in seen:
            seen[key] = segment["segment_id"]
            continue
        pair = [seen[key], segment["segment_id"]]
        duplicates.append(
            {
                "candidate_id": f"tic-{short_hash('|'.join([request_id, *pair, 'duplicate']))}",
                "request_id": request_id,
                "created_at": created_at,
                "segment_ids": pair,
                "candidate_text": "Duplicate transcript segment repeated and excluded from extraction evidence.",
                "confidence": 1.0,
                "status": "DUPLICATE_SEGMENT",
            }
        )
    return duplicates


def insufficient_candidate_payload(request_id: str, segments: list[dict[str, Any]], *, created_at: str) -> dict[str, Any]:
    segment_text = " ".join(segment["segment_text"] for segment in segments)[:900]
    return {
        "candidate_id": f"tic-{short_hash('|'.join([request_id, segment_text, 'insufficient']))}",
        "request_id": request_id,
        "created_at": created_at,
        "segment_ids": [segment["segment_id"] for segment in segments],
        "candidate_text": sanitize_claim_text(segment_text or "Transcript lacks explicit entry, exit, filter, or risk detail."),
        "confidence": 0.15,
        "status": "INSUFFICIENT_DETAIL",
    }


def normalized_segment_key(text: str) -> str:
    lowered = re.sub(r"https?://\S+", " ", text.lower())
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _read_transcript(args: argparse.Namespace) -> str:
    if args.transcript_text:
        return args.transcript_text
    if args.transcript_file:
        return Path(args.transcript_file).read_text(encoding="utf-8")
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 manual transcript intake V1")
    parser.add_argument("--source-title", required=True)
    parser.add_argument("--source-url")
    parser.add_argument("--source-channel")
    parser.add_argument("--transcript-text")
    parser.add_argument("--transcript-file")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2TranscriptIntake(AtlasV2Ledger(Path(args.ledger_root))).intake(
        source_title=args.source_title,
        source_url=args.source_url,
        source_channel=args.source_channel,
        transcript_text=_read_transcript(args),
        created_at=args.created_at,
    )
    print(json.dumps(result.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
