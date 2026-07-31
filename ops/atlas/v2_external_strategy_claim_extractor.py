from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import (
    AtlasV2Ledger,
    AtlasV2ValidationError,
    EXTERNAL_STRATEGY_HANDOFF_TIERS,
    EXTERNAL_STRATEGY_SOURCE_TYPES,
    PROHIBITED_AUTHORITY_FIELDS,
)

EXTRACTOR_VERSION = "atlas_v2_external_strategy_claim_extractor_v1"
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_external_strategy_claim_extractor_v1_ledgers")
TRANSCRIPT_REQUIRED_TEXT = "Transcript or manual notes are required before external strategy claim extraction."
CLAIM_STATUS_EXTRACTED = "EXTRACTED"
CLAIM_STATUS_INSUFFICIENT = "INSUFFICIENT_RULE_DETAIL"
CLAIM_STATUS_TRANSCRIPT_REQUIRED = "TRANSCRIPT_REQUIRED"
CLAIM_STATUS_UNSUPPORTED = "UNSUPPORTED_SOURCE"
CLAIM_STATUS_DUPLICATE = "DUPLICATE_CLAIM"
ALLOWED_HANDOFF_TIERS = ("TIER_0_DEDUPE", "TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION")

CLAIM_STOPWORDS = {"a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "if", "in", "into", "is", "it", "of", "on", "or", "the", "then", "to", "use", "using", "when", "with"}
INDICATOR_WORDS = {"ema", "sma", "rsi", "macd", "stochastic", "bollinger", "supertrend", "indicator", "indicators", "moving", "average", "vwap"}


@dataclass(frozen=True)
class ExternalStrategyExtractionResult:
    source: dict[str, Any]
    claims: list[dict[str, Any]]
    mechanisms: list[dict[str, Any]]
    deduplication_results: list[dict[str, Any]]
    contrarian_theories: list[dict[str, Any]]
    handoffs: list[dict[str, Any]]

    def summary(self) -> dict[str, Any]:
        return {
            "source_id": self.source["source_id"],
            "source_status": self.source["status"],
            "claims": len(self.claims),
            "mechanisms": len(self.mechanisms),
            "deduplication_results": len(self.deduplication_results),
            "contrarian_theories": len(self.contrarian_theories),
            "handoffs": len(self.handoffs),
            "claim_statuses": [record["extraction_status"] for record in self.claims],
            "mechanism_families": [record["mechanism_family"] for record in self.mechanisms],
            "contrarian_statuses": [record["status"] for record in self.contrarian_theories],
            "recommended_tiers": [record["recommended_tier"] for record in self.handoffs],
        }


class AtlasV2ExternalStrategyClaimExtractor:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def extract(self, *, source_title: str, input_text: str | None, source_type: str = "YOUTUBE_TRANSCRIPT", source_url: str | None = None, source_channel: str | None = None, created_at: str = DEFAULT_CREATED_AT, source_id: str | None = None, prior_failure_matches: list[dict[str, Any]] | None = None) -> ExternalStrategyExtractionResult:
        normalized_source_type = str(source_type or "").strip().upper()
        text = str(input_text or "").strip()
        transcript_available = bool(text)
        source_hash = sha256_text(text)
        source_record_id = source_id or f"ess-{short_hash('|'.join([normalized_source_type, source_title, source_url or '', source_hash]))}"
        source_status = "recorded"
        if normalized_source_type not in EXTERNAL_STRATEGY_SOURCE_TYPES:
            normalized_source_type = "OTHER"
            source_status = CLAIM_STATUS_UNSUPPORTED
        elif not transcript_available:
            source_status = CLAIM_STATUS_TRANSCRIPT_REQUIRED

        source_payload: dict[str, Any] = {
            "source_id": source_record_id,
            "created_at": created_at,
            "source_type": normalized_source_type,
            "source_title": source_title or "untitled external strategy source",
            "input_text": text,
            "input_text_hash": source_hash,
            "transcript_available": transcript_available,
            "status": source_status,
        }
        if source_url:
            source_payload["source_url"] = source_url
        if source_channel:
            source_payload["source_channel"] = source_channel
        source = self.ledger.create_record("ExternalStrategySource", source_payload, reason="external strategy source recorded from explicit manual input", triggering_object=EXTRACTOR_VERSION)

        claims: list[dict[str, Any]] = []
        mechanisms: list[dict[str, Any]] = []
        dedupe_results: list[dict[str, Any]] = []
        contrarian_theories: list[dict[str, Any]] = []
        handoffs: list[dict[str, Any]] = []
        for payload in self._claim_payloads_for_source(source, created_at=created_at):
            claim = self.ledger.create_record("ExternalStrategyClaim", payload, reason="external strategy claim extracted from supplied text only", triggering_object=f"ExternalStrategySource:{source['source_id']}")
            claims.append(claim)
            mechanism = self.ledger.create_record("ExternalStrategyMechanism", self.classify_mechanism(claim, created_at=created_at), reason="external strategy mechanism classified before cheap handoff", triggering_object=f"ExternalStrategyClaim:{claim['claim_id']}")
            mechanisms.append(mechanism)
            dedupe = self.ledger.create_record("ExternalStrategyDeduplicationResult", self.deduplicate_claim(claim, mechanism, created_at=created_at), reason="external strategy claim and mechanism fingerprints compared", triggering_object=f"ExternalStrategyMechanism:{mechanism['mechanism_id']}")
            dedupe_results.append(dedupe)
            contrarian = self.ledger.create_record("ExternalStrategyContrarianTheory", self.create_contrarian_theory(claim, mechanism, dedupe, prior_failure_matches=prior_failure_matches or [], created_at=created_at), reason="external strategy contrarian theory generated before cheap handoff", triggering_object=f"ExternalStrategyDeduplicationResult:{dedupe['dedupe_id']}")
            contrarian_theories.append(contrarian)
            handoff = self.ledger.create_record("ExternalStrategyCheapExperimentHandoff", self.create_handoff(claim, mechanism, dedupe, contrarian=contrarian, created_at=created_at), reason="external strategy claim handed off to cheap experiment tier only", triggering_object=f"ExternalStrategyContrarianTheory:{contrarian['contrarian_id']}")
            handoffs.append(handoff)
        return ExternalStrategyExtractionResult(source=source, claims=claims, mechanisms=mechanisms, deduplication_results=dedupe_results, contrarian_theories=contrarian_theories, handoffs=handoffs)

    def _claim_payloads_for_source(self, source: dict[str, Any], *, created_at: str) -> list[dict[str, Any]]:
        status = str(source["status"])
        text = str(source.get("input_text") or "")
        if status == CLAIM_STATUS_TRANSCRIPT_REQUIRED:
            return [self._base_claim_payload(source, claim_text=TRANSCRIPT_REQUIRED_TEXT, extraction_status=CLAIM_STATUS_TRANSCRIPT_REQUIRED, confidence=0.0, created_at=created_at)]
        if status == CLAIM_STATUS_UNSUPPORTED:
            return [self._base_claim_payload(source, claim_text="Unsupported external source type for V1 manual claim extraction.", extraction_status=CLAIM_STATUS_UNSUPPORTED, confidence=0.0, created_at=created_at)]
        rules = extract_rule_fields(text)
        enough_rule_detail = bool(rules.get("entry_rule") and (rules.get("exit_rule") or rules.get("risk_rule") or rules.get("filter_rule")))
        extraction_status = CLAIM_STATUS_EXTRACTED if enough_rule_detail else CLAIM_STATUS_INSUFFICIENT
        confidence = 0.72 if enough_rule_detail else 0.25
        return [self._base_claim_payload(source, claim_text=summarize_claim_text(text), extraction_status=extraction_status, confidence=confidence, created_at=created_at, **{key: value for key, value in rules.items() if value})]

    def _base_claim_payload(self, source: dict[str, Any], *, claim_text: str, extraction_status: str, confidence: float, created_at: str, **optional_fields: str) -> dict[str, Any]:
        return {"claim_id": f"esc-{short_hash('|'.join([str(source['source_id']), claim_text, extraction_status]))}", "source_id": source["source_id"], "claim_text": sanitize_claim_text(claim_text), "confidence": confidence, "extraction_status": extraction_status, "created_at": created_at, **optional_fields}

    def classify_mechanism(self, claim: dict[str, Any], *, created_at: str) -> dict[str, Any]:
        text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "market_context", "timeframe"))
        family, confidence, description = classify_mechanism_text(text)
        return {"mechanism_id": f"esm-{short_hash(str(claim['claim_id']) + family)}", "claim_id": claim["claim_id"], "mechanism_family": family, "mechanism_description": description, "classification_confidence": confidence, "created_at": created_at}

    def deduplicate_claim(self, claim: dict[str, Any], mechanism: dict[str, Any], *, created_at: str) -> dict[str, Any]:
        claim_fp = claim_fingerprint(claim)
        mechanism_fp = mechanism_fingerprint(claim, mechanism)
        duplicate_of = None
        duplicate_reason = None
        for prior in self.ledger.records("ExternalStrategyDeduplicationResult"):
            prior_claim_id = str(prior.get("claim_id") or "")
            if prior_claim_id == claim["claim_id"]:
                continue
            if str(prior.get("claim_fingerprint") or "") == claim_fp:
                duplicate_of = prior_claim_id
                duplicate_reason = "CLAIM_FINGERPRINT_MATCH"
                break
            if str(prior.get("mechanism_fingerprint") or "") == mechanism_fp:
                duplicate_of = prior_claim_id
                duplicate_reason = "MECHANISM_FINGERPRINT_MATCH"
                break
        payload: dict[str, Any] = {"dedupe_id": f"esd-{short_hash(str(claim['claim_id']) + claim_fp + mechanism_fp)}", "claim_id": claim["claim_id"], "claim_fingerprint": claim_fp, "mechanism_fingerprint": mechanism_fp, "is_duplicate": duplicate_of is not None, "status": CLAIM_STATUS_DUPLICATE if duplicate_of else "UNIQUE_OR_FIRST_SEEN", "created_at": created_at}
        if duplicate_of:
            payload["duplicate_of_claim_id"] = duplicate_of
            payload["duplicate_reason"] = duplicate_reason
        return payload


    def create_contrarian_theory(
        self,
        claim: dict[str, Any],
        mechanism: dict[str, Any],
        dedupe: dict[str, Any],
        *,
        prior_failure_matches: list[dict[str, Any]] | None = None,
        created_at: str,
    ) -> dict[str, Any]:
        failure_modes, confidence, status = infer_primary_failure_modes(claim, mechanism)
        opposite = opposite_hypothesis_for_failure_modes(failure_modes, mechanism)
        duplicate = self._duplicate_contrarian(failure_modes, opposite, claim["claim_id"])
        if duplicate:
            status = "DUPLICATE_CONTRARIAN"
        return {
            "contrarian_id": f"est-{short_hash(str(claim['claim_id']) + str(mechanism['mechanism_id']))}",
            "claim_id": claim["claim_id"],
            "mechanism_id": mechanism["mechanism_id"],
            "primary_failure_modes": failure_modes,
            "opposite_hypothesis": opposite,
            "fragility_conditions": fragility_conditions_for_failure_modes(failure_modes),
            "required_falsification_tests": falsification_tests_for_failure_modes(failure_modes),
            "prior_failure_matches": prior_failure_matches or [],
            "contrarian_confidence": confidence,
            "status": status,
            "created_at": created_at,
        }

    def _duplicate_contrarian(self, failure_modes: list[str], opposite_hypothesis: str, claim_id: str) -> bool:
        key = (tuple(sorted(failure_modes)), opposite_hypothesis)
        for prior in self.ledger.records("ExternalStrategyContrarianTheory"):
            if prior.get("claim_id") == claim_id:
                continue
            prior_key = (tuple(sorted(str(mode) for mode in prior.get("primary_failure_modes", []))), str(prior.get("opposite_hypothesis") or ""))
            if prior_key == key:
                return True
        return False

    def create_handoff(self, claim: dict[str, Any], mechanism: dict[str, Any], dedupe: dict[str, Any], *, contrarian: dict[str, Any] | None = None, created_at: str) -> dict[str, Any]:
        extraction_status = str(claim["extraction_status"])
        family = str(mechanism["mechanism_family"])
        is_duplicate = bool(dedupe["is_duplicate"])
        if extraction_status == CLAIM_STATUS_EXTRACTED and not is_duplicate and family != "UNKNOWN":
            eligible, tier, reason, status = True, "TIER_1_SANITY", "Claim has explicit supplied rule detail and classified mechanism; cheap sanity test only.", "READY_FOR_CHEAP_EXPERIMENT_HANDOFF"
        elif is_duplicate:
            eligible, tier, reason, status = True, "TIER_0_DEDUPE", "Duplicate relationship should be resolved before any sanity test.", "DEDUPE_ONLY"
        else:
            eligible, tier, reason, status = False, "TIER_0_DEDUPE", "Claim lacks sufficient detail or classified mechanism for cheap sanity testing.", "NOT_ELIGIBLE_FOR_CHEAP_EXPERIMENT"
        if tier not in EXTERNAL_STRATEGY_HANDOFF_TIERS:
            raise AtlasV2ValidationError(f"handoff tier outside external strategy boundary: {tier}")
        payload = {"handoff_id": f"esh-{short_hash(str(claim['claim_id']) + str(mechanism['mechanism_id']) + str(dedupe['dedupe_id']))}", "claim_id": claim["claim_id"], "mechanism_id": mechanism["mechanism_id"], "dedupe_id": dedupe["dedupe_id"], "eligible_for_cheap_experiment": eligible, "recommended_tier": tier, "reason": reason, "authority_boundary_acknowledged": True, "status": status, "created_at": created_at}
        if contrarian is not None:
            payload["contrarian_id"] = contrarian["contrarian_id"]
        return payload


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def normalize_text(text: str) -> str:
    lowered = re.sub(r"https?://\S+", " ", text.lower())
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def sanitize_claim_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    for old, new in {"profitable": "claimed to have an edge", "validated": "claimed", "proven": "claimed", "guaranteed": "claimed", "trade advice": "external claim", "recommendation": "external claim"}.items():
        cleaned = re.sub(old, new, cleaned, flags=re.IGNORECASE)
    return cleaned or "External strategy claim unavailable."


def summarize_claim_text(text: str) -> str:
    sentences = split_sentences(text)
    if not sentences:
        return "External strategy description supplied without extractable sentence detail."
    return " ".join(sorted(sentences, key=rule_score, reverse=True)[:2])[:900]


def split_sentences(text: str) -> list[str]:
    return [chunk.strip(" -\t") for chunk in re.split(r"(?<=[.!?])\s+|\n+", text) if chunk.strip(" -\t")]


def rule_score(sentence: str) -> int:
    lowered = sentence.lower()
    return sum(1 for term in ("enter", "buy", "sell", "short", "break", "breakout", "opening range", "orb", "stop", "target", "exit", "filter") if term in lowered)


def extract_rule_fields(text: str) -> dict[str, str]:
    sentences = split_sentences(text)
    fields = {
        "market_context": first_matching(sentences, ("session", "open", "market", "trend", "range", "volatility")),
        "timeframe": extract_timeframe(text),
        "entry_rule": first_matching(sentences, ("enter", "entry", "buy", "short", "sell short", "break above", "break below", "breakout", "reclaim", "touch", "turn")),
        "exit_rule": first_matching(sentences, ("exit", "target", "take profit", "close when", "close the trade", "close position", "trail")),
        "filter_rule": first_matching(sentences, ("filter", "only if", "confirm", "confirmation", "above vwap", "below vwap", "ema", "rsi", "atr")),
        "risk_rule": first_matching(sentences, ("stop", "risk", "loss", "invalid", "position size")),
        "instrument": extract_instrument(text),
        "claimed_edge": first_matching(sentences, ("edge", "works", "win rate", "expectancy", "captures", "avoids")),
    }
    return {key: value for key, value in fields.items() if value}


def first_matching(sentences: list[str], terms: tuple[str, ...]) -> str:
    for term in terms:
        for sentence in sentences:
            if term in sentence.lower():
                return sentence[:500]
    return ""


def extract_timeframe(text: str) -> str:
    match = re.search(r"\b(\d+\s*(?:second|sec|minute|min|hour|day)s?|[1-9]\d?m|[1-9]\d?h)\b", text, flags=re.IGNORECASE)
    return match.group(0) if match else ""


def extract_instrument(text: str) -> str:
    match = re.search(r"\b(NQ|ES|YM|RTY|SPY|QQQ|DAX|NASDAQ|S&P\s*500|FOREX|EURUSD|GBPUSD)\b", text, flags=re.IGNORECASE)
    return match.group(0).upper() if match else ""


def classify_mechanism_text(text: str) -> tuple[str, float, str]:
    lowered = text.lower()
    if any(term in lowered for term in ("opening range", "orb", "initial balance", "first 5 minute range", "first 15 minute range")):
        return "OPENING_RANGE", 0.86, "Uses an opening-session range high/low or ORB reference as the main decision level."
    if any(term in lowered for term in ("touch and turn", "touch & turn", "reject", "rejection", "bounce", "fade", "reversal")):
        return "MEAN_REVERSION", 0.74, "Uses a touch, rejection, or turn away from a reference level."
    if any(term in lowered for term in ("breakout", "break above", "break below", "range break", "breaks the high", "breaks the low")):
        return "BREAKOUT", 0.78, "Uses a break through a prior level or range boundary."
    if any(term in lowered for term in ("liquidity sweep", "stop hunt", "sweep the high", "sweep the low")):
        return "LIQUIDITY_SWEEP", 0.78, "Uses a sweep of visible highs/lows as the claimed setup condition."
    if any(term in lowered for term in ("atr", "average true range")):
        return "ATR_FILTER", 0.7, "Uses ATR as a volatility or distance filter."
    if any(term in lowered for term in ("vwap", "moving average reclaim", "average reclaim", "ema reclaim")):
        return "VWAP_OR_AVERAGE_RECLAIM", 0.72, "Uses VWAP or moving-average reclaim as the claimed setup condition."
    if any(term in lowered for term in ("pullback", "retracement", "dip")):
        return "PULLBACK", 0.66, "Uses pullback continuation after an initial directional move."
    if any(term in lowered for term in ("momentum", "strong candle", "impulse")):
        return "MOMENTUM", 0.64, "Uses directional momentum as the claimed mechanism."
    if any(term in lowered for term in ("trend continuation", "continue the trend", "with the trend")):
        return "TREND_CONTINUATION", 0.66, "Uses continuation in the direction of an existing trend."
    if any(term in lowered for term in ("volatility expansion", "expansion", "range expansion")):
        return "VOLATILITY_EXPANSION", 0.64, "Uses expansion from a lower-volatility condition."
    if any(term in lowered for term in ("session", "london open", "new york open", "market open")):
        return "SESSION_TIMING", 0.55, "Uses session timing as the primary supplied mechanism."
    return "UNKNOWN", 0.0, "No explicit mechanism family is identifiable from the supplied text."


def claim_fingerprint(claim: dict[str, Any]) -> str:
    text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "risk_rule", "instrument", "timeframe"))
    tokens = [token for token in normalize_text(text).split() if token not in CLAIM_STOPWORDS]
    return sha256_text(" ".join(tokens))


def mechanism_fingerprint(claim: dict[str, Any], mechanism: dict[str, Any]) -> str:
    family = str(mechanism.get("mechanism_family") or "UNKNOWN")
    text = " ".join(str(claim.get(field) or "") for field in ("entry_rule", "exit_rule", "filter_rule", "risk_rule"))
    tokens = [token for token in normalize_text(text).split() if token not in CLAIM_STOPWORDS and token not in INDICATOR_WORDS]
    mechanism_tokens = [token for token in tokens if token in mechanism_keywords_for_family(family)]
    if not mechanism_tokens:
        mechanism_tokens = [family.lower()]
    return sha256_text(" ".join([family.lower(), *sorted(set(mechanism_tokens))]))


def mechanism_keywords_for_family(family: str) -> set[str]:
    return {
        "OPENING_RANGE": {"opening", "range", "orb", "open", "high", "low", "break", "breakout"},
        "BREAKOUT": {"break", "breakout", "above", "below", "high", "low", "range"},
        "MEAN_REVERSION": {"touch", "turn", "reject", "rejection", "bounce", "fade", "reversal"},
        "TREND_CONTINUATION": {"trend", "continuation", "continue", "pullback"},
        "LIQUIDITY_SWEEP": {"liquidity", "sweep", "stop", "hunt", "high", "low"},
        "VOLATILITY_EXPANSION": {"volatility", "expansion", "range"},
        "PULLBACK": {"pullback", "retracement", "dip", "trend"},
        "MOMENTUM": {"momentum", "impulse", "strong", "candle"},
        "VWAP_OR_AVERAGE_RECLAIM": {"vwap", "reclaim", "above", "below"},
        "ATR_FILTER": {"atr", "volatility", "range"},
        "SESSION_TIMING": {"session", "open", "london", "york"},
        "UNKNOWN": {"unknown"},
    }.get(family, {family.lower()})



def infer_primary_failure_modes(claim: dict[str, Any], mechanism: dict[str, Any]) -> tuple[list[str], float, str]:
    extraction_status = str(claim.get("extraction_status") or "")
    family = str(mechanism.get("mechanism_family") or "UNKNOWN")
    claim_text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "risk_rule", "claimed_edge")).lower()
    if extraction_status in {"INSUFFICIENT_RULE_DETAIL", "TRANSCRIPT_REQUIRED", "UNSUPPORTED_SOURCE"}:
        return ["VAGUE_RULES"], 0.72, "REQUIRES_CLARIFICATION"
    if family == "UNKNOWN":
        return ["UNKNOWN"], 0.0, "UNKNOWN"
    by_family = {
        "OPENING_RANGE": ["REGIME_DEPENDENCE", "SLIPPAGE", "CHERRY_PICKING", "LOW_SAMPLE_SIZE"],
        "BREAKOUT": ["REGIME_DEPENDENCE", "SLIPPAGE", "CROWDING", "CHERRY_PICKING", "LOW_SAMPLE_SIZE"],
        "MEAN_REVERSION": ["REGIME_DEPENDENCE", "SLIPPAGE", "CROWDING"],
        "TREND_CONTINUATION": ["REGIME_DEPENDENCE", "CROWDING", "LOW_SAMPLE_SIZE"],
        "LIQUIDITY_SWEEP": ["DATA_MINING", "CHERRY_PICKING", "SLIPPAGE", "REGIME_DEPENDENCE"],
        "VOLATILITY_EXPANSION": ["REGIME_DEPENDENCE", "SLIPPAGE", "LOW_SAMPLE_SIZE"],
        "PULLBACK": ["REGIME_DEPENDENCE", "CROWDING", "CHERRY_PICKING"],
        "MOMENTUM": ["REGIME_DEPENDENCE", "SLIPPAGE", "CROWDING"],
        "VWAP_OR_AVERAGE_RECLAIM": ["REGIME_DEPENDENCE", "DATA_MINING", "CROWDING"],
        "ATR_FILTER": ["REGIME_DEPENDENCE", "DATA_MINING", "LOW_SAMPLE_SIZE"],
        "SESSION_TIMING": ["REGIME_DEPENDENCE", "CHERRY_PICKING", "LOW_SAMPLE_SIZE"],
    }
    modes = list(by_family.get(family, ["UNKNOWN"]))
    if any(term in claim_text for term in ("win rate", "works", "edge", "expectancy")):
        for extra in ("CHERRY_PICKING", "LOW_SAMPLE_SIZE"):
            if extra not in modes:
                modes.append(extra)
    return modes, 0.68, "GENERATED"


def opposite_hypothesis_for_failure_modes(failure_modes: list[str], mechanism: dict[str, Any]) -> str:
    family = str(mechanism.get("mechanism_family") or "UNKNOWN")
    if failure_modes == ["VAGUE_RULES"]:
        return "The supplied description is too vague to define a falsifiable external strategy claim."
    if failure_modes == ["UNKNOWN"]:
        return "The supplied text does not identify enough mechanism detail to infer a specific failure theory."
    return f"The claimed {family} mechanism may not survive out-of-sample costs, crowding, regime shifts, or unbiased sampling."


def fragility_conditions_for_failure_modes(failure_modes: list[str]) -> list[str]:
    mapping = {
        "OVERFITTING": "Performance appears only after many parameter choices or hindsight rule selection.",
        "CROWDING": "The setup degrades when many participants watch the same obvious level.",
        "SLIPPAGE": "Entry/exit assumptions fail once spread, queue position, and fast movement are included.",
        "REGIME_DEPENDENCE": "The setup only appears under specific volatility, trend, or session regimes.",
        "VAGUE_RULES": "Entry, exit, filter, or risk rules are not specific enough to replay.",
        "LOOKAHEAD_BIAS": "Rule construction may use information not available at decision time.",
        "SURVIVORSHIP_BIAS": "Instrument or sample selection may omit failed comparable cases.",
        "COMMISSION_DRAG": "Small gross edge may disappear after commissions and fees.",
        "CHERRY_PICKING": "Examples may be selected from favorable sessions rather than a complete sample.",
        "DATA_MINING": "The mechanism may be one of many tried patterns that survived by chance.",
        "LOW_SAMPLE_SIZE": "The claim may rely on too few independent observations.",
        "UNKNOWN": "No specific fragility condition can be inferred from supplied text.",
    }
    return [mapping[mode] for mode in failure_modes]


def falsification_tests_for_failure_modes(failure_modes: list[str]) -> list[str]:
    mapping = {
        "OVERFITTING": "Freeze rules before replay and compare against simple parameter perturbations.",
        "CROWDING": "Compare obvious-level trades against randomized nearby levels and later-session repeats.",
        "SLIPPAGE": "Replay with conservative spread, delay, and adverse fill assumptions.",
        "REGIME_DEPENDENCE": "Stratify cheap test results by volatility, trend, and session regime before any promotion.",
        "VAGUE_RULES": "Require explicit entry, exit, filter, and risk text before any TIER_1 sanity run.",
        "LOOKAHEAD_BIAS": "Verify every input is timestamp-available before the claimed entry decision.",
        "SURVIVORSHIP_BIAS": "Include comparable instruments/sessions that did not produce showcased examples.",
        "COMMISSION_DRAG": "Subtract realistic commissions and fees from every cheap replay outcome.",
        "CHERRY_PICKING": "Sample a consecutive date window rather than selected video examples.",
        "DATA_MINING": "Compare against naive baselines and shuffled-rule controls.",
        "LOW_SAMPLE_SIZE": "Set a minimum independent observation count before interpreting any effect direction.",
        "UNKNOWN": "Request more source detail; do not infer a specific falsification design beyond transcript clarification.",
    }
    return [mapping[mode] for mode in failure_modes]

def validate_no_forbidden_payload_fields(record: dict[str, Any]) -> None:
    for field in PROHIBITED_AUTHORITY_FIELDS:
        if record.get(field) not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"external strategy payload cannot set prohibited authority field: {field}")


def _read_input_text(args: argparse.Namespace) -> str:
    if args.input_text:
        return args.input_text
    if args.input_file:
        return Path(args.input_file).read_text(encoding="utf-8")
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 manual external strategy claim extractor V1")
    parser.add_argument("--source-title", required=True)
    parser.add_argument("--source-type", default="YOUTUBE_TRANSCRIPT")
    parser.add_argument("--source-url")
    parser.add_argument("--source-channel")
    parser.add_argument("--input-text")
    parser.add_argument("--input-file")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2ExternalStrategyClaimExtractor(AtlasV2Ledger(Path(args.ledger_root))).extract(source_title=args.source_title, input_text=_read_input_text(args), source_type=args.source_type, source_url=args.source_url, source_channel=args.source_channel, created_at=args.created_at)
    print(json.dumps(result.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
