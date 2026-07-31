from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from typing import Any

from .session_context import SESSION_CONTEXTS, dominant_session_context, normalize_session_context, session_distribution


def normalize_observation_text(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def timestamp_bucket(timestamp: str) -> str:
    value = str(timestamp)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%dT%H")
    except Exception:
        return value[:13]


def observation_dedup_key(record: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    return (
        normalize_observation_text(record.get("observation_text", "")),
        str(record.get("symbol", "")).upper(),
        str(record.get("timeframe", "")).lower(),
        str(record.get("mechanism", "")).upper(),
        str(record.get("market_structure") or record.get("metadata", {}).get("market_structure") or "UNKNOWN").upper(),
        str(record.get("regime", "UNKNOWN")).upper(),
        timestamp_bucket(str(record.get("timestamp", ""))),
    )


def deduplicate_observations(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seen: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    unique: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    for record in records:
        key = observation_dedup_key(record)
        if key in seen:
            duplicate = dict(record)
            duplicate.setdefault("metadata", {})["duplicate_of"] = seen[key]["observation_id"]
            duplicates.append(duplicate)
            continue
        seen[key] = record
        unique.append(record)
    return unique, duplicates


def cluster_observations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from .bulk_observation_import import stable_id
    from .observation_models import DEFAULT_MARKET_STRUCTURE, ObservationCluster

    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for record in records:
        normalized = normalize_observation_text(record.get("observation_text", ""))
        signature = " ".join(normalized.split()[:6])
        market_structure = str(record.get("market_structure") or record.get("metadata", {}).get("market_structure") or "UNKNOWN").upper()
        key = (str(record.get("mechanism", "UNKNOWN")).upper(), market_structure, str(record.get("regime", "UNKNOWN")).upper(), signature)
        groups.setdefault(key, []).append(record)
    clusters = []
    for (mechanism, market_structure, regime, signature), rows in sorted(groups.items()):
        observation_ids = sorted(row["observation_id"] for row in rows)
        symbols = sorted({str(row.get("symbol", "")).upper() for row in rows})
        timeframes = sorted({str(row.get("timeframe", "")).lower() for row in rows})
        structures = sorted({str(row.get("market_structure") or DEFAULT_MARKET_STRUCTURE).upper() for row in rows})
        source_types = sorted({str(row.get("metadata", {}).get("source_type") or "UNKNOWN") for row in rows})
        session_contexts = sorted({str(row.get("metadata", {}).get("session_context") or "UNKNOWN").upper() for row in rows})
        session_context = Counter(str(row.get("metadata", {}).get("session_context") or "UNKNOWN").upper() for row in rows).most_common(1)[0][0]
        confidence = round(sum(float(row.get("confidence", 0.0)) for row in rows) / len(rows), 6)
        session_values = [(row.get("metadata") or {}).get("session_context") for row in rows]
        cluster = ObservationCluster(
            cluster_id=stable_id("obs_cluster", [mechanism, market_structure, regime, signature, observation_ids]),
            mechanism=mechanism,
            market_structure=market_structure,
            regime=regime,
            symbols=symbols,
            timeframes=timeframes,
            source_observation_ids=observation_ids,
            cluster_summary=f"{len(rows)} {mechanism} / {market_structure} observations in {regime} regime: {signature}",
            confidence=confidence,
            observation_count=len(rows),
            metadata={
                "normalized_signature": signature,
                "market_structure": market_structure,
                "session_context": session_context,
                "market_structures": structures,
                "source_types": source_types,
                "session_contexts": session_contexts,
                "measurement_only": True,
            },
        ).to_dict()
        clusters.append(cluster)
    return clusters
