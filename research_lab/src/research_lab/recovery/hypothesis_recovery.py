from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.event_intake.event_cluster import build_event_cluster
from research_lab.event_intake.event_family import validate_event_family
from research_lab.event_intake.event_hypothesis_templates import event_family_spec, intent_template
from research_lab.event_intake.event_intake_registry import (
    load_event_family,
    seed_event_families,
    store_event_cluster,
    store_event_observation,
    store_hypothesis_proposal,
    store_intent_candidate,
)
from research_lab.event_intake.event_observation import build_event_observation
from research_lab.event_intake.hypothesis_proposal import build_hypothesis_proposal, recompute_hypothesis_proposal_hash, validate_hypothesis_proposal
from research_lab.event_intake.intent_candidate import build_intent_candidate
from research_lab.projections.projection_builder import rebuild_research_projections
from research_lab.projections.projection_validator import validate_research_projections
from research_lab.research_intake.intake_registry import (
    assess_hypothesis_readiness,
    build_and_store_research_intake_dossier,
    latest_priority_score,
    latest_readiness_assessment,
    latest_research_intake_dossier,
    score_hypothesis_proposal,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout, project_root

RECOVERY_ROOT = Path("recovery") / "hypothesis_recovery"
RECOVERY_SCHEMA_VERSION = "hypothesis_recovery.v1"
ALLOWED_SOURCE_TYPES = {
    "canonical_proposal",
    "event_observation",
    "intent_candidate",
    "event_cluster",
    "research_plan",
    "test_fixture",
    "audit_log",
    "registry_row",
    "ui_seed",
    "operator_note",
    "unknown_text",
}
ALLOWED_CLASSIFICATIONS = {
    "canonical",
    "orphan_observation",
    "orphan_intent",
    "orphan_cluster",
    "orphan_research_plan",
    "draft_hypothesis_text",
    "duplicate",
    "malformed",
    "not_hypothesis",
    "needs_operator_review",
}
EVENT_FAMILIES = [
    "oil_shock",
    "volatility_spike",
    "volatility_compression",
    "breadth_collapse",
    "breadth_recovery",
    "gap_event",
    "drawdown_recovery",
    "regime_transition",
    "correlation_break",
    "credit_stress",
    "rate_shock",
    "commodity_dislocation",
    "macro_headline_shock",
    "lottery_event",
]
SEARCH_TERMS = [
    "hypothesis",
    "hypotheses",
    "proposal",
    "HypothesisProposal",
    "event_family",
    "intent_candidate",
    "research idea",
    "volatility compression",
    "breadth collapse",
    "oil shock",
    "gap",
    "drawdown",
    "regime transition",
    "credit stress",
    "rate shock",
    "volatility spike",
    "lottery",
    "ETF drop",
    "mean reversion",
    "reversal",
    "breakout",
    "compression",
    *EVENT_FAMILIES,
]
TEXT_SUFFIXES = {".json", ".jsonl", ".md", ".txt", ".yaml", ".yml", ".py", ".js", ".html", ".csv", ".log"}
SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", ".pytest_cache", "dist", "build"}
MAX_FILE_BYTES = 1_500_000
RECOVERY_LABEL = "proposal only; no evidence yet"

DEFAULT_DISCUSSIONS: list[dict[str, Any]] = [
    {"title": "ETF drop mean reversion", "event_family_id": "volatility_spike", "hypothesis": "Large short-horizon ETF drops may exhibit measurable mean-reversion behavior after controlling for volatility and trend regime.", "candidate_edge_type": "mean_reversion", "symbols": ["SPY", "QQQ", "IWM"], "required_data": ["daily OHLCV for broad ETFs", "forward returns", "regime labels where used"]},
    {"title": "Volatility spike mean reversion", "event_family_id": "volatility_spike", "hypothesis": "Large volatility or range spikes in broad-market ETFs may create short-horizon mean-reversion behavior, conditional on risk regime.", "candidate_edge_type": "mean_reversion"},
    {"title": "Volatility compression breakout", "event_family_id": "volatility_compression", "hypothesis": "Compressed daily range or realized volatility regimes may precede subsequent range expansion or directional breakout.", "candidate_edge_type": "volatility_expansion"},
    {"title": "Breadth collapse recovery", "event_family_id": "breadth_collapse", "hypothesis": "Extreme breadth deterioration may create short-horizon recovery behavior when selling pressure exhausts.", "candidate_edge_type": "risk_off_exhaustion", "requires_operator_review": True},
    {"title": "Breadth recovery continuation", "event_family_id": "breadth_recovery", "hypothesis": "Sharp breadth recovery after broad deterioration may show measurable follow-through over short forward windows.", "candidate_edge_type": "momentum_continuation", "requires_operator_review": True},
    {"title": "Gap-fill after large overnight gaps", "event_family_id": "gap_event", "hypothesis": "Large overnight gaps may show measurable gap-fill behavior depending on regime and opening range.", "candidate_edge_type": "gap_fill"},
    {"title": "Gap continuation after large overnight gaps", "event_family_id": "gap_event", "hypothesis": "Large overnight gaps may show measurable continuation behavior when confirmed by trend and opening range.", "candidate_edge_type": "gap_continuation"},
    {"title": "Drawdown recovery continuation", "event_family_id": "drawdown_recovery", "hypothesis": "Large drawdowns followed by recovery thrusts may show continuation over 5 to 20 sessions.", "candidate_edge_type": "momentum_continuation"},
    {"title": "Risk-off exhaustion reversal", "event_family_id": "breadth_collapse", "hypothesis": "Risk-off episodes with extreme breadth deterioration and volatility stress may reverse when selling pressure exhausts.", "candidate_edge_type": "risk_off_exhaustion", "requires_operator_review": True},
    {"title": "Regime transition fragility", "event_family_id": "regime_transition", "hypothesis": "Transitions between risk-on, mixed, and risk-off regimes may increase fragility of mean-reversion and momentum assumptions.", "candidate_edge_type": "regime_filter", "governance_classification": "regime_filter"},
    {"title": "Credit stress reversal", "event_family_id": "credit_stress", "hypothesis": "Credit ETF stress episodes may show reversal or continuation depending on treasury confirmation and equity regime.", "candidate_edge_type": "risk_off_exhaustion"},
    {"title": "Rate shock reversal", "event_family_id": "rate_shock", "hypothesis": "Large rate-sensitive ETF shocks may show short-horizon reversal after abrupt yield moves, conditional on macro calendar context.", "candidate_edge_type": "mean_reversion"},
    {"title": "Oil shock continuation or reversal", "event_family_id": "oil_shock", "hypothesis": "Large oil or energy ETF shocks following geopolitical or macro headlines may exhibit measurable short-horizon continuation or reversal behavior.", "candidate_edge_type": "momentum_continuation"},
    {"title": "Commodity dislocation reversal", "event_family_id": "commodity_dislocation", "hypothesis": "Large commodity-linked ETF dislocations may show short-horizon reversal when cross-asset confirmation fades.", "candidate_edge_type": "mean_reversion"},
    {"title": "Correlation break regime instability", "event_family_id": "correlation_break", "hypothesis": "Cross-asset correlation breaks may indicate regime instability and alter expected behavior of existing research signals.", "candidate_edge_type": "regime_filter", "governance_classification": "regime_filter"},
    {"title": "Volatility term structure panic normalization", "event_family_id": "volatility_spike", "hypothesis": "Panic behavior in volatility-linked ETFs may normalize over short forward windows after extreme range expansion.", "candidate_edge_type": "volatility_normalization"},
    {"title": "Short-horizon ETF reversion in mixed regimes", "event_family_id": "regime_transition", "hypothesis": "Short-horizon ETF mean reversion may behave differently in mixed regimes than in clear risk-on or risk-off regimes.", "candidate_edge_type": "regime_filter", "governance_classification": "regime_filter"},
    {"title": "Lottery macro/geopolitical dislocation", "event_family_id": "lottery_event", "hypothesis": "Rare macro or geopolitical dislocations may create high-variance, low-frequency opportunities that require stricter evidence and governance thresholds.", "candidate_edge_type": "lottery_dislocation", "governance_classification": "lottery", "expected_fragility": "extreme", "proposal_status": "watchlist", "requires_operator_review": True},
    {"title": "Sector relative-strength divergence", "event_family_id": "regime_transition", "hypothesis": "Sector relative-strength divergence during regime shifts may identify persistent leadership changes for research review.", "candidate_edge_type": "momentum_continuation", "symbols": ["SPY", "RSP", "XLF", "XLK", "XLE", "XLV", "XLY"]},
    {"title": "Sector relative-weakness continuation", "event_family_id": "regime_transition", "hypothesis": "Sector relative weakness during regime transitions may persist over short forward windows and alter basket-level risk assumptions.", "candidate_edge_type": "momentum_continuation", "symbols": ["SPY", "RSP", "XLF", "XLK", "XLE", "XLV", "XLY"]},
]


def recover_hypotheses(*, dry_run: bool, seed_defaults: bool = False, store_root: Path | None = None, actor: str = "recovery_packet", scan_roots: list[Path] | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    _ensure_projection_registry_files(store)
    started_at = utc_now_iso()
    recovery_run_id = f"hrec_{short_hash(content_hash({'started_at': started_at, 'dry_run': dry_run, 'seed_defaults': seed_defaults}), 16)}"
    recovery_dir = store / RECOVERY_ROOT
    recovery_dir.mkdir(parents=True, exist_ok=True)
    if not dry_run:
        seed_event_families(store_root=store, actor=actor)
    canonical_before = _canonical_proposals(store)
    inventory_items, sources_scanned = build_forensic_inventory(store_root=store, scan_roots=scan_roots)
    canonical_index = _canonical_index(canonical_before, store=store)
    inventory_items = [_classify_item(item, canonical_index) for item in inventory_items]
    inventory = {
        "recovery_run_id": recovery_run_id,
        "created_at": started_at,
        "dry_run": bool(dry_run),
        "sources_scanned": sources_scanned,
        "items_discovered": len(inventory_items),
        "items": inventory_items,
        "schema_version": RECOVERY_SCHEMA_VERSION,
    }
    inventory["content_hash"] = content_hash(inventory, exclude={"created_at", "content_hash"}, sort_lists=False)
    write_json(recovery_dir / "hypothesis_forensic_inventory.json", inventory, overwrite=True)
    (recovery_dir / "hypothesis_forensic_inventory.md").write_text(_inventory_markdown(inventory), encoding="utf-8")
    recovered: list[dict[str, Any]] = []
    seeded: list[dict[str, Any]] = []
    warnings: list[str] = []
    errors: list[str] = []
    duplicate_count = sum(1 for item in inventory_items if item.get("classification") == "duplicate")
    malformed_count = sum(1 for item in inventory_items if item.get("classification") == "malformed")
    if not dry_run:
        for item in inventory_items:
            if item.get("canonical_exists"):
                continue
            if _should_recover_item(item):
                try:
                    created = _recover_inventory_item(item, store=store, actor=actor, canonical_index=canonical_index)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"recover_failed:{item.get('recovery_item_id')}:{exc}")
                    continue
                if created:
                    recovered.append(created)
                    canonical_index = _add_to_index(canonical_index, created["proposal"])
        if seed_defaults:
            for spec in DEFAULT_DISCUSSIONS:
                if _find_existing_match(spec["title"], spec["hypothesis"], spec["event_family_id"], canonical_index):
                    duplicate_count += 1
                    continue
                try:
                    created = _create_canonical_chain_from_spec(
                        spec,
                        store=store,
                        actor=actor,
                        source_path="operator_discussed_defaults",
                        recovery_action="seeded_default_discussed_hypothesis",
                        recovery_confidence="medium",
                        needs_operator_review=bool(spec.get("requires_operator_review")),
                    )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"seed_default_failed:{spec['title']}:{exc}")
                    continue
                seeded.append(created)
                canonical_index = _add_to_index(canonical_index, created["proposal"])
        for created in [*recovered, *seeded]:
            proposal_id = created["proposal"]["hypothesis_proposal_id"]
            _ensure_intake_artifacts(proposal_id, store=store, actor=actor, warnings=warnings)
            created["readiness"] = latest_readiness_assessment(proposal_id, store_root=store) or {}
        projection_result = rebuild_research_projections(projection="all", store_root=store, actor=actor, strict=False)
        validation = validate_research_projections(projection="all", store_root=store, actor=actor, strict=True)
    else:
        projection_result = {}
        validation = {}
    canonical_after = _canonical_proposals(store)
    completed_at = utc_now_iso()
    report = {
        "recovery_run_id": recovery_run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "dry_run": bool(dry_run),
        "seed_defaults": bool(seed_defaults),
        "sources_scanned": sources_scanned,
        "items_discovered": len(inventory_items),
        "canonical_before_count": len(canonical_before),
        "canonical_after_count": len(canonical_after),
        "recovered_count": len(recovered),
        "seeded_default_count": len(seeded),
        "duplicate_count": duplicate_count,
        "malformed_count": malformed_count,
        "needs_operator_review_count": _needs_operator_review_count(inventory_items, recovered, seeded),
        "projection_build_id": ((projection_result.get("outputs") or {}).get("hypothesis_queue") or {}).get("build", {}).get("projection_build_id", ""),
        "operator_projection_build_id": ((projection_result.get("outputs") or {}).get("operator_home") or {}).get("build", {}).get("projection_build_id", ""),
        "projection_integrity_status": (((projection_result.get("outputs") or {}).get("hypothesis_queue") or {}).get("projection", {}) or {}).get("integrity_status", "not_rebuilt" if dry_run else "unknown"),
        "projection_validation_ok": bool(validation.get("ok")) if validation else False,
        "warnings": warnings,
        "errors": errors,
        "recovered_proposals": [_report_row(item, "recovered") for item in recovered],
        "seeded_default_proposals": [_report_row(item, "seeded_default") for item in seeded],
        "schema_version": RECOVERY_SCHEMA_VERSION,
    }
    report["content_hash"] = content_hash(report, exclude={"completed_at", "content_hash"}, sort_lists=False)
    write_json(recovery_dir / "hypothesis_recovery_report.json", report, overwrite=True)
    (recovery_dir / "hypothesis_recovery_report.md").write_text(_report_markdown(report), encoding="utf-8")
    append_jsonl(store / "registries" / "hypothesis_recovery_runs.jsonl", {k: report[k] for k in ["recovery_run_id", "started_at", "completed_at", "dry_run", "items_discovered", "canonical_before_count", "canonical_after_count", "recovered_count", "seeded_default_count", "content_hash", "schema_version"]})
    write_audit_event(actor=actor, entity_type="hypothesis_recovery", entity_id=recovery_run_id, action="hypothesis_recovery_completed", previous_state_hash="", new_state_hash=report["content_hash"], reason="Forensic hypothesis recovery completed; no research plans, sleeves, trades, or evidence were auto-created.", metadata={"dry_run": dry_run, "seed_defaults": seed_defaults, "recovered_count": len(recovered), "seeded_default_count": len(seeded), "warnings": warnings}, store_root=store)
    return {"ok": not errors, "inventory": inventory, "report": report, "inventory_path": str(recovery_dir / "hypothesis_forensic_inventory.json"), "report_path": str(recovery_dir / "hypothesis_recovery_report.json")}



def _ensure_projection_registry_files(store: Path) -> None:
    for filename in [
        "hypothesis_proposals.jsonl",
        "hypothesis_proposal_reviews.jsonl",
        "research_readiness_assessments.jsonl",
        "proposal_priority_scores.jsonl",
        "research_intake_dossiers.jsonl",
        "research_plans.jsonl",
        "evidence_packages.jsonl",
        "paper_trials.jsonl",
        "paper_trial_operations_reports.jsonl",
        "sleeve_definitions.jsonl",
        "sleeve_reviews.jsonl",
        "sleeve_health_snapshots.jsonl",
    ]:
        path = store / "registries" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("", encoding="utf-8")

def build_forensic_inventory(*, store_root: Path | None = None, scan_roots: list[Path] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    store = ensure_store_layout(store_root)
    roots = _default_scan_roots(store) if scan_roots is None else scan_roots
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in _iter_scan_files(root):
            for item in _items_from_path(path):
                key = content_hash({"source_path": item["source_path"], "raw_title": item.get("raw_title"), "raw_text": item.get("raw_text")})
                if key in seen:
                    continue
                seen.add(key)
                items.append(item)
    items.sort(key=lambda item: (item["source_type"], item["source_path"], item.get("raw_title") or ""))
    return items, [str(root) for root in roots if root.exists()]


def latest_hypothesis_recovery_report(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    path = store / RECOVERY_ROOT / "hypothesis_recovery_report.json"
    if not path.exists():
        return {"ok": False, "error": "hypothesis recovery report missing", "path": str(path)}
    return {"ok": True, "report": read_json(path), "path": str(path)}


def _default_scan_roots(store: Path) -> list[Path]:
    repo = project_root()
    roots = [store, repo / "research_lab", repo / "ops", repo / "constellation_2", repo / "logs"]
    runtime = repo.parent / "constellation_runtime_data"
    if runtime.exists():
        roots.append(runtime)
    return roots


def _iter_scan_files(root: Path):
    if root.is_file():
        if _is_scan_file(root):
            yield root
        return
    for path in root.rglob("*"):
        text_path = str(path)
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if "research_store/recovery/hypothesis_recovery" in text_path or "research_store/projections" in text_path:
            continue
        if path.is_file() and _is_scan_file(path):
            yield path


def _is_scan_file(path: Path) -> bool:
    try:
        if path.stat().st_size > MAX_FILE_BYTES:
            return False
    except OSError:
        return False
    return path.suffix.lower() in TEXT_SUFFIXES


def _items_from_path(path: Path) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []
    if not _looks_relevant(text, path):
        return []
    source_type = _source_type_for_path(path)
    parsed_items = _structured_items(path, text, source_type)
    if parsed_items:
        return parsed_items
    return [_inventory_item(path=path, source_type=source_type, raw_title=_title_from_text(text, path), raw_text=_snippet(text), payload={})]


def _structured_items(path: Path, text: str, source_type: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return [_inventory_item(path=path, source_type=source_type, raw_title=path.stem, raw_text=_snippet(text), payload={}, classification="malformed", blocking_reason="json_parse_error")]
        if isinstance(payload, dict):
            maybe = _item_from_payload(path, payload, source_type)
            if maybe:
                out.append(maybe)
    elif path.suffix.lower() == ".jsonl":
        for idx, line in enumerate(text.splitlines(), start=1):
            if not line.strip() or not _looks_relevant(line, path):
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                maybe = _item_from_payload(Path(f"{path}:{idx}"), payload, source_type)
                if maybe:
                    out.append(maybe)
    return out


def _item_from_payload(path: Path, payload: dict[str, Any], source_type: str) -> dict[str, Any] | None:
    schema = str(payload.get("schema_version") or payload.get("schema_id") or "")
    if source_type == "registry_row":
        raw = json.dumps(payload, sort_keys=True)[:2000]
        return _inventory_item(path=path, source_type="registry_row", raw_title=str(payload.get("hypothesis_proposal_id") or payload.get("event_observation_id") or payload.get("event_cluster_id") or payload.get("intent_candidate_id") or payload.get("research_plan_id") or path.stem), raw_text=raw, payload=payload)
    if payload.get("hypothesis_proposal_id") and (payload.get("hypothesis") or payload.get("proposed_hypothesis_statement") or payload.get("title") or payload.get("proposal_label")):
        return _inventory_item(path=path, source_type="canonical_proposal" if "event_intake/hypothesis_proposals" in str(path) else source_type, raw_title=str(payload.get("title") or payload.get("proposal_label") or payload.get("hypothesis_proposal_id")), raw_text=str(payload.get("hypothesis") or payload.get("proposed_hypothesis_statement") or payload.get("proposed_research_question") or ""), payload=payload)
    if payload.get("event_observation_id"):
        return _inventory_item(path=path, source_type="event_observation", raw_title=str(payload.get("title") or payload.get("event_observation_id")), raw_text=str(payload.get("description") or payload.get("suspected_mechanism") or ""), payload=payload)
    if payload.get("intent_candidate_id"):
        return _inventory_item(path=path, source_type="intent_candidate", raw_title=str(payload.get("intent_name") or payload.get("intent_candidate_id")), raw_text=str(payload.get("intent_description") or ""), payload=payload)
    if payload.get("event_cluster_id"):
        return _inventory_item(path=path, source_type="event_cluster", raw_title=str(payload.get("cluster_title") or payload.get("event_cluster_id")), raw_text=str(payload.get("cluster_description") or ""), payload=payload)
    if payload.get("research_plan_id"):
        return _inventory_item(path=path, source_type="research_plan", raw_title=str(payload.get("title") or payload.get("hypothesis") or payload.get("research_plan_id")), raw_text=str(payload.get("hypothesis") or ""), payload=payload)
    if "audit" in str(path) and any(key in payload for key in ("reason", "action", "metadata")):
        raw = json.dumps(payload, sort_keys=True)[:2000]
        return _inventory_item(path=path, source_type="audit_log", raw_title=str(payload.get("action") or payload.get("entity_id") or path.name), raw_text=raw, payload=payload)
    if schema or any(term in json.dumps(payload, sort_keys=True).lower() for term in ("hypothesis", "proposal", "research idea")):
        raw = json.dumps(payload, sort_keys=True)[:2000]
        return _inventory_item(path=path, source_type=source_type, raw_title=str(payload.get("title") or payload.get("name") or path.stem), raw_text=raw, payload=payload)
    return None


def _inventory_item(*, path: Path, source_type: str, raw_title: str, raw_text: str, payload: dict[str, Any], classification: str | None = None, blocking_reason: str = "") -> dict[str, Any]:
    combined = f"{raw_title}\n{raw_text}\n{json.dumps(payload, sort_keys=True)[:2000]}"
    family = _detect_event_family(combined)
    symbols = _detect_symbols(combined)
    status = str(payload.get("proposal_status") or payload.get("status") or payload.get("review_decision") or "unknown")
    confidence = _confidence_for_text(combined, payload)
    item = {
        "recovery_item_id": f"hri_{short_hash(content_hash({'source_path': str(path), 'title': raw_title, 'text': raw_text}), 16)}",
        "source_path": str(path),
        "source_type": source_type if source_type in ALLOWED_SOURCE_TYPES else "unknown_text",
        "raw_title": str(raw_title or path.stem)[:240],
        "raw_text": str(raw_text or "")[:4000],
        "detected_event_family": family,
        "detected_symbols": symbols,
        "detected_status": status,
        "confidence": confidence,
        "canonical_exists": False,
        "canonical_hypothesis_proposal_id": "",
        "classification": classification or _initial_classification(source_type, combined),
        "blocking_reason": blocking_reason,
        "recommended_recovery_action": "classify",
        "schema_version": RECOVERY_SCHEMA_VERSION,
    }
    if item["classification"] not in ALLOWED_CLASSIFICATIONS:
        item["classification"] = "needs_operator_review"
    return item


def _classify_item(item: dict[str, Any], canonical_index: dict[str, Any]) -> dict[str, Any]:
    match = _find_existing_match(item.get("raw_title", ""), item.get("raw_text", ""), item.get("detected_event_family", ""), canonical_index)
    if item["source_type"] == "canonical_proposal":
        item["classification"] = "canonical"
        item["canonical_exists"] = True
        item["canonical_hypothesis_proposal_id"] = item.get("raw_title") if str(item.get("raw_title", "")).startswith("ehp_") else (match or "")
        item["recommended_recovery_action"] = "none_canonical"
        return item
    linked_match = _linked_existing_match(item, canonical_index)
    if linked_match:
        item["classification"] = "duplicate"
        item["canonical_exists"] = True
        item["canonical_hypothesis_proposal_id"] = linked_match
        item["recommended_recovery_action"] = "skip_duplicate_linked_canonical_chain"
        return item
    if match:
        item["classification"] = "duplicate"
        item["canonical_exists"] = True
        item["canonical_hypothesis_proposal_id"] = match
        item["recommended_recovery_action"] = "skip_duplicate"
        return item
    if item.get("classification") == "malformed":
        item["recommended_recovery_action"] = "operator_review_source"
        return item
    source_type = item.get("source_type")
    if source_type == "event_observation":
        item["classification"] = "orphan_observation"
        item["recommended_recovery_action"] = "create_cluster_intent_proposal"
    elif source_type == "intent_candidate":
        item["classification"] = "orphan_intent"
        item["recommended_recovery_action"] = "create_proposal_from_intent_if_linkage_valid"
    elif source_type == "event_cluster":
        item["classification"] = "orphan_cluster"
        item["recommended_recovery_action"] = "create_intent_proposal"
    elif source_type == "research_plan":
        item["classification"] = "orphan_research_plan"
        item["recommended_recovery_action"] = "recapture_as_hypothesis_proposal_only"
    elif _is_hypothesis_like(item):
        item["classification"] = "draft_hypothesis_text" if item.get("confidence") != "low" else "needs_operator_review"
        item["recommended_recovery_action"] = "recapture_as_low_confidence_proposal"
    else:
        item["classification"] = "not_hypothesis"
        item["recommended_recovery_action"] = "none"
    return item


def _initial_classification(source_type: str, text: str) -> str:
    if source_type == "canonical_proposal":
        return "canonical"
    if source_type == "event_observation":
        return "orphan_observation"
    if source_type == "intent_candidate":
        return "orphan_intent"
    if source_type == "event_cluster":
        return "orphan_cluster"
    if source_type == "research_plan":
        return "orphan_research_plan"
    if _text_is_hypothesis_like(text):
        return "draft_hypothesis_text"
    return "not_hypothesis"



def _should_recover_item(item: dict[str, Any]) -> bool:
    if item.get("canonical_exists"):
        return False
    if item.get("classification") in {"orphan_observation", "orphan_intent", "orphan_cluster", "orphan_research_plan"}:
        return True
    if item.get("classification") in {"draft_hypothesis_text", "needs_operator_review"} and item.get("source_type") == "operator_note":
        return True
    return False


def _linked_existing_match(item: dict[str, Any], canonical_index: dict[str, Any]) -> str:
    payload = _payload_from_item(item)
    source_type = item.get("source_type")
    if source_type == "event_observation":
        return canonical_index.get("by_observation", {}).get(str(payload.get("event_observation_id") or ""), "")
    if source_type == "event_cluster":
        return canonical_index.get("by_cluster", {}).get(str(payload.get("event_cluster_id") or ""), "")
    if source_type == "intent_candidate":
        return canonical_index.get("by_intent", {}).get(str(payload.get("intent_candidate_id") or ""), "")
    return ""


def _payload_from_item(item: dict[str, Any]) -> dict[str, Any]:
    path_text = str(item.get("source_path") or "")
    path = Path(path_text.split(":", 1)[0])
    if path.exists() and path.suffix == ".json":
        try:
            return read_json(path)
        except Exception:  # noqa: BLE001
            return {}
    return {}

def _recover_inventory_item(item: dict[str, Any], *, store: Path, actor: str, canonical_index: dict[str, Any]) -> dict[str, Any] | None:
    if _find_existing_match(item.get("raw_title", ""), item.get("raw_text", ""), item.get("detected_event_family", ""), canonical_index):
        return None
    spec = {
        "title": _clean_title(item.get("raw_title") or "Recovered hypothesis"),
        "event_family_id": item.get("detected_event_family") or "macro_headline_shock",
        "hypothesis": _hypothesis_from_item(item),
        "candidate_edge_type": _edge_for_family(item.get("detected_event_family") or "macro_headline_shock"),
        "symbols": item.get("detected_symbols") or [],
        "proposal_status": "watchlist" if item.get("classification") == "needs_operator_review" else "proposed",
        "requires_operator_review": item.get("classification") == "needs_operator_review",
    }
    return _create_canonical_chain_from_spec(spec, store=store, actor=actor, source_path=item["source_path"], recovery_action=f"recovered_from_{item['classification']}", recovery_confidence=item.get("confidence") or "low", needs_operator_review=item.get("classification") == "needs_operator_review")


def _create_canonical_chain_from_spec(spec: dict[str, Any], *, store: Path, actor: str, source_path: str, recovery_action: str, recovery_confidence: str, needs_operator_review: bool = False) -> dict[str, Any]:
    family_id = str(spec.get("event_family_id") or "macro_headline_shock").strip().lower()
    try:
        family = load_event_family(family_id, store_root=store)
    except Exception:  # noqa: BLE001
        family = _family_from_spec(family_id, store=store, actor=actor)
    template = _template_for_recovery(spec, family)
    observation = build_event_observation(
        event_family_id=family_id,
        title=str(spec["title"]),
        description=str(spec.get("hypothesis") or spec["title"]),
        observed_by=actor,
        source_type="recovered_artifact",
        source_ref=str(source_path),
        symbols_mentioned=list(spec.get("symbols") or family.get("default_symbols") or []),
        market_context={"recovery_source_path": str(source_path), "recovery_action": recovery_action, "recovery_confidence": recovery_confidence, "proposal_only_no_evidence": True},
        suspected_mechanism=str(spec.get("hypothesis") or spec["title"]),
        confidence_level="low" if recovery_confidence in {"low", "unknown"} else "unknown",
        research_priority="watchlist" if needs_operator_review or spec.get("proposal_status") == "watchlist" else "medium",
        notes=f"recovered_artifact; recovery_packet; {RECOVERY_LABEL}",
    )
    observation_result = store_event_observation(observation, store_root=store, actor=actor)
    cluster = build_event_cluster(event_family=family, observations=[observation], cluster_title=f"Recovered: {spec['title']}", cluster_description=f"Recovered hypothesis intake from {source_path}")
    cluster_result = store_event_cluster(cluster, store_root=store, actor=actor)
    intent = build_intent_candidate(event_cluster=cluster, template=template)
    intent_result = store_intent_candidate(intent, store_root=store, actor=actor)
    proposal = build_hypothesis_proposal(intent_candidate=intent, event_family=family, template=template, proposal_status=str(spec.get("proposal_status") or ("watchlist" if needs_operator_review else "proposed")), created_by=actor)
    proposal["recovery_metadata"] = {
        "source_type": "recovered_artifact",
        "created_by": actor,
        "recovery_source_path": str(source_path),
        "recovery_confidence": recovery_confidence,
        "recovery_action": recovery_action,
        "needs_operator_review": bool(needs_operator_review),
        "research_label": RECOVERY_LABEL,
    }
    proposal["evidence_requirements"] = list(proposal.get("evidence_requirements") or []) + ["Recovered or seeded proposal has no evidence yet."]
    proposal["content_hash"] = recompute_hypothesis_proposal_hash(proposal)
    validate_hypothesis_proposal(proposal)
    proposal_result = store_hypothesis_proposal(proposal, store_root=store, actor=actor)
    return {"proposal": proposal, "observation": observation_result["event_observation"], "cluster": cluster_result["event_cluster"], "intent": intent_result["intent_candidate"], "proposal_result": proposal_result, "source_path": source_path, "recovery_action": recovery_action, "needs_operator_review": needs_operator_review}


def _ensure_intake_artifacts(proposal_id: str, *, store: Path, actor: str, warnings: list[str]) -> None:
    try:
        readiness = latest_readiness_assessment(proposal_id, store_root=store)
        if readiness is None:
            readiness = assess_hypothesis_readiness(hypothesis_proposal_id=proposal_id, store_root=store, actor=actor)["readiness_assessment"]
        if latest_priority_score(proposal_id, store_root=store) is None:
            score_hypothesis_proposal(hypothesis_proposal_id=proposal_id, readiness=readiness, store_root=store, actor=actor)
        if latest_research_intake_dossier(proposal_id, store_root=store) is None:
            build_and_store_research_intake_dossier(hypothesis_proposal_id=proposal_id, store_root=store, actor=actor)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"intake_artifact_failed:{proposal_id}:{exc}")


def _template_for_recovery(spec: dict[str, Any], family: dict[str, Any]) -> dict[str, Any]:
    base = intent_template(str(family["event_family_id"]))
    edge = str(spec.get("candidate_edge_type") or base.get("candidate_edge_type") or _edge_for_family(str(family["event_family_id"])))
    governance_notes = list(base.get("governance_notes") or []) + ["recovered_or_seeded_proposal", "proposal_only_no_evidence"]
    if edge == "lottery_dislocation" and "stricter_review_required" not in governance_notes:
        governance_notes.append("stricter_review_required")
    return {
        **base,
        "intent_name": _slug(str(spec["title"])),
        "intent_description": str(spec.get("hypothesis") or spec["title"]),
        "candidate_edge_type": edge,
        "expected_fragility": str(spec.get("expected_fragility") or ("extreme" if edge == "lottery_dislocation" else base.get("expected_fragility") or "high")),
        "required_data": list(spec.get("required_data") or base.get("required_data") or ["daily OHLCV", "forward returns"]),
        "governance_notes": governance_notes,
        "title": str(spec["title"]),
        "hypothesis": str(spec.get("hypothesis") or spec["title"]),
        "primary_test": str(spec.get("primary_test") or base.get("primary_test") or "event-study definition to be specified before formal research"),
        "governance_classification": str(spec.get("governance_classification") or ("lottery" if edge == "lottery_dislocation" else "experimental")),
    }


def _family_from_spec(family_id: str, *, store: Path, actor: str) -> dict[str, Any]:
    spec = event_family_spec(family_id)
    from research_lab.event_intake.event_family import build_event_family
    from research_lab.event_intake.event_intake_registry import store_event_family

    family = build_event_family(spec)
    validate_event_family(family)
    store_event_family(family, store_root=store, actor=actor)
    return family


def _canonical_proposals(store: Path) -> list[dict[str, Any]]:
    proposals: dict[str, dict[str, Any]] = {}
    for path in sorted((store / "event_intake" / "hypothesis_proposals").glob("*.json")):
        try:
            payload = read_json(path)
        except Exception:  # noqa: BLE001
            continue
        proposal_id = str(payload.get("hypothesis_proposal_id") or path.stem)
        proposals[proposal_id] = payload
    for row in read_jsonl(store / "registries" / "hypothesis_proposals.jsonl"):
        proposal_id = str(row.get("hypothesis_proposal_id") or "")
        if proposal_id and proposal_id not in proposals:
            proposals[proposal_id] = row
    return list(proposals.values())


def _canonical_index(proposals: list[dict[str, Any]], *, store: Path) -> dict[str, Any]:
    out = {"by_id": {}, "by_observation": {}, "by_cluster": {}, "by_intent": {}, "items": []}
    for proposal in proposals:
        _add_to_index(out, proposal)
        proposal_id = str(proposal.get("hypothesis_proposal_id") or "")
        intent_id = str(proposal.get("intent_candidate_id") or "")
        cluster_id = str(proposal.get("event_cluster_id") or "")
        if intent_id:
            out["by_intent"][intent_id] = proposal_id
        if cluster_id:
            out["by_cluster"][cluster_id] = proposal_id
            cluster_path = store / "event_intake" / "clusters" / f"{cluster_id}.json"
            if cluster_path.exists():
                try:
                    cluster = read_json(cluster_path)
                    for observation_id in cluster.get("observation_ids") or []:
                        out["by_observation"][str(observation_id)] = proposal_id
                except Exception:  # noqa: BLE001
                    pass
    return out


def _add_to_index(index: dict[str, Any], proposal: dict[str, Any]) -> dict[str, Any]:
    proposal_id = str(proposal.get("hypothesis_proposal_id") or "")
    title = str(proposal.get("title") or proposal.get("proposal_label") or "")
    hypothesis = str(proposal.get("hypothesis") or proposal.get("proposed_hypothesis_statement") or proposal.get("proposed_research_question") or "")
    item = {"id": proposal_id, "title": title, "title_norm": _normalize(title), "hypothesis": hypothesis, "hypothesis_norm": _normalize(hypothesis), "event_family_id": str(proposal.get("event_family_id") or "")}
    if proposal_id:
        index["by_id"][proposal_id] = item
    index["items"].append(item)
    return index


def _find_existing_match(title: str, hypothesis: str, event_family_id: str, canonical_index: dict[str, Any]) -> str:
    title_norm = _normalize(title)
    hyp_norm = _normalize(hypothesis)
    if title_norm.startswith("ehp_") and title_norm in canonical_index.get("by_id", {}):
        return title_norm
    best_id = ""
    best_score = 0.0
    for item in canonical_index.get("items", []):
        score = max(_similarity(title_norm, item["title_norm"]), _similarity(hyp_norm, item["hypothesis_norm"]))
        if event_family_id and item.get("event_family_id") == event_family_id:
            score += 0.05
        if title_norm and title_norm == item["title_norm"]:
            score = 1.0
        if score > best_score:
            best_score = score
            best_id = item["id"]
    return best_id if best_score >= 0.88 else ""


def _looks_relevant(text: str, path: Path) -> bool:
    lower = f"{path.name}\n{text[:5000]}".lower()
    return any(term.lower() in lower for term in SEARCH_TERMS)


def _text_is_hypothesis_like(text: str) -> bool:
    lower = text.lower()
    return any(term in lower for term in ["hypothesis", "research idea", "mean reversion", "reversal", "breakout", "continuation", "event study", "proposal"])


def _is_hypothesis_like(item: dict[str, Any]) -> bool:
    return _text_is_hypothesis_like(f"{item.get('raw_title')} {item.get('raw_text')}") and bool(item.get("detected_event_family"))


def _source_type_for_path(path: Path) -> str:
    text = str(path)
    if "event_intake/hypothesis_proposals" in text:
        return "canonical_proposal"
    if "event_intake/observations" in text:
        return "event_observation"
    if "event_intake/intents" in text:
        return "intent_candidate"
    if "event_intake/clusters" in text:
        return "event_cluster"
    if "research_plans" in text:
        return "research_plan"
    if "audit_log" in text or "audit" in path.name:
        return "audit_log"
    if "registries" in text:
        return "registry_row"
    if "tests" in text or "fixtures" in text:
        return "test_fixture"
    if "static" in text or "ui" in text:
        return "ui_seed"
    if "logs" in text or path.suffix == ".log":
        return "operator_note"
    return "unknown_text"


def _detect_event_family(text: str) -> str:
    lower = text.lower().replace("-", "_")
    for family in EVENT_FAMILIES:
        if family in lower or family.replace("_", " ") in lower:
            return family
    rules = [
        ("oil_shock", ["oil", "crude", "uso", "xle", "xop", "energy"]),
        ("volatility_compression", ["volatility compression", "compressed", "breakout", "range expansion"]),
        ("volatility_spike", ["volatility spike", "vol spike", "vix", "vixy", "vxx", "etf drop", "mean reversion"]),
        ("breadth_collapse", ["breadth collapse", "breadth deterioration", "risk-off exhaustion", "risk_off exhaustion"]),
        ("breadth_recovery", ["breadth recovery"]),
        ("gap_event", ["gap-fill", "gap fill", "overnight gap", " gap "]),
        ("drawdown_recovery", ["drawdown"]),
        ("regime_transition", ["regime", "risk_on", "risk-off", "risk off", "sector relative"]),
        ("credit_stress", ["credit", "hyg", "lqd"]),
        ("rate_shock", ["rate shock", "treasury", "yield", "tlt", "ief", "shy"]),
        ("commodity_dislocation", ["commodity", "dbc", "gold", "gld"]),
        ("lottery_event", ["lottery", "geopolitical dislocation", "tail risk"]),
    ]
    for family, needles in rules:
        if any(needle in lower for needle in needles):
            return family
    return "macro_headline_shock" if "headline" in lower or "macro" in lower else ""


def _detect_symbols(text: str) -> list[str]:
    known = {"SPY", "QQQ", "IWM", "VIXY", "VXX", "SVXY", "USO", "XLE", "XOP", "DBC", "GLD", "TLT", "IEF", "SHY", "HYG", "LQD", "RSP", "XLF", "XLK", "XLV", "XLY"}
    found = set(re.findall(r"\b[A-Z]{2,5}\b", text))
    return sorted(found.intersection(known))


def _confidence_for_text(text: str, payload: dict[str, Any]) -> str:
    value = str(payload.get("confidence_level") or "").lower()
    if value in {"low", "medium", "high", "unknown"}:
        return value
    lower = text.lower()
    if "unverified" in lower or "claim" in lower or "fixture" in lower:
        return "low"
    if "hypothesis" in lower or "research" in lower:
        return "medium"
    return "unknown"


def _edge_for_family(family_id: str) -> str:
    return {
        "volatility_spike": "mean_reversion",
        "volatility_compression": "volatility_expansion",
        "breadth_collapse": "risk_off_exhaustion",
        "breadth_recovery": "momentum_continuation",
        "gap_event": "gap_fill",
        "drawdown_recovery": "momentum_continuation",
        "regime_transition": "regime_filter",
        "correlation_break": "regime_filter",
        "credit_stress": "risk_off_exhaustion",
        "rate_shock": "mean_reversion",
        "commodity_dislocation": "mean_reversion",
        "lottery_event": "lottery_dislocation",
    }.get(family_id, "momentum_continuation")


def _hypothesis_from_item(item: dict[str, Any]) -> str:
    text = str(item.get("raw_text") or "").strip()
    title = str(item.get("raw_title") or "Recovered hypothesis").strip()
    if len(text) >= 30 and not text.startswith("{"):
        return text[:800]
    return f"{title} may represent a researchable market-event behavior. This recovered proposal requires operator review and public, reproducible event definitions before formal research."


def _clean_title(title: str) -> str:
    title = re.sub(r"[_{}\[\]\"]+", " ", str(title)).strip()
    title = re.sub(r"\s+", " ", title)
    return title[:120] or "Recovered hypothesis"


def _title_from_text(text: str, path: Path) -> str:
    for line in text.splitlines():
        clean = line.strip("# -*\t")
        if 8 <= len(clean) <= 160 and _text_is_hypothesis_like(clean):
            return clean
    return path.stem


def _snippet(text: str) -> str:
    lower = text.lower()
    positions = [lower.find(term.lower()) for term in SEARCH_TERMS if lower.find(term.lower()) >= 0]
    start = max(0, min(positions) - 300) if positions else 0
    return text[start : start + 1200]


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return min(len(a), len(b)) / max(len(a), len(b))
    return SequenceMatcher(None, a, b).ratio()


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug[:80] or "recovered_hypothesis"


def _needs_operator_review_count(inventory_items: list[dict[str, Any]], recovered: list[dict[str, Any]], seeded: list[dict[str, Any]]) -> int:
    return sum(1 for item in inventory_items if item.get("classification") == "needs_operator_review") + sum(1 for item in [*recovered, *seeded] if item.get("needs_operator_review"))


def _report_row(created: dict[str, Any], action: str) -> dict[str, Any]:
    proposal = created["proposal"]
    readiness = created.get("readiness") or {}
    return {
        "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
        "title": proposal["title"],
        "event_family": proposal["event_family_id"],
        "source": created.get("source_path", ""),
        "recovery_action": action if action != "recovered" else created.get("recovery_action", "recovered"),
        "proposal_status": proposal["proposal_status"],
        "readiness_status": readiness.get("data_requirement_status", "assessed"),
        "blocking_items": readiness.get("blocking_items", []),
    }


def _inventory_markdown(inventory: dict[str, Any]) -> str:
    lines = ["# Hypothesis Forensic Inventory", "", f"Run: {inventory['recovery_run_id']}", f"Items discovered: {inventory['items_discovered']}", ""]
    for item in inventory.get("items", []):
        lines.append(f"- {item['recovery_item_id']} | {item['classification']} | {item['source_type']} | {item['raw_title']} | {item['source_path']}")
    return "\n".join(lines) + "\n"


def _report_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Hypothesis Recovery Report",
        "",
        f"Run: {report['recovery_run_id']}",
        f"Canonical before: {report['canonical_before_count']}",
        f"Canonical after: {report['canonical_after_count']}",
        f"Items discovered: {report['items_discovered']}",
        f"Recovered: {report['recovered_count']}",
        f"Seeded defaults: {report['seeded_default_count']}",
        f"Duplicates skipped: {report['duplicate_count']}",
        f"Needs operator review: {report['needs_operator_review_count']}",
        f"Projection integrity: {report['projection_integrity_status']}",
        "",
        "Recovered and seeded proposals are proposal-only research intake records. No evidence, ResearchPlan, sleeve, trade, or allocation was created automatically.",
        "",
        "## Proposals",
    ]
    for row in [*report.get("recovered_proposals", []), *report.get("seeded_default_proposals", [])]:
        lines.append(f"- {row['hypothesis_proposal_id']} | {row['event_family']} | {row['proposal_status']} | {row['title']} | blockers: {', '.join(row.get('blocking_items') or []) or 'none'}")
    if report.get("warnings"):
        lines.extend(["", "## Warnings", *[f"- {item}" for item in report["warnings"]]])
    if report.get("errors"):
        lines.extend(["", "## Errors", *[f"- {item}" for item in report["errors"]]])
    return "\n".join(lines) + "\n"
