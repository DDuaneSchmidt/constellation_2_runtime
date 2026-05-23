from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.sleeves.sleeve_registry import load_sleeve_version
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


GENERATED_AT = "1970-01-01T00:00:00Z"
RESEARCH_LABEL = "RESEARCH_ONLY"
SCHEMA_VERSION = "challenger_research_track.v1"


def _report_path(store: Path, family: str, report_id: str) -> Path:
    return store / "stability_reports" / family / f"{report_id}.json"


def _track_dir(store: Path) -> Path:
    return store / "challenger_tracks"


def _track_path(store: Path, challenger_track_id: str) -> Path:
    return _track_dir(store) / f"{challenger_track_id}.json"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "challenger_research_tracks.jsonl"


def _artifact_ref(path: Path, artifact_id: str, artifact_type: str) -> dict[str, Any]:
    if not path.exists():
        return {
            "artifact_id": artifact_id,
            "artifact_type": artifact_type,
            "path": str(path),
            "status": "missing",
            "content_hash": "",
        }
    payload = read_json(path)
    return {
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "path": str(path),
        "status": "available",
        "content_hash": str(payload.get("content_hash") or payload.get("manifest_hash") or ""),
    }


def _hypothesis(hypothesis_id: str, parent_sleeve_id: str, variant_name: str, delta: dict[str, Any], rationale: str, failure_mode: str) -> dict[str, Any]:
    return {
        "challenger_hypothesis_id": hypothesis_id,
        "parent_sleeve_id": parent_sleeve_id,
        "variant_name": variant_name,
        "deterministic_rule_delta": delta,
        "rationale": rationale,
        "expected_failure_mode": failure_mode,
        "required_evidence": [
            "DatasetSnapshot reference",
            "RegimeSnapshot reference",
            "CostModelSnapshot reference",
            "event-study evidence",
            "backtest evidence where applicable",
            "longitudinal candidate/outcome evidence",
            "regime-fragility evidence",
            "deterministic audit trail",
        ],
        "status": "proposed",
    }


def deterministic_challenger_hypotheses(parent_sleeve_id: str) -> list[dict[str, Any]]:
    return [
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_stricter_signal_threshold",
            parent_sleeve_id,
            "stricter_signal_threshold",
            {"signal_threshold": "more_negative_than_incumbent_by_fixed_50bp"},
            "Tests whether the degrading expectancy is caused by weak marginal drop signals.",
            "Sample size may fall below evidence sufficiency threshold.",
        ),
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_shorter_holding_period",
            parent_sleeve_id,
            "shorter_holding_period",
            {"holding_period": "reduce_to_nearest_supported_shorter_window"},
            "Tests whether reversion edge decays faster than the incumbent hold window.",
            "May increase turnover proxy and cost sensitivity.",
        ),
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_longer_holding_period",
            parent_sleeve_id,
            "longer_holding_period",
            {"holding_period": "increase_to_nearest_supported_longer_window"},
            "Tests whether incumbent exits before recovery completes.",
            "May increase drawdown and regime exposure concentration.",
        ),
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_regime_filtered",
            parent_sleeve_id,
            "regime_filtered_variant",
            {"risk_regime_filter": "allow_only_historically_non_negative_regime_buckets"},
            "Tests whether degrading expectancy is concentrated in hostile regimes.",
            "May overfit thin regime samples and reduce opportunity count.",
        ),
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_cost_sensitivity",
            parent_sleeve_id,
            "cost_sensitivity_variant",
            {"cost_model_stress": "require_positive_expectancy_under_higher_cost_assumption"},
            "Tests whether reported edge is fragile to transaction cost assumptions.",
            "May exclude otherwise valid low-cost environments.",
        ),
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_rank_threshold",
            parent_sleeve_id,
            "rank_threshold_variant",
            {"ranking_policy": "evaluate_only_top_rank_bucket_candidates"},
            "Tests whether candidate ranking contains enough signal to filter weaker setups.",
            "Fails closed if rank usefulness evidence is weak or unavailable.",
        ),
        _hypothesis(
            "chl_slv_etf_drop_reversion_v1_lower_turnover",
            parent_sleeve_id,
            "lower_turnover_variant",
            {"cooldown_rule": "fixed_minimum_gap_between_same_symbol_candidates"},
            "Tests whether repeated signals degrade due to clustering and turnover.",
            "May reduce measured observations and delay evidence sufficiency.",
        ),
    ]


def _comparison_requirements() -> dict[str, Any]:
    return {
        "required_metrics": [
            "post-cost expectancy",
            "win rate",
            "median return",
            "downside tail metric",
            "turnover proxy if available",
            "benchmark-relative return if available",
            "rank usefulness if applicable",
            "regime-specific expectancy",
            "sample size",
            "evidence quality classification",
        ],
        "winner_policy": "do_not_declare_winner_unless_evidence_sufficiency_rules_are_met",
        "evidence_sufficiency_rules": {
            "minimum_measured_candidates": 40,
            "minimum_regime_bucket_observations": 10,
            "requires_post_cost_positive_expectancy": True,
            "requires_research_only_labels": True,
            "requires_deterministic_audit_trail": True,
        },
    }


def _evidence_requirements(version: dict[str, Any], stability: dict[str, Any], drift: dict[str, Any], fragility: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_snapshot_id": version.get("dataset_snapshot_id", ""),
        "regime_snapshot_id": version.get("regime_snapshot_id", ""),
        "cost_model_snapshot_id": version.get("cost_model_snapshot_id", ""),
        "event_study_evidence_package_id": version.get("linked_event_study_evidence_package_id", ""),
        "backtest_evidence_package_id": version.get("linked_backtest_evidence_package_id", ""),
        "candidate_batch_ids": version.get("linked_candidate_batch_ids", []),
        "longitudinal_run_id": drift.get("longitudinal_run_id", "") or fragility.get("longitudinal_run_id", ""),
        "expectancy_drift_report_id": drift.get("expectancy_drift_report_id", ""),
        "regime_fragility_report_id": fragility.get("regime_fragility_report_id", ""),
        "sleeve_stability_report_id": stability.get("sleeve_stability_report_id", ""),
        "deterministic_audit_trail_required": True,
        "fail_closed_if_missing": True,
    }


def _exclusion_rules() -> list[str]:
    return [
        "insufficient observations",
        "negative post-cost expectancy",
        "unstable or degrading recent expectancy",
        "excessive regime concentration",
        "missing cost model",
        "missing dataset snapshot",
        "non-deterministic inputs",
        "any research-only label missing",
        "any artifact mutation detected",
    ]


def _governance_constraints() -> dict[str, bool]:
    return {
        "challenger_creation_promotes_sleeve": False,
        "challenger_creation_retires_incumbent": False,
        "challenger_creation_modifies_paper_trial": False,
        "challenger_creation_modifies_candidate_ledger": False,
        "challenger_creation_authorizes_trading": False,
        "human_review_required_before_future_lifecycle_change": True,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_trading_allowed": False,
        "order_management_allowed": False,
        "portfolio_optimizer_allowed": False,
        "capital_allocation_allowed": False,
        "automatic_promotion_allowed": False,
        "sleeve_mutation_allowed": False,
    }


def _required_source_refs(store: Path, version: dict[str, Any], stability_report_id: str, drift_report_id: str, fragility_report_id: str) -> list[dict[str, Any]]:
    refs = [
        _artifact_ref(store / "datasets" / str(version.get("dataset_snapshot_id") or "") / "dataset_snapshot.json", str(version.get("dataset_snapshot_id") or ""), "DatasetSnapshot"),
        _artifact_ref(store / "regimes" / str(version.get("regime_snapshot_id") or "") / "regime_snapshot.json", str(version.get("regime_snapshot_id") or ""), "RegimeSnapshot"),
        _artifact_ref(store / "cost_models" / str(version.get("cost_model_snapshot_id") or "") / "cost_model_snapshot.json", str(version.get("cost_model_snapshot_id") or ""), "CostModelSnapshot"),
        _artifact_ref(store / "evidence_packages" / str(version.get("linked_event_study_evidence_package_id") or "") / "evidence_manifest.json", str(version.get("linked_event_study_evidence_package_id") or ""), "event-study evidence"),
        _artifact_ref(store / "evidence_packages" / str(version.get("linked_backtest_evidence_package_id") or "") / "evidence_manifest.json", str(version.get("linked_backtest_evidence_package_id") or ""), "backtest evidence"),
        _artifact_ref(_report_path(store, "sleeve_stability", stability_report_id), stability_report_id, "SleeveStabilityReport"),
        _artifact_ref(_report_path(store, "expectancy_drift", drift_report_id), drift_report_id, "ExpectancyDriftReport"),
        _artifact_ref(_report_path(store, "regime_fragility", fragility_report_id), fragility_report_id, "RegimeFragilityReport"),
    ]
    for batch_id in version.get("linked_candidate_batch_ids", []) or []:
        refs.append(_artifact_ref(store / "candidate_batches" / str(batch_id) / "candidate_batch.json", str(batch_id), "candidate batch"))
    return refs


def _missing_required_sources(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [ref for ref in refs if ref["status"] != "available"]


def _load_trigger_reports(store: Path, sleeve_stability_report_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    stability_path = _report_path(store, "sleeve_stability", sleeve_stability_report_id)
    trigger_refs = [_artifact_ref(stability_path, sleeve_stability_report_id, "SleeveStabilityReport")]
    if not stability_path.exists():
        return {}, {}, {}, trigger_refs
    stability = read_json(stability_path)
    drift_id = str(stability.get("expectancy_drift_report_id") or "")
    fragility_id = str(stability.get("regime_fragility_report_id") or "")
    drift_ref = _artifact_ref(_report_path(store, "expectancy_drift", drift_id), drift_id, "ExpectancyDriftReport")
    fragility_ref = _artifact_ref(_report_path(store, "regime_fragility", fragility_id), fragility_id, "RegimeFragilityReport")
    trigger_refs.extend([drift_ref, fragility_ref])
    drift = read_json(Path(drift_ref["path"])) if drift_ref["status"] == "available" else {}
    fragility = read_json(Path(fragility_ref["path"])) if fragility_ref["status"] == "available" else {}
    return stability, drift, fragility, trigger_refs


def _trigger_reason(stability: dict[str, Any]) -> str:
    status = str(stability.get("overall_stability_status") or "unknown")
    action = str(stability.get("recommended_action") or "unknown")
    findings = "; ".join(str(item) for item in stability.get("key_findings", [])[:3])
    return f"Packet 15 stability status is {status}; recommended_action={action}; {findings}".strip()


def build_challenger_research_track(
    *,
    sleeve_id: str,
    sleeve_stability_report_id: str,
    store_root: Path | None = None,
    generated_at: str = GENERATED_AT,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    stability, drift, fragility, trigger_refs = _load_trigger_reports(store, sleeve_stability_report_id)
    trigger_missing = _missing_required_sources(trigger_refs)
    sleeve_version_id = str(stability.get("sleeve_version_id") or "")
    version: dict[str, Any] = {}
    source_refs: list[dict[str, Any]] = []
    if stability and stability.get("sleeve_id") != sleeve_id:
        raise RuntimeError("sleeve stability report does not match requested incumbent sleeve")
    if not trigger_missing and sleeve_version_id:
        version = load_sleeve_version(sleeve_id, sleeve_version_id, store_root=store)
        source_refs = _required_source_refs(
            store,
            version,
            sleeve_stability_report_id,
            str(stability.get("expectancy_drift_report_id") or ""),
            str(stability.get("regime_fragility_report_id") or ""),
        )
    missing_sources = _missing_required_sources(source_refs)
    blocked = bool(trigger_missing or missing_sources)
    payload = {
        "challenger_track_id": "",
        "incumbent_sleeve_id": sleeve_id,
        "incumbent_sleeve_version_id": sleeve_version_id,
        "trigger_report_ids": {
            "sleeve_stability_report_id": sleeve_stability_report_id,
            "expectancy_drift_report_id": str(stability.get("expectancy_drift_report_id") or ""),
            "regime_fragility_report_id": str(stability.get("regime_fragility_report_id") or ""),
        },
        "trigger_reason": _trigger_reason(stability) if stability else "Required Packet 15 trigger artifacts are missing.",
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "challenger_hypothesis_set": deterministic_challenger_hypotheses(sleeve_id),
        "comparison_requirements": _comparison_requirements(),
        "evidence_requirements": _evidence_requirements(version, stability, drift, fragility) if version else {"fail_closed_if_missing": True},
        "exclusion_rules": _exclusion_rules(),
        "governance_constraints": _governance_constraints(),
        "status": "blocked" if blocked else "evidence_pending",
        "recommended_next_action": "investigate_blocker" if blocked else "generate_challenger_evidence",
        "audit_refs": {
            "trigger_artifact_refs": trigger_refs,
            "required_source_refs": source_refs,
            "missing_required_sources": trigger_missing + missing_sources,
            "append_only_registry": "challenger_research_tracks.jsonl",
            "audit_action": "challenger_research_track_created",
        },
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"challenger_track_id", "content_hash", "generated_at"}, sort_lists=True)
    payload["challenger_track_id"] = f"chtrk_{sleeve_id}_{short_hash(seed, 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"generated_at"}, sort_lists=True)
    validate_contract("challenger_research_track", payload)
    return payload


def challenger_research_track_markdown(track: dict[str, Any]) -> str:
    lines = ["# Challenger Research Track", ""]
    lines.append("RESEARCH_ONLY. No broker execution. No live trading. No autonomous trading. No sleeve mutation.")
    lines.append("")
    lines.append(f"- challenger_track_id: {track['challenger_track_id']}")
    lines.append(f"- incumbent_sleeve_id: {track['incumbent_sleeve_id']}")
    lines.append(f"- status: {track['status']}")
    lines.append(f"- recommended_next_action: {track['recommended_next_action']}")
    lines.append(f"- hypothesis_count: {len(track['challenger_hypothesis_set'])}")
    return "\n".join(lines) + "\n"


def write_challenger_research_track(
    *,
    sleeve_id: str,
    sleeve_stability_report_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    track = build_challenger_research_track(
        sleeve_id=sleeve_id,
        sleeve_stability_report_id=sleeve_stability_report_id,
        store_root=store,
    )
    json_path = _track_path(store, track["challenger_track_id"])
    md_path = _track_dir(store) / f"{track['challenger_track_id']}.md"
    if json_path.exists() or md_path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable challenger research track: {json_path}")
    write_json(json_path, track, overwrite=False)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(challenger_research_track_markdown(track), encoding="utf-8")
    row = {
        "challenger_track_id": track["challenger_track_id"],
        "incumbent_sleeve_id": track["incumbent_sleeve_id"],
        "incumbent_sleeve_version_id": track["incumbent_sleeve_version_id"],
        "sleeve_stability_report_id": sleeve_stability_report_id,
        "status": track["status"],
        "hypothesis_count": len(track["challenger_hypothesis_set"]),
        "recommended_next_action": track["recommended_next_action"],
        "research_label": track["research_label"],
        "content_hash": track["content_hash"],
        "generated_at": track["generated_at"],
        "schema_version": track["schema_version"],
    }
    append_jsonl(_registry_path(store), row)
    registry_rows = read_jsonl(_registry_path(store))
    if not registry_rows or registry_rows[-1] != row:
        raise RuntimeError("challenger research track registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="challenger_research_track",
        entity_id=track["challenger_track_id"],
        action="challenger_research_track_created",
        new_state_hash=track["content_hash"],
        reason="Created deterministic research-only challenger track from Packet 15 stability degradation.",
        metadata={
            "registry_row": row,
            "json_path": str(json_path),
            "markdown_path": str(md_path),
            "governance_constraints": track["governance_constraints"],
        },
        store_root=store,
    )
    return {
        "track": track,
        "registry_row": row,
        "audit_event": audit,
        "json_path": str(json_path),
        "markdown_path": str(md_path),
    }


def load_challenger_research_track(challenger_track_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_track_path(store, challenger_track_id))


def list_challenger_research_tracks(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))
