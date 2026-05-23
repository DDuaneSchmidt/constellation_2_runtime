from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from research_lab.storage.hashing import content_hash as research_content_hash


RESEARCH_LABEL = (
    "Research / paper-trial information only. No broker execution. No live trading. "
    "No autonomous trading. No achieved portfolio performance claim. "
    "Backtests/model outputs are hypothetical research evidence."
)
DEFAULT_SLEEVE_ID = "slv_etf_drop_reversion_v1"
VOLATILE_HASH_FIELDS = {
    "content_hash",
    "source_hash",
    "manifest_hash",
    "input_hash",
    "output_hash",
    "previous_state_hash",
    "new_state_hash",
}
FORBIDDEN_CANONICAL_MARKET_DATA_TABLES = {"prices", "ohlcv", "market_bars", "historical_prices"}


@dataclass(frozen=True)
class ReadArtifact:
    path: Path
    payload: dict[str, Any]
    hash_status: str
    expected_hash: str
    actual_hash: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_research_store_root() -> Path:
    return repo_root() / "research_lab" / "research_store"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _content_hash_payload(payload: dict[str, Any], *, exclude: set[str] | None = None) -> dict[str, Any]:
    clone = deepcopy(payload)
    excluded = set(VOLATILE_HASH_FIELDS)
    if exclude:
        excluded.update(exclude)
    for field in excluded:
        clone.pop(field, None)
    return clone


def _hash_payload(payload: dict[str, Any], *, exclude: set[str] | None = None) -> str:
    return hashlib.sha256(_canonical_json(_content_hash_payload(payload, exclude=exclude)).encode("utf-8")).hexdigest()


def _short_file_hash(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path, missing: list[dict[str, Any]], errors: list[dict[str, Any]]) -> ReadArtifact | None:
    if not path.exists():
        missing.append({"path": str(path), "reason": "missing_artifact"})
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive; exercised by API partial-view behavior.
        errors.append({"path": str(path), "error": f"json_parse_failed: {exc}"})
        return None
    if not isinstance(payload, dict):
        errors.append({"path": str(path), "error": "json_root_not_object"})
        return None
    expected = str(payload.get("content_hash") or payload.get("manifest_hash") or "")
    actual = ""
    status = "not_reported"
    if expected:
        family_candidates: set[str] = set()
        if "dataset_snapshot_id" in payload:
            family_candidates.add(
                research_content_hash(
                    payload,
                    exclude={"dataset_snapshot_id", "created_at", "content_hash", "source_hash", "storage_uri", "quality_report_uri"},
                    sort_lists=False,
                )
            )
        if "regime_snapshot_id" in payload:
            family_candidates.add(
                research_content_hash(
                    payload,
                    exclude={"created_at", "content_hash", "regime_snapshot_id", "storage_uri"},
                    sort_lists=False,
                )
            )
        if "cost_model_snapshot_id" in payload:
            family_candidates.add(research_content_hash(payload, exclude={"created_at", "content_hash"}, sort_lists=True))
        if "evidence_package_id" in payload and "manifest_hash" in payload:
            family_candidates.add(
                research_content_hash(
                    payload,
                    exclude={"created_at", "manifest_hash", "evidence_package_id", "artifact_uris", "summary_uri"},
                    sort_lists=True,
                )
            )
        candidates = {
            _hash_payload(payload),
            _hash_payload(payload, exclude={"created_at", "started_at", "ended_at", "reviewed_at", "review_date", "as_of_date"}),
            research_content_hash(payload, sort_lists=True),
            research_content_hash(payload, exclude={"created_at"}, sort_lists=True),
            research_content_hash(payload, exclude={"created_at", "started_at", "ended_at", "reviewed_at", "review_date", "as_of_date"}, sort_lists=True),
            _short_file_hash(path),
        } | family_candidates
        status = "valid" if expected in candidates else "mismatch"
        actual = sorted(candidates)[0]
    return ReadArtifact(path=path, payload=payload, hash_status=status, expected_hash=expected, actual_hash=actual)


def _latest_json_file(directory: Path, pattern: str = "*.json") -> Path | None:
    if not directory.exists():
        return None
    paths = [path for path in directory.glob(pattern) if path.is_file()]
    if not paths:
        return None
    return sorted(paths, key=lambda path: (path.stat().st_mtime, path.name))[-1]


def _artifact_ref(
    *,
    artifact_id: str,
    artifact_type: str,
    storage_uri: str,
    artifact: ReadArtifact | None,
    status: str = "available",
    summary: str = "",
) -> dict[str, Any]:
    payload = artifact.payload if artifact else {}
    return {
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "storage_uri": storage_uri,
        "content_hash": payload.get("content_hash") or payload.get("manifest_hash") or payload.get("summary_hash") or "",
        "created_at": payload.get("created_at") or payload.get("created_at_utc") or payload.get("generated_at_utc") or "",
        "status": status,
        "summary": summary,
        "source_path": str(artifact.path) if artifact else "",
        "hash_status": artifact.hash_status if artifact else "missing",
    }


def _safe_get(payload: dict[str, Any] | None, key: str, default: Any = None) -> Any:
    return payload.get(key, default) if isinstance(payload, dict) else default


def _event_study_summary(manifest: dict[str, Any], summary: dict[str, Any]) -> str:
    event_count = summary.get("event_count", manifest.get("event_count", "n/a"))
    quality = summary.get("evidence_quality", manifest.get("evidence_quality", "not reported"))
    return f"{event_count} events; evidence quality {quality}."


def _backtest_summary(summary: dict[str, Any]) -> str:
    post_cost = summary.get("post_cost") if isinstance(summary.get("post_cost"), dict) else {}
    total_return = summary.get("post_cost_total_return", post_cost.get("total_return", "not reported"))
    excess = summary.get("excess_return_vs_benchmark", "not reported")
    return f"Post-cost total return {total_return}; excess vs benchmark {excess}."


def _candidate_summary(summary: dict[str, Any]) -> str:
    return f"{summary.get('candidate_count', 0)} candidates generated as of {summary.get('as_of_date', 'unknown')}."


def _longitudinal_summary(summary: dict[str, Any]) -> str:
    return (
        f"{summary.get('total_candidates', 0)} candidates across "
        f"{summary.get('total_candidate_batches', 0)} batches; ranking status "
        f"{summary.get('ranking_quality_status', 'not reported')}."
    )


def _paper_trial_summary(summary: dict[str, Any]) -> str:
    return (
        f"Paper trial {summary.get('status', 'unknown')}; "
        f"{summary.get('observation_count', 0)} observations; "
        f"{summary.get('candidate_count_total', 0)} candidates."
    )


def _sleeve_dir(store_root: Path, sleeve_id: str) -> Path:
    return store_root / "sleeves" / sleeve_id


def list_sleeves(store_root: Path | None = None) -> dict[str, Any]:
    root = (store_root or default_research_store_root()).resolve()
    missing: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not root.exists():
        return {"ok": False, "read_only": True, "research_store_available": False, "sleeves": [], "missing_artifacts": [{"path": str(root), "reason": "research_store_missing"}], "errors": []}
    sleeves = []
    for path in sorted((root / "sleeves").glob("*/sleeve_definition.json")):
        artifact = _read_json(path, missing, errors)
        if artifact:
            sleeves.append(_artifact_ref(
                artifact_id=str(artifact.payload.get("sleeve_id") or path.parent.name),
                artifact_type="sleeve",
                storage_uri=f"research://sleeves/{path.parent.name}",
                artifact=artifact,
                summary=str(artifact.payload.get("name") or artifact.payload.get("description") or ""),
            ) | {"name": artifact.payload.get("name"), "hypothesis_id": artifact.payload.get("hypothesis_id")})
    return {"ok": True, "read_only": True, "research_store_available": True, "sleeves": sleeves, "missing_artifacts": missing, "errors": errors, "research_label": RESEARCH_LABEL}


def build_evidence_chain_view(sleeve_id: str = DEFAULT_SLEEVE_ID, store_root: Path | None = None) -> dict[str, Any]:
    root = (store_root or default_research_store_root()).resolve()
    missing: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not root.exists():
        return {
            "ok": False,
            "read_only": True,
            "sleeve_id": sleeve_id,
            "status": "missing_research_store",
            "missing_artifacts": [{"path": str(root), "reason": "research_store_missing"}],
            "errors": [],
            "artifact_refs": [],
            "research_label": RESEARCH_LABEL,
        }

    sdir = _sleeve_dir(root, sleeve_id)
    sleeve = _read_json(sdir / "sleeve_definition.json", missing, errors)
    version_path = _latest_json_file(sdir / "versions")
    version = _read_json(version_path, missing, errors) if version_path else None
    if version_path is None:
        missing.append({"path": str(sdir / "versions"), "reason": "sleeve_version_missing"})
    health_path = _latest_json_file(sdir / "health")
    challenge_path = _latest_json_file(sdir / "challenges")
    review_path = _latest_json_file(sdir / "reviews")
    health = _read_json(health_path, missing, errors) if health_path else None
    challenge = _read_json(challenge_path, missing, errors) if challenge_path else None
    review = _read_json(review_path, missing, errors) if review_path else None

    version_payload = version.payload if version else {}
    event_study_id = str(version_payload.get("linked_event_study_evidence_package_id") or "")
    backtest_id = str(version_payload.get("linked_backtest_evidence_package_id") or "")
    candidate_ids = [str(row) for row in version_payload.get("linked_candidate_batch_ids") or [] if row]
    dataset_id = str(version_payload.get("dataset_snapshot_id") or "")
    regime_id = str(version_payload.get("regime_snapshot_id") or "")
    cost_model_id = str(version_payload.get("cost_model_snapshot_id") or "")

    dataset = _read_json(root / "datasets" / dataset_id / "dataset_snapshot.json", missing, errors) if dataset_id else None
    regime = _read_json(root / "regimes" / regime_id / "regime_snapshot.json", missing, errors) if regime_id else None
    cost_model = _read_json(root / "cost_models" / cost_model_id / "cost_model_snapshot.json", missing, errors) if cost_model_id else None
    event_manifest = _read_json(root / "evidence_packages" / event_study_id / "evidence_manifest.json", missing, errors) if event_study_id else None
    event_summary = _read_json(root / "evidence_packages" / event_study_id / "summary.json", missing, errors) if event_study_id else None
    backtest_manifest = _read_json(root / "evidence_packages" / backtest_id / "evidence_manifest.json", missing, errors) if backtest_id else None
    backtest_summary = _read_json(root / "evidence_packages" / backtest_id / "performance_summary.json", missing, errors) if backtest_id else None

    candidate_refs = []
    for candidate_id in candidate_ids:
        candidate_summary = _read_json(root / "candidate_batches" / candidate_id / "generation_summary.json", missing, errors)
        candidate_refs.append(_artifact_ref(
            artifact_id=candidate_id,
            artifact_type="candidate_batch",
            storage_uri=f"research://candidate_batches/{candidate_id}",
            artifact=candidate_summary,
            summary=_candidate_summary(candidate_summary.payload if candidate_summary else {}),
            status="available" if candidate_summary else "missing",
        ))

    longitudinal_ids = []
    longitudinal_dir = root / "longitudinal_runs"
    if longitudinal_dir.exists():
        for run_path in sorted(longitudinal_dir.glob("*/longitudinal_run.json")):
            run_artifact = _read_json(run_path, missing, errors)
            if run_artifact and run_artifact.payload.get("sleeve_id") == sleeve_id:
                longitudinal_ids.append(str(run_artifact.payload.get("longitudinal_run_id") or run_path.parent.name))

    longitudinal_refs = []
    for run_id in longitudinal_ids:
        learning = _read_json(root / "longitudinal_runs" / run_id / "sleeve_learning_report.json", missing, errors)
        longitudinal_refs.append(_artifact_ref(
            artifact_id=run_id,
            artifact_type="longitudinal_run",
            storage_uri=f"research://longitudinal_runs/{run_id}",
            artifact=learning,
            summary=_longitudinal_summary(learning.payload if learning else {}),
            status="available" if learning else "missing",
        ))

    paper_trial_ids = []
    paper_trial_dir = root / "paper_trials"
    if paper_trial_dir.exists():
        for trial_path in sorted(paper_trial_dir.glob("*/paper_trial_summary.json")):
            trial_artifact = _read_json(trial_path, missing, errors)
            if trial_artifact and trial_artifact.payload.get("sleeve_id") == sleeve_id:
                paper_trial_ids.append(str(trial_artifact.payload.get("paper_trial_id") or trial_path.parent.name))

    paper_trial_refs = []
    paper_trial_summaries = []
    for paper_trial_id in paper_trial_ids:
        summary = _read_json(root / "paper_trials" / paper_trial_id / "paper_trial_summary.json", missing, errors)
        if summary:
            paper_trial_summaries.append(summary.payload)
        paper_trial_refs.append(_artifact_ref(
            artifact_id=paper_trial_id,
            artifact_type="paper_trial",
            storage_uri=f"research://paper_trials/{paper_trial_id}",
            artifact=summary,
            summary=_paper_trial_summary(summary.payload if summary else {}),
            status="available" if summary else "missing",
        ))

    artifact_refs = [
        _artifact_ref(artifact_id=dataset_id, artifact_type="dataset_snapshot", storage_uri=f"research://datasets/{dataset_id}", artifact=dataset, summary=f"{_safe_get(dataset.payload if dataset else None, 'symbol_count', 'n/a')} symbols; quality {_safe_get(dataset.payload if dataset else None, 'quality_status', 'not reported')}.", status="available" if dataset else "missing"),
        _artifact_ref(artifact_id=regime_id, artifact_type="regime_snapshot", storage_uri=f"research://regimes/{regime_id}", artifact=regime, summary=f"Benchmark {_safe_get(regime.payload if regime else None, 'benchmark_symbol', 'not reported')}.", status="available" if regime else "missing"),
        _artifact_ref(artifact_id=cost_model_id, artifact_type="cost_model_snapshot", storage_uri=f"research://cost_models/{cost_model_id}", artifact=cost_model, summary=str(_safe_get(cost_model.payload if cost_model else None, 'name', 'Cost model not reported.')), status="available" if cost_model else "missing"),
        _artifact_ref(artifact_id=event_study_id, artifact_type="event_study_evidence", storage_uri=f"research://evidence/{event_study_id}", artifact=event_manifest, summary=_event_study_summary(event_manifest.payload if event_manifest else {}, event_summary.payload if event_summary else {}), status="available" if event_manifest else "missing"),
        _artifact_ref(artifact_id=backtest_id, artifact_type="backtest_evidence", storage_uri=f"research://evidence/{backtest_id}", artifact=backtest_manifest, summary=_backtest_summary(backtest_summary.payload if backtest_summary else {}), status="available" if backtest_manifest else "missing"),
        *candidate_refs,
        *longitudinal_refs,
        _artifact_ref(artifact_id=sleeve_id, artifact_type="sleeve", storage_uri=f"research://sleeves/{sleeve_id}", artifact=sleeve, summary=str(_safe_get(sleeve.payload if sleeve else None, 'name', 'Sleeve missing.')), status="available" if sleeve else "missing"),
        _artifact_ref(artifact_id=str(version_payload.get("sleeve_version_id") or ""), artifact_type="sleeve_version", storage_uri=f"research://sleeves/{sleeve_id}/versions/{version_payload.get('sleeve_version_id') or ''}", artifact=version, summary=f"Version {version_payload.get('version', 'unknown')}.", status="available" if version else "missing"),
        _artifact_ref(artifact_id=str(_safe_get(health.payload if health else None, 'sleeve_health_snapshot_id', '')), artifact_type="sleeve_health", storage_uri=f"research://sleeves/{sleeve_id}/health", artifact=health, summary=f"Overall health {_safe_get(health.payload if health else None, 'overall_health', 'not reported')}.", status="available" if health else "missing"),
        _artifact_ref(artifact_id=str(_safe_get(challenge.payload if challenge else None, 'sleeve_challenge_id', '')), artifact_type="sleeve_challenge", storage_uri=f"research://sleeves/{sleeve_id}/challenges", artifact=challenge, summary=f"{_safe_get(challenge.payload if challenge else None, 'challenge_type', 'not reported')}: {_safe_get(challenge.payload if challenge else None, 'recommended_action', 'not reported')}.", status="available" if challenge else "missing"),
        _artifact_ref(artifact_id=str(_safe_get(review.payload if review else None, 'sleeve_review_id', '')), artifact_type="sleeve_review", storage_uri=f"research://sleeves/{sleeve_id}/reviews", artifact=review, summary=f"Review decision {_safe_get(review.payload if review else None, 'review_decision', 'not reported')}.", status="available" if review else "missing"),
        *paper_trial_refs,
    ]
    hash_mismatches = [ref for ref in artifact_refs if ref.get("hash_status") == "mismatch"]
    if hash_mismatches:
        errors.extend({"path": ref.get("source_path", ""), "error": "artifact_hash_mismatch", "artifact_id": ref.get("artifact_id")} for ref in hash_mismatches)

    active_paper_trial = next((row for row in paper_trial_summaries if row.get("status") == "active"), {})
    latest_paper_trial = paper_trial_summaries[-1] if paper_trial_summaries else {}
    latest_summary = {
        "sleeve_name": _safe_get(sleeve.payload if sleeve else None, "name", sleeve_id),
        "event_study": _event_study_summary(event_manifest.payload if event_manifest else {}, event_summary.payload if event_summary else {}),
        "backtest": _backtest_summary(backtest_summary.payload if backtest_summary else {}),
        "candidate_batches": len(candidate_ids),
        "longitudinal_runs": len(longitudinal_ids),
        "paper_trials": len(paper_trial_ids),
    }
    return {
        "ok": not bool(hash_mismatches),
        "read_only": True,
        "sleeve_id": sleeve_id,
        "sleeve_version_id": str(version_payload.get("sleeve_version_id") or ""),
        "hypothesis_id": str(_safe_get(sleeve.payload if sleeve else None, "hypothesis_id", version_payload.get("hypothesis_id", ""))),
        "status": str(_safe_get(sleeve.payload if sleeve else None, "status", "missing")),
        "overall_health": str(_safe_get(health.payload if health else None, "overall_health", "missing")),
        "backtest_health": str(_safe_get(health.payload if health else None, "backtest_health", "missing")),
        "candidate_health": str(_safe_get(health.payload if health else None, "candidate_health", "missing")),
        "attribution_health": str(_safe_get(health.payload if health else None, "attribution_health", "missing")),
        "challenge_status": str(_safe_get(challenge.payload if challenge else None, "challenge_type", "missing")),
        "challenge_type": str(_safe_get(challenge.payload if challenge else None, "challenge_type", "")),
        "recommended_action": str(_safe_get(challenge.payload if challenge else None, "recommended_action", "")),
        "review_status": str(_safe_get(review.payload if review else None, "review_decision", "missing")),
        "latest_review_decision": str(_safe_get(review.payload if review else None, "review_decision", "")),
        "paper_trial_status": str(active_paper_trial.get("status") or latest_paper_trial.get("status") or "missing"),
        "dataset_snapshot_id": dataset_id,
        "regime_snapshot_id": regime_id,
        "cost_model_snapshot_id": cost_model_id,
        "event_study_evidence_id": event_study_id,
        "backtest_evidence_id": backtest_id,
        "candidate_batch_ids": candidate_ids,
        "longitudinal_run_ids": longitudinal_ids,
        "paper_trial_ids": paper_trial_ids,
        "latest_summary": latest_summary,
        "next_allowed_actions": active_paper_trial.get("next_allowed_actions") or latest_paper_trial.get("next_allowed_actions") or ["continue_research"],
        "research_label": RESEARCH_LABEL,
        "artifact_refs": [ref for ref in artifact_refs if ref.get("artifact_id")],
        "missing_artifacts": missing,
        "errors": errors,
        "hash_mismatches": hash_mismatches,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "automatic_promotion_allowed": False,
    }


def build_edge_lab_projection(sleeve_id: str = DEFAULT_SLEEVE_ID, store_root: Path | None = None) -> dict[str, Any]:
    chain = build_evidence_chain_view(sleeve_id=sleeve_id, store_root=store_root)
    paper_trial_status = str(chain.get("paper_trial_status") or "")
    challenge_type = str(chain.get("challenge_type") or "")
    overall_health = str(chain.get("overall_health") or "")
    if paper_trial_status == "active":
        lane = "Paper Trial Active"
    elif challenge_type and challenge_type != "missing":
        lane = "Challenged"
    elif overall_health in {"watch", "challenged"}:
        lane = "Watch"
    elif overall_health == "healthy":
        lane = "Evidence Produced"
    else:
        lane = "Research Only"
    return {
        "ok": chain.get("ok", False),
        "read_only": True,
        "card_id": f"edge_lab_projection:{sleeve_id}",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": chain.get("sleeve_version_id", ""),
        "title": chain.get("latest_summary", {}).get("sleeve_name") or sleeve_id,
        "lane": lane,
        "health": chain.get("overall_health", "missing"),
        "evidence_quality": "research_simulation",
        "candidate_status": chain.get("candidate_health", "missing"),
        "paper_trial_status": chain.get("paper_trial_status", "missing"),
        "latest_review_decision": chain.get("latest_review_decision", ""),
        "challenge_type": chain.get("challenge_type", ""),
        "recommended_action": chain.get("recommended_action", ""),
        "next_allowed_actions": chain.get("next_allowed_actions", []),
        "artifact_refs": chain.get("artifact_refs", []),
        "evidence_chain": chain,
        "updated_at": max([str(ref.get("created_at") or "") for ref in chain.get("artifact_refs", [])] or [""]),
        "research_label": RESEARCH_LABEL,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "automatic_promotion_allowed": False,
    }


def evidence_summary(evidence_package_id: str, store_root: Path | None = None) -> dict[str, Any]:
    root = (store_root or default_research_store_root()).resolve()
    missing: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    manifest = _read_json(root / "evidence_packages" / evidence_package_id / "evidence_manifest.json", missing, errors)
    summary_path = root / "evidence_packages" / evidence_package_id / "summary.json"
    if not summary_path.exists():
        summary_path = root / "evidence_packages" / evidence_package_id / "performance_summary.json"
    summary = _read_json(summary_path, missing, errors) if summary_path.exists() else None
    return {
        "ok": bool(manifest),
        "read_only": True,
        "evidence_package_id": evidence_package_id,
        "manifest": manifest.payload if manifest else {},
        "summary": summary.payload if summary else {},
        "artifact_ref": _artifact_ref(artifact_id=evidence_package_id, artifact_type="backtest_evidence" if _safe_get(manifest.payload if manifest else None, "evidence_type") == "backtest" else "event_study_evidence", storage_uri=f"research://evidence/{evidence_package_id}", artifact=manifest, summary="Evidence package summary.", status="available" if manifest else "missing"),
        "missing_artifacts": missing,
        "errors": errors,
        "research_label": RESEARCH_LABEL,
    }


def paper_trial_summary(paper_trial_id: str, store_root: Path | None = None) -> dict[str, Any]:
    root = (store_root or default_research_store_root()).resolve()
    missing: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    summary = _read_json(root / "paper_trials" / paper_trial_id / "paper_trial_summary.json", missing, errors)
    return {
        "ok": bool(summary),
        "read_only": True,
        "paper_trial_id": paper_trial_id,
        "summary": summary.payload if summary else {},
        "missing_artifacts": missing,
        "errors": errors,
        "research_label": RESEARCH_LABEL,
    }


def health_payload(store_root: Path | None = None) -> dict[str, Any]:
    root = (store_root or default_research_store_root()).resolve()
    registries = root / "registries"
    missing = []
    errors = []
    if not root.exists():
        missing.append({"path": str(root), "reason": "research_store_missing"})
    if not registries.exists():
        missing.append({"path": str(registries), "reason": "registries_missing"})
    latest_created = ""
    for json_path in root.glob("**/*.json") if root.exists() else []:
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        created = str(payload.get("created_at") or payload.get("created_at_utc") or "")
        if created > latest_created:
            latest_created = created
    return {
        "ok": root.exists() and registries.exists(),
        "read_only": True,
        "research_store_available": root.exists(),
        "registries_available": registries.exists(),
        "sleeves_count": len(list((root / "sleeves").glob("*/sleeve_definition.json"))) if root.exists() else 0,
        "evidence_packages_count": len(list((root / "evidence_packages").glob("*/evidence_manifest.json"))) if root.exists() else 0,
        "paper_trials_count": len(list((root / "paper_trials").glob("*/paper_trial_summary.json"))) if root.exists() else 0,
        "latest_artifact_created_at": latest_created,
        "missing_artifacts": missing,
        "errors": errors,
        "research_label": RESEARCH_LABEL,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }
