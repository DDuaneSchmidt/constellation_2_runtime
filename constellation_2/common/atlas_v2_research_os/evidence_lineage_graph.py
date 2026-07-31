from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from datetime import date
from pathlib import Path
from typing import Any

REPORT_DIR_NAME = "evidence_lineage_graph"

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "read_only": True,
    "candidate_generation_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
    "ranking_changes_authorized": False,
    "trade_recommendation_authorized": False,
    "trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
}

CONFIRMING_CLASSIFICATIONS = {
    "BACKTEST_SUPPORTED",
    "READY_FOR_PAPER_FORWARD_OBSERVATION",
    "REPLAY_POSITIVE",
    "SUPPORTED",
    "STRONGLY_SUPPORTED",
}

FAILING_CLASSIFICATIONS = {
    "BACKTEST_WEAK",
    "REJECT_FOR_NOW",
    "TOO_FRAGILE",
    "TOO_PROXY_DEPENDENT",
    "NEEDS_DATA_IMPROVEMENT",
    "FALSIFIED",
    "WEAKENED",
    "REPLAY_NEGATIVE",
}


def build_evidence_lineage_graph(root: str | Path = Path("reports/atlas_v2_research_os"), *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    artifacts = _load_artifacts(root_path)
    observations, claim_seed_artifacts = _observation_claim_seeds(artifacts)
    generated_claims = [artifact for artifact in artifacts.values() if artifact.get("artifact_type") == "GeneratedResearchClaim"]
    claims = {artifact["artifact_id"]: _claim_node(artifact) for artifact in [*claim_seed_artifacts, *generated_claims]}
    hypotheses = {
        artifact["artifact_id"]: _hypothesis_node(artifact)
        for artifact in artifacts.values()
        if artifact.get("artifact_type") == "ResearchHypothesis"
    }

    candidates = _load_candidate_rows(root_path)
    for candidate in candidates.values():
        for observation_id in candidate.get("source_observation_ids", []):
            observations.setdefault(
                observation_id,
                {
                    "id": observation_id,
                    "node_type": "Observation",
                    "mechanism": candidate.get("mechanism", ""),
                    "regime": candidate.get("regime", ""),
                    "source_claim_seed_ids": [],
                },
            )
    families = _load_family_rows(root_path)
    validation_rows = _load_validation_rows(root_path)
    _add_family_validation_nodes(families, validation_rows)

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, str]] = []
    adjacency: dict[str, set[str]] = defaultdict(set)
    reverse_adjacency: dict[str, set[str]] = defaultdict(set)

    for node in observations.values():
        nodes[node["id"]] = node
    for node in claims.values():
        nodes[node["id"]] = node
    for node in hypotheses.values():
        nodes[node["id"]] = node
    for node in candidates.values():
        nodes[node["id"]] = node
    for node in families.values():
        nodes[node["id"]] = node
    for node in validation_rows.values():
        nodes[node["id"]] = node

    def add_edge(source: str, target: str, relation: str) -> None:
        if source not in nodes or target not in nodes or target in adjacency[source]:
            return
        adjacency[source].add(target)
        reverse_adjacency[target].add(source)
        edges.append({"source": source, "target": target, "relation": relation})

    for claim in claims.values():
        for observation_id in claim.get("source_observation_ids", []):
            add_edge(observation_id, claim["id"], "observation_to_claim")
        for source_artifact_id in claim.get("source_artifact_ids", []):
            add_edge(source_artifact_id, claim["id"], "source_artifact_to_claim")

    for hypothesis in hypotheses.values():
        for source_id in hypothesis.get("source_claim_ids", []):
            add_edge(source_id, hypothesis["id"], "claim_to_hypothesis")
        if not hypothesis.get("source_claim_ids"):
            for claim in _matching_claims(hypothesis, claims):
                add_edge(claim["id"], hypothesis["id"], "claim_to_hypothesis_inferred")

    for candidate in candidates.values():
        for observation_id in candidate.get("source_observation_ids", []):
            add_edge(observation_id, candidate["id"], "observation_to_candidate")
        for hypothesis_id in candidate.get("source_hypothesis_ids", []):
            add_edge(hypothesis_id, candidate["id"], "hypothesis_to_candidate")
        if not candidate.get("source_hypothesis_ids"):
            for hypothesis in _matching_hypotheses(candidate, hypotheses):
                add_edge(hypothesis["id"], candidate["id"], "hypothesis_to_candidate_inferred")
        for backtest_id in candidate.get("backtest_ids", []):
            validation_node = validation_rows.get(backtest_id)
            if validation_node:
                add_edge(candidate["id"], validation_node["id"], "candidate_to_backtest")

    for family in families.values():
        for candidate_id in family.get("candidate_ids", []):
            candidate = candidates.get(candidate_id, {})
            backtest_ids = candidate.get("backtest_ids", [])
            if backtest_ids:
                for backtest_id in backtest_ids:
                    add_edge(backtest_id, family["id"], "backtest_to_family")
            else:
                add_edge(candidate_id, family["id"], "candidate_to_family")
        for validation_id in family.get("validation_ids", []):
            if validation_id in nodes:
                add_edge(family["id"], validation_id, "family_to_validation")

    for validation in validation_rows.values():
        for candidate_id in validation.get("candidate_ids", []):
            add_edge(candidate_id, validation["id"], "candidate_to_validation")

    observation_productivity = _observation_productivity(observations, nodes, adjacency)
    claim_productivity = _claim_productivity(claims, nodes, adjacency)
    family_summary = _family_summary(families, nodes, reverse_adjacency, adjacency)
    rankings = _rankings(observation_productivity, claim_productivity, family_summary)

    report = {
        "schema_id": "atlas_v2_research_os_evidence_lineage_graph_v1",
        "schema_version": "v1",
        "report_type": "EVIDENCE_LINEAGE_GRAPH",
        "created_at": created_at or f"{date.today().isoformat()}T00:00:00Z",
        "root": root_path.as_posix(),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "confidence_impact": "NONE",
        "summary": {
            "observations_analyzed": len(observations),
            "claims_analyzed": len(claims),
            "hypotheses_analyzed": len(hypotheses),
            "candidates_analyzed": len(candidates),
            "families_analyzed": len(families),
            "validations_analyzed": len(validation_rows),
            "edge_count": len(edges),
            "node_count": len(nodes),
            "no_promotion_authority_emitted": True,
        },
        "nodes": dict(sorted(nodes.items())),
        "edges": sorted(edges, key=lambda row: (row["source"], row["target"], row["relation"])),
        "observations": observation_productivity,
        "claims": claim_productivity,
        "families": family_summary,
        "rankings": rankings,
        "recommended_research_focus": _recommended_focus(rankings),
        "data_limitations": [
            "Lineage is read from existing Atlas Research OS artifacts and reports only.",
            "Where direct identifiers are absent, joins are deterministic mechanism/regime inferences and are marked by inferred edge relations.",
            "This report does not alter Atlas outputs, generate candidates, promote candidates, or change ranking policy.",
        ],
    }
    _validate_graph(report)
    return report


def write_evidence_lineage_graph(root: str | Path = Path("reports/atlas_v2_research_os"), *, created_at: str | None = None) -> dict[str, Path]:
    root_path = Path(root)
    report = build_evidence_lineage_graph(root_path, created_at=created_at)
    out_dir = root_path / REPORT_DIR_NAME
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "evidence_lineage_graph.json"
    summary_path = out_dir / "latest_summary.md"
    claim_csv = out_dir / "claim_productivity.csv"
    observation_csv = out_dir / "observation_productivity.csv"
    family_csv = out_dir / "family_lineage_summary.csv"

    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary_markdown(report), encoding="utf-8")
    _write_csv(claim_csv, report["claims"], ["claim_id", "hypothesis_count", "candidate_count", "confirmation_count", "failure_count", "lineage_depth", "confirmation_conversion_rate", "failure_conversion_rate", "evidence_density"])
    _write_csv(observation_csv, report["observations"], ["observation_id", "downstream_claims", "downstream_hypotheses", "downstream_candidates", "downstream_confirmations", "lineage_depth", "confirmation_conversion_rate", "failure_conversion_rate", "evidence_density"])
    _write_csv(family_csv, report["families"], ["family_id", "upstream_observations", "upstream_claims", "downstream_confirmations", "downstream_failures", "lineage_depth", "confirmation_conversion_rate", "failure_conversion_rate", "evidence_density"])
    return {
        "json": json_path,
        "summary": summary_path,
        "claim_productivity": claim_csv,
        "observation_productivity": observation_csv,
        "family_lineage_summary": family_csv,
    }


def _load_artifacts(root: Path) -> dict[str, dict[str, Any]]:
    index_path = root / "artifact_index.json"
    if not index_path.exists():
        return {}
    index = json.loads(index_path.read_text(encoding="utf-8"))
    artifacts = {}
    for row in index.get("artifacts", []):
        artifact_id = row.get("artifact_id")
        path = root / "artifacts" / f"{artifact_id}.json"
        if artifact_id and path.exists():
            artifact = json.loads(path.read_text(encoding="utf-8"))
            artifacts[artifact_id] = artifact
    return artifacts


def _observation_claim_seeds(artifacts: dict[str, dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    observations: dict[str, dict[str, Any]] = {}
    claim_seeds = []
    for artifact in artifacts.values():
        metadata = artifact.get("metadata", {})
        source_observation_ids = _string_list(metadata.get("source_observation_ids"))
        seed = metadata.get("observation_claim_seed") or {}
        if not source_observation_ids:
            source_observation_ids = _string_list(seed.get("source_observation_ids"))
        if not source_observation_ids:
            continue
        claim_seeds.append(artifact)
        for observation_id in source_observation_ids:
            observations.setdefault(
                observation_id,
                {
                    "id": observation_id,
                    "node_type": "Observation",
                    "mechanism": metadata.get("mechanism_family") or seed.get("mechanism") or "",
                    "regime": metadata.get("regime_context") or seed.get("regime") or "",
                    "source_claim_seed_ids": [],
                },
            )
            observations[observation_id]["source_claim_seed_ids"].append(artifact["artifact_id"])
    return observations, claim_seeds


def _claim_node(artifact: dict[str, Any]) -> dict[str, Any]:
    metadata = artifact.get("metadata", {})
    payload = metadata.get("atlas_component_payload") or {}
    seed = metadata.get("observation_claim_seed") or {}
    return {
        "id": artifact["artifact_id"],
        "node_type": "Claim",
        "claim_id": artifact["artifact_id"],
        "source_artifact_ids": _string_list(artifact.get("source_artifact_ids")),
        "source_observation_ids": _string_list(metadata.get("source_observation_ids")) or _string_list(seed.get("source_observation_ids")),
        "mechanism": metadata.get("mechanism_family") or payload.get("mechanism_family") or seed.get("mechanism") or "",
        "regime": metadata.get("regime_context") or seed.get("regime") or "",
        "text": metadata.get("claim_text") or payload.get("claim_text") or metadata.get("question") or "",
    }


def _hypothesis_node(artifact: dict[str, Any]) -> dict[str, Any]:
    metadata = artifact.get("metadata", {})
    payload = metadata.get("atlas_component_payload") or {}
    source_claim_ids = _string_list(artifact.get("source_artifact_ids"))
    source_claim_artifact_id = metadata.get("source_claim_artifact_id")
    if source_claim_artifact_id:
        source_claim_ids.append(str(source_claim_artifact_id))
    return {
        "id": artifact["artifact_id"],
        "node_type": "Hypothesis",
        "hypothesis_id": artifact["artifact_id"],
        "source_claim_ids": sorted(set(source_claim_ids)),
        "mechanism": metadata.get("mechanism") or payload.get("mechanism_family") or "",
        "regime": metadata.get("regime") or "",
        "text": metadata.get("hypothesis_text") or payload.get("hypothesis_text") or "",
    }


def _load_candidate_rows(root: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in _json_list(root / "final_candidate_ranking" / "latest.json", "top_20_robust_candidates"):
        _add_candidate(rows, row)
    for row in _json_list(root / "final_candidate_ranking" / "latest.json", "campaign_candidate_preview"):
        _add_candidate(rows, row)
    for row in _json_list(root / "candidate_backtests" / "latest.json", "candidates"):
        _add_candidate(rows, row)
    return rows


def _add_candidate(rows: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
    candidate_id = row.get("candidate_id")
    if not candidate_id:
        return
    existing = rows.setdefault(
        str(candidate_id),
        {
            "id": str(candidate_id),
            "node_type": "Candidate",
            "candidate_id": str(candidate_id),
            "source_observation_ids": [],
            "source_hypothesis_ids": [],
            "backtest_ids": [],
            "mechanism": "",
            "regime": "",
            "classification": "",
            "confirmation": False,
            "failure": False,
        },
    )
    candidate_observations = _string_list(row.get("candidate_source_observation_ids")) or _string_list(row.get("source_observation_ids"))
    existing["source_observation_ids"] = sorted(set(existing["source_observation_ids"]) | set(candidate_observations))
    spec = row.get("backtest_spec") or {}
    source_hypothesis_id = spec.get("source_hypothesis_id") or ((row.get("historical_replay_result") or {}).get("certification") or {}).get("hypothesis_id")
    if source_hypothesis_id:
        existing["source_hypothesis_ids"] = sorted(set(existing["source_hypothesis_ids"]) | {str(source_hypothesis_id)})
    replay_id = ((row.get("historical_replay_result") or {}).get("replay_id")) or ((row.get("historical_replay_result") or {}).get("certification") or {}).get("replay_id")
    if replay_id:
        existing["backtest_ids"] = sorted(set(existing["backtest_ids"]) | {str(replay_id)})
    classification = row.get("classification") or row.get("backtest_classification") or existing.get("classification") or ""
    existing["classification"] = str(classification)
    existing["confirmation"] = existing["confirmation"] or str(classification) in CONFIRMING_CLASSIFICATIONS
    existing["failure"] = existing["failure"] or str(classification) in FAILING_CLASSIFICATIONS
    existing["mechanism"] = row.get("mechanism") or spec.get("mechanism") or existing.get("mechanism") or ""
    existing["regime"] = row.get("regime") or spec.get("primary_regime") or existing.get("regime") or ""


def _load_family_rows(root: Path) -> dict[str, dict[str, Any]]:
    families: dict[str, dict[str, Any]] = {}
    for row in _json_list(root / "candidate_family_discovery" / "latest.json", "families"):
        family_id = row.get("family_id")
        if not family_id:
            continue
        families[str(family_id)] = {
            "id": str(family_id),
            "node_type": "Family",
            "family_id": str(family_id),
            "candidate_ids": _string_list(row.get("candidate_ids")),
            "validation_ids": [],
            "mechanism": row.get("mechanism") or row.get("dominant_mechanism") or "",
            "regime": row.get("regime") or row.get("dominant_regime") or "",
            "classification": row.get("classification") or "",
        }
    return families


def _add_family_validation_nodes(families: dict[str, dict[str, Any]], validations: dict[str, dict[str, Any]]) -> None:
    for family in families.values():
        validation_id = f"validation_{family['family_id']}"
        classification = str(family.get("classification") or "")
        validations[validation_id] = {
            "id": validation_id,
            "node_type": "Validation",
            "validation_id": validation_id,
            "candidate_ids": [],
            "classification": classification,
            "confirmation": classification in {"HIGH_PRIORITY_FAMILY", "PROMISING_FAMILY"},
            "failure": classification in {"WEAK_FAMILY"},
        }
        family["validation_ids"] = sorted(set(family.get("validation_ids", [])) | {validation_id})


def _load_validation_rows(root: Path) -> dict[str, dict[str, Any]]:
    validations: dict[str, dict[str, Any]] = {}
    for row in _json_list(root / "candidate_backtests" / "latest.json", "candidates"):
        replay = row.get("historical_replay_result") or {}
        replay_id = replay.get("replay_id") or (replay.get("certification") or {}).get("replay_id")
        if replay_id:
            status = (replay.get("certification") or {}).get("status") or row.get("classification") or ""
            validations[str(replay_id)] = {
                "id": str(replay_id),
                "node_type": "Backtest",
                "validation_id": str(replay_id),
                "candidate_ids": [str(row.get("candidate_id"))] if row.get("candidate_id") else [],
                "classification": status,
                "confirmation": status in CONFIRMING_CLASSIFICATIONS or row.get("classification") in CONFIRMING_CLASSIFICATIONS,
                "failure": status in FAILING_CLASSIFICATIONS or row.get("classification") in FAILING_CLASSIFICATIONS,
            }
    validation_report = _load_json(root / "learning_validation" / "latest.json")
    if validation_report:
        validations["learning_validation_latest"] = {
            "id": "learning_validation_latest",
            "node_type": "Validation",
            "validation_id": "learning_validation_latest",
            "candidate_ids": [],
            "classification": validation_report.get("certification", {}).get("result", ""),
            "confirmation": False,
            "failure": False,
        }
    return validations


def _matching_claims(hypothesis: dict[str, Any], claims: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    mechanism = hypothesis.get("mechanism")
    if not mechanism:
        return []
    return [claim for claim in claims.values() if claim.get("mechanism") == mechanism]


def _matching_hypotheses(candidate: dict[str, Any], hypotheses: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    mechanism = candidate.get("mechanism")
    if not mechanism:
        return []
    return [hypothesis for hypothesis in hypotheses.values() if hypothesis.get("mechanism") == mechanism][:20]


def _observation_productivity(observations: dict[str, dict[str, Any]], nodes: dict[str, dict[str, Any]], adjacency: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows = []
    for observation_id in sorted(observations):
        downstream = _reachable(observation_id, adjacency)
        rows.append(
            {
                "observation_id": observation_id,
                "downstream_claims": _count_type(downstream, nodes, "Claim"),
                "downstream_hypotheses": _count_type(downstream, nodes, "Hypothesis"),
                "downstream_candidates": _count_type(downstream, nodes, "Candidate"),
                "downstream_confirmations": _count_confirmations(downstream, nodes),
                "downstream_failures": _count_failures(downstream, nodes),
                **_metrics(observation_id, downstream, nodes, adjacency),
            }
        )
    return rows


def _claim_productivity(claims: dict[str, dict[str, Any]], nodes: dict[str, dict[str, Any]], adjacency: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows = []
    for claim_id in sorted(claims):
        downstream = _reachable(claim_id, adjacency)
        rows.append(
            {
                "claim_id": claim_id,
                "hypothesis_count": _count_type(downstream, nodes, "Hypothesis"),
                "candidate_count": _count_type(downstream, nodes, "Candidate"),
                "confirmation_count": _count_confirmations(downstream, nodes),
                "failure_count": _count_failures(downstream, nodes),
                **_metrics(claim_id, downstream, nodes, adjacency),
            }
        )
    return rows


def _family_summary(families: dict[str, dict[str, Any]], nodes: dict[str, dict[str, Any]], reverse_adjacency: dict[str, set[str]], adjacency: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows = []
    for family_id in sorted(families):
        upstream = _reverse_reachable(family_id, reverse_adjacency)
        downstream = _reachable(family_id, adjacency)
        rows.append(
            {
                "family_id": family_id,
                "upstream_observations": _count_type(upstream, nodes, "Observation"),
                "upstream_claims": _count_type(upstream, nodes, "Claim"),
                "downstream_confirmations": _count_confirmations(downstream | {family_id}, nodes),
                "downstream_failures": _count_failures(downstream | {family_id}, nodes),
                **_metrics(family_id, downstream, nodes, adjacency),
            }
        )
    return rows


def _rankings(observations: list[dict[str, Any]], claims: list[dict[str, Any]], families: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "most_productive_observations": sorted(observations, key=lambda row: (-row["downstream_confirmations"], -row["downstream_candidates"], row["observation_id"]))[:10],
        "most_productive_claims": sorted(claims, key=lambda row: (-row["confirmation_count"], -row["candidate_count"], row["claim_id"]))[:10],
        "most_productive_families": sorted(families, key=lambda row: (-row["downstream_confirmations"], -row["upstream_observations"], row["family_id"]))[:10],
        "dead_end_claims": sorted([row for row in claims if row["candidate_count"] == 0 and row["confirmation_count"] == 0], key=lambda row: (-row["hypothesis_count"], row["claim_id"]))[:10],
        "dead_end_observation_clusters": sorted([row for row in observations if row["downstream_candidates"] == 0 and row["downstream_confirmations"] == 0], key=lambda row: (-row["downstream_claims"], row["observation_id"]))[:10],
    }


def _recommended_focus(rankings: dict[str, list[dict[str, Any]]]) -> list[str]:
    focus = []
    if rankings["most_productive_families"]:
        focus.append(f"Study family lineage patterns led by {rankings['most_productive_families'][0]['family_id']}.")
    if rankings["most_productive_observations"]:
        focus.append(f"Trace repeatable source observations led by {rankings['most_productive_observations'][0]['observation_id']}.")
    if rankings["dead_end_claims"]:
        focus.append(f"Review dead-end claim structure led by {rankings['dead_end_claims'][0]['claim_id']}.")
    if rankings["dead_end_observation_clusters"]:
        focus.append(f"Audit observation clusters with no candidate path led by {rankings['dead_end_observation_clusters'][0]['observation_id']}.")
    return focus


def _metrics(root_id: str, downstream: set[str], nodes: dict[str, dict[str, Any]], adjacency: dict[str, set[str]]) -> dict[str, Any]:
    confirmations = _count_confirmations(downstream, nodes)
    failures = _count_failures(downstream, nodes)
    candidates = _count_type(downstream, nodes, "Candidate")
    terminal_count = confirmations + failures
    conversion_denominator = max(candidates, terminal_count)
    return {
        "lineage_depth": _lineage_depth(root_id, adjacency),
        "confirmation_conversion_rate": round(confirmations / conversion_denominator, 6) if conversion_denominator else 0.0,
        "failure_conversion_rate": round(failures / conversion_denominator, 6) if conversion_denominator else 0.0,
        "evidence_density": round(terminal_count / len(downstream), 6) if downstream else 0.0,
    }


def _reachable(root_id: str, adjacency: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    queue = deque(sorted(adjacency.get(root_id, set())))
    while queue:
        node_id = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        queue.extend(sorted(adjacency.get(node_id, set()) - seen))
    return seen


def _reverse_reachable(root_id: str, reverse_adjacency: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    queue = deque(sorted(reverse_adjacency.get(root_id, set())))
    while queue:
        node_id = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        queue.extend(sorted(reverse_adjacency.get(node_id, set()) - seen))
    return seen


def _lineage_depth(root_id: str, adjacency: dict[str, set[str]]) -> int:
    max_depth = 0
    queue = deque([(root_id, 0)])
    seen: set[str] = set()
    while queue:
        node_id, depth = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        max_depth = max(max_depth, depth)
        for child in sorted(adjacency.get(node_id, set())):
            queue.append((child, depth + 1))
    return max_depth


def _count_type(ids: set[str], nodes: dict[str, dict[str, Any]], node_type: str) -> int:
    return sum(1 for node_id in ids if nodes.get(node_id, {}).get("node_type") == node_type)


def _count_confirmations(ids: set[str], nodes: dict[str, dict[str, Any]]) -> int:
    return sum(1 for node_id in ids if nodes.get(node_id, {}).get("confirmation") is True)


def _count_failures(ids: set[str], nodes: dict[str, dict[str, Any]]) -> int:
    return sum(1 for node_id in ids if nodes.get(node_id, {}).get("failure") is True)


def _validate_graph(report: dict[str, Any]) -> None:
    nodes = report["nodes"]
    for edge in report["edges"]:
        if edge["source"] not in nodes or edge["target"] not in nodes:
            raise ValueError(f"invalid lineage edge: {edge}")
    boundary = report["authority_boundary"]
    forbidden_flags = ["candidate_generation_authorized", "candidate_promotion_authorized", "production_promotion_authorized", "ranking_changes_authorized"]
    if any(boundary.get(flag) for flag in forbidden_flags):
        raise ValueError("evidence lineage graph emitted forbidden authority")


def _summary_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Atlas V2 Evidence Lineage Graph",
        "",
        f"- observations analyzed: {summary['observations_analyzed']}",
        f"- claims analyzed: {summary['claims_analyzed']}",
        f"- hypotheses analyzed: {summary['hypotheses_analyzed']}",
        f"- candidates analyzed: {summary['candidates_analyzed']}",
        f"- families analyzed: {summary['families_analyzed']}",
        f"- validations analyzed: {summary['validations_analyzed']}",
        f"- confidence impact: {report['confidence_impact']}",
        "- authority: read-only research report; no candidate generation, promotion, or ranking changes",
        "",
        "## Highest Productivity Sources",
        "",
        *_ranking_lines(report["rankings"]["most_productive_observations"], "observation_id", "downstream_confirmations"),
        "",
        "## Most Productive Claims",
        "",
        *_ranking_lines(report["rankings"]["most_productive_claims"], "claim_id", "confirmation_count"),
        "",
        "## Most Productive Families",
        "",
        *_ranking_lines(report["rankings"]["most_productive_families"], "family_id", "downstream_confirmations"),
        "",
        "## Largest Dead-End Clusters",
        "",
        *_ranking_lines(report["rankings"]["dead_end_observation_clusters"], "observation_id", "downstream_claims"),
        "",
        "## Recommended Research Focus",
        "",
        *([f"- {item}" for item in report["recommended_research_focus"]] or ["- none"]),
        "",
    ]
    return "\n".join(lines)


def _ranking_lines(rows: list[dict[str, Any]], id_field: str, value_field: str) -> list[str]:
    if not rows:
        return ["- none"]
    return [f"- {row[id_field]}: {value_field}={row[value_field]}" for row in rows[:5]]


def _focus_lines(rows: list[str]) -> list[str]:
    if not rows:
        return ["- none"]
    return [f"- {item}" for item in rows]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _json_list(path: Path, key: str) -> list[dict[str, Any]]:
    payload = _load_json(path)
    value = payload.get(key, []) if payload else []
    return [row for row in value if isinstance(row, dict)]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item)]
    return []
