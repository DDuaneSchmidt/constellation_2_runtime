from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


TAXONOMY_VERSION = "atlas_failure_taxonomy_001"

AUTHORITY_BOUNDARY = {
    "taxonomy_only": True,
    "production_integration_authorized": False,
    "trade_recommendation_authorized": False,
    "capital_recommendation_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_allocation_authorized": False,
    "candidate_promotion_authorized": False,
    "replay_override_authorized": False,
    "qualification_override_authorized": False,
    "governance_override_authorized": False,
    "automatic_memory_write_authorized": False,
}


@dataclass(frozen=True)
class FailureCategory:
    id: str
    name: str
    description: str
    typical_symptoms: list[str] = field(default_factory=list)
    detection_signals: list[str] = field(default_factory=list)
    related_failures: list[str] = field(default_factory=list)
    suggested_falsification_questions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


FAILURE_CATEGORIES: tuple[FailureCategory, ...] = (
    FailureCategory(
        id="REGIME_DEPENDENCY",
        name="Regime Dependency",
        description="A hypothesis, observation, or memory appears valid only under a regime that is unknown, mislabeled, unstable, or not reconstructable.",
        typical_symptoms=[
            "UNKNOWN or conflicting regime labels",
            "support disappears when trend, chop, volatility, or event context is separated",
            "generated-only memory reused without checking regime compatibility",
        ],
        detection_signals=[
            "regime_context is UNKNOWN, generated, or missing",
            "mechanism success is concentrated in one market state",
            "review language depends on after-the-fact regime assignment",
        ],
        related_failures=["failure-demo-opening-range", "FAIL_0016"],
        suggested_falsification_questions=[
            "Does the claim still hold when replay samples are split by predeclared regime?",
            "Can the regime be identified before the observation outcome is known?",
            "Does support vanish outside the favored regime?",
        ],
    ),
    FailureCategory(
        id="PROXY_DEPENDENCY",
        name="Proxy Dependency",
        description="Evidence depends on a proxy instrument, broad index, daily fixture, or indirect artifact that may not represent the candidate-specific behavior being claimed.",
        typical_symptoms=[
            "candidate-specific data is absent or replaced by broad market evidence",
            "daily proxy evidence is used for intraday mechanism claims",
            "SPY or index behavior explains the result better than the candidate mechanism",
        ],
        detection_signals=[
            "source artifact ids refer to proxy fixtures rather than candidate samples",
            "candidate review mentions proxy limitations",
            "expectancy improves only under broad market proxy filters",
        ],
        related_failures=["FAIL_0008", "FAIL_0016"],
        suggested_falsification_questions=[
            "Does the result survive candidate-specific data reconstruction?",
            "Does a naive proxy baseline explain the same return profile?",
            "Is the asserted mechanism observable in the candidate artifact itself?",
        ],
    ),
    FailureCategory(
        id="RUNTIME_DEPENDENCY",
        name="Runtime Dependency",
        description="An architecture or workflow decision assumes supporting runtime truth, artifact availability, or legacy compatibility that has not actually been retired or proven.",
        typical_symptoms=[
            "architecture says a layer is obsolete while runtime still consumes it",
            "source artifacts referenced by execution paths cannot be loaded",
            "lineage passes but runtime execution fails closed",
        ],
        detection_signals=[
            "ArtifactStoreError",
            "runtime truth graph still references legacy artifacts",
            "execution path has missing source_artifact_ids",
        ],
        related_failures=[
            "FAIL_0011",
            "failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43",
            "failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d",
        ],
        suggested_falsification_questions=[
            "Does verified runtime truth still consume the dependency?",
            "Can every source artifact be loaded before work is selected?",
            "Does the workflow fail closed when a required artifact is absent?",
        ],
    ),
    FailureCategory(
        id="DATA_QUALITY",
        name="Data Quality",
        description="A claim depends on missing, immature, partial, stale, or non-reconstructable data rather than observed evidence.",
        typical_symptoms=[
            "macro, event, sleeve, or intraday data is missing",
            "sample evidence is underpowered or unresolved",
            "data availability is inferred from workflow progress",
        ],
        detection_signals=[
            "missing required data",
            "insufficient sample size",
            "open observations exceed closed outcomes",
            "event or macro metadata cannot be reconstructed",
        ],
        related_failures=["FAIL_0006", "FAIL_0009", "FAIL_0012"],
        suggested_falsification_questions=[
            "Is the required data present, timestamped, and reconstructable?",
            "Are there enough resolved samples to support the claim?",
            "Would the conclusion change if unresolved or partial samples were excluded?",
        ],
    ),
    FailureCategory(
        id="WORKER_COMPATIBILITY",
        name="Worker Compatibility",
        description="A ready backlog item or research action is selected even though no compatible connected worker can execute it.",
        typical_symptoms=[
            "ready queue contains items that cannot run",
            "worker registry has no connected worker for the required capability",
            "execution fails before research evidence is generated",
        ],
        detection_signals=[
            "NO_COMPATIBLE_CONNECTED_WORKER",
            "required worker capability absent from registry",
            "backlog status and worker availability disagree",
        ],
        related_failures=["failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574"],
        suggested_falsification_questions=[
            "Is there a connected worker advertising the required capability?",
            "Does the backlog item declare requirements that match available workers?",
            "Can a dry-run execute without selecting an incompatible item?",
        ],
    ),
    FailureCategory(
        id="CERTIFICATION_BLOCKER",
        name="Certification Blocker",
        description="A workflow treats partial safety, lineage, or governance progress as sufficient even though certification still blocks execution.",
        typical_symptoms=[
            "blocker count drops but certification is still not passed",
            "lineage and governance pass while certification fails",
            "research readiness is confused with execution readiness",
        ],
        detection_signals=[
            "CERTIFICATION_BLOCK",
            "SAFETY_GATE_FAILED",
            "certification status is blocked despite fewer blockers",
        ],
        related_failures=[
            "failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415",
            "failure-2026-06-05-certification-certification-block-0d8cd9475db6",
        ],
        suggested_falsification_questions=[
            "Has certification fully passed, rather than merely improved?",
            "Which blocker remains unresolved?",
            "Does the workflow stay blocked when certification has not passed?",
        ],
    ),
    FailureCategory(
        id="WARNING_RECURRENCE",
        name="Warning Recurrence",
        description="Non-blocking warnings are treated as one-day operational noise even when repeated warnings predict later blocked readiness.",
        typical_symptoms=[
            "daily integrity passes with repeated warnings",
            "warning categories recur across several available days",
            "warnings are not converted into research follow-up",
        ],
        detection_signals=[
            "same warning category recurs for multiple days",
            "warning state later escalates to blocked graph readiness",
            "report says clean while warning list remains non-empty",
        ],
        related_failures=["FAIL_0013", "FAIL_0015"],
        suggested_falsification_questions=[
            "Did this warning clear or recur across the next review window?",
            "Does the same category appear in multiple integrity reports?",
            "Would treating warnings as evidence gaps have changed the review?",
        ],
    ),
    FailureCategory(
        id="DUPLICATE_CLUSTER",
        name="Duplicate Cluster",
        description="Multiple candidates, claims, or observation clusters count the same structural idea, sample days, or mechanism exposure as independent evidence.",
        typical_symptoms=[
            "many candidates share the same mechanism and source window",
            "near-duplicate claims inflate throughput or support",
            "paper-forward plans consume capacity on overlapping samples",
        ],
        detection_signals=[
            "same mechanism and regime repeated across candidate ids",
            "deduplication or cluster-splitting reports flag overlap",
            "recommended reviews contain redundant candidate families",
        ],
        related_failures=["FAIL_0007", "FAIL_0014"],
        suggested_falsification_questions=[
            "Are these candidates independent or the same exposure restated?",
            "Do sample days overlap across the candidate cluster?",
            "Does the evidence remain after one representative per cluster is kept?",
        ],
    ),
    FailureCategory(
        id="INSUFFICIENT_EVIDENCE",
        name="Insufficient Evidence",
        description="Architecture, documentation, replay, or generated claims are treated as stronger than the evidence level permits.",
        typical_symptoms=[
            "architecture maturity outruns empirical validation",
            "positive replay is used without sufficient sample size",
            "research artifacts exist but remain underpowered",
        ],
        detection_signals=[
            "evidence level is GENERATED_ONLY or HISTORICAL_REPLAY only",
            "small sample positive replay",
            "no closed paper-forward outcomes",
            "capital or readiness language appears before evidence is mature",
        ],
        related_failures=["FAIL_0004", "FAIL_0006", "FAIL_0010"],
        suggested_falsification_questions=[
            "What independent evidence supports the claim beyond architecture or generation?",
            "How many closed samples exist?",
            "Would the conclusion survive if replay-only evidence were labeled as non-forward evidence?",
        ],
    ),
    FailureCategory(
        id="MECHANISM_MISMATCH",
        name="Mechanism Mismatch",
        description="The named mechanism does not explain the observed behavior as well as an alternate mechanism, baseline, or broader market condition.",
        typical_symptoms=[
            "technical indicator support is broad but not relationship-specific",
            "volatility expansion, breakout, opening range, and liquidity sweep overlap",
            "mechanism works only under naive baseline recovery",
        ],
        detection_signals=[
            "mechanism_tags are broad or interchangeable",
            "baseline-recoverable performance",
            "support lacks relationship-specific evidence",
        ],
        related_failures=["FAIL_0008", "FAIL_0016"],
        suggested_falsification_questions=[
            "What competing mechanism explains the same samples?",
            "Does the effect survive a naive baseline comparison?",
            "Can the relationship-specific mechanism be observed before outcome?",
        ],
    ),
    FailureCategory(
        id="QUALIFICATION_MISMATCH",
        name="Qualification Mismatch",
        description="A candidate's narrative, replay score, or workflow status is mistaken for qualification, eligibility, or review readiness.",
        typical_symptoms=[
            "positive replay exists but edge score remains below threshold",
            "paper workflow progress is read as readiness progress",
            "candidate volume is confused with quality evidence",
        ],
        detection_signals=[
            "paper_trade_eligible is false despite positive narrative",
            "edge score below threshold",
            "qualification or certification reason contradicts readiness language",
        ],
        related_failures=["FAIL_0005", "FAIL_0007", "FAIL_0010"],
        suggested_falsification_questions=[
            "What exact qualification threshold has been met?",
            "Does the candidate remain ineligible despite supporting narrative?",
            "Which gate is blocking the candidate and why?",
        ],
    ),
    FailureCategory(
        id="OBSERVATION_DRIFT",
        name="Observation Drift",
        description="Observed forward samples, open positions, or accumulated observations drift away from the original hypothesis, trigger, or evidence question.",
        typical_symptoms=[
            "open observations accumulate faster than resolved outcomes",
            "paper-forward samples do not match replay recurrence rules",
            "observation plans lack predeclared invalidation rules",
        ],
        detection_signals=[
            "sample_size grows while wins/losses remain unresolved",
            "hypothesis_supported and hypothesis_weakened both lack clear cause",
            "minimum sample size or trigger definition is absent",
        ],
        related_failures=["FAIL_0006"],
        suggested_falsification_questions=[
            "Do forward observations match the original trigger and mechanism?",
            "Are closed outcomes reviewed before strengthening the claim?",
            "What invalidation rule prevents drift from becoming anecdote?",
        ],
    ),
    FailureCategory(
        id="SELECTION_BIAS",
        name="Selection Bias",
        description="The research process overweights selected, surviving, convenient, or high-volume artifacts and underweights blocked, dormant, or rejected artifacts.",
        typical_symptoms=[
            "candidate count is treated as progress",
            "low-output sleeves are judged without checking valid no-signal states",
            "only supported replay examples are emphasized",
        ],
        detection_signals=[
            "rejected, dormant, or blocked items omitted from summary",
            "throughput metrics are cited without quality metrics",
            "surviving clusters share source or mechanism concentration",
        ],
        related_failures=["FAIL_0007", "FAIL_0014"],
        suggested_falsification_questions=[
            "What did the rejected or dormant artifacts show?",
            "Does the conclusion hold when blocked and no-signal sleeves are included?",
            "Are volume metrics separated from quality metrics?",
        ],
    ),
    FailureCategory(
        id="CONTEXT_OMISSION",
        name="Context Omission",
        description="A review omits required market, macro, event, calendar, source, or governance context needed to interpret an artifact.",
        typical_symptoms=[
            "event reaction lacks event metadata",
            "macro readiness is inferred from workflow behavior",
            "session or calendar boundary is not specified",
        ],
        detection_signals=[
            "market_context, event source, or regime fields are empty",
            "claim uses broad context language without source lineage",
            "governance or authority context is missing from the artifact",
        ],
        related_failures=["FAIL_0012", "FAIL_0016"],
        suggested_falsification_questions=[
            "Which context fields are required to reproduce the claim?",
            "Would the conclusion change under different event, macro, or calendar context?",
            "Is the missing context an evidence gap rather than a workflow gap?",
        ],
    ),
)


def list_failure_categories() -> list[FailureCategory]:
    return list(FAILURE_CATEGORIES)


def get_failure_category(category_id: str) -> FailureCategory:
    normalized = category_id.strip().upper()
    for category in FAILURE_CATEGORIES:
        if category.id == normalized:
            return category
    raise KeyError(f"unknown Atlas failure category: {category_id}")


def taxonomy_as_dict() -> dict[str, Any]:
    return {
        "schema_id": "atlas_failure_taxonomy.v1",
        "taxonomy_version": TAXONOMY_VERSION,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "categories": [category.to_dict() for category in FAILURE_CATEGORIES],
    }


def find_categories_for_text(text: str) -> list[FailureCategory]:
    normalized = text.lower()
    matches: list[FailureCategory] = []
    for category in FAILURE_CATEGORIES:
        searchable_terms = [
            category.id.replace("_", " ").lower(),
            category.name.lower(),
            *[signal.lower() for signal in category.detection_signals],
            *[symptom.lower() for symptom in category.typical_symptoms],
        ]
        if any(term and term in normalized for term in searchable_terms):
            matches.append(category)
    return matches
