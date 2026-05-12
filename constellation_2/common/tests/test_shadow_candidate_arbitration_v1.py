from __future__ import annotations

import copy
import inspect
import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import constellation_2.common.shadow_candidate_arbitration_v1 as shadow  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402


DAY = "2026-05-12"
RUN_ID = f"sleeve_evaluation_kernel_v1:{DAY}"
PRODUCED_AT = "2026-05-12T14:00:00Z"


def _candidate(
    intent_id: str,
    *,
    engine_id: str = "C2_TREND_EQ_PRIMARY_V1",
    symbol: str = "SPY",
    status: str = "CANDIDATE_CREATED",
    gate_decision: str = "ALLOW",
    allowed: bool = True,
    rejection_reason: str = "",
) -> dict[str, object]:
    return {
        "candidate_id": f"candidate-{intent_id}",
        "engine_id": engine_id,
        "symbol_or_pair": symbol,
        "status": status,
        "reason_codes": ["INTENT_OUTPUT_CREATED"] if status == "CANDIDATE_CREATED" else [status],
        "rejection_reason": rejection_reason,
        "raw_intent_id": intent_id if status in {"CANDIDATE_CREATED", "SUPPRESSED", "SIGNAL_ONLY"} else "",
        "raw_intent_path": f"/tmp/{intent_id}.json" if intent_id else "",
        "raw_intent_hash": "a" * 64 if intent_id else "",
        "lifecycle_decision": "INTENT_CREATED" if intent_id else "",
        "lifecycle_reason_codes": [],
        "portfolio_gate_decision": gate_decision,
        "allowed_by_portfolio_gate": allowed,
        "input_artifact_paths": [],
        "output_artifact_paths": [],
        "lineage_hash": "b" * 64,
    }


def _manifest(candidates: list[dict[str, object]]) -> dict[str, object]:
    status_counts: dict[str, int] = {}
    for row in candidates:
        status = str(row["status"])
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "schema_id": "candidate_generation_manifest",
        "schema_version": "v1",
        "run_id": RUN_ID,
        "day_utc": DAY,
        "environment": "PAPER",
        "produced_at_utc": PRODUCED_AT,
        "source_rollup_path": "/tmp/rollup.json",
        "portfolio_activation_gate_path": "/tmp/gate.json",
        "candidate_rows": candidates,
        "summary": {"candidate_count": len(candidates), "status_counts": status_counts},
        "selected_intent_pointer_authoritative": True,
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "canonical_json_hash": "c" * 64,
    }


def _ledger() -> dict[str, object]:
    return {
        "schema_id": "sleeve_invocation_ledger",
        "schema_version": "v1",
        "run_id": RUN_ID,
        "day_utc": DAY,
        "environment": "PAPER",
        "scheduled_run_at_utc": PRODUCED_AT,
        "produced_at_utc": PRODUCED_AT,
        "invocations": [],
        "invocation_count": 0,
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "canonical_json_hash": "d" * 64,
    }


def _scoring() -> dict[str, object]:
    return {
        "schema_id": "portfolio_scoring",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "status": "PASS",
        "rankings": [
            {
                "intent_id": "vol-iwm",
                "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                "symbol": "IWM",
                "rank": 1,
                "score_total": 55.5,
                "score_components": {"signal_strength": 25.0},
                "executable_eligible": True,
                "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
            },
            {
                "intent_id": "trend-spy",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "symbol": "SPY",
                "rank": 2,
                "score_total": 49.0,
                "score_components": {"signal_strength": 20.0},
                "executable_eligible": True,
                "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
            },
            {
                "intent_id": "spread-spy",
                "sleeve_id": "C2_MARKET_NEUTRAL_SPREAD_V1",
                "symbol": "SPY",
                "rank": 0,
                "score_total": 0.0,
                "score_components": {},
                "executable_eligible": False,
                "reason_codes": ["PAIRED_EXECUTION_NOT_SUPPORTED"],
            },
        ],
    }


def _build(
    *,
    candidate_manifest: dict[str, object] | None = None,
    sleeve_invocation_ledger: dict[str, object] | None = None,
    selected_intent_id: str = "vol-iwm",
) -> dict[str, object]:
    return shadow.build_shadow_candidate_arbitration_v1(
        day_utc=DAY,
        environment="PAPER",
        run_id=RUN_ID,
        produced_at_utc=PRODUCED_AT,
        candidate_manifest=candidate_manifest,
        sleeve_invocation_ledger=sleeve_invocation_ledger,
        portfolio_scoring=_scoring(),
        selected_intent_pointer={
            "schema_id": "selected_intent_pointer",
            "day_utc": DAY,
            "status": "SELECTED" if selected_intent_id else "NO_EXECUTABLE_INTENT",
            "selected_intent": {"intent_id": selected_intent_id, "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "symbol": "IWM"} if selected_intent_id else {},
        },
        candidate_generation_manifest_path="/tmp/candidate_generation_manifest.v1.json",
        sleeve_invocation_ledger_path="/tmp/sleeve_invocation_ledger.v1.json",
        portfolio_scoring_path="/tmp/portfolio_scoring.v1.json",
        selected_intent_pointer_path="/tmp/selected_intent_pointer.v1.json",
    )


def test_shadow_artifact_ranks_all_executable_candidate_manifest_rows() -> None:
    manifest = _manifest(
        [
            _candidate("trend-spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY"),
            _candidate("vol-iwm", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM"),
        ]
    )

    payload = _build(candidate_manifest=manifest, sleeve_invocation_ledger=_ledger())

    assert payload["status"] == "PASS"
    assert [row["raw_intent_id"] for row in payload["ranked_candidates"]] == ["vol-iwm", "trend-spy"]
    assert [row["shadow_executable_rank"] for row in payload["ranked_candidates"]] == [1, 2]
    assert payload["shadow_winner"]["raw_intent_id"] == "vol-iwm"
    assert payload["winner_matches_production"] is True
    assert payload["difference_reason"] == "MATCH"
    assert payload["non_authoritative"] is True
    assert payload["execution_authority_granted"] is False
    assert payload["order_submission_attempted"] is False
    assert payload["trading_behavior_changed"] is False
    shadow.validate_shadow_candidate_arbitration_v1(payload)


def test_suppressed_signal_only_blocked_and_no_signal_remain_visible_but_non_executable() -> None:
    manifest = _manifest(
        [
            _candidate("vol-iwm", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM"),
            _candidate("cross-dbc", engine_id="C2_CROSS_ASSET_TREND_V1", symbol="DBC", status="SUPPRESSED", gate_decision="SUPPRESS", allowed=False),
            _candidate("spread-spy", engine_id="C2_MARKET_NEUTRAL_SPREAD_V1", symbol="SPY", status="SIGNAL_ONLY", gate_decision="SIGNAL_ONLY", allowed=False),
            _candidate("", engine_id="C2_EVENT_DISLOCATION_V1", symbol="QQQ", status="BLOCKED", gate_decision="", allowed=False, rejection_reason="MISSING_INPUTS"),
            _candidate("", engine_id="C2_MEAN_REVERSION_EQ_V1", symbol="SPY", status="NO_SIGNAL", gate_decision="", allowed=False),
        ]
    )

    payload = _build(candidate_manifest=manifest, sleeve_invocation_ledger=_ledger())

    by_status = {row["candidate_status"]: row for row in payload["ranked_candidates"]}
    assert by_status["SUPPRESSED"]["executable_candidate"] is False
    assert by_status["SIGNAL_ONLY"]["executable_candidate"] is False
    assert by_status["BLOCKED"]["executable_candidate"] is False
    assert by_status["NO_SIGNAL"]["executable_candidate"] is False
    assert payload["shadow_winner"]["raw_intent_id"] == "vol-iwm"
    assert "SIGNAL_ONLY" in payload["comparison_diagnostics"]["gate_reasons"]


def test_shadow_winner_difference_is_diagnostic_only() -> None:
    manifest = _manifest(
        [
            _candidate("trend-spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY"),
            _candidate("vol-iwm", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM"),
        ]
    )

    payload = _build(candidate_manifest=manifest, sleeve_invocation_ledger=_ledger(), selected_intent_id="trend-spy")

    assert payload["shadow_winner"]["raw_intent_id"] == "vol-iwm"
    assert payload["production_selected_intent"]["intent_id"] == "trend-spy"
    assert payload["winner_matches_production"] is False
    assert payload["difference_reason"] == "SHADOW_WINNER_DIFFERS_FROM_PRODUCTION"
    assert payload["comparison_diagnostics"]["score_delta"] == "6.5"
    assert payload["execution_authority_granted"] is False


def test_missing_candidate_manifest_fails_closed_as_diagnostic_unavailable() -> None:
    payload = _build(candidate_manifest=None, sleeve_invocation_ledger=_ledger())

    assert payload["status"] == "DIAGNOSTIC_UNAVAILABLE"
    assert payload["canonical_blocker"] == "CANDIDATE_GENERATION_MANIFEST_MISSING"
    assert payload["ranked_candidates"] == []
    assert payload["shadow_winner"] == {}
    assert payload["execution_authority_granted"] is False
    assert payload["order_submission_attempted"] is False
    shadow.validate_shadow_candidate_arbitration_v1(payload)


def test_stale_manifest_fails_closed_as_diagnostic_unavailable() -> None:
    manifest = _manifest([_candidate("vol-iwm", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM")])
    manifest["day_utc"] = "2026-05-11"

    payload = _build(candidate_manifest=manifest, sleeve_invocation_ledger=_ledger())

    assert payload["status"] == "DIAGNOSTIC_UNAVAILABLE"
    assert payload["canonical_blocker"] == "CANDIDATE_GENERATION_MANIFEST_DAY_MISMATCH"
    assert payload["execution_authority_granted"] is False


def test_builder_does_not_mutate_inputs_or_selected_pointer(tmp_path: Path) -> None:
    manifest = _manifest([_candidate("vol-iwm", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM")])
    ledger = _ledger()
    manifest_before = copy.deepcopy(manifest)
    ledger_before = copy.deepcopy(ledger)
    pointer_path = tmp_path / "pointers" / "selected_intent_pointer.v1.json"
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    pointer_payload = {"schema_id": "selected_intent_pointer", "day_utc": DAY, "selected_intent": {"intent_id": "vol-iwm"}}
    pointer_path.write_bytes(canonical_json_bytes_v1(pointer_payload) + b"\n")
    pointer_before = pointer_path.read_bytes()

    payload = _build(candidate_manifest=manifest, sleeve_invocation_ledger=ledger)
    output = shadow.write_shadow_candidate_arbitration_v1(truth_root=tmp_path / "truth", payload=payload)

    assert manifest == manifest_before
    assert ledger == ledger_before
    assert pointer_path.read_bytes() == pointer_before
    assert output.read_bytes() == canonical_json_bytes_v1(payload) + b"\n"
    assert "/home/node/constellation_runtime_data" not in str(output)


def test_module_has_no_runtime_trading_imports_or_hooks() -> None:
    source = inspect.getsource(shadow)
    forbidden = [
        "ib_insync",
        "placeOrder",
        "submit_order",
        "run_submit_boundary_status_v1",
        "run_aegis_paper_ready_kernel_v1",
        "interactivebrokers",
        "selected_intent_pointer_path(",
    ]
    for token in forbidden:
        assert token not in source


def test_artifact_round_trips_as_schema_valid_json() -> None:
    manifest = _manifest([_candidate("vol-iwm", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="IWM")])
    payload = _build(candidate_manifest=manifest, sleeve_invocation_ledger=_ledger())

    raw = canonical_json_bytes_v1(payload)
    loaded = json.loads(raw.decode("utf-8"))

    shadow.validate_shadow_candidate_arbitration_v1(loaded)
