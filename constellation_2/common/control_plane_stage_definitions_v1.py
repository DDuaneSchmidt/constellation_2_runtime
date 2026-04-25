from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict

from constellation_2.common.control_plane_stage_admission_v1 import (
    CONTROL_STAGE_CONTEXT_ADMITTED,
    CONTROL_STAGE_CONTEXT_CERTIFICATION,
    CONTROL_STAGE_DAY_ADMITTED,
    CONTROL_STAGE_DAY_CERTIFICATION,
    CONTROL_STAGE_EXECUTION_BUILD_ADMITTED,
    CONTROL_STAGE_EXECUTION_BUILD_CERTIFICATION,
    CONTROL_STAGE_SESSION_ADMITTED,
    CONTROL_STAGE_SESSION_CERTIFICATION,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    validate_day_activation_family_v1,
    validate_execution_build_family_v1,
    validate_global_context_family_v1,
    validate_session_authority_family_v1,
)


POLICY_EVALUATE = "evaluate"
POLICY_ADMIT_AND_CERTIFY = "admit_and_certify"
POLICY_CERTIFY_ONLY = "certify_only"
POLICY_RECOMPUTE_FROZEN = "recompute_frozen"
POLICY_SUPERSEDE_FROM_NEW_INPUTS = "supersede_from_new_inputs"
POLICY_EXPLAIN_BLOCKED = "explain_blocked"

ALL_TRANSITION_POLICIES: tuple[str, ...] = (
    POLICY_EVALUATE,
    POLICY_ADMIT_AND_CERTIFY,
    POLICY_CERTIFY_ONLY,
    POLICY_RECOMPUTE_FROZEN,
    POLICY_SUPERSEDE_FROM_NEW_INPUTS,
    POLICY_EXPLAIN_BLOCKED,
)

CONTROL_STAGE_DAY = "CONTROL_STAGE_DAY_ACTIVATION"
CONTROL_STAGE_CONTEXT = "CONTROL_STAGE_GLOBAL_CONTEXT"
CONTROL_STAGE_SESSION = "CONTROL_STAGE_SESSION_AUTHORITY"
CONTROL_STAGE_EXECUTION_BUILD = "CONTROL_STAGE_EXECUTION_BUILD"


@dataclass(frozen=True)
class ControlPlaneStageDefinitionV1:
    stage_id: str
    artifact_id: str
    certification_artifact_id: str
    upstream_artifact_id: str
    validator_fn: Callable[..., Dict[str, Any]]
    required_families: tuple[str, ...]
    stage_invariants: tuple[str, ...]
    allowed_policies: tuple[str, ...]
    current_projection_allowed: bool


STAGE_DEFINITIONS: tuple[ControlPlaneStageDefinitionV1, ...] = (
    ControlPlaneStageDefinitionV1(
        stage_id=CONTROL_STAGE_DAY,
        artifact_id=CONTROL_STAGE_DAY_ADMITTED,
        certification_artifact_id=CONTROL_STAGE_DAY_CERTIFICATION,
        upstream_artifact_id="",
        validator_fn=validate_day_activation_family_v1,
        required_families=(
            "day_activation_build_v1",
            "day_activation_package_v1",
        ),
        stage_invariants=(
            "day activation closure is complete",
            "day activation package is sealed and context-matched",
        ),
        allowed_policies=ALL_TRANSITION_POLICIES,
        current_projection_allowed=False,
    ),
    ControlPlaneStageDefinitionV1(
        stage_id=CONTROL_STAGE_CONTEXT,
        artifact_id=CONTROL_STAGE_CONTEXT_ADMITTED,
        certification_artifact_id=CONTROL_STAGE_CONTEXT_CERTIFICATION,
        upstream_artifact_id=CONTROL_STAGE_DAY_ADMITTED,
        validator_fn=validate_global_context_family_v1,
        required_families=(
            "global_context_build_v1",
            "global_context_package_v1",
        ),
        stage_invariants=(
            "global context closure is complete",
            "global context package binds the day-activation package",
        ),
        allowed_policies=ALL_TRANSITION_POLICIES,
        current_projection_allowed=False,
    ),
    ControlPlaneStageDefinitionV1(
        stage_id=CONTROL_STAGE_SESSION,
        artifact_id=CONTROL_STAGE_SESSION_ADMITTED,
        certification_artifact_id=CONTROL_STAGE_SESSION_CERTIFICATION,
        upstream_artifact_id=CONTROL_STAGE_CONTEXT_ADMITTED,
        validator_fn=validate_session_authority_family_v1,
        required_families=(
            "target_day_build_v1",
            "target_day_admission_v1",
            "session_promotion_decision_v1",
            "active_session_v1",
        ),
        stage_invariants=(
            "target-day build is complete and closed",
            "target-day admission is binding",
            "session promotion is promoted",
            "active session is coherent for the same target day",
        ),
        allowed_policies=ALL_TRANSITION_POLICIES,
        current_projection_allowed=True,
    ),
    ControlPlaneStageDefinitionV1(
        stage_id=CONTROL_STAGE_EXECUTION_BUILD,
        artifact_id=CONTROL_STAGE_EXECUTION_BUILD_ADMITTED,
        certification_artifact_id=CONTROL_STAGE_EXECUTION_BUILD_CERTIFICATION,
        upstream_artifact_id=CONTROL_STAGE_SESSION_ADMITTED,
        validator_fn=validate_execution_build_family_v1,
        required_families=(
            "execution_build_v1",
            "execution_package_v1",
        ),
        stage_invariants=(
            "execution build closure is complete",
            "execution package is sealed and binds the same submission",
        ),
        allowed_policies=ALL_TRANSITION_POLICIES,
        current_projection_allowed=False,
    ),
)

STAGE_DEFINITION_BY_ID = {definition.stage_id: definition for definition in STAGE_DEFINITIONS}

