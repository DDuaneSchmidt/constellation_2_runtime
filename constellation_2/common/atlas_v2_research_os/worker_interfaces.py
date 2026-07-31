from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ResearchOSWorker(ABC):
    worker_id: str
    worker_type: str
    supported_input_artifact_types: list[str]
    supported_output_artifact_types: list[str]

    @abstractmethod
    def run(self, input_artifacts: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None) -> Any:
        raise NotImplementedError

    @abstractmethod
    def dry_run(self, input_artifacts: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None) -> Any:
        raise NotImplementedError

    @abstractmethod
    def validate_inputs(self, input_artifacts: list[dict[str, Any]]) -> tuple[bool, list[str]]:
        raise NotImplementedError

    @abstractmethod
    def validate_outputs(self, output_artifacts: list[dict[str, Any]], input_artifacts: list[dict[str, Any]] | None = None) -> tuple[bool, list[str]]:
        raise NotImplementedError


class ClaimWorker(ResearchOSWorker, ABC):
    worker_type = "ClaimWorker"


class HypothesisWorker(ResearchOSWorker, ABC):
    worker_type = "HypothesisWorker"


class ExperimentDesignWorker(ResearchOSWorker, ABC):
    worker_type = "ExperimentDesignWorker"


class ExperimentExecutionWorker(ResearchOSWorker, ABC):
    worker_type = "ExperimentExecutionWorker"


class LearningWorker(ResearchOSWorker, ABC):
    worker_type = "LearningWorker"


class EvaluationWorker(ResearchOSWorker, ABC):
    worker_type = "EvaluationWorker"


class AttentionWorker(ResearchOSWorker, ABC):
    worker_type = "AttentionWorker"


class MemoryCuratorWorker(ResearchOSWorker, ABC):
    worker_type = "MemoryCuratorWorker"
