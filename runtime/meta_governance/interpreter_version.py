CANONICAL_INTERPRETER_VERSION = "constellation.meta_governance.interpreter.v1"


def get_interpreter_version() -> str:
    return CANONICAL_INTERPRETER_VERSION


def require_matching_interpreter(value: str) -> None:
    if not value:
        raise ValueError("INTERPRETER_VERSION_REQUIRED")
    if value != CANONICAL_INTERPRETER_VERSION:
        raise ValueError(
            f"INTERPRETER_VERSION_MISMATCH:expected={CANONICAL_INTERPRETER_VERSION}:actual={value}"
        )
