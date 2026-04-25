import yaml


def load_policy(path: str, *, allow_compilation_input: bool = False):
    if not allow_compilation_input:
        raise RuntimeError(f"RAW_POLICY_RUNTIME_AUTHORITY_DISABLED:{path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
