from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REGISTRY_PATH = Path(__file__).resolve().parents[2] / "governance" / "02_REGISTRIES" / "AEGIS_DOMAIN_SOURCE_REGISTRY_V1.json"


def load_domain_source_registry_v1(path: Path | str | None = None) -> dict[str, Any]:
    registry_path = Path(path).expanduser().resolve() if path else REGISTRY_PATH
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("schema_id", "aegis_domain_source_registry")
    payload.setdefault("schema_version", "v1")
    payload.setdefault("domains", [])
    payload["registry_path"] = str(registry_path)
    return payload


def domain_source_contracts_by_id_v1(registry: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    payload = registry if isinstance(registry, dict) else load_domain_source_registry_v1()
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("domains", []) if isinstance(payload.get("domains"), list) else []:
        if not isinstance(row, dict):
            continue
        domain_id = str(row.get("domain_id") or "").strip().upper()
        if domain_id:
            out[domain_id] = dict(row)
    return out


def domain_source_contract_v1(domain_id: str, registry: dict[str, Any] | None = None) -> dict[str, Any]:
    return domain_source_contracts_by_id_v1(registry).get(str(domain_id or "").strip().upper(), {})


def render_domain_source_path_v1(*, truth_root: Path | str, day_utc: str, contract: dict[str, Any]) -> Path:
    template = str(contract.get("artifact_path_template") or "").strip()
    if not template:
        return Path("")
    rel = template.replace("{day_utc}", day_utc).replace("{day}", day_utc)
    return Path(truth_root).expanduser().resolve() / rel


def render_domain_source_command_v1(*, command_template: str, truth_root: Path | str, day_utc: str, domain_id: str) -> str:
    return (command_template or "").replace("{truth_root}", str(Path(truth_root).expanduser().resolve())).replace("{day_utc}", day_utc).replace("{day}", day_utc).replace("{domain_id}", domain_id)
