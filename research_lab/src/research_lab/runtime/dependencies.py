from __future__ import annotations

import importlib
import importlib.metadata
import tempfile
from pathlib import Path
from typing import Any


RESEARCH_DEPENDENCIES = {
    "pandas": "2.2.3",
    "pyarrow": "18.1.0",
    "duckdb": "1.1.3",
    "yfinance": "0.2.54",
    "yaml": "6.0.3",
    "jsonschema": "4.25.1",
}


def _module_status(name: str, expected_version: str) -> dict[str, Any]:
    try:
        module = importlib.import_module(name)
    except Exception as exc:
        return {
            "name": name,
            "expected_version": expected_version,
            "installed": False,
            "version": None,
            "status": "MISSING",
            "error": f"{type(exc).__name__}: {exc}",
        }
    package_name = "PyYAML" if name == "yaml" else name
    try:
        version = importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        version = str(getattr(module, "__version__", "unknown"))
    return {
        "name": name,
        "expected_version": expected_version,
        "installed": True,
        "version": version,
        "status": "OK" if version == expected_version else "VERSION_MISMATCH",
        "error": None,
    }


def _parquet_smoke() -> dict[str, Any]:
    try:
        import pandas as pd  # type: ignore

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "smoke.parquet"
            pd.DataFrame([{"symbol": "SPY", "close": 1.0}]).to_parquet(path, index=False)
            rows = pd.read_parquet(path).to_dict("records")
        return {"status": "OK", "rows": rows, "error": None}
    except Exception as exc:
        return {"status": "FAILED", "rows": [], "error": f"{type(exc).__name__}: {exc}"}


def _duckdb_smoke() -> dict[str, Any]:
    try:
        import duckdb  # type: ignore

        value = duckdb.sql("SELECT 1 AS ok").fetchall()[0][0]
        return {"status": "OK" if value == 1 else "FAILED", "value": value, "error": None}
    except Exception as exc:
        return {"status": "FAILED", "value": None, "error": f"{type(exc).__name__}: {exc}"}


def research_dependency_health() -> dict[str, Any]:
    modules = [_module_status(name, version) for name, version in RESEARCH_DEPENDENCIES.items()]
    parquet = _parquet_smoke()
    duckdb = _duckdb_smoke()
    missing = [row["name"] for row in modules if not row["installed"]]
    version_mismatch = [row["name"] for row in modules if row["status"] == "VERSION_MISMATCH"]
    checks_ok = not missing and parquet["status"] == "OK" and duckdb["status"] == "OK"
    if checks_ok and not version_mismatch:
        status = "READY"
    elif not missing and checks_ok:
        status = "READY_WITH_VERSION_WARNINGS"
    else:
        status = "NOT_READY"
    return {
        "status": status,
        "dependencies": modules,
        "parquet_smoke": parquet,
        "duckdb_smoke": duckdb,
        "missing_dependencies": missing,
        "version_mismatches": version_mismatch,
        "install_commands": [
            "python3 -m venv research_lab/.venv",
            "research_lab/.venv/bin/python -m pip install -r research_lab/requirements-research.lock",
        ],
        "schema_version": "research_dependency_health.v1",
    }
