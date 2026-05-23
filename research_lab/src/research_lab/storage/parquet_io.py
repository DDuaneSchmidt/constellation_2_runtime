from __future__ import annotations

import json
from pathlib import Path
from typing import Any



def _pandas_module() -> Any | None:
    try:
        import pandas as pd  # type: ignore

        return pd
    except Exception:
        return None


def parquet_engine_available() -> bool:
    try:
        import pyarrow  # noqa: F401  # type: ignore

        return True
    except Exception:
        pass
    try:
        import fastparquet  # noqa: F401  # type: ignore

        return True
    except Exception:
        return False


def write_parquet_records(path: Path, records: list[dict[str, Any]], *, allow_json_fallback: bool = False) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd = _pandas_module()
    if pd is not None and parquet_engine_available():
        pd.DataFrame(records).to_parquet(path, index=False)
        return {"path": str(path), "format": "parquet", "row_count": len(records)}
    if allow_json_fallback:
        path.write_text(
            "\n".join(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True) for row in records)
            + ("\n" if records else ""),
            encoding="utf-8",
        )
        return {"path": str(path), "format": "jsonl_test_fallback", "row_count": len(records)}
    raise RuntimeError("Parquet writer unavailable; install pandas with pyarrow or fastparquet")


def read_parquet_records(path: Path) -> list[dict[str, Any]]:
    pd = _pandas_module()
    if pd is not None and parquet_engine_available():
        return [dict(row) for row in pd.read_parquet(path).to_dict("records")]
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def file_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
