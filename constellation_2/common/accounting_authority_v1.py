from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _safe_read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"
    if not isinstance(payload, dict):
        return None, "NOT_OBJECT"
    return payload, None


def read_accounting_authority_state(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    nav_path = (root / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    cash_path = (root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json").resolve()

    nav_doc, nav_error = _safe_read_json(nav_path)
    cash_doc, cash_error = _safe_read_json(cash_path)

    reason_codes: list[str] = []
    if nav_doc is None:
        reason_codes.append(f"ACCOUNTING_NAV_{nav_error or 'UNAVAILABLE'}")
    if cash_doc is None:
        reason_codes.append(f"CASH_LEDGER_SNAPSHOT_{cash_error or 'UNAVAILABLE'}")

    return {
        "basis_class": "accounting_nav_v2" if nav_doc is not None else "UNKNOWN",
        "authoritative": bool(nav_doc is not None),
        "reason_codes": reason_codes,
        "cash_authority_basis": "cash_ledger_snapshot_v1" if cash_doc is not None else "UNKNOWN",
        "evidence_paths": {
            "accounting_nav_path": str(nav_path),
            "cash_ledger_snapshot_path": str(cash_path),
        },
    }
