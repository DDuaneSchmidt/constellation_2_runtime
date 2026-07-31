from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


UNKNOWN = "UNKNOWN"


class SleeveAttributionRecoveryIndex:
    def __init__(self, *, truth_root: Path | str, day_utc: str) -> None:
        self.root = Path(truth_root).expanduser().resolve()
        self.day = str(day_utc)
        self.paper_entry_receipts = _read_json(self.root / "reports" / "aegis_paper_entry_receipts_v1" / self.day / "paper_entry_receipts.v1.json")
        self.candidate_lifecycle = _read_json(self.root / "reports" / "aegis_candidate_lifecycle_projection_v1" / self.day / "candidate_lifecycle_projection.v1.json")
        self.candidate_contracts = _latest_payloads(self.root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json", self.day)
        self.signal_boundary = _read_json(self.root / "reports" / "aegis_signal_evidence_boundary_v1" / self.day / "signal_evidence_boundary.v1.json")
        self.signal_graph = _read_json(self.root / "reports" / "aegis_signal_evidence_graph_v1" / self.day / "signal_evidence_graph.v1.json")
        self.intent_arbitration = _read_json(self.root / "reports" / "intent_arbitration_v1" / self.day / "intent_arbitration.v1.json")
        self.candidate_intent_plane = _read_json(self.root / "reports" / "candidate_intent_plane_v1" / self.day / "candidate_intent_plane.v1.json")
        self.selected_intent_promotion = _read_json(self.root / "reports" / "aegis_selected_intent_promotion_v1" / self.day / "selected_intent_promotion.v1.json")
        self.paper_testing_sleeves = _read_json(self.root / "reports" / "aegis_paper_testing_sleeve_v1" / self.day / "paper_testing_sleeve.v1.json")

    def recover(self, row: Mapping[str, Any]) -> dict[str, Any]:
        context = _position_context(row)
        attempts: list[dict[str, Any]] = []
        raw_signal_id = ""

        for source, candidates in (
            ("paper_position_ledger_embedded_sleeve_id", [row, context["candidate_lineage"]]),
            ("paper_entry_receipt", [context["source_receipt"], *self._paper_entry_receipt_rows(context)]),
            ("candidate_lifecycle_projection", self._candidate_lifecycle_rows(context)),
            ("candidate_contract", self._candidate_contract_rows(context)),
        ):
            result = self._first_sleeve(source, candidates)
            attempts.append(result)
            if result["recovered_sleeve_id"]:
                return _reconciliation(context, source, result["recovered_sleeve_id"], attempts)

        boundary_rows = self._signal_boundary_rows(context)
        for boundary in boundary_rows:
            raw_signal_id = raw_signal_id or str(boundary.get("raw_signal_id") or boundary.get("intent_id") or "")
        result = self._first_sleeve("signal_evidence_boundary", boundary_rows)
        attempts.append(result)
        if result["recovered_sleeve_id"]:
            return _reconciliation(context, "signal_evidence_boundary", result["recovered_sleeve_id"], attempts)

        output_rows = self._output_intent_rows(context, raw_signal_id=raw_signal_id)
        result = self._first_sleeve("output_intent_lineage", output_rows)
        attempts.append(result)
        if result["recovered_sleeve_id"]:
            return _reconciliation(context, "output_intent_lineage", result["recovered_sleeve_id"], attempts)

        paper_testing_rows = self._paper_testing_rows(context, raw_signal_id=raw_signal_id)
        result = self._first_sleeve("research_paper_testing_sleeve_linkage", paper_testing_rows)
        attempts.append(result)
        if result["recovered_sleeve_id"]:
            return _reconciliation(context, "research_paper_testing_sleeve_linkage", result["recovered_sleeve_id"], attempts)

        return _reconciliation(context, "all_lineage_sources", UNKNOWN, attempts)

    def _paper_entry_receipt_rows(self, context: Mapping[str, str]) -> list[dict[str, Any]]:
        return _guarded_rows(_rows(self.paper_entry_receipts, "receipts", "entry_receipts", "rows"), context, require_session=True)

    def _candidate_lifecycle_rows(self, context: Mapping[str, str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key in ("current_session_candidates", "current_session_candidates_all", "open_paper_positions", "closed_paper_positions", "blocked_or_skipped_candidates"):
            rows.extend(_rows(self.candidate_lifecycle, key))
        return _guarded_rows(rows, context, require_session=True)

    def _candidate_contract_rows(self, context: Mapping[str, str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for payload in self.candidate_contracts:
            rows.extend(_rows(payload, "candidate_contracts", "contracts", "rows"))
        return _guarded_rows(rows, context, require_session=False)

    def _signal_boundary_rows(self, context: Mapping[str, str]) -> list[dict[str, Any]]:
        return _guarded_rows(_rows(self.signal_boundary, "boundary_rows", "rows", "signals"), context, require_session=True)

    def _output_intent_rows(self, context: Mapping[str, str], *, raw_signal_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows.extend(_rows(self.signal_graph, "signals", "rows"))
        rows.extend(_rows(self.intent_arbitration, "portfolio_ranking", "candidate_intents", "selected_intents", "output_intents"))
        rows.extend(_rows(self.candidate_intent_plane, "candidate_intents", "intents", "snapshots", "selected_intents"))
        candidate = self.selected_intent_promotion.get("candidate")
        if isinstance(candidate, dict):
            rows.append(candidate)
        rows.append(self.selected_intent_promotion)
        return _guarded_output_rows(rows, context, raw_signal_id=raw_signal_id)

    def _paper_testing_rows(self, context: Mapping[str, str], *, raw_signal_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows.extend(_rows(self.paper_testing_sleeves, "sleeves", "paper_testing_sleeves", "rows"))
        for sleeve in _rows(self.paper_testing_sleeves, "sleeves", "paper_testing_sleeves", "rows"):
            linked = sleeve.get("linked_symbols")
            if isinstance(linked, list):
                for item in linked:
                    if isinstance(item, dict):
                        rows.append({**item, "sleeve_id": sleeve.get("sleeve_id")})
                    else:
                        rows.append({"symbol": item, "sleeve_id": sleeve.get("sleeve_id")})
        return _guarded_output_rows(rows, context, raw_signal_id=raw_signal_id)

    def _first_sleeve(self, source: str, rows: list[Mapping[str, Any]]) -> dict[str, Any]:
        blockers: list[str] = []
        seen: set[str] = set()
        for row in rows:
            sleeve = _extract_sleeve(row)
            if sleeve:
                seen.add(sleeve)
        if len(seen) == 1:
            return {"source": source, "recovered_sleeve_id": next(iter(seen)), "blocker_reason": ""}
        if len(seen) > 1:
            blockers.append("MULTIPLE_CONFLICTING_SLEEVES")
        elif not rows:
            blockers.append("NO_GUARDED_LINEAGE_ROW")
        else:
            blockers.append("MATCHED_ROWS_HAVE_NO_SLEEVE_ID")
        return {"source": source, "recovered_sleeve_id": "", "blocker_reason": ",".join(blockers)}


def recover_sleeve_id_v1(row: Mapping[str, Any], *, recovery_index: SleeveAttributionRecoveryIndex) -> str:
    result = recovery_index.recover(row)
    return str(result.get("recovered_sleeve_id") or UNKNOWN)


def build_position_sleeve_reconciliation_v1(*, positions: list[Mapping[str, Any]], recovery_index: SleeveAttributionRecoveryIndex) -> list[dict[str, Any]]:
    out = []
    for row in positions:
        existing = str(row.get("sleeve_id") or UNKNOWN)
        if existing != UNKNOWN:
            continue
        out.append(recovery_index.recover(row))
    return out


def _position_context(row: Mapping[str, Any]) -> dict[str, Any]:
    receipt = row.get("source_receipt") if isinstance(row.get("source_receipt"), Mapping) else {}
    lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}
    candidate_id = str(row.get("candidate_id") or receipt.get("candidate_id") or lineage.get("candidate_id") or "").strip()
    contract_id = str(row.get("candidate_contract_id") or receipt.get("candidate_contract_id") or "").strip() or (candidate_id if candidate_id.startswith("candidate_contract_") else "")
    return {
        "position_id": str(row.get("position_id") or "").strip(),
        "symbol": str(row.get("symbol") or receipt.get("symbol") or "").upper().strip(),
        "entry_receipt_id": str(row.get("entry_receipt_id") or receipt.get("receipt_id") or "").strip(),
        "candidate_id": candidate_id,
        "candidate_contract_id": contract_id,
        "paper_session_id": str(row.get("paper_session_id") or receipt.get("paper_session_id") or lineage.get("paper_session_id") or "").strip(),
        "entry_timestamp": str(row.get("entry_time") or row.get("entry_timestamp") or receipt.get("timestamp_utc") or receipt.get("timestamp") or "").strip(),
        "existing_sleeve_field": str(row.get("sleeve_id") or UNKNOWN),
        "source_receipt": receipt,
        "candidate_lineage": lineage,
    }


def _reconciliation(context: Mapping[str, Any], source: str, sleeve: str, attempts: list[dict[str, Any]]) -> dict[str, Any]:
    blockers = [f"{row['source']}:{row['blocker_reason']}" for row in attempts if row.get("blocker_reason")]
    blocker = "" if sleeve != UNKNOWN else (";".join(blockers) or "NO_LINEAGE_SOURCES_AVAILABLE")
    return {
        "position_id": context["position_id"],
        "symbol": context["symbol"],
        "entry_receipt_id": context["entry_receipt_id"],
        "candidate_id": context["candidate_id"],
        "candidate_contract_id": context["candidate_contract_id"],
        "paper_session_id": context["paper_session_id"],
        "entry_timestamp": context["entry_timestamp"],
        "existing_sleeve_field": context["existing_sleeve_field"],
        "attempted_recovery_source": source,
        "recovered_sleeve_id": sleeve,
        "blocker_reason": blocker,
        "attempts": attempts,
        "symbol_only_match_used": False,
    }


def _guarded_rows(rows: list[dict[str, Any]], context: Mapping[str, str], *, require_session: bool) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if not _candidate_matches(row, context):
            continue
        if require_session and not _session_matches(row, context):
            continue
        out.append(row)
    return out


def _guarded_output_rows(rows: list[dict[str, Any]], context: Mapping[str, str], *, raw_signal_id: str) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        candidate_match = _candidate_matches(row, context)
        raw_match = raw_signal_id and raw_signal_id in {str(row.get("raw_signal_id") or ""), str(row.get("intent_id") or ""), str(row.get("candidate_id") or "")}
        if candidate_match or raw_match:
            out.append(row)
    return out


def _candidate_matches(row: Mapping[str, Any], context: Mapping[str, str]) -> bool:
    ids = {str(row.get("candidate_id") or ""), str(row.get("candidate_contract_id") or ""), str(row.get("contract_id") or "")}
    wanted = {str(context.get("candidate_id") or ""), str(context.get("candidate_contract_id") or "")}
    return bool((ids - {""}) & (wanted - {""}))


def _session_matches(row: Mapping[str, Any], context: Mapping[str, str]) -> bool:
    row_session = str(row.get("paper_session_id") or "")
    wanted = str(context.get("paper_session_id") or "")
    return not row_session or not wanted or row_session == wanted


def _extract_sleeve(row: Mapping[str, Any]) -> str:
    context = row.get("candidate_context") if isinstance(row.get("candidate_context"), Mapping) else {}
    for value in (row.get("sleeve_id"), row.get("sleeve"), row.get("source_sleeve"), context.get("sleeve_id")):
        text = str(value or "").strip()
        if text and text != UNKNOWN:
            return text
    graph = str(row.get("graph_linkage") or "")
    if "." in graph:
        text = graph.rsplit(".", 1)[-1].strip()
        if text and text != UNKNOWN:
            return text
    return ""


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_payloads(root: Path, family: str, filename: str, day_utc: str) -> list[dict[str, Any]]:
    base = root / "reports" / family
    if not base.exists():
        return []
    paths = sorted(path for path in base.glob(f"*/{filename}") if path.parent.name <= day_utc)
    return [payload for payload in (_read_json(path) for path in paths) if payload]
