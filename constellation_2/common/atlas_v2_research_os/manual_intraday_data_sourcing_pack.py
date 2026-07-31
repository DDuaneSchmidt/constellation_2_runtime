from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "manual_intraday_data_sourcing_pack"
MANUAL_DROP_DIR = "data/manual_intraday_import"
REQUIRED_ROWS = [
    {
        "symbol": "DIA",
        "required_timeframe": "30m",
        "accepted_alternative": "DIA_1m.csv",
        "target_drop_path": "data/manual_intraday_import/DIA_30m.csv",
        "normalized_output_path": "data/cache/DIA_30m.csv",
        "required_for_candidate_id": "ptc_backtest_final_469607b8340421b7;ptc_backtest_final_854ad10b904e1ae9",
        "candidate_rank": "1;4",
        "notes": "Historical OHLCV only. 1m source may be used instead and resampled by Atlas.",
    },
    {
        "symbol": "DIA",
        "required_timeframe": "5m",
        "accepted_alternative": "DIA_1m.csv",
        "target_drop_path": "data/manual_intraday_import/DIA_5m.csv",
        "normalized_output_path": "data/cache/DIA_5m.csv",
        "required_for_candidate_id": "ptc_backtest_final_3a4ac24107c77136",
        "candidate_rank": "2",
        "notes": "Historical OHLCV only. 1m source may be used instead and resampled by Atlas.",
    },
    {
        "symbol": "QQQ",
        "required_timeframe": "30m",
        "accepted_alternative": "QQQ_1m.csv",
        "target_drop_path": "data/manual_intraday_import/QQQ_30m.csv",
        "normalized_output_path": "data/cache/QQQ_30m.csv",
        "required_for_candidate_id": "ptc_backtest_final_469607b8340421b7;ptc_backtest_final_854ad10b904e1ae9",
        "candidate_rank": "1;4",
        "notes": "Historical OHLCV only. 1m source may be used instead and resampled by Atlas.",
    },
    {
        "symbol": "QQQ",
        "required_timeframe": "5m",
        "accepted_alternative": "QQQ_1m.csv",
        "target_drop_path": "data/manual_intraday_import/QQQ_5m.csv",
        "normalized_output_path": "data/cache/QQQ_5m.csv",
        "required_for_candidate_id": "ptc_backtest_final_3a4ac24107c77136",
        "candidate_rank": "2",
        "notes": "Historical OHLCV only. 1m source may be used instead and resampled by Atlas.",
    },
    {
        "symbol": "SPY",
        "required_timeframe": "30m",
        "accepted_alternative": "SPY_1m.csv",
        "target_drop_path": "data/manual_intraday_import/SPY_30m.csv",
        "normalized_output_path": "data/cache/SPY_30m.csv",
        "required_for_candidate_id": "ptc_backtest_final_469607b8340421b7",
        "candidate_rank": "1",
        "notes": "Historical OHLCV only. 1m source may be used instead and resampled by Atlas.",
    },
    {
        "symbol": "SPY",
        "required_timeframe": "5m",
        "accepted_alternative": "SPY_1m.csv",
        "target_drop_path": "data/manual_intraday_import/SPY_5m.csv",
        "normalized_output_path": "data/cache/SPY_5m.csv",
        "required_for_candidate_id": "ptc_backtest_final_3a4ac24107c77136",
        "candidate_rank": "2",
        "notes": "Historical OHLCV only. 1m source may be used instead and resampled by Atlas.",
    },
]
AUTHORITY_BOUNDARY = {
    "manual_sourcing_support_only": True,
    "historical_data_only": True,
    "external_api_calls_authorized": False,
    "vendor_purchase_automation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "broker_endpoint_allowed": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def build_manual_intraday_data_sourcing_pack(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    return {
        "schema_id": "atlas_v2_research_os_manual_intraday_data_sourcing_pack",
        "schema_version": "1.0",
        "report_type": "MANUAL_INTRADAY_DATA_SOURCING_PACK",
        "created_at": created,
        "day": created[:10],
        "manual_drop_dir": MANUAL_DROP_DIR,
        "summary": {
            "required_files": len(REQUIRED_ROWS),
            "required_symbols": ["DIA", "QQQ", "SPY"],
            "required_timeframes": ["5m", "30m"],
            "accepted_1m_alternatives": ["DIA_1m.csv", "QQQ_1m.csv", "SPY_1m.csv"],
            "blocked_candidates": ["ptc_backtest_final_469607b8340421b7", "ptc_backtest_final_3a4ac24107c77136"],
            "status": "MANUAL_DATA_REQUIRED",
        },
        "priority1_required_files": list(REQUIRED_ROWS),
        "human_summary": [
            "Buy/download either DIA_1m.csv, QQQ_1m.csv, SPY_1m.csv; Atlas will resample 1m into 5m and 30m.",
            "Or buy/download DIA_5m.csv, DIA_30m.csv, QQQ_5m.csv, QQQ_30m.csv, SPY_5m.csv, SPY_30m.csv.",
            "Place the CSV files into data/manual_intraday_import/.",
            "Then run the post-drop validation commands.",
        ],
        "csv_schema": {
            "required_columns": ["timestamp", "open", "high", "low", "close", "volume"],
            "example_row": "2024-01-02T09:30:00-05:00,0,0,0,0,0",
        },
        "post_drop_validation_commands": post_drop_validation_commands(),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Manual sourcing support only.",
            "No external API calls.",
            "No vendor checkout or download automation.",
            "No broker access.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def write_manual_intraday_data_sourcing_pack(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Path]:
    report = build_manual_intraday_data_sourcing_pack(root=root, created_at=created_at)
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out_dir / "manual_intraday_data_sourcing_pack.json",
        "summary": out_dir / "manual_intraday_data_sourcing_pack_summary.md",
        "priority1_required_files": out_dir / "priority1_required_files.csv",
        "vendor_request_checklist": out_dir / "vendor_request_checklist.md",
        "csv_schema_template": out_dir / "csv_schema_template.csv",
        "post_drop_validation_commands": out_dir / "post_drop_validation_commands.md",
        "latest_json": root_path / "latest.json",
        "latest_summary": root_path / "latest_summary.md",
    }
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_manual_intraday_data_sourcing_pack_summary(report)
    paths["json"].write_text(payload, encoding="utf-8")
    paths["latest_json"].write_text(payload, encoding="utf-8")
    paths["summary"].write_text(summary, encoding="utf-8")
    paths["latest_summary"].write_text(summary, encoding="utf-8")
    _write_priority_csv(paths["priority1_required_files"], report["priority1_required_files"])
    paths["vendor_request_checklist"].write_text(vendor_request_checklist(), encoding="utf-8")
    paths["csv_schema_template"].write_text("timestamp,open,high,low,close,volume\n2024-01-02T09:30:00-05:00,0,0,0,0,0\n", encoding="utf-8")
    paths["post_drop_validation_commands"].write_text(post_drop_validation_commands_markdown(), encoding="utf-8")
    (Path.cwd() / MANUAL_DROP_DIR).mkdir(parents=True, exist_ok=True)
    return paths


def render_manual_intraday_data_sourcing_pack_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Manual Intraday Data Sourcing Pack",
        "",
        "David should manually acquire historical intraday OHLCV CSV files for DIA, QQQ, and SPY.",
        "",
        "Buy/download either:",
        "- `DIA_1m.csv`, `QQQ_1m.csv`, `SPY_1m.csv`",
        "",
        "or:",
        "- `DIA_5m.csv`, `DIA_30m.csv`, `QQQ_5m.csv`, `QQQ_30m.csv`, `SPY_5m.csv`, `SPY_30m.csv`",
        "",
        "Place them into:",
        "- `data/manual_intraday_import/`",
        "",
        "Then run the validation commands in `post_drop_validation_commands.md`.",
        "",
        "## Required Files",
    ]
    for row in report.get("priority1_required_files", []):
        lines.append(f"- `{row['target_drop_path']}` -> `{row['normalized_output_path']}` for candidate rank {row['candidate_rank']}")
    lines.extend(["", "Authority: manual data sourcing support only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def vendor_request_checklist() -> str:
    return """# Vendor Request Checklist

Need historical OHLCV intraday data.

- Symbols: DIA, QQQ, SPY.
- Timeframes: 5-minute and 30-minute.
- 1-minute is acceptable if 5m/30m are not directly available.
- Regular market hours preferred; extended hours must be labeled.
- Timezone must be specified.
- CSV format required.
- Required columns: timestamp, open, high, low, close, volume.
- Date range: use maximum available history, minimum several years if possible.
- No trade execution data needed.
- No account/broker connection needed.
- No live or real-time data feed needed.

Do not purchase or request broker, account, order, position, capital, portfolio, or execution data for this workflow.
"""


def post_drop_validation_commands() -> list[str]:
    return [
        "python3 -m constellation_2.common.atlas_v2_research_os.cli --manual-intraday-csv-intake",
        "python3 -m constellation_2.common.atlas_v2_research_os.cli --validate-local-market-data",
        "python3 -m constellation_2.common.atlas_v2_research_os.cli --market-data-import-report",
        "python3 -m constellation_2.common.atlas_v2_research_os.cli --market-data-coverage-report",
        "python3 -m constellation_2.common.atlas_v2_research_os.cli --direct-candidate-data-validation",
    ]


def post_drop_validation_commands_markdown() -> str:
    commands = "\n".join(post_drop_validation_commands())
    return f"""# Post-Drop Validation Commands

Run after placing CSV files into `data/manual_intraday_import/`.

```bash
{commands}
```
"""


def _write_priority_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = ["symbol", "required_timeframe", "accepted_alternative", "target_drop_path", "normalized_output_path", "required_for_candidate_id", "candidate_rank", "notes"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
