from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseJ.tools import ib_historical_market_data_snapshot_downloader_v1 as downloader  # noqa: E402


def _write_registry(repo_root: Path) -> None:
    path = repo_root / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "engines": [
                    {
                        "engine_id": "C2_INTENT_SIMULATOR_V1",
                        "activation_status": "ACTIVE",
                        "allowed_symbols": ["IWM"],
                    },
                    {
                        "engine_id": "C2_ACTIVE_ALPHA_A",
                        "activation_status": "ACTIVE",
                        "allowed_symbols": ["SPY"],
                    },
                    {
                        "engine_id": "C2_ACTIVE_ALPHA_B",
                        "activation_status": "ACTIVE",
                        "allowed_symbols": ["TLT", "SPY"],
                    },
                    {
                        "engine_id": "C2_RETIRED_ALPHA",
                        "activation_status": "INACTIVE",
                        "allowed_symbols": ["QQQ", "GLD"],
                    },
                ]
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_registry_symbols_replace_deprecated_default_when_policy_exists(tmp_path: Path) -> None:
    _write_registry(tmp_path)

    symbols, diagnostics = downloader._resolve_requested_symbols(tmp_path, [], "")

    assert symbols == ["SPY", "TLT"]
    assert diagnostics["symbol_source"] == downloader.REGISTRY_SYMBOL_SOURCE
    assert diagnostics["symbols_requested"] == ["SPY", "TLT"]
    assert diagnostics["symbols_authoritative"] == ["SPY", "TLT"]
    assert diagnostics["deprecated_symbol_source_detected"] is False


def test_cli_symbols_are_honored_as_operator_override_not_canonical(tmp_path: Path) -> None:
    _write_registry(tmp_path)

    symbols, diagnostics = downloader._resolve_requested_symbols(tmp_path, ["QQQ"], "IWM,GLD,TLT")

    assert symbols == ["GLD", "IWM", "QQQ", "TLT"]
    assert diagnostics["symbol_source"] == downloader.OPERATOR_SYMBOL_SOURCE
    assert diagnostics["symbols_requested"] == ["GLD", "IWM", "QQQ", "TLT"]
    assert diagnostics["symbols_authoritative"] == ["SPY", "TLT"]
    assert diagnostics["deprecated_symbol_source_detected"] is True
