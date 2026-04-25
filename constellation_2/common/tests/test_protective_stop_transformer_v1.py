from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseH.tools import c2_risk_transformer_offline_v1 as transformer


def test_derive_stop_price_for_buy_is_below_entry() -> None:
    stop = transformer._derive_stop_price_or_fail(  # noqa: SLF001
        entry_price=Decimal("100.00"),
        action="BUY",
        stop_loss_bps=1000,
    )
    assert stop == Decimal("90.00")


def test_missing_stop_loss_bps_is_fail_closed() -> None:
    with pytest.raises(transformer.TransformerError, match="INTENT_PROTECTIVE_STOP_MISSING"):
        transformer._require_stop_loss_bps_from_exposure_or_fail(  # noqa: SLF001
            {"constraints": {"max_risk_pct": "0.01"}}
        )
