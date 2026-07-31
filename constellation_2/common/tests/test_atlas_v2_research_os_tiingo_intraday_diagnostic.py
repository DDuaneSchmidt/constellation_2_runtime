from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import tiingo_intraday_diagnostic as diag


class _FakeResponse:
    status = 200

    def __init__(self, body: str) -> None:
        self.body = body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self.body

    def getcode(self) -> int:
        return self.status


def test_current_tiingo_request_shape_is_sanitized() -> None:
    shape = diag.build_current_tiingo_request_shape()

    dumped = json.dumps(shape)
    assert shape["endpoint_path"] == "/iex/{ticker}/prices"
    assert shape["auth_method"] == "query param token=<redacted>"
    assert shape["params"]["token"] == "[REDACTED]"
    assert "TIINGO_API_KEY" not in dumped


def test_classify_tiingo_403_entitlement_failure() -> None:
    attempts = [{"http_status": 403, "response_body_first_500_chars": "Forbidden: IEX intraday subscription required"}]

    assert diag.classify_tiingo_failure(attempts) == "ENTITLEMENT_FAILURE"


def test_diagnostic_success_writes_only_spy_sample_and_report(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(diag, "_provider_keys", lambda: {"TIINGO_API_KEY": "unit-test-token"})

    def fake_urlopen(request, timeout=20):  # noqa: ANN001, ARG001
        url = request.full_url
        if "/iex/spy/prices" in url and "resampleFreq=5min" in url:
            return _FakeResponse("date,open,high,low,close,volume\n2026-06-01T09:30:00Z,1,2,0.5,1.5,100\n")
        return _FakeResponse("message,detail\nempty,not ohlcv\n")

    monkeypatch.setattr(diag.urllib.request, "urlopen", fake_urlopen)

    report = diag.run_tiingo_intraday_diagnostic(root=tmp_path / "reports", created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["classification"] == "SUCCESS"
    assert report["summary"]["diagnostic_csv_written"] == "data/cache/tiingo_intraday_diagnostic_SPY_5m.csv"
    assert (tmp_path / "data" / "cache" / "tiingo_intraday_diagnostic_SPY_5m.csv").exists()
    assert not (tmp_path / "data" / "cache" / "SPY_5m.csv").exists()
    dumped = (tmp_path / "reports" / "tiingo_intraday_diagnostic" / "latest.json").read_text(encoding="utf-8")
    assert "unit-test-token" not in dumped
