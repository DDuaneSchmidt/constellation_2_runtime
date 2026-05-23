from __future__ import annotations


MINIMUM_VIABLE_SYMBOLS = ["SPY", "QQQ", "IWM", "TLT", "GLD"]
MINIMUM_REQUIRED_COUNT = 3

SYMBOL_SOURCE_HINTS = {
    "SPY": {"expected_file": "SPY.csv", "stooq_symbol_hint": "spy.us", "yahoo_symbol_hint": "SPY"},
    "QQQ": {"expected_file": "QQQ.csv", "stooq_symbol_hint": "qqq.us", "yahoo_symbol_hint": "QQQ"},
    "IWM": {"expected_file": "IWM.csv", "stooq_symbol_hint": "iwm.us", "yahoo_symbol_hint": "IWM"},
    "TLT": {"expected_file": "TLT.csv", "stooq_symbol_hint": "tlt.us", "yahoo_symbol_hint": "TLT"},
    "GLD": {"expected_file": "GLD.csv", "stooq_symbol_hint": "gld.us", "yahoo_symbol_hint": "GLD"},
}

MANUAL_SOURCE_GUIDANCE = {
    "stooq": [
        "Stooq provides historical data pages and CSV download links, but may require CAPTCHA/API-key access depending on route/environment.",
        "Stooq has a historical data download interface and bulk historical market data pages.",
        "Research Lab does not bypass CAPTCHA, API-key, or site access limits.",
    ],
    "yahoo": [
        "Yahoo Finance data should be downloaded manually from the symbol's Historical Data page when available to the operator.",
        "Save each downloaded file using the expected SYMBOL.csv filename.",
        "Research Lab does not implement automated Yahoo scraping.",
    ],
}

