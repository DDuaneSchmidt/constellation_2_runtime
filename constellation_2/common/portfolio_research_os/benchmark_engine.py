from __future__ import annotations

BENCHMARK_STACK = [
    {
        "benchmark_id": "VTI",
        "benchmark_name": "Vanguard Total Stock Market ETF",
        "benchmark_type": "market_beta",
        "purpose": "broad US equity baseline",
    },
    {
        "benchmark_id": "SIXTY_FORTY",
        "benchmark_name": "60/40 stock-bond portfolio",
        "benchmark_type": "allocation_baseline",
        "purpose": "simple diversified allocation baseline",
    },
    {
        "benchmark_id": "SIMPLE_FACTOR_PORTFOLIO",
        "benchmark_name": "Simple fixed-weight factor portfolio",
        "benchmark_type": "factor_baseline",
        "purpose": "tests whether Portfolio Atlas adds value beyond simple factors",
    },
    {
        "benchmark_id": "OAK_HARVEST_PROXY",
        "benchmark_name": "Oak Harvest proxy",
        "benchmark_type": "external_style_proxy",
        "purpose": "compares against a transparent proxy for the referenced style",
    },
]


def benchmark_stack() -> list[dict[str, str]]:
    return [dict(row) for row in BENCHMARK_STACK]

