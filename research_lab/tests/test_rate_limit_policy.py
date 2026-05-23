from __future__ import annotations

from research_lab.providers.rate_limit import RateLimiter, RateLimitPolicy


def test_rate_limit_policy_reads_env_defaults(monkeypatch) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_RATE_LIMIT_SLEEP_SECONDS", raising=False)
    monkeypatch.delenv("ALPHA_VANTAGE_MAX_RETRIES", raising=False)

    policy = RateLimitPolicy.from_env(prefix="ALPHA_VANTAGE")

    assert policy.sleep_seconds == 15
    assert policy.max_retries == 3


def test_rate_limiter_sleeps_after_first_call() -> None:
    sleeps: list[float] = []
    limiter = RateLimiter(RateLimitPolicy(sleep_seconds=2, max_retries=1), sleeper=sleeps.append)

    limiter.wait_before_call()
    limiter.wait_before_call()

    assert sleeps == [2]

