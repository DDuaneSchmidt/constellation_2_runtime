from __future__ import annotations

import os
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitPolicy:
    sleep_seconds: float = 15.0
    max_retries: int = 3

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str,
        default_sleep_seconds: float = 15.0,
        default_max_retries: int = 3,
    ) -> "RateLimitPolicy":
        sleep_value = os.environ.get(f"{prefix}_RATE_LIMIT_SLEEP_SECONDS", str(default_sleep_seconds)).strip()
        retries_value = os.environ.get(f"{prefix}_MAX_RETRIES", str(default_max_retries)).strip()
        return cls(sleep_seconds=float(sleep_value), max_retries=int(retries_value))


class RateLimiter:
    def __init__(self, policy: RateLimitPolicy, *, sleeper=time.sleep) -> None:
        self.policy = policy
        self._sleeper = sleeper
        self._has_called = False

    def wait_before_call(self) -> None:
        if self._has_called and self.policy.sleep_seconds > 0:
            self._sleeper(self.policy.sleep_seconds)
        self._has_called = True
