"""
ingestion/rate_limiter.py

Token bucket rate limiter for the ingestion service.

The token bucket algorithm works as follows:
  - Each vehicle has a bucket with a maximum capacity of N tokens.
  - Tokens are added at a fixed rate (refill_rate tokens per second).
  - Each incoming event consumes one token.
  - If the bucket is empty, the request is rejected with HTTP 429.

This is a standard production pattern used by API gateways (AWS API Gateway,
Kong, Nginx) to protect services from misbehaving clients. Here we implement
it in Python so the concept is clear, even though in production it would sit
in the gateway layer before traffic reaches the application.

Reference: Tanenbaum & Wetherall, "Computer Networks" (5th ed.) — leaky bucket
and token bucket are covered as traffic shaping algorithms in Chapter 6.
"""

import time
import threading
import logging
from config.settings import settings

logger = logging.getLogger("voltfleet.rate_limiter")


class TokenBucket:
    """
    A single token bucket for one vehicle.

    Thread-safe: uses a lock because the ingestion service handles concurrent
    requests from multiple vehicles simultaneously.
    """

    def __init__(self, capacity: int, refill_rate: float):
        """
        Args:
            capacity:    Maximum number of tokens the bucket can hold.
                         Also the initial fill level (full on startup).
            refill_rate: Tokens added per second (e.g. 0.25 = 1 every 4 seconds).
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = float(capacity)        # Start full
        self.last_refill_time = time.monotonic()
        self._lock = threading.Lock()

    def consume(self) -> bool:
        """
        Attempt to consume one token.

        Returns:
            True  — token consumed, request is allowed.
            False — bucket empty, request should be rate-limited (HTTP 429).
        """
        with self._lock:
            self._refill()

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True   # Request allowed

            return False      # Rate limited

    def _refill(self) -> None:
        """
        Add tokens based on elapsed time since the last refill.

        This is called on every consume() call rather than on a background
        timer — a simpler approach known as "lazy refill" that avoids
        needing a background thread per vehicle.
        """
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        new_tokens = elapsed * self.refill_rate

        # Add new tokens but do not exceed bucket capacity
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill_time = now


class RateLimiter:
    """
    Fleet-level rate limiter.

    Maintains one TokenBucket per vehicle_id. Buckets are created on first
    contact and never removed (in a long-running production service, you would
    expire buckets for vehicles that have not been seen in a while to reclaim
    memory — this is left as a known limitation for v1.0).
    """

    def __init__(self):
        # Dictionary mapping vehicle_id -> TokenBucket
        self._buckets: dict[str, TokenBucket] = {}
        # Lock for the dictionary itself (adding new buckets)
        self._dict_lock = threading.Lock()

    def is_allowed(self, vehicle_id: str) -> bool:
        """
        Check whether a request from vehicle_id is within the rate limit.

        Args:
            vehicle_id: The vehicle making the request.

        Returns:
            True if the request is allowed, False if it should be rejected.
        """
        bucket = self._get_or_create_bucket(vehicle_id)
        allowed = bucket.consume()

        if not allowed:
            logger.warning(
                f"Rate limit exceeded | vehicle_id={vehicle_id}"
            )

        return allowed

    def _get_or_create_bucket(self, vehicle_id: str) -> TokenBucket:
        """
        Return the existing bucket for this vehicle, or create a new one.

        The double-checked locking pattern here avoids acquiring the dict lock
        on every request (the common case is that the bucket already exists).
        """
        if vehicle_id not in self._buckets:
            with self._dict_lock:
                # Check again inside the lock — another thread may have added
                # this vehicle between our first check and acquiring the lock.
                if vehicle_id not in self._buckets:
                    self._buckets[vehicle_id] = TokenBucket(
                        capacity=settings.rate_limit_bucket_capacity,
                        refill_rate=settings.rate_limit_events_per_second,
                    )
                    logger.debug(f"Created rate limiter bucket for vehicle_id={vehicle_id}")

        return self._buckets[vehicle_id]


# Module-level singleton — created once when the ingestion app starts
rate_limiter = RateLimiter()
