"""Exponential backoff timer for reconnection attempts."""
from __future__ import annotations
import random
import time


class BackoffTimer:
    """Manages exponential backoff timing for reconnect attempts."""
    
    def __init__(self, initial_sec: float = 1.0, max_sec: float = 30.0, jitter_ratio: float = 0.15):
        self.initial_sec = initial_sec
        self.max_sec = max_sec
        self.jitter_ratio = max(0.0, min(0.75, float(jitter_ratio or 0.0)))
        self.current_sec = initial_sec
        self.next_attempt_time = 0.0
        self.attempt_count = 0
    
    def reset(self):
        """Reset backoff to initial delay."""
        self.current_sec = self.initial_sec
        self.next_attempt_time = 0.0
        self.attempt_count = 0
    
    def schedule_next(self):
        """Schedule next attempt with current delay, then increase delay."""
        delay = self.current_sec
        if self.jitter_ratio > 0:
            delay = delay * random.uniform(1.0 - self.jitter_ratio, 1.0 + self.jitter_ratio)
        self.next_attempt_time = time.time() + max(0.0, delay)
        self.attempt_count += 1
        # Exponential backoff: 1s -> 2s -> 4s -> 8s -> 16s -> 30s (capped)
        self.current_sec = min(self.current_sec * 2, self.max_sec)
    
    def can_attempt_now(self) -> bool:
        """Check if enough time has passed to attempt reconnection."""
        return time.time() >= self.next_attempt_time
    
    def seconds_until_next(self) -> float:
        """Get seconds remaining until next attempt (0 if ready now)."""
        remaining = self.next_attempt_time - time.time()
        return max(0.0, remaining)
