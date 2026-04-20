import time

class RateLimiter:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = min_interval_sec
        self._last = 0.0

    def ok(self) -> bool:
        now = time.time()
        if now - self._last >= self.min_interval_sec:
            self._last = now
            return True
        return False
