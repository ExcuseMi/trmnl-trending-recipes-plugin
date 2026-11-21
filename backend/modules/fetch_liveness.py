"""
Is the hourly recipe fetch still running? /health answers 503 when it is not, and the compose healthcheck then
restarts the container.

It watches the worker loop, not the data: the loop marks itself on every pass, also when a fetch fails. A trmnl.com
outage therefore keeps the service healthy (no restart loop that hammers trmnl.com), while a process that never
started the worker (no primary) or a worker thread that died or hung goes unhealthy.
"""
import time
from typing import Callable, Optional

# The loop sleeps at most one hour between passes (plus a fetch of under a minute); two hours means it is gone.
DEFAULT_MAX_SILENCE_S = 2 * 3600


class FetchLiveness:
    def __init__(self, max_silence_s: float = DEFAULT_MAX_SILENCE_S, clock: Callable[[], float] = time.monotonic):
        self.max_silence_s = max_silence_s
        self._clock = clock
        self._last: Optional[float] = None

    def mark(self) -> None:
        self._last = self._clock()

    def problem(self, is_primary: bool) -> Optional[str]:
        """None when healthy, otherwise why not."""
        if not is_primary:
            return "no primary worker: the recipe fetch is not running in this process"
        if self._last is None:
            return "recipe fetch worker never started"
        silent = self._clock() - self._last
        if silent > self.max_silence_s:
            return f"recipe fetch worker silent for {silent / 3600:.1f} h"
        return None
