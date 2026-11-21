"""
The fetch must never stop silently again (2026-11-17: a stale lock file made the only worker secondary; no fetch for
four days while /health said healthy).
"""
import os

from modules.fetch_liveness import FetchLiveness
from modules.primary_lock import PrimaryLock


def test_stale_lock_file_from_dead_process_does_not_block(tmp_path):
    path = tmp_path / "primary.lock"
    path.write_text("7\n")  # exactly what the server had: a file from a process long gone
    assert PrimaryLock(str(path)).acquire() is True
    assert path.read_text() == f"{os.getpid()}\n"


def test_second_holder_is_secondary_while_first_lives(tmp_path):
    path = str(tmp_path / "primary.lock")
    first = PrimaryLock(path)
    assert first.acquire() is True
    assert PrimaryLock(path).acquire() is False  # a separate open file: flock refuses it while `first` holds it


def test_lock_frees_when_holder_goes(tmp_path):
    path = str(tmp_path / "primary.lock")
    first = PrimaryLock(path)
    assert first.acquire() is True
    os.close(first._fd)  # what process exit does
    assert PrimaryLock(path).acquire() is True


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_no_primary_is_unhealthy():
    live = FetchLiveness(clock=Clock())
    live.mark()
    assert "no primary" in live.problem(is_primary=False)


def test_worker_never_started_is_unhealthy():
    assert FetchLiveness(clock=Clock()).problem(is_primary=True) is not None


def test_silent_worker_is_unhealthy_and_a_pass_heals_it():
    clock = Clock()
    live = FetchLiveness(max_silence_s=7200, clock=clock)
    live.mark()
    clock.t += 3700  # one normal hourly sleep
    assert live.problem(is_primary=True) is None
    clock.t += 3600  # 2 h without a pass
    assert "silent" in live.problem(is_primary=True)
    live.mark()
    assert live.problem(is_primary=True) is None
