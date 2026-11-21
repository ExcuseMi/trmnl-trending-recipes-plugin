"""
Primary worker election: exactly one process runs the background jobs.

The lock is a kernel flock on an open file, so it ends with the process that holds it. A lock file left behind by a
dead process (a reboot, a crash, a container restart that keeps /tmp) is just an unlocked file and never blocks a
new worker. The old O_EXCL "file exists" lock did: after the 2026-11-17 reboot the only worker saw a stale file from
October, made itself secondary and nothing fetched recipes for four days.
"""
import fcntl
import os


class PrimaryLock:
    def __init__(self, path: str):
        self.path = path
        self._fd = None  # held open for the life of the process; closing it would release the lock

    def acquire(self) -> bool:
        """True if this process is now the primary worker, False if another live process holds the lock."""
        if self._fd is not None:
            return True
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())  # for people reading the file; the lock does not depend on it
        self._fd = fd
        return True
