"""Cross-process single-writer archiver lock for the SQLite backend.

PostgreSQL gives the archiver a session-level advisory lock.  SQLite has no
counterpart, so the authority is an exclusive OS lock on
``<database>.archiver.lock`` held on an open handle for the whole run:

* The operating system drops it when the holder exits, crashes or is killed,
  so a dead archiver can never leave the lock stuck (nobody has to walk to
  the machine to clear it).
* Windows: ``msvcrt.locking`` on one byte at a high offset.  Windows range
  locks are mandatory, so locking far past the content keeps the owner JSON
  at the start of the file readable by other processes.
* POSIX: ``fcntl.flock`` on the open file.  POSIX *record* locks
  (``lockf``/``F_SETLK``) are per process and are silently dropped when any
  descriptor of the file is closed, which would let a probe in the holder's
  own process release the lock; ``flock`` belongs to the open handle.  POSIX
  locks are advisory, so the owner JSON stays readable anyway.

Everything fails closed: if the lock file cannot be opened or locked, or the
path is not provably local, acquisition is refused and a probe answers
"held".
"""
from __future__ import annotations

import errno
import json
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

from .sqlite_adapter import is_network_path, path_from_dsn

try:                                    # pragma: no cover - platform import
    import msvcrt
except ImportError:                     # pragma: no cover
    msvcrt = None
try:                                    # pragma: no cover - platform import
    import fcntl
except ImportError:                     # pragma: no cover
    fcntl = None

__all__ = [
    "LOCK_SUFFIX", "LOCK_OFFSET", "ArchiverLockError", "ArchiverLockHeld",
    "ArchiverLockLost", "ArchiverLock", "lock_path_for", "read_owner",
    "probe_held",
]

LOCK_SUFFIX = ".archiver.lock"
#: Byte locked on Windows: far beyond the owner record at the file start.
LOCK_OFFSET = 0x40000000
#: The owner record is padded to this size and rewritten in place, so the
#: file is never truncated while another process may be reading it.
OWNER_RECORD_BYTES = 1024
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 0.2

_O_BINARY = getattr(os, "O_BINARY", 0)
_CONTENTION_ERRNOS = frozenset(
    code for code in (
        errno.EACCES, errno.EAGAIN, getattr(errno, "EWOULDBLOCK", None),
        getattr(errno, "EDEADLK", None), getattr(errno, "EDEADLOCK", None))
    if code is not None)

# Locks held by THIS process: lets a same-process probe answer without
# touching the file, and refuses a second acquisition up front.
_HELD = {}
_HELD_GUARD = threading.Lock()


class ArchiverLockError(RuntimeError):
    """The archiver lock could not be taken (always fail closed)."""


class ArchiverLockHeld(ArchiverLockError):
    """Another archiver instance holds the lock."""

    def __init__(self, message, holder=None):
        super().__init__(message)
        self.holder = holder or {}


class ArchiverLockLost(ArchiverLockError):
    """This process no longer provably owns the lock."""


def lock_path_for(db_path):
    """``<db>.archiver.lock`` next to the database file."""
    return os.path.abspath(path_from_dsn(db_path)) + LOCK_SUFFIX


def _key(lock_path):
    return os.path.normcase(os.path.realpath(lock_path))


def _try_lock(fd):
    """Take the exclusive lock without blocking; OSError if unavailable."""
    if msvcrt is not None:
        os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    elif fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:                               # pragma: no cover - no such platform
        raise OSError(errno.ENOSYS, "no file locking available")


def _unlock(fd):
    if msvcrt is not None:
        os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    elif fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_UN)


def _is_contention(exc):
    return isinstance(exc, BlockingIOError) or exc.errno in _CONTENTION_ERRNOS


def _read_owner_fd(fd):
    """Owner record through an open descriptor ({} if absent/unreadable)."""
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        data = os.read(fd, OWNER_RECORD_BYTES)
        record = json.loads(data.decode("utf-8").strip() or "{}")
    except (OSError, ValueError):
        return {}
    return record if isinstance(record, dict) else {}


def _write_owner_fd(fd, record):
    data = json.dumps(record, sort_keys=True).encode("utf-8")
    if len(data) >= OWNER_RECORD_BYTES:
        raise ValueError("archiver lock owner record is too large")
    data = data + b"\n" + b" " * (OWNER_RECORD_BYTES - len(data) - 1)
    os.lseek(fd, 0, os.SEEK_SET)
    written = os.write(fd, data)
    if written != len(data):
        raise OSError(errno.EIO, "short write on the archiver lock file")
    os.fsync(fd)


def read_owner(db_path):
    """Owner record of the lock file ({} when absent, blank or unreadable).

    Informational only: a record can outlive a killed holder.  Whether the
    lock is *held* is answered by :func:`probe_held`.
    """
    try:
        fd = os.open(lock_path_for(db_path), os.O_RDONLY | _O_BINARY)
    except OSError:
        return {}
    try:
        return _read_owner_fd(fd)
    finally:
        os.close(fd)


def _describe(holder):
    if not holder or not holder.get("pid"):
        return "holder unknown"
    return (f"pid {holder.get('pid')} on {holder.get('host', '?')}, "
            f"since {holder.get('acquired_at', '?')}")


def probe_held(db_path):
    """Is any archiver running on this database?  Non-destructive.

    Tries the lock without blocking on a read-only handle and lets go at
    once; never creates, truncates or rewrites the lock file.  Any doubt
    (cannot open, cannot lock, non-local path) answers True.
    """
    lock_path = lock_path_for(db_path)
    try:
        if is_network_path(lock_path):
            return True
        with _HELD_GUARD:
            if _key(lock_path) in _HELD:
                return True
        try:
            fd = os.open(lock_path, os.O_RDONLY | _O_BINARY)
        except FileNotFoundError:
            return False
    except OSError:
        return True
    try:
        try:
            _try_lock(fd)
        except OSError:
            return True
        try:
            _unlock(fd)
        except OSError:
            pass                        # closing the handle releases it
        return False
    finally:
        os.close(fd)


class ArchiverLock:
    """Exclusive, crash-safe, cross-process lock for one database.

    ``acquire()`` raises :class:`ArchiverLockHeld` (naming the holder) when
    another instance has it, after a brief retry that rides out a concurrent
    :func:`probe_held`.  ``verify()`` is the cheap "do I still own it" check
    for chunk boundaries; it must not be called mid-write.
    """

    def __init__(self, db_path, *, retries=DEFAULT_RETRIES,
                 retry_delay=DEFAULT_RETRY_DELAY):
        self.db_path = os.path.abspath(path_from_dsn(db_path))
        self.lock_path = self.db_path + LOCK_SUFFIX
        self.retries = max(1, int(retries))
        self.retry_delay = float(retry_delay)
        self.token = None
        self._fd = None
        self._identity = None

    @property
    def held(self):
        return self._fd is not None

    def owner(self):
        return read_owner(self.db_path)

    # ------------------------------------------------------------------
    def acquire(self):
        """Take the lock and return this owner's token."""
        if self._fd is not None:
            raise ArchiverLockError(
                "[LOCK] This ArchiverLock object already holds the lock")
        try:
            non_local = is_network_path(self.lock_path)
        except OSError as exc:
            raise ArchiverLockError(
                f"[LOCK] Cannot inspect {self.lock_path}: {exc}") from exc
        if non_local:
            raise ArchiverLockError(
                "[LOCK] Refusing the archiver lock on a network or non-local "
                f"path: {self.lock_path}")
        key = _key(self.lock_path)
        with _HELD_GUARD:
            if key in _HELD:
                raise ArchiverLockHeld(
                    "[LOCK] This process already holds the tape-writer lock "
                    f"for {self.db_path}.",
                    {"pid": os.getpid(), "token": _HELD[key]})

        holder = {}
        for attempt in range(self.retries):
            if attempt:
                time.sleep(self.retry_delay)
            try:
                fd = os.open(self.lock_path,
                             os.O_RDWR | os.O_CREAT | _O_BINARY, 0o644)
            except OSError as exc:
                raise ArchiverLockError(
                    f"[LOCK] Cannot open the archiver lock file "
                    f"{self.lock_path}: {exc}. Refusing to run without the "
                    "single-writer lock.") from exc
            try:
                _try_lock(fd)
            except OSError as exc:
                holder = _read_owner_fd(fd)
                os.close(fd)
                if not _is_contention(exc):
                    raise ArchiverLockError(
                        f"[LOCK] Cannot lock {self.lock_path}: {exc}. "
                        "Refusing to run without the single-writer lock."
                    ) from exc
                continue
            token = uuid.uuid4().hex
            try:
                _write_owner_fd(fd, {
                    "token": token,
                    "pid": os.getpid(),
                    "host": socket.gethostname(),
                    "acquired_at": datetime.now(timezone.utc).isoformat(),
                    "db_path": self.db_path,
                })
                stat = os.fstat(fd)
            except (OSError, ValueError) as exc:
                self._drop(fd)
                raise ArchiverLockError(
                    f"[LOCK] Cannot record ownership in {self.lock_path}: "
                    f"{exc}") from exc
            with _HELD_GUARD:
                _HELD[key] = token
            self._fd = fd
            self._identity = (stat.st_dev, stat.st_ino)
            self.token = token
            return token

        raise ArchiverLockHeld(
            "[LOCK] Another archiver instance already holds the tape-writer "
            f"lock ({_describe(holder)}). Only one tape-write run may be "
            "active at a time; finish or close the other instance and retry.",
            holder)

    @staticmethod
    def _drop(fd):
        try:
            _unlock(fd)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    def release(self):
        """Give the lock up.  Idempotent; never raises."""
        fd, self._fd = self._fd, None
        token, self.token = self.token, None
        self._identity = None
        if fd is None:
            return
        try:
            if _read_owner_fd(fd).get("token") == token:
                _write_owner_fd(fd, {})     # do not advertise a stale owner
        except (OSError, ValueError):
            pass
        self._drop(fd)
        with _HELD_GUARD:
            key = _key(self.lock_path)
            if _HELD.get(key) == token:
                del _HELD[key]

    def verify(self, token=None):
        """Prove this object still owns the lock; raise
        :class:`ArchiverLockLost` otherwise.  Returns True."""
        def lost(why):
            return ArchiverLockLost(
                f"[LOCK] The archiver lock on {self.db_path} is no longer "
                f"provably held by this process: {why}")

        fd = self._fd
        if fd is None or self.token is None:
            raise lost("it was never acquired or was released")
        if token is not None and token != self.token:
            raise lost("the caller's owner token does not match")
        with _HELD_GUARD:
            if _HELD.get(_key(self.lock_path)) != self.token:
                raise lost("the in-process registry disagrees")
        try:
            handle_stat = os.fstat(fd)
            path_stat = os.stat(self.lock_path)
        except OSError as exc:
            raise lost(f"the lock file or handle is gone ({exc})") from exc
        handle_id = (handle_stat.st_dev, handle_stat.st_ino)
        if handle_id != self._identity:
            raise lost("the handle no longer refers to the locked file")
        if handle_stat.st_ino and path_stat.st_ino and (
                (path_stat.st_dev, path_stat.st_ino) != handle_id):
            raise lost("the lock file was replaced")
        if os.name != "nt" and handle_stat.st_nlink == 0:
            raise lost("the lock file was deleted")
        if _read_owner_fd(fd).get("token") != self.token:
            raise lost("the owner token in the lock file does not match")
        if msvcrt is None and fcntl is not None:
            try:                        # re-asserting a held flock is a no-op
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise lost(f"the OS lock is gone ({exc})") from exc
        return True

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
