"""Authoritative cross-process ownership of the LTFS drive.

``runtime._TAPE_IO_LOCK`` is a ``threading.RLock``: it serialises threads inside
one interpreter and nothing else. Every other process on this host — the
watchdog, ``post_remount_check.py``, a second ``run.py``, an operator running a
maintenance command — could touch the tape while the archiver was mid-write, and
the audit found the watchdog doing exactly that whenever the archiver was down.

This module supplies the real boundary: a **Windows named mutex**, which the
kernel owns, which survives a crashed holder (reported as *abandoned* to the
next waiter), and which every process referring to the same configured drive
resolves to the same name.

Design notes
------------
* The mutex name is derived from stable machine-level configuration — the
  configured ``[HARDWARE] lto_drive`` letter — never from a path, PID or
  temporary value, so unrelated processes agree on it without coordination.
* ``Global\\`` is preferred so ownership spans logon sessions (the watchdog runs
  from Task Scheduler, potentially in a different session). Creating a Global
  object needs a privilege standard users may lack; if that is refused we fall
  back to ``Local\\`` and log the reduced scope **loudly**. We never fall back
  to the in-process lock.
* Windows mutex ownership is per *thread*. The in-process ``RLock`` is retained
  as the thread serialiser, so exactly one thread per process is ever inside the
  critical section, and the same thread that acquires the mutex releases it.
* No lock file, and nothing is ever written to the tape to represent ownership.

Abandoned-mutex detection has one non-obvious requirement, verified
experimentally on this host: Windows destroys a named mutex when its **last
handle** closes. If the only holder dies, the object disappears and the next
``CreateMutexW`` makes a *fresh* one, so the crash is reported as a normal
acquisition (``WAIT_OBJECT_0``), not ``WAIT_ABANDONED``. Detection therefore
only works while some other process already holds a handle. Each
:class:`LtfsOwnership` opens its handle eagerly in ``__init__`` and keeps it for
the process lifetime precisely so a peer's crash is observable.
"""
import atexit
import configparser
import ctypes
import os
import re
import sys
import threading
import time

from .constants import PROJECT_ROOT
from .logsetup import get_logger

_IS_WINDOWS = os.name == "nt"

# Win32 wait results.
_WAIT_OBJECT_0 = 0x00000000
_WAIT_ABANDONED = 0x00000080
_WAIT_TIMEOUT = 0x00000102
_WAIT_FAILED = 0xFFFFFFFF
_INFINITE = 0xFFFFFFFF
_ERROR_ACCESS_DENIED = 5

DEFAULT_WRITER_TIMEOUT_SECONDS = 300.0
DEFAULT_HELPER_TIMEOUT_SECONDS = 30.0
# Bounds: a timeout must be positive, and must never be long enough to look
# like an indefinite wait to an operator watching a stalled run.
MIN_TIMEOUT_SECONDS = 1.0
MAX_TIMEOUT_SECONDS = 3600.0

_OWNERSHIP_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

# Failure kinds, so callers can distinguish causes without string matching.
FAILURE_TIMEOUT = "ownership_timeout"
FAILURE_CROSS_SESSION_UNAVAILABLE = "cross_session_mutex_unavailable"
FAILURE_PRIMITIVE = "ownership_primitive_failure"
FAILURE_BAD_IDENTITY = "ownership_identity_invalid"


class LtfsOwnershipError(RuntimeError):
    """Raised when authoritative LTFS ownership cannot be acquired.

    ``classification`` is the stable stop reason; ``kind`` distinguishes the
    cause (timeout / cross-session unavailable / primitive failure / bad
    identity) so callers can branch without parsing messages.
    """

    classification = "ltfs_ownership_unavailable"

    def __init__(self, message, operation=None, waited_seconds=None,
                 timeout=None, kind=FAILURE_PRIMITIVE):
        super().__init__(message)
        self.operation = operation
        self.waited_seconds = waited_seconds
        self.timeout = timeout
        self.kind = kind


def _read_hardware_option(key):
    parser = configparser.ConfigParser()
    try:
        parser.read(os.path.join(PROJECT_ROOT, "config.ini"), encoding="utf-8")
        return (parser.get("HARDWARE", key, fallback="") or "").strip()
    except (configparser.Error, OSError, UnicodeDecodeError):
        return ""


def configured_ownership_id():
    """The stable physical-drive identity used to name the mutex.

    Deliberately NOT the drive letter: the LTFS mount letter has moved (E: ->
    Z:) and two processes reading different config revisions would otherwise
    build *different* mutex names for the *same* physical drive — the exact
    failure this lock exists to prevent. Requires an explicit
    ``[HARDWARE] ltfs_ownership_id``; no device is queried to compute it.
    """
    return _read_hardware_option("ltfs_ownership_id")


def validate_ownership_id(value):
    """Return a validated id, or raise LtfsOwnershipError (fail closed)."""
    value = (value or "").strip()
    if not value:
        raise LtfsOwnershipError(
            "[HARDWARE] ltfs_ownership_id is not set. It is required: the "
            "mutex name must identify the PHYSICAL drive, and the mount letter "
            "is not stable. Set it (e.g. drive_1097008774) in config.ini.",
            kind=FAILURE_BAD_IDENTITY)
    if not _OWNERSHIP_ID_RE.match(value):
        raise LtfsOwnershipError(
            f"[HARDWARE] ltfs_ownership_id={value!r} is malformed. Use 1-64 "
            f"characters from A-Z a-z 0-9 . _ - (no path separators, no "
            f"namespace characters).", kind=FAILURE_BAD_IDENTITY)
    return value


def default_mutex_name():
    """Mutex name for the configured physical drive. Fails closed if unset."""
    return f"OWC_LTO8_LTFS_OWNERSHIP_{validate_ownership_id(configured_ownership_id())}"


def _bounded_timeout(raw, default):
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return default
    return max(MIN_TIMEOUT_SECONDS, min(MAX_TIMEOUT_SECONDS, value))


def writer_timeout_seconds():
    return _bounded_timeout(
        _read_hardware_option("ltfs_writer_lock_timeout_seconds"),
        DEFAULT_WRITER_TIMEOUT_SECONDS)


def helper_timeout_seconds():
    return _bounded_timeout(
        _read_hardware_option("ltfs_helper_lock_timeout_seconds"),
        DEFAULT_HELPER_TIMEOUT_SECONDS)


# Registered by src.ltfs at import time; keeps this module free of a circular
# import while still letting ownership changes invalidate the readiness cache.
_ownership_change_callback = None


def set_ownership_change_callback(callback):
    """Register the invalidation hook called when ownership changes hands."""
    global _ownership_change_callback
    previous = _ownership_change_callback
    _ownership_change_callback = callback
    return previous


def _notify_ownership_change(detail):
    if _ownership_change_callback is None:
        return
    try:
        _ownership_change_callback(detail)
    except Exception:            # never let invalidation break the tape path
        get_logger().exception("ltfs_ownership_callback_failed")


class LtfsOwnership:
    """Cross-process ownership of the configured LTFS drive."""

    def __init__(self, name=None, timeout=None, _allow_local_scope=False):
        """``_allow_local_scope`` is TEST-ONLY.

        Its leading underscore, its absence from every production call site and
        the loud warning it emits are all deliberate: a ``Local\\`` mutex is
        per-logon-session, so the Task Scheduler watchdog and an interactive
        helper would get *different* objects and both believe they own the
        drive. Production must always be ``Global\\`` or fail closed.
        """
        # Name resolution is deferred: an unset/malformed ltfs_ownership_id must
        # fail closed at first USE, not at import time (which would take the
        # whole package down, including tooling that never touches tape).
        self._name_override = name
        self._resolved_name = None
        self.timeout = (writer_timeout_seconds() if timeout is None
                        else _bounded_timeout(timeout, DEFAULT_WRITER_TIMEOUT_SECONDS))
        self._allow_local_scope = bool(_allow_local_scope)
        if self._allow_local_scope:
            get_logger().warning(
                "ltfs_ownership_local_scope_enabled: TEST-ONLY local scope is "
                "active; this is NOT safe for production tape access.")
        self._local = threading.RLock()   # thread serialiser within this process
        self._depth = 0
        self._handle = None
        self._scope = None                # "Global" | "Local"
        self._generation = 0
        self._owner_thread = None
        self._last_abandoned = False
        atexit.register(self._atexit_release)
        # Open the handle now, not on first acquire: keeping a handle open for
        # the process lifetime is what makes a peer's crash observable as
        # WAIT_ABANDONED (see the module docstring). Failure here is not fatal —
        # acquire() will retry and raise with a precise message if it still
        # cannot create the object.
        if _IS_WINDOWS:
            try:
                self._ensure_handle()
            except Exception:
                get_logger().debug(
                    "ltfs_ownership_handle_deferred: mutex=%s", self.name)

    # -- naming ------------------------------------------------------------
    @property
    def name(self):
        """Resolved mutex name; raises (fail closed) on a bad identity."""
        if self._resolved_name is None:
            self._resolved_name = (self._name_override
                                   or default_mutex_name())
        return self._resolved_name

    def _qualified_names(self):
        """Production is Global-only. Local is offered solely under the
        explicit test-only opt-in, never as a silent fallback."""
        if self._allow_local_scope:
            return (f"Local\\{self.name}",)
        return (f"Global\\{self.name}",)

    def _ensure_handle(self):
        if self._handle is not None:
            return self._handle
        if not _IS_WINDOWS:
            raise LtfsOwnershipError(
                "cross-process LTFS ownership requires Windows named mutexes; "
                "refusing to run without an authoritative lock",
                kind=FAILURE_PRIMITIVE)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.CreateMutexW.argtypes = (ctypes.c_void_p, ctypes.c_bool,
                                          ctypes.c_wchar_p)
        last_err = None
        for qualified in self._qualified_names():
            handle = kernel32.CreateMutexW(None, False, qualified)
            if handle:
                self._handle = handle
                self._scope = qualified.split("\\", 1)[0]
                return self._handle
            last_err = ctypes.get_last_error()

        # Global refused: FAIL CLOSED. Falling back to Local would hand
        # different session-local objects to the archiver and the Task
        # Scheduler watchdog, and both would believe they owned the drive.
        hint = ""
        if last_err == _ERROR_ACCESS_DENIED:
            hint = (" Creating a Global object needs the SeCreateGlobalPrivilege"
                    " (an elevated or service context).")
        get_logger().error(
            "ltfs_ownership_global_unavailable: mutex=%s win32_error=%s -- "
            "cross-session LTFS ownership CANNOT be guaranteed. Refusing every "
            "tape, mount, readiness and robocopy operation.%s",
            self.name, last_err, hint)
        raise LtfsOwnershipError(
            f"could not create the Global LTFS ownership mutex {self.name!r} "
            f"(win32 error {last_err}). Cross-session ownership cannot be "
            f"guaranteed, so no LTFS operation may proceed.{hint}",
            kind=FAILURE_CROSS_SESSION_UNAVAILABLE)

    def assert_production_scope(self):
        """Preflight: prove ownership really is Global before any tape work."""
        if self._allow_local_scope:
            raise LtfsOwnershipError(
                "LTFS ownership is running with TEST-ONLY local scope; refusing "
                "to treat it as a production safety boundary.",
                kind=FAILURE_CROSS_SESSION_UNAVAILABLE)
        self._ensure_handle()
        if self._scope != "Global":
            raise LtfsOwnershipError(
                f"LTFS ownership scope is {self._scope!r}, not 'Global'; "
                f"cross-session ownership cannot be guaranteed.",
                kind=FAILURE_CROSS_SESSION_UNAVAILABLE)
        get_logger().info(
            "ltfs_ownership_preflight_ok: mutex=%s scope=Global writer_timeout=%ss "
            "helper_timeout=%ss", self.name, writer_timeout_seconds(),
            helper_timeout_seconds())
        return True

    def _rebind_name_for_test(self, name):
        """TEST-ONLY in-place rename (see :func:`activate_test_ownership_isolation`).

        Mutating the existing object rather than replacing it means every module
        that did ``from ... import OWNERSHIP`` sees the change. Refuses while the
        lock is held so a rebind can never orphan an acquired mutex."""
        with self._local:
            if self._depth > 0:
                raise LtfsOwnershipError(
                    "cannot rebind LTFS ownership while it is held")
            old = self._handle
            self._handle = None
            self._name_override = name
            self._resolved_name = None
            self._scope = None
            if _IS_WINDOWS and old:
                try:
                    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                    kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
                    kernel32.CloseHandle(old)
                except Exception:
                    pass
            if _IS_WINDOWS:
                try:
                    self._ensure_handle()
                except Exception:
                    get_logger().debug(
                        "ltfs_ownership_test_rebind_deferred: mutex=%s", name)

    # -- state -------------------------------------------------------------
    @property
    def generation(self):
        with self._local:
            return self._generation

    @property
    def depth(self):
        with self._local:
            return self._depth

    def owned_by_this_process(self):
        with self._local:
            return self._depth > 0

    def scope(self):
        return self._scope

    # -- acquisition -------------------------------------------------------
    def acquire(self, operation, timeout=None, blocking=True):
        """Take authoritative ownership. Raises LtfsOwnershipError on failure.

        Recursive within this process: nested calls increment a depth counter
        and return immediately without re-entering the kernel object.
        """
        wait_timeout = self.timeout if timeout is None else timeout
        pid = os.getpid()
        proc_name = os.path.basename(sys.executable)
        started = time.monotonic()

        # Thread serialiser first: only one thread per process may contend for
        # the kernel object, and the acquiring thread is the releasing thread.
        if not self._local.acquire(blocking=blocking):
            raise LtfsOwnershipError(
                f"another thread in this process holds LTFS ownership "
                f"(operation={operation})", operation=operation, timeout=0)

        if self._depth > 0:                          # recursive re-entry
            self._depth += 1
            get_logger().debug(
                "ltfs_ownership_reentered: mutex=%s pid=%s op=%s depth=%s",
                self.name, pid, operation, self._depth)
            return True

        # Depth 0: enter the kernel object. Any failure below must give the
        # thread lock back before raising.
        try:
            handle = self._ensure_handle()
            ms = (0 if not blocking
                  else (_INFINITE if wait_timeout is None
                        else int(max(0.0, wait_timeout) * 1000)))
            get_logger().info(
                "ltfs_ownership_requested: mutex=%s scope=%s pid=%s proc=%s "
                "op=%s mode=%s timeout=%s",
                self.name, self._scope, pid, proc_name, operation,
                "blocking" if blocking else "nonblocking",
                "infinite" if ms == _INFINITE else f"{ms / 1000:.1f}s")
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.WaitForSingleObject.argtypes = (ctypes.c_void_p,
                                                     ctypes.c_uint32)
            kernel32.WaitForSingleObject.restype = ctypes.c_uint32
            result = kernel32.WaitForSingleObject(handle, ms)
            waited = time.monotonic() - started
        except BaseException:
            self._local.release()
            raise

        if result in (_WAIT_OBJECT_0, _WAIT_ABANDONED):
            abandoned = (result == _WAIT_ABANDONED)
            self._depth = 1
            self._generation += 1
            self._owner_thread = threading.get_ident()
            self._last_abandoned = abandoned
            get_logger().info(
                "ltfs_ownership_acquired: mutex=%s scope=%s pid=%s proc=%s "
                "op=%s waited=%.3fs abandoned_recovered=%s generation=%s "
                "result=granted",
                self.name, self._scope, pid, proc_name, operation, waited,
                abandoned, self._generation)
            if abandoned:
                get_logger().error(
                    "ltfs_ownership_abandoned_recovered: mutex=%s pid=%s op=%s "
                    "-- the previous owner died holding LTFS ownership; device "
                    "state is unverified and cached readiness is discarded.",
                    self.name, pid, operation)
            # Any (re)acquisition means another owner may have touched LTFS
            # while we did not hold it. Never reuse cached readiness.
            _notify_ownership_change(
                f"ownership acquired (generation={self._generation}, "
                f"abandoned={abandoned}, op={operation})")
            return True

        # Not acquired: give the thread lock back and fail closed.
        self._local.release()
        if result == _WAIT_TIMEOUT:
            get_logger().error(
                "ltfs_ownership_denied: mutex=%s pid=%s op=%s waited=%.3fs "
                "timeout=%ss result=timeout",
                self.name, pid, operation, waited, wait_timeout)
            raise LtfsOwnershipError(
                f"another process owns the LTFS drive; could not acquire "
                f"{self.name!r} for {operation!r} within {wait_timeout}s",
                operation=operation, waited_seconds=waited,
                timeout=wait_timeout, kind=FAILURE_TIMEOUT)
        err = ctypes.get_last_error()
        get_logger().error(
            "ltfs_ownership_denied: mutex=%s pid=%s op=%s result=failed "
            "win32_error=%s", self.name, pid, operation, err)
        raise LtfsOwnershipError(
            f"waiting for LTFS ownership {self.name!r} failed "
            f"(win32 error {err})", operation=operation,
            waited_seconds=waited, timeout=wait_timeout,
            kind=FAILURE_PRIMITIVE)

    def release(self, operation=None, invalidate_readiness=True):
        """Release one level of ownership; frees the mutex at depth 0."""
        if self._depth == 0:
            raise LtfsOwnershipError("release() without holding LTFS ownership")
        self._depth -= 1
        if self._depth > 0:
            self._local.release()
            return True

        released_ok = True
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.ReleaseMutex.argtypes = (ctypes.c_void_p,)
            kernel32.ReleaseMutex.restype = ctypes.c_bool
            released_ok = bool(kernel32.ReleaseMutex(self._handle))
            if not released_ok:
                get_logger().error(
                    "ltfs_ownership_release_failed: mutex=%s pid=%s op=%s "
                    "win32_error=%s", self.name, os.getpid(), operation,
                    ctypes.get_last_error())
        finally:
            self._owner_thread = None
            get_logger().info(
                "ltfs_ownership_released: mutex=%s pid=%s op=%s generation=%s "
                "result=%s", self.name, os.getpid(), operation,
                self._generation, "ok" if released_ok else "failed")
            if invalidate_readiness:
                # Phase 3 safe default: once we let go, another process may
                # touch LTFS. Reacquisition must re-verify from scratch.
                _notify_ownership_change(
                    f"ownership released (generation={self._generation}, "
                    f"op={operation})")
            self._local.release()
        return released_ok

    def _atexit_release(self):
        try:
            while self._depth > 0:
                self.release(operation="process exit")
        except Exception:
            pass
        handle = self._handle
        self._handle = None
        if handle and _IS_WINDOWS:
            try:
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
                kernel32.CloseHandle(handle)
            except Exception:
                pass


# The process-wide singleton. It ALWAYS resolves the production identity from
# config — no environment variable can change it. Test isolation is applied
# only by an EXPLICIT call to activate_test_ownership_isolation() (below), which
# refuses to run outside pytest. This is deliberately not env-driven: an
# arbitrary production environment variable must never be able to silently
# repoint the ownership mutex.
OWNERSHIP = LtfsOwnership()


def _running_under_pytest():
    """True only inside a pytest process. Used to gate the test-only override so
    it can never take effect during a normal production run."""
    return ("pytest" in sys.modules
            or bool(os.environ.get("PYTEST_CURRENT_TEST"))
            or bool(os.environ.get("PYTEST_VERSION")))


def activate_test_ownership_isolation(test_id=None):
    """TEST-ONLY: rebind the module-level :data:`OWNERSHIP` singleton to a unique
    per-process ``Global\\`` mutex so accidentally-parallel pytest workers never
    contend on the real production mutex and hang for the writer timeout.

    Must be called EXPLICITLY by the test harness (``tests/conftest.py``);
    production never calls it, and it refuses to run outside pytest. Because it
    mutates the *same* singleton object in place, every ``from ... import
    OWNERSHIP`` reference picks up the isolated name. A test that must prove
    genuine cross-process contention still constructs ``LtfsOwnership(name=...)``
    with an explicit shared name and is unaffected by this.

    ``test_id`` defaults to ``LTO_TEST_OWNERSHIP_ID`` if set, else ``test_p<pid>``
    — deterministic and unique per process. Production identity, fail-closed
    scope, and ``default_mutex_name`` are all unchanged.
    """
    if not _running_under_pytest():
        raise RuntimeError(
            "activate_test_ownership_isolation() is test-only and refuses to run "
            "outside pytest; the production LTFS ownership identity must never be "
            "overridden by an environment variable or a stray call.")
    tid = (test_id or os.environ.get("LTO_TEST_OWNERSHIP_ID")
           or f"test_p{os.getpid()}")
    name = f"OWC_LTO8_LTFS_OWNERSHIP_{validate_ownership_id(tid)}"
    OWNERSHIP._rebind_name_for_test(name)
    get_logger().info("ltfs_ownership_test_isolation: singleton rebound to %s",
                      name)
    return name


def owns_ltfs():
    """True when this process currently holds authoritative LTFS ownership."""
    return OWNERSHIP.owned_by_this_process()


def require_ownership(operation):
    """Assert the caller owns LTFS before a low-level tape operation.

    Stops a second Python process (or a careless import) from calling a
    low-level helper directly and bypassing the mutex.
    """
    if not owns_ltfs():
        raise LtfsOwnershipError(
            f"{operation} requires authoritative LTFS ownership, which this "
            f"process does not hold. Use the tape I/O lock rather than calling "
            f"the low-level helper directly.", operation=operation)
