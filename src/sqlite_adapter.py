"""psycopg-shaped connection, cursor and pool over the stdlib ``sqlite3``.

The catalog mixins talk to psycopg 3 through a small surface
(``conn.execute``, ``conn.cursor()``, ``conn.transaction()``, ``commit`` /
``rollback`` / ``close`` / ``autocommit``, ``cur.rowcount`` / ``executemany``,
dict rows, and a pool with ``connection()`` / ``getconn`` / ``putconn`` /
``close``).  This module provides exactly that surface for SQLite so the same
mixin code can run on either engine.

Guarantees:

* Every statement goes through :func:`src.sqlite_dialect.rewrite`; a statement
  that cannot be translated completely raises and never reaches SQLite.
* One writer at a time.  A write transaction is ``BEGIN IMMEDIATE`` and holds a
  process-wide per-database write lock from ``BEGIN`` to ``COMMIT`` /
  ``ROLLBACK`` so pool threads queue instead of colliding on ``SQLITE_BUSY``.
  Readers never take it (WAL lets them run beside the writer).
* Durability first: WAL (verified, else the connection is refused),
  ``synchronous=FULL``, ``foreign_keys=ON``, ``trusted_schema=OFF``; UNC and
  other network paths are refused because WAL needs local shared memory.
* Errors are mapped to a small hierarchy with one clearly *retryable* class
  (``SQLITE_BUSY`` / ``SQLITE_LOCKED``) and one *never retried* class
  (``FULL`` / ``IOERR`` / ``CORRUPT`` / ``READONLY`` ...).

Recommended column declarations (``PARSE_DECLTYPES`` uses the first word of
the declared type to pick a converter; the second word fixes the affinity):
``TIMESTAMPTZ TEXT``, ``BOOLEAN INTEGER``, ``JSONB TEXT``, ``BLOB``.
"""
from __future__ import annotations

import collections
import collections.abc
import contextlib
import json
import os
import sqlite3
import threading
import time
import urllib.parse
import uuid
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal

from .sqlite_dialect import SqliteDialectError, rewrite

__all__ = [
    "SQLITE_MIN_VERSION", "DEFAULT_BUSY_TIMEOUT_MS", "MAX_POOL_SIZE",
    "SqliteError", "RetryableError", "BusyError", "PoolTimeout",
    "FatalError", "ReadOnlyError", "IntegrityError", "UniqueViolation",
    "ForeignKeyViolation", "NotNullViolation", "CheckViolation",
    "TriggerAbort", "ProgrammingError", "InterfaceError", "NotSupportedError",
    "QueryCanceled", "PoolClosed", "SqliteVersionError", "NonLocalPathError",
    "SqliteDialectError",
    "Row", "SqliteCursor", "SqliteConnection", "SqlitePool", "connect",
    "map_error", "require_sqlite_version", "is_network_path",
    "assert_local_path", "format_timestamptz", "parse_timestamptz",
    "path_from_dsn",
]

SQLITE_MIN_VERSION = (3, 38, 0)
DEFAULT_BUSY_TIMEOUT_MS = 30000
MAX_POOL_SIZE = 8


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class SqliteError(Exception):
    """Base class for every error raised by this adapter."""

    #: True only for errors a caller may retry the whole transaction on.
    retryable = False
    sqlite_errorcode = None
    sqlite_errorname = None


class RetryableError(SqliteError):
    """Transient: retry the whole transaction (the ``_transaction`` hook)."""

    retryable = True


class BusyError(RetryableError):
    """``SQLITE_BUSY`` / ``SQLITE_LOCKED``, or a write-lock wait timeout."""


class PoolTimeout(RetryableError):
    """No pooled connection became free in time."""


class FatalError(SqliteError):
    """Never retried: ``FULL``, ``IOERR``, ``CORRUPT``, ``NOTADB``,
    ``CANTOPEN``, ``READONLY``.  Retrying cannot help and may make it worse."""


class ReadOnlyError(FatalError):
    """A write was attempted on a read-only database or connection."""


class IntegrityError(SqliteError):
    """A constraint rejected the statement."""


class UniqueViolation(IntegrityError):
    pass


class ForeignKeyViolation(IntegrityError):
    pass


class NotNullViolation(IntegrityError):
    pass


class CheckViolation(IntegrityError):
    pass


class TriggerAbort(IntegrityError):
    """``RAISE(ABORT, ...)`` from a trigger (the plpgsql RAISE EXCEPTION
    counterpart)."""


class ProgrammingError(SqliteError):
    """Bad SQL, wrong parameters, or misuse of the connection."""


class InterfaceError(SqliteError):
    """A Python value cannot be sent to the database."""


class NotSupportedError(SqliteError):
    """A psycopg feature with no SQLite counterpart (e.g. ``COPY``)."""


class QueryCanceled(SqliteError):
    """The statement timeout interrupted the statement."""


class PoolClosed(SqliteError):
    pass


class SqliteVersionError(SqliteError):
    pass


class NonLocalPathError(SqliteError):
    """The database path is on a network share (or cannot be proven local)."""


_PRIMARY_FATAL = {
    8: ReadOnlyError,    # SQLITE_READONLY
    10: FatalError,      # SQLITE_IOERR
    11: FatalError,      # SQLITE_CORRUPT
    13: FatalError,      # SQLITE_FULL
    14: FatalError,      # SQLITE_CANTOPEN
    26: FatalError,      # SQLITE_NOTADB
}
_CONSTRAINT_CLASSES = {
    275: CheckViolation,         # SQLITE_CONSTRAINT_CHECK
    787: ForeignKeyViolation,    # SQLITE_CONSTRAINT_FOREIGNKEY
    1299: NotNullViolation,      # SQLITE_CONSTRAINT_NOTNULL
    1555: UniqueViolation,       # SQLITE_CONSTRAINT_PRIMARYKEY
    1811: TriggerAbort,          # SQLITE_CONSTRAINT_TRIGGER
    2067: UniqueViolation,       # SQLITE_CONSTRAINT_UNIQUE
    2579: UniqueViolation,       # SQLITE_CONSTRAINT_ROWID
}


def map_error(exc):
    """Translate a ``sqlite3`` exception into the adapter hierarchy."""
    if isinstance(exc, SqliteError):
        return exc
    code = getattr(exc, "sqlite_errorcode", None)
    name = getattr(exc, "sqlite_errorname", None)
    primary = code & 0xFF if isinstance(code, int) else None
    if primary in (5, 6):                      # BUSY, LOCKED
        cls = BusyError
    elif primary in _PRIMARY_FATAL:
        cls = _PRIMARY_FATAL[primary]
    elif primary == 19:                        # CONSTRAINT
        cls = _CONSTRAINT_CLASSES.get(code, IntegrityError)
    elif primary == 9:                         # INTERRUPT
        cls = QueryCanceled
    elif isinstance(exc, sqlite3.IntegrityError):
        cls = IntegrityError
    elif isinstance(exc, (sqlite3.ProgrammingError, sqlite3.InterfaceError,
                          sqlite3.OperationalError)):
        cls = ProgrammingError
    elif isinstance(exc, sqlite3.DatabaseError) and primary is None \
            and "malformed" in str(exc).lower():
        cls = FatalError
    else:
        cls = SqliteError
    label = f" ({name})" if name else ""
    mapped = cls(f"[SQLITE] {exc}{label}")
    mapped.sqlite_errorcode = code
    mapped.sqlite_errorname = name
    return mapped


# ---------------------------------------------------------------------------
# Environment checks
# ---------------------------------------------------------------------------

def require_sqlite_version(version_info=None):
    """Hard floor: ``->>``, ``json_each``, RETURNING and strict WAL behaviour
    all need SQLite >= 3.38."""
    found = tuple(version_info or sqlite3.sqlite_version_info)
    if found < SQLITE_MIN_VERSION:
        raise SqliteVersionError(
            "[SQLITE] SQLite "
            + ".".join(str(part) for part in SQLITE_MIN_VERSION)
            + " or newer is required; this Python is linked against "
            + ".".join(str(part) for part in found))
    return found


_NETWORK_FS_TYPES = frozenset((
    "nfs", "nfs3", "nfs4", "cifs", "smb", "smb2", "smb3", "smbfs", "afs",
    "ncpfs", "9p", "ceph", "glusterfs", "lustre", "gpfs", "beegfs", "davfs",
    "fuse.sshfs", "fuse.rclone", "fuse.glusterfs", "fuse.cephfs",
))
_DRIVE_REMOVABLE, _DRIVE_FIXED, _DRIVE_RAMDISK = 2, 3, 6


def _windows_drive_type(root):  # pragma: no cover - trivial OS wrapper
    import ctypes
    return ctypes.windll.kernel32.GetDriveTypeW(root)


def _linux_mount_fstype(path):
    """Filesystem type of the mount holding ``path`` (None if unknown)."""
    try:
        with open("/proc/self/mounts", "r", encoding="utf-8",
                  errors="replace") as handle:
            lines = handle.read().splitlines()
    except OSError:
        return None
    best = ("", None)
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        mount = parts[1].replace("\\040", " ")
        if path == mount or path.startswith(mount.rstrip("/") + "/"):
            if len(mount) >= len(best[0]):
                best = (mount, parts[2])
    return best[1]


def is_network_path(path):
    """True when ``path`` is on a network share or cannot be proven local.

    Fails closed: any doubt (no drive letter, unknown drive type) counts as
    network.  WAL needs shared memory and reliable byte-range locks, which
    network filesystems do not provide.
    """
    text = os.path.abspath(os.fspath(path))
    try:
        text = os.path.realpath(text)
    except OSError:
        return True
    if os.name == "nt":
        norm = text.replace("/", "\\")
        if norm.startswith("\\\\"):
            if norm[2:4] in ("?\\", ".\\"):
                rest = norm[4:]
                if rest.upper().startswith("UNC\\"):
                    return True
                norm = rest
            else:
                return True
        drive = os.path.splitdrive(norm)[0]
        if len(drive) != 2 or drive[1] != ":":
            return True
        try:
            kind = _windows_drive_type(drive + "\\")
        except Exception:
            return True
        return kind not in (_DRIVE_REMOVABLE, _DRIVE_FIXED, _DRIVE_RAMDISK)
    if text.startswith("//"):
        return True
    fstype = _linux_mount_fstype(text)
    return fstype is not None and fstype.lower() in _NETWORK_FS_TYPES


def assert_local_path(path, what="SQLite database"):
    if is_network_path(path):
        raise NonLocalPathError(
            f"[SQLITE] Refusing {what} on a network or non-local path: "
            f"{os.fspath(path)}. It must live on a local disk.")


def path_from_dsn(dsn):
    """Accept a plain path or ``sqlite:///<path>`` and return the path."""
    text = os.fspath(dsn)
    if text.lower().startswith("sqlite:///"):
        text = urllib.parse.unquote(text[len("sqlite:///"):])
        if os.name != "nt" and not text.startswith("/"):
            text = "/" + text
    elif text.lower().startswith("sqlite:"):
        raise ProgrammingError(
            f"[SQLITE] Unsupported SQLite DSN (use sqlite:///<path>): {text}")
    return text


# ---------------------------------------------------------------------------
# Type adaptation
# ---------------------------------------------------------------------------

def format_timestamptz(value):
    """Canonical storage text: ``YYYY-MM-DD HH:MM:SS.ffffff+00:00`` (UTC).

    Fixed width, so text comparison orders chronologically.  A naive
    ``datetime`` is taken as local time, as PostgreSQL does for the session
    time zone.
    """
    return value.astimezone(timezone.utc).isoformat(
        sep=" ", timespec="microseconds")


def parse_timestamptz(text):
    """Stored text -> aware UTC ``datetime``.  Naive text is taken as UTC."""
    if isinstance(text, (bytes, bytearray)):
        text = bytes(text).decode("utf-8")
    value = datetime.fromisoformat(text.strip())
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _convert_boolean(raw):
    text = bytes(raw).strip().lower()
    if text in (b"1", b"true", b"t"):
        return True
    if text in (b"0", b"false", b"f"):
        return False
    raise ValueError(f"not a boolean column value: {raw!r}")


def _convert_json(raw):
    return json.loads(bytes(raw).decode("utf-8"))


# sqlite3 keeps converters in one process-wide registry keyed by the first
# word of the declared column type.  These names are specific enough not to
# collide with other users of the module.
for _name, _converter in (
        ("timestamptz", parse_timestamptz),
        ("boolean", _convert_boolean),
        ("bool", _convert_boolean),
        ("jsonb", _convert_json),
        ("json", _convert_json)):
    sqlite3.register_converter(_name, _converter)


def _adapt(value):
    """One Python parameter -> a value ``sqlite3`` stores natively."""
    kind = type(value)
    if value is None or kind in (int, float, str, bytes):
        return value
    if kind is bool:
        return int(value)
    if isinstance(value, datetime):
        return format_timestamptz(value)
    if isinstance(value, (date, dt_time)):
        return value.isoformat()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() \
            else float(value)
    if isinstance(value, (list, tuple, dict, set, frozenset)):
        raise InterfaceError(
            f"[SQLITE] Cannot send a {kind.__name__} as a plain parameter. "
            "Lists are only accepted for '= ANY(%s)'; JSON columns take "
            "json.dumps() text.")
    return value


def _json_element(value):
    if value is None or type(value) in (int, float, str, bool):
        return value
    if isinstance(value, datetime):
        return format_timestamptz(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise InterfaceError(
            "[SQLITE] A bytes list cannot be sent through '= ANY(%s)' "
            "(json_each has no BLOB values); this site needs a backend hook.")
    if isinstance(value, (int, str)):       # subclasses (IntEnum, str enums)
        return value
    raise InterfaceError(
        f"[SQLITE] Unsupported element type {type(value).__name__} in an "
        "ANY() list parameter")


def _adapt_any_list(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple, set, frozenset)):
        return json.dumps([_json_element(item) for item in value])
    raise InterfaceError(
        "[SQLITE] '= ANY(%s)' needs a list parameter, got "
        f"{type(value).__name__}")


def _bind(stmt, params):
    """Adapt psycopg-style parameters for a rewritten statement."""
    if stmt.param_style == "named":
        if not isinstance(params, collections.abc.Mapping):
            raise ProgrammingError(
                "[SQLITE] Statement uses %(name)s placeholders; parameters "
                "must be a mapping")
        return {key: (_adapt_any_list(value) if key in stmt.json_names
                      else _adapt(value))
                for key, value in params.items()}
    if params is None:
        params = ()
    if isinstance(params, collections.abc.Mapping) or isinstance(
            params, (str, bytes)):
        raise ProgrammingError(
            "[SQLITE] Statement uses positional placeholders; parameters "
            "must be a sequence")
    params = list(params)
    if len(params) != stmt.param_count:
        raise ProgrammingError(
            f"[SQLITE] The statement has {stmt.param_count} placeholders "
            f"but {len(params)} parameters were passed")
    if stmt.json_positions:
        return [_adapt_any_list(value) if index in stmt.json_positions
                else _adapt(value) for index, value in enumerate(params)]
    return [_adapt(value) for value in params]


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

class Row(dict):
    """Dict row that also answers positional access, like ``PgRow``."""

    __slots__ = ("_keys",)

    def __init__(self, data=(), keys=None):
        super().__init__(data)
        self._keys = keys if keys is not None else tuple(dict.keys(self))

    def __getitem__(self, key):
        if isinstance(key, int) and not isinstance(key, bool):
            return dict.__getitem__(self, self._keys[key])
        return dict.__getitem__(self, key)

    def keys(self):
        return self._keys

    def __reduce__(self):
        return (Row, (dict(self),))


Column = collections.namedtuple(
    "Column", "name type_code display_size internal_size precision scale "
              "null_ok")


# ---------------------------------------------------------------------------
# Process-wide write locks (one per database file)
# ---------------------------------------------------------------------------

_WRITE_LOCKS = {}
_WRITE_LOCKS_GUARD = threading.Lock()


def _write_lock_for(path):
    key = os.path.normcase(os.path.realpath(path))
    with _WRITE_LOCKS_GUARD:
        lock = _WRITE_LOCKS.get(key)
        if lock is None:
            lock = _WRITE_LOCKS[key] = threading.Lock()
        return lock


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------

class SqliteCursor:
    """psycopg-shaped cursor.

    Unnamed cursors buffer the whole result eagerly (as psycopg client-side
    cursors do), so ``rowcount`` is exact for SELECT and RETURNING and no
    half-read statement can block a later COMMIT.  A *named* cursor iterates
    unbuffered, the counterpart of a server-side cursor.
    """

    def __init__(self, connection, name=None):
        self.connection = connection
        self.name = name
        self.itersize = 2000
        self.arraysize = 1
        self._rows = None
        self._pos = 0
        self._raw = None
        self._keys = None
        self._unique = True
        self._description = None
        self._rowcount = -1
        self._closed = False

    # -- context manager ----------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        self._closed = True
        self._discard()

    @property
    def closed(self):
        return self._closed

    def _discard(self):
        raw, self._raw = self._raw, None
        if raw is not None:
            try:
                raw.close()
            except sqlite3.Error:
                pass
        self._rows = None
        self._pos = 0

    # -- execution ----------------------------------------------------------
    def execute(self, query, params=None, **_psycopg_options):
        if self._closed:
            raise InterfaceError("[SQLITE] The cursor is closed")
        self._discard()
        self.connection._execute(self, query, params)
        return self

    def executemany(self, query, params_seq, **_psycopg_options):
        if self._closed:
            raise InterfaceError("[SQLITE] The cursor is closed")
        self._discard()
        self.connection._execute(self, query, params_seq, many=True)

    def copy(self, *args, **kwargs):
        raise NotSupportedError(
            "[SQLITE] COPY is PostgreSQL-only; bulk loads go through the "
            "backend's stage-table hook (executemany)")

    def _set_result(self, description, rows, rowcount, raw=None):
        if description is None:
            self._description = None
            self._keys = None
        else:
            self._keys = tuple(col[0] for col in description)
            self._unique = len(set(self._keys)) == len(self._keys)
            self._description = [
                Column(col[0], *col[1:7]) for col in description]
        self._raw = raw
        self._rows = None if rows is None else self._make_rows(rows)
        self._pos = 0
        self._rowcount = rowcount

    def _make_rows(self, tuples):
        keys = self._keys
        if self._unique:
            return [Row(zip(keys, values), keys) for values in tuples]
        return [Row(zip(keys, values)) for values in tuples]

    # -- results ------------------------------------------------------------
    @property
    def rowcount(self):
        return self._rowcount

    @property
    def description(self):
        return self._description

    def _require_result(self):
        if self._keys is None:
            raise ProgrammingError("[SQLITE] No result available")

    def _fetch_raw(self, size):
        try:
            data = (self._raw.fetchall() if size is None
                    else self._raw.fetchmany(size))
        except sqlite3.Error as exc:
            raise map_error(exc) from exc
        return self._make_rows(data)

    def fetchone(self):
        self._require_result()
        if self._raw is not None:
            rows = self._fetch_raw(1)
            return rows[0] if rows else None
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        self._require_result()
        size = self.arraysize if size is None else size
        if self._raw is not None:
            return self._fetch_raw(size)
        chunk = self._rows[self._pos:self._pos + size]
        self._pos += len(chunk)
        return chunk

    def fetchall(self):
        self._require_result()
        if self._raw is not None:
            return self._fetch_raw(None)
        chunk = self._rows[self._pos:]
        self._pos = len(self._rows)
        return chunk

    def __iter__(self):
        self._require_result()
        if self._raw is not None:
            while True:
                batch = self._fetch_raw(max(1, int(self.itersize)))
                if not batch:
                    return
                yield from batch
        else:
            while self._rows is not None and self._pos < len(self._rows):
                row = self._rows[self._pos]
                self._pos += 1
                yield row


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_TXN_KEYWORDS = frozenset((
    "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"))
_NO_TXN_KEYWORDS = frozenset(("PRAGMA", "VACUUM", "ATTACH", "DETACH"))


class SqliteConnection:
    """One SQLite connection with psycopg transaction semantics.

    ``autocommit=False`` (the default, as in the PostgreSQL pool): the first
    statement opens an implicit transaction that lasts until ``commit()`` /
    ``rollback()`` -- deferred when it starts with a read, ``BEGIN IMMEDIATE``
    when it starts with a write.  A write arriving inside a read transaction
    ends the (still read-only) transaction and reopens it as IMMEDIATE, which
    matches PostgreSQL READ COMMITTED and avoids ``SQLITE_BUSY_SNAPSHOT``;
    ``strict_read_then_write=True`` raises instead, for tests.

    A connection belongs to one thread at a time.
    """

    def __init__(self, path, *, read_only=False,
                 busy_timeout_ms=DEFAULT_BUSY_TIMEOUT_MS, autocommit=False,
                 strict_read_then_write=False):
        require_sqlite_version()
        path = os.path.abspath(path_from_dsn(path))
        assert_local_path(path)
        self.path = path
        self.read_only = bool(read_only)
        self.strict_read_then_write = bool(strict_read_then_write)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.settings = {}
        self._txn_settings_dirty = False
        self._autocommit = bool(autocommit)
        self._tx_depth = 0
        self._savepoint_seq = 0
        self._has_write_lock = False
        self._deadline = None
        self._statement_timeout = None
        self._closed = False
        self._broken = False
        self._write_lock = _write_lock_for(path)
        self._pool = None

        quoted = urllib.parse.quote(path.replace("\\", "/"), safe="/:")
        uri = ("file://" + ("" if quoted.startswith("/") else "/") + quoted
               + ("?mode=ro" if self.read_only else "?mode=rwc"))
        try:
            self._raw = sqlite3.connect(
                uri, uri=True, isolation_level=None, check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES,
                timeout=self.busy_timeout_ms / 1000.0, cached_statements=256)
        except sqlite3.Error as exc:
            raise map_error(exc) from exc
        try:
            self._configure()
        except BaseException:
            self._raw.close()
            raise

    def _configure(self):
        raw = self._raw
        try:
            raw.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            raw.execute("PRAGMA trusted_schema=OFF")
            raw.execute("PRAGMA foreign_keys=ON")
            if self.read_only:
                raw.execute("PRAGMA query_only=ON")
                mode = raw.execute("PRAGMA journal_mode").fetchone()[0]
            else:
                mode = raw.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                raw.execute("PRAGMA synchronous=FULL")
                if raw.execute("PRAGMA synchronous").fetchone()[0] != 2:
                    raise FatalError(
                        "[SQLITE] synchronous=FULL did not take effect")
            if str(mode).lower() != "wal":
                raise FatalError(
                    f"[SQLITE] Refusing {self.path}: journal mode is "
                    f"{mode!r}, WAL is required"
                    + (" (open it read-write once to convert it)"
                       if self.read_only else ""))
            if raw.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
                raise FatalError(
                    "[SQLITE] foreign_keys=ON did not take effect")
            raw.create_function(
                "lto_setting", 1, self._lto_setting, deterministic=False)
        except sqlite3.Error as exc:
            raise map_error(exc) from exc

    def _lto_setting(self, name):
        value = self.settings.get(name)
        return None if value is None else str(value)

    # -- simple state ---------------------------------------------------------
    @property
    def raw(self):
        """The underlying ``sqlite3.Connection`` (backup, PRAGMAs, tests)."""
        return self._raw

    @property
    def closed(self):
        return self._closed

    @property
    def broken(self):
        return self._broken

    @property
    def in_transaction(self):
        return not self._closed and self._raw.in_transaction

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if self.in_transaction:
            raise ProgrammingError(
                "[SQLITE] Cannot change autocommit inside a transaction")
        self._autocommit = bool(value)

    @property
    def statement_timeout(self):
        """Seconds a single statement may run (None = unlimited)."""
        return self._statement_timeout

    @statement_timeout.setter
    def statement_timeout(self, seconds):
        self._statement_timeout = None if not seconds else float(seconds)
        if self._statement_timeout is None:
            self._raw.set_progress_handler(None, 0)
        else:
            self._raw.set_progress_handler(self._progress, 10000)

    def _progress(self):
        deadline = self._deadline
        return 1 if deadline is not None and time.monotonic() > deadline else 0

    def set_setting(self, name, value):
        """Transaction-scoped setting readable in SQL as
        ``lto_setting(name)`` (the ``set_config(name, value, true)``
        counterpart).  Cleared when the transaction ends."""
        self.settings[str(name)] = None if value is None else str(value)

    #: Table that mirrors transaction-scoped settings for schema triggers.
    #: A trigger cannot call ``lto_setting()`` (an application function is
    #: "unsafe" in a trigger under ``trusted_schema=OFF``), so guards read
    #: this table instead.
    TXN_SETTINGS_TABLE = "lto_txn_settings"

    def set_txn_setting(self, name, value):
        """Like :meth:`set_setting`, and also visible to schema triggers.

        The row lives in ``lto_txn_settings`` only inside the current write
        transaction: it is deleted in the same transaction just before COMMIT
        and vanishes with a ROLLBACK, so it is never durable and never seen by
        another connection.  Requires an open transaction.
        """
        self._check_open()
        if self.read_only:
            raise ReadOnlyError(
                "[SQLITE] set_txn_setting() on a read-only connection")
        if not self._raw.in_transaction:
            raise ProgrammingError(
                "[SQLITE] set_txn_setting() needs an open transaction; the "
                "setting is transaction-scoped")
        if not self._has_write_lock:
            self._upgrade_to_write()
        self.set_setting(name, value)
        try:
            self._raw.execute(
                f"INSERT OR REPLACE INTO {self.TXN_SETTINGS_TABLE} "
                "(name, value) VALUES (?, ?)",
                (str(name), None if value is None else str(value)))
        except sqlite3.Error as exc:
            self._after_statement_error()
            raise map_error(exc) from exc
        self._txn_settings_dirty = True

    def _check_open(self):
        if self._closed:
            raise InterfaceError("[SQLITE] The connection is closed")

    # -- write lock -----------------------------------------------------------
    def _acquire_write_lock(self):
        if self._has_write_lock:
            return
        if not self._write_lock.acquire(
                timeout=max(self.busy_timeout_ms / 1000.0, 0.001)):
            raise BusyError(
                "[SQLITE] Timed out waiting for the in-process write lock "
                f"after {self.busy_timeout_ms} ms")
        self._has_write_lock = True

    def _release_write_lock(self):
        if self._has_write_lock:
            self._has_write_lock = False
            self._write_lock.release()

    # -- transaction plumbing -------------------------------------------------
    def _raw_exec(self, sql):
        try:
            self._raw.execute(sql)
        except sqlite3.Error as exc:
            raise map_error(exc) from exc

    def _begin(self, write):
        if write and not self.read_only:
            self._acquire_write_lock()
            try:
                self._raw_exec("BEGIN IMMEDIATE")
            except BaseException:
                self._release_write_lock()
                raise
        else:
            self._raw_exec("BEGIN")

    def _upgrade_to_write(self):
        """Turn the current read transaction into a write transaction."""
        if self.strict_read_then_write:
            raise ProgrammingError(
                "[SQLITE] strict mode: a write (or FOR UPDATE) arrived "
                "inside a transaction that started with a read; wrap the "
                "block in conn.transaction()")
        self._acquire_write_lock()
        try:
            self._raw_exec("COMMIT")
            self._raw_exec("BEGIN IMMEDIATE")
        except BaseException:
            if not self._raw.in_transaction:
                self.settings.clear()
                self._txn_settings_dirty = False
            self._release_write_lock()
            raise

    def _end(self, commit):
        """Finish the whole transaction and release everything it held."""
        try:
            if self._raw.in_transaction:
                try:
                    if commit and self._txn_settings_dirty:
                        # Transaction-scoped settings must never be durable.
                        self._raw.execute(
                            f"DELETE FROM {self.TXN_SETTINGS_TABLE}")
                    self._raw.execute("COMMIT" if commit else "ROLLBACK")
                except sqlite3.Error as exc:
                    if self._raw.in_transaction:
                        try:
                            self._raw.execute("ROLLBACK")
                        except sqlite3.Error:
                            self._broken = True
                    raise map_error(exc) from exc
        finally:
            self._tx_depth = 0
            self.settings.clear()
            self._txn_settings_dirty = False
            self._release_write_lock()

    def _after_statement_error(self):
        # Some errors (FULL, IOERR, NOMEM, an interrupted write...) make
        # SQLite roll the whole transaction back by itself.
        if not self._raw.in_transaction and (
                self._has_write_lock or self._tx_depth):
            self._tx_depth = 0
            self.settings.clear()
            self._txn_settings_dirty = False
            self._release_write_lock()

    def commit(self):
        self._check_open()
        if self._tx_depth:
            raise ProgrammingError(
                "[SQLITE] commit() is not allowed inside a transaction() "
                "block")
        self._end(True)

    def rollback(self):
        self._check_open()
        if self._tx_depth:
            raise ProgrammingError(
                "[SQLITE] rollback() is not allowed inside a transaction() "
                "block")
        self._end(False)

    @contextlib.contextmanager
    def transaction(self, savepoint_name=None, force_rollback=False):
        """``BEGIN IMMEDIATE`` ... ``COMMIT``; nested use is a savepoint."""
        self._check_open()
        savepoint = None
        if not self._raw.in_transaction:
            self._begin(write=True)
        else:
            if not self._has_write_lock and not self.read_only:
                self._upgrade_to_write()
            self._savepoint_seq += 1
            savepoint = savepoint_name or f"lto_sp_{self._savepoint_seq}"
            if not savepoint.replace("_", "").isalnum():
                raise ProgrammingError(
                    f"[SQLITE] Unsafe savepoint name: {savepoint!r}")
            self._raw_exec(f'SAVEPOINT "{savepoint}"')
        self._tx_depth += 1
        depth = self._tx_depth
        try:
            yield self
        except BaseException:
            self._leave_transaction(savepoint, depth, commit=False)
            raise
        else:
            self._leave_transaction(
                savepoint, depth, commit=not force_rollback)

    def _leave_transaction(self, savepoint, depth, commit):
        if self._closed:
            return
        if self._tx_depth < depth or not self._raw.in_transaction:
            # SQLite already rolled the transaction back (or an outer level
            # ended it); only bookkeeping is left.
            self._after_statement_error()
            if not commit:
                return
            raise ProgrammingError(
                "[SQLITE] The transaction ended before its block did")
        self._tx_depth = depth - 1
        if savepoint is None:
            self._end(commit)
            return
        if commit:
            self._raw_exec(f'RELEASE "{savepoint}"')
        else:
            self._raw_exec(f'ROLLBACK TO "{savepoint}"')
            self._raw_exec(f'RELEASE "{savepoint}"')

    # -- statements -----------------------------------------------------------
    def cursor(self, name=None, **_psycopg_options):
        self._check_open()
        return SqliteCursor(self, name=name)

    def execute(self, query, params=None, **_psycopg_options):
        cur = SqliteCursor(self)
        self._execute(cur, query, params)
        return cur

    def _enter_statement(self, stmt):
        """Open the implicit transaction / take the write lock as needed.
        Returns True when the lock is held only for this statement."""
        write = stmt.write_intent and not self.read_only
        if self._raw.in_transaction:
            if stmt.keyword == "VACUUM":
                raise ProgrammingError(
                    "[SQLITE] VACUUM cannot run inside a transaction")
            if write and not self._has_write_lock:
                self._upgrade_to_write()
            return False
        if stmt.keyword in _NO_TXN_KEYWORDS or self._autocommit:
            if write:
                self._acquire_write_lock()
                return True
            return False
        self._begin(write=stmt.write_intent)
        return False

    def _execute(self, cursor, query, params, many=False):
        self._check_open()
        stmt = rewrite(query)
        if stmt.keyword in _TXN_KEYWORDS:
            raise ProgrammingError(
                f"[SQLITE] {stmt.keyword} must not be sent as SQL; use "
                "conn.transaction() / commit() / rollback()")
        if many:
            bound = [_bind(stmt, item) for item in params]
        else:
            bound = _bind(stmt, params)
        unbuffered = (cursor.name is not None and not many
                      and not stmt.write_intent)
        transient_lock = self._enter_statement(stmt)
        raw = self._raw.cursor()
        keep_raw = False
        try:
            if self._statement_timeout is not None:
                self._deadline = time.monotonic() + self._statement_timeout
            try:
                if not many:
                    raw.execute(stmt.sql, bound)
                    if raw.description is None:
                        cursor._set_result(None, None, raw.rowcount)
                    elif unbuffered:
                        keep_raw = True
                        cursor._set_result(raw.description, None, -1, raw)
                    else:
                        rows = raw.fetchall()
                        cursor._set_result(raw.description, rows, len(rows))
                elif stmt.has_returning:
                    # sqlite3.executemany refuses statements that return
                    # rows; run them one by one and discard the rows, as
                    # psycopg does with returning=False.
                    total = 0
                    for item in bound:
                        raw.execute(stmt.sql, item)
                        total += len(raw.fetchall())
                    cursor._set_result(None, None, total)
                else:
                    raw.executemany(stmt.sql, bound)
                    cursor._set_result(None, None, raw.rowcount)
            except sqlite3.Error as exc:
                self._after_statement_error()
                raise map_error(exc) from exc
        finally:
            self._deadline = None
            if not keep_raw:
                try:
                    raw.close()
                except sqlite3.Error:
                    pass
            if transient_lock:
                self._release_write_lock()

    def executescript(self, script):
        """Run a multi-statement SQLite script atomically, *without* dialect
        rewriting (it is for native SQLite schema files)."""
        self._check_open()
        if self._raw.in_transaction:
            raise ProgrammingError(
                "[SQLITE] executescript() needs a connection with no open "
                "transaction")
        if self.read_only:
            raise ReadOnlyError("[SQLITE] executescript() on a read-only "
                                "connection")
        self._acquire_write_lock()
        try:
            try:
                self._raw.executescript(
                    "BEGIN IMMEDIATE;\n" + script + "\n;COMMIT;")
            except sqlite3.Error as exc:
                if self._raw.in_transaction:
                    try:
                        self._raw.execute("ROLLBACK")
                    except sqlite3.Error:
                        self._broken = True
                raise map_error(exc) from exc
        finally:
            self._release_write_lock()

    # -- lifecycle ------------------------------------------------------------
    def _reset(self, commit):
        """Force the connection back to 'no transaction' (pool check-in)."""
        self._tx_depth = 0
        self._end(commit)

    def close(self):
        if self._closed:
            return
        try:
            if self._raw.in_transaction:
                try:
                    self._raw.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        finally:
            self._tx_depth = 0
            self.settings.clear()
            self._txn_settings_dirty = False
            self._release_write_lock()
            self._closed = True
            try:
                self._raw.close()
            except sqlite3.Error:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # psycopg: leaving the block commits (or rolls back) and closes.
        try:
            if not self._closed:
                self._reset(commit=exc_type is None)
        finally:
            self.close()
        return False


def connect(path, **options):
    """Open one :class:`SqliteConnection` (see its docstring for options)."""
    return SqliteConnection(path, **options)


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

class SqlitePool:
    """Thread-safe pool of at most :data:`MAX_POOL_SIZE` connections with the
    ``psycopg_pool.ConnectionPool`` methods the application uses."""

    def __init__(self, path, *, min_size=1, max_size=MAX_POOL_SIZE,
                 timeout=30.0, **connect_options):
        if not 1 <= int(max_size) <= MAX_POOL_SIZE:
            raise ValueError(
                f"max_size must be between 1 and {MAX_POOL_SIZE}")
        if not 0 <= int(min_size) <= int(max_size):
            raise ValueError("min_size must be between 0 and max_size")
        self.path = os.path.abspath(path_from_dsn(path))
        self.max_size = int(max_size)
        self.min_size = int(min_size)
        self.timeout = float(timeout)
        self._options = dict(connect_options)
        self._cond = threading.Condition()
        self._idle = collections.deque()
        self._size = 0
        self._closed = False
        try:
            for _ in range(self.min_size):
                self._idle.append(self._new_connection())
                self._size += 1
        except BaseException:
            self.close()
            raise

    def _new_connection(self):
        conn = SqliteConnection(self.path, **self._options)
        conn._pool = self
        return conn

    @property
    def closed(self):
        return self._closed

    def getconn(self, timeout=None):
        deadline = time.monotonic() + (
            self.timeout if timeout is None else float(timeout))
        with self._cond:
            while True:
                if self._closed:
                    raise PoolClosed("[SQLITE] The connection pool is closed")
                while self._idle:
                    conn = self._idle.pop()
                    if conn.closed or conn.broken:
                        self._size -= 1
                        continue
                    return conn
                if self._size < self.max_size:
                    self._size += 1     # reserve the slot, connect unlocked
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeout(
                        "[SQLITE] Couldn't get a connection after "
                        f"{self.timeout if timeout is None else timeout} s "
                        f"(all {self.max_size} are in use)")
                self._cond.wait(remaining)
        try:
            return self._new_connection()
        except BaseException:
            with self._cond:
                self._size -= 1
                self._cond.notify()
            raise

    def putconn(self, conn):
        if conn._pool is not self:
            raise ValueError("[SQLITE] Connection does not belong to this "
                             "pool")
        discard = False
        if not conn.closed:
            try:
                conn._reset(commit=False)   # an unfinished transaction is lost
                conn._autocommit = False
            except Exception:
                discard = True
        with self._cond:
            if conn.closed or conn.broken or discard or self._closed:
                self._size -= 1
                discard = True
            else:
                self._idle.append(conn)
            self._cond.notify()
        if discard:
            conn.close()

    @contextlib.contextmanager
    def connection(self, timeout=None):
        """Check a connection out; commit on success, roll back on error."""
        conn = self.getconn(timeout)
        try:
            try:
                yield conn
            except BaseException:
                if not conn.closed:
                    try:
                        conn._reset(commit=False)
                    except Exception:
                        pass
                raise
            else:
                if not conn.closed:
                    conn._reset(commit=True)
        finally:
            self.putconn(conn)

    def close(self, timeout=None):
        with self._cond:
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            self._size -= len(idle)
            self._cond.notify_all()
        for conn in idle:
            conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
