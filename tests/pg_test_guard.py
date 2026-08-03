"""Fail-closed guard: PostgreSQL tests may only touch a disposable server.

Why this exists
---------------
``src.pg_bulk.build_conninfo`` falls back to ``host=localhost port=5432
dbname=lto_archive user=lto`` when no ``PG*`` environment is set — and the
production ``lto_pg`` container listens on exactly ``127.0.0.1:5432``. So a bare
``python -m pytest`` on the archive workstation connected the suite to the
**production catalog server**. It created and dropped throwaway databases there
rather than touching ``lto_archive``, and fixture cleanup was observed to leak
stray databases, but the connection itself was to production. That is not a
risk worth carrying for the convenience of typing one command.

The rules, all fail-closed
--------------------------
1. **Explicit opt-in only.** PostgreSQL tests read :data:`TEST_DSN_ENV`
   (``LTO_TEST_PG_DSN``). Ambient ``PG*`` variables are never used to build a
   test connection. Unset means "PostgreSQL tests do not run" — a legitimate
   configuration, not an unsafe one, so it skips.
2. **An unsafe DSN never skips — it FAILS.** Once an operator has said "run
   PostgreSQL tests against this", pointing at the wrong server is an error to
   surface loudly, not to quietly step around.
3. **Refuse anything that looks like production or a normal local install:**
   the default port 5432, a non-loopback host, or a DSN naming a production
   database.
4. **Refuse a server that hosts the production catalog.** If ``lto_archive``
   exists on the target, it *is* the production server, whatever the port says.
   This is the check that cannot be talked around.
5. **Every test database carries this run's marker** (:data:`RUN_PREFIX`), and
   cleanup drops **only** databases carrying it — never a name it did not
   create.

Running PostgreSQL tests safely
-------------------------------
```powershell
docker run -d --name lto_pg_test `
  -e POSTGRES_DB=postgres -e POSTGRES_USER=lto -e POSTGRES_PASSWORD=<pw> `
  -p 127.0.0.1:15432:5432 --tmpfs /var/lib/postgresql/data:rw,size=2g `
  --shm-size=1g postgres:17

$env:LTO_TEST_PG_DSN = "postgresql://lto:<pw>@127.0.0.1:15432/postgres"
$env:LTO_PG_SEALED_BATCH_IT = "1"      # optional opt-in suite
python -m pytest tests/ -q
docker rm -f lto_pg_test                # tmpfs: everything vanishes with it
```
"""
import os
import re
import uuid

#: The ONLY environment variable that may configure a test connection.
TEST_DSN_ENV = "LTO_TEST_PG_DSN"

#: Databases that must never be the target of a test connection or fixture.
PRODUCTION_DATABASE_NAMES = frozenset({"lto_archive", "lto_archive_prod"})

#: A server hosting any of these is the production catalog server, whatever
#: host/port it is reached on.
PRODUCTION_MARKER_DATABASES = frozenset({"lto_archive"})

#: The standard PostgreSQL port. A test server must NOT use it: on this
#: workstation it is the production container, and on a developer machine it is
#: whatever PostgreSQL they installed for real work.
FORBIDDEN_PORTS = frozenset({5432})

#: Only loopback. A remote host cannot be shown to be disposable.
ALLOWED_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

#: Unique per pytest process. Every database this run creates carries it, and
#: cleanup refuses to drop anything that does not.
RUN_ID = uuid.uuid4().hex[:10]
RUN_PREFIX = f"ltotest_{RUN_ID}_"

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_]+$")


class UnsafeTestDatabase(RuntimeError):
    """The configured PostgreSQL target is not provably disposable.

    Raised — never swallowed, never downgraded to a skip — whenever a test
    would otherwise connect somewhere it has not been proven safe to write.
    """


def _parse_dsn(dsn):
    """Extract ``(host, port, dbname)`` from a DSN without connecting."""
    try:
        from psycopg.conninfo import conninfo_to_dict
    except ImportError:                       # pragma: no cover
        raise UnsafeTestDatabase(
            "psycopg is required to validate the test DSN; refusing to guess "
            "whether the target is safe.")
    try:
        parsed = conninfo_to_dict(dsn)
    except Exception as exc:
        raise UnsafeTestDatabase(f"{TEST_DSN_ENV} is not a valid DSN: {exc}")
    host = (parsed.get("host") or "").strip()
    port = str(parsed.get("port") or "").strip()
    dbname = (parsed.get("dbname") or "").strip()
    return host, port, dbname


def assert_safe_dsn(dsn):
    """Raise :class:`UnsafeTestDatabase` unless ``dsn`` is provably disposable.

    Static checks only — no connection is made, so this is safe to call before
    anything has been contacted.
    """
    if not dsn or not dsn.strip():
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} is empty. PostgreSQL tests require an explicit "
            "disposable server; implicit defaults are refused.")

    host, port, dbname = _parse_dsn(dsn)

    if not host:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} does not name a host. An implicit host would "
            "resolve to localhost, where the production catalog listens.")
    if host not in ALLOWED_HOSTS:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} points at host {host!r}. Only a loopback address "
            f"is accepted ({sorted(ALLOWED_HOSTS)}): a remote server cannot be "
            "shown to be disposable.")
    if not port:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} does not name a port. The implicit default is "
            "5432, which is the production container on this host.")
    if int(port) in FORBIDDEN_PORTS:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} uses port {port}, the standard PostgreSQL port. "
            "On this workstation that is the production catalog container, and "
            "on a developer machine it is a real local install. Start a "
            "throwaway server on another port (15432 works).")
    if not dbname:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} does not name a database. The implicit default is "
            "'lto_archive' — the production catalog.")
    if dbname in PRODUCTION_DATABASE_NAMES:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} names database {dbname!r}, which is a production "
            "catalog. Point the maintenance connection at 'postgres'.")
    return host, int(port), dbname


def assert_server_is_not_production(dsn):
    """Connect read-only and refuse a server that hosts the production catalog.

    The decisive check. Port and host are conventions an operator can get
    wrong; the presence of ``lto_archive`` is what the production server
    actually *is*. Runs one ``SELECT`` against ``pg_database`` and writes
    nothing.
    """
    import psycopg

    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        rows = conn.execute(
            "SELECT datname FROM pg_database WHERE datname = ANY(%s)",
            (sorted(PRODUCTION_MARKER_DATABASES),),
        ).fetchall()
        found = sorted(row[0] for row in rows)
    if found:
        raise UnsafeTestDatabase(
            f"the server behind {TEST_DSN_ENV} hosts {found} — it is the "
            "PRODUCTION catalog server. Refusing to create or drop test "
            "databases on it. Start a disposable server instead.")
    return True


def configured_dsn():
    """The validated maintenance DSN, or ``None`` when tests are not enabled.

    ``None`` means the operator has not opted in — PostgreSQL tests skip, which
    is correct and safe. A DSN that IS set but unsafe raises instead.
    """
    dsn = os.environ.get(TEST_DSN_ENV)
    if dsn is None:
        return None
    assert_safe_dsn(dsn)
    return dsn


def pg_available():
    """True when a validated, provably-disposable server is reachable.

    Never returns False to hide an unsafe configuration: an unsafe DSN raises.
    False means only "not configured" or "configured safely but unreachable".
    """
    dsn = configured_dsn()
    if dsn is None:
        return False
    try:
        import psycopg
    except ImportError:
        return False
    try:
        with psycopg.connect(dsn, connect_timeout=3):
            pass
    except Exception:
        return False
    # Reachable — now prove it is not production. This one MUST raise.
    assert_server_is_not_production(dsn)
    return True


SKIP_REASON = (
    f"set {TEST_DSN_ENV} to a disposable PostgreSQL server to run these tests "
    "(see tests/pg_test_guard.py for the exact docker command)")


def test_database_name(tag="db"):
    """A database name carrying this run's marker."""
    clean = re.sub(r"[^A-Za-z0-9_]", "_", str(tag))[:24]
    name = f"{RUN_PREFIX}{clean}_{uuid.uuid4().hex[:8]}"
    if not _SAFE_NAME.match(name):           # pragma: no cover - defensive
        raise UnsafeTestDatabase(f"generated an unsafe database name: {name!r}")
    return name


def assert_created_by_this_run(dbname):
    """Refuse to drop a database this run did not create."""
    if not str(dbname).startswith(RUN_PREFIX):
        raise UnsafeTestDatabase(
            f"refusing to drop database {dbname!r}: it does not carry this "
            f"run's marker {RUN_PREFIX!r}, so this run did not create it.")
    return True


def dsn_for(dbname):
    """A DSN for one of this run's databases, derived from the validated one."""
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    assert_created_by_this_run(dbname)
    parsed = conninfo_to_dict(configured_dsn())
    parsed["dbname"] = dbname
    return make_conninfo("", **parsed)


def create_test_database(tag="db"):
    """Create a marked database and return ``(name, dsn)``."""
    import psycopg

    dsn = configured_dsn()
    if dsn is None:
        raise UnsafeTestDatabase(
            f"{TEST_DSN_ENV} is not set; cannot create a test database.")
    name = test_database_name(tag)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    return name, dsn_for(name)


def drop_test_database(dbname):
    """Drop one of THIS run's databases. Refuses anything else."""
    import psycopg

    assert_created_by_this_run(dbname)
    dsn = configured_dsn()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()", (dbname,))
        conn.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
    return True


def drop_all_run_databases():
    """Sweep away anything this run leaked. Scoped to the run marker.

    Leaked fixture databases are how a shared server accumulates junk; this is
    the backstop, and it can only ever touch names carrying this run's prefix.
    """
    import psycopg

    dsn = configured_dsn()
    if dsn is None:
        return []
    dropped = []
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            names = [row[0] for row in conn.execute(
                "SELECT datname FROM pg_database WHERE datname LIKE %s",
                (RUN_PREFIX + "%",)).fetchall()]
    except Exception:
        return []
    for name in names:
        try:
            drop_test_database(name)
            dropped.append(name)
        except Exception:
            pass
    return dropped


__all__ = [
    "ALLOWED_HOSTS", "FORBIDDEN_PORTS", "PRODUCTION_DATABASE_NAMES",
    "PRODUCTION_MARKER_DATABASES", "RUN_ID", "RUN_PREFIX", "SKIP_REASON",
    "TEST_DSN_ENV", "UnsafeTestDatabase", "assert_created_by_this_run",
    "assert_safe_dsn", "assert_server_is_not_production", "configured_dsn",
    "create_test_database", "drop_all_run_databases", "drop_test_database",
    "dsn_for", "pg_available", "test_database_name",
]
