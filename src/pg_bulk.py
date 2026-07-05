"""High-throughput PostgreSQL ingest helpers."""
import os

try:
    import psycopg
    from psycopg.conninfo import make_conninfo
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - exercised only when PG extras missing
    psycopg = None
    make_conninfo = None
    ConnectionPool = None


def require_psycopg():
    if psycopg is None or ConnectionPool is None:
        raise RuntimeError(
            "[PG] psycopg 3 is not installed. Run "
            "`python -m pip install -r requirements.txt`."
        )


def build_conninfo(
        host=None, port=None, dbname=None, user=None, password=None,
        sslmode=None, dsn=None):
    """Build a psycopg conninfo string from explicit values or env defaults."""
    if dsn:
        return dsn
    host = host or os.environ.get("PGHOST", "localhost")
    port = port or os.environ.get("PGPORT", "5432")
    dbname = dbname or os.environ.get("PGDATABASE", "lto_archive")
    user = user or os.environ.get("PGUSER", "lto")
    password = password if password is not None else os.environ.get("PGPASSWORD")
    sslmode = sslmode or os.environ.get("PGSSLMODE", "prefer")

    return make_conninfo(
        "",
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password or None,
        sslmode=sslmode,
    )


def _conninfo_with_timeout(conninfo, connect_timeout):
    if not connect_timeout:
        return conninfo
    return make_conninfo(conninfo or build_conninfo(),
                         connect_timeout=int(connect_timeout))


def make_pool(conninfo=None, *, min_size=2, max_size=8, pool_timeout=5,
              reconnect_timeout=5, connect_timeout=5, **kwargs):
    require_psycopg()
    ready_conninfo = _conninfo_with_timeout(conninfo, connect_timeout)
    with psycopg.connect(ready_conninfo, autocommit=False, **kwargs):
        pass
    pool = ConnectionPool(
        conninfo=ready_conninfo,
        min_size=min_size,
        max_size=max_size,
        kwargs={"autocommit": False, **kwargs},
        open=False,
        timeout=pool_timeout,
        reconnect_timeout=reconnect_timeout,
        # Validate connections on checkout so a Docker DB restart mid-run
        # surfaces as a fresh connection, not a stale-socket OperationalError.
        check=ConnectionPool.check_connection,
    )
    try:
        pool.open(wait=True, timeout=pool_timeout)
    except Exception:
        pool.close()
        raise
    return pool


def copy_rows(cur, table, columns, rows):
    """Stream rows through PostgreSQL COPY using psycopg's row writer."""
    col_sql = ", ".join(columns)
    with cur.copy(f"COPY {table} ({col_sql}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def execute_sql_file(conn, path):
    with open(path, "r", encoding="utf-8") as handle:
        sql = handle.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
