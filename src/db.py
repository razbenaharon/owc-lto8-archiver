"""PostgreSQL database factory and shared catalog identity helpers."""
import hashlib


class DatabaseManager:
    """Removed SQLite manager placeholder.

    The archive catalog is PostgreSQL-only. Use ``create_database_manager(cfg)``
    so connection handling and error messages stay consistent.
    """

    def __new__(cls, *args, **kwargs):
        raise RuntimeError(
            "[DB] SQLite DatabaseManager has been removed. Configure PostgreSQL "
            "in [DATABASE] and call create_database_manager(cfg)."
        )


def _fmt_ts(value, width=19):
    """Render a timestamp for fixed-width display.

    PostgreSQL returns datetime/date objects where the legacy SQLite catalog
    returned ISO strings; both forms (and None) must display safely.
    """
    if value is None:
        return ''
    if hasattr(value, 'isoformat'):
        value = value.isoformat(sep=' ') if hasattr(value, 'time') else value.isoformat()
    return str(value)[:width]


def _derived_file_name(stored_path, original_path=None):
    """Derive a display/restore name without storing legacy file_name."""
    path = str(stored_path or original_path or "").replace("\\", "/")
    return path.rstrip("/").rsplit("/", 1)[-1]


def _short_source_host(value):
    value = (value or "").strip()
    if not value:
        return "local"
    return value.split(".", 1)[0]


def _file_record_key(original_path, tape_label, local_session_id=None,
                     local_chunk_index=None, source_host="so02"):
    """Stable compact key for NULL-safe file-record upserts."""
    digest = hashlib.sha256()
    for value in (_short_source_host(source_host), original_path or "",
                  tape_label or "",
                  -1 if local_session_id is None else local_session_id,
                  -1 if local_chunk_index is None else local_chunk_index):
        raw = str(value).encode("utf-8", errors="surrogatepass")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.digest()


def _apply_canonical_remote_paths(metadata, manifest_rows):
    """Attach durable remote SOURCE paths to staging-produced metadata."""
    remote_by_local = {}
    for row in manifest_rows:
        local_rel = row["local_rel_path"]
        remote_path = row["remote_path"]
        if local_rel and remote_path:
            key = str(local_rel).replace("\\", "/")
            previous = remote_by_local.setdefault(key, remote_path)
            if previous != remote_path:
                raise RuntimeError(
                    "[DB] Ambiguous canonical source mapping for staged path "
                    f"'{key}': '{previous}' and '{remote_path}'"
                )
    replaced = 0
    for item in metadata:
        stored = str(item.get("stored_path") or "").replace("\\", "/")
        canonical = remote_by_local.get(stored)
        if canonical:
            item["canonical_source_path"] = canonical
            item["original_path"] = canonical
            replaced += 1
    return replaced


def create_database_manager(cfg):
    """Return the PostgreSQL database manager."""
    from .pg_db import PgDatabaseManager

    try:
        return PgDatabaseManager(cfg.db_dsn)
    except Exception as exc:
        if _is_postgres_connection_error(exc):
            raise RuntimeError(_postgres_connection_error_message(cfg)) from exc
        raise


def _is_postgres_connection_error(exc):
    try:
        import psycopg
        from psycopg_pool import PoolTimeout
    except ImportError:
        return False
    return isinstance(exc, (PoolTimeout, psycopg.OperationalError))


def _postgres_connection_error_message(cfg):
    host = getattr(cfg, "pg_host", "localhost")
    port = getattr(cfg, "pg_port", "5432")
    dbname = getattr(cfg, "pg_dbname", "lto_archive")
    user = getattr(cfg, "pg_user", "lto")
    lines = [
        f"[DB] Could not connect to PostgreSQL at {host}:{port} "
        f"(database '{dbname}', user '{user}').",
        "[DB] Start the local database with: docker compose up -d db",
        "[DB] Then check it with: docker ps --filter name=lto_pg",
        "[DB] If the container just started, wait until it is healthy; "
        "PostgreSQL may be replaying WAL after an unclean shutdown.",
        "[DB] Recovery details are visible with: docker logs --tail 80 lto_pg",
    ]
    if not getattr(cfg, "pg_password", ""):
        lines.append(
            "[DB] No PostgreSQL password is configured. Copy .env.example to "
            ".env or set PGPASSWORD; the local Docker default is "
            "'change_me_local'."
        )
    return "\n".join(lines)
