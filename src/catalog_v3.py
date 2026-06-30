"""Catalog-browser schema helpers for lazy inspector access."""
import os
import re
import sqlite3


CATALOG_SCHEMA_VERSION = 3
CATALOG_MIGRATION_NAME = "v3_catalog_browser"

_DRIVE_RE = re.compile(r"^[A-Za-z]:")


def normalize_catalog_path(path):
    """Normalize a stored/source path for catalog browsing keys."""
    value = str(path or "").replace("\\", "/").strip()
    value = re.sub(r"/+", "/", value)
    if value != "/" and value.endswith("/"):
        value = value.rstrip("/")
    return value


def _split_path(path):
    value = normalize_catalog_path(path)
    if not value:
        return []
    if _DRIVE_RE.match(value):
        drive = value[:2]
        rest = value[2:].strip("/")
        return [drive] + ([p for p in rest.split("/") if p] if rest else [])
    if value.startswith("/"):
        rest = value.strip("/")
        return ["/"] + ([p for p in rest.split("/") if p] if rest else [])
    return [p for p in value.split("/") if p]


def catalog_file_name(stored_path=None, original_path=None):
    """Return the display/search filename used by the v3 catalog indexes."""
    for candidate in (stored_path, original_path):
        parts = _split_path(candidate)
        if parts:
            leaf = parts[-1]
            if leaf not in ("/",) and not _DRIVE_RE.match(leaf):
                return leaf
    return ""


def catalog_directory_chain(file_path):
    """Return directory rows as (normalized_path, parent_path, name)."""
    parts = _split_path(file_path)
    if len(parts) <= 1:
        return []

    chain = []
    current = None
    for index, part in enumerate(parts[:-1]):
        if index == 0:
            normalized = part
        elif current == "/":
            normalized = "/" + part
        else:
            normalized = current + "/" + part
        parent = current
        name = normalized if parent is None else part
        chain.append((normalized, parent, name))
        current = normalized
    return chain


def ensure_catalog_schema(conn):
    """Create the v3 catalog browsing tables, columns, indexes and triggers."""
    conn.execute("""CREATE TABLE IF NOT EXISTS catalog_directories(
        directory_id INTEGER PRIMARY KEY AUTOINCREMENT,
        tape_label TEXT NOT NULL
            REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE CASCADE,
        parent_id INTEGER REFERENCES catalog_directories(directory_id)
            ON DELETE CASCADE,
        name TEXT NOT NULL,
        normalized_path TEXT NOT NULL,
        UNIQUE(tape_label, normalized_path)
    )""")

    columns = {row[1] for row in conn.execute("PRAGMA table_info(files_index)")}
    for name, sql_type in (
            ("directory_id", "INTEGER REFERENCES catalog_directories(directory_id)"),
            ("catalog_name", "TEXT"),
            ("catalog_backup_date", "DATETIME")):
        if name not in columns:
            conn.execute(f"ALTER TABLE files_index ADD COLUMN {name} {sql_type}")

    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_catalog_dirs_parent
            ON catalog_directories(tape_label, parent_id, name, directory_id);
        CREATE INDEX IF NOT EXISTS idx_catalog_dirs_parent_id
            ON catalog_directories(parent_id, name, directory_id);
        CREATE INDEX IF NOT EXISTS idx_files_directory_name
            ON files_index(directory_id, catalog_name, file_id);
        CREATE INDEX IF NOT EXISTS idx_files_directory_size
            ON files_index(directory_id, file_size_bytes, catalog_name, file_id);
        CREATE INDEX IF NOT EXISTS idx_files_directory_date
            ON files_index(directory_id, catalog_backup_date, catalog_name, file_id);
        CREATE INDEX IF NOT EXISTS idx_files_source_host
            ON files_index(source_host, tape_label, original_path);
    """)
    _ensure_fts(conn)
    _ensure_fts_triggers(conn)


def _ensure_fts(conn):
    try:
        conn.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS files_index_fts
            USING fts5(
                catalog_name,
                original_path,
                content='files_index',
                content_rowid='file_id',
                tokenize='trigram'
            )""")
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            "[DB] SQLite FTS5 trigram support is required for catalog v3 search."
        ) from exc


def _ensure_fts_triggers(conn):
    conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS files_index_catalog_fts_ai
        AFTER INSERT ON files_index
        WHEN new.catalog_name IS NOT NULL
        BEGIN
            INSERT INTO files_index_fts(rowid, catalog_name, original_path)
            VALUES (new.file_id, new.catalog_name, new.original_path);
        END;

        CREATE TRIGGER IF NOT EXISTS files_index_catalog_fts_ad
        AFTER DELETE ON files_index
        WHEN old.catalog_name IS NOT NULL
        BEGIN
            INSERT INTO files_index_fts(files_index_fts, rowid, catalog_name, original_path)
            VALUES ('delete', old.file_id, old.catalog_name, old.original_path);
        END;

        CREATE TRIGGER IF NOT EXISTS files_index_catalog_fts_au
        AFTER UPDATE OF catalog_name, original_path ON files_index
        WHEN old.catalog_name IS NOT NULL OR new.catalog_name IS NOT NULL
        BEGIN
            INSERT INTO files_index_fts(files_index_fts, rowid, catalog_name, original_path)
            VALUES ('delete', old.file_id, old.catalog_name, old.original_path);
            INSERT INTO files_index_fts(rowid, catalog_name, original_path)
            VALUES (new.file_id, new.catalog_name, new.original_path);
        END;
    """)


def catalog_v3_available(conn):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='catalog_directories'"
    ).fetchone()
    if not row:
        return False
    columns = {r[1] for r in conn.execute("PRAGMA table_info(files_index)")}
    return {"directory_id", "catalog_name", "catalog_backup_date"}.issubset(columns)


def ensure_directory_chain(conn, tape_label, file_path, cache=None):
    """Insert directory ancestors for file_path and return the leaf directory id."""
    cache = cache if cache is not None else {}
    parent_id = None
    for normalized_path, parent_path, name in catalog_directory_chain(file_path):
        key = (tape_label, normalized_path)
        cached = cache.get(key)
        if cached is not None:
            parent_id = cached
            continue
        if parent_path is not None:
            parent_id = cache.get((tape_label, parent_path), parent_id)
        conn.execute(
            """INSERT INTO catalog_directories
               (tape_label, parent_id, name, normalized_path)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(tape_label, normalized_path) DO NOTHING""",
            (tape_label, parent_id, name, normalized_path),
        )
        row = conn.execute(
            """SELECT directory_id FROM catalog_directories
               WHERE tape_label=? AND normalized_path=?""",
            (tape_label, normalized_path),
        ).fetchone()
        directory_id = row[0]
        cache[key] = directory_id
        parent_id = directory_id
    return parent_id


def catalog_values_for_file(conn, row, cache=None):
    """Return (directory_id, catalog_name, catalog_backup_date) for a file row."""
    tape_label = row["tape_label"]
    original_path = row["original_path"]
    stored_path = row["stored_path"]
    directory_id = ensure_directory_chain(
        conn, tape_label, original_path or stored_path, cache)
    return (
        directory_id,
        catalog_file_name(stored_path, original_path),
        row["backup_date"] or row["run_started_at"],
    )


def quote_identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def format_bytes(value):
    if value is None:
        return None
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    n = float(value)
    for unit in units:
        if abs(n) < 1024 or unit == units[-1]:
            return f"{n:.1f} {unit}"
        n /= 1024


def free_space_report(db_path):
    db_path = os.path.abspath(db_path)
    size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    free = None
    try:
        free = __import__("shutil").disk_usage(os.path.dirname(db_path)).free
    except OSError:
        pass
    required = size * 3 + 1024 ** 3
    return {
        "database_size_bytes": size,
        "database_size": format_bytes(size),
        "estimated_required_free_bytes": required,
        "estimated_required_free": format_bytes(required),
        "available_free_bytes": free,
        "available_free": format_bytes(free) if free is not None else None,
        "has_required_free_space": (free is None or free >= required),
    }
