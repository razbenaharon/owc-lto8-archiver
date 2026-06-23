"""DatabaseManager and catalog normalization helpers."""
import os
import re
import sys
import time
import queue
import signal
import shutil
import hashlib
import zipfile
import sqlite3
import threading
import configparser
import subprocess
import tempfile
import shlex
import posixpath
import atexit
from datetime import datetime
from collections import defaultdict

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .constants import CONFIG_FILE, DB_UPSERT_BATCH_SIZE
from .paths import _clean_config_path
from .runtime import CANCEL


def _hash_to_blob(value):
    """Return a compact 32-byte SHA-256 value, or None when no hash exists."""
    if not value:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        return value if len(value) == 32 else None
    try:
        raw = bytes.fromhex(str(value))
    except ValueError:
        return None
    return raw if len(raw) == 32 else None


def _derived_file_name(stored_path, original_path=None):
    """Derive a display/restore name without storing it once per DB row."""
    path = stored_path or original_path or ''
    return os.path.basename(str(path).replace('/', os.sep))


def _file_record_key(original_path, tape_label, local_session_id=None,
                     local_chunk_index=None):
    """Stable compact key for NULL-safe file-record upserts."""
    digest = hashlib.sha256()
    for value in (original_path or '', tape_label or '',
                  -1 if local_session_id is None else local_session_id,
                  -1 if local_chunk_index is None else local_chunk_index):
        raw = str(value).encode('utf-8', errors='surrogatepass')
        digest.update(len(raw).to_bytes(8, 'big'))
        digest.update(raw)
    return digest.digest()


def _apply_canonical_remote_paths(metadata, manifest_rows):
    """Replace temporary staging paths with durable remote source paths."""
    remote_by_local = {}
    for row in manifest_rows:
        local_rel = row['local_rel_path']
        remote_path = row['remote_path']
        if local_rel and remote_path:
            remote_by_local[str(local_rel).replace('\\', '/')] = remote_path
    replaced = 0
    for item in metadata:
        stored = str(item.get('stored_path') or '').replace('\\', '/')
        canonical = remote_by_local.get(stored)
        if canonical:
            item['original_path'] = canonical
            replaced += 1
    return replaced


class DatabaseManager:
    def __init__(self, db_path):
        db_path = _clean_config_path(db_path)
        db_dir = os.path.dirname(os.path.abspath(db_path))
        try:
            os.makedirs(db_dir, exist_ok=True)
            # check_same_thread=False: the streaming pipeline updates the DB from
            # both the producer (fetch/pack) thread and the consumer (tape) thread.
            # self.lock serialises every write so the shared connection stays safe.
            self.conn = sqlite3.connect(
                db_path,
                check_same_thread=False,
                timeout=60,
            )
            self.conn.execute("PRAGMA busy_timeout = 60000")
            self.conn.execute("PRAGMA foreign_keys = ON")
        except (OSError, sqlite3.Error) as e:
            raise RuntimeError(
                f"[DB] Cannot open database at: {db_path}\n"
                f"     Directory: {db_dir}\n"
                f"     Reason: {e}\n"
                f"     Edit {CONFIG_FILE} and set [PATHS] db_path to a writable location."
            ) from e
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._bundle_cache = {}
        self._init_schema()
        self._init_remote_schema()
        self._init_local_schema()

    def _require_updated(self, cur, message):
        """Raise if the preceding UPDATE/DELETE matched no rows (target missing).

        sqlite3 reports an accurate rowcount for UPDATE/DELETE, so a value of 0
        means the WHERE clause matched nothing — i.e. the row we expected to
        change does not exist. Callers pass a '[DB] ... not found' message."""
        if cur.rowcount == 0:
            raise RuntimeError(message)

    def _commit_write(self, operation, description, attempts=5):
        """Run one write transaction, retrying transient SQLite lock errors."""
        for attempt in range(1, attempts + 1):
            with self.lock:
                try:
                    result = operation()
                    self.conn.commit()
                    return result
                except sqlite3.OperationalError as e:
                    self.conn.rollback()
                    locked = 'locked' in str(e).lower() or 'busy' in str(e).lower()
                    if not locked or attempt == attempts:
                        raise
            wait_s = min(5, attempt)
            print(f"[DB] Database busy during {description}; retrying in "
                  f"{wait_s}s ({attempt}/{attempts})...")
            time.sleep(wait_s)

    def _update_manifest_batches(self, sql, params, description,
                                 batch_size=5000):
        """Apply large manifest updates without one commit per source file."""
        params = list(params)
        for offset in range(0, len(params), batch_size):
            batch = params[offset:offset + batch_size]
            self._commit_write(
                lambda batch=batch: self.conn.executemany(sql, batch),
                description,
            )

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tapes (
                tape_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_label   TEXT    UNIQUE NOT NULL,
                date_formatted DATETIME,
                total_capacity INTEGER,
                used_space     INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS files_index (
                file_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name       TEXT,
                original_path   TEXT,
                file_size_bytes INTEGER,
                file_hash       TEXT,
                backup_date     DATETIME,
                tape_label      TEXT,
                is_packed       BOOLEAN,
                container_name  TEXT,
                stored_path     TEXT,
                FOREIGN KEY (tape_label) REFERENCES tapes(volume_label)
            );
            CREATE TABLE IF NOT EXISTS archive_bundles (
                bundle_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                tape_label  TEXT NOT NULL
                    REFERENCES tapes(volume_label)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                tape_path   TEXT NOT NULL,
                UNIQUE(tape_label, tape_path)
            );
        """)
        self.conn.commit()
        # Migrate existing DB: add used_space if missing
        try:
            self.conn.execute("ALTER TABLE tapes ADD COLUMN used_space INTEGER DEFAULT 0")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
        for column, col_type in (
            ('local_session_id', 'INTEGER'),
            ('local_chunk_index', 'INTEGER'),
            ('bundle_id', 'INTEGER REFERENCES archive_bundles(bundle_id)'),
            ('file_hash_blob', 'BLOB'),
            ('record_key', 'BLOB'),
        ):
            try:
                self.conn.execute(f"ALTER TABLE files_index ADD COLUMN {column} {col_type}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
        # The expression index is the permanent form of the live rescue index.
        # It accelerates legacy rows whose record_key has not yet been populated.
        # New rows use the compact partial-unique record_key index for SQL upserts.
        self.conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_files_dedup_expr
                ON files_index(
                    COALESCE(original_path, ''),
                    COALESCE(tape_label, ''),
                    COALESCE(local_session_id, -1),
                    COALESCE(local_chunk_index, -1)
                );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_files_record_key
                ON files_index(record_key)
                WHERE record_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_bundle_id
                ON files_index(bundle_id);
        """)
        self.conn.commit()

    def _init_local_schema(self):
        """Create local multi-tape session tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS local_sessions (
                session_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                session_label TEXT    NOT NULL,
                source_dir    TEXT    NOT NULL,
                total_chunks  INTEGER NOT NULL,
                backup_mode   TEXT NOT NULL DEFAULT 'auto'
                    CHECK(backup_mode IN ('auto','direct','pack')),
                created_at    DATETIME NOT NULL,
                completed_at  DATETIME,
                status        TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','completed','abandoned'))
            );
            CREATE TABLE IF NOT EXISTS local_chunks_manifest (
                manifest_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      INTEGER NOT NULL
                    REFERENCES local_sessions(session_id),
                chunk_index     INTEGER NOT NULL,
                top_level_dir   TEXT    NOT NULL,
                dir_size_bytes  INTEGER NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','staged','backed_up')),
                tape_label      TEXT,
                started_at      DATETIME,
                completed_at    DATETIME,
                updated_at      DATETIME
            );
            CREATE INDEX IF NOT EXISTS idx_local_manifest_session_chunk
                ON local_chunks_manifest(session_id, chunk_index);
            CREATE INDEX IF NOT EXISTS idx_files_local_session_chunk
                ON files_index(local_session_id, local_chunk_index, tape_label);
        """)
        self.conn.commit()
        try:
            self.conn.execute(
                """ALTER TABLE local_sessions
                   ADD COLUMN backup_mode TEXT NOT NULL DEFAULT 'auto'
                   CHECK(backup_mode IN ('auto','direct','pack'))"""
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists

    def _init_remote_schema(self):
        """Create remote_sessions and remote_manifest tables if they don't exist.
        Safe to call on existing databases — uses CREATE TABLE IF NOT EXISTS."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS remote_sessions (
                session_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                session_label TEXT    NOT NULL,
                remote_host   TEXT    NOT NULL,
                remote_user   TEXT    NOT NULL,
                remote_path   TEXT    NOT NULL,
                tape_label    TEXT    NOT NULL,
                staging_dir   TEXT    NOT NULL,
                total_files   INTEGER DEFAULT 0,
                total_bytes   INTEGER DEFAULT 0,
                chunk_count   INTEGER DEFAULT 0,
                created_at    DATETIME NOT NULL,
                completed_at  DATETIME,
                status        TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','completed','abandoned'))
            );
            CREATE TABLE IF NOT EXISTS remote_manifest (
                manifest_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      INTEGER NOT NULL
                    REFERENCES remote_sessions(session_id),
                chunk_index     INTEGER NOT NULL,
                remote_path     TEXT    NOT NULL,
                file_name       TEXT    NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                local_rel_path  TEXT,
                status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN (
                        'pending','fetching','fetched','packing','packed',
                        'backing','backed','done','source_missing',
                        'fetch_failed','backup_failed'
                    )),
                chunk_status    TEXT NOT NULL DEFAULT 'pending'
                    CHECK(chunk_status IN (
                        'pending','fetching','packing','backing','done',
                        'fetch_failed','backup_failed'
                    )),
                error_msg       TEXT,
                updated_at      DATETIME
            );
            CREATE INDEX IF NOT EXISTS idx_remote_manifest_session_chunk
                ON remote_manifest(session_id, chunk_index);
        """)
        self.conn.commit()

        self._migrate_remote_manifest_source_missing()

    def _migrate_remote_manifest_source_missing(self):
        """Allow source_missing in databases created by older versions.

        SQLite cannot alter a CHECK constraint in place, so rebuild the table
        transactionally while preserving manifest IDs and all session state.
        If an older migration attempt was interrupted after the table rename,
        resume from the preserved remote_manifest_legacy table.
        """
        row = self.conn.execute(
            """SELECT sql FROM sqlite_master
               WHERE type = 'table' AND name = 'remote_manifest'"""
        ).fetchone()
        legacy_exists = self.conn.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type = 'table' AND name = 'remote_manifest_legacy'"""
        ).fetchone() is not None
        if not row:
            return
        needs_schema_migration = 'source_missing' not in (row[0] or '')
        if not needs_schema_migration and not legacy_exists:
            return

        action = "Recovering" if legacy_exists else "Migrating"
        print(f"[DB] {action} remote manifest for source-missing file support...")
        with self.lock:
            try:
                # Explicit BEGIN is required before DDL; otherwise an interrupt
                # between ALTER TABLE and the first INSERT can leave an empty new
                # table beside the still-intact legacy table.
                self.conn.execute("BEGIN IMMEDIATE")
                if not legacy_exists:
                    self.conn.execute(
                        "ALTER TABLE remote_manifest "
                        "RENAME TO remote_manifest_legacy"
                    )
                    self.conn.execute("""
                        CREATE TABLE remote_manifest (
                            manifest_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                            session_id      INTEGER NOT NULL
                                REFERENCES remote_sessions(session_id),
                            chunk_index     INTEGER NOT NULL,
                            remote_path     TEXT    NOT NULL,
                            file_name       TEXT    NOT NULL,
                            file_size_bytes INTEGER NOT NULL,
                            local_rel_path  TEXT,
                            status          TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN (
                                    'pending','fetching','fetched','packing','packed',
                                    'backing','backed','done','source_missing',
                                    'fetch_failed','backup_failed'
                                )),
                            chunk_status    TEXT NOT NULL DEFAULT 'pending'
                                CHECK(chunk_status IN (
                                    'pending','fetching','packing','backing','done',
                                    'fetch_failed','backup_failed'
                                )),
                            error_msg       TEXT,
                            updated_at      DATETIME
                        )
                    """)

                legacy_count = self.conn.execute(
                    "SELECT COUNT(*) FROM remote_manifest_legacy"
                ).fetchone()[0]
                self.conn.execute("""
                    INSERT OR IGNORE INTO remote_manifest (
                        manifest_id, session_id, chunk_index, remote_path,
                        file_name, file_size_bytes, local_rel_path, status,
                        chunk_status, error_msg, updated_at
                    )
                    SELECT manifest_id, session_id, chunk_index, remote_path,
                           file_name, file_size_bytes, local_rel_path, status,
                           chunk_status, error_msg, updated_at
                    FROM remote_manifest_legacy
                """)
                restored_count = self.conn.execute(
                    "SELECT COUNT(*) FROM remote_manifest"
                ).fetchone()[0]
                if restored_count != legacy_count:
                    raise RuntimeError(
                        "[DB] Remote manifest migration count mismatch: "
                        f"legacy={legacy_count}, restored={restored_count}"
                    )

                self.conn.execute(
                    "DROP INDEX IF EXISTS idx_remote_manifest_session_chunk"
                )
                self.conn.execute("DROP TABLE remote_manifest_legacy")
                self.conn.execute("""
                    CREATE INDEX idx_remote_manifest_session_chunk
                        ON remote_manifest(session_id, chunk_index)
                """)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
        print("[DB] Remote manifest migration complete.")

    def create_remote_session(self, session_label, remote_host, remote_user,
                               remote_path, tape_label, staging_dir):
        with self.lock:
            cur = self.conn.execute(
                """INSERT INTO remote_sessions
                   (session_label, remote_host, remote_user, remote_path,
                    tape_label, staging_dir, created_at, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'active')""",
                (session_label, remote_host, remote_user, remote_path,
                 tape_label, staging_dir, datetime.now().isoformat())
            )
            self.conn.commit()
            return cur.lastrowid

    def update_remote_session(self, session_id, **kwargs):
        if not kwargs:
            return
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [session_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE remote_sessions SET {sets} WHERE session_id = ?", vals
            )
            self._require_updated(cur, f"[DB] Remote session not found: {session_id}")
            self.conn.commit()

    def get_active_remote_session(self, remote_host, remote_path):
        with self.lock:
            return self.conn.execute(
                """SELECT * FROM remote_sessions
                   WHERE remote_host = ? AND remote_path = ? AND status = 'active'
                   ORDER BY session_id DESC LIMIT 1""",
                (remote_host, remote_path)
            ).fetchone()

    def get_remote_session(self, session_id):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM remote_sessions WHERE session_id = ?",
                (session_id,)
            ).fetchone()

    def insert_remote_manifest_batch(self, session_id, rows):
        """rows: list of (chunk_index, remote_path, file_name, file_size_bytes)"""
        with self.lock:
            self.conn.executemany(
                """INSERT INTO remote_manifest
                   (session_id, chunk_index, remote_path, file_name, file_size_bytes,
                    status, chunk_status, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'pending', 'pending', ?)""",
                [(session_id, r[0], r[1], r[2], r[3], datetime.now().isoformat())
                 for r in rows]
            )
            self.conn.commit()

    def get_chunk_files(self, session_id, chunk_index):
        with self.lock:
            return self.conn.execute(
                """SELECT * FROM remote_manifest
                   WHERE session_id = ? AND chunk_index = ?
                   ORDER BY manifest_id""",
                (session_id, chunk_index)
            ).fetchall()

    def update_manifest_row(self, manifest_id, **kwargs):
        kwargs['updated_at'] = datetime.now().isoformat()
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [manifest_id]
        cur = self._commit_write(
            lambda: self.conn.execute(
                f"UPDATE remote_manifest SET {sets} WHERE manifest_id = ?", vals
            ),
            f"manifest row {manifest_id} update",
        )
        self._require_updated(cur, f"[DB] Remote manifest row not found: {manifest_id}")

    def update_manifest_rows_fetching(self, manifest_ids):
        now = datetime.now().isoformat()
        self._update_manifest_batches(
            """UPDATE remote_manifest
               SET status = 'fetching', error_msg = NULL, updated_at = ?
               WHERE manifest_id = ?""",
            ((now, manifest_id) for manifest_id in manifest_ids),
            "manifest fetching-status batch",
        )

    def update_manifest_rows_fetched(self, rows):
        """rows contains (local_rel_path, manifest_id) pairs."""
        now = datetime.now().isoformat()
        self._update_manifest_batches(
            """UPDATE remote_manifest
               SET status = 'fetched', local_rel_path = ?, error_msg = NULL,
                   updated_at = ?
               WHERE manifest_id = ?""",
            ((local_rel_path, now, manifest_id)
             for local_rel_path, manifest_id in rows),
            "manifest fetched-status batch",
        )

    def update_manifest_rows_fetch_failed(self, manifest_ids, error_msg):
        now = datetime.now().isoformat()
        error_msg = (error_msg or '')[:500]
        self._update_manifest_batches(
            """UPDATE remote_manifest
               SET status = 'fetch_failed', error_msg = ?, updated_at = ?
               WHERE manifest_id = ?""",
            ((error_msg, now, manifest_id) for manifest_id in manifest_ids),
            "manifest fetch-failure batch",
        )

    def update_chunk_status(self, session_id, chunk_index, status):
        cur = self._commit_write(
            lambda: self.conn.execute(
                """UPDATE remote_manifest SET chunk_status = ?, updated_at = ?
                   WHERE session_id = ? AND chunk_index = ?""",
                (status, datetime.now().isoformat(), session_id, chunk_index)
            ),
            f"chunk {chunk_index + 1} status update",
        )
        self._require_updated(
            cur,
            f"[DB] Remote chunk not found: session {session_id}, chunk {chunk_index}"
        )

    def get_pending_chunks(self, session_id):
        with self.lock:
            rows = self.conn.execute(
                """SELECT DISTINCT chunk_index FROM remote_manifest
                   WHERE session_id = ? AND chunk_status NOT IN ('done')
                   ORDER BY chunk_index""",
                (session_id,)
            ).fetchall()
        return [r[0] for r in rows]

    def count_chunks(self, session_id):
        with self.lock:
            return self.conn.execute(
                "SELECT COUNT(DISTINCT chunk_index) FROM remote_manifest WHERE session_id = ?",
                (session_id,)
            ).fetchone()[0]

    def create_local_session(self, session_label, source_dir, chunks,
                             backup_mode='auto'):
        """Persist a local multi-tape allocation plan.

        chunks: list of lists containing allocation dicts with name/size_bytes.
        """
        now = datetime.now().isoformat()
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO local_sessions
                       (session_label, source_dir, total_chunks, backup_mode,
                        created_at, status)
                       VALUES (?, ?, ?, ?, ?, 'active')""",
                    (session_label, source_dir, len(chunks), backup_mode, now)
                )
                session_id = cur.lastrowid
                rows = []
                for chunk_index, entries in enumerate(chunks):
                    for entry in entries:
                        rows.append((
                            session_id, chunk_index, entry['name'],
                            entry['size_bytes'], 'pending', now
                        ))
                self.conn.executemany(
                    """INSERT INTO local_chunks_manifest
                       (session_id, chunk_index, top_level_dir, dir_size_bytes,
                        status, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    rows
                )
            return session_id

    def update_local_session(self, session_id, **kwargs):
        if not kwargs:
            return
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [session_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE local_sessions SET {sets} WHERE session_id = ?", vals
            )
            self._require_updated(cur, f"[DB] Local session not found: {session_id}")
            self.conn.commit()

    def get_active_local_session(self, source_dir):
        with self.lock:
            return self.conn.execute(
                """SELECT * FROM local_sessions
                   WHERE source_dir = ? AND status = 'active'
                   ORDER BY session_id DESC LIMIT 1""",
                (source_dir,)
            ).fetchone()

    def get_local_session(self, session_id):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM local_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()

    def get_local_pending_chunks(self, session_id):
        with self.lock:
            rows = self.conn.execute(
                """SELECT chunk_index FROM local_chunks_manifest
                   WHERE session_id = ?
                   GROUP BY chunk_index
                   HAVING SUM(CASE WHEN status != 'backed_up' THEN 1 ELSE 0 END) > 0
                   ORDER BY chunk_index""",
                (session_id,)
            ).fetchall()
        return [r[0] for r in rows]

    def get_local_chunk_entries(self, session_id, chunk_index):
        with self.lock:
            return self.conn.execute(
                """SELECT * FROM local_chunks_manifest
                   WHERE session_id = ? AND chunk_index = ?
                   ORDER BY manifest_id""",
                (session_id, chunk_index)
            ).fetchall()

    def assign_local_chunk_tape(self, session_id, chunk_index, tape_label):
        now = datetime.now().isoformat()
        with self.lock:
            cur = self.conn.execute(
                """UPDATE local_chunks_manifest
                   SET tape_label = COALESCE(tape_label, ?),
                       started_at = COALESCE(started_at, ?),
                       updated_at = ?
                   WHERE session_id = ? AND chunk_index = ?""",
                (tape_label, now, now, session_id, chunk_index)
            )
            self._require_updated(
                cur,
                f"[DB] Local chunk not found: session {session_id}, chunk {chunk_index}"
            )
            self.conn.commit()

    def update_local_chunk_status(self, session_id, chunk_index, status):
        kwargs = {
            'status': status,
            'updated_at': datetime.now().isoformat(),
        }
        if status == 'backed_up':
            kwargs['completed_at'] = datetime.now().isoformat()
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [session_id, chunk_index]
        with self.lock:
            cur = self.conn.execute(
                f"""UPDATE local_chunks_manifest SET {sets}
                    WHERE session_id = ? AND chunk_index = ?""",
                vals
            )
            self._require_updated(
                cur,
                f"[DB] Local chunk not found: session {session_id}, chunk {chunk_index}"
            )
            self.conn.commit()

    def update_local_manifest_row(self, manifest_id, **kwargs):
        if not kwargs:
            return
        kwargs['updated_at'] = datetime.now().isoformat()
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [manifest_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE local_chunks_manifest SET {sets} WHERE manifest_id = ?",
                vals
            )
            self._require_updated(cur, f"[DB] Local manifest row not found: {manifest_id}")
            self.conn.commit()

    def count_tape_file_records(self, tape_label):
        with self.lock:
            return self.conn.execute(
                "SELECT COUNT(*) FROM files_index WHERE tape_label = ?",
                (tape_label,)
            ).fetchone()[0]

    @staticmethod
    def _hydrate_file_row(row):
        """Expose normalized and legacy catalog rows through the old API shape."""
        if row is None:
            return None
        item = dict(row)
        if not item.get('file_name'):
            item['file_name'] = _derived_file_name(
                item.get('stored_path'), item.get('original_path'))
        if not item.get('file_hash') and item.get('file_hash_blob'):
            item['file_hash'] = bytes(item['file_hash_blob']).hex()
        if not item.get('container_name'):
            item['container_name'] = item.get('bundle_tape_path')
        return item

    @staticmethod
    def _catalog_select():
        return """SELECT f.*, b.tape_path AS bundle_tape_path
                  FROM files_index AS f
                  LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id"""

    def _catalog_rows(self, where='', params=(), order_by=''):
        sql = self._catalog_select()
        if where:
            sql += ' WHERE ' + where
        if order_by:
            sql += ' ORDER BY ' + order_by
        with self.lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def _existing_record_keys(self, keys):
        if not keys:
            return set()
        placeholders = ','.join('?' for _ in keys)
        rows = self.conn.execute(
            f"SELECT record_key FROM files_index WHERE record_key IN ({placeholders})",
            list(keys),
        ).fetchall()
        return {bytes(row[0]) for row in rows if row[0] is not None}

    def _bulk_upsert_batch(self, records, update_existing):
        """Upsert one batch inside the caller's transaction."""
        bundle_paths = {
            (record['tape_label'], record.get('container_name'))
            for record in records
            if record.get('is_packed') and record.get('container_name')
        }
        self.conn.executemany(
            """INSERT INTO archive_bundles(tape_label, tape_path)
               VALUES (?, ?)
               ON CONFLICT(tape_label, tape_path) DO NOTHING""",
            bundle_paths,
        )
        bundle_ids = {}
        for tape_label, tape_path in bundle_paths:
            row = self.conn.execute(
                """SELECT bundle_id FROM archive_bundles
                   WHERE tape_label = ? AND tape_path = ?""",
                (tape_label, tape_path),
            ).fetchone()
            bundle_ids[(tape_label, tape_path)] = row['bundle_id']

        normalized_by_key = {}
        legacy_lookup = {}
        for record in records:
            original_path = record.get('original_path') or ''
            tape_label = record.get('tape_label') or ''
            session_id = record.get('local_session_id')
            chunk_index = record.get('local_chunk_index')
            key = _file_record_key(
                original_path, tape_label, session_id, chunk_index)
            container = record.get('container_name')
            bundle_id = bundle_ids.get((tape_label, container))
            normalized_by_key[key] = (
                None,  # file_name is derived from stored_path on reads
                original_path,
                record.get('file_size_bytes'),
                None,  # text hashes are legacy-only
                _hash_to_blob(record.get('file_hash')),
                record.get('backup_date') or datetime.now().isoformat(),
                tape_label,
                bool(record.get('is_packed')),
                None if bundle_id is not None else container,
                record.get('stored_path'),
                session_id,
                chunk_index,
                bundle_id,
                key,
            )
            legacy_lookup[key] = (
                key, original_path, tape_label, session_id, chunk_index)

        existing = self._existing_record_keys(normalized_by_key)
        # Adopt matching pre-normalization rows lazily. The permanent rescue
        # expression index makes these lookups fast without rewriting the DB.
        adoption_rows = [legacy_lookup[key] for key in normalized_by_key
                         if key not in existing]
        if adoption_rows:
            self.conn.executemany(
                """UPDATE files_index AS target
                   SET record_key = ?
                   WHERE target.file_id = (
                       SELECT legacy.file_id FROM files_index AS legacy
                       WHERE COALESCE(legacy.original_path, '') = COALESCE(?, '')
                         AND COALESCE(legacy.tape_label, '') = COALESCE(?, '')
                         AND COALESCE(legacy.local_session_id, -1) = COALESCE(?, -1)
                         AND COALESCE(legacy.local_chunk_index, -1) = COALESCE(?, -1)
                       ORDER BY legacy.file_id LIMIT 1
                   )
                     AND target.record_key IS NULL""",
                adoption_rows,
            )
            existing = self._existing_record_keys(normalized_by_key)

        columns = """file_name, original_path, file_size_bytes, file_hash,
                     file_hash_blob, backup_date, tape_label, is_packed,
                     container_name, stored_path, local_session_id,
                     local_chunk_index, bundle_id, record_key"""
        values = list(normalized_by_key.values())
        if update_existing:
            self.conn.executemany(
                f"""INSERT INTO files_index({columns})
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(record_key) WHERE record_key IS NOT NULL DO UPDATE SET
                        file_name = excluded.file_name,
                        original_path = excluded.original_path,
                        file_size_bytes = excluded.file_size_bytes,
                        file_hash = excluded.file_hash,
                        file_hash_blob = excluded.file_hash_blob,
                        backup_date = excluded.backup_date,
                        tape_label = excluded.tape_label,
                        is_packed = excluded.is_packed,
                        container_name = excluded.container_name,
                        stored_path = excluded.stored_path,
                        local_session_id = excluded.local_session_id,
                        local_chunk_index = excluded.local_chunk_index,
                        bundle_id = excluded.bundle_id""",
                values,
            )
            return {
                'inserted': len(values) - len(existing),
                'updated': len(existing),
                'skipped': 0,
            }

        self.conn.executemany(
            f"""INSERT INTO files_index({columns})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) WHERE record_key IS NOT NULL DO NOTHING""",
            values,
        )
        return {
            'inserted': len(values) - len(existing),
            'updated': 0,
            'skipped': len(existing),
        }

    def bulk_upsert_files(self, records, batch_size=DB_UPSERT_BATCH_SIZE,
                          update_existing=True):
        """Commit file records in bounded transactions instead of per row."""
        totals = {'inserted': 0, 'updated': 0, 'skipped': 0}
        batch = []
        registered = set()

        def flush(items):
            if not items:
                return
            labels = {item.get('tape_label') for item in items}
            missing = []
            for label in labels - registered:
                if not self.conn.execute(
                        "SELECT 1 FROM tapes WHERE volume_label = ?", (label,)
                ).fetchone():
                    missing.append(label)
                else:
                    registered.add(label)
            if missing:
                raise RuntimeError(
                    f"[DB] Cannot index files for unregistered tape(s): {missing}")
            stats = self._commit_write(
                lambda: self._bulk_upsert_batch(items, update_existing),
                f"file catalog batch ({len(items):,} rows)",
            )
            for key in totals:
                totals[key] += stats[key]

        for record in records:
            if CANCEL.is_set():
                raise RuntimeError("file catalog sync cancelled")
            batch.append(record)
            if len(batch) >= max(1, batch_size):
                flush(batch)
                batch = []
        flush(batch)
        return totals

    def get_local_indexed_original_paths(self, session_id, chunk_index, tape_label):
        with self.lock:
            rows = self.conn.execute(
                """SELECT original_path FROM files_index
                   WHERE local_session_id = ?
                     AND local_chunk_index = ?
                     AND tape_label = ?""",
                (session_id, chunk_index, tape_label)
            ).fetchall()
        return {r[0] for r in rows}

    def get_local_written_tape_paths(self, session_id, chunk_index, tape_label):
        with self.lock:
            rows = self.conn.execute(
                """SELECT DISTINCT COALESCE(f.container_name, b.tape_path,
                                             f.stored_path) AS tape_path
                   FROM files_index AS f
                   LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                   WHERE f.local_session_id = ?
                     AND f.local_chunk_index = ?
                     AND f.tape_label = ?
                     AND COALESCE(f.container_name, b.tape_path,
                                  f.stored_path) IS NOT NULL""",
                (session_id, chunk_index, tape_label)
            ).fetchall()
        return [r['tape_path'] for r in rows if r['tape_path']]

    def file_record_exists(self, original_path, tape_label, local_session_id=None,
                           local_chunk_index=None):
        with self.lock:
            key = _file_record_key(original_path, tape_label,
                                   local_session_id, local_chunk_index)
            if self.conn.execute(
                    "SELECT 1 FROM files_index WHERE record_key = ?", (key,)
            ).fetchone():
                return True
            return bool(self.conn.execute(
                """SELECT 1 FROM files_index
                   WHERE COALESCE(original_path, '') = COALESCE(?, '')
                     AND COALESCE(tape_label, '') = COALESCE(?, '')
                     AND COALESCE(local_session_id, -1) = COALESCE(?, -1)
                     AND COALESCE(local_chunk_index, -1) = COALESCE(?, -1)""",
                (original_path, tape_label, local_session_id, local_chunk_index)
            ).fetchone())

    def register_tape(self, volume_label, capacity_gb=None):
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO tapes (volume_label, date_formatted, total_capacity) VALUES (?, ?, ?)",
                    (volume_label, datetime.now().isoformat(), capacity_gb)
                )
                self.conn.commit()
                print(f"[DB] Tape '{volume_label}' registered successfully.")
                return True
            except sqlite3.IntegrityError:
                print(f"[DB] Tape '{volume_label}' is already in the database.")
                return False

    def delete_tape(self, volume_label):
        with self.lock:
            self.conn.execute("DELETE FROM files_index WHERE tape_label = ?", (volume_label,))
            self.conn.execute("DELETE FROM archive_bundles WHERE tape_label = ?",
                              (volume_label,))
            cur = self.conn.execute("DELETE FROM tapes WHERE volume_label = ?", (volume_label,))
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()
            print(f"[DB] Tape '{volume_label}' and its file records removed from database.")

    def tape_exists(self, volume_label):
        with self.lock:
            return bool(self.conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label = ?", (volume_label,)
            ).fetchone())

    def get_tape(self, volume_label):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM tapes WHERE volume_label = ?", (volume_label,)
            ).fetchone()

    def insert_file(self, file_name, original_path, file_size_bytes, file_hash,
                    tape_label, is_packed, container_name, stored_path,
                    local_session_id=None, local_chunk_index=None):
        stats = self.bulk_upsert_files([{
            'file_name': file_name,
            'original_path': original_path,
            'file_size_bytes': file_size_bytes,
            'file_hash': file_hash,
            'tape_label': tape_label,
            'is_packed': is_packed,
            'container_name': container_name,
            'stored_path': stored_path,
            'local_session_id': local_session_id,
            'local_chunk_index': local_chunk_index,
        }])
        return bool(stats['inserted'])

    def search_files(self, name_query=None, date_from=None, date_to=None):
        return self.search_catalog(name_query=name_query,
                                   date_from=date_from, date_to=date_to)

    def search_catalog(self, name_query=None, tape_label=None,
                       date_from=None, date_to=None, limit=None):
        where = ["1=1"]
        params = []
        if name_query:
            where.append("COALESCE(f.file_name, f.stored_path) LIKE ?")
            pattern = name_query.replace('*', '%').replace('?', '_')
            if '%' not in pattern and '_' not in pattern:
                pattern = f'%{pattern}%'
            params.append(pattern)
        if date_from:
            where.append("DATE(f.backup_date) >= ?")
            params.append(date_from)
        if date_to:
            where.append("DATE(f.backup_date) <= ?")
            params.append(date_to)
        if tape_label:
            where.append("f.tape_label = ?")
            params.append(tape_label)
        order = 'f.original_path, COALESCE(f.file_name, f.stored_path)'
        sql = self._catalog_select() + ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY ' + order
        if limit is not None:
            sql += ' LIMIT ?'
            params.append(int(limit))
        with self.lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def get_file_by_id(self, file_id):
        rows = self._catalog_rows("f.file_id = ?", (file_id,))
        return rows[0] if rows else None

    def search_by_directory(self, dir_path):
        pattern = dir_path.rstrip('/\\') + '%'
        return self._catalog_rows("f.original_path LIKE ?", (pattern,),
                                  'f.original_path')

    def list_backup_sessions(self):
        with self.lock:
            return self.conn.execute("""
                SELECT DATE(backup_date) as session_date, tape_label,
                       COUNT(*)          as file_count,
                       SUM(file_size_bytes) as total_bytes
                FROM files_index
                GROUP BY DATE(backup_date), tape_label
                ORDER BY session_date DESC
            """).fetchall()

    def search_by_session(self, session_date, tape_label):
        return self._catalog_rows(
            "DATE(f.backup_date) = ? AND f.tape_label = ?",
            (session_date, tape_label), 'f.original_path')

    def list_tapes(self):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM tapes ORDER BY date_formatted DESC"
            ).fetchall()

    def delete_file(self, file_id):
        with self.lock:
            cur = self.conn.execute("DELETE FROM files_index WHERE file_id = ?", (file_id,))
            self._require_updated(cur, f"[DB] File record not found: {file_id}")
            self.conn.commit()

    def rename_tape(self, old_label, new_label):
        with self.lock:
            try:
                self.conn.execute("BEGIN")
                self.conn.execute("PRAGMA defer_foreign_keys = ON")
                cur = self.conn.execute(
                    "UPDATE tapes SET volume_label = ? WHERE volume_label = ?",
                    (new_label, old_label)
                )
                self._require_updated(cur, f"[DB] Tape not found: {old_label}")
                self.conn.execute(
                    "UPDATE files_index SET tape_label = ? WHERE tape_label = ?",
                    (new_label, old_label)
                )
                # tape_label participates in record_key; clear keys so a later
                # bulk upsert can lazily rebuild them with the new label.
                self.conn.execute(
                    "UPDATE files_index SET record_key = NULL WHERE tape_label = ?",
                    (new_label,)
                )
                self.conn.execute(
                    "UPDATE archive_bundles SET tape_label = ? WHERE tape_label = ?",
                    (new_label, old_label)
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        print(f"[DB] Tape '{old_label}' renamed to '{new_label}'.")

    def update_tape_capacity(self, volume_label, capacity_gb):
        with self.lock:
            cur = self.conn.execute(
                "UPDATE tapes SET total_capacity = ? WHERE volume_label = ?",
                (capacity_gb, volume_label)
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()
        print(f"[DB] Tape '{volume_label}' capacity set to {capacity_gb} GB.")

    def recalculate_tape_used_space(self, volume_label):
        with self.lock:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(file_size_bytes), 0) FROM files_index WHERE tape_label = ?",
                (volume_label,)
            ).fetchone()
            new_used = row[0]
            cur = self.conn.execute(
                "UPDATE tapes SET used_space = ? WHERE volume_label = ?",
                (new_used, volume_label)
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()
            return new_used

    def delete_files_for_tape(self, volume_label):
        with self.lock:
            if not self.conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label = ?", (volume_label,)
            ).fetchone():
                raise RuntimeError(f"[DB] Tape not found: {volume_label}")
            cur = self.conn.execute("DELETE FROM files_index WHERE tape_label = ?", (volume_label,))
            removed = cur.rowcount
            self.conn.execute("DELETE FROM archive_bundles WHERE tape_label = ?",
                              (volume_label,))
            self.conn.execute("UPDATE tapes SET used_space = 0 WHERE volume_label = ?", (volume_label,))
            self.conn.commit()
            print(f"[DB] Removed {removed} file record(s) for tape '{volume_label}' (tape entry kept).")

    def close(self):
        self.conn.close()
