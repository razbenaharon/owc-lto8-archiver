"""DatabaseManager and catalog normalization helpers."""
import os
import time
import hashlib
import sqlite3
import threading
from datetime import datetime

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .constants import CONFIG_FILE, DB_UPSERT_BATCH_SIZE
from .catalog_v3 import (
    catalog_v3_available,
    catalog_values_for_file,
)
from .maintenance_lock import ensure_no_active_maintenance_lock
from .paths import _clean_config_path
from .runtime import CANCEL

SCHEMA_VERSION = 2


def _derived_file_name(stored_path, original_path=None):
    """Derive a display/restore name without storing it once per DB row."""
    path = stored_path or original_path or ''
    return os.path.basename(str(path).replace('/', os.sep))


def _short_source_host(value):
    value = (value or '').strip()
    if not value:
        return 'local'
    return value.split('.', 1)[0]


def _file_record_key(original_path, tape_label, local_session_id=None,
                     local_chunk_index=None, source_host='so02'):
    """Stable compact key for NULL-safe file-record upserts."""
    digest = hashlib.sha256()
    for value in (_short_source_host(source_host), original_path or '', tape_label or '',
                  -1 if local_session_id is None else local_session_id,
                  -1 if local_chunk_index is None else local_chunk_index):
        raw = str(value).encode('utf-8', errors='surrogatepass')
        digest.update(len(raw).to_bytes(8, 'big'))
        digest.update(raw)
    return digest.digest()


def _apply_canonical_remote_paths(metadata, manifest_rows):
    """Attach durable remote SOURCE paths to staging-produced metadata.

    ``stored_path`` remains the staging/tape-relative path.  The canonical
    source is carried separately so callers never have to infer identity from
    a temporary ``_fetch_*`` directory.
    """
    remote_by_local = {}
    for row in manifest_rows:
        local_rel = row['local_rel_path']
        remote_path = row['remote_path']
        if local_rel and remote_path:
            key = str(local_rel).replace('\\', '/')
            previous = remote_by_local.setdefault(key, remote_path)
            if previous != remote_path:
                raise RuntimeError(
                    "[DB] Ambiguous canonical source mapping for staged path "
                    f"'{key}': '{previous}' and '{remote_path}'"
                )
    replaced = 0
    for item in metadata:
        stored = str(item.get('stored_path') or '').replace('\\', '/')
        canonical = remote_by_local.get(stored)
        if canonical:
            item['canonical_source_path'] = canonical
            item['original_path'] = canonical
            replaced += 1
    return replaced


class DatabaseManager:
    def __init__(self, db_path):
        db_path = _clean_config_path(db_path)
        self.db_path = os.path.abspath(db_path)
        maintenance_lock = os.path.abspath(db_path) + '.maintenance.lock'
        ensure_no_active_maintenance_lock(maintenance_lock)
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
                    self.conn.execute("BEGIN IMMEDIATE")
                    result = operation()
                    self.conn.commit()
                    return result
                except sqlite3.OperationalError as e:
                    self.conn.rollback()
                    locked = 'locked' in str(e).lower() or 'busy' in str(e).lower()
                    if not locked or attempt == attempts:
                        raise
                except BaseException:
                    self.conn.rollback()
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
                backup_date     DATETIME,
                tape_label      TEXT,
                source_host     TEXT NOT NULL DEFAULT 'so02',
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
            CREATE TABLE IF NOT EXISTS archive_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_label TEXT NOT NULL,
                tape_label TEXT NOT NULL REFERENCES tapes(volume_label),
                session_kind TEXT NOT NULL DEFAULT 'legacy',
                session_id INTEGER,
                started_at DATETIME NOT NULL,
                completed_at DATETIME,
                UNIQUE(run_label, tape_label)
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
            ('record_key', 'BLOB'),
            ('archive_run_id', 'INTEGER REFERENCES archive_runs(run_id)'),
            ('source_host', "TEXT NOT NULL DEFAULT 'so02'"),
        ):
            try:
                self.conn.execute(f"ALTER TABLE files_index ADD COLUMN {column} {col_type}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
        if self.conn.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_files_dedup_expr
                    ON files_index(
                        COALESCE(original_path, ''),
                        COALESCE(tape_label, ''),
                        COALESCE(local_session_id, -1),
                        COALESCE(local_chunk_index, -1)
                    )
            """)
        self.conn.executescript("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_files_record_key
                ON files_index(record_key)
                WHERE record_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_bundle_id
                ON files_index(bundle_id) WHERE bundle_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_source_host
                ON files_index(source_host, tape_label, original_path);
        """)
        self.conn.execute(
            "UPDATE files_index SET source_host='so02' "
            "WHERE source_host IS NULL OR source_host=''"
        )
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
                ON files_index(local_session_id, local_chunk_index, tape_label)
                WHERE local_session_id IS NOT NULL;
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
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        legacy = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_manifest'"
        ).fetchone()
        if version >= SCHEMA_VERSION or not legacy:
            self._init_remote_schema_v2()
            if not legacy and version < SCHEMA_VERSION:
                self.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self.conn.commit()
            return

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

    def _init_remote_schema_v2(self):
        """Create normalized remote snapshot, plan, chunk, and state tables."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS remote_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_label TEXT NOT NULL,
                remote_host TEXT NOT NULL,
                remote_user TEXT NOT NULL,
                remote_path TEXT NOT NULL,
                tape_label TEXT NOT NULL,
                staging_dir TEXT NOT NULL,
                total_files INTEGER DEFAULT 0,
                total_bytes INTEGER DEFAULT 0,
                chunk_count INTEGER DEFAULT 0,
                plan_id INTEGER REFERENCES remote_plans(plan_id),
                created_at DATETIME NOT NULL,
                completed_at DATETIME,
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','completed','abandoned'))
            );
            CREATE TABLE IF NOT EXISTS remote_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                remote_host TEXT NOT NULL,
                remote_path TEXT NOT NULL,
                fingerprint BLOB NOT NULL UNIQUE,
                total_files INTEGER NOT NULL,
                total_bytes INTEGER NOT NULL,
                created_at DATETIME NOT NULL
            );
            CREATE TABLE IF NOT EXISTS remote_snapshot_files (
                snapshot_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL REFERENCES remote_snapshots(snapshot_id)
                    ON DELETE CASCADE,
                remote_path TEXT NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                UNIQUE(snapshot_id, remote_path)
            );
            CREATE TABLE IF NOT EXISTS remote_plans (
                plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL REFERENCES remote_snapshots(snapshot_id)
                    ON DELETE CASCADE,
                fingerprint BLOB NOT NULL UNIQUE,
                chunk_count INTEGER NOT NULL,
                created_at DATETIME NOT NULL
            );
            CREATE TABLE IF NOT EXISTS remote_plan_files (
                plan_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL REFERENCES remote_plans(plan_id)
                    ON DELETE CASCADE,
                snapshot_file_id INTEGER NOT NULL
                    REFERENCES remote_snapshot_files(snapshot_file_id),
                chunk_index INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                UNIQUE(plan_id, snapshot_file_id)
            );
            CREATE INDEX IF NOT EXISTS idx_remote_plan_chunk
                ON remote_plan_files(plan_id, chunk_index, ordinal);
            CREATE TABLE IF NOT EXISTS remote_chunks (
                session_id INTEGER NOT NULL REFERENCES remote_sessions(session_id)
                    ON DELETE CASCADE,
                chunk_index INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error_msg TEXT,
                updated_at DATETIME,
                PRIMARY KEY(session_id, chunk_index)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS remote_file_state (
                session_id INTEGER NOT NULL REFERENCES remote_sessions(session_id)
                    ON DELETE CASCADE,
                plan_file_id INTEGER NOT NULL REFERENCES remote_plan_files(plan_file_id),
                status TEXT,
                local_rel_path TEXT,
                error_msg TEXT,
                updated_at DATETIME,
                PRIMARY KEY(session_id, plan_file_id)
            ) WITHOUT ROWID;
        """)
        self.conn.commit()

    def _uses_remote_v2(self):
        return self.conn.execute("PRAGMA user_version").fetchone()[0] >= SCHEMA_VERSION

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
        def operation():
            cur = self.conn.execute(
                f"UPDATE remote_sessions SET {sets} WHERE session_id = ?", vals
            )
            self._require_updated(cur, f"[DB] Remote session not found: {session_id}")
        self._commit_write(operation, f"update remote session {session_id}")

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
        if self._uses_remote_v2():
            return self._insert_remote_plan_v2(session_id, list(rows))
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

    def _insert_remote_plan_v2(self, session_id, rows):
        """Persist a canonical snapshot and reusable chunk plan."""
        session = self.get_remote_session(session_id)
        if not session:
            raise RuntimeError(f"[DB] Remote session not found: {session_id}")
        by_path = {}
        for chunk_index, remote_path, _file_name, size in rows:
            canonical = str(remote_path).replace('\\', '/')
            if not canonical.startswith('/'):
                raise RuntimeError(
                    f"[DB] Non-canonical remote SOURCE path: {remote_path}")
            previous = by_path.setdefault(canonical, int(size))
            if previous != int(size):
                raise RuntimeError(
                    f"[DB] Conflicting sizes for remote SOURCE path: {canonical}")
        if len(by_path) != len(rows):
            raise RuntimeError("[DB] Duplicate canonical paths in remote snapshot")

        snapshot_hash = hashlib.sha256()
        for identity in (session['remote_host'], session['remote_path']):
            raw = str(identity).encode('utf-8', errors='surrogatepass')
            snapshot_hash.update(len(raw).to_bytes(8, 'big'))
            snapshot_hash.update(raw)
        for path, size in sorted(by_path.items()):
            raw = path.encode('utf-8', errors='surrogatepass')
            snapshot_hash.update(len(raw).to_bytes(8, 'big'))
            snapshot_hash.update(raw)
            snapshot_hash.update(size.to_bytes(8, 'big', signed=False))
        fingerprint = snapshot_hash.digest()
        plan_hash = hashlib.sha256(fingerprint)
        for chunk_index, remote_path, _file_name, size in rows:
            raw = str(remote_path).encode('utf-8', errors='surrogatepass')
            plan_hash.update(int(chunk_index).to_bytes(4, 'big'))
            plan_hash.update(len(raw).to_bytes(8, 'big'))
            plan_hash.update(raw)
            plan_hash.update(int(size).to_bytes(8, 'big'))
        plan_fingerprint = plan_hash.digest()
        now = datetime.now().isoformat()

        with self.lock:
            with self.conn:
                self.conn.execute(
                    """INSERT INTO remote_snapshots
                       (remote_host,remote_path,fingerprint,total_files,total_bytes,created_at)
                       VALUES (?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING""",
                    (session['remote_host'], session['remote_path'], fingerprint,
                     len(rows), sum(int(r[3]) for r in rows), now),
                )
                snapshot_id = self.conn.execute(
                    "SELECT snapshot_id FROM remote_snapshots WHERE fingerprint=?",
                    (fingerprint,),
                ).fetchone()[0]
                existing = self.conn.execute(
                    "SELECT COUNT(*) FROM remote_snapshot_files WHERE snapshot_id=?",
                    (snapshot_id,),
                ).fetchone()[0]
                if not existing:
                    self.conn.executemany(
                        """INSERT INTO remote_snapshot_files
                           (snapshot_id,remote_path,file_size_bytes) VALUES (?,?,?)""",
                        ((snapshot_id, path, size) for path, size in by_path.items()),
                    )
                self.conn.execute(
                    """INSERT INTO remote_plans
                       (snapshot_id,fingerprint,chunk_count,created_at)
                       VALUES (?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING""",
                    (snapshot_id, plan_fingerprint,
                     len({int(r[0]) for r in rows}), now),
                )
                plan_id = self.conn.execute(
                    "SELECT plan_id FROM remote_plans WHERE fingerprint=?",
                    (plan_fingerprint,),
                ).fetchone()[0]
                existing = self.conn.execute(
                    "SELECT COUNT(*) FROM remote_plan_files WHERE plan_id=?",
                    (plan_id,),
                ).fetchone()[0]
                if not existing:
                    ids = dict(self.conn.execute(
                        "SELECT remote_path,snapshot_file_id FROM remote_snapshot_files "
                        "WHERE snapshot_id=?", (snapshot_id,)).fetchall())
                    self.conn.executemany(
                        """INSERT INTO remote_plan_files
                           (plan_id,snapshot_file_id,chunk_index,ordinal)
                           VALUES (?,?,?,?)""",
                        ((plan_id, ids[str(r[1])], int(r[0]), ordinal)
                         for ordinal, r in enumerate(rows)),
                    )
                self.conn.execute(
                    "UPDATE remote_sessions SET plan_id=? WHERE session_id=?",
                    (plan_id, session_id),
                )
                self.conn.executemany(
                    """INSERT INTO remote_chunks(session_id,chunk_index,status,updated_at)
                       VALUES (?,?,'pending',?) ON CONFLICT DO NOTHING""",
                    ((session_id, ci, now) for ci in sorted({int(r[0]) for r in rows})),
                )
        return plan_id

    def get_chunk_files(self, session_id, chunk_index):
        if self._uses_remote_v2():
            with self.lock:
                return self.conn.execute(
                    """SELECT pf.plan_file_id AS manifest_id,
                              sf.remote_path, sf.file_size_bytes,
                              st.local_rel_path,
                              COALESCE(st.status,
                                CASE WHEN c.status='done' THEN 'fetched' ELSE 'pending' END
                              ) AS status,
                              st.error_msg, st.updated_at
                       FROM remote_sessions s
                       JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
                       JOIN remote_snapshot_files sf
                         ON sf.snapshot_file_id=pf.snapshot_file_id
                       JOIN remote_chunks c ON c.session_id=s.session_id
                         AND c.chunk_index=pf.chunk_index
                       LEFT JOIN remote_file_state st ON st.session_id=s.session_id
                         AND st.plan_file_id=pf.plan_file_id
                       WHERE s.session_id=? AND pf.chunk_index=?
                       ORDER BY pf.ordinal""",
                    (session_id, chunk_index),
                ).fetchall()
        with self.lock:
            return self.conn.execute(
                """SELECT * FROM remote_manifest
                   WHERE session_id = ? AND chunk_index = ?
                   ORDER BY manifest_id""",
                (session_id, chunk_index)
            ).fetchall()

    def update_manifest_row(self, manifest_id, session_id=None, **kwargs):
        if self._uses_remote_v2():
            if session_id is None:
                raise RuntimeError("[DB] session_id required for normalized remote state")
            return self._upsert_remote_file_state(session_id, manifest_id, kwargs)
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

    def _upsert_remote_file_state(self, session_id, plan_file_id, values):
        allowed = {'status', 'local_rel_path', 'error_msg'}
        unknown = set(values) - allowed
        if unknown:
            raise RuntimeError(f"[DB] Invalid remote state field(s): {sorted(unknown)}")

        def operation():
            current = self.conn.execute(
                "SELECT * FROM remote_file_state WHERE session_id=? AND plan_file_id=?",
                (session_id, plan_file_id),
            ).fetchone()
            merged = {key: (current[key] if current else None)
                      for key in ('status','local_rel_path','error_msg')}
            merged.update(values)
            return self.conn.execute(
                """INSERT INTO remote_file_state
                   (session_id,plan_file_id,status,local_rel_path,error_msg,updated_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(session_id,plan_file_id) DO UPDATE SET
                     status=excluded.status,local_rel_path=excluded.local_rel_path,
                     error_msg=excluded.error_msg,updated_at=excluded.updated_at""",
                (session_id, plan_file_id, merged['status'],
                 merged['local_rel_path'], merged['error_msg'],
                 datetime.now().isoformat()),
            )

        self._commit_write(
            operation,
            f"normalized remote file {plan_file_id} update",
        )

    def update_manifest_rows_fetching(self, manifest_ids, session_id=None):
        manifest_ids = list(manifest_ids)
        if self._uses_remote_v2():
            if session_id is None:
                raise RuntimeError("[DB] session_id required for normalized remote state")
            now = datetime.now().isoformat()
            return self._update_manifest_batches(
                """INSERT INTO remote_file_state
                   (session_id,plan_file_id,status,error_msg,updated_at)
                   VALUES (?,?,'fetching',NULL,?)
                   ON CONFLICT(session_id,plan_file_id) DO UPDATE SET
                     status='fetching',error_msg=NULL,updated_at=excluded.updated_at""",
                ((session_id, manifest_id, now) for manifest_id in manifest_ids),
                "normalized manifest fetching-status batch",
            )
        now = datetime.now().isoformat()
        self._update_manifest_batches(
            """UPDATE remote_manifest
               SET status = 'fetching', error_msg = NULL, updated_at = ?
               WHERE manifest_id = ?""",
            ((now, manifest_id) for manifest_id in manifest_ids),
            "manifest fetching-status batch",
        )

    def update_manifest_rows_fetched(self, rows, session_id=None):
        """rows contains (local_rel_path, manifest_id) pairs."""
        rows = list(rows)
        if self._uses_remote_v2():
            if session_id is None:
                raise RuntimeError("[DB] session_id required for normalized remote state")
            now = datetime.now().isoformat()
            return self._update_manifest_batches(
                """INSERT INTO remote_file_state
                   (session_id,plan_file_id,status,local_rel_path,error_msg,updated_at)
                   VALUES (?,?,'fetched',?,NULL,?)
                   ON CONFLICT(session_id,plan_file_id) DO UPDATE SET
                     status='fetched',local_rel_path=excluded.local_rel_path,
                     error_msg=NULL,updated_at=excluded.updated_at""",
                ((session_id, manifest_id, local_rel_path, now)
                 for local_rel_path, manifest_id in rows),
                "normalized manifest fetched-status batch",
            )
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

    def update_manifest_rows_fetch_failed(self, manifest_ids, error_msg,
                                          session_id=None):
        manifest_ids = list(manifest_ids)
        if self._uses_remote_v2():
            if session_id is None:
                raise RuntimeError("[DB] session_id required for normalized remote state")
            now = datetime.now().isoformat()
            error_msg = (error_msg or '')[:500]
            return self._update_manifest_batches(
                """INSERT INTO remote_file_state
                   (session_id,plan_file_id,status,error_msg,updated_at)
                   VALUES (?,?,'fetch_failed',?,?)
                   ON CONFLICT(session_id,plan_file_id) DO UPDATE SET
                     status='fetch_failed',error_msg=excluded.error_msg,
                     updated_at=excluded.updated_at""",
                ((session_id, manifest_id, error_msg, now)
                 for manifest_id in manifest_ids),
                "normalized manifest fetch-failure batch",
            )
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
        if self._uses_remote_v2():
            cur = self._commit_write(
                lambda: self.conn.execute(
                    """UPDATE remote_chunks SET status=?,updated_at=?
                       WHERE session_id=? AND chunk_index=?""",
                    (status, datetime.now().isoformat(), session_id, chunk_index),
                ),
                f"normalized chunk {chunk_index + 1} status update",
            )
            self._require_updated(
                cur, f"[DB] Remote chunk not found: session {session_id}, chunk {chunk_index}")
            if status == 'done':
                self._commit_write(
                    lambda: self.conn.execute(
                        """DELETE FROM remote_file_state
                           WHERE session_id=? AND plan_file_id IN (
                             SELECT plan_file_id FROM remote_plan_files pf
                             JOIN remote_sessions s ON s.plan_id=pf.plan_id
                             WHERE s.session_id=? AND pf.chunk_index=?
                           ) AND COALESCE(status,'') != 'source_missing'""",
                        (session_id, session_id, chunk_index),
                    ),
                    f"normalized chunk {chunk_index + 1} transient-state cleanup",
                )
            return
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
        if self._uses_remote_v2():
            with self.lock:
                rows = self.conn.execute(
                    """SELECT chunk_index FROM remote_chunks
                       WHERE session_id=? AND status!='done' ORDER BY chunk_index""",
                    (session_id,),
                ).fetchall()
            return [r[0] for r in rows]
        with self.lock:
            rows = self.conn.execute(
                """SELECT DISTINCT chunk_index FROM remote_manifest
                   WHERE session_id = ? AND chunk_status NOT IN ('done')
                   ORDER BY chunk_index""",
                (session_id,)
            ).fetchall()
        return [r[0] for r in rows]

    def count_chunks(self, session_id):
        if self._uses_remote_v2():
            with self.lock:
                return self.conn.execute(
                    "SELECT COUNT(*) FROM remote_chunks WHERE session_id=?",
                    (session_id,),
                ).fetchone()[0]
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
        item.pop('file_hash', None)
        item.pop('file_hash_blob', None)
        if not item.get('backup_date'):
            item['backup_date'] = item.get('run_started_at')
        if not item.get('file_name'):
            item['file_name'] = _derived_file_name(
                item.get('stored_path'), item.get('original_path'))
        item['source_host'] = _short_source_host(item.get('source_host') or 'so02')
        if not item.get('container_name'):
            item['container_name'] = item.get('bundle_tape_path')
        return item

    @staticmethod
    def _catalog_select():
        return """SELECT f.*, b.tape_path AS bundle_tape_path,
                         r.started_at AS run_started_at
                  FROM files_index AS f
                  LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                  LEFT JOIN archive_runs AS r ON r.run_id = f.archive_run_id"""

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

        batch_now = datetime.now().isoformat()
        run_specs = {}
        for record in records:
            if record.get('archive_run_id') is not None:
                continue
            backup_date = record.get('backup_date') or batch_now
            tape_label = record.get('tape_label') or ''
            run_label = f"{str(backup_date)[:10]}:{tape_label}"
            kind = 'local' if record.get('local_session_id') is not None else 'remote'
            run_specs[(run_label, tape_label)] = (
                run_label, tape_label, kind, record.get('local_session_id'),
                backup_date, backup_date)
        self.conn.executemany(
            """INSERT INTO archive_runs
               (run_label,tape_label,session_kind,session_id,started_at,completed_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(run_label,tape_label) DO NOTHING""",
            run_specs.values(),
        )
        run_ids = {
            key: self.conn.execute(
                "SELECT run_id FROM archive_runs WHERE run_label=? AND tape_label=?",
                key).fetchone()['run_id']
            for key in run_specs
        }

        has_catalog_v3 = catalog_v3_available(self.conn)
        catalog_dir_cache = {}
        normalized_by_key = {}
        legacy_lookup = {}
        for record in records:
            canonical = record.get('canonical_source_path')
            if canonical is not None:
                canonical = str(canonical)
                if not canonical.startswith('/') or '\\' in canonical:
                    raise RuntimeError(
                        "[DB] Remote catalog records require an absolute POSIX "
                        f"canonical SOURCE path, got: {canonical}"
                    )
            original_path = canonical or record.get('original_path') or ''
            tape_label = record.get('tape_label') or ''
            session_id = record.get('local_session_id')
            chunk_index = record.get('local_chunk_index')
            source_host = _short_source_host(record.get('source_host') or 'so02')
            key = _file_record_key(
                original_path, tape_label, session_id, chunk_index, source_host)
            container = record.get('container_name')
            bundle_id = bundle_ids.get((tape_label, container))
            backup_date = record.get('backup_date') or batch_now
            run_label = f"{str(backup_date)[:10]}:{tape_label}"
            archive_run_id = (record.get('archive_run_id') or
                              run_ids[(run_label, tape_label)])
            values = [
                None,  # file_name is derived from stored_path on reads
                original_path,
                record.get('file_size_bytes'),
                None,  # timestamp is normalized through archive_runs
                tape_label,
                source_host,
                bool(record.get('is_packed')),
                None if bundle_id is not None else container,
                record.get('stored_path'),
                session_id,
                chunk_index,
                bundle_id,
                key,
                archive_run_id,
            ]
            if has_catalog_v3:
                row = {
                    'tape_label': tape_label,
                    'original_path': original_path,
                    'stored_path': record.get('stored_path'),
                    'backup_date': backup_date,
                    'run_started_at': backup_date,
                    'source_host': source_host,
                }
                values.extend(catalog_values_for_file(
                    self.conn, row, catalog_dir_cache))
            normalized_by_key[key] = tuple(values)
            legacy_lookup[key] = (
                key, original_path, source_host, tape_label, session_id, chunk_index)

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
                         AND COALESCE(legacy.source_host, 'so02') = COALESCE(?, 'so02')
                         AND COALESCE(legacy.tape_label, '') = COALESCE(?, '')
                         AND COALESCE(legacy.local_session_id, -1) = COALESCE(?, -1)
                         AND COALESCE(legacy.local_chunk_index, -1) = COALESCE(?, -1)
                       ORDER BY legacy.file_id LIMIT 1
                   )
                     AND target.record_key IS NULL""",
                adoption_rows,
            )
            existing = self._existing_record_keys(normalized_by_key)

        columns = """file_name, original_path, file_size_bytes, backup_date,
                     tape_label, source_host, is_packed,
                     container_name, stored_path, local_session_id,
                     local_chunk_index, bundle_id, record_key, archive_run_id"""
        placeholders = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
        update_sets = """file_name = excluded.file_name,
                        original_path = excluded.original_path,
                        file_size_bytes = excluded.file_size_bytes,
                        backup_date = excluded.backup_date,
                        tape_label = excluded.tape_label,
                        source_host = excluded.source_host,
                        is_packed = excluded.is_packed,
                        container_name = excluded.container_name,
                        stored_path = excluded.stored_path,
                        local_session_id = excluded.local_session_id,
                        local_chunk_index = excluded.local_chunk_index,
                        bundle_id = excluded.bundle_id,
                        archive_run_id = excluded.archive_run_id"""
        if has_catalog_v3:
            columns += ", directory_id, catalog_name, catalog_backup_date"
            placeholders += ", ?, ?, ?"
            update_sets += """,
                        directory_id = excluded.directory_id,
                        catalog_name = excluded.catalog_name,
                        catalog_backup_date = excluded.catalog_backup_date"""
        values = list(normalized_by_key.values())
        if update_existing:
            self.conn.executemany(
                f"""INSERT INTO files_index({columns})
                    VALUES ({placeholders})
                    ON CONFLICT(record_key) WHERE record_key IS NOT NULL DO UPDATE SET
                        {update_sets}""",
                values,
            )
            return {
                'inserted': len(values) - len(existing),
                'updated': len(existing),
                'skipped': 0,
            }

        self.conn.executemany(
            f"""INSERT INTO files_index({columns})
                VALUES ({placeholders})
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

            def operation():
                labels = {item.get('tape_label') for item in items}
                found = set()
                missing = []
                for label in labels - registered:
                    if not self.conn.execute(
                            "SELECT 1 FROM tapes WHERE volume_label = ?", (label,)
                    ).fetchone():
                        missing.append(label)
                    else:
                        found.add(label)
                if missing:
                    raise RuntimeError(
                        f"[DB] Cannot index files for unregistered tape(s): {missing}")
                stats = self._bulk_upsert_batch(items, update_existing)
                return stats, found

            stats, found = self._commit_write(
                operation,
                f"file catalog batch ({len(items):,} rows)",
            )
            registered.update(found)
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
                           local_chunk_index=None, source_host='so02'):
        with self.lock:
            source_host = _short_source_host(source_host or 'so02')
            key = _file_record_key(original_path, tape_label,
                                   local_session_id, local_chunk_index,
                                   source_host)
            if self.conn.execute(
                    "SELECT 1 FROM files_index WHERE record_key = ?", (key,)
            ).fetchone():
                return True
            return bool(self.conn.execute(
                """SELECT 1 FROM files_index
                   WHERE COALESCE(original_path, '') = COALESCE(?, '')
                     AND COALESCE(source_host, 'so02') = COALESCE(?, 'so02')
                     AND COALESCE(tape_label, '') = COALESCE(?, '')
                     AND COALESCE(local_session_id, -1) = COALESCE(?, -1)
                     AND COALESCE(local_chunk_index, -1) = COALESCE(?, -1)""",
                (original_path, source_host, tape_label, local_session_id,
                 local_chunk_index)
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

    def _delete_tape_records_unlocked(self, volume_label):
        stats = {}
        cur = self.conn.execute(
            "DELETE FROM files_index WHERE tape_label = ?", (volume_label,))
        stats['file_records'] = cur.rowcount
        cur = self.conn.execute(
            "DELETE FROM archive_bundles WHERE tape_label = ?",
            (volume_label,))
        stats['bundles'] = cur.rowcount
        cur = self.conn.execute(
            "DELETE FROM archive_runs WHERE tape_label = ?",
            (volume_label,))
        stats['runs'] = cur.rowcount
        if catalog_v3_available(self.conn):
            cur = self.conn.execute(
                "DELETE FROM catalog_directories WHERE tape_label = ?",
                (volume_label,))
            stats['directories'] = cur.rowcount
        else:
            stats['directories'] = 0
        return stats

    def replace_formatted_tape(self, volume_label, capacity_gb=None,
                               previous_labels=None):
        """Remove stale DB records for a formatted tape and register it fresh."""
        labels_to_clear = []
        for label in list(previous_labels or []) + [volume_label]:
            label = (label or '').strip()
            if label and label not in labels_to_clear:
                labels_to_clear.append(label)

        def operation():
            removed = {}
            for label in labels_to_clear:
                stats = self._delete_tape_records_unlocked(label)
                cur = self.conn.execute(
                    "DELETE FROM tapes WHERE volume_label = ?", (label,))
                if cur.rowcount or any(stats.values()):
                    removed[label] = stats
            self.conn.execute(
                """INSERT INTO tapes
                   (volume_label, date_formatted, total_capacity, used_space)
                   VALUES (?, ?, ?, 0)""",
                (volume_label, datetime.now().isoformat(), capacity_gb),
            )
            return removed

        removed = self._commit_write(
            operation, f"replace formatted tape {volume_label}")

        if removed:
            for label, stats in removed.items():
                print(
                    f"[DB] Cleared formatted tape '{label}': "
                    f"{stats['file_records']} file record(s), "
                    f"{stats['bundles']} bundle(s), {stats['runs']} run(s)."
                )
        else:
            print("[DB] No existing tape records matched the formatted tape.")
        print(f"[DB] Tape '{volume_label}' registered fresh with 0 used bytes.")
        return True

    def _prune_empty_catalog_directories(self, tape_label, directory_id=None):
        if not catalog_v3_available(self.conn):
            return
        if directory_id is not None:
            current = directory_id
            while current is not None:
                row = self.conn.execute(
                    """SELECT directory_id,parent_id FROM catalog_directories
                       WHERE directory_id=? AND tape_label=?""",
                    (current, tape_label),
                ).fetchone()
                if not row:
                    break
                has_files = self.conn.execute(
                    "SELECT 1 FROM files_index WHERE directory_id=? LIMIT 1",
                    (current,),
                ).fetchone()
                has_children = self.conn.execute(
                    "SELECT 1 FROM catalog_directories WHERE parent_id=? LIMIT 1",
                    (current,),
                ).fetchone()
                if has_files or has_children:
                    break
                self.conn.execute(
                    "DELETE FROM catalog_directories WHERE directory_id=?",
                    (current,),
                )
                current = row['parent_id']
            return

        while True:
            cur = self.conn.execute(
                """DELETE FROM catalog_directories
                   WHERE tape_label=?
                     AND NOT EXISTS (
                         SELECT 1 FROM files_index f
                         WHERE f.directory_id=catalog_directories.directory_id)
                     AND NOT EXISTS (
                         SELECT 1 FROM catalog_directories child
                         WHERE child.parent_id=catalog_directories.directory_id)""",
                (tape_label,),
            )
            if cur.rowcount == 0:
                break

    def delete_tape(self, volume_label):
        def operation():
            self._delete_tape_records_unlocked(volume_label)
            cur = self.conn.execute(
                "DELETE FROM tapes WHERE volume_label = ?", (volume_label,))
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")

        self._commit_write(operation, f"delete tape {volume_label}")
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

    def insert_file(self, file_name, original_path, file_size_bytes,
                    tape_label, is_packed, container_name, stored_path,
                    local_session_id=None, local_chunk_index=None,
                    source_host='local'):
        stats = self.bulk_upsert_files([{
            'file_name': file_name,
            'original_path': original_path,
            'file_size_bytes': file_size_bytes,
            'tape_label': tape_label,
            'source_host': source_host,
            'is_packed': is_packed,
            'container_name': container_name,
            'stored_path': stored_path,
            'local_session_id': local_session_id,
            'local_chunk_index': local_chunk_index,
        }])
        return bool(stats['inserted'])

    def search_files(self, name_query=None, date_from=None, date_to=None,
                     limit=None, offset=None, source_host=None):
        return self.search_catalog(name_query=name_query,
                                   date_from=date_from, date_to=date_to,
                                   limit=limit, offset=offset,
                                   source_host=source_host)

    def search_catalog(self, name_query=None, tape_label=None,
                       date_from=None, date_to=None, limit=None, offset=None,
                       source_host=None):
        where = ["1=1"]
        params = []
        if name_query:
            where.append("COALESCE(f.file_name, f.stored_path) LIKE ?")
            pattern = name_query.replace('*', '%').replace('?', '_')
            if '%' not in pattern and '_' not in pattern:
                pattern = f'%{pattern}%'
            params.append(pattern)
        if date_from:
            where.append("DATE(COALESCE(f.backup_date,r.started_at)) >= ?")
            params.append(date_from)
        if date_to:
            where.append("DATE(COALESCE(f.backup_date,r.started_at)) <= ?")
            params.append(date_to)
        if tape_label:
            where.append("f.tape_label = ?")
            params.append(tape_label)
        if source_host:
            where.append("f.source_host = ?")
            params.append(_short_source_host(source_host))
        order = 'f.original_path, COALESCE(f.file_name, f.stored_path)'
        sql = self._catalog_select() + ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY ' + order
        if limit is not None:
            sql += ' LIMIT ?'
            params.append(int(limit))
            if offset is not None:
                sql += ' OFFSET ?'
                params.append(int(offset))
        with self.lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_search_files(self, name_query=None, date_from=None, date_to=None,
                           source_host=None):
        where = ["1=1"]
        params = []
        if name_query:
            where.append("COALESCE(f.file_name, f.stored_path) LIKE ?")
            pattern = name_query.replace('*', '%').replace('?', '_')
            if '%' not in pattern and '_' not in pattern:
                pattern = f'%{pattern}%'
            params.append(pattern)
        if date_from:
            where.append("DATE(COALESCE(f.backup_date,r.started_at)) >= ?")
            params.append(date_from)
        if date_to:
            where.append("DATE(COALESCE(f.backup_date,r.started_at)) <= ?")
            params.append(date_to)
        if source_host:
            where.append("f.source_host = ?")
            params.append(_short_source_host(source_host))
        sql = self._catalog_select() + ' WHERE ' + ' AND '.join(where)
        with self.lock:
            return self.conn.execute(
                f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]

    def get_file_by_id(self, file_id):
        rows = self._catalog_rows("f.file_id = ?", (file_id,))
        return rows[0] if rows else None

    def search_by_directory(self, dir_path, limit=None, offset=None,
                            source_host=None):
        needle = dir_path.strip().rstrip('/\\')
        if not needle:
            return []
        prefix_pattern = needle + '%'
        contains_pattern = f'%{needle}%'
        where = "(f.original_path LIKE ? OR f.original_path LIKE ?)"
        params = [prefix_pattern, contains_pattern]
        if source_host:
            where += " AND f.source_host = ?"
            params.append(_short_source_host(source_host))
        if limit is None:
            return self._catalog_rows(where, params, 'f.original_path')
        sql = self._catalog_select() + ' WHERE ' + where
        sql += ' ORDER BY f.original_path LIMIT ? OFFSET ?'
        params.extend([int(limit), int(offset or 0)])
        with self.lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_by_directory(self, dir_path, source_host=None):
        needle = dir_path.strip().rstrip('/\\')
        if not needle:
            return 0
        where = "(f.original_path LIKE ? OR f.original_path LIKE ?)"
        params = [needle + '%', f'%{needle}%']
        if source_host:
            where += " AND f.source_host = ?"
            params.append(_short_source_host(source_host))
        sql = self._catalog_select() + ' WHERE ' + where
        with self.lock:
            return self.conn.execute(
                f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]

    def list_backup_sessions(self):
        with self.lock:
            return self.conn.execute("""
                SELECT DATE(COALESCE(f.backup_date,r.started_at)) AS session_date,
                       f.tape_label, COUNT(*) AS file_count,
                       SUM(f.file_size_bytes) AS total_bytes
                FROM files_index f
                LEFT JOIN archive_runs r ON r.run_id=f.archive_run_id
                GROUP BY DATE(COALESCE(f.backup_date,r.started_at)), f.tape_label
                ORDER BY session_date DESC
            """).fetchall()

    def search_by_session(self, session_date, tape_label, limit=None, offset=None):
        where = "DATE(COALESCE(f.backup_date,r.started_at)) = ? AND f.tape_label = ?"
        params = [session_date, tape_label]
        if limit is None:
            return self._catalog_rows(where, params, 'f.original_path')
        sql = self._catalog_select() + ' WHERE ' + where
        sql += ' ORDER BY f.original_path LIMIT ? OFFSET ?'
        params.extend([int(limit), int(offset or 0)])
        with self.lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_by_session(self, session_date, tape_label):
        with self.lock:
            return self.conn.execute("""
                SELECT COUNT(*)
                FROM files_index f
                LEFT JOIN archive_runs r ON r.run_id=f.archive_run_id
                WHERE DATE(COALESCE(f.backup_date,r.started_at)) = ?
                  AND f.tape_label = ?
            """, (session_date, tape_label)).fetchone()[0]

    def list_tapes(self):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM tapes ORDER BY date_formatted DESC"
            ).fetchall()

    def list_source_hosts(self):
        with self.lock:
            return [row[0] for row in self.conn.execute(
                """SELECT DISTINCT COALESCE(source_host,'so02') AS source_host
                   FROM files_index
                   ORDER BY source_host"""
            ).fetchall() if row[0]]

    def delete_file(self, file_id):
        def operation():
            row = self.conn.execute(
                "SELECT tape_label,directory_id FROM files_index WHERE file_id = ?",
                (file_id,),
            ).fetchone() if catalog_v3_available(self.conn) else None
            cur = self.conn.execute("DELETE FROM files_index WHERE file_id = ?", (file_id,))
            self._require_updated(cur, f"[DB] File record not found: {file_id}")
            if row:
                self._prune_empty_catalog_directories(
                    row['tape_label'], row['directory_id'])
        self._commit_write(operation, f"delete file record {file_id}")

    def rename_tape(self, old_label, new_label):
        def operation():
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
            rows = self.conn.execute(
                """SELECT file_id,original_path,source_host,
                          local_session_id,local_chunk_index
                   FROM files_index WHERE tape_label=?""", (new_label,))
            self.conn.executemany(
                "UPDATE files_index SET record_key=? WHERE file_id=?",
                ((_file_record_key(r['original_path'], new_label,
                                   r['local_session_id'],
                                   r['local_chunk_index'],
                                   r['source_host']),
                  r['file_id']) for r in rows),
            )
            self.conn.execute(
                "UPDATE archive_bundles SET tape_label = ? WHERE tape_label = ?",
                (new_label, old_label)
            )
            self.conn.execute(
                "UPDATE archive_runs SET tape_label = ? WHERE tape_label = ?",
                (new_label, old_label)
            )
            self.conn.execute(
                "UPDATE remote_sessions SET tape_label = ? WHERE tape_label = ?",
                (new_label, old_label)
            )
        self._commit_write(operation, f"rename tape {old_label}")
        print(f"[DB] Tape '{old_label}' renamed to '{new_label}'.")

    def update_tape_capacity(self, volume_label, capacity_gb):
        def operation():
            cur = self.conn.execute(
                "UPDATE tapes SET total_capacity = ? WHERE volume_label = ?",
                (capacity_gb, volume_label)
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
        self._commit_write(operation, f"update tape capacity {volume_label}")
        print(f"[DB] Tape '{volume_label}' capacity set to {capacity_gb} GB.")

    def recalculate_tape_used_space(self, volume_label):
        def operation():
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
            return new_used
        return self._commit_write(
            operation, f"recalculate tape used space {volume_label}")

    def delete_files_for_tape(self, volume_label):
        def operation():
            if not self.conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label = ?", (volume_label,)
            ).fetchone():
                raise RuntimeError(f"[DB] Tape not found: {volume_label}")
            cur = self.conn.execute("DELETE FROM files_index WHERE tape_label = ?", (volume_label,))
            removed = cur.rowcount
            self.conn.execute("DELETE FROM archive_bundles WHERE tape_label = ?",
                              (volume_label,))
            self.conn.execute("DELETE FROM archive_runs WHERE tape_label = ?",
                              (volume_label,))
            self._prune_empty_catalog_directories(volume_label)
            self.conn.execute("UPDATE tapes SET used_space = 0 WHERE volume_label = ?", (volume_label,))
            return removed
        removed = self._commit_write(
            operation, f"delete file records for tape {volume_label}")
        print(f"[DB] Removed {removed} file record(s) for tape '{volume_label}' (tape entry kept).")

    def delete_session(self, kind, session_id):
        """Delete one local or remote session and session-owned state.

        Tape catalog records are intentionally preserved. This mirrors the
        inspector's historical behavior: deleting a session removes resumability
        bookkeeping, not archived file records.
        """
        kind = (kind or '').strip().lower()
        session_id = int(session_id)
        if kind not in ('local', 'remote'):
            raise RuntimeError(f"[DB] Unknown session kind: {kind}")

        def operation():
            if kind == 'local':
                self.conn.execute(
                    "DELETE FROM local_chunks_manifest WHERE session_id=?",
                    (session_id,))
                cur = self.conn.execute(
                    "DELETE FROM local_sessions WHERE session_id=?",
                    (session_id,))
                self._require_updated(
                    cur, f"[DB] Local session not found: {session_id}")
                return cur.rowcount

            tables = {row[0] for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'remote_manifest' in tables:
                self.conn.execute(
                    "DELETE FROM remote_manifest WHERE session_id=?",
                    (session_id,))
            if 'remote_chunks' in tables:
                self.conn.execute(
                    "DELETE FROM remote_chunks WHERE session_id=?",
                    (session_id,))
            if 'remote_file_state' in tables:
                self.conn.execute(
                    "DELETE FROM remote_file_state WHERE session_id=?",
                    (session_id,))
            cur = self.conn.execute(
                "DELETE FROM remote_sessions WHERE session_id=?",
                (session_id,))
            self._require_updated(
                cur, f"[DB] Remote session not found: {session_id}")
            return cur.rowcount

        removed = self._commit_write(
            operation, f"delete {kind} session {session_id}")
        print(f"[DB] Deleted {kind} session {session_id}.")
        return removed

    def get_unreferenced_remote_data_summary(self):
        """Describe normalized remote data not reachable from any session."""
        with self.lock:
            tables = {row[0] for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            required = {
                'remote_sessions', 'remote_snapshots', 'remote_snapshot_files',
                'remote_plans', 'remote_plan_files',
            }
            if not required.issubset(tables):
                return {
                    'supported': False, 'active_sessions': 0,
                    'plans': 0, 'plan_files': 0,
                    'snapshots': 0, 'snapshot_files': 0,
                }
            return dict(self.conn.execute("""
                SELECT
                  1 AS supported,
                  (SELECT COUNT(*) FROM remote_sessions
                   WHERE status='active') AS active_sessions,
                  (SELECT COUNT(*) FROM remote_plans p
                   WHERE NOT EXISTS (
                     SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
                   )) AS plans,
                  (SELECT COUNT(*) FROM remote_plan_files pf
                   WHERE EXISTS (
                     SELECT 1 FROM remote_plans p
                     WHERE p.plan_id=pf.plan_id AND NOT EXISTS (
                       SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
                     )
                   )) AS plan_files,
                  (SELECT COUNT(*) FROM remote_snapshots sn
                   WHERE NOT EXISTS (
                     SELECT 1 FROM remote_plans p
                     JOIN remote_sessions s ON s.plan_id=p.plan_id
                     WHERE p.snapshot_id=sn.snapshot_id
                   )) AS snapshots,
                  (SELECT COUNT(*) FROM remote_snapshot_files sf
                   WHERE EXISTS (
                     SELECT 1 FROM remote_snapshots sn
                     WHERE sn.snapshot_id=sf.snapshot_id AND NOT EXISTS (
                       SELECT 1 FROM remote_plans p
                       JOIN remote_sessions s ON s.plan_id=p.plan_id
                       WHERE p.snapshot_id=sn.snapshot_id
                     )
                   )) AS snapshot_files
            """).fetchone())

    def _permanent_catalog_signature(self):
        """Small invariant fingerprint for tables cleanup must never change."""
        return tuple(self.conn.execute("""
            SELECT
              (SELECT COUNT(*) FROM files_index),
              (SELECT COALESCE(SUM(file_size_bytes),0) FROM files_index),
              (SELECT COALESCE(SUM(file_id),0) FROM files_index),
              (SELECT COUNT(record_key) FROM files_index),
              (SELECT COUNT(*) FROM tapes),
              (SELECT COALESCE(SUM(tape_id),0) FROM tapes),
              (SELECT COALESCE(SUM(used_space),0) FROM tapes),
              (SELECT COUNT(*) FROM archive_bundles),
              (SELECT COALESCE(SUM(bundle_id),0) FROM archive_bundles),
              (SELECT COUNT(*) FROM archive_runs),
              (SELECT COALESCE(SUM(run_id),0) FROM archive_runs)
        """).fetchone())

    def cleanup_unreferenced_remote_data(self, compact=False):
        """Delete only plans/snapshots unreachable from every remote session.

        Permanent tape catalog tables are fingerprinted before and after the
        transaction.  Any unexpected catalog change rolls the entire cleanup
        back.  Active sessions block cleanup even when their own data would not
        match the orphan predicates.
        """
        before_bytes = os.path.getsize(self.db_path)
        with self.lock:
            summary = self.get_unreferenced_remote_data_summary()
            if not summary['supported']:
                raise RuntimeError(
                    "[DB] Normalized remote session storage is not available; "
                    "run Optimize & Migrate first.")
            if summary['active_sessions']:
                raise RuntimeError(
                    "[DB] Refusing session-data cleanup while remote sessions "
                    "are active.")
            catalog_before = self._permanent_catalog_signature()
            session_before = tuple(self.conn.execute(
                "SELECT COUNT(*),COALESCE(SUM(session_id),0) FROM remote_sessions"
            ).fetchone())
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                active = self.conn.execute(
                    "SELECT COUNT(*) FROM remote_sessions WHERE status='active'"
                ).fetchone()[0]
                if active:
                    raise RuntimeError(
                        "[DB] A remote session became active; cleanup cancelled.")
                plan_cur = self.conn.execute("""
                    DELETE FROM remote_plans AS p
                    WHERE NOT EXISTS (
                      SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
                    )
                """)
                snapshot_cur = self.conn.execute("""
                    DELETE FROM remote_snapshots AS sn
                    WHERE NOT EXISTS (
                      SELECT 1 FROM remote_plans p
                      WHERE p.snapshot_id=sn.snapshot_id
                    )
                """)
                catalog_after = self._permanent_catalog_signature()
                session_after = tuple(self.conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(session_id),0) FROM remote_sessions"
                ).fetchone())
                if catalog_after != catalog_before:
                    raise RuntimeError(
                        "[DB] Permanent tape catalog invariant changed; cleanup rolled back.")
                if session_after != session_before:
                    raise RuntimeError(
                        "[DB] Remote session rows changed; cleanup rolled back.")
                fk_rows = self.conn.execute('PRAGMA foreign_key_check').fetchall()
                if fk_rows:
                    raise RuntimeError(
                        f"[DB] Cleanup produced {len(fk_rows)} foreign-key violation(s).")
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

            result = {
                'plans_deleted': plan_cur.rowcount,
                'plan_files_deleted': summary['plan_files'],
                'snapshots_deleted': snapshot_cur.rowcount,
                'snapshot_files_deleted': summary['snapshot_files'],
                'catalog_files_preserved': catalog_before[0],
                'before_bytes': before_bytes,
            }
            if compact:
                self.conn.execute('VACUUM')
            result['after_bytes'] = os.path.getsize(self.db_path)
            result['reclaimed_bytes'] = before_bytes - result['after_bytes']
            result['quick_check'] = self.conn.execute(
                'PRAGMA quick_check').fetchone()[0]
            result['foreign_key_violations'] = len(
                self.conn.execute('PRAGMA foreign_key_check').fetchall())
            if result['quick_check'] != 'ok' or result['foreign_key_violations']:
                raise RuntimeError('[DB] Post-cleanup validation failed.')
            return result

    def close(self):
        self.conn.close()
