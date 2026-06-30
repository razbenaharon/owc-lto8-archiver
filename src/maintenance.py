"""Offline, resumable database-v2 optimization and canonical-path repair."""
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime

from .catalog_v3 import (
    CATALOG_MIGRATION_NAME,
    CATALOG_SCHEMA_VERSION,
    catalog_v3_available,
    catalog_values_for_file,
    ensure_catalog_schema,
    free_space_report,
)
from .db import SCHEMA_VERSION, _file_record_key
from .maintenance_lock import maintenance_lock
from .reporting import append_maintenance_summary_row


_STAGING_COMPONENT = re.compile(r"^_fetch(?:_s(?P<session>\d+))?_\d+$",
                                re.IGNORECASE)


def _staging_relative(path):
    """Return the path below a real _fetch_* component, never a substring."""
    if not path:
        return None
    value = str(path)
    if not re.match(r"^[A-Za-z]:[\\/]", value):
        return None
    parts = re.split(r"[\\/]", value)
    for index, part in enumerate(parts):
        if _STAGING_COMPONENT.fullmatch(part) and index + 1 < len(parts):
            return "/".join(parts[index + 1:])
    return None


def _staging_session_id(path):
    """Return the session id encoded in _fetch_sNNNN_NNN paths, if present."""
    if not path:
        return None
    value = str(path)
    if not re.match(r"^[A-Za-z]:[\\/]", value):
        return None
    for part in re.split(r"[\\/]", value):
        match = _STAGING_COMPONENT.fullmatch(part)
        if match and match.group('session'):
            return int(match.group('session'))
    return None


def _fallback_canonical(path, source_root):
    rel = _staging_relative(path)
    if rel is None:
        return None
    return source_root.rstrip('/') + '/' + rel.lstrip('/')


def inspect_legacy_database(db_path, source_root='/strg/E/shared-data'):
    """Return a read-only migration preflight report."""
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.create_function('staging_rel', 1, _staging_relative, deterministic=True)
    report = {
        'database': db_path,
        'size_bytes': os.path.getsize(db_path),
        'user_version': conn.execute('PRAGMA user_version').fetchone()[0],
        'quick_check': conn.execute('PRAGMA quick_check').fetchone()[0],
        'source_root': source_root,
        'files': conn.execute('SELECT COUNT(*) FROM files_index').fetchone()[0],
        'staging_original_paths': conn.execute(
            "SELECT COUNT(*) FROM files_index WHERE staging_rel(original_path) IS NOT NULL"
        ).fetchone()[0],
        'local_session_records': conn.execute(
            'SELECT COUNT(*) FROM files_index WHERE local_session_id IS NOT NULL'
        ).fetchone()[0],
        'remote_sessions': [dict(r) for r in conn.execute(
            'SELECT session_id,session_label,tape_label,status,total_files,chunk_count '
            'FROM remote_sessions ORDER BY session_id'
        )],
    }
    conn.close()
    return report


def _query_plan(conn, sql, params=()):
    return [tuple(row) for row in conn.execute('EXPLAIN QUERY PLAN ' + sql, params)]


def _fts5_available(conn):
    try:
        options = [row[0] for row in conn.execute('PRAGMA compile_options')]
    except sqlite3.Error:
        return False
    return any('ENABLE_FTS5' in option for option in options)


def inspect_catalog_database(db_path):
    """Return a read-only v3 catalog-browser preflight report."""
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        tables = {row['name'] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {
            row['name'] for row in conn.execute('PRAGMA table_info(files_index)')
        } if 'files_index' in tables else set()
        tapes = [dict(row) for row in conn.execute(
            "SELECT volume_label,total_capacity,used_space FROM tapes "
            "ORDER BY volume_label"
        )] if 'tapes' in tables else []
        first_tape = tapes[0]['volume_label'] if tapes else None
        current_tape_plan = None
        current_name_plan = None
        if first_tape:
            current_tape_plan = _query_plan(conn, """
                SELECT f.file_id
                FROM files_index f
                LEFT JOIN archive_runs r ON r.run_id=f.archive_run_id
                WHERE f.tape_label=?
                ORDER BY f.original_path, COALESCE(f.file_name, f.stored_path)
                LIMIT 251
            """, (first_tape,))
            current_name_plan = _query_plan(conn, """
                SELECT f.file_id
                FROM files_index f
                LEFT JOIN archive_runs r ON r.run_id=f.archive_run_id
                WHERE COALESCE(f.file_name, f.stored_path) LIKE ?
                ORDER BY f.original_path, COALESCE(f.file_name, f.stored_path)
                LIMIT 251
            """, ('%jpg%',))

        v3 = catalog_v3_available(conn)
        expected_v3_plan = None
        if v3:
            directory = conn.execute(
                "SELECT directory_id FROM catalog_directories ORDER BY directory_id LIMIT 1"
            ).fetchone()
            if directory:
                expected_v3_plan = _query_plan(conn, """
                    SELECT f.file_id
                    FROM files_index f
                    WHERE f.directory_id=?
                      AND (f.catalog_name > ? OR
                           (f.catalog_name = ? AND f.file_id > ?))
                    ORDER BY f.catalog_name, f.file_id
                    LIMIT 251
                """, (directory['directory_id'], '', '', 0))

        report = {
            'database': db_path,
            'is_workspace_lto_archive_db':
                os.path.basename(db_path).lower() == 'lto_archive.db',
            'user_version': conn.execute('PRAGMA user_version').fetchone()[0],
            'catalog_v3_available': v3,
            'tables': sorted(tables),
            'files_index_columns': sorted(columns),
            'row_counts': {},
            'tapes': tapes,
            'quick_check': conn.execute('PRAGMA quick_check').fetchone()[0],
            'foreign_key_violations':
                len(conn.execute('PRAGMA foreign_key_check').fetchall()),
            'sqlite_version': sqlite3.sqlite_version,
            'fts5_available': _fts5_available(conn),
            'free_space': free_space_report(db_path),
            'current_query_plans': {
                'tape_ordered_page': current_tape_plan,
                'name_contains_page': current_name_plan,
            },
            'expected_v3_query_plan': expected_v3_plan,
            'notes': [
                'lto_archive.db is runtime data and must remain gitignored.',
                'Preflight is read-only; migration uses copy-and-swap with rollback.',
            ],
        }
        for table in (
                'files_index', 'catalog_directories', 'files_index_fts',
                'archive_bundles', 'archive_runs', 'remote_sessions',
                'remote_snapshots', 'remote_plans', 'remote_chunks',
                'remote_file_state'):
            if table in tables:
                try:
                    report['row_counts'][table] = conn.execute(
                        f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                except sqlite3.Error as exc:
                    report['row_counts'][table] = f'ERROR: {exc}'
        return report
    finally:
        conn.close()


class DatabaseOptimizer:
    """Build and validate an optimized copy before atomically installing it."""

    def __init__(self, db_path, source_root='/strg/E/shared-data', progress=print):
        self.db_path = os.path.abspath(db_path)
        self.source_root = source_root.rstrip('/')
        self.progress = progress or (lambda _message: None)
        self.started = datetime.now()
        stamp = self.started.strftime('%Y%m%d_%H%M%S')
        self.work_path = self.db_path + f'.optimize_{stamp}.work'
        self.compact_path = self.db_path + f'.optimize_{stamp}.compact'
        self.rollback_path = self.db_path + f'.pre_optimize_{stamp}.bak'
        self.lock_path = self.db_path + '.maintenance.lock'
        self.stats = {'started_at': self.started.isoformat(), 'phases': {}}

    def _phase(self, name, function):
        start = time.perf_counter()
        self.progress(f"[MIGRATE] {name}...")
        result = function()
        self.stats['phases'][name] = round(time.perf_counter() - start, 3)
        return result

    def _connect(self, path):
        conn = sqlite3.connect(path, timeout=60)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=60000')
        conn.create_function('staging_rel', 1, _staging_relative, deterministic=True)
        conn.create_function('staging_session', 1, _staging_session_id,
                             deterministic=True)
        conn.create_function(
            'fallback_canonical', 2, _fallback_canonical, deterministic=True)
        conn.create_function(
            'catalog_key', 5, _file_record_key, deterministic=True)
        return conn

    def _checkpoint(self, conn, phase):
        conn.execute(
            """INSERT INTO schema_migrations(name,completed_at)
               VALUES (?,?) ON CONFLICT(name) DO UPDATE SET
               completed_at=excluded.completed_at""",
            (phase, datetime.now().isoformat()),
        )
        conn.commit()

    def _done(self, conn, phase):
        try:
            return conn.execute(
                'SELECT 1 FROM schema_migrations WHERE name=?', (phase,)
            ).fetchone() is not None
        except sqlite3.OperationalError:
            return False

    def _prepare(self, conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations(
            name TEXT PRIMARY KEY, completed_at DATETIME NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS archive_runs(
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_label TEXT NOT NULL,tape_label TEXT NOT NULL
              REFERENCES tapes(volume_label),
            session_kind TEXT NOT NULL DEFAULT 'legacy',session_id INTEGER,
            started_at DATETIME NOT NULL,completed_at DATETIME,
            UNIQUE(run_label,tape_label))""")
        columns = {row[1] for row in conn.execute('PRAGMA table_info(files_index)')}
        if 'archive_run_id' not in columns:
            conn.execute('ALTER TABLE files_index ADD COLUMN archive_run_id INTEGER '
                         'REFERENCES archive_runs(run_id)')
        if 'source_host' not in columns:
            conn.execute("ALTER TABLE files_index ADD COLUMN source_host TEXT "
                         "NOT NULL DEFAULT 'so02'")
        if conn.execute('PRAGMA user_version').fetchone()[0] >= SCHEMA_VERSION:
            now = datetime.now().isoformat()
            conn.executemany(
                """INSERT INTO schema_migrations(name,completed_at) VALUES (?,?)
                   ON CONFLICT(name) DO NOTHING""",
                ((name, now) for name in (
                    'v2_canonical_paths', 'v2_remote_storage', 'v2_catalog')),
            )
        conn.commit()

    def _canonicalize(self, conn):
        phase = 'v2_canonical_paths'
        if self._done(conn, phase):
            return
        conn.execute('DROP TABLE IF EXISTS migration_path_map_session')
        conn.execute('DROP TABLE IF EXISTS migration_path_map_tape')
        conn.execute("""CREATE TABLE migration_path_map_session(
            session_id INTEGER NOT NULL,
            local_rel_path TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            PRIMARY KEY(session_id,local_rel_path)) WITHOUT ROWID""")
        conn.execute("""CREATE TABLE migration_path_map_tape(
            tape_label TEXT NOT NULL,
            local_rel_path TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            PRIMARY KEY(tape_label,local_rel_path)) WITHOUT ROWID""")
        ambiguous = conn.execute("""
            SELECT COUNT(*) FROM (
              SELECT m.session_id,m.local_rel_path
              FROM remote_manifest m
              WHERE m.local_rel_path IS NOT NULL
              GROUP BY m.session_id,m.local_rel_path
              HAVING COUNT(DISTINCT m.remote_path)>1)
        """).fetchone()[0]
        if ambiguous:
            raise RuntimeError(f"{ambiguous} ambiguous manifest path mappings")
        conn.execute("""
            INSERT INTO migration_path_map_session(session_id,local_rel_path,remote_path)
            SELECT m.session_id,m.local_rel_path,MIN(m.remote_path)
            FROM remote_manifest m
            WHERE m.local_rel_path IS NOT NULL
            GROUP BY m.session_id,m.local_rel_path
        """)
        conn.execute("""
            INSERT INTO migration_path_map_tape(tape_label,local_rel_path,remote_path)
            SELECT s.tape_label,m.local_rel_path,MIN(m.remote_path)
            FROM remote_manifest m JOIN remote_sessions s USING(session_id)
            WHERE m.local_rel_path IS NOT NULL
            GROUP BY s.tape_label,m.local_rel_path
            HAVING COUNT(DISTINCT m.remote_path)=1
        """)
        ambiguous_fallback = conn.execute("""
            SELECT COUNT(*)
            FROM files_index f
            WHERE staging_rel(f.original_path) IS NOT NULL
              AND staging_session(f.original_path) IS NULL
              AND (
                SELECT COUNT(DISTINCT m.remote_path)
                FROM remote_manifest m JOIN remote_sessions s USING(session_id)
                WHERE s.tape_label=f.tape_label
                  AND m.local_rel_path=staging_rel(f.original_path)
              ) > 1
        """).fetchone()[0]
        if ambiguous_fallback:
            raise RuntimeError(
                f"{ambiguous_fallback} staging path(s) need ambiguous "
                "legacy tape-level fallback mappings")
        before = conn.execute(
            'SELECT COUNT(*) FROM files_index WHERE staging_rel(original_path) IS NOT NULL'
        ).fetchone()[0]
        conn.execute("""
            UPDATE files_index AS f
            SET original_path=COALESCE(
              (SELECT remote_path FROM migration_path_map_session p
               WHERE p.session_id=staging_session(f.original_path)
                 AND p.local_rel_path=staging_rel(f.original_path)),
              (SELECT remote_path FROM migration_path_map_tape p
               WHERE p.tape_label=f.tape_label
                 AND p.local_rel_path=staging_rel(f.original_path)),
              fallback_canonical(f.original_path,?))
            WHERE staging_rel(f.original_path) IS NOT NULL
        """, (self.source_root,))
        unmapped = conn.execute(
            'SELECT COUNT(*) FROM files_index WHERE original_path IS NULL'
        ).fetchone()[0]
        remaining = conn.execute(
            'SELECT COUNT(*) FROM files_index WHERE staging_rel(original_path) IS NOT NULL'
        ).fetchone()[0]
        if unmapped or remaining:
            raise RuntimeError(
                f"canonical path repair incomplete: unmapped={unmapped}, remaining={remaining}")
        duplicates = conn.execute("""
            SELECT COALESCE(SUM(n-1),0) FROM (
              SELECT COUNT(*) n FROM files_index
              GROUP BY original_path,tape_label,
                       COALESCE(local_session_id,-1),COALESCE(local_chunk_index,-1)
              HAVING n>1)
        """).fetchone()[0]
        if duplicates:
            raise RuntimeError(
                f"canonicalization produced {duplicates} duplicate catalog rows")
        self.stats['canonical_paths_repaired'] = before
        conn.execute('DROP TABLE migration_path_map_session')
        conn.execute('DROP TABLE migration_path_map_tape')
        self._checkpoint(conn, phase)

    def _create_remote_v2(self, conn):
        phase = 'v2_remote_storage'
        if self._done(conn, phase):
            return
        conn.execute('PRAGMA foreign_keys=OFF')
        conn.execute('ALTER TABLE remote_sessions RENAME TO remote_sessions_legacy')
        conn.executescript("""
            CREATE TABLE remote_sessions(
              session_id INTEGER PRIMARY KEY AUTOINCREMENT,session_label TEXT NOT NULL,
              remote_host TEXT NOT NULL,remote_user TEXT NOT NULL,remote_path TEXT NOT NULL,
              tape_label TEXT NOT NULL,staging_dir TEXT NOT NULL,total_files INTEGER DEFAULT 0,
              total_bytes INTEGER DEFAULT 0,chunk_count INTEGER DEFAULT 0,
              plan_id INTEGER REFERENCES remote_plans(plan_id),
              created_at DATETIME NOT NULL,completed_at DATETIME,status TEXT NOT NULL);
            CREATE TABLE remote_snapshots(
              snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,remote_host TEXT NOT NULL,
              remote_path TEXT NOT NULL,fingerprint BLOB NOT NULL UNIQUE,
              total_files INTEGER NOT NULL,total_bytes INTEGER NOT NULL,created_at DATETIME NOT NULL);
            CREATE TABLE remote_snapshot_files(
              snapshot_file_id INTEGER PRIMARY KEY AUTOINCREMENT,snapshot_id INTEGER NOT NULL
                REFERENCES remote_snapshots(snapshot_id) ON DELETE CASCADE,
              remote_path TEXT NOT NULL,file_size_bytes INTEGER NOT NULL,
              UNIQUE(snapshot_id,remote_path));
            CREATE TABLE remote_plans(
              plan_id INTEGER PRIMARY KEY AUTOINCREMENT,snapshot_id INTEGER NOT NULL
                REFERENCES remote_snapshots(snapshot_id) ON DELETE CASCADE,
              fingerprint BLOB NOT NULL UNIQUE,chunk_count INTEGER NOT NULL,created_at DATETIME NOT NULL);
            CREATE TABLE remote_plan_files(
              plan_file_id INTEGER PRIMARY KEY AUTOINCREMENT,plan_id INTEGER NOT NULL
                REFERENCES remote_plans(plan_id) ON DELETE CASCADE,
              snapshot_file_id INTEGER NOT NULL REFERENCES remote_snapshot_files(snapshot_file_id),
              chunk_index INTEGER NOT NULL,ordinal INTEGER NOT NULL,
              UNIQUE(plan_id,snapshot_file_id));
            CREATE INDEX idx_remote_plan_chunk ON remote_plan_files(plan_id,chunk_index,ordinal);
            CREATE TABLE remote_chunks(
              session_id INTEGER NOT NULL REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
              chunk_index INTEGER NOT NULL,status TEXT NOT NULL,error_msg TEXT,updated_at DATETIME,
              PRIMARY KEY(session_id,chunk_index)) WITHOUT ROWID;
            CREATE TABLE remote_file_state(
              session_id INTEGER NOT NULL REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
              plan_file_id INTEGER NOT NULL REFERENCES remote_plan_files(plan_file_id),
              status TEXT,local_rel_path TEXT,error_msg TEXT,updated_at DATETIME,
              PRIMARY KEY(session_id,plan_file_id)) WITHOUT ROWID;
        """)
        now = datetime.now().isoformat()
        sessions = conn.execute(
            "SELECT * FROM remote_sessions_legacy ORDER BY session_id"
        ).fetchall()
        for session in sessions:
            session_id = session['session_id']
            manifest_by_path = conn.execute("""
                SELECT remote_path,file_size_bytes
                FROM remote_manifest
                WHERE session_id=?
                ORDER BY remote_path
            """, (session_id,)).fetchall()
            manifest_by_order = conn.execute("""
                SELECT manifest_id,chunk_index,remote_path,file_size_bytes
                FROM remote_manifest
                WHERE session_id=?
                ORDER BY manifest_id
            """, (session_id,)).fetchall()

            snapshot_hash = hashlib.sha256()
            for identity in (session['remote_host'], session['remote_path']):
                raw = str(identity).encode('utf-8', errors='surrogatepass')
                snapshot_hash.update(len(raw).to_bytes(8, 'big'))
                snapshot_hash.update(raw)
            for row in manifest_by_path:
                raw = row['remote_path'].encode('utf-8', errors='surrogatepass')
                snapshot_hash.update(len(raw).to_bytes(8, 'big'))
                snapshot_hash.update(raw)
                snapshot_hash.update(
                    int(row['file_size_bytes']).to_bytes(8, 'big'))
            snapshot_fp = snapshot_hash.digest()

            plan_hash = hashlib.sha256(snapshot_fp)
            for row in manifest_by_order:
                raw = row['remote_path'].encode('utf-8', errors='surrogatepass')
                plan_hash.update(int(row['chunk_index']).to_bytes(4, 'big'))
                plan_hash.update(len(raw).to_bytes(8, 'big'))
                plan_hash.update(raw)
                plan_hash.update(int(row['file_size_bytes']).to_bytes(8, 'big'))
            plan_fp = plan_hash.digest()

            conn.execute("""INSERT INTO remote_snapshots
                (remote_host,remote_path,fingerprint,total_files,total_bytes,created_at)
                VALUES (?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING""",
                (session['remote_host'], session['remote_path'], snapshot_fp,
                 session['total_files'] or len(manifest_by_order),
                 session['total_bytes'] or sum(
                     int(row['file_size_bytes']) for row in manifest_by_order),
                 now))
            snapshot_id = conn.execute(
                "SELECT snapshot_id FROM remote_snapshots WHERE fingerprint=?",
                (snapshot_fp,)).fetchone()[0]
            existing = conn.execute(
                "SELECT COUNT(*) FROM remote_snapshot_files WHERE snapshot_id=?",
                (snapshot_id,)).fetchone()[0]
            if not existing:
                conn.executemany(
                    """INSERT INTO remote_snapshot_files
                       (snapshot_id,remote_path,file_size_bytes) VALUES (?,?,?)""",
                    ((snapshot_id, row['remote_path'], row['file_size_bytes'])
                     for row in manifest_by_path))

            conn.execute("""INSERT INTO remote_plans
                (snapshot_id,fingerprint,chunk_count,created_at)
                VALUES (?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING""",
                (snapshot_id, plan_fp,
                 session['chunk_count'] or len(
                     {int(row['chunk_index']) for row in manifest_by_order}),
                 now))
            plan_id = conn.execute(
                "SELECT plan_id FROM remote_plans WHERE fingerprint=?",
                (plan_fp,)).fetchone()[0]
            existing = conn.execute(
                "SELECT COUNT(*) FROM remote_plan_files WHERE plan_id=?",
                (plan_id,)).fetchone()[0]
            if not existing:
                snapshot_ids = dict(conn.execute(
                    """SELECT remote_path,snapshot_file_id
                       FROM remote_snapshot_files WHERE snapshot_id=?""",
                    (snapshot_id,)).fetchall())
                conn.executemany(
                    """INSERT INTO remote_plan_files
                       (plan_id,snapshot_file_id,chunk_index,ordinal)
                       VALUES (?,?,?,?)""",
                    ((plan_id, snapshot_ids[row['remote_path']],
                      row['chunk_index'], row['manifest_id'])
                     for row in manifest_by_order))

            conn.execute("""INSERT INTO remote_sessions
                (session_id,session_label,remote_host,remote_user,remote_path,tape_label,
                 staging_dir,total_files,total_bytes,chunk_count,plan_id,created_at,
                 completed_at,status)
                SELECT session_id,session_label,remote_host,remote_user,remote_path,
                       tape_label,staging_dir,total_files,total_bytes,chunk_count,?,
                       created_at,completed_at,status
                FROM remote_sessions_legacy WHERE session_id=?""",
                (plan_id, session_id))
            conn.execute("""INSERT INTO remote_chunks(session_id,chunk_index,status,updated_at)
                SELECT session_id,chunk_index,
                       CASE WHEN MIN(chunk_status)='done' AND MAX(chunk_status)='done'
                            THEN 'done'
                            ELSE COALESCE(MAX(chunk_status),'pending') END,
                       MAX(updated_at)
                FROM remote_manifest
                WHERE session_id=?
                GROUP BY session_id,chunk_index""", (session_id,))
            conn.execute("""INSERT INTO remote_file_state
                (session_id,plan_file_id,status,local_rel_path,error_msg,updated_at)
                SELECT m.session_id,pf.plan_file_id,m.status,m.local_rel_path,
                       m.error_msg,m.updated_at
                FROM remote_manifest m
                JOIN remote_snapshot_files sf
                  ON sf.snapshot_id=? AND sf.remote_path=m.remote_path
                JOIN remote_plan_files pf
                  ON pf.plan_id=? AND pf.snapshot_file_id=sf.snapshot_file_id
                WHERE m.session_id=?
                  AND COALESCE(m.status,'pending')!='pending'""",
                (snapshot_id, plan_id, session_id))
        conn.execute('DROP TABLE remote_manifest')
        conn.execute('DROP TABLE remote_sessions_legacy')
        conn.execute('PRAGMA foreign_keys=ON')
        self.stats['remote_snapshot_files'] = conn.execute(
            'SELECT COUNT(*) FROM remote_snapshot_files').fetchone()[0]
        self.stats['source_missing'] = conn.execute(
            "SELECT COUNT(*) FROM remote_file_state WHERE status='source_missing'"
        ).fetchone()[0]
        self._checkpoint(conn, phase)

    def _normalize_catalog(self, conn):
        phase = 'v2_catalog'
        if self._done(conn, phase):
            return
        conn.execute('DROP INDEX IF EXISTS idx_files_record_key')
        conn.execute("""INSERT INTO archive_bundles(tape_label,tape_path)
            SELECT DISTINCT tape_label,container_name FROM files_index
            WHERE container_name IS NOT NULL
            ON CONFLICT(tape_label,tape_path) DO NOTHING""")
        conn.execute("""UPDATE files_index AS f SET bundle_id=(
            SELECT bundle_id FROM archive_bundles b
            WHERE b.tape_label=f.tape_label AND b.tape_path=f.container_name)
            WHERE container_name IS NOT NULL""")
        conn.execute("""INSERT INTO archive_runs
            (run_label,tape_label,session_kind,started_at,completed_at)
            SELECT DATE(backup_date)||':'||tape_label,tape_label,'legacy',
                   MIN(backup_date),MAX(backup_date)
            FROM files_index GROUP BY DATE(backup_date),tape_label
            ON CONFLICT(run_label,tape_label) DO NOTHING""")
        conn.execute("""UPDATE files_index AS f SET archive_run_id=(
            SELECT run_id FROM archive_runs r
            WHERE r.run_label=DATE(f.backup_date)||':'||f.tape_label
              AND r.tape_label=f.tape_label)""")
        conn.execute(
            "UPDATE files_index SET source_host='so02' "
            "WHERE source_host IS NULL OR source_host=''")
        conn.execute("""UPDATE files_index SET
            record_key=catalog_key(original_path,tape_label,local_session_id,local_chunk_index,source_host),
            file_name=NULL,container_name=NULL,backup_date=NULL""")
        conn.execute('DROP INDEX IF EXISTS idx_files_dedup_expr')
        conn.execute('DROP INDEX IF EXISTS idx_files_bundle_id')
        conn.execute('DROP INDEX IF EXISTS idx_files_local_session_chunk')
        conn.executescript("""
            CREATE UNIQUE INDEX idx_files_record_key ON files_index(record_key)
              WHERE record_key IS NOT NULL;
            CREATE INDEX idx_files_bundle_id ON files_index(bundle_id)
              WHERE bundle_id IS NOT NULL;
            CREATE INDEX idx_files_local_session_chunk
              ON files_index(local_session_id,local_chunk_index,tape_label)
              WHERE local_session_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_archive_run
              ON files_index(archive_run_id) WHERE archive_run_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_source_host
              ON files_index(source_host,tape_label,original_path);
        """)
        conn.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
        self._checkpoint(conn, phase)

    def _validate(self, path):
        conn = self._connect(path)
        result = {
            'quick_check': conn.execute('PRAGMA quick_check').fetchone()[0],
            'foreign_key_violations': len(conn.execute('PRAGMA foreign_key_check').fetchall()),
            'files': conn.execute('SELECT COUNT(*) FROM files_index').fetchone()[0],
            'tapes': conn.execute('SELECT COUNT(*) FROM tapes').fetchone()[0],
            'staging_paths': conn.execute(
                'SELECT COUNT(*) FROM files_index WHERE staging_rel(original_path) IS NOT NULL'
            ).fetchone()[0],
            'remote_sessions': conn.execute('SELECT COUNT(*) FROM remote_sessions').fetchone()[0],
            'snapshot_files': conn.execute('SELECT COUNT(*) FROM remote_snapshot_files').fetchone()[0],
            'chunks': conn.execute('SELECT COUNT(*) FROM remote_chunks').fetchone()[0],
            'source_missing': conn.execute(
                "SELECT COUNT(*) FROM remote_file_state WHERE status='source_missing'"
            ).fetchone()[0],
        }
        conn.close()
        expected = {
            'quick_check': 'ok', 'foreign_key_violations': 0,
            'staging_paths': 0,
        }
        for key, value in expected.items():
            if result[key] != value:
                raise RuntimeError(f"validation failed: {key}={result[key]!r}")
        return result

    def run(self):
        before_stat = os.stat(self.db_path)
        required = os.path.getsize(self.db_path) * 3 + 1024**3
        if shutil.disk_usage(os.path.dirname(self.db_path)).free < required:
            raise RuntimeError('insufficient free disk for safe copy-and-swap migration')
        with maintenance_lock(self.lock_path, 'optimize-db'):
            self._phase('copy source database', self._copy_database)
            conn = self._connect(self.work_path)
            try:
                self._prepare(conn)
                self._phase('repair canonical SOURCE paths', lambda: self._canonicalize(conn))
                self._phase('normalize remote snapshots and state', lambda: self._create_remote_v2(conn))
                self._phase('normalize file catalog and indexes', lambda: self._normalize_catalog(conn))
                conn.execute('ANALYZE')
                conn.commit()
            finally:
                conn.close()
            self._phase('validate migrated working copy', lambda: self._validate(self.work_path))
            compact_sql = "VACUUM INTO '" + self.compact_path.replace("'", "''") + "'"
            self._phase('compact optimized copy', lambda: self._vacuum(compact_sql))
            validation = self._phase(
                'validate compact copy', lambda: self._validate(self.compact_path))
            current_stat = os.stat(self.db_path)
            if (current_stat.st_size, current_stat.st_mtime_ns) != (
                    before_stat.st_size, before_stat.st_mtime_ns):
                raise RuntimeError('source database changed during migration; refusing swap')
            os.replace(self.db_path, self.rollback_path)
            try:
                os.replace(self.compact_path, self.db_path)
                final_validation = self._validate(self.db_path)
            except BaseException:
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                os.replace(self.rollback_path, self.db_path)
                raise
            self.stats['validation'] = final_validation
            self.stats['before_bytes'] = before_stat.st_size
            self.stats['after_bytes'] = os.path.getsize(self.db_path)
            self.stats['reduction_pct'] = round(
                (1 - self.stats['after_bytes'] / before_stat.st_size) * 100, 2)
            report_dir = os.path.join(os.path.dirname(self.db_path), 'backup_logs')
            self.stats['report_path'] = append_maintenance_summary_row(
                report_dir, 'db_optimization', self.stats)
            os.remove(self.rollback_path)
            os.remove(self.work_path)
            self.progress(json.dumps(self.stats, indent=2))
            return self.stats

    def _copy_database(self):
        source = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        target = sqlite3.connect(self.work_path)
        try:
            source.backup(target, pages=8192)
        finally:
            target.close()
            source.close()

    def _vacuum(self, sql):
        conn = self._connect(self.work_path)
        try:
            conn.execute(sql)
        finally:
            conn.close()


class HashlessOriginOptimizer:
    """Copy/swap migration that drops file-content hash columns and adds origin."""

    def __init__(self, db_path, progress=print):
        self.db_path = os.path.abspath(db_path)
        self.progress = progress or (lambda _message: None)
        self.started = datetime.now()
        stamp = self.started.strftime('%Y%m%d_%H%M%S')
        self.work_path = self.db_path + f'.hashless_{stamp}.work'
        self.compact_path = self.db_path + f'.hashless_{stamp}.compact'
        self.rollback_path = self.db_path + f'.pre_hashless_{stamp}.bak'
        self.lock_path = self.db_path + '.maintenance.lock'
        self.stats = {'started_at': self.started.isoformat(), 'phases': {}}

    def _phase(self, name, function):
        start = time.perf_counter()
        self.progress(f"[HASHLESS] {name}...")
        result = function()
        self.stats['phases'][name] = round(time.perf_counter() - start, 3)
        return result

    def _connect(self, path):
        conn = sqlite3.connect(path, timeout=60)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=60000')
        conn.create_function('catalog_key', 5, _file_record_key,
                             deterministic=True)
        return conn

    def _copy_database(self):
        source = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        target = sqlite3.connect(self.work_path)
        try:
            source.backup(target, pages=8192)
        finally:
            target.close()
            source.close()

    @staticmethod
    def _columns(conn):
        return {row['name'] for row in conn.execute('PRAGMA table_info(files_index)')}

    def _apply_hashless_origin(self, conn):
        columns = self._columns(conn)
        if 'source_host' not in columns:
            conn.execute("ALTER TABLE files_index ADD COLUMN source_host TEXT "
                         "NOT NULL DEFAULT 'so02'")
        conn.execute(
            "UPDATE files_index SET source_host='so02' "
            "WHERE source_host IS NULL OR source_host=''")

        for index_name in (
                'idx_files_record_key', 'idx_files_source_host',
                'idx_files_bundle_id', 'idx_files_local_session_chunk',
                'idx_files_archive_run'):
            conn.execute(f'DROP INDEX IF EXISTS {index_name}')
        for row in conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='index' AND tbl_name='files_index'
                  AND sql IS NOT NULL
                  AND (sql LIKE '%file_hash%' OR sql LIKE '%file_hash_blob%')
        """):
            index_name = '"' + str(row['name']).replace('"', '""') + '"'
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")

        columns = self._columns(conn)
        for column in ('file_hash_blob', 'file_hash'):
            if column in columns:
                conn.execute(f'ALTER TABLE files_index DROP COLUMN {column}')
                columns.remove(column)

        conn.execute("""UPDATE files_index SET record_key=
            catalog_key(original_path,tape_label,local_session_id,
                        local_chunk_index,source_host)""")
        conn.executescript("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_files_record_key
              ON files_index(record_key) WHERE record_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_bundle_id
              ON files_index(bundle_id) WHERE bundle_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_local_session_chunk
              ON files_index(local_session_id,local_chunk_index,tape_label)
              WHERE local_session_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_files_source_host
              ON files_index(source_host,tape_label,original_path);
        """)
        columns = self._columns(conn)
        if 'archive_run_id' in columns:
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_files_archive_run
                ON files_index(archive_run_id)
                WHERE archive_run_id IS NOT NULL""")
        if catalog_v3_available(conn):
            ensure_catalog_schema(conn)
            conn.execute("INSERT INTO files_index_fts(files_index_fts) VALUES ('rebuild')")
        conn.execute('ANALYZE')
        conn.commit()

    def _validate(self, path, expected_files=None, allow_hash_columns=False):
        conn = self._connect(path)
        try:
            columns = self._columns(conn)
            result = {
                'quick_check': conn.execute('PRAGMA quick_check').fetchone()[0],
                'foreign_key_violations':
                    len(conn.execute('PRAGMA foreign_key_check').fetchall()),
                'files': conn.execute('SELECT COUNT(*) FROM files_index').fetchone()[0],
                'hash_columns_present':
                    sorted(columns.intersection({'file_hash', 'file_hash_blob'})),
            }
            if 'source_host' in columns:
                result['source_hosts'] = [row[0] for row in conn.execute(
                    'SELECT DISTINCT source_host FROM files_index ORDER BY source_host')]
                result['source_host_missing'] = conn.execute(
                    """SELECT COUNT(*) FROM files_index
                       WHERE source_host IS NULL OR source_host=''"""
                ).fetchone()[0]
            else:
                result['source_hosts'] = []
                result['source_host_missing'] = result['files']
            if catalog_v3_available(conn):
                result['fts_rows'] = conn.execute(
                    'SELECT COUNT(*) FROM files_index_fts').fetchone()[0]
        finally:
            conn.close()
        if result['quick_check'] != 'ok':
            raise RuntimeError(f"hashless validation failed: {result!r}")
        if result['foreign_key_violations']:
            raise RuntimeError(f"hashless validation failed: {result!r}")
        if result['hash_columns_present'] and not allow_hash_columns:
            raise RuntimeError(f"hashless validation failed: {result!r}")
        if result['source_host_missing'] and not allow_hash_columns:
            raise RuntimeError(f"hashless validation failed: {result!r}")
        if expected_files is not None and result['files'] != expected_files:
            raise RuntimeError(
                f"hashless validation failed: files={result['files']} "
                f"expected={expected_files}")
        if result.get('fts_rows') is not None and result['fts_rows'] != result['files']:
            raise RuntimeError(f"hashless validation failed: {result!r}")
        return result

    def _vacuum(self, sql):
        conn = self._connect(self.work_path)
        try:
            conn.execute(sql)
        finally:
            conn.close()

    def run(self):
        before_stat = os.stat(self.db_path)
        before_files = self._validate(
            self.db_path, allow_hash_columns=True)['files']
        required = os.path.getsize(self.db_path) * 3 + 1024**3
        if shutil.disk_usage(os.path.dirname(self.db_path)).free < required:
            raise RuntimeError('insufficient free disk for safe hashless migration')
        with maintenance_lock(self.lock_path, 'hashless-origin-migrate'):
            self._phase('copy source database', self._copy_database)
            conn = self._connect(self.work_path)
            try:
                self._phase('drop hash columns and add source origin',
                            lambda: self._apply_hashless_origin(conn))
            finally:
                conn.close()
            self._phase('validate working copy',
                        lambda: self._validate(self.work_path, before_files))
            compact_sql = "VACUUM INTO '" + self.compact_path.replace("'", "''") + "'"
            self._phase('compact hashless copy', lambda: self._vacuum(compact_sql))
            self._phase('validate compact copy',
                        lambda: self._validate(self.compact_path, before_files))
            current_stat = os.stat(self.db_path)
            if (current_stat.st_size, current_stat.st_mtime_ns) != (
                    before_stat.st_size, before_stat.st_mtime_ns):
                raise RuntimeError('source database changed during migration; refusing swap')
            os.replace(self.db_path, self.rollback_path)
            try:
                os.replace(self.compact_path, self.db_path)
                final_validation = self._validate(self.db_path, before_files)
            except BaseException:
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                os.replace(self.rollback_path, self.db_path)
                raise
            self.stats['validation'] = final_validation
            self.stats['before_bytes'] = before_stat.st_size
            self.stats['after_bytes'] = os.path.getsize(self.db_path)
            self.stats['reclaimed_bytes'] = (
                self.stats['before_bytes'] - self.stats['after_bytes'])
            report_dir = os.path.join(os.path.dirname(self.db_path), 'backup_logs')
            self.stats['report_path'] = append_maintenance_summary_row(
                report_dir, 'db_hashless', self.stats)
            os.remove(self.rollback_path)
            if os.path.exists(self.work_path):
                os.remove(self.work_path)
            self.progress(json.dumps(self.stats, indent=2))
            return self.stats


class CatalogV3Optimizer:
    """Build catalog-browser indexes in a copy, then atomically install it."""

    def __init__(self, db_path, progress=print, batch_size=5000):
        self.db_path = os.path.abspath(db_path)
        self.progress = progress or (lambda _message: None)
        self.batch_size = max(1, int(batch_size))
        self.started = datetime.now()
        stamp = self.started.strftime('%Y%m%d_%H%M%S')
        self.work_path = self.db_path + f'.catalog_v3_{stamp}.work'
        self.compact_path = self.db_path + f'.catalog_v3_{stamp}.compact'
        self.rollback_path = self.db_path + f'.pre_catalog_v3_{stamp}.bak'
        self.lock_path = self.db_path + '.maintenance.lock'
        self.stats = {'started_at': self.started.isoformat(), 'phases': {}}

    def _phase(self, name, function):
        start = time.perf_counter()
        self.progress(f"[CATALOG-V3] {name}...")
        result = function()
        self.stats['phases'][name] = round(time.perf_counter() - start, 3)
        return result

    def _connect(self, path):
        conn = sqlite3.connect(path, timeout=60)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=60000')
        return conn

    def _checkpoint(self, conn, phase):
        conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations(
            name TEXT PRIMARY KEY, completed_at DATETIME NOT NULL)""")
        conn.execute(
            """INSERT INTO schema_migrations(name,completed_at)
               VALUES (?,?) ON CONFLICT(name) DO UPDATE SET
               completed_at=excluded.completed_at""",
            (phase, datetime.now().isoformat()),
        )
        conn.commit()

    def _done(self, conn, phase):
        try:
            return conn.execute(
                'SELECT 1 FROM schema_migrations WHERE name=?', (phase,)
            ).fetchone() is not None
        except sqlite3.OperationalError:
            return False

    def _copy_database(self):
        source = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        target = sqlite3.connect(self.work_path)
        try:
            source.backup(target, pages=8192)
        finally:
            target.close()
            source.close()

    def _drop_fts_triggers(self, conn):
        conn.executescript("""
            DROP TRIGGER IF EXISTS files_index_catalog_fts_ai;
            DROP TRIGGER IF EXISTS files_index_catalog_fts_ad;
            DROP TRIGGER IF EXISTS files_index_catalog_fts_au;
        """)

    def _apply_catalog_v3(self, conn):
        if self._done(conn, CATALOG_MIGRATION_NAME) and catalog_v3_available(conn):
            self.stats['already_current'] = True
            return
        user_version = conn.execute('PRAGMA user_version').fetchone()[0]
        legacy_remote = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_manifest'"
        ).fetchone()
        if user_version < SCHEMA_VERSION or legacy_remote:
            raise RuntimeError(
                "[DB] Run the existing v2 optimizer before catalog-v3 migration.")
        columns = {row['name'] for row in conn.execute('PRAGMA table_info(files_index)')}
        if 'source_host' not in columns:
            conn.execute("ALTER TABLE files_index ADD COLUMN source_host TEXT "
                         "NOT NULL DEFAULT 'so02'")
        conn.execute(
            "UPDATE files_index SET source_host='so02' "
            "WHERE source_host IS NULL OR source_host=''")
        ensure_catalog_schema(conn)
        self._drop_fts_triggers(conn)
        conn.commit()
        self._index_catalog_rows(conn)

    def _rebuild_catalog_v3(self, conn):
        """Rebuild the directory tree in place from the current path logic."""
        ensure_catalog_schema(conn)
        self._drop_fts_triggers(conn)
        conn.execute(
            'UPDATE files_index SET directory_id=NULL, catalog_name=NULL, '
            'catalog_backup_date=NULL')
        conn.execute('DELETE FROM catalog_directories')
        conn.commit()
        self._index_catalog_rows(conn)

    def _index_catalog_rows(self, conn):
        total = conn.execute('SELECT COUNT(*) FROM files_index').fetchone()[0]
        self.stats['files'] = total
        cache = {}
        last_file_id = 0
        processed = 0
        while True:
            rows = conn.execute(
                """SELECT f.file_id, f.tape_label, f.original_path, f.stored_path,
                          f.backup_date, f.source_host,
                          r.started_at AS run_started_at
                   FROM files_index f
                   LEFT JOIN archive_runs r ON r.run_id=f.archive_run_id
                   WHERE f.file_id > ?
                   ORDER BY f.file_id
                   LIMIT ?""",
                (last_file_id, self.batch_size),
            ).fetchall()
            if not rows:
                break
            updates = []
            for row in rows:
                directory_id, name, backup_date = catalog_values_for_file(
                    conn, row, cache)
                updates.append((directory_id, name, backup_date, row['file_id']))
                last_file_id = row['file_id']
            conn.executemany(
                """UPDATE files_index
                   SET directory_id=?, catalog_name=?, catalog_backup_date=?
                   WHERE file_id=?""",
                updates,
            )
            conn.commit()
            processed += len(rows)
            if processed % (self.batch_size * 20) == 0 or processed == total:
                self.progress(
                    f"[CATALOG-V3] indexed {processed:,}/{total:,} file rows")

        conn.execute("INSERT INTO files_index_fts(files_index_fts) VALUES ('rebuild')")
        conn.execute(f'PRAGMA user_version={CATALOG_SCHEMA_VERSION}')
        self._checkpoint(conn, CATALOG_MIGRATION_NAME)
        ensure_catalog_schema(conn)
        conn.execute('ANALYZE')
        conn.commit()
        self.stats['directories'] = conn.execute(
            'SELECT COUNT(*) FROM catalog_directories').fetchone()[0]
        self.stats['fts_rows'] = conn.execute(
            'SELECT COUNT(*) FROM files_index_fts').fetchone()[0]

    def _validate(self, path):
        conn = self._connect(path)
        try:
            result = {
                'quick_check': conn.execute('PRAGMA quick_check').fetchone()[0],
                'foreign_key_violations':
                    len(conn.execute('PRAGMA foreign_key_check').fetchall()),
                'files': conn.execute('SELECT COUNT(*) FROM files_index').fetchone()[0],
                'catalog_names': conn.execute(
                    "SELECT COUNT(*) FROM files_index WHERE COALESCE(catalog_name,'') != ''"
                ).fetchone()[0],
                'directories': conn.execute(
                    'SELECT COUNT(*) FROM catalog_directories').fetchone()[0],
                'fts_rows': conn.execute(
                    'SELECT COUNT(*) FROM files_index_fts').fetchone()[0],
                'user_version': conn.execute('PRAGMA user_version').fetchone()[0],
                'migration_recorded': self._done(conn, CATALOG_MIGRATION_NAME),
            }
        finally:
            conn.close()
        expected = {
            'quick_check': 'ok',
            'foreign_key_violations': 0,
            'user_version': CATALOG_SCHEMA_VERSION,
            'migration_recorded': True,
        }
        for key, value in expected.items():
            if result[key] != value:
                raise RuntimeError(f"catalog-v3 validation failed: {key}={result[key]!r}")
        if result['catalog_names'] != result['files']:
            raise RuntimeError(
                "catalog-v3 validation failed: not every file has catalog_name")
        if result['fts_rows'] != result['files']:
            raise RuntimeError(
                "catalog-v3 validation failed: FTS row count does not match files")
        return result

    def _catalog_signature(self, path):
        conn = self._connect(path)
        try:
            return {
                'files': conn.execute(
                    'SELECT COUNT(*) FROM files_index').fetchone()[0],
                'file_bytes': conn.execute(
                    'SELECT COALESCE(SUM(file_size_bytes),0) FROM files_index'
                ).fetchone()[0],
                'file_id_sum': conn.execute(
                    'SELECT COALESCE(SUM(file_id),0) FROM files_index'
                ).fetchone()[0],
                'record_keys': conn.execute(
                    'SELECT COUNT(record_key) FROM files_index').fetchone()[0],
                'tapes': [dict(row) for row in conn.execute(
                    """SELECT volume_label,total_capacity,used_space
                       FROM tapes ORDER BY volume_label""")],
                'archive_bundles': conn.execute(
                    'SELECT COUNT(*) FROM archive_bundles').fetchone()[0],
                'archive_runs': conn.execute(
                    'SELECT COUNT(*) FROM archive_runs').fetchone()[0],
            }
        finally:
            conn.close()

    @staticmethod
    def _require_same_catalog(before, after, label):
        keys = ('files', 'file_bytes', 'file_id_sum', 'record_keys',
                'tapes', 'archive_bundles', 'archive_runs')
        mismatches = {
            key: (before[key], after[key])
            for key in keys
            if before[key] != after[key]
        }
        if mismatches:
            raise RuntimeError(
                f"catalog-v3 validation failed: {label} changed permanent "
                f"catalog data: {mismatches!r}")

    def _vacuum(self, sql):
        conn = self._connect(self.work_path)
        try:
            conn.execute(sql)
        finally:
            conn.close()

    def rebuild(self):
        """Re-index the directory tree (e.g. after path-rooting logic changes)."""
        return self.run(build=self._rebuild_catalog_v3,
                        lock_label='catalog-v3-rebuild',
                        report_kind='db_catalog_v3_rebuild')

    def run(self, build=None, lock_label='catalog-v3-migrate',
            report_kind='db_catalog_v3'):
        build = build or self._apply_catalog_v3
        before_stat = os.stat(self.db_path)
        source_signature = self._catalog_signature(self.db_path)
        required = os.path.getsize(self.db_path) * 3 + 1024**3
        if shutil.disk_usage(os.path.dirname(self.db_path)).free < required:
            raise RuntimeError('insufficient free disk for safe catalog-v3 migration')
        with maintenance_lock(self.lock_path, lock_label):
            self._phase('copy source database', self._copy_database)
            conn = self._connect(self.work_path)
            try:
                self._phase('build catalog directory and search indexes',
                            lambda: build(conn))
            finally:
                conn.close()
            self._phase('validate catalog-v3 working copy',
                        lambda: self._validate(self.work_path))
            self._phase('compare working-copy catalog invariants',
                        lambda: self._require_same_catalog(
                            source_signature,
                            self._catalog_signature(self.work_path),
                            'working copy'))
            compact_sql = "VACUUM INTO '" + self.compact_path.replace("'", "''") + "'"
            self._phase('compact catalog-v3 copy', lambda: self._vacuum(compact_sql))
            self._phase('validate compact copy', lambda: self._validate(self.compact_path))
            self._phase('compare compact-copy catalog invariants',
                        lambda: self._require_same_catalog(
                            source_signature,
                            self._catalog_signature(self.compact_path),
                            'compact copy'))
            current_stat = os.stat(self.db_path)
            if (current_stat.st_size, current_stat.st_mtime_ns) != (
                    before_stat.st_size, before_stat.st_mtime_ns):
                raise RuntimeError('source database changed during migration; refusing swap')
            os.replace(self.db_path, self.rollback_path)
            try:
                os.replace(self.compact_path, self.db_path)
                final_validation = self._validate(self.db_path)
            except BaseException:
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                os.replace(self.rollback_path, self.db_path)
                raise
            final_signature = self._catalog_signature(self.db_path)
            self._require_same_catalog(source_signature, final_signature,
                                       'installed database')
            self.stats['validation'] = final_validation
            self.stats['catalog_signature'] = final_signature
            self.stats['before_bytes'] = before_stat.st_size
            self.stats['after_bytes'] = os.path.getsize(self.db_path)
            self.stats['reduction_pct'] = round(
                (1 - self.stats['after_bytes'] / before_stat.st_size) * 100, 2)
            report_dir = os.path.join(os.path.dirname(self.db_path), 'backup_logs')
            self.stats['report_path'] = append_maintenance_summary_row(
                report_dir, report_kind, self.stats)
            os.remove(self.rollback_path)
            if os.path.exists(self.work_path):
                os.remove(self.work_path)
            self.progress(json.dumps(self.stats, indent=2))
            return self.stats
