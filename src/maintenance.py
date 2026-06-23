"""Offline, resumable database-v2 optimization and canonical-path repair."""
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime

from .db import SCHEMA_VERSION, _file_record_key, _hash_to_blob


_STAGING_COMPONENT = re.compile(r"^_fetch(?:_s\d+)?_\d+$", re.IGNORECASE)


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
        conn.create_function(
            'fallback_canonical', 2, _fallback_canonical, deterministic=True)
        conn.create_function(
            'compact_hash', 1, _hash_to_blob, deterministic=True)
        conn.create_function(
            'catalog_key', 4, _file_record_key, deterministic=True)
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
        return conn.execute(
            'SELECT 1 FROM schema_migrations WHERE name=?', (phase,)
        ).fetchone() is not None

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
        conn.execute('DROP TABLE IF EXISTS migration_path_map')
        conn.execute("""CREATE TABLE migration_path_map(
            tape_label TEXT NOT NULL,
            local_rel_path TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            PRIMARY KEY(tape_label,local_rel_path)) WITHOUT ROWID""")
        ambiguous = conn.execute("""
            SELECT COUNT(*) FROM (
              SELECT s.tape_label,m.local_rel_path
              FROM remote_manifest m JOIN remote_sessions s USING(session_id)
              WHERE m.session_id IN (4,5,6) AND m.local_rel_path IS NOT NULL
              GROUP BY s.tape_label,m.local_rel_path
              HAVING COUNT(DISTINCT m.remote_path)>1)
        """).fetchone()[0]
        if ambiguous:
            raise RuntimeError(f"{ambiguous} ambiguous manifest path mappings")
        conn.execute("""
            INSERT INTO migration_path_map(tape_label,local_rel_path,remote_path)
            SELECT s.tape_label,m.local_rel_path,MIN(m.remote_path)
            FROM remote_manifest m JOIN remote_sessions s USING(session_id)
            WHERE m.session_id IN (4,5,6) AND m.local_rel_path IS NOT NULL
            GROUP BY s.tape_label,m.local_rel_path
        """)
        before = conn.execute(
            'SELECT COUNT(*) FROM files_index WHERE staging_rel(original_path) IS NOT NULL'
        ).fetchone()[0]
        conn.execute("""
            UPDATE files_index AS f
            SET original_path=COALESCE(
              (SELECT remote_path FROM migration_path_map p
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
        conn.execute('DROP TABLE migration_path_map')
        self._checkpoint(conn, phase)

    def _create_remote_v2(self, conn):
        phase = 'v2_remote_storage'
        if self._done(conn, phase):
            return
        session = conn.execute(
            'SELECT * FROM remote_sessions WHERE session_id=6').fetchone()
        if not session:
            raise RuntimeError('remote session 6 is missing')

        snapshot_hash = hashlib.sha256()
        for identity in (session['remote_host'], session['remote_path']):
            raw = str(identity).encode('utf-8', errors='surrogatepass')
            snapshot_hash.update(len(raw).to_bytes(8, 'big'))
            snapshot_hash.update(raw)
        for row in conn.execute("""SELECT remote_path,file_size_bytes
                                    FROM remote_manifest WHERE session_id=6
                                    ORDER BY remote_path"""):
            raw = row['remote_path'].encode('utf-8', errors='surrogatepass')
            snapshot_hash.update(len(raw).to_bytes(8, 'big'))
            snapshot_hash.update(raw)
            snapshot_hash.update(int(row['file_size_bytes']).to_bytes(8, 'big'))
        snapshot_fp = snapshot_hash.digest()
        plan_hash = hashlib.sha256(snapshot_fp)
        for row in conn.execute("""SELECT chunk_index,remote_path,file_size_bytes
                                    FROM remote_manifest WHERE session_id=6
                                    ORDER BY manifest_id"""):
            raw = row['remote_path'].encode('utf-8', errors='surrogatepass')
            plan_hash.update(int(row['chunk_index']).to_bytes(4, 'big'))
            plan_hash.update(len(raw).to_bytes(8, 'big'))
            plan_hash.update(raw)
            plan_hash.update(int(row['file_size_bytes']).to_bytes(8, 'big'))
        plan_fp = plan_hash.digest()

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
        cur = conn.execute("""INSERT INTO remote_snapshots
            (remote_host,remote_path,fingerprint,total_files,total_bytes,created_at)
            VALUES (?,?,?,?,?,?)""",
            (session['remote_host'],session['remote_path'],snapshot_fp,
             session['total_files'],session['total_bytes'],now))
        snapshot_id = cur.lastrowid
        conn.execute("""INSERT INTO remote_snapshot_files(snapshot_id,remote_path,file_size_bytes)
            SELECT ?,remote_path,file_size_bytes FROM remote_manifest WHERE session_id=6""",
            (snapshot_id,))
        cur = conn.execute("""INSERT INTO remote_plans
            (snapshot_id,fingerprint,chunk_count,created_at) VALUES (?,?,?,?)""",
            (snapshot_id,plan_fp,session['chunk_count'],now))
        plan_id = cur.lastrowid
        conn.execute("""INSERT INTO remote_plan_files
            (plan_id,snapshot_file_id,chunk_index,ordinal)
            SELECT ?,sf.snapshot_file_id,m.chunk_index,m.manifest_id
            FROM remote_manifest m JOIN remote_snapshot_files sf
              ON sf.snapshot_id=? AND sf.remote_path=m.remote_path
            WHERE m.session_id=6""", (plan_id,snapshot_id))
        conn.execute("""INSERT INTO remote_sessions
            (session_id,session_label,remote_host,remote_user,remote_path,tape_label,
             staging_dir,total_files,total_bytes,chunk_count,plan_id,created_at,completed_at,status)
            SELECT session_id,session_label,remote_host,remote_user,remote_path,tape_label,
                   staging_dir,total_files,total_bytes,chunk_count,?,created_at,completed_at,status
            FROM remote_sessions_legacy WHERE session_id=6""", (plan_id,))
        conn.execute("""INSERT INTO remote_chunks(session_id,chunk_index,status,updated_at)
            SELECT 6,chunk_index,
                   CASE WHEN MIN(chunk_status)='done' AND MAX(chunk_status)='done'
                        THEN 'done' ELSE MAX(chunk_status) END,
                   MAX(updated_at)
            FROM remote_manifest WHERE session_id=6 GROUP BY chunk_index""")
        conn.execute("""INSERT INTO remote_file_state
            (session_id,plan_file_id,status,local_rel_path,error_msg,updated_at)
            SELECT 6,pf.plan_file_id,'source_missing',NULL,m.error_msg,m.updated_at
            FROM remote_manifest m
            JOIN remote_snapshot_files sf ON sf.snapshot_id=? AND sf.remote_path=m.remote_path
            JOIN remote_plan_files pf ON pf.plan_id=? AND pf.snapshot_file_id=sf.snapshot_file_id
            WHERE m.session_id=6 AND m.status='source_missing'""",
            (snapshot_id,plan_id))
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
        conn.execute("""UPDATE files_index SET
            file_hash_blob=compact_hash(file_hash),
            file_hash=CASE WHEN LENGTH(file_hash)=64 THEN NULL ELSE NULLIF(file_hash,'') END,
            record_key=catalog_key(original_path,tape_label,local_session_id,local_chunk_index),
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
        if os.path.exists(self.lock_path):
            raise RuntimeError(f"maintenance lock already exists: {self.lock_path}")
        before_stat = os.stat(self.db_path)
        required = os.path.getsize(self.db_path) * 3 + 1024**3
        if shutil.disk_usage(os.path.dirname(self.db_path)).free < required:
            raise RuntimeError('insufficient free disk for safe copy-and-swap migration')
        with open(self.lock_path, 'x', encoding='utf-8') as lock:
            lock.write(str(os.getpid()))
        try:
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
            os.makedirs(report_dir, exist_ok=True)
            report_path = os.path.join(
                report_dir, self.started.strftime('DB_OPTIMIZATION_%Y%m%d_%H%M%S.json'))
            with open(report_path, 'w', encoding='utf-8') as report_file:
                json.dump(self.stats, report_file, indent=2)
            self.stats['report_path'] = report_path
            os.remove(self.rollback_path)
            os.remove(self.work_path)
            self.progress(json.dumps(self.stats, indent=2))
            return self.stats
        finally:
            if os.path.exists(self.lock_path):
                os.remove(self.lock_path)

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
