"""PostgreSQL DatabaseManager implementation for the archive catalog."""
import hashlib
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg
    from psycopg import errors
    from psycopg.rows import dict_row
    from psycopg_pool import PoolTimeout
except ImportError:  # pragma: no cover - handled by require_psycopg at runtime
    psycopg = None
    errors = None
    dict_row = None
    PoolTimeout = None

from .catalog_query import contains_pattern, prefix_pattern, substring_pattern
from .catalog_v3 import catalog_directory_chain, catalog_file_name
from .constants import (DB_UPSERT_BATCH_SIZE, LEGACY_DEFAULT_SOURCE_HOST,
                        PROJECT_ROOT)
from .db import _derived_file_name, _file_record_key, _short_source_host
from .pg_bulk import copy_rows, make_pool, require_psycopg
from .runtime import CANCEL


class PgRow(dict):
    """Small row wrapper matching the row access the app expects."""

    def __init__(self, data):
        super().__init__(data)
        self._keys = tuple(data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return dict.__getitem__(self, self._keys[key])
        return dict.__getitem__(self, key)

    def keys(self):
        return self._keys


def _rows(items):
    return [PgRow(dict(item)) for item in items]


def _row(item):
    return PgRow(dict(item)) if item is not None else None


def _valid_columns(kwargs):
    for key in kwargs:
        if not key.replace("_", "").isalnum():
            raise RuntimeError(f"[DB] Unsafe column name: {key}")


def _canonical_remote_path(value):
    """Normalize a remote SOURCE path to the POSIX form used as a catalog key.

    Remote paths are stored with forward slashes so the snapshot-file rows and
    the plan-file lookups agree even when a Linux filename legally contains a
    backslash.
    """
    return str(value).replace("\\", "/")


def _snapshot_fingerprint(remote_host, remote_path, by_path):
    digest = hashlib.sha256()
    for identity in (remote_host, remote_path):
        raw = str(identity).encode("utf-8", errors="surrogatepass")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    for path, size in sorted(by_path.items()):
        raw = path.encode("utf-8", errors="surrogatepass")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
        digest.update(int(size).to_bytes(8, "big", signed=False))
    return digest.digest()


def _plan_fingerprint(snapshot_fingerprint, rows):
    digest = hashlib.sha256(snapshot_fingerprint)
    for chunk_index, remote_path, _file_name, size in rows:
        raw = str(remote_path).encode("utf-8", errors="surrogatepass")
        digest.update(int(chunk_index).to_bytes(4, "big"))
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
        digest.update(int(size).to_bytes(8, "big"))
    return digest.digest()


def _now_utc():
    return datetime.now(timezone.utc)


def _as_utc(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return value


def _coerce_timestamptz(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return value
    return _as_utc(value)


def _coerce_timestamp_kwargs(kwargs):
    timestamp_columns = {"created_at", "started_at", "completed_at", "updated_at"}
    return {
        key: _coerce_timestamptz(value) if key in timestamp_columns else value
        for key, value in kwargs.items()
    }


class PgDatabaseManager:
    """PostgreSQL-backed subset of the DatabaseManager API.

    This covers the live local archive/restore/catalog workflows.
    """

    # Session-level advisory lock key ('LTO8'): cross-process single-writer
    # guard for tape-write runs. The in-process tape I/O lock cannot stop a
    # second `python run.py` instance; this can.
    ARCHIVER_LOCK_KEY = 0x4C544F38

    def __init__(self, conninfo, *, init_schema=True, pool=None):
        require_psycopg()
        self.db_path = conninfo
        self._pool = None
        self._lock_conn = None
        self._pool = pool or make_pool(conninfo, min_size=1, max_size=8,
                                       row_factory=dict_row)
        try:
            if init_schema:
                self._init_schema()
                self._ensure_runtime_constraints()
        except Exception:
            self.close()
            raise

    def _init_schema(self):
        sql_dir = Path(PROJECT_ROOT) / "scripts" / "sql"
        migrations = (
            "001_postgres_schema.sql",
            "002_postgres_indexes.sql",
            "003_postgres_constraints.sql",
            "004_postgres_archive_runs_sessions.sql",
            "005_postgres_session_label_unique.sql",
        )
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                for migration in migrations:
                    cur.execute((sql_dir / migration).read_text(encoding="utf-8"))
            conn.commit()

    def _ensure_runtime_constraints(self):
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'uq_files_record_key'
                              AND conrelid = 'files_index'::regclass
                        ) THEN
                            ALTER TABLE files_index
                                ADD CONSTRAINT uq_files_record_key
                                UNIQUE (record_key);
                        END IF;
                    END $$;
                """)
            conn.commit()

    def _transaction(self, operation, description, attempts=5):
        # Serialization conflicts and deadlocks retry quickly; OperationalError
        # and PoolTimeout (connection drops, Docker DB restarts) back off longer
        # so a restarting server can come back. Every write routed through here
        # is an idempotent keyed upsert/update, so re-running a batch whose
        # commit outcome is unknown converges to the same state.
        retryable = (errors.SerializationFailure, errors.DeadlockDetected,
                     psycopg.OperationalError, PoolTimeout)
        for attempt in range(1, attempts + 1):
            try:
                with self._pool.connection() as conn:
                    with conn.transaction():
                        return operation(conn)
            except retryable as e:
                if attempt == attempts:
                    raise
                if isinstance(e, (errors.SerializationFailure,
                                  errors.DeadlockDetected)):
                    wait_s = min(5, attempt)
                else:
                    wait_s = min(30, 2 ** attempt)
                print(f"[DB] {type(e).__name__} during {description}; "
                      f"retrying in {wait_s}s ({attempt}/{attempts})...")
                time.sleep(wait_s)

    @staticmethod
    def _require_updated(cur, message):
        if cur.rowcount == 0:
            raise RuntimeError(message)

    @staticmethod
    def _hydrate_file_row(row):
        if row is None:
            return None
        item = dict(row)
        item["file_name"] = item.get("catalog_name") or _derived_file_name(
            item.get("stored_path"), item.get("original_path"))
        item["backup_date"] = item.get("catalog_backup_date") or item.get(
            "run_started_at")
        item["source_host"] = _short_source_host(
            item.get("source_host") or LEGACY_DEFAULT_SOURCE_HOST)
        item["container_name"] = item.get("bundle_tape_path")
        return item

    @staticmethod
    def _catalog_select():
        return """SELECT f.*, b.tape_path AS bundle_tape_path,
                         r.started_at AS run_started_at
                  FROM files_index AS f
                  LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                  LEFT JOIN archive_runs AS r ON r.run_id = f.archive_run_id"""

    def _catalog_rows(self, where="", params=(), order_by=""):
        sql = self._catalog_select()
        if where:
            sql += " WHERE " + where
        if order_by:
            sql += " ORDER BY " + order_by
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    @staticmethod
    def _collect_directory_specs(records):
        """Pre-compute the directory chain for a batch of file records.

        Returns ``(specs, targets)`` where ``specs`` maps
        ``(tape_label, normalized_path)`` -> ``(parent_path, name)`` for every
        distinct directory the batch touches (deduplicated across files), and
        ``targets`` is a list, parallel to ``records``, giving each file's
        immediate parent directory key. Files with no directory component fall
        back to a per-tape synthetic ``ROOT`` bucket, matching the previous
        per-record behaviour.
        """
        specs = {}
        targets = []
        for record in records:
            tape_label = record.get("tape_label") or ""
            source_host = _short_source_host(
                record.get("source_host") or LEGACY_DEFAULT_SOURCE_HOST)
            canonical = record.get("canonical_source_path")
            original_path = (str(canonical) if canonical
                             else record.get("original_path") or "")
            dir_path = original_path or record.get("stored_path")
            chain = catalog_directory_chain(dir_path, source_host)
            if chain:
                for normalized_path, parent_path, name in chain:
                    specs[(tape_label, normalized_path)] = (parent_path, name)
                targets.append((tape_label, chain[-1][0]))
            else:
                specs[(tape_label, "ROOT")] = (None, "ROOT")
                targets.append((tape_label, "ROOT"))
        return specs, targets

    def _ensure_directories(self, conn, specs):
        """Batch-upsert ``catalog_directories`` and return their ids.

        ``specs`` maps ``(tape_label, normalized_path)`` -> ``(parent_path,
        name)``. Directories are grouped by tree depth and inserted parents-first
        with a single multi-row upsert per level, so the whole chain resolves in
        O(depth) round-trips instead of one per directory per file (the former
        N+1 that dominated Phase-3 sync time). Returns
        ``{(tape_label, normalized_path): directory_id}``.
        """
        resolved = {}
        if not specs:
            return resolved
        by_depth = defaultdict(list)
        for (tape_label, normalized_path), (parent_path, name) in specs.items():
            by_depth[normalized_path.count("/")].append(
                (tape_label, normalized_path, parent_path, name))
        # libpq caps a single statement at 65,535 bind parameters (4 per row).
        # Slicing each level keeps the upsert safe even if DB_UPSERT_BATCH_SIZE
        # is ever raised past that ceiling.
        max_rows = 5000
        for depth in sorted(by_depth):
            level = by_depth[depth]
            for start in range(0, len(level), max_rows):
                values = []
                params = []
                for tape_label, normalized_path, parent_path, name in (
                        level[start:start + max_rows]):
                    parent_id = (resolved.get((tape_label, parent_path))
                                 if parent_path is not None else None)
                    values.append("(%s, %s, %s, %s)")
                    params.extend([tape_label, parent_id, name, normalized_path])
                rows = conn.execute(
                    "INSERT INTO catalog_directories "
                    "(tape_label, parent_id, name, normalized_path) "
                    f"VALUES {', '.join(values)} "
                    "ON CONFLICT (tape_label, normalized_path) DO UPDATE "
                    "SET name = EXCLUDED.name "
                    "RETURNING directory_id, tape_label, normalized_path",
                    params,
                ).fetchall()
                for row in rows:
                    resolved[(row["tape_label"], row["normalized_path"])] = (
                        row["directory_id"])
        return resolved

    def _normalize_file_records(self, conn, records):
        bundle_paths = {
            (record["tape_label"], record.get("container_name"))
            for record in records
            if record.get("is_packed") and record.get("container_name")
        }
        for tape_label, tape_path in bundle_paths:
            conn.execute(
                """INSERT INTO archive_bundles(tape_label, tape_path)
                   VALUES (%s, %s)
                   ON CONFLICT (tape_label, tape_path) DO NOTHING""",
                (tape_label, tape_path),
            )
        bundle_ids = {}
        if bundle_paths:
            placeholders = ", ".join(["(%s, %s)"] * len(bundle_paths))
            params = []
            for tape_label, tape_path in bundle_paths:
                params.extend([tape_label, tape_path])
            rows = conn.execute(
                f"""WITH wanted(tape_label, tape_path) AS (VALUES {placeholders})
                    SELECT b.tape_label, b.tape_path, b.bundle_id
                    FROM archive_bundles AS b
                    JOIN wanted AS w
                      ON w.tape_label = b.tape_label
                     AND w.tape_path = b.tape_path""",
                params,
            ).fetchall()
            bundle_ids = {
                (row["tape_label"], row["tape_path"]): row["bundle_id"]
                for row in rows
            }

        now = _now_utc()
        run_specs = {}
        for record in records:
            if record.get("archive_run_id") is not None:
                continue
            backup_date = _as_utc(record.get("backup_date") or now)
            tape_label = record.get("tape_label") or ""
            run_label = f"{str(backup_date)[:10]}:{tape_label}"
            local_session_id = record.get("local_session_id")
            kind = "local" if local_session_id is not None else "remote"
            # Only the matching typed column is populated; the FK guarantees the
            # reference is valid and the CHECK keeps it consistent with `kind`.
            # (Remote runs currently carry no session id at catalog time.)
            run_specs[(run_label, tape_label)] = (
                run_label, tape_label, kind, local_session_id, None,
                backup_date, backup_date)
        for spec in run_specs.values():
            conn.execute(
                """INSERT INTO archive_runs
                   (run_label, tape_label, session_kind,
                    local_session_id, remote_session_id,
                    started_at, completed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (run_label, tape_label) DO NOTHING""",
                spec,
            )
        run_ids = {
            key: conn.execute(
                """SELECT run_id FROM archive_runs
                   WHERE run_label=%s AND tape_label=%s""",
                key,
            ).fetchone()["run_id"]
            for key in run_specs
        }

        dir_specs, dir_targets = self._collect_directory_specs(records)
        resolved_dirs = self._ensure_directories(conn, dir_specs)

        normalized = {}
        for idx, record in enumerate(records):
            canonical = record.get("canonical_source_path")
            if canonical is not None:
                canonical = str(canonical)
                if not canonical.startswith("/") or "\\" in canonical:
                    raise RuntimeError(
                        "[DB] Remote catalog records require an absolute POSIX "
                        f"canonical SOURCE path, got: {canonical}"
                    )
            original_path = canonical or record.get("original_path") or ""
            tape_label = record.get("tape_label") or ""
            source_host = _short_source_host(
                record.get("source_host") or LEGACY_DEFAULT_SOURCE_HOST)
            session_id = record.get("local_session_id")
            chunk_index = record.get("local_chunk_index")
            key = _file_record_key(
                original_path, tape_label, session_id, chunk_index, source_host)
            container = record.get("container_name")
            bundle_id = bundle_ids.get((tape_label, container))
            if record.get("is_packed") and bundle_id is None:
                raise RuntimeError(
                    f"[DB] Packed file has no archive bundle: {container}")
            backup_date = _as_utc(record.get("backup_date") or now)
            run_label = f"{str(backup_date)[:10]}:{tape_label}"
            archive_run_id = record.get("archive_run_id") or run_ids[
                (run_label, tape_label)]
            directory_id = resolved_dirs[dir_targets[idx]]
            normalized[key] = {
                "original_path": original_path,
                "file_size_bytes": int(record.get("file_size_bytes") or 0),
                "tape_label": tape_label,
                "source_host": source_host,
                "is_packed": bool(record.get("is_packed")),
                "stored_path": record.get("stored_path") or "",
                "local_session_id": session_id,
                "local_chunk_index": chunk_index,
                "bundle_id": bundle_id,
                "record_key": key,
                "archive_run_id": archive_run_id,
                "directory_id": directory_id,
                "catalog_name": catalog_file_name(
                    record.get("stored_path"), original_path),
                "catalog_backup_date": backup_date,
            }
        return normalized

    def _bulk_upsert_batch(self, conn, records, update_existing):
        normalized_by_key = self._normalize_file_records(conn, records)
        total = len(normalized_by_key)
        columns = (
            "original_path", "file_size_bytes", "tape_label", "source_host",
            "is_packed", "stored_path", "local_session_id", "local_chunk_index",
            "bundle_id", "record_key", "archive_run_id", "directory_id",
            "catalog_name", "catalog_backup_date",
        )
        col_sql = ", ".join(columns)
        update_sql = ", ".join(
            f"{column}=EXCLUDED.{column}"
            for column in columns if column != "record_key"
        )
        conn.execute(
            "CREATE TEMP TABLE _stage ON COMMIT DROP AS "
            f"SELECT {col_sql} FROM files_index WITH NO DATA"
        )
        with conn.cursor() as cur:
            copy_rows(cur, "_stage", columns, (
                [row[column] for column in columns]
                for row in normalized_by_key.values()
            ))
        conflict = (
            f"DO UPDATE SET {update_sql}" if update_existing else "DO NOTHING")
        # RETURNING (xmax = 0) distinguishes freshly inserted rows (xmax 0) from
        # updated ones without a second membership scan. With DO NOTHING, only
        # inserted rows are returned, so anything not returned was a skip.
        affected = conn.execute(
            f"""INSERT INTO files_index ({col_sql})
                SELECT {col_sql} FROM _stage
                ON CONFLICT (record_key) {conflict}
                RETURNING (xmax = 0) AS inserted"""
        ).fetchall()
        inserted = sum(1 for row in affected if row["inserted"])
        if update_existing:
            return {
                "inserted": inserted,
                "updated": len(affected) - inserted,
                "skipped": 0,
            }
        return {
            "inserted": inserted,
            "updated": 0,
            "skipped": total - inserted,
        }

    def bulk_upsert_files(self, records, batch_size=DB_UPSERT_BATCH_SIZE,
                          update_existing=True):
        totals = {"inserted": 0, "updated": 0, "skipped": 0}
        batch = []
        registered = set()

        def flush(items):
            if not items:
                return

            def operation(conn):
                labels = {item.get("tape_label") for item in items}
                found = set()
                missing = []
                for label in labels - registered:
                    row = conn.execute(
                        "SELECT 1 FROM tapes WHERE volume_label = %s",
                        (label,),
                    ).fetchone()
                    if row:
                        found.add(label)
                    else:
                        missing.append(label)
                if missing:
                    raise RuntimeError(
                        f"[DB] Cannot index files for unregistered tape(s): {missing}")
                return self._bulk_upsert_batch(conn, items, update_existing), found

            stats, found = self._transaction(
                operation, f"file catalog batch ({len(items):,} rows)")
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

    def create_local_session(self, session_label, source_dir, chunks,
                             backup_mode="auto"):
        now = _now_utc()

        def operation(conn):
            # Upsert on the timestamped label so a connection-loss retry whose
            # first COMMIT actually landed converges on the committed session
            # instead of creating a duplicate (with a duplicate manifest).
            row = conn.execute(
                """INSERT INTO local_sessions
                   (session_label, source_dir, total_chunks, backup_mode,
                    created_at, status)
                   VALUES (%s, %s, %s, %s, %s, 'active')
                   ON CONFLICT (session_label) DO UPDATE
                       SET session_label = EXCLUDED.session_label
                   RETURNING session_id, (xmax = 0) AS inserted""",
                (session_label, source_dir, len(chunks), backup_mode, now),
            ).fetchone()
            session_id = row["session_id"]
            if not row["inserted"]:
                # Session + manifest were committed atomically by the earlier
                # attempt; re-inserting the manifest would duplicate it.
                return session_id
            rows = []
            for chunk_index, entries in enumerate(chunks):
                for entry in entries:
                    rows.append((
                        session_id, chunk_index, entry["name"],
                        entry["size_bytes"], "pending", now))
            if rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO local_chunks_manifest
                           (session_id, chunk_index, top_level_dir,
                            dir_size_bytes, status, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        rows,
                    )
            return session_id

        return self._transaction(operation, "create local session")

    def update_local_session(self, session_id, **kwargs):
        if not kwargs:
            return
        _valid_columns(kwargs)
        kwargs = _coerce_timestamp_kwargs(kwargs)
        sets = ", ".join(f"{key}=%s" for key in kwargs)
        vals = list(kwargs.values()) + [session_id]

        def operation(conn):
            cur = conn.execute(
                f"UPDATE local_sessions SET {sets} WHERE session_id=%s", vals)
            self._require_updated(
                cur, f"[DB] Local session not found: {session_id}")

        return self._transaction(operation, f"update local session {session_id}")

    def get_active_local_session(self, source_dir):
        with self._pool.connection() as conn:
            return _row(conn.execute(
                """SELECT * FROM local_sessions
                   WHERE source_dir=%s AND status='active'
                   ORDER BY session_id DESC LIMIT 1""",
                (source_dir,),
            ).fetchone())

    def get_local_session(self, session_id):
        with self._pool.connection() as conn:
            return _row(conn.execute(
                "SELECT * FROM local_sessions WHERE session_id=%s",
                (session_id,),
            ).fetchone())

    def get_local_pending_chunks(self, session_id):
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT chunk_index FROM local_chunks_manifest
                   WHERE session_id=%s
                   GROUP BY chunk_index
                   HAVING SUM(CASE WHEN status != 'backed_up' THEN 1 ELSE 0 END) > 0
                   ORDER BY chunk_index""",
                (session_id,),
            ).fetchall()
        return [row["chunk_index"] for row in rows]

    def get_local_chunk_entries(self, session_id, chunk_index):
        with self._pool.connection() as conn:
            return _rows(conn.execute(
                """SELECT * FROM local_chunks_manifest
                   WHERE session_id=%s AND chunk_index=%s
                   ORDER BY manifest_id""",
                (session_id, chunk_index),
            ).fetchall())

    def assign_local_chunk_tape(self, session_id, chunk_index, tape_label):
        now = _now_utc()

        def operation(conn):
            cur = conn.execute(
                """UPDATE local_chunks_manifest
                   SET tape_label = COALESCE(tape_label, %s),
                       started_at = COALESCE(started_at, %s),
                       updated_at = %s
                   WHERE session_id=%s AND chunk_index=%s""",
                (tape_label, now, now, session_id, chunk_index),
            )
            self._require_updated(
                cur,
                f"[DB] Local chunk not found: session {session_id}, chunk {chunk_index}",
            )

        self._transaction(operation, "assign local chunk tape")

    def update_local_chunk_status(self, session_id, chunk_index, status):
        kwargs = {"status": status, "updated_at": _now_utc()}
        if status == "backed_up":
            kwargs["completed_at"] = _now_utc()
        self._update_local_manifest(
            kwargs, "session_id=%s AND chunk_index=%s",
            [session_id, chunk_index],
            f"[DB] Local chunk not found: session {session_id}, chunk {chunk_index}",
        )

    def update_local_manifest_row(self, manifest_id, **kwargs):
        if not kwargs:
            return
        kwargs["updated_at"] = _now_utc()
        kwargs = _coerce_timestamp_kwargs(kwargs)
        self._update_local_manifest(
            kwargs, "manifest_id=%s", [manifest_id],
            f"[DB] Local manifest row not found: {manifest_id}",
        )

    def _update_local_manifest(self, kwargs, where, params, missing):
        _valid_columns(kwargs)
        sets = ", ".join(f"{key}=%s" for key in kwargs)
        values = list(kwargs.values()) + params

        def operation(conn):
            cur = conn.execute(
                f"UPDATE local_chunks_manifest SET {sets} WHERE {where}",
                values,
            )
            self._require_updated(cur, missing)

        self._transaction(operation, "update local manifest")

    @staticmethod
    def _upsert_remote_session(conn, session_label, remote_host, remote_user,
                               remote_path, tape_label, staging_dir, now):
        """Insert a remote session, converging on the timestamped label.

        The ON CONFLICT arm makes an ambiguous-commit retry return the already
        committed session instead of creating a duplicate 'active' row.
        """
        return conn.execute(
            """INSERT INTO remote_sessions
               (session_label, remote_host, remote_user, remote_path,
                tape_label, staging_dir, created_at, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
               ON CONFLICT (session_label) DO UPDATE
                   SET session_label = EXCLUDED.session_label
               RETURNING session_id""",
            (session_label, remote_host, remote_user, remote_path,
             tape_label, staging_dir, now),
        ).fetchone()["session_id"]

    def create_remote_session(self, session_label, remote_host, remote_user,
                              remote_path, tape_label, staging_dir):
        now = _now_utc()

        def operation(conn):
            return self._upsert_remote_session(
                conn, session_label, remote_host, remote_user, remote_path,
                tape_label, staging_dir, now)

        return self._transaction(operation, "create remote session")

    def create_remote_session_with_plan(self, session_label, remote_host,
                                        remote_user, remote_path, tape_label,
                                        staging_dir, rows):
        """Create a remote session and persist its plan in ONE transaction.

        A session must never become visible without its chunk plan: the old
        three-transaction flow (create -> set totals -> insert manifest) could
        crash in between, leaving an 'active' session with zero chunks that a
        later resume silently marked 'completed'.
        """
        rows = list(rows)
        by_path = self._validate_remote_manifest_rows(rows)
        chunk_count = len({int(row[0]) for row in rows})
        total_bytes = sum(int(row[3]) for row in rows)
        now = _now_utc()

        def operation(conn):
            session_id = self._upsert_remote_session(
                conn, session_label, remote_host, remote_user, remote_path,
                tape_label, staging_dir, now)
            conn.execute(
                """UPDATE remote_sessions
                   SET total_files=%s, total_bytes=%s, chunk_count=%s
                   WHERE session_id=%s""",
                (len(rows), total_bytes, chunk_count, session_id),
            )
            self._persist_remote_plan(
                conn, session_id, remote_host, remote_path, rows, by_path, now)
            return session_id

        return self._transaction(operation, "create remote session with plan")

    def update_remote_session(self, session_id, **kwargs):
        if not kwargs:
            return
        _valid_columns(kwargs)
        kwargs = _coerce_timestamp_kwargs(kwargs)
        sets = ", ".join(f"{key}=%s" for key in kwargs)
        values = list(kwargs.values()) + [session_id]

        def operation(conn):
            cur = conn.execute(
                f"UPDATE remote_sessions SET {sets} WHERE session_id=%s",
                values,
            )
            self._require_updated(
                cur, f"[DB] Remote session not found: {session_id}")

        self._transaction(operation, f"update remote session {session_id}")

    def get_active_remote_session(self, remote_host, remote_path):
        with self._pool.connection() as conn:
            return _row(conn.execute(
                """SELECT * FROM remote_sessions
                   WHERE remote_host=%s AND remote_path=%s AND status='active'
                   ORDER BY session_id DESC LIMIT 1""",
                (remote_host, remote_path),
            ).fetchone())

    def get_remote_session(self, session_id):
        with self._pool.connection() as conn:
            return _row(conn.execute(
                "SELECT * FROM remote_sessions WHERE session_id=%s",
                (session_id,),
            ).fetchone())

    @staticmethod
    def _validate_remote_manifest_rows(rows):
        """Validate (chunk, path, name, size) rows; return {canonical: size}."""
        by_path = {}
        for chunk_index, remote_path, _file_name, size in rows:
            canonical = _canonical_remote_path(remote_path)
            if not canonical.startswith("/"):
                raise RuntimeError(
                    f"[DB] Non-canonical remote SOURCE path: {remote_path}")
            previous = by_path.setdefault(canonical, int(size))
            if previous != int(size):
                raise RuntimeError(
                    f"[DB] Conflicting sizes for remote SOURCE path: {canonical}")
        if len(by_path) != len(rows):
            raise RuntimeError("[DB] Duplicate canonical paths in remote snapshot")
        return by_path

    def insert_remote_manifest_batch(self, session_id, rows):
        """Persist a canonical snapshot and reusable chunk plan."""
        rows = list(rows)
        session = self.get_remote_session(session_id)
        if not session:
            raise RuntimeError(f"[DB] Remote session not found: {session_id}")
        by_path = self._validate_remote_manifest_rows(rows)
        now = _now_utc()

        def operation(conn):
            return self._persist_remote_plan(
                conn, session_id, session["remote_host"],
                session["remote_path"], rows, by_path, now)

        return self._transaction(operation, "insert remote manifest batch")

    def _persist_remote_plan(self, conn, session_id, remote_host, remote_path,
                             rows, by_path, now):
        """Persist snapshot/plan/chunk rows for a session (idempotent by
        fingerprint, so it is safe inside the ambiguous-commit retry loop)."""
        snapshot_fp = _snapshot_fingerprint(remote_host, remote_path, by_path)
        plan_fp = _plan_fingerprint(snapshot_fp, rows)
        chunk_indexes = sorted({int(row[0]) for row in rows})

        conn.execute(
            """INSERT INTO remote_snapshots
               (remote_host, remote_path, fingerprint, total_files,
                total_bytes, created_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (fingerprint) DO NOTHING""",
            (remote_host, remote_path, snapshot_fp,
             len(rows), sum(int(row[3]) for row in rows), now),
        )
        snapshot_id = conn.execute(
            "SELECT snapshot_id FROM remote_snapshots WHERE fingerprint=%s",
            (snapshot_fp,),
        ).fetchone()["snapshot_id"]
        existing = conn.execute(
            """SELECT COUNT(*) AS n FROM remote_snapshot_files
               WHERE snapshot_id=%s""",
            (snapshot_id,),
        ).fetchone()["n"]
        if not existing:
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO remote_snapshot_files
                       (snapshot_id, remote_path, file_size_bytes)
                       VALUES (%s, %s, %s)""",
                    ((snapshot_id, path, size)
                     for path, size in by_path.items()),
                )
        conn.execute(
            """INSERT INTO remote_plans
               (snapshot_id, fingerprint, chunk_count, created_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (fingerprint) DO NOTHING""",
            (snapshot_id, plan_fp, len(chunk_indexes), now),
        )
        plan_id = conn.execute(
            "SELECT plan_id FROM remote_plans WHERE fingerprint=%s",
            (plan_fp,),
        ).fetchone()["plan_id"]
        existing = conn.execute(
            "SELECT COUNT(*) AS n FROM remote_plan_files WHERE plan_id=%s",
            (plan_id,),
        ).fetchone()["n"]
        if not existing:
            ids = {
                row["remote_path"]: row["snapshot_file_id"]
                for row in conn.execute(
                    """SELECT remote_path, snapshot_file_id
                       FROM remote_snapshot_files
                       WHERE snapshot_id=%s""",
                    (snapshot_id,),
                ).fetchall()
            }
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO remote_plan_files
                       (plan_id, snapshot_file_id, chunk_index, ordinal)
                       VALUES (%s, %s, %s, %s)""",
                    ((plan_id, ids[_canonical_remote_path(row[1])],
                      int(row[0]), ordinal)
                     for ordinal, row in enumerate(rows)),
                )
        conn.execute(
            "UPDATE remote_sessions SET plan_id=%s WHERE session_id=%s",
            (plan_id, session_id),
        )
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO remote_chunks
                   (session_id, chunk_index, status, updated_at)
                   VALUES (%s, %s, 'pending', %s)
                   ON CONFLICT (session_id, chunk_index) DO NOTHING""",
                ((session_id, chunk_index, now)
                 for chunk_index in chunk_indexes),
            )
        return plan_id

    def get_chunk_files(self, session_id, chunk_index):
        with self._pool.connection() as conn:
            return _rows(conn.execute(
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
                   WHERE s.session_id=%s AND pf.chunk_index=%s
                   ORDER BY pf.ordinal""",
                (session_id, chunk_index),
            ).fetchall())

    def get_chunk_size_summary(self, session_id, chunk_index=None):
        """Per-chunk byte totals without materializing millions of file rows.

        Returns ``{chunk_index: (planned_bytes, present_bytes, file_count)}``
        where ``planned_bytes`` counts every planned file, ``present_bytes``
        excludes files already known to be ``source_missing``, and
        ``file_count`` counts every planned file (the staging-capacity gate
        uses it to estimate per-file cluster rounding on disk).
        """
        where = "s.session_id=%s"
        params = [session_id]
        if chunk_index is not None:
            where += " AND pf.chunk_index=%s"
            params.append(chunk_index)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""SELECT pf.chunk_index,
                           COALESCE(SUM(sf.file_size_bytes), 0) AS planned_bytes,
                           COALESCE(SUM(sf.file_size_bytes) FILTER (
                               WHERE COALESCE(st.status, '') != 'source_missing'
                           ), 0) AS present_bytes,
                           COUNT(*) AS file_count
                    FROM remote_sessions s
                    JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
                    JOIN remote_snapshot_files sf
                      ON sf.snapshot_file_id=pf.snapshot_file_id
                    LEFT JOIN remote_file_state st ON st.session_id=s.session_id
                      AND st.plan_file_id=pf.plan_file_id
                    WHERE {where}
                    GROUP BY pf.chunk_index""",
                params,
            ).fetchall()
        return {
            row["chunk_index"]: (int(row["planned_bytes"]),
                                 int(row["present_bytes"]),
                                 int(row["file_count"]))
            for row in rows
        }

    def update_manifest_row(self, manifest_id, session_id=None, **kwargs):
        if session_id is None:
            raise RuntimeError("[DB] session_id required for normalized remote state")
        return self._upsert_remote_file_state(session_id, manifest_id, kwargs)

    def _upsert_remote_file_state(self, session_id, plan_file_id, values):
        allowed = {"status", "local_rel_path", "error_msg"}
        unknown = set(values) - allowed
        if unknown:
            raise RuntimeError(f"[DB] Invalid remote state field(s): {sorted(unknown)}")

        def operation(conn):
            current = conn.execute(
                """SELECT * FROM remote_file_state
                   WHERE session_id=%s AND plan_file_id=%s""",
                (session_id, plan_file_id),
            ).fetchone()
            merged = {
                key: (current[key] if current else None)
                for key in ("status", "local_rel_path", "error_msg")
            }
            merged.update(values)
            conn.execute(
                """INSERT INTO remote_file_state
                   (session_id, plan_file_id, status, local_rel_path,
                    error_msg, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (session_id, plan_file_id) DO UPDATE SET
                     status=EXCLUDED.status,
                     local_rel_path=EXCLUDED.local_rel_path,
                     error_msg=EXCLUDED.error_msg,
                     updated_at=EXCLUDED.updated_at""",
                (session_id, plan_file_id, merged["status"],
                 merged["local_rel_path"], merged["error_msg"],
                  _now_utc()),
            )

        self._transaction(
            operation, f"normalized remote file {plan_file_id} update")

    def _remote_state_batch(self, rows, sql, description):
        rows = list(rows)
        if not rows:
            return

        def operation(conn):
            with conn.cursor() as cur:
                cur.executemany(sql, rows)

        self._transaction(operation, description)

    def update_manifest_rows_fetching(self, manifest_ids, session_id=None):
        if session_id is None:
            raise RuntimeError("[DB] session_id required for normalized remote state")
        now = _now_utc()
        self._remote_state_batch(
            ((session_id, manifest_id, now) for manifest_id in manifest_ids),
            """INSERT INTO remote_file_state
               (session_id, plan_file_id, status, error_msg, updated_at)
               VALUES (%s, %s, 'fetching', NULL, %s)
               ON CONFLICT (session_id, plan_file_id) DO UPDATE SET
                 status='fetching', error_msg=NULL,
                 updated_at=EXCLUDED.updated_at""",
            "normalized manifest fetching-status batch",
        )

    def update_manifest_rows_fetched(self, rows, session_id=None):
        if session_id is None:
            raise RuntimeError("[DB] session_id required for normalized remote state")
        now = _now_utc()
        self._remote_state_batch(
            ((session_id, manifest_id, local_rel_path, now)
             for local_rel_path, manifest_id in rows),
            """INSERT INTO remote_file_state
               (session_id, plan_file_id, status, local_rel_path,
                error_msg, updated_at)
               VALUES (%s, %s, 'fetched', %s, NULL, %s)
               ON CONFLICT (session_id, plan_file_id) DO UPDATE SET
                 status='fetched',
                 local_rel_path=EXCLUDED.local_rel_path,
                 error_msg=NULL,
                 updated_at=EXCLUDED.updated_at""",
            "normalized manifest fetched-status batch",
        )

    def update_manifest_rows_fetch_failed(self, manifest_ids, error_msg,
                                          session_id=None):
        if session_id is None:
            raise RuntimeError("[DB] session_id required for normalized remote state")
        now = _now_utc()
        error_msg = (error_msg or "")[:500]
        self._remote_state_batch(
            ((session_id, manifest_id, error_msg, now)
             for manifest_id in manifest_ids),
            """INSERT INTO remote_file_state
               (session_id, plan_file_id, status, error_msg, updated_at)
               VALUES (%s, %s, 'fetch_failed', %s, %s)
               ON CONFLICT (session_id, plan_file_id) DO UPDATE SET
                 status='fetch_failed',
                 error_msg=EXCLUDED.error_msg,
                 updated_at=EXCLUDED.updated_at""",
            "normalized manifest fetch-failure batch",
        )

    def update_chunk_status(self, session_id, chunk_index, status):
        now = _now_utc()

        def operation(conn):
            cur = conn.execute(
                """UPDATE remote_chunks SET status=%s, updated_at=%s
                   WHERE session_id=%s AND chunk_index=%s""",
                (status, now, session_id, chunk_index),
            )
            self._require_updated(
                cur,
                f"[DB] Remote chunk not found: session {session_id}, chunk {chunk_index}",
            )
            if status == "done":
                conn.execute(
                    """DELETE FROM remote_file_state
                       WHERE session_id=%s AND plan_file_id IN (
                         SELECT plan_file_id FROM remote_plan_files pf
                         JOIN remote_sessions s ON s.plan_id=pf.plan_id
                         WHERE s.session_id=%s AND pf.chunk_index=%s
                       ) AND COALESCE(status,'') != 'source_missing'""",
                    (session_id, session_id, chunk_index),
                )

        self._transaction(
            operation, f"normalized chunk {chunk_index + 1} status update")

    def get_pending_chunks(self, session_id):
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT chunk_index FROM remote_chunks
                   WHERE session_id=%s AND status!='done'
                   ORDER BY chunk_index""",
                (session_id,),
            ).fetchall()
        return [row["chunk_index"] for row in rows]

    def count_chunks(self, session_id):
        with self._pool.connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM remote_chunks WHERE session_id=%s",
                (session_id,),
            ).fetchone()["n"]

    def count_tape_file_records(self, tape_label):
        with self._pool.connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM files_index WHERE tape_label=%s",
                (tape_label,),
            ).fetchone()["n"]

    def get_local_indexed_original_paths(self, session_id, chunk_index, tape_label):
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT original_path FROM files_index
                   WHERE local_session_id=%s
                     AND local_chunk_index=%s
                     AND tape_label=%s""",
                (session_id, chunk_index, tape_label),
            ).fetchall()
        return {row["original_path"] for row in rows}

    def get_local_written_tape_paths(self, session_id, chunk_index, tape_label):
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT DISTINCT COALESCE(b.tape_path, f.stored_path) AS tape_path
                   FROM files_index AS f
                   LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                   WHERE f.local_session_id=%s
                     AND f.local_chunk_index=%s
                     AND f.tape_label=%s
                     AND COALESCE(b.tape_path, f.stored_path) IS NOT NULL""",
                (session_id, chunk_index, tape_label),
            ).fetchall()
        return [row["tape_path"] for row in rows if row["tape_path"]]

    def file_record_exists(self, original_path, tape_label, local_session_id=None,
                           local_chunk_index=None,
                           source_host=LEGACY_DEFAULT_SOURCE_HOST):
        source_host = _short_source_host(
            source_host or LEGACY_DEFAULT_SOURCE_HOST)
        key = _file_record_key(
            original_path, tape_label, local_session_id, local_chunk_index,
            source_host)
        with self._pool.connection() as conn:
            return bool(conn.execute(
                "SELECT 1 FROM files_index WHERE record_key=%s", (key,)
            ).fetchone())

    def register_tape(self, volume_label, capacity_gb=None):
        try:
            self._transaction(
                lambda conn: conn.execute(
                    """INSERT INTO tapes
                       (volume_label, date_formatted, total_capacity)
                       VALUES (%s, %s, %s)""",
                    (volume_label, _now_utc(), capacity_gb),
                ),
                f"register tape {volume_label}",
            )
            print(f"[DB] Tape '{volume_label}' registered successfully.")
            return True
        except errors.UniqueViolation:
            print(f"[DB] Tape '{volume_label}' is already in the database.")
            return False

    def tape_exists(self, volume_label):
        with self._pool.connection() as conn:
            return bool(conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label=%s", (volume_label,)
            ).fetchone())

    def get_tape(self, volume_label):
        with self._pool.connection() as conn:
            return _row(conn.execute(
                "SELECT * FROM tapes WHERE volume_label=%s", (volume_label,)
            ).fetchone())

    def list_tapes(self):
        with self._pool.connection() as conn:
            return _rows(conn.execute(
                "SELECT * FROM tapes ORDER BY date_formatted DESC"
            ).fetchall())

    def replace_formatted_tape(self, volume_label, capacity_gb=None,
                               previous_labels=None):
        labels = []
        for label in list(previous_labels or []) + [volume_label]:
            label = (label or "").strip()
            if label and label not in labels:
                labels.append(label)

        def operation(conn):
            removed = {}
            for label in labels:
                stats = self._delete_tape_records(conn, label)
                cur = conn.execute(
                    "DELETE FROM tapes WHERE volume_label=%s", (label,))
                if cur.rowcount or any(stats.values()):
                    removed[label] = stats
            conn.execute(
                """INSERT INTO tapes
                   (volume_label, date_formatted, total_capacity, used_space)
                   VALUES (%s, %s, %s, 0)""",
                (volume_label, _now_utc(), capacity_gb),
            )
            return removed

        removed = self._transaction(
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

    def _delete_tape_records(self, conn, volume_label):
        stats = {}
        stats["file_records"] = conn.execute(
            "DELETE FROM files_index WHERE tape_label=%s", (volume_label,)
        ).rowcount
        stats["bundles"] = conn.execute(
            "DELETE FROM archive_bundles WHERE tape_label=%s", (volume_label,)
        ).rowcount
        stats["runs"] = conn.execute(
            "DELETE FROM archive_runs WHERE tape_label=%s", (volume_label,)
        ).rowcount
        stats["directories"] = conn.execute(
            "DELETE FROM catalog_directories WHERE tape_label=%s", (volume_label,)
        ).rowcount
        return stats

    def delete_tape(self, volume_label):
        def operation(conn):
            self._delete_tape_records(conn, volume_label)
            cur = conn.execute(
                "DELETE FROM tapes WHERE volume_label=%s", (volume_label,))
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")

        self._transaction(operation, f"delete tape {volume_label}")
        print(f"[DB] Tape '{volume_label}' and its file records removed from database.")

    def update_tape_capacity(self, volume_label, capacity_gb):
        def operation(conn):
            cur = conn.execute(
                "UPDATE tapes SET total_capacity=%s WHERE volume_label=%s",
                (capacity_gb, volume_label),
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")

        self._transaction(operation, f"update tape capacity {volume_label}")
        print(f"[DB] Tape '{volume_label}' capacity set to {capacity_gb} GB.")

    def recalculate_tape_used_space(self, volume_label):
        def operation(conn):
            row = conn.execute(
                """SELECT COALESCE(SUM(file_size_bytes), 0) AS used
                   FROM files_index WHERE tape_label=%s""",
                (volume_label,),
            ).fetchone()
            new_used = row["used"]
            cur = conn.execute(
                "UPDATE tapes SET used_space=%s WHERE volume_label=%s",
                (new_used, volume_label),
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            return new_used

        return self._transaction(
            operation, f"recalculate tape used space {volume_label}")

    def delete_files_for_tape(self, volume_label):
        def operation(conn):
            if not conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label=%s", (volume_label,)
            ).fetchone():
                raise RuntimeError(f"[DB] Tape not found: {volume_label}")
            removed = self._delete_tape_records(conn, volume_label)["file_records"]
            conn.execute(
                "UPDATE tapes SET used_space=0 WHERE volume_label=%s",
                (volume_label,),
            )
            return removed

        removed = self._transaction(
            operation, f"delete file records for tape {volume_label}")
        print(f"[DB] Removed {removed} file record(s) for tape '{volume_label}' (tape entry kept).")

    def rename_tape(self, old_label, new_label):
        def operation(conn):
            old = conn.execute(
                "SELECT * FROM tapes WHERE volume_label=%s",
                (old_label,),
            ).fetchone()
            if not old:
                raise RuntimeError(f"[DB] Tape not found: {old_label}")
            conn.execute(
                """INSERT INTO tapes
                   (volume_label, date_formatted, total_capacity, used_space)
                   VALUES (%s, %s, %s, %s)""",
                (new_label, old["date_formatted"],
                 old["total_capacity"], old["used_space"]),
            )
            conn.execute(
                "UPDATE catalog_directories SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            conn.execute(
                "UPDATE archive_bundles SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            conn.execute(
                "UPDATE archive_runs SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            conn.execute(
                "UPDATE files_index SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            rows = conn.execute(
                """SELECT file_id, original_path, source_host,
                          local_session_id, local_chunk_index
                   FROM files_index WHERE tape_label=%s""",
                (new_label,),
            ).fetchall()
            # executemany pipelines the statements — one round-trip flight
            # instead of one per file record (hours at catalog scale).
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE files_index SET record_key=%s WHERE file_id=%s",
                    ((_file_record_key(
                        row["original_path"], new_label,
                        row["local_session_id"], row["local_chunk_index"],
                        row["source_host"]),
                      row["file_id"]) for row in rows),
                )
            conn.execute(
                "UPDATE remote_sessions SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            # local_chunks_manifest.tape_label is ON DELETE SET NULL: without
            # this repoint, deleting the old tape row silently wipes the chunk
            # assignments of every in-flight local session.
            conn.execute(
                "UPDATE local_chunks_manifest SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            conn.execute("DELETE FROM tapes WHERE volume_label=%s", (old_label,))

        self._transaction(operation, f"rename tape {old_label}")
        print(f"[DB] Tape '{old_label}' renamed to '{new_label}'.")

    def insert_file(self, file_name, original_path, file_size_bytes,
                    tape_label, is_packed, container_name, stored_path,
                    local_session_id=None, local_chunk_index=None,
                    source_host="local"):
        stats = self.bulk_upsert_files([{
            "file_name": file_name,
            "original_path": original_path,
            "file_size_bytes": file_size_bytes,
            "tape_label": tape_label,
            "source_host": source_host,
            "is_packed": is_packed,
            "container_name": container_name,
            "stored_path": stored_path,
            "local_session_id": local_session_id,
            "local_chunk_index": local_chunk_index,
        }])
        return bool(stats["inserted"])

    def search_files(self, name_query=None, date_from=None, date_to=None,
                     limit=None, offset=None, source_host=None, after_id=None):
        return self.search_catalog(
            name_query=name_query, date_from=date_from, date_to=date_to,
            limit=limit, offset=offset, source_host=source_host,
            after_id=after_id)

    @staticmethod
    def _catalog_filter(name_query=None, tape_label=None, date_from=None,
                        date_to=None, source_host=None):
        """Build a WHERE clause + params shared by search and count queries.

        ``name_query`` honours ``*``/``?`` wildcards; every other character
        (notably ``_``) is matched literally via ``ESCAPE '\\'``.
        """
        where = ["1=1"]
        params = []
        if name_query:
            pattern = contains_pattern(name_query)
            where.append("(f.catalog_name ILIKE %s ESCAPE '\\' "
                         "OR f.original_path ILIKE %s ESCAPE '\\')")
            params.extend([pattern, pattern])
        if date_from:
            where.append("f.catalog_backup_date >= %s::date")
            params.append(date_from)
        if date_to:
            where.append("f.catalog_backup_date < (%s::date + INTERVAL '1 day')")
            params.append(date_to)
        if tape_label:
            where.append("f.tape_label = %s")
            params.append(tape_label)
        if source_host:
            where.append("f.source_host = %s")
            params.append(_short_source_host(source_host))
        return where, params

    def search_catalog(self, name_query=None, tape_label=None,
                       date_from=None, date_to=None, limit=None, offset=None,
                       source_host=None, after_id=None):
        where, params = self._catalog_filter(
            name_query, tape_label, date_from, date_to, source_host)
        if after_id is not None:
            # Keyset pagination for bulk consumers (restore-all): OFFSET paging
            # rescans every skipped row, which is O(n^2) over a large result.
            where.append("f.file_id > %s")
            params.append(int(after_id))
        sql = self._catalog_select() + " WHERE " + " AND ".join(where)
        sql += (" ORDER BY f.file_id" if after_id is not None
                else " ORDER BY f.original_path, f.catalog_name")
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
            if offset is not None and after_id is None:
                sql += " OFFSET %s"
                params.append(int(offset))
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_search_files(self, name_query=None, date_from=None, date_to=None,
                           source_host=None):
        where, params = self._catalog_filter(
            name_query, date_from=date_from, date_to=date_to,
            source_host=source_host)
        sql = "SELECT COUNT(*) AS n FROM files_index f WHERE " + " AND ".join(where)
        with self._pool.connection() as conn:
            return conn.execute(sql, params).fetchone()["n"]

    def get_file_by_id(self, file_id):
        rows = self._catalog_rows("f.file_id = %s", (file_id,))
        return rows[0] if rows else None

    def search_by_directory(self, dir_path, limit=None, offset=None,
                            source_host=None, after_id=None):
        needle = dir_path.strip().rstrip("/\\")
        if not needle:
            return []
        where = ("(f.original_path ILIKE %s ESCAPE '\\' "
                 "OR f.original_path ILIKE %s ESCAPE '\\')")
        params = [prefix_pattern(needle), substring_pattern(needle)]
        if source_host:
            where += " AND f.source_host = %s"
            params.append(_short_source_host(source_host))
        if after_id is not None:
            where += " AND f.file_id > %s"
            params.append(int(after_id))
            sql = self._catalog_select() + " WHERE " + where
            sql += " ORDER BY f.file_id LIMIT %s"
            params.append(int(limit or 250))
            with self._pool.connection() as conn:
                rows = conn.execute(sql, params).fetchall()
            return [self._hydrate_file_row(row) for row in rows]
        if limit is None:
            return self._catalog_rows(where, params, "f.original_path")
        sql = self._catalog_select() + " WHERE " + where
        sql += " ORDER BY f.original_path LIMIT %s OFFSET %s"
        params.extend([int(limit), int(offset or 0)])
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_by_directory(self, dir_path, source_host=None):
        needle = dir_path.strip().rstrip("/\\")
        if not needle:
            return 0
        where = ("(f.original_path ILIKE %s ESCAPE '\\' "
                 "OR f.original_path ILIKE %s ESCAPE '\\')")
        params = [prefix_pattern(needle), substring_pattern(needle)]
        if source_host:
            where += " AND f.source_host = %s"
            params.append(_short_source_host(source_host))
        with self._pool.connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM files_index f WHERE " + where,
                params,
            ).fetchone()["n"]

    def list_backup_sessions(self):
        with self._pool.connection() as conn:
            return _rows(conn.execute("""
                SELECT DATE(f.catalog_backup_date) AS session_date,
                       f.tape_label, COUNT(*) AS file_count,
                       SUM(f.file_size_bytes) AS total_bytes
                FROM files_index f
                GROUP BY DATE(f.catalog_backup_date), f.tape_label
                ORDER BY session_date DESC
            """).fetchall())

    def search_by_session(self, session_date, tape_label, limit=None,
                          offset=None, after_id=None):
        where = """f.catalog_backup_date >= %s::date
                   AND f.catalog_backup_date < (%s::date + INTERVAL '1 day')
                   AND f.tape_label = %s"""
        params = [session_date, session_date, tape_label]
        if after_id is not None:
            where += " AND f.file_id > %s"
            params.append(int(after_id))
            sql = self._catalog_select() + " WHERE " + where
            sql += " ORDER BY f.file_id LIMIT %s"
            params.append(int(limit or 250))
            with self._pool.connection() as conn:
                rows = conn.execute(sql, params).fetchall()
            return [self._hydrate_file_row(row) for row in rows]
        if limit is None:
            return self._catalog_rows(where, params, "f.original_path")
        sql = self._catalog_select() + " WHERE " + where
        sql += " ORDER BY f.original_path LIMIT %s OFFSET %s"
        params.extend([int(limit), int(offset or 0)])
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_by_session(self, session_date, tape_label):
        with self._pool.connection() as conn:
            return conn.execute(
                """SELECT COUNT(*) AS n
                   FROM files_index f
                   WHERE f.catalog_backup_date >= %s::date
                     AND f.catalog_backup_date < (%s::date + INTERVAL '1 day')
                     AND f.tape_label = %s""",
                (session_date, session_date, tape_label),
            ).fetchone()["n"]

    def list_source_hosts(self):
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT DISTINCT COALESCE(source_host,'so02') AS source_host
                   FROM files_index
                   ORDER BY source_host"""
            ).fetchall()
        return [row["source_host"] for row in rows if row["source_host"]]

    def delete_files(self, file_ids):
        """Delete file records in ONE transaction and reconcile used_space.

        Replaces the per-row delete loop (one transaction per record) and fixes
        the ``tapes.used_space`` drift a single-record delete used to leave
        behind until the next full recalculation.
        """
        ids = sorted({int(file_id) for file_id in file_ids})
        if not ids:
            return 0

        def operation(conn):
            rows = conn.execute(
                "DELETE FROM files_index WHERE file_id = ANY(%s) "
                "RETURNING tape_label",
                (ids,),
            ).fetchall()
            labels = sorted({row["tape_label"] for row in rows})
            if labels:
                conn.execute(
                    """UPDATE tapes t
                       SET used_space = COALESCE(
                           (SELECT SUM(f.file_size_bytes) FROM files_index f
                            WHERE f.tape_label = t.volume_label), 0)
                       WHERE t.volume_label = ANY(%s)""",
                    (labels,),
                )
            return len(rows)

        return self._transaction(
            operation, f"delete {len(ids)} file record(s)")

    def delete_file(self, file_id):
        removed = self.delete_files([file_id])
        if not removed:
            raise RuntimeError(f"[DB] File record not found: {file_id}")

    def delete_session(self, kind, session_id):
        kind = (kind or "").strip().lower()
        session_id = int(session_id)
        if kind not in ("local", "remote"):
            raise RuntimeError(f"[DB] Unknown session kind: {kind}")

        def operation(conn):
            if kind == "local":
                refs = conn.execute(
                    "SELECT COUNT(*) AS n FROM files_index WHERE local_session_id=%s",
                    (session_id,),
                ).fetchone()["n"]
                if refs:
                    raise RuntimeError(
                        "[DB] Cannot delete a local session with archived file "
                        f"records still attached ({refs} file record(s)). "
                        "Delete the file records first or keep the session for "
                        "catalog provenance."
                    )
                conn.execute(
                    "DELETE FROM local_chunks_manifest WHERE session_id=%s",
                    (session_id,),
                )
                cur = conn.execute(
                    "DELETE FROM local_sessions WHERE session_id=%s",
                    (session_id,))
                self._require_updated(
                    cur, f"[DB] Local session not found: {session_id}")
                return cur.rowcount

            conn.execute(
                "DELETE FROM remote_file_state WHERE session_id=%s",
                (session_id,),
            )
            conn.execute(
                "DELETE FROM remote_chunks WHERE session_id=%s",
                (session_id,),
            )
            cur = conn.execute(
                "DELETE FROM remote_sessions WHERE session_id=%s", (session_id,))
            self._require_updated(
                cur, f"[DB] Remote session not found: {session_id}")
            return cur.rowcount

        removed = self._transaction(
            operation, f"delete {kind} session {session_id}")
        print(f"[DB] Deleted {kind} session {session_id}.")
        return removed

    def get_unreferenced_remote_data_summary(self):
        with self._pool.connection() as conn:
            return dict(conn.execute("""
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

    def cleanup_unreferenced_remote_data(self, compact=False):
        summary = self.get_unreferenced_remote_data_summary()
        if summary["active_sessions"]:
            raise RuntimeError(
                "[DB] Refusing cleanup while a remote session is active.")

        def operation(conn):
            plan_files = conn.execute("""
                DELETE FROM remote_plan_files pf
                USING remote_plans p
                WHERE p.plan_id=pf.plan_id
                  AND NOT EXISTS (
                    SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
                  )
            """)
            plans = conn.execute("""
                DELETE FROM remote_plans p
                WHERE NOT EXISTS (
                    SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
                )
            """)
            snapshot_files = conn.execute("""
                DELETE FROM remote_snapshot_files sf
                USING remote_snapshots sn
                WHERE sn.snapshot_id=sf.snapshot_id
                  AND NOT EXISTS (
                    SELECT 1 FROM remote_plans p WHERE p.snapshot_id=sn.snapshot_id
                  )
            """)
            snapshots = conn.execute("""
                DELETE FROM remote_snapshots sn
                WHERE NOT EXISTS (
                    SELECT 1 FROM remote_plans p WHERE p.snapshot_id=sn.snapshot_id
                )
            """)
            return {
                "plans_deleted": plans.rowcount,
                "plan_files_deleted": plan_files.rowcount,
                "snapshots_deleted": snapshots.rowcount,
                "snapshot_files_deleted": snapshot_files.rowcount,
            }

        result = self._transaction(operation, "cleanup unreferenced remote data")
        result.update({
            "catalog_files_preserved": self.count_search_files(),
            "before_bytes": None,
            "after_bytes": None,
            "reclaimed_bytes": None,
            "quick_check": "not_applicable_postgres",
            "foreign_key_violations": 0,
        })
        if compact:
            with self._pool.connection() as conn:
                # VACUUM cannot run inside a transaction block, and pooled
                # connections open one implicitly on first execute.
                previous = conn.autocommit
                conn.autocommit = True
                try:
                    conn.execute("VACUUM (ANALYZE)")
                finally:
                    conn.autocommit = previous
        return result

    def acquire_archiver_lock(self):
        """Take the cross-process single-writer lock for a tape-write run.

        Rides a dedicated pooled connection (session-level advisory locks
        survive commits) that stays pinned until ``release_archiver_lock``.
        Raises RuntimeError when another archiver instance holds it.
        """
        if self._lock_conn is not None:
            return
        conn = self._pool.getconn()
        try:
            conn.autocommit = True
            locked = conn.execute(
                "SELECT pg_try_advisory_lock(%s) AS locked",
                (self.ARCHIVER_LOCK_KEY,),
            ).fetchone()["locked"]
        except Exception:
            self._pool.putconn(conn)
            raise
        if not locked:
            self._pool.putconn(conn)
            raise RuntimeError(
                "[LOCK] Another archiver instance already holds the "
                "tape-writer lock. Only one tape-write run may be active at "
                "a time; finish or close the other instance and retry."
            )
        self._lock_conn = conn

    def release_archiver_lock(self):
        conn = self._lock_conn
        self._lock_conn = None
        if conn is None:
            return
        try:
            conn.execute(
                "SELECT pg_advisory_unlock(%s)", (self.ARCHIVER_LOCK_KEY,))
        except Exception:
            pass  # dropping the session releases the lock anyway
        try:
            conn.autocommit = False
            if self._pool is not None:
                self._pool.putconn(conn)
        except Exception:
            pass

    def close(self):
        self.release_archiver_lock()
        if self._pool is not None:
            self._pool.close()
            self._pool = None
