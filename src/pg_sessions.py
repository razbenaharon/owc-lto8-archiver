"""Local/remote session, snapshot, plan and chunk-state method group."""
import hashlib
import os

from .cli_errors import OperationalError
from .logsetup import get_logger
from .pipeline_types import ChunkStatus
from .pg_bulk import copy_rows
from .pg_core import (_coerce_timestamp_kwargs, _now_utc, _row, _rows,
                      _valid_columns)


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
def _streaming_fingerprint(kind, session_id):
    digest = hashlib.sha256()
    for value in ("remote-streaming", kind, int(session_id)):
        raw = str(value).encode("utf-8", errors="surrogatepass")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.digest()


class PgSessionMixin:
    """Local/remote archive sessions, snapshots, plans and chunk state.

    Mixin over :class:`src.pg_core.PgConnectionCore` (uses ``self._pool`` and
    ``self._transaction``); assembled in :class:`src.pg_db.PgDatabaseManager`.
    """

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
                tape_label, tape_generation, staging_dir, created_at, status)
               VALUES (%s, %s, %s, %s, %s,
                       (SELECT current_generation FROM tapes
                        WHERE volume_label=%s),
                       %s, %s, 'active')
               ON CONFLICT (session_label) DO UPDATE
                   SET session_label = EXCLUDED.session_label
               RETURNING session_id""",
            (session_label, remote_host, remote_user, remote_path,
             tape_label, tape_label, staging_dir, now),
        ).fetchone()["session_id"]

    def create_remote_session(self, session_label, remote_host, remote_user,
                              remote_path, tape_label, staging_dir):
        now = _now_utc()

        def operation(conn):
            return self._upsert_remote_session(
                conn, session_label, remote_host, remote_user, remote_path,
                tape_label, staging_dir, now)

        return self._transaction(operation, "create remote session")

    def create_remote_streaming_session(self, session_label, remote_host,
                                        remote_user, remote_path, tape_label,
                                        staging_dir):
        """Create an active remote session whose plan can grow by chunk."""
        now = _now_utc()

        def operation(conn):
            session_id = self._upsert_remote_session(
                conn, session_label, remote_host, remote_user, remote_path,
                tape_label, staging_dir, now)
            snapshot_fp = _streaming_fingerprint("snapshot", session_id)
            plan_fp = _streaming_fingerprint("plan", session_id)
            conn.execute(
                """INSERT INTO remote_snapshots
                   (remote_host, remote_path, fingerprint, total_files,
                    total_bytes, created_at)
                   VALUES (%s, %s, %s, 0, 0, %s)
                   ON CONFLICT (fingerprint) DO NOTHING""",
                (remote_host, remote_path, snapshot_fp, now),
            )
            snapshot_id = conn.execute(
                "SELECT snapshot_id FROM remote_snapshots WHERE fingerprint=%s",
                (snapshot_fp,),
            ).fetchone()["snapshot_id"]
            conn.execute(
                """INSERT INTO remote_plans
                   (snapshot_id, fingerprint, chunk_count, created_at)
                   VALUES (%s, %s, 0, %s)
                   ON CONFLICT (fingerprint) DO NOTHING""",
                (snapshot_id, plan_fp, now),
            )
            plan_id = conn.execute(
                "SELECT plan_id FROM remote_plans WHERE fingerprint=%s",
                (plan_fp,),
            ).fetchone()["plan_id"]
            conn.execute(
                """UPDATE remote_sessions
                   SET plan_id=%s, scan_complete=FALSE, scan_error=NULL
                   WHERE session_id=%s""",
                (plan_id, session_id),
            )
            return session_id

        return self._transaction(
            operation, "create remote streaming session")

    def create_remote_session_with_plan(
            self, session_label, remote_host, remote_user, remote_path,
            tape_label, staging_dir, rows, *,
            stored_tar_write_enabled=False, reader_contract_version=None,
            require_container_format_schema=False):
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
                conn, session_id, remote_host, remote_path, rows, by_path, now,
                stored_tar_write_enabled=stored_tar_write_enabled,
                reader_contract_version=reader_contract_version,
                require_container_format_schema=
                    require_container_format_schema)
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

    def list_active_remote_sessions(self, remote_host, remote_path):
        """All active sessions for this host+path, newest first.

        The interactive path resumes the single newest active session, but the
        non-interactive (headless) path must refuse to guess when more than one
        exists — so it needs the full list, not just the newest.
        """
        rows = self._run_read(
            lambda conn: conn.execute(
                """SELECT * FROM remote_sessions
                   WHERE remote_host=%s AND remote_path=%s AND status='active'
                   ORDER BY session_id DESC""",
                (remote_host, remote_path),
            ).fetchall(),
            f"list_active_remote_sessions({remote_host})")
        return [_row(r) for r in rows]

    def get_remote_session(self, session_id):
        return self._run_read(
            lambda conn: _row(conn.execute(
                "SELECT * FROM remote_sessions WHERE session_id=%s",
                (session_id,),
            ).fetchone()),
            f"get_remote_session({session_id})")

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

    def insert_remote_manifest_batch(
            self, session_id, rows, *, stored_tar_write_enabled=False,
            reader_contract_version=None,
            require_container_format_schema=False):
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
                session["remote_path"], rows, by_path, now,
                stored_tar_write_enabled=stored_tar_write_enabled,
                reader_contract_version=reader_contract_version,
                require_container_format_schema=
                    require_container_format_schema)

        return self._transaction(operation, "insert remote manifest batch")

    def append_remote_streaming_chunk(
            self, session_id, chunk_index, rows, *,
            stored_tar_write_enabled=False, reader_contract_version=None,
            require_container_format_schema=False):
        """Append one discovered chunk to a streaming remote session.

        Duplicate canonical remote paths already present in the session
        snapshot are ignored so an incomplete streaming scan can be resumed by
        rescanning from the beginning.
        """
        rows = list(rows)
        by_path = self._validate_remote_manifest_rows(rows)
        now = _now_utc()

        def operation(conn):
            if (require_container_format_schema
                    and not self._column_exists_conn(
                        conn, "remote_chunks", "packaging_format")):
                raise RuntimeError(
                    "[DB] Migration 015 is required before a production "
                    "scanner can create a new chunk")
            session = conn.execute(
                """SELECT s.*, p.snapshot_id
                   FROM remote_sessions s
                   JOIN remote_plans p ON p.plan_id=s.plan_id
                   WHERE s.session_id=%s
                   FOR UPDATE OF s""",
                (session_id,),
            ).fetchone()
            if not session:
                raise RuntimeError(f"[DB] Remote session not found: {session_id}")
            if not session["plan_id"]:
                raise RuntimeError(
                    f"[DB] Remote session {session_id} has no streaming plan")

            # Plan 1 Task 2.4: a SEALED chunk's membership is immutable.
            # Appending to one would change what an already-written (or
            # about-to-be-written) tape chunk is supposed to contain, and the
            # catalog would then describe bytes that are not there.
            if self._column_exists_conn(conn, 'remote_chunks',
                                        'membership_state'):
                sealed = conn.execute(
                    """SELECT membership_state FROM remote_chunks
                       WHERE session_id=%s AND chunk_index=%s""",
                    (session_id, int(chunk_index)),
                ).fetchone()
                if sealed and sealed["membership_state"] == 'sealed':
                    raise RuntimeError(
                        f"[DB] Chunk {int(chunk_index) + 1} of session "
                        f"{session_id} is SEALED; refusing to append members. "
                        "Its contents are fixed — a later append would change "
                        "what a written chunk is recorded as containing.")

            existing = {
                row["remote_path"]
                for row in conn.execute(
                    """SELECT remote_path
                       FROM remote_snapshot_files
                       WHERE snapshot_id=%s AND remote_path = ANY(%s)""",
                    (session["snapshot_id"], list(by_path.keys())),
                ).fetchall()
            }
            filtered = [
                (int(chunk_index), path, os.path.basename(path), size)
                for path, size in by_path.items()
                if path not in existing
            ]
            if not filtered:
                return {"inserted_files": 0, "inserted_bytes": 0}

            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO remote_snapshot_files
                       (snapshot_id, remote_path, file_size_bytes)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (snapshot_id, remote_path) DO NOTHING""",
                    ((session["snapshot_id"], path, int(size))
                     for _, path, _, size in filtered),
                )

            ids = {
                row["remote_path"]: row["snapshot_file_id"]
                for row in conn.execute(
                    """SELECT remote_path, snapshot_file_id
                       FROM remote_snapshot_files
                       WHERE snapshot_id=%s AND remote_path = ANY(%s)""",
                    (session["snapshot_id"], [row[1] for row in filtered]),
                ).fetchall()
            }
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO remote_plan_files
                       (plan_id, snapshot_file_id, chunk_index, ordinal)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (plan_id, snapshot_file_id) DO NOTHING""",
                    ((session["plan_id"], ids[path], int(chunk_index), ordinal)
                     for ordinal, (_, path, _, _) in enumerate(filtered)),
                )
                has_format_schema = self._column_exists_conn(
                    conn, "remote_chunks", "packaging_format")
                if has_format_schema:
                    packaging_format = self.assign_new_chunk_format(
                        session_id, int(chunk_index),
                        stored_tar_write_enabled=stored_tar_write_enabled,
                        reader_contract_version=reader_contract_version,
                        conn=conn)
                    cur.execute(
                        """INSERT INTO remote_chunks
                           (session_id, chunk_index, status, updated_at,
                            packaging_format, packaging_assigned_at)
                           VALUES (%s, %s, 'pending', %s, %s, %s)
                           ON CONFLICT (session_id, chunk_index) DO NOTHING""",
                        (session_id, int(chunk_index), now,
                         packaging_format.value, now),
                    )
                    persisted = conn.execute(
                        """SELECT packaging_format FROM remote_chunks
                           WHERE session_id=%s AND chunk_index=%s""",
                        (session_id, int(chunk_index)),
                    ).fetchone()
                    if persisted is None or persisted["packaging_format"] != \
                            packaging_format.value:
                        raise RuntimeError(
                            "[DB] Existing chunk format conflicts with the "
                            "durable session boundary")
                else:
                    if stored_tar_write_enabled:
                        raise RuntimeError(
                            "[DB] Stored TAR assignment requested but migration "
                            "015 is not installed")
                    cur.execute(
                        """INSERT INTO remote_chunks
                           (session_id, chunk_index, status, updated_at)
                           VALUES (%s, %s, 'pending', %s)
                           ON CONFLICT (session_id, chunk_index) DO NOTHING""",
                        (session_id, int(chunk_index), now),
                    )

            inserted_files = len(filtered)
            inserted_bytes = sum(int(row[3]) for row in filtered)
            conn.execute(
                """UPDATE remote_sessions
                   SET total_files=total_files + %s,
                       total_bytes=total_bytes + %s,
                       chunk_count=GREATEST(chunk_count, %s)
                   WHERE session_id=%s""",
                (inserted_files, inserted_bytes, int(chunk_index) + 1,
                 session_id),
            )
            conn.execute(
                """UPDATE remote_snapshots
                   SET total_files=total_files + %s,
                       total_bytes=total_bytes + %s
                   WHERE snapshot_id=%s""",
                (inserted_files, inserted_bytes, session["snapshot_id"]),
            )
            conn.execute(
                """UPDATE remote_plans
                   SET chunk_count=GREATEST(chunk_count, %s)
                   WHERE plan_id=%s""",
                (int(chunk_index) + 1, session["plan_id"]),
            )
            return {
                "inserted_files": inserted_files,
                "inserted_bytes": inserted_bytes,
            }

        return self._transaction(
            operation, f"append streaming remote chunk {chunk_index + 1}")

    def seal_remote_chunk(self, session_id, chunk_index, *,
                          expected_file_count, expected_bytes,
                          scan_segment_id=None, first_scan_ordinal=None,
                          last_scan_ordinal=None):
        """Fix a chunk's membership, atomically, with what it should contain.

        Plan 1, Task 2.4. Sealing records three things in ONE transaction:
        the membership state, the expectation (how many files and how many
        LOGICAL SOURCE bytes the chunk was planned from), and — when the chunk
        came from the frontier — which ready segment range it consumed.

        They must be atomic together. A chunk that is sealed without its
        expectation cannot be checked against what was actually written, and a
        chunk whose segment reference lands separately can be sealed against a
        range the segment does not know it gave away.

        Returns True when this call sealed it, False when it was already sealed
        with the same expectation (idempotent under the ambiguous-commit retry).
        """
        now = _now_utc()

        def operation(conn):
            if not self._column_exists_conn(conn, 'remote_chunks',
                                            'membership_state'):
                raise RuntimeError(
                    "[DB] Sealing requires migration 014 (remote_chunks has no "
                    "membership_state column). Apply it explicitly first.")
            current = conn.execute(
                """SELECT membership_state, expected_file_count, expected_bytes
                   FROM remote_chunks
                   WHERE session_id=%s AND chunk_index=%s FOR UPDATE""",
                (session_id, int(chunk_index)),
            ).fetchone()
            if current is None:
                raise RuntimeError(
                    f"[DB] Remote chunk not found: session {session_id}, "
                    f"chunk {chunk_index}")
            if current["membership_state"] == 'sealed':
                same = (current["expected_file_count"] == int(expected_file_count)
                        and current["expected_bytes"] == int(expected_bytes))
                if same:
                    return False
                raise RuntimeError(
                    f"[DB] Chunk {int(chunk_index) + 1} of session "
                    f"{session_id} is already sealed with a DIFFERENT "
                    f"expectation ({current['expected_file_count']} files / "
                    f"{current['expected_bytes']} bytes). Refusing to re-seal.")

            conn.execute(
                """UPDATE remote_chunks
                   SET membership_state='sealed', expected_file_count=%s,
                       expected_bytes=%s, updated_at=%s
                   WHERE session_id=%s AND chunk_index=%s""",
                (int(expected_file_count), int(expected_bytes), now,
                 session_id, int(chunk_index)),
            )
            if scan_segment_id is not None:
                conn.execute(
                    """INSERT INTO remote_chunk_scan_segments
                       (session_id, chunk_index, scan_segment_id,
                        first_scan_ordinal, last_scan_ordinal, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (scan_segment_id, first_scan_ordinal,
                                    last_scan_ordinal) DO NOTHING""",
                    (session_id, int(chunk_index), scan_segment_id,
                     int(first_scan_ordinal), int(last_scan_ordinal), now),
                )
            return True

        return self._transaction(
            operation, f"seal chunk {int(chunk_index) + 1}")

    def _persist_remote_plan(
            self, conn, session_id, remote_host, remote_path, rows, by_path, now,
            *, stored_tar_write_enabled=False, reader_contract_version=None,
            require_container_format_schema=False):
        """Persist snapshot/plan/chunk rows for a session (idempotent by
        fingerprint, so it is safe inside the ambiguous-commit retry loop)."""
        if (require_container_format_schema
                and not self._column_exists_conn(
                    conn, "remote_chunks", "packaging_format")):
            raise RuntimeError(
                "[DB] Migration 015 is required before a production plan can "
                "create new chunks")

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
            # COPY, not executemany: multi-million-file snapshots insert in
            # one stream instead of a pipelined statement per row.
            with conn.cursor() as cur:
                copy_rows(
                    cur, "remote_snapshot_files",
                    ("snapshot_id", "remote_path", "file_size_bytes"),
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
                copy_rows(
                    cur, "remote_plan_files",
                    ("plan_id", "snapshot_file_id", "chunk_index", "ordinal"),
                    ((plan_id, ids[_canonical_remote_path(row[1])],
                      int(row[0]), ordinal)
                     for ordinal, row in enumerate(rows)),
                )
        conn.execute(
            "UPDATE remote_sessions SET plan_id=%s WHERE session_id=%s",
            (plan_id, session_id),
        )
        has_format_schema = self._column_exists_conn(
            conn, "remote_chunks", "packaging_format")
        with conn.cursor() as cur:
            if has_format_schema:
                format_rows = []
                for chunk_index in chunk_indexes:
                    packaging_format = self.assign_new_chunk_format(
                        session_id, chunk_index,
                        stored_tar_write_enabled=stored_tar_write_enabled,
                        reader_contract_version=reader_contract_version,
                        conn=conn)
                    format_rows.append((
                        session_id, chunk_index, now,
                        packaging_format.value, now))
                cur.executemany(
                    """INSERT INTO remote_chunks
                       (session_id, chunk_index, status, updated_at,
                        packaging_format, packaging_assigned_at)
                       VALUES (%s, %s, 'pending', %s, %s, %s)
                       ON CONFLICT (session_id, chunk_index) DO NOTHING""",
                    format_rows,
                )
                persisted = {
                    int(row["chunk_index"]): row["packaging_format"]
                    for row in conn.execute(
                        """SELECT chunk_index, packaging_format
                           FROM remote_chunks
                           WHERE session_id=%s AND chunk_index=ANY(%s)""",
                        (session_id, chunk_indexes),
                    ).fetchall()
                }
                expected = {
                    int(row[1]): row[3] for row in format_rows}
                if persisted != expected:
                    raise RuntimeError(
                        "[DB] Existing chunk format conflicts with the durable "
                        "session boundary")
            else:
                if stored_tar_write_enabled:
                    raise RuntimeError(
                        "[DB] Stored TAR assignment requested but migration 015 "
                        "is not installed")
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
        return self._run_read(
            lambda conn: _rows(conn.execute(
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
            ).fetchall()),
            f"get_chunk_files(session {session_id}, chunk {chunk_index})")

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
        rows = self._run_read(
            lambda conn: conn.execute(
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
            ).fetchall(),
            f"get_chunk_size_summary(session {session_id})")
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

    def transition_chunk(self, session_id, chunk_index, to_status, *,
                         expected_from=None, owner_token=None, attempt_id=None,
                         error_msg=None, clear_error=False, validate=True):
        """The ONE method that moves a chunk between persisted states.

        Plan 1, Task 1.5. Before this, chunk status was written from half a
        dozen call sites with bare strings, so "which transitions are legal"
        existed only as a mental model — and the one transition that must never
        happen automatically (out of ``backing``, where bytes may already be on
        tape) was protected by convention rather than by code.

        Arguments:
          ``expected_from``  compare-and-swap predicate: a status or iterable of
                             statuses the row must currently be in. ``None``
                             means unconditional (the legacy behaviour, kept for
                             the compatibility wrapper).
          ``owner_token`` /
          ``attempt_id``     validated against the row when those columns exist
                             (migration 014). On a database without them a
                             caller that supplies either is refused rather than
                             silently unprotected — a claim you believe you hold
                             but that is not enforced is worse than none.
          ``error_msg`` /
          ``clear_error``    written deliberately, never as a side effect. Pass
                             ``clear_error=True`` to blank a stale message.
          ``validate``       check :data:`~src.pipeline_types.CHUNK_TRANSITIONS`.

        Returns True when the row moved, False when the compare-and-swap did not
        match (someone else changed it first). Raises
        :class:`~src.pipeline_types.ForbiddenTransition` for a transition the
        matrix does not allow — the old state is preserved either way.
        """
        from .pipeline_types import (ChunkStatus, ForbiddenTransition,
                                     assert_chunk_transition)

        target = ChunkStatus(to_status).value if validate else str(to_status)
        if expected_from is None:
            expected = None
        elif isinstance(expected_from, (str, ChunkStatus)):
            expected = [ChunkStatus(expected_from).value]
        else:
            expected = [ChunkStatus(s).value for s in expected_from]
        if validate and expected:
            for source in expected:
                assert_chunk_transition(source, target)
        now = _now_utc()

        def operation(conn):
            if validate and not expected:
                # No caller-supplied predicate: read the row's actual state and
                # check the matrix against THAT, so a forbidden move (notably an
                # automatic retry out of 'backing') is refused wherever it comes
                # from, not only where the caller happened to know the source.
                row = conn.execute(
                    """SELECT status FROM remote_chunks
                       WHERE session_id=%s AND chunk_index=%s FOR UPDATE""",
                    (session_id, chunk_index),
                ).fetchone()
                if row is not None and row["status"] != target:
                    assert_chunk_transition(row["status"], target)
            has_owner = self._column_exists_conn(
                conn, 'remote_chunks', 'owner_token')
            if (owner_token is not None or attempt_id is not None) \
                    and not has_owner:
                raise RuntimeError(
                    "[DB] A chunk claim was supplied, but this database has no "
                    "owner_token/attempt_id columns (migration 014 is not "
                    "applied). Refusing the transition rather than pretending "
                    "the claim is enforced.")

            sets = ["status=%s", "updated_at=%s"]
            params = [target, now]
            if clear_error:
                sets.append("error_msg=NULL")
            elif error_msg is not None:
                sets.append("error_msg=%s")
                params.append(error_msg)

            where = ["session_id=%s", "chunk_index=%s"]
            params.extend([session_id, chunk_index])
            if expected:
                where.append("status = ANY(%s)")
                params.append(expected)
            if owner_token is not None:
                where.append("owner_token=%s")
                params.append(owner_token)
            if attempt_id is not None:
                where.append("attempt_id=%s")
                params.append(attempt_id)

            cur = conn.execute(
                f"UPDATE remote_chunks SET {', '.join(sets)} "
                f"WHERE {' AND '.join(where)}",
                params,
            )
            if cur.rowcount == 0:
                if expected or owner_token is not None or attempt_id is not None:
                    # A lost compare-and-swap is a legitimate outcome, not an
                    # error: another worker (or a reconciliation) got there
                    # first and the old state stands.
                    return False
                self._require_updated(
                    cur,
                    f"[DB] Remote chunk not found: session {session_id}, "
                    f"chunk {chunk_index}",
                )
            if target == ChunkStatus.DONE.value:
                conn.execute(
                    """DELETE FROM remote_file_state
                       WHERE session_id=%s AND plan_file_id IN (
                         SELECT plan_file_id FROM remote_plan_files pf
                         JOIN remote_sessions s ON s.plan_id=pf.plan_id
                         WHERE s.session_id=%s AND pf.chunk_index=%s
                       ) AND COALESCE(status,'') != 'source_missing'""",
                    (session_id, session_id, chunk_index),
                )
            return True

        return self._transaction(
            operation, f"chunk {chunk_index + 1} -> {target}")

    #: Chunk states a stale-lease sweep may EVER return to the pool.
    #:
    #: 'backing' is deliberately absent and must stay absent. It means the
    #: physical tape write began, so the bytes may be on the cartridge; a lease
    #: timeout says nothing about that. Returning a 'backing' chunk to the pool
    #: would let another worker re-fetch and re-write it — a double write onto
    #: media that cannot be corrected in place (incident 010).
    RECLAIMABLE_CHUNK_STATES = (ChunkStatus.FETCHING.value,
                                ChunkStatus.PACKING.value)

    def claim_chunk_for_staging(self, session_id, chunk_index, owner_token,
                                attempt_id, lease_seconds=3600):
        """Take exclusive PRE-WRITE ownership of a chunk (Plan 1, Task 3.1).

        A compare-and-swap: the chunk must currently be re-drivable
        (``pending`` / ``fetch_failed`` / ``backup_failed``) and hold no live
        lease. Exactly one caller wins; the loser gets ``False`` and must pick
        different work rather than assume it is free.

        This claim covers **fetch and pack only**. The tape itself is guarded by
        LTFS ownership, and duplicate *processes* by the cluster-wide archiver
        advisory lock — three layers that protect different things, none of
        which replaces the others.
        """
        now = _now_utc()

        def operation(conn):
            if not self._column_exists_conn(conn, 'remote_chunks',
                                            'owner_token'):
                raise RuntimeError(
                    "[DB] Chunk claims require migration 014. Refusing to "
                    "pretend a claim is enforced when the columns do not "
                    "exist.")
            cur = conn.execute(
                """UPDATE remote_chunks
                   SET status='fetching', owner_token=%s, attempt_id=%s,
                       lease_expires_at=%s + make_interval(secs => %s),
                       updated_at=%s
                   WHERE session_id=%s AND chunk_index=%s
                     AND status IN ('pending','fetch_failed','backup_failed')
                     AND (owner_token IS NULL OR lease_expires_at < %s)""",
                (owner_token, attempt_id, now, float(lease_seconds), now,
                 session_id, int(chunk_index), now),
            )
            return cur.rowcount == 1

        return self._transaction(
            operation, f"claim chunk {int(chunk_index) + 1} for staging")

    def renew_chunk_claim(self, session_id, chunk_index, owner_token,
                          lease_seconds=3600):
        """Extend a live claim. Fails if someone else now owns it."""
        now = _now_utc()

        def operation(conn):
            cur = conn.execute(
                """UPDATE remote_chunks
                   SET lease_expires_at=%s + make_interval(secs => %s),
                       updated_at=%s
                   WHERE session_id=%s AND chunk_index=%s AND owner_token=%s
                     AND status <> 'backing'""",
                (now, float(lease_seconds), now, session_id, int(chunk_index),
                 owner_token),
            )
            return cur.rowcount == 1

        return self._transaction(
            operation, f"renew claim on chunk {int(chunk_index) + 1}")

    def release_chunk_claim(self, session_id, chunk_index, owner_token,
                            to_status=None):
        """Give a PRE-WRITE claim back. Never touches a 'backing' chunk.

        ``to_status`` optionally returns the chunk to the pool (normally
        ``pending``). Omit it to drop the lease while leaving the status alone
        — used when the caller has already transitioned the chunk itself.
        """
        now = _now_utc()

        def operation(conn):
            sets = ["owner_token=NULL", "attempt_id=NULL",
                    "lease_expires_at=NULL", "updated_at=%s"]
            params = [now]
            if to_status is not None:
                sets.insert(0, "status=%s")
                params.insert(0, str(to_status))
            params.extend([session_id, int(chunk_index), owner_token])
            cur = conn.execute(
                f"""UPDATE remote_chunks SET {', '.join(sets)}
                    WHERE session_id=%s AND chunk_index=%s AND owner_token=%s
                      AND status <> 'backing'""",
                params,
            )
            return cur.rowcount == 1

        return self._transaction(
            operation, f"release claim on chunk {int(chunk_index) + 1}")

    def list_expired_chunk_claims(self, session_id):
        """Chunks whose PRE-WRITE lease has lapsed. Read-only.

        Deliberately **reports** rather than reclaims. A lapsed lease means
        "nobody renewed this", not "no worker is running": the worker may be
        alive on a wedged SSH call, or on another host. Startup reconciliation
        (Task 3.2) matches this against process and staging evidence before
        anything is returned to the pool.

        A 'backing' chunk can never appear here — the SQL excludes it — because
        no amount of elapsed time makes an ambiguous tape write safe to retry.
        """
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT session_id, chunk_index, status, owner_token,
                          attempt_id, lease_expires_at
                   FROM remote_chunks
                   WHERE session_id=%s AND owner_token IS NOT NULL
                     AND lease_expires_at IS NOT NULL
                     AND lease_expires_at < %s
                     AND status = ANY(%s)
                   ORDER BY chunk_index""",
                (session_id, _now_utc(), list(self.RECLAIMABLE_CHUNK_STATES)),
            ).fetchall()),
            f"list_expired_chunk_claims({session_id})")

    def reclaim_expired_chunk(self, session_id, chunk_index, owner_token,
                              evidence):
        """Return ONE provably-abandoned pre-write chunk to the pool.

        ``evidence`` is required and recorded: the caller must have established
        that the previous owner is gone (Task 3.2 matches PID + process
        creation time + remote token). This method will not reclaim on elapsed
        time alone, and the SQL refuses a 'backing' chunk outright even if a
        caller asks.
        """
        if not evidence:
            raise RuntimeError(
                "[DB] Refusing to reclaim a chunk without process evidence. A "
                "lapsed lease is not proof that the worker is gone.")
        now = _now_utc()

        def operation(conn):
            cur = conn.execute(
                """UPDATE remote_chunks
                   SET status='pending', owner_token=NULL, attempt_id=NULL,
                       lease_expires_at=NULL, error_msg=%s, updated_at=%s
                   WHERE session_id=%s AND chunk_index=%s AND owner_token=%s
                     AND status = ANY(%s)""",
                (f"reclaimed after abandonment: {evidence}", now, session_id,
                 int(chunk_index), owner_token,
                 list(self.RECLAIMABLE_CHUNK_STATES)),
            )
            if cur.rowcount == 1:
                get_logger().warning(
                    "chunk_reclaimed: session=%s chunk=%s previous_owner=%s "
                    "evidence=%s", session_id, int(chunk_index) + 1,
                    owner_token, evidence)
            return cur.rowcount == 1

        return self._transaction(
            operation, f"reclaim chunk {int(chunk_index) + 1}")

    def update_chunk_status(self, session_id, chunk_index, status):
        """Compatibility wrapper over :meth:`transition_chunk` (Task 1.5).

        Unconditional and unvalidated, exactly as before, so no existing caller
        changes behaviour while they migrate. New code should call
        ``transition_chunk`` with an ``expected_from`` predicate.
        """
        self.transition_chunk(session_id, chunk_index, status, validate=False)

    def get_pending_chunks(self, session_id):
        rows = self._run_read(
            lambda conn: conn.execute(
                """SELECT chunk_index FROM remote_chunks
                   WHERE session_id=%s AND status!='done'
                   ORDER BY chunk_index""",
                (session_id,),
            ).fetchall(),
            f"get_pending_chunks({session_id})")
        return [row["chunk_index"] for row in rows]

    def count_chunks(self, session_id):
        return self._run_read(
            lambda conn: conn.execute(
                "SELECT COUNT(*) AS n FROM remote_chunks WHERE session_id=%s",
                (session_id,),
            ).fetchone()["n"],
            f"count_chunks({session_id})")

    def get_chunks_with_status(self, session_id, status):
        """Chunk indices currently in ``status`` (e.g. 'backing'), ascending.

        Used to detect a chunk a *prior* run left mid-write: the current run has
        not written anything yet, so a 'backing' chunk at resume start is
        unambiguously from an earlier, interrupted run and must not be blindly
        re-written (it may already be on tape).
        """
        rows = self._run_read(
            lambda conn: conn.execute(
                """SELECT chunk_index FROM remote_chunks
                   WHERE session_id=%s AND status=%s
                   ORDER BY chunk_index""",
                (session_id, status),
            ).fetchall(),
            f"get_chunks_with_status({session_id},{status})")
        return [row["chunk_index"] for row in rows]

    def get_next_remote_chunk_index(self, session_id):
        value = self._run_read(
            lambda conn: conn.execute(
                """SELECT COALESCE(MAX(chunk_index) + 1, 0) AS next_index
                   FROM remote_chunks WHERE session_id=%s""",
                (session_id,),
            ).fetchone()["next_index"],
            f"get_next_remote_chunk_index({session_id})")
        return int(value)

    def get_remote_existing_snapshot_paths(self, session_id, paths):
        paths = [_canonical_remote_path(path) for path in paths]
        if not paths:
            return set()
        rows = self._run_read(
            lambda conn: conn.execute(
                """SELECT sf.remote_path
                   FROM remote_sessions s
                   JOIN remote_plans p ON p.plan_id=s.plan_id
                   JOIN remote_snapshot_files sf
                    ON sf.snapshot_id=p.snapshot_id
                   WHERE s.session_id=%s AND sf.remote_path = ANY(%s)""",
                (session_id, paths),
            ).fetchall(),
            f"get_remote_existing_snapshot_paths({session_id})")
        return {row["remote_path"] for row in rows}

    def session_has_snapshot_membership(self, session_id):
        """Whether the session's snapshot already contains any file row.

        This is a cheap existence probe, not a count. It is used only when no
        durable bootstrap row exists: a definite empty snapshot proves that
        reconciliation is unnecessary, while any membership could pre-date
        the frontier and must be treated conservatively.
        """
        return self._run_read(
            lambda conn: bool(conn.execute(
                """SELECT EXISTS (
                         SELECT 1
                         FROM remote_sessions s
                         JOIN remote_plans p ON p.plan_id=s.plan_id
                         JOIN remote_snapshot_files sf
                           ON sf.snapshot_id=p.snapshot_id
                         WHERE s.session_id=%s
                       ) AS has_membership""",
                (session_id,),
            ).fetchone()["has_membership"]),
            f"session_has_snapshot_membership({session_id})")

    def get_session_membership_summary(self, session_id):
        """Read-only membership facts for a session (Plan 1, Task 4.1).

        Reports what the PLAN says, from the plan rows themselves rather than
        from the session's cached totals — the cached figures are the ones an
        interrupted run can leave stale, and this report exists precisely to be
        trusted when other numbers are not.
        """
        return self._run_read(
            lambda conn: _row(conn.execute(
                """SELECT COUNT(*)                       AS member_rows,
                          COUNT(DISTINCT pf.chunk_index) AS distinct_chunks,
                          MAX(pf.chunk_index)            AS max_chunk_index,
                          COALESCE(SUM(sf.file_size_bytes), 0) AS member_bytes
                   FROM remote_sessions s
                   JOIN remote_plan_files pf ON pf.plan_id = s.plan_id
                   JOIN remote_snapshot_files sf
                        ON sf.snapshot_file_id = pf.snapshot_file_id
                   WHERE s.session_id=%s""",
                (session_id,),
            ).fetchone()),
            f"get_session_membership_summary({session_id})")

    def get_chunk_state_counts(self, session_id):
        """``{status: count}`` plus the highest chunk index. Read-only."""
        rows = self._run_read(
            lambda conn: conn.execute(
                """SELECT status, COUNT(*) AS n, MAX(chunk_index) AS max_index
                   FROM remote_chunks WHERE session_id=%s
                   GROUP BY status ORDER BY status""",
                (session_id,),
            ).fetchall(),
            f"get_chunk_state_counts({session_id})")
        return [dict(row) for row in rows]

    def list_remote_sessions(self):
        """Every remote session, oldest first. Read-only.

        The all-session health report discovers its subjects here rather than
        from a list in code, so a session created after the report was written
        is still audited.
        """
        return self._run_read(
            lambda conn: conn.execute(
                "SELECT * FROM remote_sessions ORDER BY session_id"
            ).fetchall(),
            "list remote sessions")

    def session_tape_footprint(self, session_id):
        """Which tapes this session ACTUALLY wrote to, with what. Read-only.

        Derived from ``archive_runs`` and ``files_index``, not from
        ``remote_sessions.tape_label`` — that column is the session's *next*
        write target and says nothing about where finished chunks landed. The
        two diverge routinely after a cartridge change, and reading the label as
        if it were the footprint once produced a wrong conclusion that a
        session's completed work had been destroyed.
        """
        return self._run_read(
            lambda conn: conn.execute(
                """SELECT r.tape_label,
                          count(DISTINCT r.run_id)      AS runs,
                          count(f.file_id)              AS stored_objects,
                          coalesce(sum(f.file_size_bytes), 0) AS bytes,
                          min(r.started_at)             AS first_run,
                          max(r.started_at)             AS last_run
                   FROM archive_runs r
                   LEFT JOIN files_index f ON f.archive_run_id = r.run_id
                   WHERE r.remote_session_id = %s
                   GROUP BY r.tape_label
                   ORDER BY r.tape_label""",
                (session_id,),
            ).fetchall(),
            f"tape footprint for session {session_id}")

    def find_sessions_sharing_plan(self, session_id):
        """Other sessions on the SAME plan or snapshot. Read-only.

        A shared plan means a decision about this session's membership is also
        a decision about another's. Nothing may be reorganised until that is
        understood, so the report surfaces it rather than leaving it implicit.
        """
        rows = self._run_read(
            lambda conn: conn.execute(
                """WITH me AS (
                       SELECT s.session_id, s.plan_id, p.snapshot_id
                       FROM remote_sessions s
                       LEFT JOIN remote_plans p ON p.plan_id = s.plan_id
                       WHERE s.session_id=%s
                   )
                   SELECT o.session_id, o.session_label, o.status,
                          o.plan_id, p.snapshot_id
                   FROM remote_sessions o
                   LEFT JOIN remote_plans p ON p.plan_id = o.plan_id
                   JOIN me ON (o.plan_id = me.plan_id
                               OR p.snapshot_id = me.snapshot_id)
                   WHERE o.session_id <> me.session_id
                   ORDER BY o.session_id""",
                (session_id,),
            ).fetchall(),
            f"find_sessions_sharing_plan({session_id})")
        return [dict(row) for row in rows]

    def get_pending_remote_reserved_bytes(self, session_id):
        value = self._run_read(
            lambda conn: conn.execute(
                """SELECT COALESCE(SUM(sf.file_size_bytes), 0) AS n
                   FROM remote_sessions s
                   JOIN remote_chunks c ON c.session_id=s.session_id
                   JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
                    AND pf.chunk_index=c.chunk_index
                   JOIN remote_snapshot_files sf
                    ON sf.snapshot_file_id=pf.snapshot_file_id
                   WHERE s.session_id=%s AND c.status!='done'""",
                (session_id,),
            ).fetchone()["n"],
            f"get_pending_remote_reserved_bytes({session_id})")
        return int(value)

    def mark_remote_scan_complete(self, session_id):
        self.update_remote_session(
            session_id,
            scan_complete=True,
            scan_error=None,
        )

    def mark_remote_scan_error(self, session_id, message):
        self.update_remote_session(
            session_id,
            scan_complete=False,
            scan_error=(message or "")[:1000],
        )

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
            raise OperationalError(
                "[DB] Refusing cleanup while a remote session is active. "
                "Run --reconcile-stale-sessions --dry-run to see whether the "
                "active row is a live archiver or a stale crashed session.")

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
