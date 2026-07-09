"""Local/remote session, snapshot, plan and chunk-state method group."""
import hashlib
import os

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

    def append_remote_streaming_chunk(self, session_id, chunk_index, rows):
        """Append one discovered chunk to a streaming remote session.

        Duplicate canonical remote paths already present in the session
        snapshot are ignored so an incomplete streaming scan can be resumed by
        rescanning from the beginning.
        """
        rows = list(rows)
        by_path = self._validate_remote_manifest_rows(rows)
        now = _now_utc()

        def operation(conn):
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

    def get_next_remote_chunk_index(self, session_id):
        with self._pool.connection() as conn:
            value = conn.execute(
                """SELECT COALESCE(MAX(chunk_index) + 1, 0) AS next_index
                   FROM remote_chunks WHERE session_id=%s""",
                (session_id,),
            ).fetchone()["next_index"]
        return int(value)

    def get_remote_existing_snapshot_paths(self, session_id, paths):
        paths = [_canonical_remote_path(path) for path in paths]
        if not paths:
            return set()
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT sf.remote_path
                   FROM remote_sessions s
                   JOIN remote_plans p ON p.plan_id=s.plan_id
                   JOIN remote_snapshot_files sf
                    ON sf.snapshot_id=p.snapshot_id
                   WHERE s.session_id=%s AND sf.remote_path = ANY(%s)""",
                (session_id, paths),
            ).fetchall()
        return {row["remote_path"] for row in rows}

    def get_pending_remote_reserved_bytes(self, session_id):
        with self._pool.connection() as conn:
            value = conn.execute(
                """SELECT COALESCE(SUM(sf.file_size_bytes), 0) AS n
                   FROM remote_sessions s
                   JOIN remote_chunks c ON c.session_id=s.session_id
                   JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
                    AND pf.chunk_index=c.chunk_index
                   JOIN remote_snapshot_files sf
                    ON sf.snapshot_file_id=pf.snapshot_file_id
                   WHERE s.session_id=%s AND c.status!='done'""",
                (session_id,),
            ).fetchone()["n"]
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
