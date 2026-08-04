"""Tape-registry method group: tapes table and label-wide maintenance."""
from .constants import TAPE_STATUS_ACTIVE, TAPE_STATUSES
from .db import _file_record_key
from .pg_core import _now_utc, _row, _rows


class PgTapeMixin:
    """Tape registry: register/rename/delete, capacity and used-space upkeep.

    Mixin over :class:`src.pg_core.PgConnectionCore` (uses ``self._pool`` and
    ``self._transaction``); assembled in :class:`src.pg_db.PgDatabaseManager`.
    """

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

    def register_tape(self, volume_label, capacity_gb=None):
        """Register a cartridge AND its matching active generation, atomically.

        Plan 1 Task 1.4. ``tapes.current_generation`` defaults to 1, but before
        this the matching ``tape_generations`` row was only created by migration
        013's backfill — so a tape registered *after* the migration had a
        generation number with no generation row behind it. Every guard that
        asks "is this session's generation still the active one?" then had
        nothing to compare against, and the safest available answer for a
        missing row is to refuse a resume. Creating both in ONE transaction
        removes that state entirely.

        Idempotent by ``ON CONFLICT DO NOTHING`` (not try/except) so it stays
        correct under the ambiguous-commit retry loop in ``_transaction``: a
        retry whose first COMMIT actually landed reports rowcount 0 rather than
        raising and being misreported as "already in the database".
        """
        now = _now_utc()

        def operation(conn):
            rowcount = conn.execute(
                """INSERT INTO tapes
                   (volume_label, date_formatted, total_capacity)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (volume_label) DO NOTHING""",
                (volume_label, now, capacity_gb),
            ).rowcount
            # The generation row is created whenever the schema supports it,
            # including for a tape row that already existed without one.
            if self._table_exists_conn(conn, 'tape_generations'):
                conn.execute(
                    """INSERT INTO tape_generations
                           (tape_id, volume_label, generation, state,
                            formatted_at)
                       SELECT tape_id, volume_label, current_generation,
                              'active', %s
                       FROM tapes
                       WHERE volume_label=%s
                       ON CONFLICT (tape_id, generation) DO NOTHING""",
                    (now, volume_label),
                )
            return rowcount

        inserted = self._transaction(
            operation, f"register tape {volume_label}")
        if inserted:
            print(f"[DB] Tape '{volume_label}' registered successfully.")
            return True
        print(f"[DB] Tape '{volume_label}' is already in the database.")
        return False

    def get_active_tape_generation(self, volume_label):
        """The catalog's ACTIVE generation number for a cartridge, or None.

        Read-only, PostgreSQL-only (Plan 1 Task 1.4). ``None`` means "no active
        generation row" — which callers must treat as a refusal, not as
        agreement: a generation number in ``tapes.current_generation`` with no
        active row behind it is exactly the state that makes "has this cartridge
        been reformatted since?" unanswerable.

        Returns ``None`` on a database without ``tape_generations`` (pre-013);
        the caller's column-presence check has already established there is
        nothing to compare in that case.
        """
        def operation(conn):
            if not self._table_exists_conn(conn, 'tape_generations'):
                return None
            row = conn.execute(
                """SELECT g.generation
                   FROM tape_generations g
                   JOIN tapes t ON t.tape_id = g.tape_id
                   WHERE t.volume_label=%s AND g.state='active'""",
                (volume_label,),
            ).fetchone()
            return row["generation"] if row else None

        return self._run_read(
            operation, f"get_active_tape_generation({volume_label})")

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
        raise RuntimeError(
            "[DB] replace_formatted_tape is disabled: deleting/recreating a "
            "tape row can erase sessions and chunks through foreign-key "
            "cascades. Use the explicit `python run.py tape-reset --tape "
            "<LABEL> --delete-catalog --yes ...` workflow instead.")

    def _delete_tape_records(self, conn, volume_label):
        stats = {}
        stats["file_records"] = conn.execute(
            "DELETE FROM files_index WHERE tape_label=%s", (volume_label,)
        ).rowcount
        stats["bundles"] = conn.execute(
            "DELETE FROM archive_bundles WHERE tape_label=%s", (volume_label,)
        ).rowcount
        if self._table_exists_conn(conn, "directory_archive_bundles"):
            stats["directory_bundles"] = conn.execute(
                "DELETE FROM directory_archive_bundles WHERE tape_label=%s",
                (volume_label,),
            ).rowcount
        else:
            stats["directory_bundles"] = 0
        if self._table_exists_conn(conn, "directory_archive_stats"):
            stats["directory_stats"] = conn.execute(
                "DELETE FROM directory_archive_stats WHERE tape_label=%s",
                (volume_label,),
            ).rowcount
        else:
            stats["directory_stats"] = 0
        if self._table_exists_conn(conn, "directory_tree_index"):
            stats["directory_tree"] = conn.execute(
                "DELETE FROM directory_tree_index WHERE tape_label=%s",
                (volume_label,),
            ).rowcount
        else:
            stats["directory_tree"] = 0
        stats["runs"] = conn.execute(
            "DELETE FROM archive_runs WHERE tape_label=%s", (volume_label,)
        ).rowcount
        stats["directories"] = conn.execute(
            "DELETE FROM catalog_directories WHERE tape_label=%s", (volume_label,)
        ).rowcount
        return stats

    def _calculate_tape_used_space_conn(self, conn, volume_label):
        manifest_bytes = 0
        if self._table_exists_conn(conn, "local_manifest_folder_aggregates"):
            row = conn.execute(
                """SELECT COALESCE(SUM(
                            CASE WHEN %s THEN a.direct_uncovered_bytes
                                 ELSE a.direct_bytes END),0) AS used
                   FROM local_manifest_folder_aggregates a
                   JOIN local_manifest_exports e ON e.export_id=a.export_id
                   WHERE a.tape_label=%s AND e.status='pruned'""",
                (self._table_exists_conn(conn, "directory_archive_bundles"),
                 volume_label),
            ).fetchone()
            manifest_bytes = int(row["used"] or 0)
        if not self._table_exists_conn(conn, "directory_archive_bundles"):
            row = conn.execute(
                """SELECT COALESCE(SUM(file_size_bytes), 0) AS used
                   FROM files_index
                   WHERE tape_label=%s""",
                (volume_label,),
            ).fetchone()
            return int(row["used"] or 0) + manifest_bytes
        row = conn.execute(
            """WITH bundle_paths AS (
                   SELECT stored_bundle_path
                   FROM directory_archive_bundles
                   WHERE tape_label=%s
               ),
               legacy_file_bytes AS (
                   SELECT COALESCE(SUM(f.file_size_bytes), 0) AS n
                   FROM files_index f
                   LEFT JOIN archive_bundles b
                     ON b.bundle_id=f.bundle_id
                   WHERE f.tape_label=%s
                     AND NOT EXISTS (
                         SELECT 1 FROM bundle_paths bp
                         WHERE bp.stored_bundle_path = b.tape_path
                     )
               ),
               directory_bundle_bytes AS (
                   SELECT COALESCE(SUM(byte_count), 0) AS n
                   FROM directory_archive_bundles
                   WHERE tape_label=%s
               )
               SELECT
                   (SELECT n FROM legacy_file_bytes)
                   + (SELECT n FROM directory_bundle_bytes) AS used""",
            (volume_label, volume_label, volume_label),
        ).fetchone()
        return int(row["used"] or 0) + manifest_bytes

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

    def set_tape_status(self, volume_label, status, reason=None):
        """Mark a tape ``active`` or ``full`` (retired from writing).

        ``full`` is the only durable way to retire a cartridge:
        ``recalculate_tape_used_space`` rewrites ``used_space`` from the
        catalog on every mount, so hand-edited byte counters do not survive.
        """
        status = str(status or "").strip().lower()
        if status not in TAPE_STATUSES:
            raise ValueError(
                f"[DB] Invalid tape status '{status}'. "
                f"Expected one of: {', '.join(TAPE_STATUSES)}.")

        def operation(conn):
            if not self._column_exists_conn(conn, "tapes", "status"):
                raise RuntimeError(
                    "[DB] tapes.status is missing on this database. Apply "
                    "scripts/sql/011_postgres_tape_status.sql first.")
            cur = conn.execute(
                """UPDATE tapes
                   SET status=%s, status_reason=%s, status_changed_at=%s
                   WHERE volume_label=%s""",
                (status, (reason or "").strip() or None, _now_utc(),
                 volume_label),
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")

        self._transaction(operation, f"set tape status {volume_label}")
        note = f" ({reason.strip()})" if (reason or "").strip() else ""
        print(f"[DB] Tape '{volume_label}' marked {status.upper()}{note}.")

    def get_tape_status(self, volume_label):
        """Return ``tapes.status`` (``'active'`` when the column predates 011)."""
        tape = self.get_tape(volume_label) or {}
        return tape.get("status") or TAPE_STATUS_ACTIVE

    def recalculate_tape_used_space(self, volume_label):
        def operation(conn):
            new_used = self._calculate_tape_used_space_conn(conn, volume_label)
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
                # Ambiguous-commit retry: if the first COMMIT landed, the old
                # label is gone and the new one exists — converge on success
                # instead of raising a false "Tape not found" after the rename
                # actually happened.
                if conn.execute(
                    "SELECT 1 FROM tapes WHERE volume_label=%s",
                    (new_label,),
                ).fetchone():
                    return
                raise RuntimeError(f"[DB] Tape not found: {old_label}")
            # status/status_reason must be carried across: a relabel of a tape
            # retired as 'full' would otherwise land on the column default
            # ('active') and silently make a dead cartridge writable again.
            if self._column_exists_conn(conn, "tapes", "status"):
                conn.execute(
                    """INSERT INTO tapes
                       (volume_label, date_formatted, total_capacity,
                        used_space, status, status_reason, status_changed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (new_label, old["date_formatted"],
                     old["total_capacity"], old["used_space"],
                     old["status"], old["status_reason"],
                     old["status_changed_at"]),
                )
            else:
                conn.execute(
                    """INSERT INTO tapes
                       (volume_label, date_formatted, total_capacity, used_space)
                       VALUES (%s, %s, %s, %s)""",
                    (new_label, old["date_formatted"],
                     old["total_capacity"], old["used_space"]),
                )
            # Migration 013 added two tables keyed by tape_id, both with
            # ON DELETE RESTRICT. A rename creates a NEW tapes row and deletes
            # the old one, so anything still pointing at the old tape_id blocks
            # that delete outright. Repoint them here, for the same reason
            # every tape_label table above is repointed: a relabel is the SAME
            # cartridge, so its generation history and reset history must
            # follow it. (Surfaced by the isolated-PostgreSQL suite once
            # register_tape started creating a generation row for every tape.)
            new_tape_id = conn.execute(
                "SELECT tape_id FROM tapes WHERE volume_label=%s",
                (new_label,),
            ).fetchone()["tape_id"]
            if self._table_exists_conn(conn, "tape_generations"):
                conn.execute(
                    """UPDATE tape_generations
                       SET tape_id=%s, volume_label=%s WHERE tape_id=%s""",
                    (new_tape_id, new_label, old["tape_id"]),
                )
            if self._table_exists_conn(conn, "tape_reset_operations"):
                conn.execute(
                    """UPDATE tape_reset_operations
                       SET target_tape_id=%s WHERE target_tape_id=%s""",
                    (new_tape_id, old["tape_id"]),
                )
            conn.execute(
                "UPDATE catalog_directories SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            conn.execute(
                "UPDATE archive_bundles SET tape_label=%s WHERE tape_label=%s",
                (new_label, old_label),
            )
            if self._table_exists_conn(conn, "directory_archive_bundles"):
                conn.execute(
                    """UPDATE directory_archive_bundles
                       SET tape_label=%s WHERE tape_label=%s""",
                    (new_label, old_label),
                )
            if self._table_exists_conn(conn, "directory_archive_stats"):
                conn.execute(
                    """UPDATE directory_archive_stats
                       SET tape_label=%s WHERE tape_label=%s""",
                    (new_label, old_label),
                )
            if self._table_exists_conn(conn, "directory_tree_index"):
                conn.execute(
                    """UPDATE directory_tree_index
                       SET tape_label=%s WHERE tape_label=%s""",
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
                          local_session_id, local_chunk_index,
                          remote_session_id, remote_chunk_index
                   FROM files_index WHERE tape_label=%s""",
                (new_label,),
            ).fetchall()
            # executemany pipelines the statements — one round-trip flight
            # instead of one per file record (hours at catalog scale).
            # Remote provenance MUST be threaded through: rebuilding with the
            # legacy 5-field key would collide provenance-distinct rows and
            # silently re-key remote records so later resume/upsert lookups
            # miss them and insert duplicates.
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE files_index SET record_key=%s WHERE file_id=%s",
                    ((_file_record_key(
                        row["original_path"], new_label,
                        row["local_session_id"], row["local_chunk_index"],
                        row["source_host"],
                        remote_session_id=row["remote_session_id"],
                        remote_chunk_index=row["remote_chunk_index"]),
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
