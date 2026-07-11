"""Tape-registry method group: tapes table and label-wide maintenance."""
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
        # ON CONFLICT DO NOTHING (not try/except UniqueViolation) keeps this
        # idempotent under the ambiguous-commit retry loop in _transaction: a
        # retry whose first COMMIT actually landed reports rowcount 0 instead
        # of raising and being misreported as "already in the database".
        inserted = self._transaction(
            lambda conn: conn.execute(
                """INSERT INTO tapes
                   (volume_label, date_formatted, total_capacity)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (volume_label) DO NOTHING""",
                (volume_label, _now_utc(), capacity_gb),
            ).rowcount,
            f"register tape {volume_label}",
        )
        if inserted:
            print(f"[DB] Tape '{volume_label}' registered successfully.")
            return True
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
        if not self._table_exists_conn(conn, "directory_archive_bundles"):
            row = conn.execute(
                """SELECT COALESCE(SUM(file_size_bytes), 0) AS used
                   FROM files_index
                   WHERE tape_label=%s""",
                (volume_label,),
            ).fetchone()
            return row["used"]
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
        return row["used"]

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
