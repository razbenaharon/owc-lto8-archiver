"""Transactional database half of an explicitly authorized tape reset."""
import json
from .pg_core import _now_utc, _row


_RESET_TABLES = (
    ("directory_tree_index", "dir_id"),
    ("directory_archive_bundles", "bundle_id"),
    ("directory_archive_stats", "stat_id"),
    ("files_index", "file_id"),
    ("archive_bundles", "bundle_id"),
    ("archive_runs", "run_id"),
    ("catalog_directories", "directory_id"),
    ("local_manifest_folder_aggregates", "aggregate_id"),
    ("local_manifest_export_rows", "source_file_id"),
    ("local_chunks_manifest", "manifest_id"),
    ("small_file_cold_migration_sources", None),
)


class PgTapeResetMixin:
    """Durable intent and explicit, non-cascading cleanup operations."""

    def get_tape_reset_operation(self, operation_id):
        return self._run_read(
            lambda conn: _row(conn.execute(
                "SELECT * FROM tape_reset_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()),
            f"get tape reset operation {operation_id}")

    def create_tape_reset_intent(self, *, operation_id, tape_label,
                                 expected_drive, expected_drive_serial,
                                 expected_ltfs_gen, expected_vcr,
                                 backup_path, backup_sha256,
                                 restore_list_path, restore_list_sha256,
                                 shadow_database, impact_report_path,
                                 impact_report_sha256, impact):
        def operation(conn):
            existing = conn.execute(
                "SELECT * FROM tape_reset_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if existing:
                return _row(existing)
            tape = conn.execute(
                "SELECT * FROM tapes WHERE volume_label=%s FOR UPDATE",
                (tape_label,),
            ).fetchone()
            if not tape:
                raise RuntimeError(f"[TAPE RESET] Tape not found: {tape_label}")
            old_generation = int(tape["current_generation"])
            conn.execute(
                """INSERT INTO tape_generations
                   (tape_id, volume_label, generation, state, formatted_at)
                   VALUES (%s,%s,%s,'active',%s)
                   ON CONFLICT (tape_id,generation) DO NOTHING""",
                (tape["tape_id"], tape_label, old_generation,
                 tape["date_formatted"]),
            )
            conn.execute(
                """INSERT INTO tape_reset_operations
                   (operation_id,target_tape_id,target_label,old_generation,
                    new_generation,state,expected_drive,
                    expected_drive_serial,expected_ltfs_gen,expected_vcr,
                    backup_path,backup_sha256,restore_list_path,
                    restore_list_sha256,shadow_database,impact_report_path,
                    impact_report_sha256,impact)
                   VALUES (%s,%s,%s,%s,%s,'intent_recorded',%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                (operation_id, tape["tape_id"], tape_label, old_generation,
                 old_generation + 1, expected_drive, expected_drive_serial,
                 expected_ltfs_gen, expected_vcr, backup_path, backup_sha256,
                 restore_list_path, restore_list_sha256, shadow_database,
                 impact_report_path, impact_report_sha256,
                 json.dumps(impact, default=str, sort_keys=True)),
            )
            return _row(conn.execute(
                "SELECT * FROM tape_reset_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone())

        return self._transaction(operation, f"record tape reset {operation_id}")

    def mark_tape_reset_hardware_started(self, operation_id):
        def operation(conn):
            row = conn.execute(
                "SELECT state FROM tape_reset_operations WHERE operation_id=%s FOR UPDATE",
                (operation_id,),
            ).fetchone()
            if not row:
                raise RuntimeError(f"[TAPE RESET] Unknown operation {operation_id}")
            if row["state"] in ("hardware_succeeded", "cleanup_succeeded", "verified"):
                return row["state"]
            if row["state"] not in ("intent_recorded", "hardware_started"):
                raise RuntimeError(
                    f"[TAPE RESET] Cannot start hardware from state {row['state']}")
            conn.execute(
                """UPDATE tape_reset_operations
                   SET state='hardware_started',
                       hardware_started_at=COALESCE(hardware_started_at,%s)
                   WHERE operation_id=%s""",
                (_now_utc(), operation_id),
            )
            return "hardware_started"
        return self._transaction(operation, f"start tape reset {operation_id}")

    def mark_tape_reset_hardware_succeeded(self, operation_id, evidence):
        def operation(conn):
            row = conn.execute(
                "SELECT state FROM tape_reset_operations WHERE operation_id=%s FOR UPDATE",
                (operation_id,),
            ).fetchone()
            if not row:
                raise RuntimeError(f"[TAPE RESET] Unknown operation {operation_id}")
            if row["state"] in ("cleanup_succeeded", "verified"):
                return row["state"]
            if row["state"] not in ("hardware_started", "hardware_succeeded"):
                raise RuntimeError(
                    f"[TAPE RESET] Cannot record hardware success from {row['state']}")
            conn.execute(
                """UPDATE tape_reset_operations
                   SET state='hardware_succeeded',hardware_evidence=%s::jsonb,
                       hardware_succeeded_at=COALESCE(hardware_succeeded_at,%s),
                       error_message=NULL
                   WHERE operation_id=%s""",
                (json.dumps(evidence, default=str, sort_keys=True),
                 _now_utc(), operation_id),
            )
            return "hardware_succeeded"
        return self._transaction(operation, f"record hardware success {operation_id}")

    @staticmethod
    def _count_label_rows(conn, tape_label):
        counts = {}
        for table, _ in _RESET_TABLES:
            exists = conn.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema='public' AND table_name=%s
                     AND column_name='tape_label'""",
                (table,),
            ).fetchone()
            if exists:
                counts[table] = int(conn.execute(
                    f"SELECT count(*) AS n FROM {table} WHERE tape_label=%s",
                    (tape_label,),
                ).fetchone()["n"])
        return counts

    def apply_tape_reset_cleanup(self, operation_id, transitions):
        """Delete only old-generation storage rows and reconcile chunks.

        ``transitions`` is the immutable impact report's exact list of
        ``{session_id, chunk_index, before, after}`` records.  Every status is
        compare-and-set checked; scope drift aborts the whole transaction.
        """
        transitions = list(transitions)

        def operation(conn):
            op = conn.execute(
                "SELECT * FROM tape_reset_operations WHERE operation_id=%s FOR UPDATE",
                (operation_id,),
            ).fetchone()
            if not op:
                raise RuntimeError(f"[TAPE RESET] Unknown operation {operation_id}")
            if op["state"] in ("cleanup_succeeded", "verified"):
                return dict(op["cleanup_evidence"] or {})
            if op["state"] != "hardware_succeeded":
                raise RuntimeError(
                    f"[TAPE RESET] Cleanup requires hardware_succeeded, got {op['state']}")
            label = op["target_label"]
            tape = conn.execute(
                "SELECT * FROM tapes WHERE tape_id=%s FOR UPDATE",
                (op["target_tape_id"],),
            ).fetchone()
            if not tape or tape["volume_label"] != label:
                raise RuntimeError("[TAPE RESET] Tape identity changed before cleanup")
            if int(tape["current_generation"]) != int(op["old_generation"]):
                raise RuntimeError("[TAPE RESET] Tape generation changed before cleanup")

            before_counts = self._count_label_rows(conn, label)

            # Compare every affected chunk before deleting a single catalog row.
            for item in transitions:
                row = conn.execute(
                    """SELECT status FROM remote_chunks
                       WHERE session_id=%s AND chunk_index=%s FOR UPDATE""",
                    (item["session_id"], item["chunk_index"]),
                ).fetchone()
                if not row:
                    raise RuntimeError(
                        f"[TAPE RESET] Missing chunk {item['session_id']}/{item['chunk_index']}")
                if row["status"] != item["before"]:
                    raise RuntimeError(
                        "[TAPE RESET] Chunk scope drift: "
                        f"{item['session_id']}/{item['chunk_index']} is "
                        f"{row['status']}, expected {item['before']}")

            deleted = {}
            # Children and storage locators first.  The tape row is never
            # deleted, so no tape FK cascade participates in this operation.
            if self._table_exists_conn(conn, "local_manifest_catalog_aggregates"):
                deleted["local_manifest_catalog_aggregates"] = conn.execute(
                    """DELETE FROM local_manifest_catalog_aggregates
                       WHERE directory_id IN (
                         SELECT directory_id FROM catalog_directories
                         WHERE tape_label=%s)""", (label,)).rowcount
            for table in ("directory_tree_index", "files_index",
                          "directory_archive_stats",
                          "directory_archive_bundles",
                          "local_manifest_folder_aggregates",
                          "archive_bundles"):
                if self._table_exists_conn(conn, table):
                    deleted[table] = conn.execute(
                        f"DELETE FROM {table} WHERE tape_label=%s", (label,)
                    ).rowcount
            if self._table_exists_conn(conn, "local_chunks_manifest"):
                deleted["local_chunks_manifest_reset"] = conn.execute(
                    """UPDATE local_chunks_manifest
                       SET status='pending',tape_label=NULL,completed_at=NULL,
                           updated_at=%s
                       WHERE tape_label=%s""", (_now_utc(), label)).rowcount
            if self._table_exists_conn(conn, "catalog_directories"):
                deleted["catalog_directories"] = conn.execute(
                    "DELETE FROM catalog_directories WHERE tape_label=%s",
                    (label,),
                ).rowcount
            deleted["archive_runs"] = conn.execute(
                "DELETE FROM archive_runs WHERE tape_label=%s", (label,)
            ).rowcount

            now = _now_utc()
            affected_sessions = set()
            for item in transitions:
                cur = conn.execute(
                    """UPDATE remote_chunks
                       SET status=%s,error_msg=NULL,updated_at=%s
                       WHERE session_id=%s AND chunk_index=%s AND status=%s""",
                    (item["after"], now, item["session_id"],
                     item["chunk_index"], item["before"]),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("[TAPE RESET] Chunk compare-and-set failed")
                affected_sessions.add(int(item["session_id"]))
            for session_id in sorted(affected_sessions):
                conn.execute(
                    """UPDATE remote_sessions
                       SET status='active',completed_at=NULL
                       WHERE session_id=%s""", (session_id,))

            conn.execute(
                """UPDATE tape_generations
                   SET state='retired',retired_at=%s,
                       retired_reason=%s,reset_operation_id=%s
                   WHERE tape_id=%s AND generation=%s AND state='active'""",
                (now, "physical contents intentionally destroyed by tape reset",
                 operation_id, op["target_tape_id"], op["old_generation"]),
            )
            conn.execute(
                """INSERT INTO tape_generations
                   (tape_id,volume_label,generation,state,formatted_at,
                    reset_operation_id)
                   VALUES (%s,%s,%s,'active',%s,%s)""",
                (op["target_tape_id"], label, op["new_generation"], now,
                 operation_id),
            )
            conn.execute(
                """UPDATE tapes
                   SET current_generation=%s,date_formatted=%s,used_space=0,
                       status='active',status_reason=NULL,status_changed_at=%s
                   WHERE tape_id=%s""",
                (op["new_generation"], now, now, op["target_tape_id"]),
            )

            after_counts = self._count_label_rows(conn, label)
            evidence = {
                "before_counts": before_counts,
                "after_counts": after_counts,
                "deleted": deleted,
                "chunk_transitions": transitions,
                "affected_sessions": sorted(affected_sessions),
                "old_generation": int(op["old_generation"]),
                "new_generation": int(op["new_generation"]),
            }
            conn.execute(
                """UPDATE tape_reset_operations
                   SET state='cleanup_succeeded',cleanup_evidence=%s::jsonb,
                       cleanup_succeeded_at=%s,error_message=NULL
                   WHERE operation_id=%s""",
                (json.dumps(evidence, default=str, sort_keys=True),
                 now, operation_id),
            )
            return evidence

        return self._transaction(operation, f"cleanup tape reset {operation_id}")

    def mark_tape_reset_verified(self, operation_id, verification):
        def operation(conn):
            row = conn.execute(
                "SELECT state,cleanup_evidence FROM tape_reset_operations WHERE operation_id=%s FOR UPDATE",
                (operation_id,),
            ).fetchone()
            if not row:
                raise RuntimeError(f"[TAPE RESET] Unknown operation {operation_id}")
            if row["state"] == "verified":
                return
            if row["state"] != "cleanup_succeeded":
                raise RuntimeError(
                    f"[TAPE RESET] Verification requires cleanup_succeeded, got {row['state']}")
            evidence = dict(row["cleanup_evidence"] or {})
            evidence["verification"] = verification
            conn.execute(
                """UPDATE tape_reset_operations
                   SET state='verified',cleanup_evidence=%s::jsonb,verified_at=%s
                   WHERE operation_id=%s""",
                (json.dumps(evidence, default=str, sort_keys=True),
                 _now_utc(), operation_id),
            )
        return self._transaction(operation, f"verify tape reset {operation_id}")
