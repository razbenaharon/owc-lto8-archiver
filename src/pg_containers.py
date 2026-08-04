"""Durable packaging-format, container and artifact repository (Plan 2).

This mixin is intentionally below :mod:`src.pg_db` and beside the other
PostgreSQL concern modules.  It owns the one authoritative new-chunk format
assignment seam and the identities handed from staging to the writer.  It never
opens a tape path and never infers a format from a filename extension.
"""
import json
import os
import posixpath
from dataclasses import asdict, is_dataclass

from .pipeline_types import (ArtifactReadiness, ContainerFormat,
                             ContainerValidationState, SourceDisposition)
from .pg_core import _row, _rows
from .stored_tar_planning import (build_stored_tar_chunk_plan,
                                  StoredTarChunkPlan,
                                  StoredTarContainerPlan,
                                  StoredTarPlanMember)
from .tar_container import STORED_TAR_FORMAT_VERSION


CONTAINER_FORMAT_SCHEMA_VERSION = 1
REQUIRED_STORED_TAR_READER_CONTRACT_VERSION = 1
STORED_TAR_DIALECT = "gnu-pax-sparse-v1"


def stored_tar_reader_contract_version():
    """Return the locally implemented reader contract, or ``None`` in Phase 0.

    Phase 1 supplies ``src.tar_container.STORED_TAR_READER_CONTRACT_VERSION``.
    Importing lazily keeps migration/report tooling available before that reader
    exists while making producer activation fail closed.
    """
    try:
        from . import tar_container
    except ImportError:
        return None
    return getattr(tar_container, "STORED_TAR_READER_CONTRACT_VERSION", None)


class PgContainerMixin:
    """Methods for migration-015 schema truth and staged identities."""

    CONTAINER_FORMAT_TABLES = (
        "container_format_schema_metadata",
        "remote_packaging_boundaries",
        "remote_packaging_boundary_chunks",
        "archive_containers",
        "archive_artifacts",
    )
    CONTAINER_FORMAT_COLUMNS = {
        "container_format_schema_metadata": (
            "singleton", "schema_version",
            "required_reader_contract_version", "stored_tar_dialect",
            "migration_checksum", "schema_fingerprint", "applied_at",
        ),
        "remote_sessions": (
            "default_packaging_format",
        ),
        "remote_chunks": (
            "packaging_format", "packaging_assigned_at",
            "writer_started_at", "writer_completed_at",
            "catalog_committed_at",
        ),
        "remote_packaging_boundary_chunks": (
            "session_id", "chunk_index", "classification",
            "assigned_format", "prefix_evidence_basis",
            "plan_member_count", "plan_logical_bytes", "first_ordinal",
            "last_ordinal", "evidence",
        ),
        "archive_bundles": ("container_id", "container_format"),
        "archive_runs": ("remote_chunk_index", "tape_generation_id"),
        "archive_containers": (
            "container_id", "session_id", "chunk_index",
            "container_ordinal", "container_format", "format_version",
            "tar_dialect", "storage_class", "container_name",
            "temporary_data_locator", "permanent_local_metadata_locator",
            "tape_label", "tape_path", "tape_generation_id",
            "expected_member_count", "expected_logical_bytes",
            "observed_member_count", "observed_logical_bytes",
            "actual_artifact_bytes", "validation_state", "writer_state",
            "catalog_state", "owner_token", "lease_expires_at",
            "writer_started_at", "writer_completed_at",
            "catalog_committed_at",
        ),
        "archive_artifacts": (
            "artifact_id", "session_id", "chunk_index", "container_id",
            "artifact_kind", "artifact_version", "local_locator",
            "tape_locator", "artifact_size_bytes", "readiness_state",
            "publication_started_at", "published_at", "tape_published_at",
        ),
    }
    CONTAINER_FORMAT_INDEXES = (
        "uq_archive_artifacts_container_kind_version",
        "uq_archive_artifacts_chunk_kind_version",
        "uq_archive_runs_remote_chunk_generation",
    )
    CONTAINER_FORMAT_TRIGGERS = (
        "trg_remote_packaging_boundaries_immutable",
        "trg_remote_packaging_boundary_chunks_immutable",
        "trg_remote_chunks_packaging_write_once",
        "trg_archive_containers_format_write_once",
        "trg_archive_bundles_format_write_once",
        "trg_remote_sessions_packaging_default",
        "trg_remote_chunks_initial_format",
        "trg_archive_containers_tape_generation",
        "trg_archive_artifacts_container_format",
        "trg_archive_artifacts_identity_write_once",
        "trg_container_format_schema_metadata_immutable",
    )
    CONTAINER_FORMAT_CONSTRAINTS = (
        "remote_chunks_identity_format_uq",
        "remote_chunks_packaging_format_ck",
        "remote_sessions_default_packaging_format_ck",
        "archive_containers_chunk_format_fk",
        "archive_containers_ordinal_uq",
        "archive_artifacts_chunk_fk",
        "archive_artifacts_container_fk",
        "archive_artifacts_ready_ck",
        "archive_bundles_container_fk",
        "archive_bundles_container_format_ck",
        "archive_runs_tape_generation_fk",
    )

    # ------------------------------------------------------------------
    # Read-only schema report / validation
    # ------------------------------------------------------------------

    def _container_format_schema_report_conn(self, conn):
        report = {
            "database": conn.execute(
                "SELECT current_database() AS db").fetchone()["db"],
            "expected_schema_version": CONTAINER_FORMAT_SCHEMA_VERSION,
            "expected_reader_contract_version":
                REQUIRED_STORED_TAR_READER_CONTRACT_VERSION,
            "expected_dialect": STORED_TAR_DIALECT,
            "expected_migration_checksum":
                self.container_format_migration_checksum(),
            "metadata": None,
            "installation_state": "absent",
            "optional_007_state": "absent",
            "format_counts": {},
            "prefix_evidence_counts": {
                "corroborated": 0,
                "status_only": 0,
            },
            "null_chunk_formats": None,
            "issues": [],
        }
        issues = report["issues"]

        for table in self.CONTAINER_FORMAT_TABLES:
            if not self._table_exists_conn(conn, table):
                issues.append(f"missing table: {table}")

        for table, columns in self.CONTAINER_FORMAT_COLUMNS.items():
            if not self._table_exists_conn(conn, table):
                if table not in self.CONTAINER_FORMAT_TABLES:
                    issues.append(f"missing table: {table}")
                continue
            existing = {
                row["column_name"]: row
                for row in conn.execute(
                    """SELECT column_name, is_nullable, column_default
                       FROM information_schema.columns
                       WHERE table_schema='public' AND table_name=%s""",
                    (table,),
                ).fetchall()
            }
            for column in columns:
                if column not in existing:
                    issues.append(f"missing column: {table}.{column}")

        marker_count = sum(
            self._table_exists_conn(conn, table)
            for table in self.CONTAINER_FORMAT_TABLES)
        marker_count += sum(
            self._column_exists_conn(conn, table, column)
            for table, columns in self.CONTAINER_FORMAT_COLUMNS.items()
            for column in columns)
        marker_count += int(conn.execute(
            """SELECT count(*) AS n FROM pg_proc p
               JOIN pg_namespace n ON n.oid=p.pronamespace
               WHERE n.nspname='public' AND p.proname IN
                 ('lto_reject_packaging_boundary_mutation',
                  'lto_reject_packaging_format_change',
                  'lto_reject_container_format_change',
                  'lto_reject_container_identity_change',
                  'lto_reject_artifact_identity_change',
                  'lto_guard_session_packaging_default',
                  'lto_assign_new_chunk_format',
                  'lto_guard_new_chunk_format',
                  'lto_guard_container_tape_generation',
                  'lto_guard_artifact_container_format',
                  'lto_guard_container_format_metadata',
                  'lto_container_format_schema_fingerprint')"""
        ).fetchone()["n"])
        marker_count += int(conn.execute(
            """SELECT count(*) AS n FROM pg_class c
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='public' AND c.relname IN
                 ('uq_archive_artifacts_container_kind_version',
                  'uq_archive_artifacts_chunk_kind_version',
                  'uq_archive_runs_remote_chunk_generation')"""
        ).fetchone()["n"])

        if self._table_exists_conn(conn, "remote_sessions") and \
                self._column_exists_conn(
                    conn, "remote_sessions", "default_packaging_format"):
            column = conn.execute(
                """SELECT is_nullable, column_default
                   FROM information_schema.columns
                   WHERE table_schema='public' AND table_name='remote_sessions'
                     AND column_name='default_packaging_format'""").fetchone()
            if column["is_nullable"] != "NO":
                issues.append(
                    "remote_sessions.default_packaging_format is nullable")
            if "zip" not in str(column["column_default"] or ""):
                issues.append(
                    "remote_sessions.default_packaging_format default drift")

        if self._table_exists_conn(conn, "remote_chunks") and \
                self._column_exists_conn(conn, "remote_chunks",
                                         "packaging_format"):
            definitions = {
                row["column_name"]: row
                for row in conn.execute(
                    """SELECT column_name, is_nullable, column_default
                       FROM information_schema.columns
                       WHERE table_schema='public' AND table_name='remote_chunks'
                         AND column_name IN
                             ('packaging_format','packaging_assigned_at')""").fetchall()
            }
            for column in ("packaging_format", "packaging_assigned_at"):
                if definitions.get(column, {}).get("is_nullable") != "NO":
                    issues.append(f"remote_chunks.{column} is nullable")
            if definitions.get("packaging_format", {}).get("column_default"):
                issues.append(
                    "remote_chunks.packaging_format unexpectedly has a default")
            row = conn.execute(
                """SELECT count(*) AS nulls,
                          count(*) FILTER (WHERE packaging_format='zip') AS zip,
                          count(*) FILTER (
                              WHERE packaging_format='stored_tar') AS stored_tar
                   FROM remote_chunks""").fetchone()
            report["null_chunk_formats"] = int(row["nulls"] or 0)
            # nulls above is total; keep the explicitly useful measure separate.
            report["chunk_count"] = int(row["nulls"] or 0)
            has_assigned_at = self._column_exists_conn(
                conn, "remote_chunks", "packaging_assigned_at")
            null_where = ("packaging_format IS NULL OR "
                          "packaging_assigned_at IS NULL"
                          if has_assigned_at else
                          "packaging_format IS NULL")
            nulls = conn.execute(
                f"SELECT count(*) AS n FROM remote_chunks WHERE {null_where}"
            ).fetchone()["n"]
            report["null_chunk_formats"] = int(nulls)
            report["format_counts"] = {
                "zip": int(row["zip"] or 0),
                "stored_tar": int(row["stored_tar"] or 0),
            }
            if nulls:
                issues.append(f"{nulls} chunk(s) lack durable format assignment")
            invalid = int(conn.execute(
                """SELECT count(*) AS n FROM remote_chunks
                   WHERE packaging_format NOT IN ('zip','stored_tar')"""
            ).fetchone()["n"])
            if invalid:
                issues.append(f"{invalid} chunk(s) have invalid formats")

        if self._table_exists_conn(conn, "container_format_schema_metadata"):
            metadata = conn.execute(
                """SELECT schema_version, required_reader_contract_version,
                          stored_tar_dialect, migration_checksum,
                          schema_fingerprint, applied_at
                   FROM container_format_schema_metadata WHERE singleton""").fetchone()
            report["metadata"] = dict(metadata) if metadata else None
            if metadata is None:
                issues.append("container format metadata row is absent")
            else:
                expected = {
                    "schema_version": CONTAINER_FORMAT_SCHEMA_VERSION,
                    "required_reader_contract_version":
                        REQUIRED_STORED_TAR_READER_CONTRACT_VERSION,
                    "stored_tar_dialect": STORED_TAR_DIALECT,
                    "migration_checksum":
                        self.container_format_migration_checksum(),
                }
                for key, value in expected.items():
                    if metadata[key] != value:
                        issues.append(
                            f"container format metadata drift: {key} is "
                            f"{metadata[key]!r}, expected {value!r}")
                fingerprint_proc = conn.execute(
                    """SELECT to_regprocedure(
                         'lto_container_format_schema_fingerprint()') AS proc"""
                ).fetchone()["proc"]
                if fingerprint_proc is None:
                    issues.append(
                        "missing function: container schema fingerprint")
                else:
                    actual_fingerprint = conn.execute(
                        "SELECT lto_container_format_schema_fingerprint() AS fp"
                    ).fetchone()["fp"]
                    report["actual_schema_fingerprint"] = actual_fingerprint
                    if metadata["schema_fingerprint"] != actual_fingerprint:
                        issues.append(
                            "container format catalog-definition fingerprint "
                            "drift")

        if (self._table_exists_conn(
                conn, "remote_packaging_boundary_chunks")
                and self._column_exists_conn(
                    conn, "remote_packaging_boundary_chunks",
                    "prefix_evidence_basis")):
            prefix_counts = conn.execute(
                """SELECT count(*) FILTER (
                              WHERE prefix_evidence_basis='corroborated')
                                  AS corroborated,
                          count(*) FILTER (
                              WHERE prefix_evidence_basis='status_only')
                                  AS status_only
                   FROM remote_packaging_boundary_chunks""").fetchone()
            report["prefix_evidence_counts"] = {
                "corroborated": int(prefix_counts["corroborated"] or 0),
                "status_only": int(prefix_counts["status_only"] or 0),
            }

        if report["metadata"] is not None:
            report["installation_state"] = "installed"
        elif marker_count:
            report["installation_state"] = "partial"

        indexes = {
            row["indexname"] for row in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
            ).fetchall()
        }
        for index in self.CONTAINER_FORMAT_INDEXES:
            if index not in indexes:
                issues.append(f"missing index: {index}")

        triggers = {
            row["tgname"] for row in conn.execute(
                """SELECT t.tgname FROM pg_trigger t
                   JOIN pg_class c ON c.oid=t.tgrelid
                   JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname='public' AND NOT t.tgisinternal""").fetchall()
        }
        for trigger in self.CONTAINER_FORMAT_TRIGGERS:
            if trigger not in triggers:
                issues.append(f"missing trigger: {trigger}")

        constraints = {
            row["conname"] for row in conn.execute(
                """SELECT conname FROM pg_constraint c
                   JOIN pg_namespace n ON n.oid=c.connamespace
                   WHERE n.nspname='public'""").fetchall()
        }
        for constraint in self.CONTAINER_FORMAT_CONSTRAINTS:
            if constraint not in constraints:
                issues.append(f"missing constraint: {constraint}")

        function_signature = conn.execute(
            """SELECT to_regprocedure(
                   'lto_assign_new_chunk_format(bigint,integer,boolean,integer,integer)')
                   AS proc""").fetchone()["proc"]
        if function_signature is None:
            issues.append("missing function: lto_assign_new_chunk_format")

        if (self._table_exists_conn(conn, "remote_sessions")
                and self._column_exists_conn(
                    conn, "remote_sessions", "default_packaging_format")):
            invalid_defaults = int(conn.execute(
                """SELECT count(*) AS n FROM remote_sessions
                   WHERE default_packaging_format IS NULL
                      OR default_packaging_format NOT IN ('zip','stored_tar')"""
            ).fetchone()["n"])
            if invalid_defaults:
                issues.append(
                    f"{invalid_defaults} session(s) have invalid format defaults")

        if (self._table_exists_conn(conn, "remote_packaging_boundaries")
                and self._table_exists_conn(
                    conn, "remote_packaging_boundary_chunks")
                and self._column_exists_conn(
                    conn, "remote_chunks", "packaging_format")):
            boundary_issues = int(conn.execute(
                """SELECT count(*) AS n FROM remote_chunks c
                   LEFT JOIN remote_packaging_boundaries b
                     ON b.session_id=c.session_id
                   WHERE (c.packaging_format='stored_tar' AND
                          (b.session_id IS NULL OR
                           c.chunk_index < b.first_stored_tar_chunk_index))
                      OR (b.session_id IS NOT NULL AND
                          c.packaging_format IS DISTINCT FROM
                            CASE WHEN c.chunk_index >=
                                      b.first_stored_tar_chunk_index
                                 THEN 'stored_tar' ELSE 'zip' END)"""
            ).fetchone()["n"])
            audit_issues = int(conn.execute(
                """SELECT count(*) AS n FROM remote_packaging_boundaries b
                   WHERE EXISTS (
                       SELECT 1 FROM remote_chunks c
                       LEFT JOIN remote_packaging_boundary_chunks a
                         ON a.session_id=c.session_id
                        AND a.chunk_index=c.chunk_index
                       WHERE c.session_id=b.session_id
                         AND c.chunk_index<=b.last_existing_chunk_index
                         AND (a.session_id IS NULL
                              OR a.assigned_format<>c.packaging_format
                               OR a.classification<>
                                 CASE WHEN c.chunk_index >=
                                           b.first_stored_tar_chunk_index
                                       THEN 'approved_stored_tar_exception'
                                       ELSE 'immutable_zip' END
                              OR a.prefix_evidence_basis IS DISTINCT FROM
                                 CASE WHEN c.chunk_index >=
                                           b.first_stored_tar_chunk_index
                                      THEN NULL
                                      WHEN (COALESCE((a.evidence->>
                                               'catalog_file_count')::bigint,0)>0
                                         OR COALESCE((a.evidence->>
                                               'archive_run_count')::bigint,0)>0
                                         OR COALESCE((a.evidence->>
                                               'container_count')::bigint,0)>0
                                         OR COALESCE((a.evidence->>
                                               'artifact_count')::bigint,0)>0
                                         OR COALESCE((a.evidence->>
                                               'written_container_count')::bigint,0)>0
                                         OR COALESCE((a.evidence->>
                                               'sealed_batch_written_count')::bigint,0)>0)
                                      THEN 'corroborated'
                                      ELSE 'status_only' END
                              OR a.plan_member_count<>(
                                  SELECT count(*) FROM remote_plan_files pf
                                  JOIN remote_sessions s
                                    ON s.session_id=c.session_id
                                  WHERE pf.plan_id=s.plan_id
                                    AND pf.chunk_index=c.chunk_index)
                              OR a.plan_logical_bytes<>(
                                  SELECT COALESCE(sum(sf.file_size_bytes),0)
                                  FROM remote_plan_files pf
                                  JOIN remote_sessions s
                                    ON s.session_id=c.session_id
                                  JOIN remote_snapshot_files sf
                                    ON sf.snapshot_file_id=pf.snapshot_file_id
                                  WHERE pf.plan_id=s.plan_id
                                    AND pf.chunk_index=c.chunk_index)
                              OR a.first_ordinal<>(
                                  SELECT min(pf.ordinal)
                                  FROM remote_plan_files pf
                                  JOIN remote_sessions s
                                    ON s.session_id=c.session_id
                                  WHERE pf.plan_id=s.plan_id
                                    AND pf.chunk_index=c.chunk_index)
                              OR a.last_ordinal<>(
                                  SELECT max(pf.ordinal)
                                  FROM remote_plan_files pf
                                  JOIN remote_sessions s
                                    ON s.session_id=c.session_id
                                  WHERE pf.plan_id=s.plan_id
                                    AND pf.chunk_index=c.chunk_index)))"""
            ).fetchone()["n"])
            if boundary_issues:
                issues.append(
                    f"{boundary_issues} chunk(s) conflict with boundaries")
            if audit_issues:
                issues.append(
                    f"{audit_issues} boundary record(s) lack exact audit coverage")

        directory_tables = (
            "directory_archive_stats", "directory_archive_bundles",
            "directory_tree_index")
        installed = [name for name in directory_tables
                     if self._table_exists_conn(conn, name)]
        if len(installed) == 3:
            report["optional_007_state"] = "installed"
            for column in (
                    "container_id", "container_format", "tape_generation_id",
                    "actual_artifact_bytes"):
                if not self._column_exists_conn(
                        conn, "directory_archive_bundles", column):
                    issues.append(
                        f"missing optional-007 compatibility column: {column}")
        elif installed:
            report["optional_007_state"] = "partial"
            issues.append(
                f"migration 007 partially installed: {installed!r}")

        report["ready"] = not issues
        return report

    def container_format_schema_report(self):
        """Exact, read-only migration-015 report."""
        return self._run_read(
            self._container_format_schema_report_conn,
            "container format schema report")

    def container_format_schema_installed(self):
        return bool(self.container_format_schema_report()["ready"])

    def validate_container_format_schema(self):
        report = self.container_format_schema_report()
        if not report["ready"]:
            raise RuntimeError(
                "[DB] Container-format schema missing or drifted: "
                + "; ".join(report["issues"]))
        return report

    # ------------------------------------------------------------------
    # Legacy-session exception classification (read-only preflight)
    # ------------------------------------------------------------------

    @staticmethod
    def _format_boundary_row(row):
        item = dict(row)
        integer_fields = (
            "chunk_index", "member_count", "distinct_ordinal_count",
            "first_ordinal", "last_ordinal", "logical_bytes",
            "file_state_count", "worker_attempt_count", "catalog_file_count",
            "archive_run_count", "container_count", "written_container_count",
            "artifact_count", "directory_evidence_count",
            "sealed_batch_evidence_count", "sealed_batch_written_count",
        )
        for field in integer_fields:
            if item.get(field) is not None:
                item[field] = int(item[field])
        item["fixed_membership"] = bool(
            item["member_count"] > 0
            and item["distinct_ordinal_count"] == item["member_count"]
            and item["first_ordinal"] == 0
            and item["last_ordinal"] == item["member_count"] - 1
            and (item.get("expected_file_count") is None
                 or int(item["expected_file_count"]) == item["member_count"])
            and (item.get("expected_bytes") is None
                 or int(item["expected_bytes"]) == item["logical_bytes"])
            and item.get("membership_state") in (None, "sealed"))
        item["eligible_stored_tar"] = bool(
            item["status"] == "pending"
            and not item.get("error_msg")
            and item.get("owner_token") is None
            and item.get("lease_expires_at") is None
            and item.get("attempt_id") is None
            and item["fixed_membership"]
            and all(item[name] == 0 for name in (
                "file_state_count", "worker_attempt_count",
                "catalog_file_count", "archive_run_count", "container_count",
                "artifact_count", "directory_evidence_count",
                "sealed_batch_evidence_count")))
        item["immutable_zip"] = bool(
            any(item[name] > 0 for name in (
                "catalog_file_count", "archive_run_count",
                "container_count", "artifact_count",
                "written_container_count",
                "sealed_batch_written_count")))
        item["corroborating_evidence"] = item["immutable_zip"]
        if item["immutable_zip"]:
            item["classification"] = "immutable_zip"
        elif item["eligible_stored_tar"]:
            item["classification"] = "eligible_stored_tar_exception"
        else:
            item["classification"] = "blocked"
        item["has_error_evidence"] = bool(item.get("error_msg"))
        item["has_owner_evidence"] = item.get("owner_token") is not None
        item["has_lease_evidence"] = item.get("lease_expires_at") is not None
        item["has_attempt_evidence"] = item.get("attempt_id") is not None
        for sensitive in (
                "error_msg", "owner_token", "lease_expires_at", "attempt_id"):
            item.pop(sensitive, None)
        return item

    def classify_format_boundary(self, session_id):
        """Read real DB evidence and derive a safe contiguous boundary.

        This is a preflight/report.  Migration 015 repeats the evidence query
        under table locks at assignment time; this result never authorizes a
        write by itself.
        """
        session_id = int(session_id)

        def operation(conn):
            if not self._table_exists_conn(conn, "remote_worker_attempts"):
                return {
                    "session_id": session_id,
                    "chunks": [], "derived_boundary": None,
                    "blocking": ["migration 014 base is absent"],
                }

            directory_tables = (
                "directory_archive_stats", "directory_archive_bundles",
                "directory_tree_index")
            directory_count = sum(self._table_exists_conn(conn, name)
                                  for name in directory_tables)
            batch_tables = (
                "tape_write_batches", "tape_write_batch_chunks",
                "tape_write_active_chunk")
            batch_count = sum(self._table_exists_conn(conn, name)
                              for name in batch_tables)
            blocking = []
            if directory_count not in (0, 3):
                blocking.append(
                    f"migration 007 is partial ({directory_count}/3 tables)")
            if batch_count not in (0, 3):
                blocking.append(
                    f"migration 012 is partial ({batch_count}/3 tables)")

            has_containers = self._table_exists_conn(conn, "archive_containers")
            has_artifacts = self._table_exists_conn(conn, "archive_artifacts")
            has_run_chunk = self._column_exists_conn(
                conn, "archive_runs", "remote_chunk_index")
            packaging_format = (
                "c.packaging_format" if self._column_exists_conn(
                    conn, "remote_chunks", "packaging_format")
                else "NULL::text")

            container_count = "0"
            written_container_count = "0"
            if has_containers:
                container_count = (
                    "(SELECT count(*) FROM archive_containers ac "
                    "WHERE ac.session_id=c.session_id "
                    "AND ac.chunk_index=c.chunk_index)")
                written_container_count = (
                    "(SELECT count(*) FROM archive_containers ac "
                    "WHERE ac.session_id=c.session_id "
                    "AND ac.chunk_index=c.chunk_index "
                    "AND (ac.tape_path IS NOT NULL OR ac.writer_state IN "
                    "('writing','copied','ambiguous') OR ac.catalog_state IN "
                    "('committing','committed','ambiguous')))")
            artifact_count = "0"
            if has_artifacts:
                artifact_count = (
                    "(SELECT count(*) FROM archive_artifacts aa "
                    "WHERE aa.session_id=c.session_id "
                    "AND aa.chunk_index=c.chunk_index)")
            archive_run_count = "0"
            if has_run_chunk:
                archive_run_count = (
                    "(SELECT count(*) FROM archive_runs ar "
                    "WHERE ar.remote_session_id=c.session_id "
                    "AND ar.remote_chunk_index=c.chunk_index)")
            # The migration-007 chunk_index is not trustworthy remote
            # provenance. Directory rows are reconciled globally through their
            # archive_run_id and files_index after the boundary is derived.
            directory_evidence = "0"
            batch_evidence = "0"
            batch_written = "0"
            if batch_count == 3:
                batch_evidence = (
                    "((SELECT count(*) FROM tape_write_batch_chunks bc "
                    "WHERE bc.session_id=c.session_id AND "
                    "bc.chunk_index=c.chunk_index) + "
                    "(SELECT count(*) FROM tape_write_active_chunk ba "
                    "WHERE ba.session_id=c.session_id AND "
                    "ba.chunk_index=c.chunk_index))")
                batch_written = (
                    "(SELECT count(*) FROM tape_write_batch_chunks bc "
                    "JOIN tape_write_batches b ON b.batch_id=bc.batch_id "
                    "WHERE bc.session_id=c.session_id AND "
                    "bc.chunk_index=c.chunk_index AND "
                    "(bc.member_write_phase='copied' OR b.state='durable'))")

            sql = f"""SELECT c.session_id, c.chunk_index, c.status,
                               c.error_msg, c.owner_token, c.lease_expires_at,
                               c.attempt_id, c.membership_state,
                               c.expected_file_count, c.expected_bytes,
                               {packaging_format} AS packaging_format,
                               COALESCE(m.member_count,0) AS member_count,
                               COALESCE(m.distinct_ordinal_count,0)
                                   AS distinct_ordinal_count,
                               m.first_ordinal, m.last_ordinal,
                               COALESCE(m.logical_bytes,0) AS logical_bytes,
                               (SELECT count(*) FROM remote_file_state fs
                                JOIN remote_plan_files pfs
                                  ON pfs.plan_file_id=fs.plan_file_id
                                WHERE fs.session_id=c.session_id
                                  AND pfs.plan_id=s.plan_id
                                  AND pfs.chunk_index=c.chunk_index)
                                   AS file_state_count,
                               (SELECT count(*) FROM remote_worker_attempts wa
                                WHERE wa.session_id=c.session_id
                                  AND wa.chunk_index=c.chunk_index)
                                   AS worker_attempt_count,
                               (SELECT count(*) FROM files_index fi
                                WHERE fi.remote_session_id=c.session_id
                                  AND fi.remote_chunk_index=c.chunk_index)
                                   AS catalog_file_count,
                               {archive_run_count} AS archive_run_count,
                               {container_count} AS container_count,
                               {written_container_count}
                                   AS written_container_count,
                               {artifact_count} AS artifact_count,
                               {directory_evidence}
                                   AS directory_evidence_count,
                               {batch_evidence}
                                   AS sealed_batch_evidence_count,
                               {batch_written} AS sealed_batch_written_count
                        FROM remote_chunks c
                        JOIN remote_sessions s ON s.session_id=c.session_id
                        LEFT JOIN LATERAL (
                            SELECT count(*) AS member_count,
                                   count(DISTINCT pf.ordinal)
                                       AS distinct_ordinal_count,
                                   min(pf.ordinal) AS first_ordinal,
                                   max(pf.ordinal) AS last_ordinal,
                                   COALESCE(sum(sf.file_size_bytes),0)
                                       AS logical_bytes
                            FROM remote_plan_files pf
                            JOIN remote_snapshot_files sf
                              ON sf.snapshot_file_id=pf.snapshot_file_id
                            WHERE pf.plan_id=s.plan_id
                              AND pf.chunk_index=c.chunk_index
                        ) m ON TRUE
                        WHERE c.session_id=%s
                        ORDER BY c.chunk_index"""
            raw = conn.execute(sql, (session_id,)).fetchall()
            chunks = [self._format_boundary_row(row) for row in raw]

            ambiguous_files = int(conn.execute(
                """SELECT count(*) AS n FROM files_index fi
                   WHERE fi.remote_session_id=%s
                     AND (fi.remote_chunk_index IS NULL OR NOT EXISTS (
                         SELECT 1 FROM remote_chunks c
                         WHERE c.session_id=%s
                           AND c.chunk_index=fi.remote_chunk_index))""",
                (session_id, session_id)).fetchone()["n"])
            if ambiguous_files:
                blocking.append(
                    f"{ambiguous_files} catalog row(s) lack trustworthy "
                    "remote chunk provenance")

            if has_run_chunk:
                run_ambiguity_sql = """
                    SELECT count(*) AS n FROM archive_runs ar
                    WHERE ar.remote_session_id=%s AND (
                      (ar.remote_chunk_index IS NOT NULL AND (
                        NOT EXISTS (SELECT 1 FROM remote_chunks c
                                    WHERE c.session_id=%s
                                      AND c.chunk_index=ar.remote_chunk_index)
                        OR EXISTS (SELECT 1 FROM files_index fi
                                   WHERE fi.archive_run_id=ar.run_id
                                     AND (fi.remote_session_id IS DISTINCT FROM %s
                                          OR fi.remote_chunk_index IS DISTINCT FROM
                                             ar.remote_chunk_index))))
                      OR (ar.remote_chunk_index IS NULL AND (
                        NOT EXISTS (SELECT 1 FROM files_index fi
                                    WHERE fi.archive_run_id=ar.run_id
                                      AND fi.remote_session_id=%s
                                      AND fi.remote_chunk_index IS NOT NULL)
                        OR EXISTS (SELECT 1 FROM files_index fi
                                   WHERE fi.archive_run_id=ar.run_id
                                     AND (fi.remote_session_id IS DISTINCT FROM %s
                                          OR fi.remote_chunk_index IS NULL)))))"""
                run_params = (session_id,) * 5
            else:
                run_ambiguity_sql = """
                    SELECT count(*) AS n FROM archive_runs ar
                    WHERE ar.remote_session_id=%s AND (
                        NOT EXISTS (SELECT 1 FROM files_index fi
                                    WHERE fi.archive_run_id=ar.run_id
                                      AND fi.remote_session_id=%s
                                      AND fi.remote_chunk_index IS NOT NULL)
                        OR EXISTS (SELECT 1 FROM files_index fi
                                   WHERE fi.archive_run_id=ar.run_id
                                     AND (fi.remote_session_id IS DISTINCT FROM %s
                                          OR fi.remote_chunk_index IS NULL)))"""
                run_params = (session_id,) * 3
            ambiguous_runs = int(conn.execute(
                run_ambiguity_sql, run_params).fetchone()["n"])
            if ambiguous_runs:
                blocking.append(
                    f"{ambiguous_runs} archive run(s) lack trustworthy "
                    "remote chunk provenance")

            prefix_evidence_counts = {
                "corroborated": 0,
                "status_only": 0,
            }
            if not chunks:
                blocking.append("session has no chunks or does not exist")
                derived = None
            else:
                indexes = [item["chunk_index"] for item in chunks]
                if indexes != list(range(indexes[-1] + 1)):
                    blocking.append("chunk identities are non-contiguous")
                eligible = [item["chunk_index"] for item in chunks
                            if item["eligible_stored_tar"]]
                derived = min(eligible) if eligible else None
                if derived is None:
                    blocking.append("no evidence-proven never-started suffix")
                else:
                    # ZIP is the conservative legacy default. A fixed done
                    # prefix is safe to classify as ZIP without positive write
                    # evidence; that absence is exposed, not erased. This
                    # cannot grant TAR because eligible_stored_tar independently
                    # requires pending status and zero operational evidence.
                    for item in chunks:
                        if item["chunk_index"] < derived:
                            basis = ("corroborated"
                                     if item["corroborating_evidence"]
                                     else "status_only")
                            item["prefix_evidence_basis"] = basis
                            prefix_evidence_counts[basis] += 1
                            item["classification"] = (
                                "immutable_zip"
                                if (item["status"] == "done"
                                    and item["fixed_membership"])
                                else "blocked")
                        else:
                            item["prefix_evidence_basis"] = None
                            item["classification"] = (
                                "eligible_stored_tar_exception"
                                if item["eligible_stored_tar"]
                                else "blocked")
                    contradictory = [
                        item["chunk_index"] for item in chunks
                        if ((item["chunk_index"] < derived
                             and (item["status"] != "done"
                                  or not item["fixed_membership"]))
                            or (item["chunk_index"] >= derived
                                and not item["eligible_stored_tar"]))
                    ]
                    if contradictory:
                        blocking.append(
                            "contradictory/indeterminate chunks: "
                            + ",".join(map(str, contradictory)))

                    if directory_count == 3:
                        directory_sql = """
                            SELECT count(*) AS n FROM (
                                SELECT archive_run_id
                                FROM directory_archive_stats
                                WHERE remote_session_id=%s
                                UNION ALL
                                SELECT archive_run_id
                                FROM directory_archive_bundles
                                WHERE remote_session_id=%s
                                UNION ALL
                                SELECT archive_run_id
                                FROM directory_tree_index
                                WHERE remote_session_id=%s
                            ) d
                            WHERE EXISTS (
                                  SELECT 1 FROM files_index fi
                                  WHERE fi.archive_run_id=d.archive_run_id
                                    AND (fi.remote_session_id IS DISTINCT FROM %s
                                         OR (fi.remote_chunk_index IS NOT NULL
                                             AND fi.remote_chunk_index >= %s)))"""
                        params = (session_id, session_id, session_id,
                                  session_id, derived)
                        ambiguous_directories = int(conn.execute(
                            directory_sql, params).fetchone()["n"])
                        if ambiguous_directories:
                            blocking.append(
                                f"{ambiguous_directories} directory catalog "
                                "row(s) have contradictory TAR-suffix provenance")

            nonnull_formats = [item for item in chunks
                               if item.get("packaging_format") is not None]
            if nonnull_formats:
                boundary_row = None
                if self._table_exists_conn(
                        conn, "remote_packaging_boundaries"):
                    boundary_row = conn.execute(
                        """SELECT first_stored_tar_chunk_index
                           FROM remote_packaging_boundaries
                           WHERE session_id=%s""", (session_id,)).fetchone()
                persisted = (int(boundary_row["first_stored_tar_chunk_index"])
                             if boundary_row else None)
                formats_match = persisted is not None and all(
                    item["packaging_format"] == (
                        "stored_tar" if item["chunk_index"] >= persisted
                        else "zip") for item in chunks)
                if not formats_match:
                    blocking.append(
                        "chunk formats were already assigned without an "
                        "exactly matching persisted exception boundary")

            return {
                "session_id": session_id,
                "chunk_count": len(chunks),
                "derived_boundary": derived,
                "prefix_evidence_counts": prefix_evidence_counts,
                "chunks": chunks,
                "blocking": blocking,
            }

        return self._run_read(
            operation, f"classify format boundary for session {session_id}")

    def container_format_schema_preflight(
            self, exception_session_id=None, *, ignore_archiver_lock=False):
        """Read-only preflight; never initializes or mutates schema."""
        report = self.container_format_schema_report()
        report = {
            "database": report["database"],
            "schema": report,
            "incremental_scan_installed":
                self.incremental_scan_schema_installed(),
            "incremental_scan_finalized":
                self.incremental_scan_schema_finalized(),
            "archiver_lock_held": False,
            "exception": None,
            "blocking": [],
        }

        def lock_read(conn):
            return bool(conn.execute(
                """SELECT count(*) AS n FROM pg_locks
                   WHERE locktype='advisory' AND granted
                     AND classid=%s AND objid=%s AND objsubid=1""",
                (self.ARCHIVER_LOCK_KEY >> 32,
                 self.ARCHIVER_LOCK_KEY & 0xFFFFFFFF),
            ).fetchone()["n"])

        report["archiver_lock_held"] = self._run_read(
            lock_read, "container format migration lock preflight")
        if not report["incremental_scan_installed"]:
            report["blocking"].append("migration 014 base is absent")
        if not report["incremental_scan_finalized"]:
            report["blocking"].append("migration 014 is not finalized")
        if report["archiver_lock_held"] and not ignore_archiver_lock:
            report["blocking"].append("the cluster archiver lock is held")
        if report["schema"].get("installation_state") == "partial":
            report["blocking"].append(
                "migration 015 is partially installed")
        elif report["schema"]["metadata"] and not report["schema"]["ready"]:
            report["blocking"].append("installed migration 015 schema is drifted")
        if exception_session_id is not None:
            report["exception"] = self.classify_format_boundary(
                int(exception_session_id))
            report["blocking"].extend(report["exception"]["blocking"])
        return report

    # ------------------------------------------------------------------
    # Initial assignment and format reads
    # ------------------------------------------------------------------

    def _assign_new_chunk_format_conn(
            self, conn, session_id, chunk_index, *,
            stored_tar_write_enabled=False, reader_contract_version=None):
        if not self._table_exists_conn(
                conn, "container_format_schema_metadata"):
            raise RuntimeError(
                "[DB] Migration 015 is not installed; refusing to create an "
                "unformatted remote chunk")
        row = conn.execute(
            """SELECT lto_assign_new_chunk_format(%s,%s,%s,%s,%s)
                      AS packaging_format""",
            (int(session_id), int(chunk_index),
             stored_tar_write_enabled is True,
             CONTAINER_FORMAT_SCHEMA_VERSION, reader_contract_version),
        ).fetchone()
        return ContainerFormat(row["packaging_format"])

    def assign_new_chunk_format(
            self, session_id, chunk_index, *, stored_tar_write_enabled=False,
            reader_contract_version=None, conn=None):
        """Return the only legal format for an initial chunk INSERT.

        Both production INSERT call sites pass their existing transaction as
        ``conn`` and put this result directly into the INSERT.  Consequently no
        row can exist between "chunk created" and "format assigned".
        """
        if conn is not None:
            return self._assign_new_chunk_format_conn(
                conn, session_id, chunk_index,
                stored_tar_write_enabled=stored_tar_write_enabled,
                reader_contract_version=reader_contract_version)
        return self._run_read(
            lambda active: self._assign_new_chunk_format_conn(
                active, session_id, chunk_index,
                stored_tar_write_enabled=stored_tar_write_enabled,
                reader_contract_version=reader_contract_version),
            f"resolve new chunk format for session {session_id}, "
            f"chunk {chunk_index}")

    def get_chunk_packaging_format(self, session_id, chunk_index):
        def operation(conn):
            if not self._column_exists_conn(
                    conn, "remote_chunks", "packaging_format"):
                raise RuntimeError(
                    "[DB] Migration 015 is absent; chunk format is unknown")
            row = conn.execute(
                """SELECT packaging_format FROM remote_chunks
                   WHERE session_id=%s AND chunk_index=%s""",
                (int(session_id), int(chunk_index)),
            ).fetchone()
            if row is None:
                raise RuntimeError(
                    f"[DB] Remote chunk not found: session {session_id}, "
                    f"chunk {chunk_index}")
            return ContainerFormat(row["packaging_format"])
        return self._run_read(
            operation,
            f"get chunk format for session {session_id}, chunk {chunk_index}")

    def require_existing_stored_tar_recovery(
            self, session_id, chunk_index, *, reader_contract_version=None):
        """Authorize an already assigned TAR row independently of write flag.

        Disabling new TAR creation must never strand an existing TAR.  Reader
        compatibility is still mandatory.  Phase 0 has no reader, so the normal
        runtime version is ``None`` and this correctly refuses until Phase 1.
        """
        self.validate_container_format_schema()
        if self.get_chunk_packaging_format(
                session_id, chunk_index) is not ContainerFormat.STORED_TAR:
            raise RuntimeError("chunk is not assigned Stored TAR")
        if reader_contract_version is None:
            reader_contract_version = stored_tar_reader_contract_version()
        if reader_contract_version != \
                REQUIRED_STORED_TAR_READER_CONTRACT_VERSION:
            raise RuntimeError(
                "Stored TAR reader contract is unavailable or mismatched")
        return True

    # ------------------------------------------------------------------
    # Container/artifact identities and writer-readiness comparison
    # ------------------------------------------------------------------

    def create_archive_container(self, record):
        values = dict(record)
        required = (
            "session_id", "chunk_index", "container_ordinal",
            "container_format", "format_version", "storage_class",
            "container_name", "expected_member_count",
            "expected_logical_bytes")
        missing = [name for name in required if values.get(name) is None]
        if missing:
            raise ValueError(f"missing archive container fields: {missing}")

        def operation(conn):
            row = conn.execute(
                """INSERT INTO archive_containers
                       (session_id, chunk_index, container_ordinal,
                        container_format, format_version, tar_dialect,
                        storage_class, container_name,
                        temporary_data_locator,
                        permanent_local_metadata_locator,
                        expected_member_count, expected_logical_bytes,
                        observed_member_count, observed_logical_bytes,
                        actual_artifact_bytes, validation_state,
                        writer_state, catalog_state, owner_token,
                        lease_expires_at)
                   VALUES
                       (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s)
                   ON CONFLICT (session_id, chunk_index, container_ordinal)
                   DO NOTHING
                   RETURNING *""",
                (int(values["session_id"]), int(values["chunk_index"]),
                 int(values["container_ordinal"]),
                 ContainerFormat(values["container_format"]).value,
                 values["format_version"], values.get("tar_dialect"),
                 values["storage_class"], values["container_name"],
                 values.get("temporary_data_locator"),
                 values.get("permanent_local_metadata_locator"),
                 int(values["expected_member_count"]),
                 int(values["expected_logical_bytes"]),
                 values.get("observed_member_count"),
                 values.get("observed_logical_bytes"),
                 values.get("actual_artifact_bytes"),
                 values.get("validation_state", "planned"),
                 values.get("writer_state", "not_started"),
                 values.get("catalog_state", "not_started"),
                 values.get("owner_token"), values.get("lease_expires_at")),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """SELECT * FROM archive_containers
                       WHERE session_id=%s AND chunk_index=%s
                         AND container_ordinal=%s FOR UPDATE""",
                    (int(values["session_id"]), int(values["chunk_index"]),
                     int(values["container_ordinal"])),
                ).fetchone()
                immutable = (
                    "container_format", "format_version", "tar_dialect",
                    "storage_class", "container_name",
                    "expected_member_count", "expected_logical_bytes")
                mismatched = [name for name in immutable
                              if row[name] != values.get(name)]
                comparable = (
                    "temporary_data_locator",
                    "permanent_local_metadata_locator",
                    "observed_member_count", "observed_logical_bytes",
                    "actual_artifact_bytes")
                mismatched.extend(
                    name for name in comparable
                    if name in values and values.get(name) is not None
                    and row[name] != values.get(name))
                if mismatched:
                    raise RuntimeError(
                        "existing container identity has conflicting fields: "
                        + ", ".join(mismatched))
            return _row(row)
        return self._transaction(operation, "create archive container")

    def create_archive_artifact(self, record):
        values = dict(record)
        required = (
            "session_id", "chunk_index", "artifact_kind",
            "artifact_version")
        missing = [name for name in required if values.get(name) is None]
        if missing:
            raise ValueError(f"missing archive artifact fields: {missing}")

        def operation(conn):
            row = conn.execute(
                """INSERT INTO archive_artifacts
                       (session_id, chunk_index, container_id, artifact_kind,
                        artifact_version, local_locator, tape_locator,
                        artifact_size_bytes, readiness_state,
                        publication_started_at, published_at, tape_published_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING RETURNING *""",
                (int(values["session_id"]), int(values["chunk_index"]),
                 values.get("container_id"), values["artifact_kind"],
                 values["artifact_version"], values.get("local_locator"),
                 values.get("tape_locator"), values.get("artifact_size_bytes"),
                 values.get("readiness_state", "planned"),
                 values.get("publication_started_at"),
                 values.get("published_at"),
                 values.get("tape_published_at")),
            ).fetchone()
            if row is None:
                if values.get("container_id") is None:
                    row = conn.execute(
                        """SELECT * FROM archive_artifacts
                           WHERE session_id=%s AND chunk_index=%s
                             AND container_id IS NULL AND artifact_kind=%s
                             AND artifact_version=%s FOR UPDATE""",
                        (int(values["session_id"]), int(values["chunk_index"]),
                         values["artifact_kind"], values["artifact_version"]),
                    ).fetchone()
                else:
                    row = conn.execute(
                        """SELECT * FROM archive_artifacts
                           WHERE container_id=%s AND artifact_kind=%s
                             AND artifact_version=%s FOR UPDATE""",
                        (int(values["container_id"]), values["artifact_kind"],
                         values["artifact_version"]),
                    ).fetchone()
                if row is None:
                    raise RuntimeError(
                        "artifact insert conflicted without a matching identity")
                compare = (
                    "session_id", "chunk_index", "container_id",
                    "artifact_kind", "artifact_version", "local_locator",
                    "artifact_size_bytes", "tape_locator",
                    "publication_started_at", "published_at",
                    "tape_published_at")
                mismatched = [
                    name for name in compare
                    if name in values and values.get(name) is not None
                    and row[name] != values.get(name)]
                if mismatched:
                    raise RuntimeError(
                        "existing artifact identity has conflicting fields: "
                        + ", ".join(mismatched))
            return _row(row)
        return self._transaction(operation, "create archive artifact")

    def get_archive_containers(self, session_id, chunk_index):
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT * FROM archive_containers
                   WHERE session_id=%s AND chunk_index=%s
                   ORDER BY container_ordinal""",
                (int(session_id), int(chunk_index)),
            ).fetchall()),
            f"get containers for session {session_id}, chunk {chunk_index}")

    def _stored_tar_plan_schema_installed_conn(self, conn):
        return (
            self._column_exists_conn(
                conn, "remote_chunks", "stored_tar_max_size_bytes")
            and self._column_exists_conn(
                conn, "archive_containers", "estimated_archive_bytes")
            and self._column_exists_conn(
                conn, "archive_containers", "validated_part_locator")
            and self._column_exists_conn(
                conn, "archive_containers", "validation_summary")
            and self._column_exists_conn(
                conn, "archive_containers", "disposition_counts")
            and self._table_exists_conn(conn, "archive_container_members"))

    def stored_tar_plan_schema_installed(self):
        return self._run_read(
            self._stored_tar_plan_schema_installed_conn,
            "stored TAR plan schema preflight")

    def _read_stored_tar_chunk_plan_conn(self, conn, session_id, chunk_index):
        chunk = conn.execute(
            """SELECT stored_tar_max_size_bytes FROM remote_chunks
               WHERE session_id=%s AND chunk_index=%s""",
            (int(session_id), int(chunk_index)),
        ).fetchone()
        if chunk is None:
            raise RuntimeError(
                f"[DB] Remote chunk not found: session {session_id}, "
                f"chunk {chunk_index}")
        containers = [
            StoredTarContainerPlan(
                session_id=int(row["session_id"]),
                chunk_index=int(row["chunk_index"]),
                container_ordinal=int(row["container_ordinal"]),
                container_name=row["container_name"],
                expected_member_count=int(row["expected_member_count"]),
                expected_logical_bytes=int(row["expected_logical_bytes"]),
                estimated_archive_bytes=int(row["estimated_archive_bytes"] or 0),
                max_size_bytes=int(chunk["stored_tar_max_size_bytes"] or 0),
                container_format=ContainerFormat(row["container_format"]),
                format_version=row["format_version"],
                tar_dialect=row["tar_dialect"],
                storage_class=row["storage_class"],
            )
            for row in conn.execute(
                """SELECT * FROM archive_containers
                   WHERE session_id=%s AND chunk_index=%s
                     AND container_format='stored_tar'
                   ORDER BY container_ordinal""",
                (int(session_id), int(chunk_index)),
            ).fetchall()
        ]
        members = [
            StoredTarPlanMember(
                manifest_id=int(row["plan_file_id"]),
                plan_ordinal=int(row["plan_ordinal"]),
                remote_path=row["remote_path"],
                file_size_bytes=int(row["expected_logical_bytes"]),
                storage_class=row["storage_class"],
                container_ordinal=(
                    None if row["container_ordinal"] is None
                    else int(row["container_ordinal"])),
                estimated_tar_bytes=int(row["estimated_tar_bytes"]),
            )
            for row in conn.execute(
                """SELECT * FROM archive_container_members
                   WHERE session_id=%s AND chunk_index=%s
                   ORDER BY plan_ordinal""",
                (int(session_id), int(chunk_index)),
            ).fetchall()
        ]
        return StoredTarChunkPlan(
            session_id=int(session_id),
            chunk_index=int(chunk_index),
            max_size_bytes=int(chunk["stored_tar_max_size_bytes"] or 0),
            containers=tuple(containers),
            members=tuple(members),
        )

    def get_stored_tar_chunk_plan(self, session_id, chunk_index):
        def operation(conn):
            if not self._stored_tar_plan_schema_installed_conn(conn):
                raise RuntimeError(
                    "[DB] Stored TAR plan/publication schema is absent; apply "
                    "migrations 016 and 017")
            return self._read_stored_tar_chunk_plan_conn(
                conn, session_id, chunk_index)
        return self._run_read(
            operation,
            f"get stored TAR plan for session {session_id}, chunk {chunk_index}")

    def get_or_create_stored_tar_chunk_plan(
            self, session_id, chunk_index, chunk_files, *,
            loose_threshold_bytes, max_size_bytes):
        """Persist the immutable Task-2.1 split before any TAR worker starts."""
        chunk_files = list(chunk_files)

        def operation(conn):
            if not self._stored_tar_plan_schema_installed_conn(conn):
                raise RuntimeError(
                    "[DB] Stored TAR plan/publication schema is absent; apply "
                    "migrations 016 and 017")
            chunk = conn.execute(
                """SELECT packaging_format, membership_state,
                          stored_tar_max_size_bytes
                   FROM remote_chunks
                   WHERE session_id=%s AND chunk_index=%s
                   FOR UPDATE""",
                (int(session_id), int(chunk_index)),
            ).fetchone()
            if chunk is None:
                raise RuntimeError(
                    f"[DB] Remote chunk not found: session {session_id}, "
                    f"chunk {chunk_index}")
            if chunk["packaging_format"] != ContainerFormat.STORED_TAR.value:
                raise RuntimeError(
                    "[DB] Stored TAR plan requested for a non-TAR chunk")
            if chunk["membership_state"] != "sealed":
                raise RuntimeError(
                    "[DB] Stored TAR planning requires a sealed chunk")

            existing = conn.execute(
                """SELECT count(*) AS n FROM archive_container_members
                   WHERE session_id=%s AND chunk_index=%s""",
                (int(session_id), int(chunk_index)),
            ).fetchone()["n"]
            if existing:
                return self._read_stored_tar_chunk_plan_conn(
                    conn, session_id, chunk_index)

            resolved_cap = (
                int(chunk["stored_tar_max_size_bytes"])
                if chunk["stored_tar_max_size_bytes"] is not None
                else int(max_size_bytes))
            conn.execute(
                """UPDATE remote_chunks
                   SET stored_tar_max_size_bytes=COALESCE(
                         stored_tar_max_size_bytes, %s)
                   WHERE session_id=%s AND chunk_index=%s""",
                (resolved_cap, int(session_id), int(chunk_index)),
            )

            plan = build_stored_tar_chunk_plan(
                session_id, chunk_index, chunk_files,
                loose_threshold_bytes=int(loose_threshold_bytes),
                max_size_bytes=resolved_cap)
            container_ids = {}
            for container in plan.containers:
                row = conn.execute(
                    """INSERT INTO archive_containers
                           (session_id, chunk_index, container_ordinal,
                            container_format, format_version, tar_dialect,
                            storage_class, container_name,
                            expected_member_count, expected_logical_bytes,
                            estimated_archive_bytes, validation_state,
                            writer_state, catalog_state)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               'planned','not_started','not_started')
                       ON CONFLICT (session_id, chunk_index, container_ordinal)
                       DO NOTHING
                       RETURNING *""",
                    (container.session_id, container.chunk_index,
                     container.container_ordinal,
                     container.container_format.value,
                     STORED_TAR_FORMAT_VERSION, container.tar_dialect,
                     container.storage_class, container.container_name,
                     container.expected_member_count,
                     container.expected_logical_bytes,
                     container.estimated_archive_bytes),
                ).fetchone()
                if row is None:
                    row = conn.execute(
                        """SELECT * FROM archive_containers
                           WHERE session_id=%s AND chunk_index=%s
                             AND container_ordinal=%s FOR UPDATE""",
                        (container.session_id, container.chunk_index,
                         container.container_ordinal),
                    ).fetchone()
                    checks = {
                        "container_format": container.container_format.value,
                        "format_version": STORED_TAR_FORMAT_VERSION,
                        "tar_dialect": container.tar_dialect,
                        "storage_class": container.storage_class,
                        "container_name": container.container_name,
                        "expected_member_count":
                            container.expected_member_count,
                        "expected_logical_bytes":
                            container.expected_logical_bytes,
                        "estimated_archive_bytes":
                            container.estimated_archive_bytes,
                    }
                    mismatch = [
                        key for key, value in checks.items()
                        if row[key] != value]
                    if mismatch:
                        raise RuntimeError(
                            "existing Stored TAR container plan conflicts: "
                            + ", ".join(mismatch))
                container_ids[int(row["container_ordinal"])] = int(
                    row["container_id"])

            member_rows = []
            for member in plan.members:
                container_id = (
                    None if member.container_ordinal is None
                    else container_ids[int(member.container_ordinal)])
                member_rows.append((
                    plan.session_id, plan.chunk_index, member.manifest_id,
                    member.plan_ordinal, member.storage_class, container_id,
                    member.container_ordinal, member.remote_path,
                    member.file_size_bytes, member.estimated_tar_bytes))
            if member_rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO archive_container_members
                               (session_id, chunk_index, plan_file_id,
                                plan_ordinal, storage_class, container_id,
                                container_ordinal, remote_path,
                                expected_logical_bytes, estimated_tar_bytes)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT DO NOTHING""",
                        member_rows,
                    )
            return self._read_stored_tar_chunk_plan_conn(
                conn, session_id, chunk_index)

        return self._transaction(
            operation,
            f"persist stored TAR plan for session {session_id}, "
            f"chunk {chunk_index}")

    def get_archive_artifacts(self, session_id, chunk_index):
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT * FROM archive_artifacts
                   WHERE session_id=%s AND chunk_index=%s
                   ORDER BY artifact_id""",
                (int(session_id), int(chunk_index)),
            ).fetchall()),
            f"get artifacts for session {session_id}, chunk {chunk_index}")

    # ------------------------------------------------------------------
    # Tasks 2.2-2.4: owner-scoped build and paired readiness publication
    # ------------------------------------------------------------------

    @staticmethod
    def _stored_tar_json(value):
        if is_dataclass(value):
            value = asdict(value)

        def encode(item):
            return getattr(item, "value", str(item))
        return json.dumps(
            value, default=encode, sort_keys=True, separators=(",", ":"))

    def claim_stored_tar_container_build(
            self, container_id, owner_token, part_locator, *,
            lease_seconds=3600):
        """Claim one unique part before launching SSH; never steal an owner."""
        token = str(owner_token or "").strip()
        part = str(part_locator or "")
        if not token:
            raise ValueError("Stored TAR build owner token is required")
        if not part.lower().endswith(".part"):
            raise ValueError("Stored TAR build locator must name a .part")
        lease_seconds = max(1, int(lease_seconds))

        def operation(conn):
            row = conn.execute(
                """SELECT * FROM archive_containers
                   WHERE container_id=%s FOR UPDATE""",
                (int(container_id),),
            ).fetchone()
            if row is None or row["container_format"] != "stored_tar":
                raise RuntimeError("Stored TAR container does not exist")
            if row["writer_state"] != "not_started":
                raise RuntimeError("Stored TAR build cannot begin after writer start")
            state = row["validation_state"]
            if state == "ready":
                return _row(row)
            if row["owner_token"] not in (None, token):
                raise RuntimeError("Stored TAR container is owned by another builder")
            existing_part = row.get("validated_part_locator")
            if state in ("building", "validated_part"):
                if row["owner_token"] != token or existing_part != part:
                    raise RuntimeError(
                        "Stored TAR build identity conflicts with its existing part")
                return _row(row)
            if state != "planned":
                raise RuntimeError(
                    f"Stored TAR container is not buildable from state {state!r}")
            row = conn.execute(
                """UPDATE archive_containers
                   SET validation_state='building', owner_token=%s,
                       validated_part_locator=%s,
                       lease_expires_at=now()+(%s * interval '1 second'),
                       validation_started_at=COALESCE(validation_started_at, now()),
                       updated_at=now()
                   WHERE container_id=%s AND validation_state='planned'
                     AND owner_token IS NULL
                   RETURNING *""",
                (token, part, lease_seconds, int(container_id)),
            ).fetchone()
            if row is None:
                raise RuntimeError("Stored TAR build claim lost its owner race")
            return _row(row)
        return self._transaction(operation, "claim Stored TAR container build")

    def mark_stored_tar_validated_part(
            self, container_id, owner_token, part_locator, validation_summary,
            source_diagnostics=()):
        """Persist only unpublished-part proof; no final locator becomes ready."""
        token = str(owner_token or "").strip()
        part = str(part_locator or "")
        if not token or not part.lower().endswith(".part"):
            raise ValueError("validated Stored TAR part requires owner and .part")
        summary = (asdict(validation_summary) if is_dataclass(validation_summary)
                   else dict(validation_summary))
        summary["source_diagnostics"] = [
            asdict(item) if is_dataclass(item) else dict(item)
            for item in (source_diagnostics or ())]
        try:
            observed_count = int(summary["member_count"])
            observed_bytes = int(summary["logical_bytes"])
            artifact_bytes = int(summary["archive_size"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid Stored TAR validation summary") from exc
        payload = self._stored_tar_json(summary)

        def operation(conn):
            row = conn.execute(
                """SELECT * FROM archive_containers
                   WHERE container_id=%s FOR UPDATE""",
                (int(container_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("Stored TAR container does not exist")
            if row["owner_token"] != token:
                raise RuntimeError("Stored TAR validation owner check failed")
            if row["validated_part_locator"] != part:
                raise RuntimeError("Stored TAR validated part identity changed")
            if row["validation_state"] == "validated_part":
                if (row["observed_member_count"] != observed_count
                        or row["observed_logical_bytes"] != observed_bytes
                        or row["actual_artifact_bytes"] != artifact_bytes
                        or row["validation_summary"] != json.loads(payload)):
                    raise RuntimeError(
                        "Stored TAR validated-part replay is not equivalent")
                return _row(row)
            if row["validation_state"] != "building":
                raise RuntimeError("Stored TAR container is not being built")
            row = conn.execute(
                """UPDATE archive_containers
                   SET observed_member_count=%s, observed_logical_bytes=%s,
                       actual_artifact_bytes=%s,
                       validation_summary=%s::jsonb,
                       validation_state='validated_part', validated_at=now(),
                       updated_at=now()
                   WHERE container_id=%s AND owner_token=%s
                     AND validation_state='building'
                   RETURNING *""",
                (observed_count, observed_bytes, artifact_bytes, payload,
                 int(container_id), token),
            ).fetchone()
            if row is None:
                raise RuntimeError("Stored TAR validated-part CAS failed")
            return _row(row)
        return self._transaction(operation, "mark Stored TAR validated part")

    def restart_stored_tar_build_from_source(
            self, container_id, owner_token, new_part_locator):
        """Owner-proven pre-writer reset when exception evidence was lost."""
        token = str(owner_token or "").strip()
        part = str(new_part_locator or "")
        if not token or not part.lower().endswith(".part"):
            raise ValueError("source rebuild requires owner and a new .part")

        def operation(conn):
            row = conn.execute(
                """SELECT * FROM archive_containers
                   WHERE container_id=%s FOR UPDATE""",
                (int(container_id),),
            ).fetchone()
            if (row is None or row["owner_token"] != token
                    or row["writer_state"] != "not_started"
                    or row["catalog_state"] != "not_started"
                    or row["validation_state"] not in (
                        "building", "validated_part")):
                raise RuntimeError(
                    "Stored TAR source rebuild lacks proven pre-writer ownership")
            ready_sidecar = conn.execute(
                """SELECT 1 FROM archive_artifacts
                   WHERE container_id=%s AND artifact_kind='tar_sidecar'
                     AND readiness_state='ready' LIMIT 1""",
                (int(container_id),),
            ).fetchone()
            if ready_sidecar is not None:
                raise RuntimeError(
                    "Stored TAR source rebuild refused after sidecar readiness")
            row = conn.execute(
                """UPDATE archive_containers
                   SET validated_part_locator=%s, validation_summary=NULL,
                       disposition_counts=NULL, observed_member_count=NULL,
                       observed_logical_bytes=NULL, actual_artifact_bytes=NULL,
                       temporary_data_locator=NULL,
                       permanent_local_metadata_locator=NULL,
                       validation_state='building', validated_at=NULL,
                       updated_at=now()
                   WHERE container_id=%s AND owner_token=%s
                   RETURNING *""",
                (part, int(container_id), token),
            ).fetchone()
            return _row(row)
        return self._transaction(
            operation, "restart Stored TAR build from source")

    def reconcile_stored_tar_build_owner(
            self, container_id, expected_owner_token, expected_state,
            new_owner_token):
        """CAS a pre-writer build to a reconciliation owner.

        The caller must first prove that ``expected_owner_token`` is not live.
        This method deliberately does not turn lease expiry into liveness
        evidence; it only closes the race between that proof and adoption.
        """
        old = str(expected_owner_token or "").strip()
        new = str(new_owner_token or "").strip()
        state = str(expected_state or "")
        if not old or not new or old == new:
            raise ValueError("Stored TAR owner reconciliation needs two owners")
        if state not in ("building", "validated_part"):
            raise ValueError("Stored TAR owner reconciliation state is invalid")

        def operation(conn):
            row = conn.execute(
                """UPDATE archive_containers
                   SET owner_token=%s,
                       lease_expires_at=now()+(3600 * interval '1 second'),
                       updated_at=now()
                   WHERE container_id=%s AND owner_token=%s
                     AND validation_state=%s
                     AND writer_state='not_started'
                     AND catalog_state='not_started'
                   RETURNING *""",
                (new, int(container_id), old, state),
            ).fetchone()
            if row is None:
                raise RuntimeError(
                    "Stored TAR reconciliation owner/state CAS failed")
            return _row(row)
        return self._transaction(operation, "reconcile Stored TAR build owner")

    def reset_reconciled_stored_tar_build(
            self, container_id, owner_token, expected_state):
        """Return a proven-dead, unusable pre-writer build to ``planned``."""
        token = str(owner_token or "").strip()
        state = str(expected_state or "")
        if not token or state not in ("building", "validated_part"):
            raise ValueError("invalid Stored TAR reset identity")

        def operation(conn):
            ready = conn.execute(
                """SELECT 1 FROM archive_artifacts
                   WHERE container_id=%s AND artifact_kind='tar_sidecar'
                     AND readiness_state='ready' LIMIT 1""",
                (int(container_id),),
            ).fetchone()
            if ready is not None:
                raise RuntimeError(
                    "refusing to reset a build with a ready sidecar record")
            row = conn.execute(
                """UPDATE archive_containers
                   SET validation_state='planned', owner_token=NULL,
                       lease_expires_at=NULL, validated_part_locator=NULL,
                       validation_summary=NULL, disposition_counts=NULL,
                       observed_member_count=NULL,
                       observed_logical_bytes=NULL,
                       actual_artifact_bytes=NULL,
                       temporary_data_locator=NULL,
                       permanent_local_metadata_locator=NULL,
                       validation_started_at=NULL, validated_at=NULL,
                       updated_at=now()
                   WHERE container_id=%s AND owner_token=%s
                     AND validation_state=%s
                     AND writer_state='not_started'
                     AND catalog_state='not_started'
                   RETURNING *""",
                (int(container_id), token, state),
            ).fetchone()
            if row is None:
                raise RuntimeError("Stored TAR reconciled-build reset CAS failed")
            return _row(row)
        return self._transaction(operation, "reset reconciled Stored TAR build")

    def block_stored_tar_container(
            self, container_id, expected_state, expected_owner_token=None):
        """Durably fail closed on a proven local/DB artifact contradiction."""
        state = str(expected_state or "")

        def operation(conn):
            current = conn.execute(
                """SELECT * FROM archive_containers
                   WHERE container_id=%s FOR UPDATE""",
                (int(container_id),),
            ).fetchone()
            if current is None:
                raise RuntimeError("Stored TAR container does not exist")
            if current["validation_state"] == "blocked":
                return _row(current)
            if (current["validation_state"] != state
                    or current["owner_token"] != expected_owner_token
                    or current["writer_state"] != "not_started"
                    or current["catalog_state"] != "not_started"):
                raise RuntimeError("Stored TAR block owner/state CAS failed")
            row = conn.execute(
                """UPDATE archive_containers
                   SET validation_state='blocked', owner_token=NULL,
                       lease_expires_at=NULL, updated_at=now()
                   WHERE container_id=%s AND validation_state=%s
                     AND owner_token IS NOT DISTINCT FROM %s
                     AND writer_state='not_started'
                     AND catalog_state='not_started'
                   RETURNING *""",
                (int(container_id), state, expected_owner_token),
            ).fetchone()
            if row is None:
                raise RuntimeError("Stored TAR block CAS failed")
            conn.execute(
                """UPDATE archive_artifacts
                   SET readiness_state='blocked', updated_at=now()
                   WHERE container_id=%s AND readiness_state <> 'blocked'""",
                (int(container_id),),
            )
            return _row(row)
        return self._transaction(operation, "block inconsistent Stored TAR")

    def publish_stored_tar_pair(
            self, *, container_id, owner_token, sidecar_locator,
            sidecar_version, sidecar_size_bytes, temporary_data_locator,
            tar_size_bytes, observed_member_count, observed_logical_bytes,
            disposition_counts):
        """Make TAR + permanent sidecar writer-visible in one owner-checked tx."""
        token = str(owner_token or "").strip()
        sidecar_locator = str(sidecar_locator or "")
        data_locator = str(temporary_data_locator or "")
        if (not token or not sidecar_locator
                or sidecar_locator.lower().endswith(".part")
                or not data_locator or data_locator.lower().endswith(".part")):
            raise ValueError("paired TAR publication requires final locators")
        if (sidecar_locator.startswith(("/", "\\"))
                or (len(sidecar_locator) >= 2
                    and sidecar_locator[1] == ":")
                or any(part in ("", ".", "..")
                       for part in sidecar_locator.split("/"))
                or posixpath.normpath(sidecar_locator) != sidecar_locator):
            raise ValueError("TAR sidecar locator must be safe and root-relative")
        if not os.path.isabs(data_locator):
            raise ValueError("temporary TAR data locator must be a local absolute path")
        if int(sidecar_size_bytes) <= 0 or int(tar_size_bytes) <= 0:
            raise ValueError("paired TAR artifact sizes must be positive")
        counts = dict(disposition_counts)
        required_dispositions = {item.value for item in SourceDisposition}
        if set(counts) != required_dispositions:
            raise ValueError("Stored TAR disposition aggregate has unknown/missing keys")
        try:
            counts = {key: int(value) for key, value in counts.items()}
        except (TypeError, ValueError) as exc:
            raise ValueError("Stored TAR disposition counts must be integers") from exc
        if any(value < 0 for value in counts.values()):
            raise ValueError("Stored TAR disposition counts cannot be negative")
        if counts[SourceDisposition.ARCHIVED.value] != int(
                observed_member_count):
            raise ValueError("archived disposition count disagrees with TAR members")
        if counts[SourceDisposition.SOURCE_CHANGED.value] \
                or counts[SourceDisposition.UNRESOLVED.value]:
            raise ValueError("blocked Stored TAR dispositions cannot become ready")
        counts_payload = self._stored_tar_json(counts)

        def operation(conn):
            container = conn.execute(
                """SELECT * FROM archive_containers
                   WHERE container_id=%s FOR UPDATE""",
                (int(container_id),),
            ).fetchone()
            if container is None or container["container_format"] != "stored_tar":
                raise RuntimeError("Stored TAR container does not exist")

            artifact = conn.execute(
                """SELECT * FROM archive_artifacts
                   WHERE container_id=%s AND artifact_kind='tar_sidecar'
                     AND artifact_version=%s FOR UPDATE""",
                (int(container_id), str(sidecar_version)),
            ).fetchone()
            if container["validation_state"] == "ready":
                checks = {
                    "temporary_data_locator": data_locator,
                    "permanent_local_metadata_locator": sidecar_locator,
                    "actual_artifact_bytes": int(tar_size_bytes),
                    "observed_member_count": int(observed_member_count),
                    "observed_logical_bytes": int(observed_logical_bytes),
                    "disposition_counts": json.loads(counts_payload),
                }
                mismatch = [key for key, value in checks.items()
                            if container[key] != value]
                if (mismatch or artifact is None
                        or artifact["local_locator"] != sidecar_locator
                        or artifact["artifact_size_bytes"] != int(
                            sidecar_size_bytes)
                        or artifact["readiness_state"] != "ready"):
                    raise RuntimeError(
                        "ready Stored TAR pair conflicts with adoption request")
                return {"container": _row(container), "artifact": _row(artifact)}

            if (container["validation_state"] != "validated_part"
                    or container["owner_token"] != token):
                raise RuntimeError("Stored TAR paired publication owner check failed")
            if sum(counts.values()) != int(container["expected_member_count"]):
                raise RuntimeError(
                    "Stored TAR disposition aggregate does not cover its plan")
            if (container["actual_artifact_bytes"] != int(tar_size_bytes)
                    or container["observed_member_count"] != int(
                        observed_member_count)
                    or container["observed_logical_bytes"] != int(
                        observed_logical_bytes)):
                raise RuntimeError(
                    "Stored TAR filesystem pair disagrees with validated part")

            if artifact is None:
                artifact = conn.execute(
                    """INSERT INTO archive_artifacts
                           (session_id, chunk_index, container_id,
                            artifact_kind, artifact_version, local_locator,
                            artifact_size_bytes, readiness_state,
                            publication_started_at, published_at)
                       VALUES (%s,%s,%s,'tar_sidecar',%s,%s,%s,'ready',now(),now())
                       RETURNING *""",
                    (container["session_id"], container["chunk_index"],
                     int(container_id), str(sidecar_version), sidecar_locator,
                     int(sidecar_size_bytes)),
                ).fetchone()
            else:
                if artifact["readiness_state"] not in (
                        "planned", "writing", "validated"):
                    raise RuntimeError(
                        "existing TAR sidecar artifact is not publishable")
                for key, value in {
                        "local_locator": sidecar_locator,
                        "artifact_size_bytes": int(sidecar_size_bytes)}.items():
                    if artifact[key] not in (None, value):
                        raise RuntimeError(
                            f"existing TAR sidecar conflicts on {key}")
                artifact = conn.execute(
                    """UPDATE archive_artifacts
                       SET local_locator=%s, artifact_size_bytes=%s,
                           readiness_state='ready',
                           publication_started_at=COALESCE(
                               publication_started_at, now()),
                           published_at=now(), updated_at=now()
                       WHERE artifact_id=%s RETURNING *""",
                    (sidecar_locator, int(sidecar_size_bytes),
                     artifact["artifact_id"]),
                ).fetchone()

            container = conn.execute(
                """UPDATE archive_containers
                   SET temporary_data_locator=%s,
                       permanent_local_metadata_locator=%s,
                       actual_artifact_bytes=%s,
                       observed_member_count=%s, observed_logical_bytes=%s,
                       disposition_counts=%s::jsonb,
                       validation_state='ready', owner_token=NULL,
                       lease_expires_at=NULL, updated_at=now()
                   WHERE container_id=%s AND owner_token=%s
                     AND validation_state='validated_part'
                   RETURNING *""",
                (data_locator, sidecar_locator, int(tar_size_bytes),
                 int(observed_member_count), int(observed_logical_bytes),
                 counts_payload, int(container_id), token),
            ).fetchone()
            if container is None:
                raise RuntimeError("Stored TAR paired readiness CAS failed")
            return {"container": _row(container), "artifact": _row(artifact)}
        return self._transaction(operation, "publish ready Stored TAR pair")

    def find_container_restore_sidecars(self, container_ids, *, limit=100):
        """Return local TAR sidecar identities for an explicit container set.

        This is deliberately not a global per-file search.  It only narrows an
        already selected restore route and returns locators as data; it never
        opens either the local or tape locator.
        """
        ids = sorted({int(value) for value in container_ids
                      if value is not None})
        if not ids:
            return []
        limit = max(1, int(limit))
        if len(ids) > limit:
            raise ValueError(
                f"restore sidecar selection exceeds {limit} containers")
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT a.artifact_id, a.container_id, a.artifact_kind,
                          a.artifact_version, a.local_locator,
                          a.tape_locator, a.artifact_size_bytes,
                          a.readiness_state,
                          c.container_format, c.format_version,
                          c.tar_dialect, c.permanent_local_metadata_locator
                   FROM archive_artifacts a
                   JOIN archive_containers c
                     ON c.container_id=a.container_id
                   WHERE a.container_id = ANY(%s)
                     AND a.artifact_kind='tar_sidecar'
                     AND a.readiness_state='ready'
                   ORDER BY a.container_id, a.artifact_id DESC""",
                (ids,),
            ).fetchall()),
            f"find restore sidecars for {len(ids)} container(s)")

    def validate_staged_chunk_readiness(self, staged_chunk):
        """Match an identity-aware handoff to DB rows before LTFS ownership."""
        staged_chunk.assert_writer_ready()
        if staged_chunk.skip_tape:
            return True
        self.validate_container_format_schema()

        def operation(conn):
            chunk = conn.execute(
                """SELECT packaging_format FROM remote_chunks
                   WHERE session_id=%s AND chunk_index=%s""",
                (int(staged_chunk.session_id), int(staged_chunk.chunk_index)),
            ).fetchone()
            if (chunk is None or chunk["packaging_format"] !=
                    staged_chunk.packaging_format.value):
                raise RuntimeError(
                    "staged chunk disagrees with durable chunk format")

            containers = {
                int(row["container_id"]): row
                for row in conn.execute(
                    """SELECT * FROM archive_containers
                       WHERE session_id=%s AND chunk_index=%s""",
                    (int(staged_chunk.session_id),
                     int(staged_chunk.chunk_index)),
                ).fetchall()
            }
            expected_container_ids = {
                int(item.container_id) for item in staged_chunk.containers}
            if set(containers) != expected_container_ids:
                raise RuntimeError(
                    "staged container identities disagree with database")
            for item in staged_chunk.containers:
                row = containers[item.container_id]
                checks = {
                    "container_ordinal": item.container_ordinal,
                    "container_format": item.container_format.value,
                    "format_version": item.format_version,
                    "tar_dialect": item.tar_dialect,
                    "storage_class": item.storage_class,
                    "container_name": item.container_name,
                    "temporary_data_locator": item.temporary_data_locator,
                    "permanent_local_metadata_locator":
                        item.permanent_local_metadata_locator,
                    "expected_member_count": item.expected_member_count,
                    "expected_logical_bytes": item.expected_logical_bytes,
                    "observed_member_count": item.observed_member_count,
                    "observed_logical_bytes": item.observed_logical_bytes,
                    "actual_artifact_bytes": item.data_size_bytes,
                    "validation_state": ContainerValidationState.READY.value,
                    "writer_state": "not_started",
                    "catalog_state": "not_started",
                }
                mismatch = [
                    key for key, value in checks.items()
                    if row[key] != value]
                if mismatch:
                    raise RuntimeError(
                        f"container {item.container_id} DB mismatch: "
                        + ", ".join(mismatch))

            artifacts = {
                int(row["artifact_id"]): row
                for row in conn.execute(
                    """SELECT * FROM archive_artifacts
                       WHERE session_id=%s AND chunk_index=%s
                         AND readiness_state='ready'""",
                    (int(staged_chunk.session_id),
                     int(staged_chunk.chunk_index)),
                ).fetchall()
            }
            expected_artifact_ids = {
                int(item.artifact_id) for item in staged_chunk.artifacts}
            if set(artifacts) != expected_artifact_ids:
                raise RuntimeError(
                    "staged artifact identities disagree with database")
            for item in staged_chunk.artifacts:
                row = artifacts[item.artifact_id]
                checks = {
                    "container_id": item.container_id,
                    "artifact_kind": item.artifact_kind.value,
                    "artifact_version": item.artifact_version,
                    "local_locator": item.local_locator,
                    "artifact_size_bytes": item.staged_size_bytes,
                    "readiness_state": ArtifactReadiness.READY.value,
                }
                mismatch = [key for key, value in checks.items()
                            if row[key] != value]
                if mismatch:
                    raise RuntimeError(
                        f"artifact {item.artifact_id} DB mismatch: "
                        + ", ".join(mismatch))
            return True

        return self._run_read(
            operation, "validate staged container database readiness")
