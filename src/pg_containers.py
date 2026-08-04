"""Durable packaging-format, container and artifact repository (Plan 2).

This mixin is intentionally below :mod:`src.pg_db` and beside the other
PostgreSQL concern modules.  It owns the one authoritative new-chunk format
assignment seam and the identities handed from staging to the writer.  It never
opens a tape path and never infers a format from a filename extension.
"""
from .pipeline_types import (ArtifactReadiness, ContainerFormat,
                             ContainerValidationState)
from .pg_core import _row, _rows


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

    def get_archive_artifacts(self, session_id, chunk_index):
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT * FROM archive_artifacts
                   WHERE session_id=%s AND chunk_index=%s
                   ORDER BY artifact_id""",
                (int(session_id), int(chunk_index)),
            ).fetchall()),
            f"get artifacts for session {session_id}, chunk {chunk_index}")

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
                       WHERE session_id=%s AND chunk_index=%s""",
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
