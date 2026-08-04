"""Manifest-plan authority and the normalized directory catalog (Plan 3).

This mixin owns read-only migration-018 presence and authority validation, and
the directory completeness calculation of Task 2.2.  It never reads archive
media: completeness is decided entirely from persisted contributions, scan
coverage and local artifact evidence.  Migration application remains an explicit
operation on ``PgConnectionCore``.
"""
from collections import defaultdict

from .pg_core import _now_utc, _rows


MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION = 1


def _parent_path(path):
    """The canonical parent directory of a source path, POSIX-normalized."""
    text = str(path or "").replace("\\", "/").rstrip("/")
    head, _sep, _tail = text.rpartition("/")
    return head or "/"


class PgDirectoryCatalogMixin:
    """Read-only migration-018 validation and thin catalog accessors."""

    MANIFEST_DIRECTORY_CATALOG_TABLES = (
        "manifest_directory_catalog_schema_metadata",
        "remote_session_plan_transitions",
        "remote_chunk_plan_source_transitions",
        "archive_directories",
        "directory_scan_coverage",
        "directory_archive_parts",
        "directory_completeness",
    )
    MANIFEST_DIRECTORY_CATALOG_VIEW = "directory_catalog_status_v"
    MANIFEST_DIRECTORY_CATALOG_CHUNK_COLUMNS = (
        "plan_source",
        "plan_manifest_artifact_id",
        "terminal_manifest_artifact_id",
        "plan_ordinal_scope",
        "final_archived_count",
        "final_source_missing_count",
        "final_source_permission_denied_count",
        "final_source_unreadable_count",
        "final_source_changed_count",
        "final_unresolved_count",
        "final_archived_bytes",
    )
    MANIFEST_DIRECTORY_CATALOG_TRANSITION_COLUMNS = (
        "transition_id",
        "session_id",
        "transition_epoch",
        "state",
        "prior_plan_source",
        "new_plan_source",
        "last_chunk_before_transition",
        "first_chunk_after_transition",
        "scan_frontier_generation",
        "evidence_report_locator",
        "approval_identity",
        "approved_at",
        "created_at",
        "updated_at",
    )
    MANIFEST_DIRECTORY_CATALOG_CHUNK_TRANSITION_COLUMNS = (
        "chunk_plan_transition_id",
        "session_id",
        "chunk_index",
        "from_plan_source",
        "to_plan_source",
        "prior_plan_manifest_artifact_id",
        "new_plan_manifest_artifact_id",
        "plan4_gate_id",
        "plan4_evidence_id",
        "approval_identity",
        "equivalence_confirmed",
        "transitioned_at",
    )
    MANIFEST_DIRECTORY_CATALOG_INDEXES = (
        "uq_remote_session_plan_active_boundary",
        "ix_remote_session_plan_effective_boundary",
        "uq_remote_chunk_plan_source_transition",
        "uq_remote_chunk_plan_transition_artifact",
        "uq_remote_chunks_plan_manifest_artifact",
        "uq_remote_chunks_terminal_manifest_artifact",
        "ix_archive_directories_parent",
        "ix_directory_scan_coverage_session_generation",
        "uq_directory_archive_parts_container",
        "uq_directory_archive_parts_loose",
        "ix_directory_archive_parts_restore",
        "ix_directory_completeness_status",
        "uq_archive_artifacts_ready_plan_chunk",
    )
    MANIFEST_DIRECTORY_CATALOG_TRIGGERS = (
        "trg_archive_directories_parent_guard",
        "trg_directory_scan_coverage_scope_guard",
        "trg_directory_archive_parts_guard",
        "trg_remote_chunks_plan_source_guard",
        "trg_remote_chunks_manifest_authority_deferred",
        "trg_remote_session_plan_transitions_guard",
        "trg_remote_chunk_plan_source_transitions_immutable",
        "trg_archive_artifacts_sealed_immutable",
        "trg_manifest_directory_catalog_schema_metadata_immutable",
    )

    _OPTIONAL_007_TABLES = (
        "directory_archive_stats",
        "directory_archive_bundles",
        "directory_tree_index",
    )
    _OPTIONAL_012_TABLES = (
        "tape_write_batches",
        "tape_write_batch_chunks",
        "tape_write_active_chunk",
    )

    @staticmethod
    def _view_exists_conn(conn, view_name):
        return conn.execute(
            """SELECT 1 FROM information_schema.views
               WHERE table_schema='public' AND table_name=%s""",
            (view_name,),
        ).fetchone() is not None

    @staticmethod
    def _named_objects_conn(conn, catalog, name_column):
        return {
            row[name_column]
            for row in conn.execute(catalog).fetchall()
        }

    @classmethod
    def _optional_schema_state_conn(cls, conn, table_names):
        installed = [
            name for name in table_names
            if cls._table_exists_conn(conn, name)
        ]
        if not installed:
            return "absent"
        if len(installed) == len(table_names):
            return "installed"
        return "partial"

    def _manifest_directory_catalog_prerequisite_issues_conn(self, conn):
        """Check concrete 014-017 objects without using 015's fingerprint."""
        issues = []
        required_tables = (
            "remote_sessions",
            "remote_chunks",
            "remote_plan_files",
            "remote_snapshot_files",
            "remote_scan_scopes",
            "remote_worker_attempts",
            "tape_generations",
            "files_index",
            "container_format_schema_metadata",
            "remote_packaging_boundaries",
            "remote_packaging_boundary_chunks",
            "archive_containers",
            "archive_artifacts",
            "archive_container_members",
        )
        for table in required_tables:
            if not self._table_exists_conn(conn, table):
                issues.append(f"missing prerequisite table: {table}")

        required_columns = {
            "remote_chunks": (
                "session_id", "chunk_index", "status",
                "packaging_format", "stored_tar_max_size_bytes",
                "membership_state", "owner_token", "lease_expires_at",
                "attempt_id",
            ),
            "remote_sessions": (
                "session_id", "session_label", "plan_id",
            ),
            "archive_containers": (
                "estimated_archive_bytes", "validated_part_locator",
                "validation_summary", "disposition_counts",
            ),
            "archive_artifacts": (
                "artifact_id", "session_id", "chunk_index", "container_id",
                "artifact_kind", "readiness_state", "local_locator",
                "artifact_size_bytes", "published_at",
            ),
            "files_index": ("record_key",),
        }
        for table, columns in required_columns.items():
            if not self._table_exists_conn(conn, table):
                continue
            for column in columns:
                if not self._column_exists_conn(conn, table, column):
                    issues.append(
                        f"missing prerequisite column: {table}.{column}")

        finalized = conn.execute(
            """SELECT 1 FROM pg_indexes
               WHERE schemaname='public'
                 AND indexname='uq_remote_plan_files_chunk_ordinal'"""
        ).fetchone()
        if finalized is None:
            issues.append("migration 014 is not finalized")

        if self._table_exists_conn(
                conn, "container_format_schema_metadata"):
            metadata_count = int(conn.execute(
                "SELECT count(*) AS n FROM container_format_schema_metadata"
            ).fetchone()["n"])
            if metadata_count != 1:
                issues.append(
                    "migration 015 metadata does not contain exactly one "
                    "authority row")

        constraints = self._named_objects_conn(
            conn,
            """SELECT conname FROM pg_constraint c
               JOIN pg_namespace n ON n.oid=c.connamespace
               WHERE n.nspname='public'""",
            "conname",
        )
        for constraint in (
                "archive_container_members_container_ck",
                "archive_containers_observed_ck",
                "archive_containers_validated_part_ck",
                "archive_containers_ready_pair_ck",
                "uq_remote_sessions_label",
                "uq_files_record_key"):
            if constraint not in constraints:
                issues.append(
                    f"missing prerequisite constraint: {constraint}")

        if self._optional_schema_state_conn(
                conn, self._OPTIONAL_007_TABLES) == "partial":
            issues.append("migration 007 is partially installed")
        elif self._optional_schema_state_conn(
                conn, self._OPTIONAL_007_TABLES) == "installed":
            for column in (
                    "container_id", "container_format", "tape_generation_id",
                    "actual_artifact_bytes"):
                if not self._column_exists_conn(
                        conn, "directory_archive_bundles", column):
                    issues.append(
                        "missing migration-015 optional-007 compatibility "
                        f"column: directory_archive_bundles.{column}")
        if self._optional_schema_state_conn(
                conn, self._OPTIONAL_012_TABLES) == "partial":
            issues.append("migration 012 is partially installed")
        return issues

    def _manifest_authority_report_conn(self, conn):
        report = {
            "plan_source_counts": {},
            "manifest_chunk_count": 0,
            "invalid_manifest_chunk_count": 0,
            "shared_plan_artifact_reference_count": 0,
            "invalid_chunk_plan_source_transition_count": 0,
            "inconsistent_transition_boundary_count": 0,
            "inconsistent_effective_plan_source_count": 0,
            "duplicate_active_transition_boundary_count": 0,
            "authority_issues": [],
        }
        counts = conn.execute(
            """SELECT plan_source, count(*) AS n
               FROM remote_chunks GROUP BY plan_source
               ORDER BY plan_source NULLS FIRST"""
        ).fetchall()
        report["plan_source_counts"] = {
            (row["plan_source"] if row["plan_source"] is not None else
             "<null>"): int(row["n"])
            for row in counts
        }

        authority = conn.execute(
            """WITH manifest_authority AS (
                   SELECT c.session_id, c.chunk_index,
                          c.plan_manifest_artifact_id,
                          c.plan_ordinal_scope,
                          count(a.artifact_id) AS ready_plan_count,
                          count(a.artifact_id) FILTER (
                              WHERE a.artifact_id=
                                    c.plan_manifest_artifact_id)
                              AS referenced_ready_plan_count
                   FROM remote_chunks c
                   LEFT JOIN archive_artifacts a
                     ON a.session_id=c.session_id
                    AND a.chunk_index=c.chunk_index
                    AND a.container_id IS NULL
                    AND a.artifact_kind='plan_manifest'
                    AND a.readiness_state='ready'
                    AND a.local_locator IS NOT NULL
                    AND lower(right(a.local_locator, 5))<>'.part'
                    AND a.artifact_size_bytes IS NOT NULL
                    AND a.published_at IS NOT NULL
                   WHERE c.plan_source='manifest'
                   GROUP BY c.session_id, c.chunk_index,
                            c.plan_manifest_artifact_id,
                            c.plan_ordinal_scope
               )
               SELECT count(*) AS manifest_chunks,
                      count(*) FILTER (
                          WHERE ready_plan_count<>1
                             OR referenced_ready_plan_count<>1
                             OR plan_ordinal_scope<>'chunk') AS invalid
               FROM manifest_authority"""
        ).fetchone()
        report["manifest_chunk_count"] = int(
            authority["manifest_chunks"] or 0)
        report["invalid_manifest_chunk_count"] = int(
            authority["invalid"] or 0)

        shared = conn.execute(
            """SELECT count(*) AS n FROM (
                   SELECT plan_manifest_artifact_id
                   FROM remote_chunks
                   WHERE plan_manifest_artifact_id IS NOT NULL
                   GROUP BY plan_manifest_artifact_id
                   HAVING count(*) > 1
               ) duplicate_references"""
        ).fetchone()["n"]
        report["shared_plan_artifact_reference_count"] = int(shared or 0)

        invalid_audits = conn.execute(
            """SELECT count(*) AS n
               FROM remote_chunk_plan_source_transitions t
               LEFT JOIN remote_chunks c
                 ON c.session_id=t.session_id
                AND c.chunk_index=t.chunk_index
               LEFT JOIN archive_artifacts a
                 ON a.artifact_id=t.new_plan_manifest_artifact_id
               WHERE t.from_plan_source<>'legacy_db'
                  OR t.to_plan_source<>'manifest'
                  OR t.equivalence_confirmed IS NOT TRUE
                  OR nullif(t.plan4_gate_id, '') IS NULL
                  OR nullif(t.plan4_evidence_id, '') IS NULL
                  OR nullif(t.approval_identity, '') IS NULL
                  OR c.plan_source IS DISTINCT FROM 'manifest'
                  OR c.plan_manifest_artifact_id IS DISTINCT FROM
                     t.new_plan_manifest_artifact_id
                  OR a.session_id IS DISTINCT FROM t.session_id
                  OR a.chunk_index IS DISTINCT FROM t.chunk_index
                  OR a.artifact_kind IS DISTINCT FROM 'plan_manifest'
                  OR a.readiness_state IS DISTINCT FROM 'ready'
                  OR a.local_locator IS NULL
                  OR lower(right(a.local_locator, 5))='.part'
                  OR a.artifact_size_bytes IS NULL
                  OR a.published_at IS NULL"""
        ).fetchone()["n"]
        report["invalid_chunk_plan_source_transition_count"] = int(
            invalid_audits or 0)

        inconsistent = conn.execute(
            """WITH activated_epochs AS (
                   SELECT t.*,
                          lag(t.first_chunk_after_transition) OVER (
                              PARTITION BY t.session_id
                              ORDER BY t.transition_epoch)
                              AS previous_boundary,
                          lag(t.new_plan_source) OVER (
                              PARTITION BY t.session_id
                              ORDER BY t.transition_epoch)
                              AS previous_source
                   FROM remote_session_plan_transitions t
                   WHERE t.state IN ('active','rolled_back')
               )
               SELECT count(*) AS n
               FROM remote_session_plan_transitions t
               LEFT JOIN activated_epochs e
                 ON e.transition_id=t.transition_id
               WHERE NOT (
                         (t.last_chunk_before_transition IS NULL
                          AND t.first_chunk_after_transition=0)
                         OR t.first_chunk_after_transition=
                            t.last_chunk_before_transition + 1)
                  OR (t.state IN ('active','rolled_back')
                      AND e.previous_boundary IS NOT NULL
                      AND t.first_chunk_after_transition <
                          e.previous_boundary)
                  OR (t.state IN ('active','rolled_back')
                      AND t.prior_plan_source IS DISTINCT FROM
                          COALESCE(e.previous_source, 'legacy_db'))
                  OR (t.state IN ('active','rolled_back')
                      AND t.last_chunk_before_transition IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM remote_chunks c
                          WHERE c.session_id=t.session_id
                            AND c.chunk_index=
                                t.last_chunk_before_transition))
                  OR (t.state IN ('active','rolled_back')
                      AND EXISTS (
                          SELECT 1 FROM remote_chunks later
                          WHERE later.session_id=t.session_id
                            AND later.chunk_index >
                                t.first_chunk_after_transition)
                      AND NOT EXISTS (
                          SELECT 1 FROM remote_chunks first_chunk
                          WHERE first_chunk.session_id=t.session_id
                            AND first_chunk.chunk_index=
                                t.first_chunk_after_transition))"""
        ).fetchone()["n"]
        report["inconsistent_transition_boundary_count"] = int(
            inconsistent or 0)

        effective_mismatches = conn.execute(
            """WITH latest_boundary_epochs AS (
                   SELECT * FROM (
                       SELECT t.*,
                              row_number() OVER (
                                  PARTITION BY t.session_id,
                                               t.first_chunk_after_transition
                                  ORDER BY t.transition_epoch DESC) AS rank
                       FROM remote_session_plan_transitions t
                       WHERE t.state IN ('active','rolled_back')
                   ) ranked
                   WHERE rank=1
               ), chunk_expectations AS (
                   SELECT c.session_id, c.chunk_index, c.plan_source,
                          c.plan_manifest_artifact_id,
                          COALESCE((
                              SELECT b.new_plan_source
                              FROM latest_boundary_epochs b
                              WHERE b.session_id=c.session_id
                                AND b.first_chunk_after_transition <=
                                    c.chunk_index
                              ORDER BY b.first_chunk_after_transition DESC,
                                       b.transition_epoch DESC
                              LIMIT 1), 'legacy_db') AS expected_source
                   FROM remote_chunks c
               )
               SELECT count(*) AS n
               FROM chunk_expectations c
               WHERE c.plan_source IS DISTINCT FROM c.expected_source
                 AND NOT (
                     c.expected_source='legacy_db'
                     AND c.plan_source='manifest'
                     AND EXISTS (
                         SELECT 1
                         FROM remote_chunk_plan_source_transitions a
                         WHERE a.session_id=c.session_id
                           AND a.chunk_index=c.chunk_index
                           AND a.from_plan_source='legacy_db'
                           AND a.to_plan_source='manifest'
                           AND a.equivalence_confirmed
                           AND a.new_plan_manifest_artifact_id=
                               c.plan_manifest_artifact_id))"""
        ).fetchone()["n"]
        report["inconsistent_effective_plan_source_count"] = int(
            effective_mismatches or 0)

        duplicate_boundaries = conn.execute(
            """SELECT COALESCE(sum(n - 1), 0) AS n FROM (
                   SELECT count(*) AS n
                   FROM remote_session_plan_transitions
                   WHERE state='active'
                   GROUP BY session_id, first_chunk_after_transition
                   HAVING count(*) > 1
               ) duplicates"""
        ).fetchone()["n"]
        report["duplicate_active_transition_boundary_count"] = int(
            duplicate_boundaries or 0)

        issue_fields = (
            ("invalid_manifest_chunk_count",
             "manifest chunk(s) lack exactly one referenced ready plan "
             "artifact"),
            ("shared_plan_artifact_reference_count",
             "plan artifact(s) are shared by more than one chunk"),
            ("invalid_chunk_plan_source_transition_count",
             "audited chunk authority transition(s) are inconsistent"),
            ("inconsistent_transition_boundary_count",
             "session plan transition boundary/boundaries are inconsistent"),
            ("inconsistent_effective_plan_source_count",
             "chunk(s) conflict with the effective transition source without "
             "an audited authority override"),
            ("duplicate_active_transition_boundary_count",
             "duplicate active transition boundary/boundaries exist"),
        )
        for key, message in issue_fields:
            if report[key]:
                report["authority_issues"].append(
                    f"{report[key]} {message}")
        return report

    def _manifest_directory_catalog_schema_report_conn(self, conn):
        report = {
            "database": conn.execute(
                "SELECT current_database() AS db").fetchone()["db"],
            "expected_schema_version":
                MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION,
            "expected_migration_checksum":
                self.manifest_directory_catalog_migration_checksum(),
            "metadata": None,
            "installation_state": "absent",
            "optional_007_state": self._optional_schema_state_conn(
                conn, self._OPTIONAL_007_TABLES),
            "optional_012_state": self._optional_schema_state_conn(
                conn, self._OPTIONAL_012_TABLES),
            "schema_issues": [],
            "prerequisite_issues": [],
            "authority_issues": [],
            "issues": [],
            "apply_blocking": [],
            "schema_ready": False,
            "ready": False,
            "plan_source_counts": {},
            "manifest_chunk_count": 0,
            "invalid_manifest_chunk_count": 0,
            "shared_plan_artifact_reference_count": 0,
            "invalid_chunk_plan_source_transition_count": 0,
            "inconsistent_transition_boundary_count": 0,
            "inconsistent_effective_plan_source_count": 0,
            "duplicate_active_transition_boundary_count": 0,
        }
        schema_issues = report["schema_issues"]
        report["prerequisite_issues"] = (
            self._manifest_directory_catalog_prerequisite_issues_conn(conn))

        for table in self.MANIFEST_DIRECTORY_CATALOG_TABLES:
            if not self._table_exists_conn(conn, table):
                schema_issues.append(f"missing table: {table}")
        if not self._view_exists_conn(
                conn, self.MANIFEST_DIRECTORY_CATALOG_VIEW):
            schema_issues.append(
                f"missing view: {self.MANIFEST_DIRECTORY_CATALOG_VIEW}")

        column_groups = {
            "remote_chunks":
                self.MANIFEST_DIRECTORY_CATALOG_CHUNK_COLUMNS,
            "remote_session_plan_transitions":
                self.MANIFEST_DIRECTORY_CATALOG_TRANSITION_COLUMNS,
            "remote_chunk_plan_source_transitions":
                self.MANIFEST_DIRECTORY_CATALOG_CHUNK_TRANSITION_COLUMNS,
        }
        for table, columns in column_groups.items():
            if not self._table_exists_conn(conn, table):
                continue
            for column in columns:
                if not self._column_exists_conn(conn, table, column):
                    schema_issues.append(f"missing column: {table}.{column}")

        indexes = self._named_objects_conn(
            conn,
            "SELECT indexname FROM pg_indexes WHERE schemaname='public'",
            "indexname",
        )
        for index in self.MANIFEST_DIRECTORY_CATALOG_INDEXES:
            if index not in indexes:
                schema_issues.append(f"missing index: {index}")
        triggers = self._named_objects_conn(
            conn,
            """SELECT t.tgname FROM pg_trigger t
               JOIN pg_class c ON c.oid=t.tgrelid
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='public' AND NOT t.tgisinternal""",
            "tgname",
        )
        for trigger in self.MANIFEST_DIRECTORY_CATALOG_TRIGGERS:
            if trigger not in triggers:
                schema_issues.append(f"missing trigger: {trigger}")

        fingerprint_proc = conn.execute(
            """SELECT to_regprocedure(
                   'public.lto_manifest_directory_catalog_schema_fingerprint()')
                   AS proc"""
        ).fetchone()["proc"]
        if fingerprint_proc is None:
            schema_issues.append(
                "missing function: migration-018 schema fingerprint")

        marker_count = sum(
            self._table_exists_conn(conn, table)
            for table in self.MANIFEST_DIRECTORY_CATALOG_TABLES)
        marker_count += int(self._view_exists_conn(
            conn, self.MANIFEST_DIRECTORY_CATALOG_VIEW))
        marker_count += sum(
            self._column_exists_conn(conn, "remote_chunks", column)
            for column in self.MANIFEST_DIRECTORY_CATALOG_CHUNK_COLUMNS)
        marker_count += sum(index in indexes
                            for index in self.MANIFEST_DIRECTORY_CATALOG_INDEXES)
        marker_count += sum(trigger in triggers
                            for trigger in self.MANIFEST_DIRECTORY_CATALOG_TRIGGERS)
        marker_count += int(fingerprint_proc is not None)

        metadata_table = self._table_exists_conn(
            conn, "manifest_directory_catalog_schema_metadata")
        if metadata_table:
            metadata_rows = conn.execute(
                "SELECT * FROM manifest_directory_catalog_schema_metadata"
            ).fetchall()
            if len(metadata_rows) == 1:
                metadata = dict(metadata_rows[0])
                report["metadata"] = metadata
                report["installation_state"] = "installed"
                expected = {
                    "schema_version":
                        MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION,
                    "migration_checksum":
                        report["expected_migration_checksum"],
                }
                for key, value in expected.items():
                    if metadata.get(key) != value:
                        schema_issues.append(
                            f"migration 018 metadata drift: {key} is "
                            f"{metadata.get(key)!r}, expected {value!r}")
                fingerprint_queryable = (
                    fingerprint_proc is not None
                    and all(self._table_exists_conn(conn, table)
                            for table in
                            self.MANIFEST_DIRECTORY_CATALOG_TABLES)
                    and self._view_exists_conn(
                        conn, self.MANIFEST_DIRECTORY_CATALOG_VIEW))
                if fingerprint_queryable:
                    actual = conn.execute(
                        """SELECT
                           lto_manifest_directory_catalog_schema_fingerprint()
                           AS fingerprint"""
                    ).fetchone()["fingerprint"]
                    report["actual_schema_fingerprint"] = actual
                    if metadata.get("schema_fingerprint") != actual:
                        schema_issues.append(
                            "migration 018 catalog-definition fingerprint "
                            "drift")
            else:
                report["installation_state"] = "partial"
                schema_issues.append(
                    "migration 018 metadata does not contain exactly one "
                    "authority row")
        elif marker_count:
            report["installation_state"] = "partial"

        structurally_queryable = (
            report["installation_state"] == "installed"
            and not report["prerequisite_issues"]
            and not any(issue.startswith(("missing table:",
                                          "missing column:"))
                        for issue in schema_issues))
        if structurally_queryable:
            authority = self._manifest_authority_report_conn(conn)
            for key, value in authority.items():
                report[key] = value

        report["schema_ready"] = bool(
            report["installation_state"] == "installed"
            and not schema_issues
            and not report["prerequisite_issues"])
        report["issues"] = (
            list(schema_issues)
            + list(report["prerequisite_issues"])
            + list(report["authority_issues"]))
        report["ready"] = bool(
            report["schema_ready"] and not report["authority_issues"])

        apply_blocking = report["apply_blocking"]
        apply_blocking.extend(report["prerequisite_issues"])
        if report["installation_state"] == "partial":
            apply_blocking.append(
                "migration 018 is partially installed")
        elif (report["installation_state"] == "installed"
              and not report["ready"]):
            apply_blocking.extend(schema_issues)
            apply_blocking.extend(report["authority_issues"])
        report["apply_blocking"] = list(dict.fromkeys(apply_blocking))
        return report

    def manifest_directory_catalog_schema_report(self):
        """Return exact schema plus manifest/transition authority evidence."""
        return self._run_read(
            self._manifest_directory_catalog_schema_report_conn,
            "manifest directory catalog schema report")

    def manifest_directory_catalog_schema_installed(self):
        """True only when migration 018 and its live authority checks pass."""
        return bool(self.manifest_directory_catalog_schema_report()["ready"])

    def validate_manifest_directory_catalog_schema(self):
        """Fail closed on schema drift or inconsistent manifest authority."""
        report = self.manifest_directory_catalog_schema_report()
        if not report["ready"]:
            raise RuntimeError(
                "[DB] Manifest directory-catalog schema/authority is missing "
                "or inconsistent: " + "; ".join(report["issues"]))
        return report

    def _require_manifest_directory_catalog_schema(self):
        return self.validate_manifest_directory_catalog_schema()

    def transition_chunk_plan_source(
            self, session_id, chunk_index, plan_manifest_artifact_id,
            plan4_gate_id, plan4_evidence_id, approval_identity):
        """Run the sole atomic terminal ``legacy_db`` to ``manifest`` move."""
        session_id = int(session_id)
        chunk_index = int(chunk_index)
        plan_manifest_artifact_id = int(plan_manifest_artifact_id)
        identity_values = (
            plan4_gate_id, plan4_evidence_id, approval_identity)
        identities = tuple(
            "" if value is None else str(value).strip()
            for value in identity_values)
        if (session_id <= 0 or chunk_index < 0
                or plan_manifest_artifact_id <= 0
                or any(not value for value in identities)):
            raise ValueError(
                "chunk plan transition requires valid chunk/artifact, Plan 4 "
                "gate/evidence, and approval identities")
        self._require_manifest_directory_catalog_schema()

        def operation(conn):
            row = conn.execute(
                """SELECT lto_transition_chunk_plan_source(
                           %s, %s, %s, %s, %s, %s) AS transition_id""",
                (session_id, chunk_index, plan_manifest_artifact_id,
                 *identities),
            ).fetchone()
            return int(row["transition_id"])

        return self._transaction(
            operation,
            f"transition plan authority for session {session_id}, "
            f"chunk {chunk_index}")

    # ------------------------------------------------------------------
    # Task 2.3 - legacy ZIP, new TAR and loose contributions, one table
    # ------------------------------------------------------------------

    def ingest_legacy_directory_parts(self, session_id, *,
                                      evidence_generation=1, dry_run=False):
        """Fold legacy evidence into ``directory_archive_parts``.

        Two kinds of legacy evidence behave very differently, and conflating
        them is how a coarse guess gets treated as proof:

        **Loose large files** (``files_index`` rows) carry their own
        ``record_key``, tape label and stored path, so they become real loose
        parts with ``routing_precision='exact'`` when their remote provenance is
        proven, and ``'coarse'`` when it is not.

        **Legacy ZIP bundles** usually cannot become parts at all. A part must
        name either a container (a foreign key into ``archive_containers``) or a
        catalog record, and migration 015 links ``directory_archive_bundles``
        to a container only where one genuinely exists - historical bundles
        predate the container registry entirely. Those are reported as **coarse
        candidates**: they stay readable through the legacy tables, they block
        anything that needs exact routing, and they never trigger a tape read to
        "resolve" themselves.

        Returns a report; with ``dry_run`` nothing is written.
        """
        self._require_manifest_directory_catalog_schema()
        now = _now_utc()

        def operation(conn):
            report = {"loose_parts": 0, "container_parts": 0,
                      "coarse_bundle_candidates": 0, "skipped_no_directory": 0,
                      "dry_run": bool(dry_run)}

            directories = {
                (row["source_host"], row["canonical_path"]):
                    row["directory_id"]
                for row in conn.execute(
                    "SELECT directory_id, source_host, canonical_path "
                    "FROM archive_directories").fetchall()}

            # -- loose large files -------------------------------------
            for row in conn.execute(
                    """SELECT fi.record_key, fi.original_path,
                              fi.file_size_bytes, fi.tape_label,
                              fi.stored_path, fi.source_host,
                              fi.remote_chunk_index,
                              tg.generation_id
                       FROM files_index fi
                       LEFT JOIN tape_generations tg
                         ON tg.volume_label=fi.tape_label
                        AND tg.state='active'
                       WHERE fi.remote_session_id=%s
                         AND NOT fi.is_packed
                         AND fi.remote_chunk_index IS NOT NULL""",
                    (session_id,)).fetchall():
                parent = _parent_path(row["original_path"])
                directory_id = directories.get((row["source_host"], parent))
                if directory_id is None:
                    report["skipped_no_directory"] += 1
                    continue
                if row["generation_id"] is None:
                    # Without a generation the restore route is incomplete, so
                    # the contribution is a coarse candidate rather than a part.
                    report["coarse_bundle_candidates"] += 1
                    continue
                report["loose_parts"] += 1
                if dry_run:
                    continue
                conn.execute(
                    """INSERT INTO directory_archive_parts
                           (directory_id, session_id, chunk_index,
                            loose_record_key, tape_generation_id,
                            storage_class, evidence_generation,
                            direct_expected_count, direct_expected_bytes,
                            direct_archived_count, direct_archived_bytes,
                            local_validation_state, writer_state,
                            catalog_state, restore_format, tape_label,
                            stored_path, source_base_path, routing_precision,
                            created_at, updated_at)
                       VALUES (%s,%s,%s,%s,%s,'loose',%s,1,%s,1,%s,
                               'succeeded','copied','committed','loose',%s,%s,
                               %s,'exact',%s,%s)
                       ON CONFLICT DO NOTHING""",
                    (directory_id, session_id, row["remote_chunk_index"],
                     row["record_key"], row["generation_id"],
                     int(evidence_generation), int(row["file_size_bytes"]),
                     int(row["file_size_bytes"]), row["tape_label"],
                     row["stored_path"], parent, now, now))

            # -- legacy ZIP bundles ------------------------------------
            if self._table_exists_conn(conn, "directory_archive_bundles"):
                has_container = self._column_exists_conn(
                    conn, "directory_archive_bundles", "container_id")
                container_select = ("dab.container_id" if has_container
                                    else "NULL::BIGINT AS container_id")
                for row in conn.execute(
                        f"""SELECT dab.bundle_id, dab.source_host,
                                   dab.original_dir_path, dab.tape_label,
                                   dab.chunk_index, dab.stored_bundle_path,
                                   dab.file_count, dab.byte_count,
                                   {container_select}
                            FROM directory_archive_bundles dab
                            WHERE dab.remote_session_id=%s""",
                        (session_id,)).fetchall():
                    if row["container_id"] is None or row["chunk_index"] is None:
                        # No container identity, or no proven chunk: this is
                        # exactly the historical evidence the plan says to keep
                        # as a coarse candidate rather than promote to a part.
                        report["coarse_bundle_candidates"] += 1
                        continue
                    directory_id = directories.get(
                        (row["source_host"],
                         str(row["original_dir_path"]).replace("\\", "/")
                         .rstrip("/")))
                    if directory_id is None:
                        report["skipped_no_directory"] += 1
                        continue
                    report["container_parts"] += 1
                    if dry_run:
                        continue
                    conn.execute(
                        """INSERT INTO directory_archive_parts
                               (directory_id, session_id, chunk_index,
                                container_id, tape_generation_id,
                                storage_class, evidence_generation,
                                direct_expected_count, direct_expected_bytes,
                                direct_archived_count, direct_archived_bytes,
                                local_validation_state, writer_state,
                                catalog_state, restore_format, tape_label,
                                stored_path, source_base_path,
                                container_member_candidate, routing_precision,
                                created_at, updated_at)
                           SELECT %s,%s,%s,%s,c.tape_generation_id,'container',
                                  %s,%s,%s,%s,%s,'succeeded','copied',
                                  'committed',c.container_format,c.tape_label,
                                  c.tape_path,%s,NULL,'coarse',%s,%s
                           FROM archive_containers c
                           WHERE c.container_id=%s
                           ON CONFLICT DO NOTHING""",
                        (directory_id, session_id, row["chunk_index"],
                         row["container_id"], int(evidence_generation),
                         int(row["file_count"] or 0),
                         int(row["byte_count"] or 0),
                         int(row["file_count"] or 0),
                         int(row["byte_count"] or 0),
                         str(row["original_dir_path"]), now, now,
                         row["container_id"]))
            return report

        if dry_run:
            return self._run_read(
                operation, f"legacy directory parts dry run for {session_id}")
        return self._transaction(
            operation, f"ingest legacy directory parts for {session_id}")

    def recalculate_directory_completeness(self, session_id, *,
                                           directory_id=None):
        """Recompute persisted completeness from one pinned generation.

        Plan 3, Task 2.2. Everything happens in ONE transaction against ONE
        pinned frontier/artifact high-water generation: counts, recursive
        aggregates, the seven independent booleans, and the derived status. A
        recomputation that read half its evidence before a concurrent write and
        half after could publish a status that was never simultaneously true.

        Idempotent. Any missing generation, artifact or contribution degrades
        the result to ``incomplete``/``ambiguous`` - never to ``complete``.
        """
        from .directory_status import evidence_from_parts, resolve_directory_status

        self._require_manifest_directory_catalog_schema()
        now = _now_utc()

        def operation(conn):
            # Pin the generation FIRST; every later read in this transaction is
            # filtered by it, so the whole calculation describes one instant.
            pinned = conn.execute(
                """SELECT COALESCE(MAX(evidence_generation), 0) AS gen
                   FROM directory_archive_parts WHERE session_id=%s""",
                (session_id,)).fetchone()["gen"]
            frontier_generation = conn.execute(
                """SELECT COALESCE(MAX(frontier_generation), 0) AS gen
                   FROM directory_scan_coverage WHERE session_id=%s""",
                (session_id,)).fetchone()["gen"]

            where = "WHERE p.session_id=%s AND p.evidence_generation<=%s"
            params = [session_id, pinned]
            if directory_id is not None:
                where += " AND p.directory_id=%s"
                params.append(int(directory_id))

            parts_by_directory = defaultdict(list)
            for row in conn.execute(
                    f"""SELECT p.* FROM directory_archive_parts p {where}
                        ORDER BY p.directory_id, p.part_id""",
                    tuple(params)).fetchall():
                parts_by_directory[row["directory_id"]].append(dict(row))

            coverage_where = "WHERE c.session_id=%s"
            coverage_params = [session_id]
            if directory_id is not None:
                coverage_where += " AND c.directory_id=%s"
                coverage_params.append(int(directory_id))
            coverage = {}
            for row in conn.execute(
                    f"""SELECT c.directory_id, c.coverage_state
                        FROM directory_scan_coverage c {coverage_where}""",
                    tuple(coverage_params)).fetchall():
                # A directory with several scopes is final only when EVERY
                # scope covering it is final.
                previous = coverage.get(row["directory_id"])
                state = row["coverage_state"]
                coverage[row["directory_id"]] = (
                    state if previous is None
                    else ("final" if previous == "final" and state == "final"
                          else ("error" if "error" in (previous, state)
                                else "provisional")))

            directories = sorted(set(parts_by_directory) | set(coverage))

            # Recursive aggregates: a directory's totals include every
            # descendant's, so an ancestor reports the whole subtree. Rolled up
            # depth-first (deepest first) so each node is added into its parent
            # exactly once.
            tree = {row["directory_id"]: (row["parent_directory_id"],
                                          int(row["depth"] or 0))
                    for row in conn.execute(
                        "SELECT directory_id, parent_directory_id, depth "
                        "FROM archive_directories").fetchall()}
            direct_totals = {}
            for directory in directories:
                parts = parts_by_directory.get(directory, [])
                direct_totals[directory] = {
                    "files": sum(int(p.get("direct_expected_count") or 0)
                                 for p in parts),
                    "bytes": sum(int(p.get("direct_expected_bytes") or 0)
                                 for p in parts),
                    "archived_files": sum(
                        int(p.get("direct_archived_count") or 0)
                        for p in parts),
                    "archived_bytes": sum(
                        int(p.get("direct_archived_bytes") or 0)
                        for p in parts),
                }
            recursive = {d: dict(v) for d, v in direct_totals.items()}
            for directory in sorted(
                    directories, key=lambda d: tree.get(d, (None, 0))[1],
                    reverse=True):
                parent = tree.get(directory, (None, 0))[0]
                if parent is None or parent not in recursive:
                    continue
                for key, value in recursive[directory].items():
                    recursive[parent][key] += value

            written = 0
            for directory in directories:
                parts = parts_by_directory.get(directory, [])
                state = coverage.get(directory)
                evidence = evidence_from_parts(
                    parts,
                    scan_is_final=(state == "final"),
                    # No coverage row at all means the scan never described
                    # this directory: that is missing evidence, not finality.
                    missing_evidence=(state is None or state == "error"))
                status, _reason = resolve_directory_status(evidence)
                totals = recursive.get(directory, direct_totals[directory])
                conn.execute(
                    """INSERT INTO directory_completeness
                           (session_id, directory_id,
                            direct_expected_file_count, direct_expected_bytes,
                            direct_archived_file_count, direct_archived_bytes,
                            direct_source_missing_count,
                            direct_source_permission_denied_count,
                            direct_source_unreadable_count,
                            direct_source_changed_count,
                            direct_unresolved_count,
                            recursive_expected_file_count,
                            recursive_expected_bytes,
                            recursive_archived_file_count,
                            recursive_archived_bytes,
                            recursive_source_missing_count,
                            recursive_source_permission_denied_count,
                            recursive_source_unreadable_count,
                            recursive_source_changed_count,
                            recursive_unresolved_count,
                            scan_is_final, all_planned_items_terminal,
                            all_required_items_archived, all_parts_written,
                            all_writer_completions_succeeded,
                            all_parts_cataloged,
                            all_local_validation_succeeded,
                            has_ambiguous_evidence, status,
                            pinned_frontier_generation,
                            pinned_artifact_evidence_generation,
                            calculated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s)
                       ON CONFLICT (session_id, directory_id) DO UPDATE SET
                            direct_expected_file_count=
                                EXCLUDED.direct_expected_file_count,
                            direct_expected_bytes=EXCLUDED.direct_expected_bytes,
                            direct_archived_file_count=
                                EXCLUDED.direct_archived_file_count,
                            direct_archived_bytes=EXCLUDED.direct_archived_bytes,
                            direct_source_missing_count=
                                EXCLUDED.direct_source_missing_count,
                            direct_source_permission_denied_count=
                                EXCLUDED.direct_source_permission_denied_count,
                            direct_source_unreadable_count=
                                EXCLUDED.direct_source_unreadable_count,
                            direct_source_changed_count=
                                EXCLUDED.direct_source_changed_count,
                            direct_unresolved_count=
                                EXCLUDED.direct_unresolved_count,
                            recursive_expected_file_count=
                                EXCLUDED.recursive_expected_file_count,
                            recursive_expected_bytes=
                                EXCLUDED.recursive_expected_bytes,
                            recursive_archived_file_count=
                                EXCLUDED.recursive_archived_file_count,
                            recursive_archived_bytes=
                                EXCLUDED.recursive_archived_bytes,
                            recursive_source_missing_count=
                                EXCLUDED.recursive_source_missing_count,
                            recursive_source_permission_denied_count=
                                EXCLUDED.recursive_source_permission_denied_count,
                            recursive_source_unreadable_count=
                                EXCLUDED.recursive_source_unreadable_count,
                            recursive_source_changed_count=
                                EXCLUDED.recursive_source_changed_count,
                            recursive_unresolved_count=
                                EXCLUDED.recursive_unresolved_count,
                            scan_is_final=EXCLUDED.scan_is_final,
                            all_planned_items_terminal=
                                EXCLUDED.all_planned_items_terminal,
                            all_required_items_archived=
                                EXCLUDED.all_required_items_archived,
                            all_parts_written=EXCLUDED.all_parts_written,
                            all_writer_completions_succeeded=
                                EXCLUDED.all_writer_completions_succeeded,
                            all_parts_cataloged=EXCLUDED.all_parts_cataloged,
                            all_local_validation_succeeded=
                                EXCLUDED.all_local_validation_succeeded,
                            has_ambiguous_evidence=
                                EXCLUDED.has_ambiguous_evidence,
                            status=EXCLUDED.status,
                            pinned_frontier_generation=
                                EXCLUDED.pinned_frontier_generation,
                            pinned_artifact_evidence_generation=
                                EXCLUDED.pinned_artifact_evidence_generation,
                            calculated_at=EXCLUDED.calculated_at""",
                    (session_id, directory,
                     evidence.expected_count,
                     sum(int(p.get("direct_expected_bytes") or 0)
                         for p in parts),
                     evidence.archived_count,
                     sum(int(p.get("direct_archived_bytes") or 0)
                         for p in parts),
                     evidence.source_missing_count,
                     evidence.source_permission_denied_count,
                     evidence.source_unreadable_count,
                     evidence.source_changed_count,
                     evidence.unresolved_count,
                     totals["files"], totals["bytes"],
                     totals["archived_files"], totals["archived_bytes"],
                     evidence.source_missing_count,
                     evidence.source_permission_denied_count,
                     evidence.source_unreadable_count,
                     evidence.source_changed_count,
                     evidence.unresolved_count,
                     evidence.scan_is_final,
                     evidence.all_planned_items_terminal,
                     evidence.all_required_items_archived,
                     evidence.all_parts_written,
                     evidence.all_writer_completions_succeeded,
                     evidence.all_parts_cataloged,
                     evidence.all_local_validation_succeeded,
                     evidence.has_ambiguous_evidence,
                     status.value, frontier_generation, pinned, now),
                )
                written += 1
            return {"directories": written,
                    "pinned_artifact_evidence_generation": int(pinned),
                    "pinned_frontier_generation": int(frontier_generation)}

        return self._transaction(
            operation, f"recalculate directory completeness for {session_id}")

    def get_directory_status(self, session_id, directory_id):
        """The persisted status of one directory, or ``None``."""
        self._require_manifest_directory_catalog_schema()

        def operation(conn):
            row = conn.execute(
                """SELECT * FROM directory_completeness
                   WHERE session_id=%s AND directory_id=%s""",
                (session_id, int(directory_id))).fetchone()
            return dict(row) if row else None

        return self._run_read(
            operation, f"read directory status {session_id}/{directory_id}")

    def get_directory_catalog_status(self, *, session_id=None,
                                     directory_id=None):
        """Read normalized directory status rows without calculating state."""
        self._require_manifest_directory_catalog_schema()
        clauses = []
        params = []
        if session_id is not None:
            clauses.append("session_id=%s")
            params.append(int(session_id))
        if directory_id is not None:
            clauses.append("directory_id=%s")
            params.append(int(directory_id))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        def operation(conn):
            return _rows(conn.execute(
                "SELECT * FROM directory_catalog_status_v" + where
                + " ORDER BY source_host, canonical_path, session_id",
                tuple(params),
            ).fetchall())

        return self._run_read(operation, "read directory catalog status")
