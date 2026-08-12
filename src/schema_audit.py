"""Read-only installed-schema and catalog-provenance audit.

This module deliberately does not use :class:`PgDatabaseManager`'s normal
constructor.  The normal constructor applies startup migrations, which would
make a schema-provenance report change the evidence it is meant to describe.
Callers must provide an already-open read-only connection (or use
``audit_manager`` with ``init_schema=False``).

The audit is intentionally conservative.  Missing optional migrations and
unverified locators are reported as ``unavailable``; they are never turned into
synthetic evidence.  No tape path is opened or inspected.
"""

from __future__ import annotations

import ast
import hashlib
import os
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from .constants import PROJECT_ROOT


AUDIT_MIGRATIONS = (
    "001_postgres_schema.sql",
    "002_postgres_indexes.sql",
    "003_postgres_constraints.sql",
    "004_postgres_archive_runs_sessions.sql",
    "005_postgres_session_label_unique.sql",
    "006_postgres_remote_streaming.sql",
    "007_postgres_directory_catalog.sql",
    "008_postgres_remote_provenance.sql",
    "009_postgres_remote_session_fk.sql",
    "010_postgres_local_manifest_archive.sql",
    "011_postgres_tape_status.sql",
    "012_sealed_tape_write_batches.sql",
    "013_postgres_tape_reset_safety.sql",
    "014_postgres_incremental_scan.sql",
    "014_postgres_incremental_scan_finalize.sql",
    "015_postgres_container_formats.sql",
    "016_postgres_stored_tar_plans.sql",
    "017_postgres_stored_tar_publication.sql",
)

BASE_TABLES = (
    "tapes", "catalog_directories", "archive_bundles", "archive_runs",
    "local_sessions", "local_chunks_manifest", "remote_snapshots",
    "remote_snapshot_files", "remote_plans", "remote_sessions",
    "remote_plan_files", "remote_chunks", "remote_file_state", "files_index",
    "local_manifest_exports", "local_manifest_export_rows",
    "local_manifest_segments", "local_manifest_folder_aggregates",
    "local_manifest_catalog_aggregates", "tape_reset_operations",
    "tape_generations",
)
OPTIONAL_TABLES = {
    "007": (
        "directory_archive_stats", "directory_archive_bundles",
        "directory_tree_index",
    ),
    "012": (
        "schema_migrations", "tape_write_batches", "tape_write_batch_chunks",
        "tape_write_active_chunk",
    ),
    "014": (
        "remote_scan_scopes", "remote_scan_directories",
        "remote_scan_segments", "remote_chunk_scan_segments",
        "remote_scan_errors", "remote_worker_attempts",
        "remote_frontier_bootstraps",
    ),
    "015": (
        "container_format_schema_metadata", "remote_packaging_boundaries",
        "remote_packaging_boundary_chunks", "archive_containers",
        "archive_artifacts",
    ),
    "016": ("archive_container_members",),
}

REQUIRED_COLUMNS = {
    "catalog_directories": ("directory_id", "tape_label", "normalized_path"),
    "archive_bundles": (
        "bundle_id", "tape_label", "tape_path", "container_id",
        "container_format",
    ),
    "archive_runs": (
        "run_id", "tape_label", "remote_session_id", "remote_chunk_index",
        "tape_generation_id",
    ),
    "remote_sessions": (
        "session_id", "remote_path", "staging_dir", "default_packaging_format",
    ),
    "remote_plan_files": ("plan_id", "snapshot_file_id", "chunk_index", "ordinal"),
    "remote_chunks": (
        "session_id", "chunk_index", "status", "membership_state",
        "packaging_format", "packaging_assigned_at", "writer_started_at",
        "writer_completed_at", "catalog_committed_at",
    ),
    "files_index": (
        "file_id", "original_path", "file_size_bytes", "tape_label",
        "stored_path", "bundle_id", "record_key", "archive_run_id",
        "remote_session_id", "remote_chunk_index",
    ),
    "directory_archive_bundles": (
        "bundle_id", "original_dir_path", "tape_label", "stored_bundle_path",
        "remote_session_id", "chunk_index", "file_count", "byte_count",
        "record_key",
    ),
    "directory_archive_stats": (
        "stat_id", "original_dir_path", "tape_label", "remote_session_id",
        "chunk_index", "direct_file_count", "recursive_file_count",
        "direct_bytes", "recursive_bytes", "record_key",
    ),
    "directory_tree_index": (
        "dir_id", "original_dir_path", "parent_original_dir_path", "tape_label",
        "remote_session_id", "chunk_index", "bundle_id", "direct_file_count",
        "recursive_file_count", "direct_bytes", "recursive_bytes", "record_key",
    ),
    "remote_scan_scopes": ("scan_scope_id", "session_id", "root_path", "coverage_state"),
    "remote_scan_segments": ("scan_segment_id", "scan_directory_id", "locator", "state", "file_count", "byte_count"),
    "remote_chunk_scan_segments": (
        "chunk_segment_id", "session_id", "chunk_index", "scan_segment_id",
        "first_scan_ordinal", "last_scan_ordinal",
    ),
    "archive_containers": (
        "container_id", "session_id", "chunk_index", "container_ordinal",
        "container_format", "container_name", "temporary_data_locator",
        "permanent_local_metadata_locator", "tape_label", "tape_path",
        "tape_generation_id", "expected_member_count", "expected_logical_bytes",
        "validation_state", "writer_state", "catalog_state",
        "stored_tar_max_size_bytes", "estimated_archive_bytes",
        "validated_part_locator", "validation_summary", "disposition_counts",
    ),
    "archive_artifacts": (
        "artifact_id", "session_id", "chunk_index", "container_id",
        "artifact_kind", "local_locator", "tape_locator", "readiness_state",
    ),
    "archive_container_members": (
        "session_id", "chunk_index", "plan_file_id", "plan_ordinal",
        "storage_class", "container_id", "container_ordinal", "remote_path",
        "expected_logical_bytes",
    ),
    "container_format_schema_metadata": (
        "singleton", "schema_version", "required_reader_contract_version",
        "stored_tar_dialect", "migration_checksum", "schema_fingerprint",
        "applied_at",
    ),
    "remote_packaging_boundaries": (
        "session_id", "first_stored_tar_chunk_index", "last_existing_chunk_index",
        "approval_id", "approval_reason", "database_evidence",
        "local_staging_evidence", "evidence_checked_at", "created_at",
    ),
    "remote_packaging_boundary_chunks": (
        "session_id", "chunk_index", "classification", "assigned_format",
        "prefix_evidence_basis", "plan_member_count", "plan_logical_bytes",
        "first_ordinal", "last_ordinal", "evidence",
    ),
}

REQUIRED_CONSTRAINTS = {
    "catalog_directories": ("catalog_directories_pkey",),
    "archive_bundles": ("archive_bundles_pkey", "archive_bundles_tape_label_tape_path_key"),
    "remote_sessions": ("remote_sessions_pkey",),
    "remote_chunks": ("remote_chunks_pkey",),
    "files_index": ("files_index_pkey", "uq_files_record_key"),
    "directory_archive_bundles": ("directory_archive_bundles_pkey",),
    "directory_tree_index": ("directory_tree_index_pkey", "fk_dir_tree_bundle_tape"),
    "remote_chunk_scan_segments": ("remote_chunk_scan_segments_pkey",),
    "archive_containers": ("archive_containers_pkey", "archive_containers_chunk_format_fk"),
    "archive_container_members": ("archive_container_members_pkey",),
    "remote_packaging_boundaries": ("remote_packaging_boundaries_pkey",),
    "remote_packaging_boundary_chunks": ("remote_packaging_boundary_chunks_pkey",),
}

REQUIRED_INDEXES = {
    "archive_bundles": ("archive_bundles_tape_label_tape_path_key",),
    "remote_plan_files": ("idx_remote_plan_chunk",),
    "files_index": ("idx_files_remote_chunk",),
    "directory_archive_bundles": ("idx_directory_bundles_tape_path",),
    "directory_tree_index": ("idx_directory_tree_path", "idx_directory_tree_bundle"),
    "remote_chunk_scan_segments": ("ix_remote_chunk_scan_segments_chunk",),
    "archive_containers": ("archive_containers_ordinal_uq",),
    "archive_container_members": ("idx_archive_container_members_container",),
    "archive_artifacts": (
        "uq_archive_artifacts_container_kind_version",
        "uq_archive_artifacts_chunk_kind_version",
    ),
    "archive_runs": ("uq_archive_runs_remote_chunk_generation",),
}

ROW_SET_SPECS = {
    "files_index": ("file_size_bytes",),
    "archive_bundles": (),
    "archive_runs": (),
    "remote_plan_files": (),
    "remote_snapshot_files": ("file_size_bytes",),
    "remote_chunks": (),
    "remote_chunk_scan_segments": (),
    "directory_archive_bundles": ("byte_count",),
    "directory_archive_stats": ("direct_bytes", "recursive_bytes"),
    "directory_tree_index": ("direct_bytes", "recursive_bytes"),
    "archive_containers": ("expected_logical_bytes", "actual_artifact_bytes"),
    "archive_container_members": ("expected_logical_bytes", "estimated_tar_bytes"),
    "archive_artifacts": ("artifact_size_bytes",),
}


def _dict_row(row: Any, columns: Iterable[str] = ()) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    values = list(row or ())
    return {name: values[index] for index, name in enumerate(columns)
            if index < len(values)}


def _safe_fetchall(conn: Any, sql: str, params: tuple[Any, ...] = ()):
    try:
        return [dict(row) if isinstance(row, Mapping) else row
                for row in conn.execute(sql, params).fetchall()], None
    except Exception as exc:  # reports must distinguish unavailable from empty
        return None, f"{type(exc).__name__}: {exc}"


def _safe_fetchone(conn: Any, sql: str, params: tuple[Any, ...] = ()):
    rows, error = _safe_fetchall(conn, sql, params)
    return ((rows[0] if rows else None), error)


def _qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _repo_ddl_inventory(root: Path) -> dict[str, Any]:
    migrations = []
    declared = {"tables": [], "columns": [], "constraints": [],
                "indexes": [], "views": []}
    for name in AUDIT_MIGRATIONS:
        path = root / "scripts" / "sql" / name
        item: dict[str, Any] = {"file": name, "exists": path.is_file()}
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            item["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            item["declared"] = {
                "tables": sorted(set(re.findall(
                    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_]\w*)",
                    text, re.I))),
                "columns": sorted(set(re.findall(
                    r"ALTER\s+TABLE\s+([a-z_]\w*)[\s\S]{0,180}?ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_]\w*)",
                    text, re.I))),
                "constraints": sorted(set(re.findall(
                    r"ADD\s+CONSTRAINT\s+([a-z_]\w*)", text, re.I))),
                "indexes": sorted(set(re.findall(
                    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_]\w*)",
                    text, re.I))),
                "views": sorted(set(re.findall(
                    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([a-z_]\w*)",
                    text, re.I))),
            }
            for kind, values in item["declared"].items():
                if kind == "columns":
                    declared[kind].extend(
                        f"{table}.{column}" for table, column in values)
                else:
                    declared[kind].extend(values)
        migrations.append(item)
    for key in declared:
        declared[key] = sorted(set(map(str, declared[key])))
    return {"migrations": migrations, "declared_objects": declared}


def _installed_schema(conn: Any) -> dict[str, Any]:
    tables, tables_error = _safe_fetchall(
        conn, """SELECT table_name FROM information_schema.tables
                   WHERE table_schema='public' ORDER BY table_name""")
    views, views_error = _safe_fetchall(
        conn, """SELECT table_name AS view_name FROM information_schema.views
                   WHERE table_schema='public' ORDER BY table_name""")
    columns, columns_error = _safe_fetchall(
        conn, """SELECT table_name, column_name, ordinal_position
                   FROM information_schema.columns
                   WHERE table_schema='public'
                   ORDER BY table_name, ordinal_position""")
    constraints, constraints_error = _safe_fetchall(
        conn, """SELECT table_name, constraint_name, constraint_type
                   FROM information_schema.table_constraints
                   WHERE table_schema='public'
                   ORDER BY table_name, constraint_name""")
    indexes, indexes_error = _safe_fetchall(
        conn, """SELECT tablename AS table_name, indexname AS index_name
                   FROM pg_indexes WHERE schemaname='public'
                   ORDER BY tablename, indexname""")
    result = {
        "tables": sorted(row.get("table_name") for row in (tables or [])
                          if row.get("table_name")),
        "views": sorted(row.get("view_name") for row in (views or [])
                         if row.get("view_name")),
        "columns": [dict(row) for row in (columns or [])],
        "constraints": [dict(row) for row in (constraints or [])],
        "indexes": [dict(row) for row in (indexes or [])],
        "query_errors": {
            name: error for name, error in (
                ("tables", tables_error), ("views", views_error),
                ("columns", columns_error), ("constraints", constraints_error),
                ("indexes", indexes_error)) if error
        },
    }
    result["columns_by_table"] = {}
    for row in result["columns"]:
        result["columns_by_table"].setdefault(row["table_name"], []).append(
            row["column_name"])
    result["constraints_by_table"] = {}
    for row in result["constraints"]:
        result["constraints_by_table"].setdefault(row["table_name"], []).append(
            row["constraint_name"])
    result["indexes_by_table"] = {}
    for row in result["indexes"]:
        result["indexes_by_table"].setdefault(row["table_name"], []).append(
            row["index_name"])
    return result


def _schema_checks(installed: dict[str, Any]) -> list[dict[str, Any]]:
    tables = set(installed["tables"])
    columns = installed["columns_by_table"]
    constraints = installed["constraints_by_table"]
    indexes = installed["indexes_by_table"]
    checks = []
    optional = {name for group in OPTIONAL_TABLES.values() for name in group}
    expected_tables = list(BASE_TABLES) + sorted(optional)
    for table in expected_tables:
        present = table in tables
        checks.append({
            "kind": "table", "object": table,
            "status": "PASS" if present else ("UNAVAILABLE" if table in optional else "FAIL"),
            "repository": True, "installed": present,
        })
    for table, required in REQUIRED_COLUMNS.items():
        for column in required:
            present = column in columns.get(table, [])
            optional_object = table in optional
            checks.append({
                "kind": "column", "object": f"{table}.{column}",
                "status": "PASS" if present else ("UNAVAILABLE" if optional_object else "FAIL"),
                "repository": True, "installed": present,
            })
    for table, required in REQUIRED_CONSTRAINTS.items():
        for constraint in required:
            present = constraint in constraints.get(table, [])
            checks.append({
                "kind": "constraint", "object": f"{table}.{constraint}",
                "status": "PASS" if present else ("UNAVAILABLE" if table in optional else "FAIL"),
                "repository": True, "installed": present,
            })
    for table, required in REQUIRED_INDEXES.items():
        for index in required:
            present = index in indexes.get(table, [])
            checks.append({
                "kind": "index", "object": f"{table}.{index}",
                "status": "PASS" if present else ("UNAVAILABLE" if table in optional else "FAIL"),
                "repository": True, "installed": present,
            })
    return checks


def _count_bytes(conn: Any, table: str, byte_columns: tuple[str, ...], installed: dict[str, Any]):
    if table not in installed["tables"]:
        return {"status": "UNAVAILABLE", "rows": None, "bytes": None,
                "bytes_columns": list(byte_columns)}
    columns = set(installed["columns_by_table"].get(table, ()))
    usable = [column for column in byte_columns if column in columns]
    sql = f"SELECT COUNT(*) AS row_count, " \
          "pg_total_relation_size(%s::regclass) AS relation_bytes"
    if usable:
        expressions = [
            f"COALESCE(SUM({_qident(column)}), 0)" for column in usable]
        sql += ", " + " + ".join(expressions) + " AS byte_total"
    sql += f" FROM {_qident(table)}"
    row, error = _safe_fetchone(conn, sql, (f"public.{table}",))
    if error or row is None:
        return {"status": "UNAVAILABLE", "rows": None, "bytes": None,
                "relation_bytes": None,
                "bytes_columns": usable, "error": error}
    row_count = row.get("row_count")
    byte_total = row.get("byte_total") if usable else None
    return {"status": "PASS", "rows": int(row_count or 0),
            "bytes": int(byte_total or 0) if byte_total is not None else None,
            "relation_bytes": int(row.get("relation_bytes") or 0),
            "bytes_columns": usable,
            "bytes_note": None if usable else "no logical byte column applies"}


def _code_provenance_facts(root: Path) -> dict[str, Any]:
    """Inspect provenance claims from Python syntax, not source-text spelling.

    The audit must not treat comments or docstrings as executable evidence.  In
    particular, a warning in a docstring about an intentionally removed SQL
    fallback must not make the fallback check fail, and a missing function must
    never make a sliced-source check pass by accident.
    """

    def read(relative):
        path = root / relative
        try:
            source = path.read_text(encoding="utf-8")
        except OSError as exc:
            return None, f"{relative}: {exc}"
        try:
            return ast.parse(source, filename=str(path)), None
        except SyntaxError as exc:
            return None, f"{relative}: syntax error at line {exc.lineno}: {exc.msg}"

    parsed = {}
    errors = []
    for relative in (
            "src/backup.py", "src/remote_writer.py", "src/remote_staging.py",
            "src/pg_catalog.py"):
        tree, error = read(relative)
        parsed[relative] = tree
        if error:
            errors.append(error)

    reasons: dict[str, list[str]] = {}
    evidence: dict[str, dict[str, Any]] = {}

    def fail_reason(fact: str, reason: str):
        reasons.setdefault(fact, []).append(reason)

    def function(relative: str, name: str, fact: str):
        tree = parsed.get(relative)
        if tree is None:
            fail_reason(fact, f"cannot inspect {relative}; parsed module unavailable")
            return None
        matches = [node for node in ast.walk(tree)
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and node.name == name]
        if not matches:
            fail_reason(fact, f"{relative}: target symbol {name!r} was not found")
            return None
        if len(matches) != 1:
            fail_reason(
                fact,
                f"{relative}: target symbol {name!r} is ambiguous ({len(matches)} definitions)",
            )
            return None
        return matches[0]

    def call_attribute(node: ast.Call, attribute: str) -> bool:
        return isinstance(node.func, ast.Attribute) and node.func.attr == attribute

    def calls(node: ast.AST, attribute: str):
        return [item for item in ast.walk(node)
                if isinstance(item, ast.Call)
                and call_attribute(item, attribute)]

    def named_calls(node: ast.AST, name: str):
        return [item for item in ast.walk(node)
                if isinstance(item, ast.Call)
                and isinstance(item.func, ast.Name)
                and item.func.id == name]

    def keyword_map(node: ast.Call):
        return {item.arg: item.value for item in node.keywords if item.arg is not None}

    def names_bound(node: ast.AST):
        if isinstance(node, ast.Name):
            return {node.id}
        if isinstance(node, (ast.Tuple, ast.List)):
            result = set()
            for item in node.elts:
                result.update(names_bound(item))
            return result
        return set()

    def static_string(node: ast.AST | None):
        """Return a statically assembled string, excluding bare docstrings."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = static_string(node.left)
            right = static_string(node.right)
            if left is not None and right is not None:
                return left + right
        return None

    def execute_sql_calls(node: ast.AST):
        result = []
        for item in ast.walk(node):
            if not isinstance(item, ast.Call) or not call_attribute(item, "execute"):
                continue
            sql = static_string(item.args[0]) if item.args else None
            if sql is not None:
                result.append((item, sql))
        return result

    def is_not_none(node: ast.AST, name: str):
        return (
            isinstance(node, ast.Compare)
            and isinstance(node.left, ast.Name)
            and node.left.id == name
            and len(node.ops) == 1
            and isinstance(node.ops[0], ast.IsNot)
            and len(node.comparators) == 1
            and isinstance(node.comparators[0], ast.Constant)
            and node.comparators[0].value is None
        )

    # Explicit identity is proven at the actual calls: backup calls the catalog
    # upsert, while the remote writer passes the same identity into that writer.
    backup_run = function("src/backup.py", "_run_locked", "explicit_remote_identity")
    writer_run = function(
        "src/remote_writer.py", "_write_one_chunk_owned", "explicit_remote_identity")
    backup_calls = (calls(backup_run, "bulk_upsert_directory_catalog")
                    if backup_run is not None else [])
    writer_calls = calls(writer_run, "run") if writer_run is not None else []
    backup_identity = all(
        set(keyword_map(item)) >= {"remote_session_id", "remote_chunk_index"}
        and isinstance(keyword_map(item)["remote_session_id"], ast.Name)
        and keyword_map(item)["remote_session_id"].id == "remote_session_id"
        and isinstance(keyword_map(item)["remote_chunk_index"], ast.Name)
        and keyword_map(item)["remote_chunk_index"].id == "remote_chunk_index"
        for item in backup_calls
    ) and bool(backup_calls)
    writer_identity = all(
        set(keyword_map(item)) >= {"remote_session_id", "remote_chunk_index"}
        and isinstance(keyword_map(item)["remote_session_id"], ast.Name)
        and keyword_map(item)["remote_session_id"].id == "session_id"
        and isinstance(keyword_map(item)["remote_chunk_index"], ast.Name)
        and keyword_map(item)["remote_chunk_index"].id == "chunk_index"
        for item in writer_calls
    ) and bool(writer_calls)
    explicit_ids = backup_identity and writer_identity
    if not backup_calls:
        fail_reason(
            "explicit_remote_identity",
            "src/backup.py: no catalog upsert call with an inspectable call site",
        )
    if not writer_calls:
        fail_reason(
            "explicit_remote_identity",
            "src/remote_writer.py: no writer call with an inspectable call site",
        )
    if backup_calls and not backup_identity:
        fail_reason(
            "explicit_remote_identity",
            "src/backup.py: a catalog upsert call lacks the required identity names",
        )
    if writer_calls and not writer_identity:
        fail_reason(
            "explicit_remote_identity",
            "src/remote_writer.py: a writer call lacks the required identity names",
        )
    evidence["explicit_remote_identity"] = {
        "backup_catalog_calls": len(backup_calls),
        "remote_writer_calls": len(writer_calls),
    }

    # The catalog chunk assignment must be an IfExp keyed by remote-session
    # presence.  Separately scan every parsed assignment for the forbidden
    # direct local-to-remote binding.
    bulk = function(
        "src/pg_catalog.py", "bulk_upsert_directory_catalog",
        "remote_chunk_not_derived_from_local")
    chunk_assignment = []
    if bulk is not None:
        chunk_assignment = [item for item in ast.walk(bulk)
                            if isinstance(item, ast.Assign)
                            and item.targets
                            and "catalog_chunk_index" in names_bound(item.targets[0])]
    good_chunk_assignment = any(
        isinstance(item.value, ast.IfExp)
        and is_not_none(item.value.test, "remote_session_id")
        and isinstance(item.value.body, ast.Name)
        and item.value.body.id == "remote_chunk_index"
        and isinstance(item.value.orelse, ast.Name)
        and item.value.orelse.id == "local_chunk_index"
        for item in chunk_assignment
    )
    forbidden_bindings = []
    for tree in parsed.values():
        if tree is None:
            continue
        for item in ast.walk(tree):
            if isinstance(item, ast.Assign) and item.targets:
                targets = names_bound(item.targets[0])
                if (targets & {"remote_chunk_index"}
                        and isinstance(item.value, ast.Name)
                        and item.value.id == "local_chunk_index"):
                    forbidden_bindings.append(item)
            elif isinstance(item, ast.AnnAssign):
                targets = names_bound(item.target)
                if (targets & {"remote_chunk_index"}
                        and isinstance(item.value, ast.Name)
                        and item.value.id == "local_chunk_index"):
                    forbidden_bindings.append(item)
    no_local_derivation = bool(chunk_assignment) and good_chunk_assignment and not forbidden_bindings
    if not chunk_assignment:
        fail_reason(
            "remote_chunk_not_derived_from_local",
            "src/pg_catalog.py: bulk_upsert_directory_catalog has no catalog_chunk_index assignment",
        )
    elif not good_chunk_assignment:
        fail_reason(
            "remote_chunk_not_derived_from_local",
            "src/pg_catalog.py: catalog_chunk_index is not selected by the required remote-session IfExp",
        )
    if forbidden_bindings:
        fail_reason(
            "remote_chunk_not_derived_from_local",
            "a parsed assignment binds remote_chunk_index directly from local_chunk_index",
        )
    evidence["remote_chunk_not_derived_from_local"] = {
        "catalog_chunk_assignments": len(chunk_assignment),
        "forbidden_direct_bindings": len(forbidden_bindings),
    }

    # Canonicalization is also checked structurally.  This keeps the existing
    # fact useful while removing its former raw substring evidence.
    staging_tree = parsed.get("src/remote_staging.py")
    staging_calls = named_calls(staging_tree, "_apply_canonical_remote_paths") \
        if staging_tree is not None else []
    canonical_key_used = False
    remote_root_branch = False
    if bulk is not None:
        canonical_key_used = any(
            any(static_string(arg) == "canonical_source_path"
                for arg in item.args)
            for item in ast.walk(bulk)
            if isinstance(item, ast.Call)
            and isinstance(item.func, ast.Attribute)
            and item.func.attr == "get"
        )
        remote_root_branch = any(
            is_not_none(item.test, "remote_session_id")
            and any("original_root" in names_bound(child.targets[0])
                    for child in item.body
                    if isinstance(child, ast.Assign) and child.targets)
            for item in ast.walk(bulk) if isinstance(item, ast.If)
        )
    canonical_before_catalog = bool(staging_calls) and canonical_key_used and remote_root_branch
    if not staging_calls:
        fail_reason(
            "canonical_remote_root_before_catalog",
            "src/remote_staging.py: no call to _apply_canonical_remote_paths was found",
        )
    if not canonical_key_used:
        fail_reason(
            "canonical_remote_root_before_catalog",
            "src/pg_catalog.py: canonical_source_path is not read at a parsed catalog call site",
        )
    if not remote_root_branch:
        fail_reason(
            "canonical_remote_root_before_catalog",
            "src/pg_catalog.py: no remote-session branch assigns original_root",
        )
    evidence["canonical_remote_root_before_catalog"] = {
        "staging_canonicalization_calls": len(staging_calls),
        "canonical_source_path_call_key": canonical_key_used,
        "remote_original_root_branch": remote_root_branch,
    }

    derive = function(
        "src/pg_catalog.py", "_derive_bundle_base_path",
        "no_remote_path_root_fallback")
    derive_for_scope = function(
        "src/pg_catalog.py", "_derive_bundle_base_path",
        "persisted_scan_scope_guard")
    derive_sql = execute_sql_calls(derive) if derive is not None else []
    scope_sql_calls = [
        (call, sql) for call, sql in (execute_sql_calls(derive_for_scope)
                                      if derive_for_scope is not None else [])
        if "remote_scan_scopes" in sql and "root_path" in sql
    ]
    remote_session_sql = [
        sql for _call, sql in derive_sql if "remote_sessions" in sql
    ]
    no_remote_path_fallback = derive is not None and not remote_session_sql
    if remote_session_sql:
        fail_reason(
            "no_remote_path_root_fallback",
            "_derive_bundle_base_path passes SQL reading remote_sessions to execute()",
        )
    evidence["no_remote_path_root_fallback"] = {
        "execute_sql_literals": len(derive_sql),
        "remote_sessions_sql_literals": len(remote_session_sql),
    }

    # Prove both halves of the scope guard: a scope query selects root_path, and
    # the fetched result is transformed into a name used by a negative-match
    # test whose body returns the empty root.  This deliberately does not accept
    # a docstring or an unrelated query elsewhere in pg_catalog.py.
    scope_result_names = set()
    if derive_for_scope is not None:
        for item in ast.walk(derive_for_scope):
            if not isinstance(item, ast.Assign) or not item.targets:
                continue
            value = item.value
            execute_call = None
            if (isinstance(value, ast.Call)
                    and isinstance(value.func, ast.Attribute)
                    and value.func.attr in {"fetchall", "fetchone"}
                    and isinstance(value.func.value, ast.Call)):
                candidate = value.func.value
                if call_attribute(candidate, "execute"):
                    execute_call = candidate
            if execute_call is not None:
                sql = static_string(execute_call.args[0]) if execute_call.args else None
                if sql is not None and "remote_scan_scopes" in sql and "root_path" in sql:
                    scope_result_names.update(names_bound(item.targets[0]))

        derived_scope_names = set(scope_result_names)
        for item in ast.walk(derive_for_scope):
            if not isinstance(item, ast.Assign) or not item.targets:
                continue
            value = item.value
            if isinstance(value, (ast.ListComp, ast.GeneratorExp)):
                if any(isinstance(generator.iter, ast.Name)
                       and generator.iter.id in scope_result_names
                       for generator in value.generators):
                    derived_scope_names.update(names_bound(item.targets[0]))

        def body_returns_empty(body):
            return any(
                isinstance(child, ast.Return)
                and isinstance(child.value, ast.Constant)
                and child.value.value == ""
                for statement in body for child in ast.walk(statement)
            )

        def is_negative_match_test(test):
            return any(
                isinstance(child, ast.UnaryOp)
                and isinstance(child.op, ast.Not)
                and any(isinstance(desc, ast.Call)
                        and isinstance(desc.func, ast.Name)
                        and desc.func.id == "any"
                        for desc in ast.walk(child.operand))
                for child in ast.walk(test)
            )

        scope_result_gates_empty = any(
            bool(derived_scope_names & {
                child.id for child in ast.walk(item.test)
                if isinstance(child, ast.Name)
            })
            and is_negative_match_test(item.test)
            and body_returns_empty(item.body)
            for item in ast.walk(derive_for_scope)
            if isinstance(item, ast.If)
        )
    else:
        scope_result_names = set()
        scope_result_gates_empty = False
    scope_guard = bool(scope_sql_calls) and scope_result_gates_empty
    if derive_for_scope is None:
        # ``function`` already recorded the explicit missing-symbol reason.
        pass
    elif not scope_sql_calls:
        fail_reason(
            "persisted_scan_scope_guard",
            "_derive_bundle_base_path has no execute() SQL literal selecting root_path from remote_scan_scopes",
        )
    if scope_sql_calls and not scope_result_gates_empty:
        fail_reason(
            "persisted_scan_scope_guard",
            "the remote_scan_scopes result does not gate an empty-root return on a negative scope match",
        )
    evidence["persisted_scan_scope_guard"] = {
        "scope_sql_literals": len(scope_sql_calls),
        "scope_result_names": sorted(scope_result_names),
        "negative_match_empty_return": scope_result_gates_empty,
    }

    return {
        "errors": errors,
        "explicit_remote_identity": explicit_ids,
        "remote_chunk_not_derived_from_local": no_local_derivation,
        "canonical_remote_root_before_catalog": canonical_before_catalog,
        "no_remote_path_root_fallback": no_remote_path_fallback,
        "persisted_scan_scope_guard": scope_guard,
        "reasons": reasons,
        "evidence": evidence,
    }


def _fact(name: str, description: str, status: str, evidence: Any, **extra):
    result = {"name": name, "description": description, "status": status,
              "evidence": evidence}
    result.update(extra)
    return result


def _provenance_counts(conn: Any, installed: dict[str, Any]) -> dict[str, Any]:
    tables = set(installed["tables"])
    result: dict[str, Any] = {}
    if "directory_tree_index" in tables:
        row, error = _safe_fetchone(
            conn, """SELECT COUNT(*) AS total,
                              COUNT(*) FILTER (WHERE chunk_index IS NULL) AS null_chunk,
                              COUNT(*) FILTER (WHERE bundle_id IS NULL) AS no_bundle
                       FROM directory_tree_index""")
        result["directory_tree_index_chunk_null"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["null_chunk"] or 0) else "FAIL")),
            "total": int(row["total"] or 0) if row else None,
            "count": int(row["null_chunk"] or 0) if row else None,
            "error": error,
        }
        row, error = _safe_fetchone(
            conn, """SELECT COUNT(*) AS n
                       FROM directory_tree_index t
                       JOIN directory_archive_bundles b ON b.bundle_id=t.bundle_id
                       WHERE t.chunk_index IS DISTINCT FROM b.chunk_index""")
        result["directory_tree_index_conflicting_chunk"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["n"] or 0) else "FAIL")),
            "count": int(row["n"] or 0) if row else None, "error": error,
        }
    else:
        result["directory_tree_index_chunk_null"] = {"status": "UNAVAILABLE"}
        result["directory_tree_index_conflicting_chunk"] = {"status": "UNAVAILABLE"}

    if "directory_archive_bundles" in tables:
        row, error = _safe_fetchone(
            conn, """SELECT COUNT(*) AS n
                       FROM directory_archive_bundles
                       WHERE original_dir_path ~ '(^|[/\\\\])_(fetch|pack)_s[0-9]+_[0-9]+([/\\\\]|$)'""")
        result["directory_archive_bundles_transient_root"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["n"] or 0) else "FAIL")),
            "count": int(row["n"] or 0) if row else None, "error": error,
        }
    else:
        result["directory_archive_bundles_transient_root"] = {"status": "UNAVAILABLE"}

    if "remote_chunks" in tables and "membership_state" in installed["columns_by_table"].get("remote_chunks", []):
        row, error = _safe_fetchone(
            conn, "SELECT COUNT(*) AS n FROM remote_chunks WHERE membership_state IS NULL")
        result["remote_chunks_membership_null"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["n"] or 0) else "FAIL")),
            "count": int(row["n"] or 0) if row else None, "error": error,
        }
    else:
        result["remote_chunks_membership_null"] = {"status": "UNAVAILABLE"}

    if "remote_chunks" in tables and "remote_chunk_scan_segments" in tables:
        row, error = _safe_fetchone(
            conn, """SELECT COUNT(*) AS n
                       FROM remote_chunks c
                       WHERE NOT EXISTS (
                           SELECT 1 FROM remote_chunk_scan_segments s
                           WHERE s.session_id=c.session_id
                             AND s.chunk_index=c.chunk_index)""")
        result["remote_chunk_scan_segments_missing"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["n"] or 0) else "FAIL")),
            "count": int(row["n"] or 0) if row else None, "error": error,
            "note": "A legacy chunk with NULL membership_state is also classified as unknown, not complete.",
        }
    else:
        result["remote_chunk_scan_segments_missing"] = {"status": "UNAVAILABLE"}

    if "directory_tree_index" in tables:
        row, error = _safe_fetchone(
            conn, """SELECT COUNT(*) AS n
                       FROM (SELECT source_host, original_dir_path, tape_label,
                                    remote_session_id, chunk_index, bundle_id,
                                    COUNT(*) AS copies
                               FROM directory_tree_index
                              GROUP BY source_host, original_dir_path, tape_label,
                                       remote_session_id, chunk_index, bundle_id
                             HAVING COUNT(*) > 1) d""")
        result["duplicate_directory_contributions"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["n"] or 0) else "FAIL")),
            "count": int(row["n"] or 0) if row else None, "error": error,
        }
        row, error = _safe_fetchone(
            conn, """SELECT COUNT(*) AS n
                       FROM (SELECT source_host, original_dir_path,
                                    COUNT(DISTINCT (tape_label, remote_session_id,
                                                    chunk_index, bundle_id)) AS routes
                               FROM directory_tree_index
                              GROUP BY source_host, original_dir_path
                             HAVING COUNT(DISTINCT (tape_label, remote_session_id,
                                                    chunk_index, bundle_id)) > 1) d""")
        result["conflicting_directory_contributions"] = {
            "status": ("UNAVAILABLE" if error else
                        ("PASS" if not int(row["n"] or 0) else "FAIL")),
            "count": int(row["n"] or 0) if row else None, "error": error,
        }
    else:
        result["duplicate_directory_contributions"] = {"status": "UNAVAILABLE"}
        result["conflicting_directory_contributions"] = {"status": "UNAVAILABLE"}
    return result


def _locator_classification(conn: Any, installed: dict[str, Any]) -> dict[str, Any]:
    if "directory_archive_bundles" not in installed["tables"]:
        return {"status": "UNAVAILABLE", "rows": [], "note": "optional migration 007 is absent"}
    rows, error = _safe_fetchall(
        conn, """SELECT bundle_id, tape_label, manifest_path,
                          stored_bundle_path
                   FROM directory_archive_bundles
                  WHERE manifest_path IS NOT NULL""")
    if error:
        return {"status": "UNAVAILABLE", "rows": [], "error": error}
    counts = {"tape": 0, "local_candidate_unverified": 0, "missing": 0}
    details = []
    for row in rows or []:
        path = str(row.get("manifest_path") or "")
        drive = os.path.splitdrive(path)[0].casefold()
        stored_drive = os.path.splitdrive(str(row.get("stored_bundle_path") or ""))[0].casefold()
        if not path:
            kind = "missing"
        elif drive and stored_drive and drive == stored_drive:
            kind = "tape"
        elif drive:
            kind = "local_candidate_unverified"
        else:
            kind = "local_candidate_unverified"
        counts[kind] += 1
        details.append({"bundle_id": row.get("bundle_id"), "kind": kind,
                        "tape_label": row.get("tape_label"),
                        "locator_verified": False})
    return {
        "status": "PASS", "counts": counts, "rows": details,
        "note": "Classification is metadata-only. A tape locator is not local rebuild evidence and no locator was opened.",
    }


def _historical_dry_run(conn: Any, installed: dict[str, Any], counts: dict[str, Any]):
    tables = set(installed["tables"])
    repairable = []
    flagged = []
    blocking = set()
    if {"directory_tree_index", "directory_archive_bundles"} <= tables:
        rows, error = _safe_fetchall(
            conn, """SELECT t.dir_id, t.original_dir_path, t.bundle_id,
                              t.remote_session_id AS tree_session,
                              t.chunk_index AS tree_chunk,
                              b.remote_session_id AS bundle_session,
                              b.chunk_index AS bundle_chunk
                       FROM directory_tree_index t
                       JOIN directory_archive_bundles b ON b.bundle_id=t.bundle_id
                      WHERE t.chunk_index IS NULL
                        AND b.chunk_index IS NOT NULL
                        AND (t.remote_session_id IS NOT DISTINCT FROM b.remote_session_id)""")
        if error:
            flagged.append({"kind": "tree_chunk", "status": "unknown", "error": error})
        else:
            for row in rows or []:
                repairable.append({
                    "kind": "directory_tree_index.chunk_index",
                    "row_id": row.get("dir_id"), "current": None,
                    "proposed": row.get("bundle_chunk"),
                    "proof": "same directory bundle FK, same remote session, non-null bundle chunk",
                    "execute": False,
                })
        rows, error = _safe_fetchall(
            conn, """SELECT bundle_id, original_dir_path, tape_label,
                              remote_session_id, chunk_index
                       FROM directory_archive_bundles
                      WHERE original_dir_path ~ '(^|[/\\\\])_(fetch|pack)_s[0-9]+_[0-9]+([/\\\\]|$)'""")
        if error:
            flagged.append({"kind": "bundle_root", "status": "unknown", "error": error})
        else:
            for row in rows or []:
                blocking.add(row.get("original_dir_path"))
                flagged.append({
                    "kind": "directory_archive_bundles.original_dir_path",
                    "row_id": row.get("bundle_id"), "status": "conflicting_or_unknown",
                    "reason": "transient staging root; no stable container/member proof was accepted",
                    "execute": False,
                })
    if "remote_chunks_membership_null" in counts and counts["remote_chunks_membership_null"].get("count"):
        flagged.append({
            "kind": "remote_chunks.membership_state", "status": "unknown",
            "count": counts["remote_chunks_membership_null"]["count"],
            "reason": "legacy NULL is not repairable without sealed membership evidence",
            "execute": False,
        })
    if counts.get("remote_chunk_scan_segments_missing", {}).get("count"):
        flagged.append({
            "kind": "remote_chunk_scan_segments", "status": "unknown",
            "count": counts["remote_chunk_scan_segments_missing"]["count"],
            "reason": "missing consumption rows cannot be invented from pack names",
            "execute": False,
        })
    return {
        "dry_run": True,
        "repairable": repairable,
        "flagged": flagged,
        "blocking_directories": sorted(x for x in blocking if x),
        "writes_performed": 0,
        "note": "No repair is executed. `_pack_sNNNN_CCC` names alone are never evidence.",
    }


def audit_schema_provenance(conn: Any, *, repository_root: str | os.PathLike[str] = PROJECT_ROOT) -> dict[str, Any]:
    """Return a structured, read-only audit for one selected DB connection."""
    root = Path(repository_root)
    installed = _installed_schema(conn)
    repo = _repo_ddl_inventory(root)
    checks = _schema_checks(installed)
    counts = _provenance_counts(conn, installed)
    code = _code_provenance_facts(root)
    locator = _locator_classification(conn, installed)

    facts = [
        _fact("catalog_directories", "base migration 001 tape navigation tree exists", "PASS" if "catalog_directories" in installed["tables"] else "FAIL", {"installed": "catalog_directories" in installed["tables"]}),
        _fact("optional_007", "migration 007 defines the three directory tables and is not auto-applied", "PASS" if repo["migrations"][6]["exists"] else "UNAVAILABLE", {"installed_tables": [x for x in OPTIONAL_TABLES["007"] if x in installed["tables"]], "startup_excluded": True}),
        _fact("unimplemented_directory_status", "directory_completeness and the unified directory view are not schema truth", "PASS" if not ({"directory_completeness", "directory_catalog_status_v"} & set(installed["tables"] + installed["views"])) else "FAIL", {"unexpected_installed": sorted({"directory_completeness", "directory_catalog_status_v"} & set(installed["tables"] + installed["views"]))}),
        _fact("feature_design_unimplemented", "feature-design migration 010 does not override the real migration 010", "PASS" if not ({"directory_completeness", "directory_catalog_status_v"} & set(installed["tables"] + installed["views"])) else "FAIL", {"real_migration_010": "010_postgres_local_manifest_archive.sql", "design_doc_is_not_ddl": True}),
        _fact("directory_tree_is_contribution_table", "directory_tree_index is per-container contribution data", "PASS" if "directory_tree_index" in installed["tables"] and "dir_id" in installed["columns_by_table"].get("directory_tree_index", []) else "UNAVAILABLE", {"primary_key_shape": "dir_id; no canonical-directory assertion is made"}),
        _fact("independent_bundle_registries", "archive_bundles and directory_archive_bundles are not joined registries", "PASS" if "archive_bundles" in installed["tables"] and "directory_archive_bundles" in installed["tables"] else "UNAVAILABLE", {"join_performed": False, "namespaces": "independent"}),
        _fact("plan2_remote_identity", "remote session/chunk identity is explicit and not derived from local chunk identity", "PASS" if code["explicit_remote_identity"] and code["remote_chunk_not_derived_from_local"] else "FAIL", code),
        _fact("plan2_canonical_root", "remote catalog roots use canonical source paths, not fetch staging", "PASS" if code["canonical_remote_root_before_catalog"] else "FAIL", code),
        _fact("plan2_restore_root", "restore routing does not fall back to remote_sessions.remote_path and constrains legacy member evidence with persisted scan scopes", "PASS" if code["no_remote_path_root_fallback"] and code["persisted_scan_scope_guard"] else "FAIL", code),
        _fact("current_transient_roots", "directory bundle roots are not transient fetch/pack staging", counts["directory_archive_bundles_transient_root"]["status"], counts["directory_archive_bundles_transient_root"]),
        _fact("current_locator_provenance", "manifest locators are classified without tape access", locator["status"], locator),
    ]
    row_sets = {table: _count_bytes(conn, table, columns, installed)
                for table, columns in ROW_SET_SPECS.items()}
    capabilities = {}
    for table, data in row_sets.items():
        usable = data["status"] == "PASS"
        capabilities[table] = {
            "row_set": table,
            "counts": {"rows": data.get("rows"), "bytes": data.get("bytes")},
            "relation_bytes": data.get("relation_bytes"),
            "can_support": {
                "session_37_legacy_export": usable and table in {"remote_plan_files", "remote_snapshot_files", "files_index", "archive_bundles"},
                "directory_completeness": usable and table in {"directory_archive_bundles", "directory_archive_stats", "directory_tree_index"} and not any(counts.get(k, {}).get("count") for k in ("directory_tree_index_chunk_null", "directory_tree_index_conflicting_chunk", "duplicate_directory_contributions", "conflicting_directory_contributions")),
                "restore": usable and table in {"archive_bundles", "directory_archive_bundles", "archive_containers", "archive_artifacts", "files_index"},
                "rebuild": usable and table in {"remote_plan_files", "remote_snapshot_files", "archive_containers", "archive_container_members", "archive_artifacts"},
            },
            "cannot_support": {},
            "reason": "unavailable" if not usable else "capability is conditional on the provenance findings above",
        }
        capabilities[table]["cannot_support"] = {
            key: not value for key, value in capabilities[table]["can_support"].items()
        }
    return {
        "report_kind": "plan3_phase0_schema_and_catalog_provenance",
        "read_only": True,
        "repository_ddl": repo,
        "installed_schema": installed,
        "schema_checks": checks,
        "facts": facts,
        "provenance_counts": counts,
        "row_sets": row_sets,
        "capabilities": capabilities,
        "historical_dry_run": _historical_dry_run(conn, installed, counts),
        "overall": {
            "status": "FAIL" if any(f["status"] == "FAIL" for f in facts) else "PASS",
            "blocking_directories": _historical_dry_run(conn, installed, counts)["blocking_directories"],
            "no_writes": True,
            "no_tape_access": True,
        },
    }


def audit_manager(manager: Any, *, repository_root: str | os.PathLike[str] = PROJECT_ROOT) -> dict[str, Any]:
    """Run the audit through a manager opened with ``init_schema=False``."""
    with manager._pool.connection() as conn:
        return audit_schema_provenance(conn, repository_root=repository_root)


def render_schema_audit(report: Mapping[str, Any]) -> str:
    """Render the structured report as concise operator-readable text."""
    lines = ["Plan 3 Phase 0 schema/catalog provenance audit (READ-ONLY)"]
    lines.append(f"Overall: {report.get('overall', {}).get('status', 'UNAVAILABLE')}")
    lines.append("Facts:")
    for fact in report.get("facts", []):
        lines.append(f"  [{fact['status']}] {fact['name']}: {fact['description']}")
    lines.append("Row sets:")
    for name, item in report.get("capabilities", {}).items():
        counts = item.get("counts", {})
        support = ", ".join(k for k, value in item.get("can_support", {}).items() if value) or "none"
        lines.append(f"  {name}: rows={counts.get('rows')!s} bytes={counts.get('bytes')!s}; can support={support}")
    historical = report.get("historical_dry_run", {})
    lines.append(f"Historical dry-run: repairable={len(historical.get('repairable', []))}, flagged={len(historical.get('flagged', []))}, writes={historical.get('writes_performed', 0)}")
    lines.append("No tape locator was opened; no database write was performed.")
    return "\n".join(lines)


# Names useful to callers and tests without prescribing a CLI implementation.
audit_catalog_boundary = audit_schema_provenance
render_text = render_schema_audit
