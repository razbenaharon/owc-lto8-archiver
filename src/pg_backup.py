"""PostgreSQL catalog backup helpers."""
import hashlib
import json
import os
import shutil
import stat
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - handled at runtime
        psycopg = None
        sql = None

from .constants import DB_BACKUP_DIR, PROJECT_ROOT
from .pg_bulk import build_conninfo, require_psycopg


DOCKER_DB_CONTAINER = "lto_pg"
PG_DUMP_TIMEOUT_SECONDS = 6 * 60 * 60


def _safe_filename_part(value, fallback):
    text = str(value or "").strip()
    safe = "".join(
        ch if ch.isalnum() or ch in ("-", "_", ".") else "_"
        for ch in text
    ).strip("._")
    return safe or fallback


def _backup_filename(dbname, timestamp=None, prefix=None):
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    name = _safe_filename_part(dbname, 'database')
    if prefix:
        return f"{_safe_filename_part(prefix, 'backup')}_{name}_{stamp}.dump"
    return f"{name}_{stamp}.dump"


def _is_loopback_host(host):
    return str(host or "").strip().lower() in {
        "localhost", "127.0.0.1", "::1", ""
    }


def _run_command(args, env=None):
    try:
        result = subprocess.run(
            args,
            env=env,
            text=True,
            capture_output=True,
            timeout=PG_DUMP_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Command not found: {args[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Command timed out after {PG_DUMP_TIMEOUT_SECONDS} seconds: {args[0]}"
        ) from exc

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        if detail:
            raise RuntimeError(detail)
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {args[0]}"
        )
    return result


def _pg_env(cfg, dbname=None):
    env = os.environ.copy()
    env.update({
        "PGHOST": cfg.pg_host,
        "PGPORT": cfg.pg_port,
        "PGDATABASE": dbname or cfg.pg_dbname,
        "PGUSER": cfg.pg_user,
        "PGSSLMODE": cfg.pg_sslmode,
    })
    if cfg.pg_password:
        env["PGPASSWORD"] = cfg.pg_password
    return env


def _docker_container(cfg=None):
    return getattr(cfg, "pg_docker_container", DOCKER_DB_CONTAINER)


def _docker_container_running(container_name=DOCKER_DB_CONTAINER):
    if not shutil.which("docker"):
        return False
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
        text=True,
        capture_output=True,
    )
    return result.returncode == 0 and result.stdout.strip().lower() == "true"


def _can_try_docker_backup(cfg):
    return (
        _is_loopback_host(getattr(cfg, "pg_host", "")) and
        _docker_container_running(_docker_container(cfg))
    )


def _backup_with_docker(cfg, output_path, snapshot_id=None):
    container = _docker_container(cfg)
    container_path = f"/tmp/{output_path.name}"
    try:
        command = [
            "docker", "exec", container,
            "pg_dump",
            "-U", cfg.pg_user,
            "-d", cfg.pg_dbname,
            "-F", "c",
            "--no-owner",
            "--no-acl",
            "-f", container_path,
        ]
        if snapshot_id:
            command.append(f"--snapshot={snapshot_id}")
        _run_command(command)
        _run_command([
            "docker", "cp",
            f"{container}:{container_path}",
            str(output_path),
        ])
    finally:
        subprocess.run(
            ["docker", "exec", container, "rm", "-f", container_path],
            text=True,
            capture_output=True,
        )


def _backup_with_local_pg_dump(cfg, output_path, snapshot_id=None):
    if not shutil.which("pg_dump"):
        raise RuntimeError(
            "pg_dump is not on PATH. Install PostgreSQL client tools or start "
            "the local Docker database container."
        )

    command = [
        "pg_dump",
        "-F", "c",
        "--no-owner",
        "--no-acl",
        "-f", str(output_path),
    ]
    if snapshot_id:
        command.append(f"--snapshot={snapshot_id}")
    _run_command(command, env=_pg_env(cfg))


def _restore_list_with_docker_container(
        backup_path, restore_list_path, container_name):
    container_path = f"/tmp/{backup_path.name}"
    try:
        _run_command([
            "docker", "cp", str(backup_path),
            f"{container_name}:{container_path}",
        ])
        result = _run_command([
            "docker", "exec", container_name,
            "pg_restore", "--list", container_path,
        ])
        restore_list_path.write_text(result.stdout, encoding="utf-8")
    finally:
        subprocess.run(
            ["docker", "exec", container_name, "rm", "-f", container_path],
            text=True,
            capture_output=True,
        )


def _restore_list_with_local_pg_restore(backup_path, restore_list_path):
    if not shutil.which("pg_restore"):
        raise RuntimeError(
            "pg_restore is not on PATH. Install PostgreSQL client tools or "
            "start the local Docker database container."
        )
    result = _run_command(["pg_restore", "--list", str(backup_path)])
    restore_list_path.write_text(result.stdout, encoding="utf-8")


def verify_backup_file(cfg, backup_path):
    backup_path = validate_backup_local_path(
        cfg, backup_path, expected_kind="file", allow_missing=False)
    if backup_path.stat().st_size <= 0:
        raise RuntimeError(f"[DB BACKUP] Backup is missing or empty: {backup_path}")
    restore_list_path = backup_path.with_suffix(".restore_list.txt")
    validate_backup_local_path(
        cfg, restore_list_path, expected_kind="file", allow_missing=True)
    temporary = restore_list_path.with_name(
        f".{restore_list_path.name}.{os.getpid()}.{uuid.uuid4().hex}.part")
    validate_backup_local_path(
        cfg, temporary, expected_kind="file", allow_missing=True)
    errors = []
    verified = False
    if _can_try_docker_backup(cfg):
        try:
            _restore_list_with_docker_container(
                backup_path, temporary, _docker_container(cfg))
            verified = True
        except RuntimeError as exc:
            errors.append(f"Docker pg_restore --list failed: {exc}")
            temporary.unlink(missing_ok=True)
    if not verified:
        try:
            _restore_list_with_local_pg_restore(backup_path, temporary)
            verified = True
        except RuntimeError as exc:
            errors.append(f"Local pg_restore --list failed: {exc}")
            temporary.unlink(missing_ok=True)
    if (not verified or not temporary.exists()
            or temporary.stat().st_size <= 0):
        details = "\n".join(f"  - {err}" for err in errors)
        raise RuntimeError(
            "[DB BACKUP] Backup file exists but restore-list verification "
            f"failed.\n{details}"
        )
    validate_backup_local_path(
        cfg, temporary, expected_kind="file", allow_missing=False)
    os.replace(temporary, restore_list_path)
    return str(restore_list_path)


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _backup_database_identity(cfg, conn=None):
    """Exact server/database identity recorded beside a guarded backup."""
    require_psycopg()
    def read(active_conn):
        return dict(active_conn.execute(
            """SELECT current_database() AS database,
                      inet_server_addr()::text AS server_addr,
                      inet_server_port() AS server_port,
                      current_user AS database_user,
                      (SELECT oid::text FROM pg_database
                       WHERE datname=current_database()) AS database_oid,
                      (SELECT system_identifier::text
                       FROM pg_control_system()) AS system_identifier"""
        ).fetchone())

    if conn is not None:
        return read(conn)
    with cast("psycopg.Connection", psycopg.connect(
            build_conninfo(
                host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_dbname,
                user=cfg.pg_user, password=cfg.pg_password,
                sslmode=cfg.pg_sslmode), autocommit=True,
            row_factory=cast(Any, dict_row))) as active_conn:
        return read(active_conn)


_BACKUP_FINGERPRINT_TABLES = (
    ("tapes", ("volume_label",)),
    ("tape_generations", ("generation_id",)),
    ("remote_sessions", ("session_id",)),
    ("remote_snapshots", ("snapshot_id",)),
    ("remote_plans", ("plan_id",)),
    ("remote_chunks", ("session_id", "chunk_index")),
    ("remote_snapshot_files", ("snapshot_id", "remote_path")),
    ("remote_plan_files", (
        "plan_id", "chunk_index", "ordinal", "plan_file_id")),
    ("remote_file_state", ("session_id", "plan_file_id")),
    ("remote_worker_attempts", ("attempt_id",)),
    ("files_index", ("file_id",)),
    ("archive_runs", ("run_id",)),
    ("archive_bundles", ("bundle_id",)),
    ("directory_archive_stats", ("stat_id",)),
    ("directory_archive_bundles", ("bundle_id",)),
    ("directory_tree_index", ("dir_id",)),
    ("tape_write_batches", ("batch_id",)),
    ("tape_write_batch_chunks", ("batch_id", "ordinal")),
    ("tape_write_active_chunk", ("session_id", "chunk_index")),
    ("archive_containers", ("container_id",)),
    ("archive_artifacts", ("artifact_id",)),
    ("remote_packaging_boundaries", ("session_id",)),
    ("remote_packaging_boundary_chunks", ("session_id", "chunk_index")),
)


def _digest_piece(digest, value):
    raw = str(value).encode("utf-8", errors="surrogatepass")
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)


def _backup_catalog_fingerprint_conn(conn):
    """Hash every relevant schema definition and row in stable key order."""
    digest = hashlib.sha256()
    for table, order_columns in _BACKUP_FINGERPRINT_TABLES:
        _digest_piece(digest, f"table:{table}")
        exists_row = conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL AS present",
            (f"public.{table}",)).fetchone()
        present = (exists_row["present"] if isinstance(exists_row, dict)
                   else exists_row[0])
        _digest_piece(digest, "present" if present else "absent")
        if not present:
            continue

        definitions = conn.execute(
            """SELECT a.attnum, a.attname,
                      format_type(a.atttypid,a.atttypmod) AS data_type,
                      a.attnotnull, a.attidentity, a.attgenerated,
                      pg_get_expr(d.adbin,d.adrelid) AS default_expr
               FROM pg_attribute a
               LEFT JOIN pg_attrdef d
                 ON d.adrelid=a.attrelid AND d.adnum=a.attnum
               WHERE a.attrelid=%s::regclass
                 AND a.attnum>0 AND NOT a.attisdropped
               ORDER BY a.attnum""", (f"public.{table}",)).fetchall()
        for definition in definitions:
            _digest_piece(digest, json.dumps(
                dict(definition) if isinstance(definition, dict)
                else list(definition), default=str, sort_keys=True,
                separators=(",", ":")))
        constraints = conn.execute(
            """SELECT conname, contype, convalidated,
                      pg_get_constraintdef(oid,true) AS definition
               FROM pg_constraint WHERE conrelid=%s::regclass
               ORDER BY conname""", (f"public.{table}",)).fetchall()
        for constraint in constraints:
            _digest_piece(digest, json.dumps(
                dict(constraint) if isinstance(constraint, dict)
                else list(constraint), default=str, sort_keys=True,
                separators=(",", ":")))

        order_sql = sql.SQL(", ").join(
            sql.Identifier(column) for column in order_columns)
        query = sql.SQL(
            "SELECT to_jsonb(t)::text AS payload FROM {} AS t ORDER BY {}"
        ).format(sql.Identifier(table), order_sql)
        cursor_name = f"backup_fingerprint_{uuid.uuid4().hex}"
        with conn.cursor(name=cursor_name) as cursor:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(10000)
                if not rows:
                    break
                for row in rows:
                    payload = row["payload"] if isinstance(row, dict) else row[0]
                    _digest_piece(digest, payload)
    return digest.hexdigest()


def _backup_catalog_fingerprint(cfg, conn=None):
    """Path-free exact catalog state tying a receipt to the dumped target."""
    require_psycopg()
    if conn is not None:
        return _backup_catalog_fingerprint_conn(conn)
    conninfo = build_conninfo(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_dbname,
        user=cfg.pg_user, password=cfg.pg_password, sslmode=cfg.pg_sslmode)
    with cast("psycopg.Connection", psycopg.connect(
            conninfo, autocommit=False,
            row_factory=cast(Any, dict_row))) as active_conn:
        active_conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        active_conn.execute("SET LOCAL TIME ZONE 'UTC'")
        return _backup_catalog_fingerprint_conn(active_conn)


def create_backup_receipt(
        cfg, backup_path, *, database_identity=None,
        catalog_fingerprint=None):
    """Write a hashed, target-bound receipt beside a verified custom dump."""
    backup = validate_backup_local_path(
        cfg, backup_path, expected_kind="file", allow_missing=False)
    restore_list = validate_backup_local_path(
        cfg, verify_backup_file(cfg, backup), expected_kind="file",
        allow_missing=False)
    payload = {
        "version": 2,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backup_file_name": backup.name,
        "backup_size_bytes": backup.stat().st_size,
        "backup_sha256": _sha256_file(backup),
        "restore_list_file_name": restore_list.name,
        "restore_list_size_bytes": restore_list.stat().st_size,
        "restore_list_sha256": _sha256_file(restore_list),
        "database_identity": (
            database_identity if database_identity is not None
            else _backup_database_identity(cfg)),
        "catalog_fingerprint": (
            catalog_fingerprint if catalog_fingerprint is not None
            else _backup_catalog_fingerprint(cfg)),
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload["receipt_checksum"] = hashlib.sha256(canonical).hexdigest()
    receipt = backup.with_suffix(".receipt.json")
    temporary = receipt.with_suffix(receipt.suffix + ".part")
    validate_backup_local_path(
        cfg, receipt, expected_kind="file", allow_missing=True)
    validate_backup_local_path(
        cfg, temporary, expected_kind="file", allow_missing=True)
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, receipt)
    return str(receipt)


def _windows_device_target(drive):
    """Resolve one drive letter without opening its filesystem."""
    if os.name != "nt":
        return drive.casefold()
    import ctypes

    buffer = ctypes.create_unicode_buffer(32768)
    result = ctypes.windll.kernel32.QueryDosDeviceW(
        drive.rstrip("\\/"), buffer, len(buffer))
    if not result:
        raise RuntimeError(
            "[DB BACKUP] A drive mapping is unreadable; local-path safety is "
            "indeterminate")
    targets = [item for item in buffer[:result].split("\0") if item]
    if len(targets) != 1:
        raise RuntimeError(
            "[DB BACKUP] A drive has multiple mappings; local-path safety is "
            "indeterminate")
    target = targets[0].casefold()
    if (not target.startswith("\\device\\")
            or target.startswith(("\\device\\mup\\",
                                  "\\device\\lanmanredirector\\"))):
        raise RuntimeError(
            "[DB BACKUP] Aliased or non-local drive mappings are refused")
    return target


def validate_backup_local_path(
        cfg, path, *, expected_kind="file", allow_missing=False):
    """Return an absolute plain-local path proved separate from LTFS.

    Device comparison is performed before any filesystem metadata call.  This
    is load-bearing for migration 015: even a backup path supplied as ``Z:\\``
    or a SUBST alias must be rejected without reading the cartridge.
    """
    text = os.fspath(path)
    absolute = os.path.abspath(text)
    if (absolute.startswith(("\\\\", "//"))
            or absolute.casefold().startswith(("\\\\?\\", "\\\\.\\"))):
        raise RuntimeError(
            "[DB BACKUP] Backup artifacts require a plain local drive path")
    drive, tail = os.path.splitdrive(absolute)
    if not drive or not tail.startswith(("\\", "/")):
        raise RuntimeError(
            "[DB BACKUP] Backup artifact path is not an absolute local path")

    lto_text = str(getattr(cfg, "lto_drive", "") or "").strip()
    if lto_text:
        lto_absolute = os.path.abspath(lto_text)
        if (lto_absolute.startswith(("\\\\", "//"))
                or lto_absolute.casefold().startswith(
                    ("\\\\?\\", "\\\\.\\"))):
            raise RuntimeError(
                "[DB BACKUP] LTFS mapping is aliased; backup-path safety is "
                "indeterminate")
        lto_drive = os.path.splitdrive(lto_absolute)[0]
        if not lto_drive:
            raise RuntimeError(
                "[DB BACKUP] LTFS drive mapping is indeterminate")
        if (drive.casefold() == lto_drive.casefold()
                or _windows_device_target(drive)
                == _windows_device_target(lto_drive)):
            raise RuntimeError(
                "[DB BACKUP] Refusing to read or write a backup artifact on "
                "the LTFS drive")
    else:
        # Still reject SUBST/network mappings even in isolated helper tests.
        _windows_device_target(drive)

    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    components = [part for part in tail.replace("/", "\\").split("\\")
                  if part]
    current = drive + os.sep
    missing_parent = False
    try:
        root_info = os.lstat(current)
        if (not stat.S_ISDIR(root_info.st_mode)
                or getattr(root_info, "st_file_attributes", 0) & reparse_flag):
            raise RuntimeError(
                "[DB BACKUP] Backup drive root is not a plain directory")
        for position, component in enumerate(components):
            current = os.path.join(current, component)
            is_leaf = position == len(components) - 1
            if missing_parent:
                continue
            try:
                info = os.lstat(current)
            except FileNotFoundError:
                if not allow_missing:
                    raise RuntimeError(
                        "[DB BACKUP] Required backup artifact is missing")
                missing_parent = True
                continue
            if getattr(info, "st_file_attributes", 0) & reparse_flag:
                raise RuntimeError(
                    "[DB BACKUP] Backup path contains a reparse point")
            if not is_leaf or expected_kind == "directory":
                if not stat.S_ISDIR(info.st_mode):
                    raise RuntimeError(
                        "[DB BACKUP] Backup path contains a non-directory "
                        "component")
            elif expected_kind == "file" and not stat.S_ISREG(info.st_mode):
                raise RuntimeError(
                    "[DB BACKUP] Backup artifact is not a regular file")
    except RuntimeError:
        raise
    except OSError as exc:
        raise RuntimeError(
            "[DB BACKUP] Backup path metadata is unreadable") from exc
    return Path(absolute)


def verify_backup_receipt(cfg, backup_path, *, max_age_seconds=24 * 60 * 60):
    """Fail closed unless the dump receipt matches this exact live target."""
    backup = validate_backup_local_path(
        cfg, backup_path, expected_kind="file", allow_missing=False)
    restore_list = validate_backup_local_path(
        cfg, verify_backup_file(cfg, backup), expected_kind="file",
        allow_missing=False)
    receipt_path = backup.with_suffix(".receipt.json")
    validate_backup_local_path(
        cfg, receipt_path, expected_kind="file", allow_missing=False)
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "[DB BACKUP] Target-bound backup receipt is missing or unreadable"
        ) from exc
    expected_keys = {
        "version", "created_at", "backup_file_name", "backup_size_bytes",
        "backup_sha256", "restore_list_file_name",
        "restore_list_size_bytes", "restore_list_sha256",
        "database_identity", "catalog_fingerprint",
        "receipt_checksum"}
    if set(payload) != expected_keys or payload.get("version") != 2:
        raise RuntimeError("[DB BACKUP] Backup receipt shape/version is invalid")
    checksum = payload.pop("receipt_checksum")
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if checksum != hashlib.sha256(canonical).hexdigest():
        raise RuntimeError("[DB BACKUP] Backup receipt checksum mismatch")
    try:
        created_at = datetime.fromisoformat(payload["created_at"])
        if created_at.tzinfo is None:
            raise ValueError
        age = (datetime.now(timezone.utc)
               - created_at.astimezone(timezone.utc)).total_seconds()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("[DB BACKUP] Backup receipt timestamp is invalid") from exc
    if age < -5 or age > max_age_seconds:
        raise RuntimeError("[DB BACKUP] Backup receipt is stale or future-dated")
    if (payload["backup_file_name"] != backup.name
            or int(payload["backup_size_bytes"]) != backup.stat().st_size
            or payload["backup_sha256"] != _sha256_file(backup)):
        raise RuntimeError("[DB BACKUP] Backup file does not match its receipt")
    if (payload["restore_list_file_name"] != restore_list.name
            or int(payload["restore_list_size_bytes"])
            != restore_list.stat().st_size
            or payload["restore_list_sha256"] != _sha256_file(restore_list)):
        raise RuntimeError(
            "[DB BACKUP] Restore-list verification does not match its receipt")
    if payload["database_identity"] != _backup_database_identity(cfg):
        raise RuntimeError("[DB BACKUP] Receipt targets a different database")
    if payload["catalog_fingerprint"] != _backup_catalog_fingerprint(cfg):
        raise RuntimeError(
            "[DB BACKUP] Live catalog changed after the guarded backup")
    return {
        "verified": True,
        "receipt_id": checksum,
        "backup_file_name": backup.name,
        "backup_size_bytes": backup.stat().st_size,
        "backup_sha256": payload["backup_sha256"],
        "created_at": payload["created_at"],
        "target_database": payload["database_identity"]["database"],
    }


def create_database_backup(
        cfg, output_dir=None, prefix=None, verify=False, snapshot_id=None):
    """Create a PostgreSQL custom-format dump and return its path."""
    target_dir = validate_backup_local_path(
        cfg, output_dir or DB_BACKUP_DIR, expected_kind="directory",
        allow_missing=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    validate_backup_local_path(
        cfg, target_dir, expected_kind="directory", allow_missing=False)
    output_path = validate_backup_local_path(
        cfg, target_dir / _backup_filename(cfg.pg_dbname, prefix=prefix),
        expected_kind="file", allow_missing=True)
    if output_path.exists():
        raise RuntimeError("[DB BACKUP] Refusing to overwrite an existing backup")
    temporary = validate_backup_local_path(
        cfg, output_path.with_suffix(output_path.suffix + ".part"),
        expected_kind="file", allow_missing=True)

    errors = []
    if _can_try_docker_backup(cfg):
        try:
            _backup_with_docker(cfg, temporary, snapshot_id=snapshot_id)
            validate_backup_local_path(
                cfg, temporary, expected_kind="file", allow_missing=False)
            os.replace(temporary, output_path)
            if verify:
                verify_backup_file(cfg, output_path)
            return str(output_path)
        except RuntimeError as exc:
            errors.append(f"Docker pg_dump failed: {exc}")
            temporary.unlink(missing_ok=True)
            output_path.unlink(missing_ok=True)

    try:
        _backup_with_local_pg_dump(cfg, temporary, snapshot_id=snapshot_id)
        validate_backup_local_path(
            cfg, temporary, expected_kind="file", allow_missing=False)
        os.replace(temporary, output_path)
        if verify:
            verify_backup_file(cfg, output_path)
        return str(output_path)
    except RuntimeError as exc:
        errors.append(f"Local pg_dump failed: {exc}")
        temporary.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)

    details = "\n".join(f"  - {err}" for err in errors)
    raise RuntimeError(
        "[DB BACKUP] Could not create PostgreSQL backup.\n"
        f"{details}\n"
        "[DB BACKUP] Start the local database with `docker compose up -d db`, "
        "or install PostgreSQL client tools so `pg_dump` is available."
    )


def create_verified_production_backup(cfg, output_dir=None):
    """Create a custom dump and receipt from one exported DB snapshot.

    The caller owns the cluster archiver lock for this whole operation.  The
    exported repeatable-read snapshot binds pg_dump, exact catalog digest, and
    database identity to one instant rather than querying a later live state.
    """
    require_psycopg()
    conninfo = build_conninfo(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_dbname,
        user=cfg.pg_user, password=cfg.pg_password, sslmode=cfg.pg_sslmode)
    with cast("psycopg.Connection", psycopg.connect(
            conninfo, autocommit=False,
            row_factory=cast(Any, dict_row))) as conn:
        conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        snapshot_row = conn.execute(
            "SELECT pg_export_snapshot() AS snapshot_id").fetchone()
        snapshot_id = snapshot_row["snapshot_id"]
        database_identity = _backup_database_identity(cfg, conn=conn)
        catalog_fingerprint = _backup_catalog_fingerprint(cfg, conn=conn)
        path = create_database_backup(
            cfg, output_dir=output_dir,
            prefix="prod_before_container_format", verify=False,
            snapshot_id=snapshot_id)
        receipt = create_backup_receipt(
            cfg, path, database_identity=database_identity,
            catalog_fingerprint=catalog_fingerprint)
        conn.commit()
    restore_list = str(Path(path).with_suffix(".restore_list.txt"))
    return {
        "database": cfg.pg_dbname,
        "target": cfg.db_display_ref,
        "backup_path": path,
        "restore_list_path": restore_list,
        "receipt_path": receipt,
    }


def _admin_conninfo(cfg, dbname="postgres"):
    return build_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname=dbname,
        user=cfg.pg_user,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
    )


def _database_exists(cfg, dbname):
    require_psycopg()
    with cast("psycopg.Connection", psycopg.connect(
            _admin_conninfo(cfg), autocommit=True)) as conn:
        row = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (dbname,)
        ).fetchone()
    return row is not None


def create_database(cfg, dbname):
    require_psycopg()
    if _database_exists(cfg, dbname):
        raise RuntimeError(f"[DB MIGRATION] Database already exists: {dbname}")
    with cast("psycopg.Connection", psycopg.connect(
            _admin_conninfo(cfg), autocommit=True)) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(dbname)))
    return dbname


def _restore_with_local_pg_restore(cfg, backup_path, dbname):
    if not shutil.which("pg_restore"):
        raise RuntimeError(
            "pg_restore is not on PATH. Install PostgreSQL client tools or "
            "use the local Docker database container."
        )
    _run_command([
        "pg_restore",
        "--no-owner",
        "--no-acl",
        "-d", dbname,
        str(backup_path),
    ], env=_pg_env(cfg, dbname=dbname))


def _restore_with_docker(cfg, backup_path, dbname):
    container_path = f"/tmp/{backup_path.name}"
    try:
        _run_command([
            "docker", "cp", str(backup_path),
            f"{DOCKER_DB_CONTAINER}:{container_path}",
        ])
        _run_command([
            "docker", "exec", DOCKER_DB_CONTAINER,
            "pg_restore",
            "-U", cfg.pg_user,
            "-d", dbname,
            "--no-owner",
            "--no-acl",
            container_path,
        ])
    finally:
        subprocess.run(
            ["docker", "exec", DOCKER_DB_CONTAINER, "rm", "-f", container_path],
            text=True,
            capture_output=True,
        )


def restore_backup_to_database(cfg, backup_path, dbname):
    backup_path = Path(backup_path).resolve()
    if not backup_path.exists() or backup_path.stat().st_size <= 0:
        raise RuntimeError(f"[DB MIGRATION] Backup is missing or empty: {backup_path}")
    errors = []
    if _can_try_docker_backup(cfg):
        try:
            _restore_with_docker(cfg, backup_path, dbname)
            return dbname
        except RuntimeError as exc:
            errors.append(f"Docker pg_restore failed: {exc}")
    try:
        _restore_with_local_pg_restore(cfg, backup_path, dbname)
        return dbname
    except RuntimeError as exc:
        errors.append(f"Local pg_restore failed: {exc}")
    details = "\n".join(f"  - {err}" for err in errors)
    raise RuntimeError(
        f"[DB MIGRATION] Could not restore {backup_path} into {dbname}.\n"
        f"{details}"
    )


def _timestamped_migration_db_name(prod_dbname, timestamp=None):
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{_safe_filename_part(prod_dbname, 'database')}_directory_catalog_{stamp}"


def apply_directory_catalog_schema_to_database(cfg, dbname):
    require_psycopg()
    path = Path(PROJECT_ROOT) / "scripts" / "sql" / "007_postgres_directory_catalog.sql"
    with cast("psycopg.Connection", psycopg.connect(
            _admin_conninfo(cfg, dbname=dbname), autocommit=True)) as conn:
        conn.execute(path.read_text(encoding="utf-8"))
    return dbname


def create_migrated_database_from_backup(cfg, backup_path, dbname=None):
    target_db = dbname or _timestamped_migration_db_name(cfg.pg_dbname)
    create_database(cfg, target_db)
    try:
        restore_backup_to_database(cfg, backup_path, target_db)
        apply_directory_catalog_schema_to_database(cfg, target_db)
    except Exception as exc:
        raise RuntimeError(
            "[DB MIGRATION] Migrated database creation failed. The partially "
            f"created database '{target_db}' was left in place for manual "
            "inspection; drop it manually only after confirming it is not needed."
        ) from exc
    return target_db
