"""PostgreSQL catalog backup helpers."""
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg import sql
else:
    try:
        import psycopg
        from psycopg import sql
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
    return _is_loopback_host(getattr(cfg, "pg_host", "")) and _docker_container_running()


def _backup_with_docker(cfg, output_path):
    container_path = f"/tmp/{output_path.name}"
    try:
        _run_command([
            "docker", "exec", DOCKER_DB_CONTAINER,
            "pg_dump",
            "-U", cfg.pg_user,
            "-d", cfg.pg_dbname,
            "-F", "c",
            "--no-owner",
            "--no-acl",
            "-f", container_path,
        ])
        _run_command([
            "docker", "cp",
            f"{DOCKER_DB_CONTAINER}:{container_path}",
            str(output_path),
        ])
    finally:
        subprocess.run(
            ["docker", "exec", DOCKER_DB_CONTAINER, "rm", "-f", container_path],
            text=True,
            capture_output=True,
        )


def _backup_with_local_pg_dump(cfg, output_path):
    if not shutil.which("pg_dump"):
        raise RuntimeError(
            "pg_dump is not on PATH. Install PostgreSQL client tools or start "
            "the local Docker database container."
        )

    _run_command([
        "pg_dump",
        "-F", "c",
        "--no-owner",
        "--no-acl",
        "-f", str(output_path),
    ], env=_pg_env(cfg))


def _restore_list_with_docker(backup_path, restore_list_path):
    container_path = f"/tmp/{backup_path.name}"
    try:
        _run_command([
            "docker", "cp", str(backup_path),
            f"{DOCKER_DB_CONTAINER}:{container_path}",
        ])
        result = _run_command([
            "docker", "exec", DOCKER_DB_CONTAINER,
            "pg_restore", "--list", container_path,
        ])
        restore_list_path.write_text(result.stdout, encoding="utf-8")
    finally:
        subprocess.run(
            ["docker", "exec", DOCKER_DB_CONTAINER, "rm", "-f", container_path],
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
    backup_path = Path(backup_path).resolve()
    if not backup_path.exists() or backup_path.stat().st_size <= 0:
        raise RuntimeError(f"[DB BACKUP] Backup is missing or empty: {backup_path}")
    restore_list_path = backup_path.with_suffix(".restore_list.txt")
    errors = []
    if _can_try_docker_backup(cfg):
        try:
            _restore_list_with_docker(backup_path, restore_list_path)
        except RuntimeError as exc:
            errors.append(f"Docker pg_restore --list failed: {exc}")
    if not restore_list_path.exists():
        try:
            _restore_list_with_local_pg_restore(backup_path, restore_list_path)
        except RuntimeError as exc:
            errors.append(f"Local pg_restore --list failed: {exc}")
    if not restore_list_path.exists() or restore_list_path.stat().st_size <= 0:
        details = "\n".join(f"  - {err}" for err in errors)
        raise RuntimeError(
            "[DB BACKUP] Backup file exists but restore-list verification "
            f"failed.\n{details}"
        )
    return str(restore_list_path)


def create_database_backup(cfg, output_dir=None, prefix=None, verify=False):
    """Create a PostgreSQL custom-format dump and return its path."""
    target_dir = Path(output_dir or DB_BACKUP_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = (target_dir / _backup_filename(
        cfg.pg_dbname, prefix=prefix)).resolve()

    errors = []
    if _can_try_docker_backup(cfg):
        try:
            _backup_with_docker(cfg, output_path)
            if verify:
                verify_backup_file(cfg, output_path)
            return str(output_path)
        except RuntimeError as exc:
            errors.append(f"Docker pg_dump failed: {exc}")
            if output_path.exists():
                output_path.unlink()

    try:
        _backup_with_local_pg_dump(cfg, output_path)
        if verify:
            verify_backup_file(cfg, output_path)
        return str(output_path)
    except RuntimeError as exc:
        errors.append(f"Local pg_dump failed: {exc}")
        if output_path.exists():
            output_path.unlink()

    details = "\n".join(f"  - {err}" for err in errors)
    raise RuntimeError(
        "[DB BACKUP] Could not create PostgreSQL backup.\n"
        f"{details}\n"
        "[DB BACKUP] Start the local database with `docker compose up -d db`, "
        "or install PostgreSQL client tools so `pg_dump` is available."
    )


def create_verified_production_backup(cfg, output_dir=None):
    path = create_database_backup(
        cfg, output_dir=output_dir,
        prefix="prod_before_directory_catalog", verify=True)
    restore_list = str(Path(path).with_suffix(".restore_list.txt"))
    return {
        "database": cfg.pg_dbname,
        "target": cfg.db_display_ref,
        "backup_path": path,
        "restore_list_path": restore_list,
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
    except Exception:
        raise RuntimeError(
            "[DB MIGRATION] Migrated database creation failed. The partially "
            f"created database '{target_db}' was left in place for manual "
            "inspection; drop it manually only after confirming it is not needed."
        )
    return target_db
