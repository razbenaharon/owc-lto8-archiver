"""PostgreSQL catalog backup helpers."""
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from .constants import DB_BACKUP_DIR


DOCKER_DB_CONTAINER = "lto_pg"
PG_DUMP_TIMEOUT_SECONDS = 6 * 60 * 60


def _safe_filename_part(value, fallback):
    text = str(value or "").strip()
    safe = "".join(
        ch if ch.isalnum() or ch in ("-", "_", ".") else "_"
        for ch in text
    ).strip("._")
    return safe or fallback


def _backup_filename(dbname, timestamp=None):
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{_safe_filename_part(dbname, 'database')}_{stamp}.dump"


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

    env = os.environ.copy()
    env.update({
        "PGHOST": cfg.pg_host,
        "PGPORT": cfg.pg_port,
        "PGDATABASE": cfg.pg_dbname,
        "PGUSER": cfg.pg_user,
        "PGSSLMODE": cfg.pg_sslmode,
    })
    if cfg.pg_password:
        env["PGPASSWORD"] = cfg.pg_password

    _run_command([
        "pg_dump",
        "-F", "c",
        "--no-owner",
        "--no-acl",
        "-f", str(output_path),
    ], env=env)


def create_database_backup(cfg, output_dir=None):
    """Create a PostgreSQL custom-format dump and return its path."""
    target_dir = Path(output_dir or DB_BACKUP_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = (target_dir / _backup_filename(cfg.pg_dbname)).resolve()

    errors = []
    if _can_try_docker_backup(cfg):
        try:
            _backup_with_docker(cfg, output_path)
            return str(output_path)
        except RuntimeError as exc:
            errors.append(f"Docker pg_dump failed: {exc}")
            if output_path.exists():
                output_path.unlink()

    try:
        _backup_with_local_pg_dump(cfg, output_path)
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
