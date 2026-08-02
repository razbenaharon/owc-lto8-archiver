"""Explicit, audited destructive reset of one mounted LTFS generation.

This is intentionally separate from :class:`src.ltfs.TapeManager` formatting.
It records immutable evidence before and after every irreversible boundary and
never deletes the shared ``tapes`` row or a remote session/plan/chunk row.
"""
import csv
import hashlib
import json
import os
import re
import stat
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .constants import DEFAULT_TAPE_CAPACITY_GB, LTFS_DIR, PROJECT_ROOT
from .ltfs import get_volume_label
from .runtime import _acquire_tape_io_lock, _release_tape_io_lock
from .windows_update_guard import ltfs_current_mount_status


LTFS_LOG = Path(r"C:\Program Files\IBM\LTFS\log\LogFile.csv")
SUMMARY = Path(PROJECT_ROOT) / "backup_logs" / "SUMMARY.csv"
AUDIT_ROOT = Path(PROJECT_ROOT) / "tape_reset_audits"
_PACK_RE = re.compile(r"_pack_s(\d+)_(\d+)(?:$|[\\/])")
_GEN_RE = re.compile(r"Gen\s*=\s*(\d+)")
_VCR_RE = re.compile(r"VCR\s*=\s*(\d+)")
_SERIAL_RE = re.compile(r"serial:'([^']+)'", re.I)
_DEVICE_RE = re.compile(r"driver:'([^']+)'", re.I)
_BARCODE_RE = re.compile(r"Barcode\s*=\s*(.*?)\s*\.$", re.I)
_LOCK_RE = re.compile(r"Volume Lock Status\s*=\s*(0x[0-9a-f]+)", re.I)


def _json_default(value):
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def canonical_json_bytes(data):
    return (json.dumps(data, default=_json_default, sort_keys=True,
                       separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_immutable_json(path, data):
    """Create one audit snapshot exactly once, then make it read-only."""
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(data)
    with open(path, "xb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(path, stat.S_IREAD)
    return str(path), hashlib.sha256(payload).hexdigest()


def read_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _ltfs_rows(since=None):
    cutoff = None
    if since:
        cutoff = datetime.fromisoformat(str(since))
        if cutoff.tzinfo is not None:
            cutoff = cutoff.astimezone().replace(tzinfo=None)
    rows = []
    with open(LTFS_LOG, "r", encoding="utf-8", errors="replace", newline="") as fh:
        for rec in csv.reader(fh):
            if len(rec) < 7:
                continue
            try:
                at = datetime.strptime(rec[2].strip(), "%Y/%m/%d %H:%M:%S.%f")
            except ValueError:
                continue
            if cutoff and at < cutoff:
                continue
            rows.append({"id": int(rec[0]), "at": rec[2].strip(),
                         "pid": rec[3].strip(), "drive": rec[5].strip(),
                         "message": rec[6].strip()})
    return rows


def mounted_identity(tape_drive):
    """Read label plus mount-scoped IBM identity evidence; ownership required."""
    mount = ltfs_current_mount_status()
    if not mount.get("determinate") or not mount.get("bound_to_current"):
        raise RuntimeError(f"[TAPE RESET] Current LTFS mount is unverifiable: {mount}")
    label = get_volume_label(tape_drive)
    rows = _ltfs_rows(mount["mount_started_at"])
    identity = {
        "label": label,
        "drive": tape_drive,
        "mount_started_at": mount["mount_started_at"],
        "sync_type": mount.get("sync_type"),
        "sync_seconds": mount.get("sync_seconds"),
        "drive_serial": None,
        "device": None,
        "barcode": None,
        "ltfs_generation": None,
        "vcr": None,
        "volume_lock_status": None,
        "evidence": [],
    }
    for row in rows:
        msg = row["message"]
        if row["id"] in (62182, 62160, 11333, 17227, 17228, 11031, 61223):
            identity["evidence"].append(row)
        m = _SERIAL_RE.search(msg)
        if m:
            identity["drive_serial"] = m.group(1).strip()
        m = _DEVICE_RE.search(msg)
        if m:
            identity["device"] = m.group(1).strip()
        m = _BARCODE_RE.search(msg)
        if m:
            identity["barcode"] = m.group(1).strip() or None
        m = _LOCK_RE.search(msg)
        if m:
            identity["volume_lock_status"] = m.group(1).lower()
        if row["id"] in (11333, 11031):
            gm = _GEN_RE.search(msg)
            vm = _VCR_RE.search(msg)
            if gm:
                identity["ltfs_generation"] = int(gm.group(1))
            if vm:
                identity["vcr"] = int(vm.group(1))
    if not identity["drive_serial"] or not identity["device"]:
        raise RuntimeError("[TAPE RESET] Could not prove drive serial/device")
    return identity


def latest_chunk_write_evidence(summary_path=SUMMARY):
    evidence = {}
    history = {}
    with open(summary_path, "r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            match = _PACK_RE.search(row.get("source_path") or "")
            if not match:
                continue
            key = (int(match.group(1)), int(match.group(2)))
            item = {field: row.get(field) for field in (
                "tape_label", "status", "source_path", "started_at",
                "finished_at", "copied_bytes", "planned_bytes",
                "tape_used_after_bytes", "robocopy_exit_code")}
            history.setdefault(key, []).append(item)
            if not (item["status"] or "").startswith("completed"):
                continue
            if key not in evidence or (item["finished_at"] or "") > (
                    evidence[key]["finished_at"] or ""):
                evidence[key] = item
    return evidence, history


def _table_exists(conn, table):
    return conn.execute(
        """SELECT 1 FROM information_schema.tables
           WHERE table_schema='public' AND table_name=%s""", (table,)
    ).fetchone() is not None


def _label_rows(conn, table, key_columns, label):
    if not _table_exists(conn, table):
        return []
    columns = ",".join(key_columns)
    return [dict(row) for row in conn.execute(
        f"SELECT {columns} FROM {table} WHERE tape_label=%s ORDER BY {columns}",
        (label,),
    ).fetchall()]


def _label_fingerprint(conn, table, key_column, label):
    if not _table_exists(conn, table):
        return {"count": 0, "min": None, "max": None, "sum": 0}
    row = conn.execute(
        f"""SELECT count(*) AS count,min({key_column}) AS min,
                   max({key_column}) AS max,
                   COALESCE(sum({key_column}),0) AS sum
            FROM {table} WHERE tape_label=%s""", (label,)
    ).fetchone()
    return dict(row)


def build_impact_report(db, *, operation_id, tape_label, identity,
                        backup, shadow_database):
    latest, history = latest_chunk_write_evidence()
    with db._pool.connection() as conn:
        tape = conn.execute(
            "SELECT * FROM tapes WHERE volume_label=%s", (tape_label,)
        ).fetchone()
        if not tape:
            raise RuntimeError(f"[TAPE RESET] Tape not found: {tape_label}")
        sessions = [dict(r) for r in conn.execute(
            """SELECT * FROM remote_sessions
               WHERE tape_label=%s OR session_id IN (
                 SELECT remote_session_id FROM archive_runs
                 WHERE tape_label=%s AND remote_session_id IS NOT NULL)
               ORDER BY session_id""", (tape_label, tape_label)
        ).fetchall()]
        session_ids = sorted({int(r["session_id"]) for r in sessions})
        chunks = []
        if session_ids:
            chunks = [dict(r) for r in conn.execute(
                """SELECT * FROM remote_chunks WHERE session_id=ANY(%s)
                   ORDER BY session_id,chunk_index""", (session_ids,)
            ).fetchall()]

        transitions = []
        unchanged = []
        chunk_evidence = []
        for chunk in chunks:
            key = (int(chunk["session_id"]), int(chunk["chunk_index"]))
            newest = latest.get(key)
            item = {"session_id": key[0], "chunk_index": key[1],
                    "before": chunk["status"], "latest_success": newest,
                    "write_history": history.get(key, [])}
            status = chunk["status"]
            if status == "done":
                if newest is None:
                    raise RuntimeError(f"[TAPE RESET] Done chunk {key} has no SUMMARY evidence")
                if newest["tape_label"] == tape_label:
                    transitions.append({"session_id": key[0],
                                        "chunk_index": key[1],
                                        "before": status, "after": "pending",
                                        "reason": "latest durable output erased"})
                else:
                    unchanged.append({"session_id": key[0],
                                      "chunk_index": key[1],
                                      "status": status,
                                      "preserved_tape": newest["tape_label"]})
            elif status in ("backing", "packing"):
                transitions.append({"session_id": key[0],
                                    "chunk_index": key[1],
                                    "before": status, "after": "pending",
                                    "reason": "ambiguous/incomplete target generation erased"})
            else:
                unchanged.append({"session_id": key[0],
                                  "chunk_index": key[1], "status": status})
            chunk_evidence.append(item)

        tape03_rows = {
            "archive_bundles": _label_rows(conn, "archive_bundles", ("bundle_id",), tape_label),
            "archive_runs": _label_rows(conn, "archive_runs", ("run_id", "remote_session_id", "local_session_id"), tape_label),
            "files_index": _label_rows(conn, "files_index", ("file_id",), tape_label),
            "catalog_directories": _label_rows(conn, "catalog_directories", ("directory_id",), tape_label),
            "directory_archive_stats": _label_rows(conn, "directory_archive_stats", ("stat_id", "remote_session_id", "chunk_index"), tape_label),
            "directory_archive_bundles": _label_rows(conn, "directory_archive_bundles", ("bundle_id", "remote_session_id", "chunk_index"), tape_label),
            "directory_tree_index": _label_rows(conn, "directory_tree_index", ("dir_id", "remote_session_id", "chunk_index", "bundle_id"), tape_label),
            "local_manifest_folder_aggregates": _label_rows(conn, "local_manifest_folder_aggregates", ("aggregate_id", "export_id"), tape_label),
            "local_manifest_export_rows": _label_rows(conn, "local_manifest_export_rows", ("export_id", "source_file_id"), tape_label),
            "local_chunks_manifest": _label_rows(conn, "local_chunks_manifest", ("manifest_id", "session_id", "chunk_index"), tape_label),
        }
        tape02_fingerprints = {
            table: _label_fingerprint(conn, table, key, "Tape_02")
            for table, key in (
                ("archive_bundles", "bundle_id"), ("archive_runs", "run_id"),
                ("files_index", "file_id"),
                ("catalog_directories", "directory_id"),
                ("directory_archive_stats", "stat_id"),
                ("directory_archive_bundles", "bundle_id"),
                ("directory_tree_index", "dir_id"),
                ("local_manifest_folder_aggregates", "aggregate_id"))
        }
        cascades = [dict(r) for r in conn.execute(
            """SELECT c.conname,c.conrelid::regclass::text AS child_table,
                      pg_get_constraintdef(c.oid) AS definition
               FROM pg_constraint c WHERE c.contype='f'
                 AND c.confrelid='tapes'::regclass
               ORDER BY child_table,c.conname""").fetchall()]
        db_identity = dict(conn.execute(
            """SELECT current_database() AS database,current_user AS username,
                      pg_database_size(current_database()) AS bytes""").fetchone())

    return {
        "schema_version": 1,
        "operation_id": operation_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "target_label": tape_label,
        "mounted_identity": identity,
        "database": db_identity,
        "backup": backup,
        "shadow_database": shadow_database,
        "tape_row": dict(tape),
        "sessions": sessions,
        "chunks": chunks,
        "chunk_write_evidence": chunk_evidence,
        "planned_chunk_transitions": transitions,
        "unchanged_chunks": unchanged,
        "tape03_rows": tape03_rows,
        "tape03_counts": {k: len(v) for k, v in tape03_rows.items()},
        "tape02_fingerprints": tape02_fingerprints,
        "tape_foreign_keys_before_migration": cascades,
        "preservation_rules": {
            "remote_sessions": "preserve",
            "remote_chunks": "preserve rows; change only listed statuses",
            "remote_plans_and_snapshots": "preserve",
            "local_manifest_export_rows": "preserve source-side reconstruction evidence",
            "Tape_02": "preserve all rows and fingerprints",
        },
    }


def verify_backup_inputs(backup_path, restore_list_path):
    backup_path = Path(backup_path).resolve()
    restore_list_path = Path(restore_list_path).resolve()
    if not backup_path.is_file() or backup_path.stat().st_size <= 0:
        raise RuntimeError("[TAPE RESET] Backup is missing or empty")
    data_lines = [line for line in restore_list_path.read_text(
        encoding="utf-8").splitlines() if line and not line.startswith(";")]
    if not data_lines:
        raise RuntimeError("[TAPE RESET] Restore list is missing or empty")
    return {
        "backup_path": str(backup_path),
        "backup_bytes": backup_path.stat().st_size,
        "backup_sha256": sha256_file(backup_path),
        "restore_list_path": str(restore_list_path),
        "restore_list_bytes": restore_list_path.stat().st_size,
        "restore_list_sha256": sha256_file(restore_list_path),
        "restore_list_entries": len(data_lines),
    }


def positive_format_evidence_since(tape_drive, label, started_at, *,
                                   command=None, returncode=None, output=""):
    """Confirm a completed format from IBM's own mount/index evidence.

    IBM SDE can return an error from its management wrapper *after* ``mkltfs``
    logged a successful format and the new volume mounted.  The physical facts
    win: require the successful formatter event, a new mounted index, lock
    status 0x00 and no read-only event in the operation window.
    """
    identity = mounted_identity(tape_drive)
    if identity["label"] != label:
        raise RuntimeError("[TAPE RESET] Newly mounted label does not match")
    new_rows = _ltfs_rows(started_at)
    formatted = [r for r in new_rows
                 if r["id"] == 15024 and "formatted successfully" in
                 r["message"].lower()]
    mounted = [r for r in new_rows if r["id"] == 11031]
    lock_ok = [r for r in new_rows
               if r["id"] == 17228 and "0x00" in r["message"]]
    volume_uuid = [r for r in new_rows
                   if r["id"] == 15013 and "Volume UUID is:" in r["message"]]
    readonly = [r for r in new_rows if r["id"] in (11333, 61223)]
    if not (formatted and mounted and lock_ok and volume_uuid) or readonly:
        raise RuntimeError(
            "[TAPE RESET] Positive LTFS format evidence is incomplete: "
            f"formatted={len(formatted)} mounted={len(mounted)} "
            f"lock_ok={len(lock_ok)} uuid={len(volume_uuid)} "
            f"readonly={len(readonly)}")
    return {
        "command": command or [],
        "returncode": returncode,
        "management_wrapper_reported_error": bool(returncode),
        "output": output,
        "started_at": started_at,
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
        "mounted_identity": identity,
        "positive_events": formatted + volume_uuid + mounted + lock_ok,
        "readonly_events": readonly,
        "ejected": False,
    }


def _format_and_positive_evidence(tape_drive, label, operation_id):
    start = datetime.now(timezone.utc)
    exe = Path(LTFS_DIR) / "LtfsCmdFormat.exe"
    command = [str(exe), tape_drive.rstrip(":\\/"), f"/N:{label}"]
    result = subprocess.run(command, cwd=LTFS_DIR, text=True,
                            capture_output=True)
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    deadline = time.monotonic() + 600
    last_error = None
    while time.monotonic() < deadline:
        try:
            return positive_format_evidence_since(
                tape_drive, label, start.isoformat(), command=command,
                returncode=result.returncode, output=output)
        except Exception as exc:
            last_error = str(exc)
        time.sleep(5)
    raise RuntimeError(
        "[TAPE RESET] Positive LTFS format/mount/writable-lock evidence was "
        f"not observed (formatter rc={result.returncode}): {last_error}")


def smallest_write_test(tape_drive, operation_id):
    root = Path(tape_drive)
    before = sorted(p.name for p in root.iterdir())
    if before:
        raise RuntimeError(f"[TAPE RESET] Formatted root is not empty: {before}")
    path = root / f"_tape_reset_write_test_{operation_id}.tmp"
    with open(path, "xb", buffering=0) as fh:
        fh.write(b"1")
        os.fsync(fh.fileno())
    os.remove(path)
    after = sorted(p.name for p in root.iterdir())
    if after:
        raise RuntimeError(f"[TAPE RESET] Temporary write-test cleanup failed: {after}")
    return {"path": str(path), "bytes_written": 1, "create_succeeded": True,
            "delete_succeeded": True, "root_empty_before": True,
            "root_empty_after": True, "read_back_performed": False}


def audit_dir(operation_id):
    return AUDIT_ROOT / operation_id


def new_operation_id():
    return f"tape03-reset-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"


def _forbidden_processes(current_pid=None):
    script = (
        "$p=Get-CimInstance Win32_Process | Where-Object {"
        "$_.Name -ieq 'Robocopy.exe' -or $_.Name -like 'LtfsCmd*.exe' -or "
        "($_.Name -ieq 'python.exe' -and $_.CommandLine -match "
        "'(?i)(run\\.py|inspect_db\\.py|storage_map\\\\run_app\\.py)')};"
        "$p | Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress")
    proc = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"[TAPE RESET] Process query failed: {proc.stderr}")
    text = (proc.stdout or "").strip()
    if not text:
        return []
    data = json.loads(text)
    if isinstance(data, dict):
        data = [data]
    return [item for item in data
            if int(item.get("ProcessId") or -1) != int(current_pid or os.getpid())]


def _impact_core(report):
    return {
        "target_label": report["target_label"],
        "tape_id": report["tape_row"]["tape_id"],
        "tape03_rows": report["tape03_rows"],
        "tape02_fingerprints": report["tape02_fingerprints"],
        "planned_chunk_transitions": report["planned_chunk_transitions"],
        "chunk_statuses": [
            (int(c["session_id"]), int(c["chunk_index"]), c["status"])
            for c in report["chunks"]],
        "session_ids": [int(s["session_id"]) for s in report["sessions"]],
    }


def prepare_impact_file(db, cfg, *, operation_id, tape_label, backup,
                        shadow_database, shadow_verification):
    """Read-only preparation helper. Caller does not need a migrated schema."""
    _acquire_tape_io_lock("prepare destructive tape-reset impact")
    try:
        identity = mounted_identity(cfg.lto_drive)
    finally:
        _release_tape_io_lock("prepare destructive tape-reset impact")
    if identity["label"] != tape_label:
        raise RuntimeError(
            f"[TAPE RESET] Mounted label {identity['label']!r} != {tape_label!r}")
    backup = dict(backup)
    backup["shadow_verified"] = True
    backup["shadow_verification"] = shadow_verification
    report = build_impact_report(
        db, operation_id=operation_id, tape_label=tape_label,
        identity=identity, backup=backup, shadow_database=shadow_database)
    path = audit_dir(operation_id) / "impact.json"
    written, digest = write_immutable_json(path, report)
    return written, digest, report


def execute_reset(db, cfg, *, tape_label, impact_path, delete_catalog,
                  confirmed, resume_operation=None):
    """Run or resume the explicit destructive reset state machine."""
    if not confirmed or not delete_catalog:
        raise RuntimeError(
            "[TAPE RESET] Both --delete-catalog and --yes are mandatory")
    impact_path = Path(impact_path).resolve()
    impact = read_json(impact_path)
    operation_id = resume_operation or impact["operation_id"]
    if operation_id != impact["operation_id"]:
        raise RuntimeError("[TAPE RESET] Resume operation does not match impact report")
    if tape_label != impact["target_label"]:
        raise RuntimeError("[TAPE RESET] CLI tape label does not match impact report")
    if sha256_file(impact_path) != hashlib.sha256(
            canonical_json_bytes(impact)).hexdigest():
        raise RuntimeError("[TAPE RESET] Impact report canonical hash mismatch")

    backup = verify_backup_inputs(
        impact["backup"]["backup_path"],
        impact["backup"]["restore_list_path"])
    for key in ("backup_sha256", "restore_list_sha256"):
        if backup[key] != impact["backup"][key]:
            raise RuntimeError(f"[TAPE RESET] Backup evidence changed: {key}")
    if not impact["backup"].get("shadow_verified"):
        raise RuntimeError("[TAPE RESET] Impact report lacks verified shadow restore")

    busy = _forbidden_processes(os.getpid())
    if busy:
        raise RuntimeError(f"[TAPE RESET] Refusing while processes are active: {busy}")

    db.acquire_archiver_lock()
    _acquire_tape_io_lock(f"destructive reset {operation_id}")
    try:
        identity = mounted_identity(cfg.lto_drive)
        op = db.get_tape_reset_operation(operation_id)
        hardware_path = audit_dir(operation_id) / "hardware_result.json"
        post_format = bool(hardware_path.exists() or (
            op and op["state"] in
            ("hardware_succeeded", "cleanup_succeeded", "verified")))
        expected = (read_json(hardware_path)["mounted_identity"]
                    if post_format and hardware_path.exists()
                    else impact["mounted_identity"])
        identity_keys = ["label", "drive", "drive_serial", "device",
                         "ltfs_generation"]
        if not post_format:
            identity_keys.append("vcr")
        for key in identity_keys:
            if identity.get(key) != expected.get(key):
                raise RuntimeError(
                    f"[TAPE RESET] Mounted identity changed at {key}: "
                    f"{identity.get(key)!r} != {expected.get(key)!r}")

        current = build_impact_report(
            db, operation_id=operation_id, tape_label=tape_label,
            identity=identity, backup=impact["backup"],
            shadow_database=impact["shadow_database"])
        if canonical_json_bytes(_impact_core(current)) != canonical_json_bytes(
                _impact_core(impact)):
            raise RuntimeError(
                "[TAPE RESET] Observed deletion scope differs from impact report")

        if op is None:
            op = db.create_tape_reset_intent(
                operation_id=operation_id, tape_label=tape_label,
                expected_drive=identity["drive"],
                expected_drive_serial=identity["drive_serial"],
                expected_ltfs_gen=identity["ltfs_generation"],
                expected_vcr=identity["vcr"],
                backup_path=backup["backup_path"],
                backup_sha256=backup["backup_sha256"],
                restore_list_path=backup["restore_list_path"],
                restore_list_sha256=backup["restore_list_sha256"],
                shadow_database=impact["shadow_database"],
                impact_report_path=str(impact_path),
                impact_report_sha256=sha256_file(impact_path), impact=impact)

        directory = audit_dir(operation_id)
        hardware_path = directory / "hardware_result.json"
        cleanup_path = directory / "cleanup_result.json"
        verification_path = directory / "verification.json"

        if op["state"] in ("intent_recorded", "hardware_started"):
            if hardware_path.exists():
                hardware = read_json(hardware_path)
            else:
                db.mark_tape_reset_hardware_started(operation_id)
                hardware = _format_and_positive_evidence(
                    cfg.lto_drive, tape_label, operation_id)
                write_immutable_json(hardware_path, hardware)
            # External evidence is persisted first.  A DB failure here leaves
            # the resume command able to converge without formatting again.
            db.mark_tape_reset_hardware_succeeded(operation_id, hardware)
            op = db.get_tape_reset_operation(operation_id)

        if op["state"] == "hardware_succeeded":
            cleanup = db.apply_tape_reset_cleanup(
                operation_id, impact["planned_chunk_transitions"])
            if not cleanup_path.exists():
                write_immutable_json(cleanup_path, cleanup)
            op = db.get_tape_reset_operation(operation_id)
        else:
            cleanup = read_json(cleanup_path) if cleanup_path.exists() else (
                op.get("cleanup_evidence") or {})

        if op["state"] == "cleanup_succeeded":
            if verification_path.exists():
                verification = read_json(verification_path)
            else:
                verification = {
                    "verified_at": datetime.now(timezone.utc).isoformat(),
                    "mounted_identity": mounted_identity(cfg.lto_drive),
                    "write_test": smallest_write_test(cfg.lto_drive, operation_id),
                    "ejected": False,
                    "backup_session_started": False,
                }
                write_immutable_json(verification_path, verification)
            db.mark_tape_reset_verified(operation_id, verification)

        final = {
            "operation_id": operation_id,
            "state": db.get_tape_reset_operation(operation_id)["state"],
            "impact_path": str(impact_path),
            "impact_sha256": sha256_file(impact_path),
            "hardware_path": str(hardware_path),
            "hardware_sha256": sha256_file(hardware_path),
            "cleanup_path": str(cleanup_path),
            "cleanup_sha256": sha256_file(cleanup_path),
            "verification_path": str(verification_path),
            "verification_sha256": sha256_file(verification_path),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "ejected": False,
            "backup_session_started": False,
        }
        final_path = directory / "final_report.json"
        if not final_path.exists():
            write_immutable_json(final_path, final)
        return final
    except Exception as exc:
        directory = audit_dir(operation_id)
        failure = directory / f"failure_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            write_immutable_json(failure, {
                "operation_id": operation_id,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
                "resume_command": (
                    f"python run.py tape-reset --tape {tape_label} "
                    f"--delete-catalog --yes --impact-report {impact_path} "
                    f"--resume-operation {operation_id}"),
            })
        except Exception:
            pass
        raise
    finally:
        _release_tape_io_lock(f"destructive reset {operation_id}")
        db.release_archiver_lock()
