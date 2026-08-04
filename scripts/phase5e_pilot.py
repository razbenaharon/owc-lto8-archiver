"""One-shot Phase 5E grouped-write pilot on disposable isolated state.

This harness deliberately exposes only three commands:

* ``prepare`` creates deterministic local data and an isolated PostgreSQL DB.
  It has no LTFS imports or device access.
* ``run`` selects one finite ReadyQueue group, observes it asynchronously,
  executes that one group through RemoteOrchestrator._write_chunk_group, then
  exits. It never loops, retries, ejects, remounts, formats, or reads back.
* ``capture-idle`` reads only already-existing local log files after the pilot.

The normal production configuration and production catalog are read-only
tripwires. All pilot writes go to a database whose name starts with
``phase5e_pilot_`` and to synthetic source paths rooted at ``/phase5e-pilot``.
"""
from __future__ import annotations

import argparse
import configparser
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.backup import _NoEjectBackup
from src.config import ConfigManager
from src.constants import PROJECT_ROOT as APP_PROJECT_ROOT
from src.db import create_database_manager
from src.exit_codes import (ExitCode, REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            StopResult)
from src.logsetup import configure_file_logging, get_logger
from src.pipeline_types import StagedChunk
from src.ready_queue import ReadyItem, ReadyQueue
from src.runtime import CANCEL


PILOT_PREFIX = "phase5e_pilot_"
PRODUCTION_DB = "lto_archive_directory_catalog_20260710_103359"
TAPE_LABEL = "Tape_03"
EXPECTED = {
    "label": TAPE_LABEL,
    "drive_serial": "0000000000",
    "volume_uuid": "55e10b7b-49a9-4f2d-a7c7-7586af1ec6a4",
    "ltfs_generation": 1,
    "volume_lock_status": "0x00",
    "database_tape_id": 37,
}
CHUNK_IDS = (50000, 50001, 50002)
CHUNK_BYTES = 8 * 1024 ** 3
TOTAL_BYTES = len(CHUNK_IDS) * CHUNK_BYTES
LTFS_LOG = Path(r"C:\Program Files\IBM\LTFS\log\LogFile.csv")
STATE_NAME = "pilot_state.json"
RESULT_NAME = "pilot_result.json"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(value, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _cfg_root(cfg):
    return Path(cfg.source_dir).resolve().parent


def _require_isolated(cfg):
    if not cfg.pg_dbname.startswith(PILOT_PREFIX):
        raise RuntimeError(f"pilot database must start with {PILOT_PREFIX!r}")
    if cfg.pg_dbname in {PRODUCTION_DB, "lto_archive"}:
        raise RuntimeError("refusing a production database")
    root = _cfg_root(cfg)
    if root == Path(APP_PROJECT_ROOT).resolve() or Path(APP_PROJECT_ROOT).resolve() in root.parents:
        raise RuntimeError("pilot data root must be outside the repository")
    if Path(cfg.staging_dir).resolve() == Path(cfg.source_dir).resolve():
        raise RuntimeError("source and staging must be separate directories")
    if cfg.sealed_tape_write_batches_enabled:
        raise RuntimeError("sealed batches must remain disabled")
    if not cfg.sealed_tape_write_batches_observation_enabled:
        raise RuntimeError("pilot observation must be enabled")


def _production_snapshot():
    cfg = ConfigManager()
    if cfg.pg_dbname != PRODUCTION_DB:
        raise RuntimeError(f"normal config points to {cfg.pg_dbname!r}, not {PRODUCTION_DB!r}")
    if cfg.sealed_tape_write_batches_enabled or cfg.sealed_tape_write_batches_observation_enabled:
        raise RuntimeError("normal production feature flags are not both false")
    with psycopg.connect(cfg.db_dsn, connect_timeout=5) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
            session = conn.execute(
                "SELECT session_id,status,scan_complete,tape_label,tape_generation "
                "FROM remote_sessions WHERE session_id=37").fetchone()
            counts = conn.execute(
                "SELECT status,count(*) FROM remote_chunks WHERE session_id=37 "
                "GROUP BY status ORDER BY status").fetchall()
            early = conn.execute(
                "SELECT count(*) FROM remote_chunks WHERE session_id=37 "
                "AND chunk_index BETWEEN 0 AND 48 AND status='done'").fetchone()[0]
            late = conn.execute(
                "SELECT count(*) FROM remote_chunks WHERE session_id=37 "
                "AND chunk_index BETWEEN 49 AND 112 AND status='pending'").fetchone()[0]
            c108 = conn.execute(
                "SELECT status FROM remote_chunks WHERE session_id=37 "
                "AND chunk_index=108").fetchone()
            tape = conn.execute(
                "SELECT tape_id,volume_label,total_capacity,used_space,status,"
                "current_generation FROM tapes WHERE volume_label=%s",
                (TAPE_LABEL,)).fetchone()
            sealed = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name IN "
                "('tape_write_batches','tape_write_batch_chunks',"
                "'tape_write_active_chunk') ORDER BY table_name").fetchall()
    snap = {
        "database": cfg.pg_dbname,
        "session37": list(session or ()),
        "status_counts": [list(x) for x in counts],
        "done_0_48": early,
        "pending_49_112": late,
        "chunk108": list(c108 or ()),
        "tape03": list(tape or ()),
        "sealed_batch_tables": [x[0] for x in sealed],
    }
    if not (session and session[1:] == ("active", False, TAPE_LABEL, 1)):
        raise RuntimeError(f"corrected Session 37 state changed: {snap}")
    if early != 49 or late != 64 or c108 != ("pending",):
        raise RuntimeError(f"corrected chunk distribution changed: {snap}")
    if not tape or tape[0] != EXPECTED["database_tape_id"] or tape[1] != TAPE_LABEL:
        raise RuntimeError(f"Tape_03 production identity changed: {snap}")
    if sealed:
        raise RuntimeError(f"sealed-batch production tables exist: {sealed}")
    return snap


def _create_pilot_database(cfg):
    parts = conninfo_to_dict(cfg.db_dsn)
    maintenance = dict(parts)
    maintenance["dbname"] = "postgres"
    with psycopg.connect(make_conninfo(**maintenance), autocommit=True,
                         connect_timeout=5) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname=%s",
                              (cfg.pg_dbname,)).fetchone()
        if exists:
            raise RuntimeError(f"pilot database already exists: {cfg.pg_dbname}")
        conn.execute(f'CREATE DATABASE "{cfg.pg_dbname}"')


def _generate_file(path, chunk_id):
    """Write deterministic, effectively incompressible data and hash it inline.

    A 256 MiB SHAKE-256 block is repeated 32 times. The repetition interval is
    far beyond ordinary streaming compression windows, while generation stays
    bounded to 256 MiB of RAM on this 16 GiB host.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise RuntimeError(f"refusing to overwrite existing pilot data: {path}")
    seed = f"OWC-LTO8-PHASE5E-v1-chunk-{chunk_id}".encode("ascii")
    block = hashlib.shake_256(seed).digest(256 * 1024 * 1024)
    digest = hashlib.sha256()
    with open(path, "xb", buffering=0) as fh:
        for _ in range(CHUNK_BYTES // len(block)):
            fh.write(block)
            digest.update(block)
        fh.flush()
        os.fsync(fh.fileno())
    if path.stat().st_size != CHUNK_BYTES:
        raise RuntimeError(f"wrong generated size for {path}")
    return digest.hexdigest()


def prepare(cfg):
    _require_isolated(cfg)
    root = _cfg_root(cfg)
    if root.exists():
        raise RuntimeError(f"pilot root already exists; refusing overwrite: {root}")
    root.mkdir(parents=True)
    Path(cfg.source_dir).mkdir()
    Path(cfg.staging_dir).mkdir()
    Path(cfg.backup_log_dir).mkdir()
    Path(cfg.sealed_batch_observation_log_path).parent.mkdir()
    shutil.copy2(cfg.config_path, root / "phase5e_pilot.ini")

    prod = _production_snapshot()
    write_json(root / "production_state_before.json", prod)
    _create_pilot_database(cfg)
    db = create_database_manager(cfg)
    manifest = []
    try:
        db.register_tape(TAPE_LABEL, 12288)
        session_id = db.create_remote_streaming_session(
            "PHASE5E_ISOLATED_20260802_01", "phase5e-pilot.invalid",
            "phase5e-pilot", "/phase5e-pilot", TAPE_LABEL, cfg.staging_dir)
        for chunk_id in CHUNK_IDS:
            source_dir = Path(cfg.source_dir) / f"chunk_{chunk_id}"
            source_file = source_dir / "payload.bin"
            sha = _generate_file(source_file, chunk_id)
            pack_dir = Path(cfg.staging_dir) / f"_phase5e_pack_{session_id}_{chunk_id}"
            pack_dir.mkdir()
            pack_file = pack_dir / "payload.bin"
            os.link(source_file, pack_file)
            remote_path = f"/phase5e-pilot/chunk_{chunk_id}/payload.bin"
            db.append_remote_streaming_chunk(
                session_id, chunk_id,
                [(chunk_id, remote_path, "payload.bin", CHUNK_BYTES)])
            manifest.append({
                "chunk_id": chunk_id,
                "relative_source_path": str(source_file.relative_to(root)).replace("\\", "/"),
                "relative_pack_path": str(pack_file.relative_to(root)).replace("\\", "/"),
                "bytes": CHUNK_BYTES,
                "sha256": sha,
                "remote_path": remote_path,
                "generation": "SHAKE-256 deterministic block v1",
            })
        state = {
            "prepared_at": utc_now(),
            "pilot_root": str(root),
            "pilot_database": cfg.pg_dbname,
            "pilot_session_id": session_id,
            "tape_label": TAPE_LABEL,
            "chunk_ids": list(CHUNK_IDS),
            "chunk_bytes": CHUNK_BYTES,
            "total_bytes": TOTAL_BYTES,
            "manifest": manifest,
            "production_snapshot_sha256": sha256_file(root / "production_state_before.json"),
        }
        write_json(root / "source_manifest.json", manifest)
        write_json(root / STATE_NAME, state)
        print(json.dumps(state, indent=2))
    finally:
        db.close()


def _load_state(cfg):
    root = _cfg_root(cfg)
    with open(root / STATE_NAME, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    if state["pilot_database"] != cfg.pg_dbname:
        raise RuntimeError("pilot state/database mismatch")
    if tuple(state["chunk_ids"]) != CHUNK_IDS or state["total_bytes"] != TOTAL_BYTES:
        raise RuntimeError("pilot membership or byte total changed")
    return state


def _verify_prepared_files(cfg, state):
    root = _cfg_root(cfg)
    for item in state["manifest"]:
        source = root / item["relative_source_path"]
        pack = root / item["relative_pack_path"]
        if source.stat().st_size != item["bytes"] or pack.stat().st_size != item["bytes"]:
            raise RuntimeError(f"pilot file size changed: {item}")
        if not os.path.samefile(source, pack):
            raise RuntimeError(f"prepared pack is not the retained source hardlink: {item}")
        actual = sha256_file(source)
        if actual != item["sha256"]:
            raise RuntimeError(f"pilot source checksum changed: {source}")


def _forbidden_processes():
    script = (
        "$p=Get-CimInstance Win32_Process | Where-Object {"
        "$_.Name -match '^(Robocopy|pytest|LtfsCmd.*)\\.exe$' -or "
        "($_.Name -eq 'python.exe' -and $_.CommandLine -match '(run\\.py|inspect_db\\.py)')};"
        "$p | Select-Object Name,ProcessId,CommandLine | ConvertTo-Json -Compress")
    proc = subprocess.run(["powershell", "-NoProfile", "-Command", script],
                          text=True, capture_output=True, timeout=20)
    if proc.returncode:
        raise RuntimeError(f"could not inspect processes: {proc.stderr}")
    text = proc.stdout.strip()
    return json.loads(text) if text else []


def _ltfs_log_offset():
    return LTFS_LOG.stat().st_size


def _passive_mount_identity():
    """Parse mount-scoped local IBM logs; performs no device/filesystem query."""
    import re
    from src.tape_reset import (_BARCODE_RE, _DEVICE_RE, _GEN_RE, _LOCK_RE,
                                _SERIAL_RE, _ltfs_rows)
    from src.windows_update_guard import ltfs_current_mount_status
    mount = ltfs_current_mount_status()
    if not mount.get("determinate") or not mount.get("bound_to_current"):
        raise RuntimeError(f"current LTFS mount unverifiable: {mount}")
    rows = _ltfs_rows(mount["mount_started_at"])
    ident = {
        "mount_started_at": mount["mount_started_at"],
        "sync_type": mount.get("sync_type"),
        "sync_seconds": mount.get("sync_seconds"),
        "drive_serial": None,
        "device": None,
        "barcode": None,
        "ltfs_generation": None,
        "volume_lock_status": None,
        "volume_uuid": None,
        "evidence": [],
    }
    uuid_re = re.compile(r"Volume UUID is:\s*([0-9a-f-]+)", re.I)
    for row in rows:
        msg = row["message"]
        for key, regex in (("drive_serial", _SERIAL_RE), ("device", _DEVICE_RE),
                           ("barcode", _BARCODE_RE),
                           ("volume_lock_status", _LOCK_RE),
                           ("volume_uuid", uuid_re)):
            match = regex.search(msg)
            if match:
                value = match.group(1).strip()
                ident[key] = value.lower() if key in {"volume_lock_status", "volume_uuid"} else value
        if row["id"] in (11333, 11031):
            match = _GEN_RE.search(msg)
            if match:
                ident["ltfs_generation"] = int(match.group(1))
        if row["id"] in (62182, 62160, 11333, 17227, 17228, 11031, 61223, 15013, 61259):
            ident["evidence"].append(row)
    return ident


class CountingDriveCommand:
    def __init__(self, delegate):
        self.delegate = delegate
        self.calls = []

    def drive_status(self, drive_path):
        self.calls.append({"at": utc_now(), "drive": drive_path})
        return self.delegate.drive_status(drive_path)


def run(cfg):
    _require_isolated(cfg)
    state = _load_state(cfg)
    _verify_prepared_files(cfg, state)
    before_prod = _production_snapshot()
    if before_prod != json.loads(((_cfg_root(cfg) / "production_state_before.json").read_text(encoding="utf-8"))):
        raise RuntimeError("production state changed since pilot preparation")
    busy = _forbidden_processes()
    if busy:
        raise RuntimeError(f"forbidden tape/test/archive process active: {busy}")
    if CANCEL.is_set():
        raise RuntimeError("global cancellation flag is already set")

    configure_file_logging(cfg.backup_log_dir)
    db = create_database_manager(cfg)
    root = _cfg_root(cfg)
    result = {
        "pilot_started_at": utc_now(),
        "pilot_database": cfg.pg_dbname,
        "pilot_session_id": state["pilot_session_id"],
        "expected_identity": EXPECTED,
        "effective_config": {
            "features": {
                "sealed_tape_write_batches_enabled": cfg.sealed_tape_write_batches_enabled,
                "sealed_tape_write_batches_observation_enabled": cfg.sealed_tape_write_batches_observation_enabled,
            },
            "ready_queue": cfg.ready_queue_limits.as_dict(),
            "validated_ready_queue": cfg.validated_ready_queue_limits()[0].as_dict(),
            "lto_drive": cfg.lto_drive,
            "staging_dir": cfg.staging_dir,
            "backup_log_dir": cfg.backup_log_dir,
            "observation_log": cfg.sealed_batch_observation_log_path,
            "observation_queue_max": cfg.sealed_batch_observation_queue_max,
            "observation_shutdown_timeout_seconds": cfg.sealed_batch_observation_shutdown_timeout_seconds,
            "observation_statement_timeout_seconds": cfg.sealed_batch_observation_statement_timeout_seconds,
            "chunk_cap_gb": cfg.chunk_cap_gb,
            "staging_max_gb": cfg.staging_max_gb,
            "robocopy_priority": cfg.robocopy_priority,
            "cpu_affinity": cfg.cpu_affinity,
            "eject_after_session": cfg.eject_after_session,
        },
        "process_check": {"forbidden": busy},
        "ltfs_log_offset_before_group": _ltfs_log_offset(),
    }
    try:
        sealed = db._run_read(lambda conn: [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' "
            "AND table_name IN ('tape_write_batches','tape_write_batch_chunks',"
            "'tape_write_active_chunk') ORDER BY table_name").fetchall()],
            "pilot sealed-table absence")
        if sealed:
            raise RuntimeError(f"sealed-batch tables unexpectedly exist in pilot DB: {sealed}")
        session_id = state["pilot_session_id"]
        session = db.get_remote_session(session_id)
        if not session or session["tape_label"] != TAPE_LABEL:
            raise RuntimeError("isolated pilot session missing or wrong tape")
        statuses = db._run_read(lambda conn: [
            (r["chunk_index"], r["status"]) for r in conn.execute(
                "SELECT chunk_index,status FROM remote_chunks WHERE session_id=%s "
                "ORDER BY chunk_index", (session_id,)).fetchall()
        ], "pilot statuses")
        if statuses != [(ci, "pending") for ci in CHUNK_IDS]:
            raise RuntimeError(f"pilot chunk state changed: {statuses}")

        from src import ltfs
        from src import remote_orchestrator as ro
        from src.ltfs_ownership import OWNERSHIP
        from src.remote_orchestrator import RemoteOrchestrator

        orch = RemoteOrchestrator(cfg, db)
        ownership_preflight = orch._assert_ownership_preflight(session_id, "phase5e-pilot")
        if ownership_preflight is not None:
            raise RuntimeError(f"ownership preflight failed: {ownership_preflight}")
        feature_block = orch._assert_feature_gate(session_id, "phase5e-pilot")
        if feature_block is not None:
            raise RuntimeError(f"feature gate failed: {feature_block}")

        descs = []
        items = []
        by_chunk = {m["chunk_id"]: m for m in state["manifest"]}
        for ci in CHUNK_IDS:
            item = by_chunk[ci]
            pack_dir = root / Path(item["relative_pack_path"]).parent
            metadata = [{
                "file_name": "payload.bin",
                "original_path": item["remote_path"],
                "canonical_source_path": item["remote_path"],
                "file_size_bytes": item["bytes"],
                "is_packed": False,
                "container_name": None,
                "stored_path": "payload.bin",
                "catalog_policy": "loose",
            }]
            desc = StagedChunk(
                chunk_index=ci, fetch_dir=str(root / "fetch-unused" / str(ci)),
                pack_dir=str(pack_dir), metadata=metadata,
                staged_bytes=item["bytes"], fetch_seconds=0.0,
                fetch_bytes=item["bytes"], pack_seconds=0.0,
                pack_bytes=item["bytes"])
            descs.append(desc)
            items.append(ReadyItem(ci, str(pack_dir), item["bytes"], 1, desc))
        orch._staged_bytes = TOTAL_BYTES

        ready_q = ReadyQueue(cfg.ready_queue_limits, name="phase5e-one-shot")
        for item in items:
            if not ready_q.put(item):
                raise RuntimeError("could not enqueue prepared pilot chunk")
        ready_q.close("phase5e finite prepared group; no further producer")
        selected, reason = ready_q.wait_for_group(timeout=1)
        if [x.chunk_index for x in selected] != list(CHUNK_IDS):
            raise RuntimeError(f"unexpected selected group: {selected}")
        if ready_q.groups_started != 1:
            raise RuntimeError("scheduler did not select exactly one group")

        obs_worker = orch._build_observation_worker()
        if obs_worker is None:
            raise RuntimeError("observation worker failed to start")
        correlation_id = uuid.uuid4().hex
        orch._obs_correlation_id = correlation_id
        snapshot = orch._capture_write_group_snapshot(
            correlation_id, session_id, [x.desc for x in selected], reason,
            TAPE_LABEL, ready_q.groups_started, False, "closed", False,
            "streaming")
        if not obs_worker.submit_snapshot(snapshot):
            raise RuntimeError("observation snapshot enqueue unexpectedly dropped")

        real_command = ltfs.get_ltfs_drive_command()
        counting_command = CountingDriveCommand(real_command)
        real_label = ro.get_volume_label
        label_reads = []
        writer_events = []
        identity = {}

        def counted_label(path):
            value = real_label(path)
            label_reads.append({"at": utc_now(), "path": path, "value": value})
            return value

        original_gate = orch._pre_write_safety_gate

        def exact_identity_gate(session_id_arg, desc, tape_label, stop_event):
            block = original_gate(session_id_arg, desc, tape_label, stop_event)
            if block is not None:
                return block
            identity.update(_passive_mount_identity())
            observed = {
                "label": label_reads[-1]["value"] if label_reads else None,
                "drive_serial": identity.get("drive_serial"),
                "volume_uuid": identity.get("volume_uuid"),
                "ltfs_generation": identity.get("ltfs_generation"),
                "volume_lock_status": identity.get("volume_lock_status"),
            }
            mismatches = {k: {"expected": EXPECTED[k], "observed": observed.get(k)}
                          for k in observed if observed.get(k) != EXPECTED[k]}
            barcode = identity.get("barcode")
            if barcode not in (None, "", "NO_BARCODE"):
                mismatches["barcode"] = {"expected": "absent/NO_BARCODE", "observed": barcode}
            if mismatches:
                return orch._record_stop(StopResult(
                    exit_code=ExitCode.SAFETY_BLOCK,
                    reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                    resumable=False, source="phase5e-exact-identity",
                    session_id=session_id_arg, chunk_index=desc.chunk_index,
                    detailed_reason=f"Phase 5E cartridge identity mismatch: {mismatches}"))
            return None

        class PilotBackup(_NoEjectBackup):
            def run(self, *args, **kwargs):
                chunk = kwargs.get("remote_chunk_index")
                event = {"chunk_id": chunk, "invoked_at": utc_now(),
                         "invoked_time": time.time(), "write_started": False}
                original_start = kwargs.get("on_write_start")

                def started():
                    event["write_started"] = True
                    event["write_started_at"] = utc_now()
                    event["write_started_time"] = time.time()
                    if original_start:
                        original_start()

                kwargs["on_write_start"] = started
                writer_events.append(event)
                try:
                    value = super().run(*args, **kwargs)
                    event["completed"] = True
                    return value
                except BaseException as exc:
                    event["completed"] = False
                    event["error"] = f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    event["finished_at"] = utc_now()
                    event["finished_time"] = time.time()

        def pilot_writer(_cls=_NoEjectBackup):
            return PilotBackup(
                db, cfg.ibm_eject_cmd, tape_priority=orch.tape_priority,
                tape_affinity=orch.tape_cores, log_dir=cfg.backup_log_dir,
                notifier=orch.notifier, governor=orch.governor,
                index_min_file_mb=cfg.index_min_file_mb)

        orch._pre_write_safety_gate = exact_identity_gate
        orch._backup_writer = pilot_writer
        ro.get_volume_label = counted_label
        ltfs.set_ltfs_drive_command(counting_command)
        counts0 = (orch._ownership_acquisitions, orch._readiness_checks,
                   orch._cartridge_verifications)
        ownership0 = OWNERSHIP.generation
        group_start = time.time()
        result["group_started_at"] = utc_now()
        stop_block = None
        try:
            stop_block = orch._write_chunk_group(
                session_id, [x.desc for x in selected], TAPE_LABEL, False,
                threading.Event())
        finally:
            group_finish = time.time()
            result["group_finished_at"] = utc_now()
            result["ltfs_log_offset_after_group"] = _ltfs_log_offset()
            ro.get_volume_label = real_label
            ltfs.set_ltfs_drive_command(real_command)

        outcome = orch._capture_group_outcome(
            correlation_id, session_id, [x.desc for x in selected], stop_block,
            counts0, group_start, group_finish)
        submitted_outcome = obs_worker.submit_outcome(outcome)
        shutdown_start = time.perf_counter()
        obs_worker.shutdown()
        shutdown_seconds = time.perf_counter() - shutdown_start

        if stop_block is None:
            for item in selected:
                ready_q.mark_written(item)
        else:
            failing = stop_block.chunk_index
            for item in selected:
                if failing is not None and item.chunk_index < failing:
                    ready_q.mark_written(item)
                elif failing is not None and item.chunk_index == failing:
                    ready_q.mark_failed(item)
                else:
                    ready_q.mark_preserved(item)

        for index, event in enumerate(writer_events):
            if index:
                event["gap_from_previous_finish_seconds"] = round(
                    event["write_started_time"] - writer_events[index - 1]["finished_time"], 6)
            if event.get("write_started"):
                event["write_duration_seconds"] = round(
                    event["finished_time"] - event["write_started_time"], 6)

        own, rdy, cart = counts0
        result.update({
            "pilot_finished_at": utc_now(),
            "correlation_id": correlation_id,
            "selection_reason": reason,
            "selected_chunk_ids": [x.chunk_index for x in selected],
            "prepared_bytes_per_chunk": [x.prepared_bytes for x in selected],
            "total_selected_bytes": sum(x.prepared_bytes for x in selected),
            "ready_queue_metrics": ready_q.metrics(),
            "physical_ownership_acquisitions": OWNERSHIP.generation - ownership0,
            "orchestrator_ownership_acquisitions": orch._ownership_acquisitions - own,
            "actual_readiness_checks": len(counting_command.calls),
            "orchestrator_readiness_checks": orch._readiness_checks - rdy,
            "cartridge_label_reads": len(label_reads),
            "orchestrator_cartridge_verifications": orch._cartridge_verifications - cart,
            "writer_invocations": len(writer_events),
            "drive_status_calls": counting_command.calls,
            "label_reads": label_reads,
            "mounted_identity": identity,
            "writer_events": writer_events,
            "group_duration_seconds": round(group_finish - group_start, 6),
            "physical_throughput_mib_s": round(TOTAL_BYTES / max(group_finish - group_start, .001) / 1024 ** 2, 3),
            "stop_result": stop_block.as_dict() if stop_block else None,
            "observation_outcome_submitted": submitted_outcome,
            "observation_shutdown_seconds": round(shutdown_seconds, 6),
            "observation_counters": obs_worker.counters(),
            "second_group_started": False,
            "automatic_retry_count": 0,
            "automatic_readback": False,
            "ejected": False,
            "remounted": False,
            "formatted": False,
        })
        result["production_state_after"] = _production_snapshot()
        result["production_state_unchanged"] = result["production_state_after"] == before_prod
        result["pilot_chunk_status_after"] = [list(x) for x in db._run_read(
            lambda conn: [
                (r["chunk_index"], r["status"]) for r in conn.execute(
                    "SELECT chunk_index,status FROM remote_chunks WHERE session_id=%s "
                    "ORDER BY chunk_index", (session_id,)).fetchall()
            ], "pilot final statuses")]
        write_json(root / RESULT_NAME, result)
        print(json.dumps(result, indent=2, default=str))
        if stop_block is not None:
            raise SystemExit(int(stop_block.exit_code))
        required = {
            "physical_ownership_acquisitions": 1,
            "actual_readiness_checks": 1,
            "cartridge_label_reads": 1,
            "writer_invocations": len(CHUNK_IDS),
        }
        bad = {k: {"expected": v, "actual": result[k]}
               for k, v in required.items() if result[k] != v}
        if bad or not result["production_state_unchanged"]:
            raise RuntimeError(f"post-pilot invariant failed: {bad}")
    finally:
        db.close()


def _read_log_range(start, end=None):
    size = LTFS_LOG.stat().st_size
    if size < start:
        raise RuntimeError("LTFS log rotated/truncated during pilot")
    target = size if end is None else min(end, size)
    with open(LTFS_LOG, "rb") as fh:
        fh.seek(start)
        return fh.read(target - start).decode("utf-8", errors="replace")


def _summarize_ltfs(text):
    rows = []
    for rec in csv.reader(text.splitlines()):
        if len(rec) >= 7:
            rows.append({"event_id": rec[0], "at": rec[2], "pid": rec[3],
                         "drive": rec[5], "message": rec[6]})
    needles = {
        "scsi": "scsi", "timeout": "timeout", "read_position": "read position",
        "locate": "locate", "log_sense": "log sense", "servo": "servo",
        "read_only": "read-only", "write_protect": "write protect",
        "index": "index", "sync": "sync",
    }
    counts = {key: sum(1 for row in rows if needle in row["message"].lower())
              for key, needle in needles.items()}
    flagged = [row for row in rows if any(
        needle in row["message"].lower() for needle in needles.values())]
    return {"row_count": len(rows), "keyword_counts": counts,
            "flagged_rows": flagged}


def capture_idle(cfg):
    state = _load_state(cfg)
    root = _cfg_root(cfg)
    with open(root / RESULT_NAME, "r", encoding="utf-8") as fh:
        result = json.load(fh)
    start = int(result["ltfs_log_offset_before_group"])
    group_end = int(result["ltfs_log_offset_after_group"])
    end = _ltfs_log_offset()
    group_text = _read_log_range(start, group_end)
    idle_text = _read_log_range(group_end, end)
    group_path = root / "ltfs_group_window.csv"
    idle_path = root / "ltfs_idle_window.csv"
    group_path.write_text(group_text, encoding="utf-8")
    idle_path.write_text(idle_text, encoding="utf-8")
    idle = {
        "captured_at": utc_now(),
        "pilot_finished_at": result["pilot_finished_at"],
        "idle_seconds": (datetime.now(timezone.utc) - datetime.fromisoformat(
            result["pilot_finished_at"])).total_seconds(),
        "application_tape_operations_during_idle": 0,
        "log_offsets": {"before_group": start, "after_group": group_end,
                        "after_idle": end},
        "group_window": _summarize_ltfs(group_text),
        "idle_window": _summarize_ltfs(idle_text),
        "production_state_after_idle": _production_snapshot(),
        "automatic_readback": False,
        "ejected": False,
        "remounted": False,
        "formatted": False,
    }
    write_json(root / "idle_observation.json", idle)
    print(json.dumps(idle, indent=2, default=str))


def finalize_evidence(cfg):
    """Assemble local-only evidence and hash every retained pilot artifact."""
    root = _cfg_root(cfg)
    state = json.loads((root / STATE_NAME).read_text(encoding="utf-8"))
    result = json.loads((root / RESULT_NAME).read_text(encoding="utf-8"))
    idle = json.loads((root / "idle_observation.json").read_text(encoding="utf-8"))
    shutil.copy2(Path(__file__), root / "phase5e_pilot.py")
    code_dir = root / "code_evidence"
    code_dir.mkdir(exist_ok=True)
    shutil.copy2(PROJECT_ROOT / "src" / "windows_update_guard.py",
                 code_dir / "windows_update_guard.py")
    shutil.copy2(PROJECT_ROOT / "tests" / "test_windows_update_guard.py",
                 code_dir / "test_windows_update_guard.py")

    commands = """Phase 5E material command history (PowerShell, serial)
python -m pytest -q -k "not TapeWriteGovernorLifecycleTests"
$env:LTO_PG_SEALED_BATCH_IT='1'; python -m pytest -q tests/test_phase5b_sealed_batch_pg.py
python -m pytest -q tests/test_windows_update_guard.py tests/test_phase0_ltfs_baseline.py tests/test_remote_failure_hardening.py
python -m py_compile src\\windows_update_guard.py scripts\\phase5e_pilot.py
python scripts\\phase5e_pilot.py prepare --config phase5e_pilot.ini
python scripts\\phase5e_pilot.py run --config phase5e_pilot.ini
  first invocation stopped before hardware: mapped-row harness assertion bug
python scripts\\phase5e_pilot.py run --config phase5e_pilot.ini
  second invocation executed the one authorized physical write group
python scripts\\phase5e_pilot.py capture-idle --config phase5e_pilot.ini
python scripts\\phase5e_pilot.py finalize-evidence --config phase5e_pilot.ini

Read-only checks also used: git status/diff, Get-CimInstance process checks,
Get-FileHash for the retained Phase 5C backup, read-only psycopg transactions,
ltfs_current_mount_status/ltfs_media_health, and local LogFile.csv parsing.
No mkltfs, format, erase, ltfsck, eject, remount, read-back, or Session 37 run.
"""
    (root / "commands_executed.txt").write_text(commands, encoding="utf-8")

    status = subprocess.run(
        ["git", "status", "--short"], cwd=PROJECT_ROOT, text=True,
        capture_output=True, timeout=30, check=True).stdout
    diff = subprocess.run(
        ["git", "diff", "--no-ext-diff", "--", "src/windows_update_guard.py",
         "tests/test_windows_update_guard.py"], cwd=PROJECT_ROOT, text=True,
        capture_output=True, timeout=30, check=True).stdout
    (root / "repository_status.txt").write_text(status, encoding="utf-8")
    (root / "format_health_gate.patch").write_text(diff, encoding="utf-8")

    with open(cfg.sealed_batch_observation_log_path, "r", encoding="utf-8") as fh:
        observations = [json.loads(line) for line in fh if line.strip()]
    with open(Path(cfg.backup_log_dir) / "SUMMARY.csv", "r",
              encoding="utf-8-sig", newline="") as fh:
        summaries = list(csv.DictReader(fh))
    report_data = {
        "assembled_at": utc_now(),
        "state": state,
        "result": result,
        "idle": idle,
        "observation_records": observations,
        "backup_summary_rows": summaries,
        "phase5c_backup": {
            "path": r"C:\lto8-shadow-backups\prod_full_20260801_184330.dump",
            "sha256": sha256_file(
                r"C:\lto8-shadow-backups\prod_full_20260801_184330.dump"),
        },
    }
    write_json(root / "phase5e_report_data.json", report_data)

    manifest_path = root / "artifact_manifest.json"
    entries = []
    for path in sorted(p for p in root.rglob("*") if p.is_file() and
                       p != manifest_path and not p.name.endswith(".tmp")):
        entries.append({
            "relative_path": str(path.relative_to(root)).replace("\\", "/"),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    write_json(manifest_path, {
        "created_at": utc_now(), "pilot_root": str(root),
        "file_count": len(entries), "files": entries,
    })
    print(json.dumps({"manifest": str(manifest_path),
                      "file_count": len(entries),
                      "manifest_sha256": sha256_file(manifest_path)}, indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=(
        "prepare", "run", "capture-idle", "finalize-evidence"))
    parser.add_argument("--config", default=str(PROJECT_ROOT / "phase5e_pilot.ini"))
    args = parser.parse_args(argv)
    cfg = ConfigManager(args.config)
    if args.command == "prepare":
        prepare(cfg)
    elif args.command == "run":
        run(cfg)
    elif args.command == "capture-idle":
        capture_idle(cfg)
    else:
        finalize_evidence(cfg)


if __name__ == "__main__":
    main()
