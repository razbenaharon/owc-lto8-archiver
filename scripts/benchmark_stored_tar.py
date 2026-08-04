"""Offline benchmark: extracted ZIP_STORED path vs direct Stored TAR.

Task 2.8 only. This harness is intentionally local-only:

* no PostgreSQL
* no SSH
* no LTFS / Z: / tape access
* no writes to backup_logs/SUMMARY.csv

It generates deterministic corpora, serializes one input TAR byte stream per
profile, and runs two local workflows against that same stream:

1. current: extract TAR to staging, then pack small files with LTOPacker
2. stored_tar: copy the TAR stream to a unique ``.part``, validate it, and
   publish the final TAR + sidecar pair

The benchmark output lives under a harness-owned, gitignored root by default:
``storage_map_logs/benchmark_stored_tar/<timestamp>/``.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    import psutil
except ImportError:  # pragma: no cover - requirements includes psutil
    psutil = None

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.archive_artifacts import (  # noqa: E402
    TAR_SIDECAR_VERSION,
    publish_stored_tar_pair,
    search_tar_sidecar,
)
from src.packer import LTOPacker  # noqa: E402
from src.paths import _dir_tree_size, _long  # noqa: E402
from src.pipeline_types import SourceDisposition, StoredTarSourceDiagnostic  # noqa: E402
from src.ram_telemetry import RamStageSampler  # noqa: E402
from src.tar_container import (  # noqa: E402
    GNU_RECORD_SIZE,
    STORED_TAR_DIALECT,
    STORED_TAR_FORMAT_VERSION,
    validate_stored_tar_part,
)


HARNESS_VERSION = "stored-tar-benchmark-v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "storage_map_logs" / "benchmark_stored_tar"
COPY_CHUNK_SIZE = 1024 * 1024
SAMPLE_INTERVAL = 0.05


@dataclass(frozen=True)
class ProfileSpec:
    name: str
    file_count: int
    bytes_per_file: int
    directories: int
    sparse: bool = False
    sparse_logical_size: int = 0
    sparse_extents: Tuple[Tuple[int, bytes], ...] = ()


@dataclass
class FlowMetrics:
    flow: str
    file_count: int
    logical_bytes: int
    output_bytes: int
    wall_seconds: float
    cpu_seconds: float
    peak_rss_mb: float | None
    peak_staging_bytes: int
    final_staging_bytes: int
    peak_entry_count: int
    final_entry_count: int
    time_to_writer_ready_seconds: float
    status: str
    notes: List[str]
    membership: List[Dict[str, object]]
    workspace: str = ""
    output_paths: Dict[str, str] | None = None

    def to_json(self):
        payload = asdict(self)
        payload["peak_rss_mb"] = (
            None if self.peak_rss_mb is None else round(self.peak_rss_mb, 3))
        payload["wall_seconds"] = round(self.wall_seconds, 6)
        payload["cpu_seconds"] = round(self.cpu_seconds, 6)
        payload["time_to_writer_ready_seconds"] = round(
            self.time_to_writer_ready_seconds, 6)
        return payload


class HarnessError(RuntimeError):
    pass


class _PairDB:
    """Tiny DB facade for publish_stored_tar_pair()."""

    def __init__(self):
        self.calls = []

    def publish_stored_tar_pair(self, **values):
        self.calls.append(dict(values))
        return {"ready": True}


class WorkspaceSampler:
    """Sample harness-owned workspace bytes and entry counts."""

    def __init__(self, root: str, interval_seconds: float = SAMPLE_INTERVAL):
        self.root = os.path.abspath(root)
        self.interval_seconds = max(0.02, float(interval_seconds))
        self._stop = threading.Event()
        self._thread = None
        self.peak_bytes = 0
        self.peak_entries = 0
        self.final_bytes = 0
        self.final_entries = 0

    def __enter__(self):
        self._sample()
        self._thread = threading.Thread(
            target=self._run, name="benchmark-workspace-sampler", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._sample()
        self.final_bytes = self.peak_bytes if self.final_bytes == 0 else self.final_bytes
        self.final_entries = (
            self.peak_entries if self.final_entries == 0 else self.final_entries)
        return False

    def _run(self):
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def _sample(self):
        total_bytes = 0
        total_entries = 0
        if os.path.isdir(_long(self.root)):
            total_bytes = _dir_tree_size(self.root)
            for _root, dirs, files in os.walk(self.root):
                total_entries += len(dirs) + len(files)
        self.peak_bytes = max(self.peak_bytes, int(total_bytes))
        self.peak_entries = max(self.peak_entries, int(total_entries))
        self.final_bytes = int(total_bytes)
        self.final_entries = int(total_entries)


def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _gnu_pad(data: bytes) -> bytes:
    return data + bytes((-len(data)) % GNU_RECORD_SIZE)


def _regular_tar(entries: Sequence[Tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as arc:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            arc.addfile(info, io.BytesIO(data))
    return _gnu_pad(output.getvalue())


def _sparse_tar(name: str, logical_size: int,
                extents: Sequence[Tuple[int, bytes]]) -> bytes:
    sparse_map = [str(len(extents)).encode("ascii") + b"\n"]
    stored = bytearray()
    for offset, data in extents:
        sparse_map.extend((
            str(int(offset)).encode("ascii") + b"\n",
            str(len(data)).encode("ascii") + b"\n",
        ))
        stored.extend(data)
    map_data = b"".join(sparse_map)
    payload = map_data + bytes((-len(map_data)) % 512) + bytes(stored)
    info = tarfile.TarInfo("GNUSparseFile.1/member")
    info.size = len(payload)
    info.mtime = 0
    info.pax_headers = {
        "GNU.sparse.major": "1",
        "GNU.sparse.minor": "0",
        "GNU.sparse.name": name,
        "GNU.sparse.realsize": str(int(logical_size)),
    }
    headers = info.tobuf(format=tarfile.PAX_FORMAT)
    archive = headers + payload + bytes((-len(payload)) % 512)
    archive += bytes(1024)
    return _gnu_pad(archive)


def _profile_specs() -> Dict[str, ProfileSpec]:
    kib = 1024
    mib = 1024 * kib
    return {
        "small": ProfileSpec(
            name="small", file_count=512, bytes_per_file=4 * kib, directories=8),
        "medium": ProfileSpec(
            name="medium", file_count=4096, bytes_per_file=2 * kib, directories=32),
        "large": ProfileSpec(
            name="large", file_count=16384, bytes_per_file=1 * kib, directories=64),
        "sparse": ProfileSpec(
            name="sparse", file_count=1, bytes_per_file=0, directories=1,
            sparse=True, sparse_logical_size=128 * mib,
            sparse_extents=((0, b"BEGIN"), (64 * mib, b"MIDDLE"), (127 * mib, b"END"))),
    }


def _spec_to_json(spec: ProfileSpec):
    payload = asdict(spec)
    payload["sparse_extents"] = [
        {"offset": int(offset), "data_hex": data.hex()}
        for offset, data in spec.sparse_extents
    ]
    return payload


def _make_file_name(index: int) -> str:
    return f"file_{index:06d}.bin"


def _make_relpath(index: int, directories: int) -> str:
    group = index % max(1, int(directories))
    return f"group_{group:03d}/{_make_file_name(index)}"


def _payload_for(index: int, size: int) -> bytes:
    seed = f"{index:08d}|".encode("ascii")
    repeats = (size + len(seed) - 1) // len(seed)
    return (seed * repeats)[:size]


def generate_profile_input(spec: ProfileSpec):
    if spec.sparse:
        rel = "sparse/virtual_sparse.bin"
        tar_bytes = _sparse_tar(rel, spec.sparse_logical_size, spec.sparse_extents)
        plan = [{
            "member_name": rel,
            "canonical_source_path": f"/synthetic/{rel}",
            "expected_size": int(spec.sparse_logical_size),
            "plan_ordinal": 0,
            "container_ordinal": 0,
        }]
        membership = [{
            "stored_path": rel,
            "logical_size": int(spec.sparse_logical_size),
        }]
        return {
            "tar_bytes": tar_bytes,
            "plan": plan,
            "membership": membership,
            "logical_bytes": int(spec.sparse_logical_size),
            "file_count": 1,
        }

    entries = []
    plan = []
    membership = []
    logical_bytes = 0
    for index in range(int(spec.file_count)):
        rel = _make_relpath(index, spec.directories)
        payload = _payload_for(index, int(spec.bytes_per_file))
        entries.append((rel, payload))
        plan.append({
            "member_name": rel,
            "canonical_source_path": f"/synthetic/{rel}",
            "expected_size": len(payload),
            "plan_ordinal": index,
            "container_ordinal": 0,
        })
        membership.append({
            "stored_path": rel,
            "logical_size": len(payload),
        })
        logical_bytes += len(payload)
    return {
        "tar_bytes": _regular_tar(entries),
        "plan": plan,
        "membership": membership,
        "logical_bytes": logical_bytes,
        "file_count": len(entries),
    }


def _write_bytes(path: str, data: bytes):
    os.makedirs(_long(os.path.dirname(path)), exist_ok=True)
    with open(_long(path), "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _copy_file_streaming(src: str, dest: str):
    os.makedirs(_long(os.path.dirname(dest)), exist_ok=True)
    with open(_long(src), "rb") as source, open(_long(dest), "xb") as target:
        while True:
            block = source.read(COPY_CHUNK_SIZE)
            if not block:
                break
            target.write(block)
        target.flush()
        os.fsync(target.fileno())


def _extract_tar(input_tar: str, dest_dir: str):
    os.makedirs(_long(dest_dir), exist_ok=True)
    with tarfile.open(_long(input_tar), "r:*") as arc:
        arc.extractall(_long(dest_dir), filter="data")


def _cpu_seconds():
    if psutil is None:
        return time.process_time()
    try:
        times = psutil.Process().cpu_times()
        return float(times.user + times.system)
    except Exception:
        return time.process_time()


def _machine_info():
    vm = None
    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
        except Exception:
            vm = None
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "processor": platform.processor(),
        "cpu_logical": (psutil.cpu_count(logical=True) if psutil else os.cpu_count()),
        "cpu_physical": (psutil.cpu_count(logical=False) if psutil else None),
        "ram_gib": (round(vm.total / 1024**3, 2) if vm else None),
    }


def _normalize_membership(items: Iterable[Dict[str, object]]):
    normalized = [{
        "stored_path": str(item["stored_path"]).replace("\\", "/"),
        "logical_size": int(item["logical_size"]),
    } for item in items]
    normalized.sort(key=lambda item: item["stored_path"])
    return normalized


def _zip_membership(metadata: Sequence[Dict[str, object]]):
    items = []
    for row in metadata:
        items.append({
            "stored_path": str(row["stored_path"]).replace("\\", "/"),
            "logical_size": int(row["file_size_bytes"]),
        })
    return _normalize_membership(items)


def _tar_membership(validation):
    return _normalize_membership({
        "stored_path": member.name,
        "logical_size": member.logical_size,
    } for member in validation.members)


def _sum_output_bytes(paths: Iterable[str]) -> int:
    total = 0
    for path in paths:
        if path and os.path.isfile(_long(path)):
            total += os.path.getsize(_long(path))
    return total


def _current_flow(workspace: str, input_tar: str, profile_input, *,
                  threshold_mb: float, max_zip_size_gb: float) -> FlowMetrics:
    notes = []
    flow_root = os.path.join(workspace, "current")
    input_copy = os.path.join(flow_root, "input.tar")
    fetch_dir = os.path.join(flow_root, "fetch")
    pack_dir = os.path.join(flow_root, "pack")
    os.makedirs(_long(flow_root), exist_ok=True)
    shutil.copyfile(_long(input_tar), _long(input_copy))
    t0 = time.perf_counter()
    c0 = _cpu_seconds()
    ready_at = None
    with WorkspaceSampler(flow_root) as ws, RamStageSampler("benchmark-current", 0.5) as ram:
        _extract_tar(input_copy, fetch_dir)
        packer = LTOPacker(
            max_zip_size_gb=max_zip_size_gb,
            manifest_enabled=True,
            manifest_format="jsonl",
            manifest_compression="zstd",
        )
        metadata = packer.run(
            fetch_dir, pack_dir, threshold_mb,
            on_existing="clean", pack_file_batch_size=10000)
        ready_at = time.perf_counter()
    wall = time.perf_counter() - t0
    cpu = _cpu_seconds() - c0
    metadata = metadata or []
    membership = _zip_membership(metadata)
    output_paths = [os.path.join(pack_dir, name) for name in os.listdir(_long(pack_dir))]
    return FlowMetrics(
        flow="current",
        file_count=int(profile_input["file_count"]),
        logical_bytes=int(profile_input["logical_bytes"]),
        output_bytes=_sum_output_bytes(output_paths),
        wall_seconds=wall,
        cpu_seconds=cpu,
        peak_rss_mb=ram.process_peak_mb,
        peak_staging_bytes=ws.peak_bytes,
        final_staging_bytes=ws.final_bytes,
        peak_entry_count=ws.peak_entries,
        final_entry_count=ws.final_entries,
        time_to_writer_ready_seconds=(ready_at - t0 if ready_at is not None else wall),
        status="ok",
        notes=notes,
        membership=membership,
        workspace=flow_root,
        output_paths={
            "input_tar": input_copy,
            "fetch_dir": fetch_dir,
            "pack_dir": pack_dir,
        },
    )


def _stored_tar_flow(workspace: str, input_tar: str, profile_input, *,
                     manifest_root: str) -> FlowMetrics:
    notes = []
    flow_root = os.path.join(workspace, "stored_tar")
    pack_dir = os.path.join(flow_root, "pack")
    os.makedirs(_long(pack_dir), exist_ok=True)
    final_tar = os.path.join(pack_dir, "container_000.tar")
    part_tar = f"{final_tar}.{uuid.uuid4().hex[:12]}.part"
    db = _PairDB()
    t0 = time.perf_counter()
    c0 = _cpu_seconds()
    ready_at = None
    with WorkspaceSampler(flow_root) as ws, RamStageSampler("benchmark-stored-tar", 0.5) as ram:
        _copy_file_streaming(input_tar, part_tar)
        validation = validate_stored_tar_part(
            part_tar, profile_input["plan"], container_ordinal=0,
            source_diagnostics=())
        publication = publish_stored_tar_pair(
            manifest_root, part_tar, final_tar, profile_input["plan"], (),
            validation=validation,
            session_id=1, chunk_index=0, container_id=1, container_ordinal=0,
            owner_token="benchmark-owner", db=db, pack_dir=pack_dir)
        ready_at = time.perf_counter()
    wall = time.perf_counter() - t0
    cpu = _cpu_seconds() - c0
    membership = _tar_membership(validation)
    sidecar = search_tar_sidecar(
        publication.sidecar_path,
        expected_container_id=1,
        expected_member_count=validation.member_count,
        limit=max(10_000, validation.member_count),
        max_records=max(10_000, len(profile_input["plan"])))
    notes.append(
        f"sidecar_version={sidecar.header.get('version', TAR_SIDECAR_VERSION)}")
    return FlowMetrics(
        flow="stored_tar",
        file_count=int(profile_input["file_count"]),
        logical_bytes=int(profile_input["logical_bytes"]),
        output_bytes=int(publication.tar_size + publication.sidecar_size),
        wall_seconds=wall,
        cpu_seconds=cpu,
        peak_rss_mb=ram.process_peak_mb,
        peak_staging_bytes=ws.peak_bytes,
        final_staging_bytes=ws.final_bytes,
        peak_entry_count=ws.peak_entries,
        final_entry_count=ws.final_entries,
        time_to_writer_ready_seconds=(ready_at - t0 if ready_at is not None else wall),
        status="ok",
        notes=notes,
        membership=membership,
        workspace=flow_root,
        output_paths={
            "pack_dir": pack_dir,
            "final_tar": final_tar,
            "sidecar": publication.sidecar_path,
            "pack_sidecar": publication.pack_sidecar_path,
        },
    )


def run_profile(spec: ProfileSpec, run_root: str, *,
                threshold_mb: float = 1024.0,
                max_zip_size_gb: float = 1.0,
                keep_workspaces: bool = False):
    profile_root = os.path.join(run_root, spec.name)
    input_root = os.path.join(profile_root, "input")
    manifest_root = os.path.join(profile_root, "manifests")
    os.makedirs(_long(input_root), exist_ok=True)
    os.makedirs(_long(manifest_root), exist_ok=True)
    profile_input = generate_profile_input(spec)
    input_tar = os.path.join(input_root, "source_stream.tar")
    _write_bytes(input_tar, profile_input["tar_bytes"])
    record = {
        "profile": spec.name,
        "spec": _spec_to_json(spec),
        "input_tar": input_tar,
        "input_tar_bytes": os.path.getsize(_long(input_tar)),
        "file_count": int(profile_input["file_count"]),
        "logical_bytes": int(profile_input["logical_bytes"]),
        "expected_membership": _normalize_membership(profile_input["membership"]),
    }
    results = {}
    for flow_name, runner in (
            ("current", _current_flow),
            ("stored_tar", _stored_tar_flow)):
        try:
            if flow_name == "stored_tar":
                results[flow_name] = runner(
                    profile_root, input_tar, profile_input,
                    manifest_root=manifest_root)
            else:
                results[flow_name] = runner(
                    profile_root, input_tar, profile_input,
                    threshold_mb=threshold_mb,
                    max_zip_size_gb=max_zip_size_gb)
        except Exception as exc:
            error_workspace = os.path.join(profile_root, flow_name)
            results[flow_name] = FlowMetrics(
                flow=flow_name,
                file_count=int(profile_input["file_count"]),
                logical_bytes=int(profile_input["logical_bytes"]),
                output_bytes=0,
                wall_seconds=0.0,
                cpu_seconds=0.0,
                peak_rss_mb=None,
                peak_staging_bytes=0,
                final_staging_bytes=0,
                peak_entry_count=0,
                final_entry_count=0,
                time_to_writer_ready_seconds=0.0,
                status="failed",
                notes=[f"{type(exc).__name__}: {exc}", traceback.format_exc()],
                membership=[],
                workspace=error_workspace,
                output_paths={},
            )
        finally:
            if not keep_workspaces and os.path.isdir(_long(os.path.join(profile_root, flow_name))):
                shutil.rmtree(_long(os.path.join(profile_root, flow_name)),
                              ignore_errors=True)

    current = results["current"]
    stored = results["stored_tar"]
    expected = record["expected_membership"]
    record["current"] = current.to_json()
    record["stored_tar"] = stored.to_json()
    record["equivalent_membership"] = (
        current.membership == expected and stored.membership == expected)
    if current.status == "ok" and stored.status == "ok":
        if current.membership != stored.membership:
            raise HarnessError(
                f"profile {spec.name}: current/stored membership mismatch")
        if current.membership != expected:
            raise HarnessError(
                f"profile {spec.name}: flow membership differs from generated plan")
    return record


def render_markdown(summary: dict) -> str:
    lines = []
    lines.append(f"# Stored TAR Offline Benchmark ({summary['version']})")
    lines.append("")
    lines.append(
        f"Run date: {summary['run_date_local']} ({summary['timezone']}). "
        "This is one local sample, not a benchmark-suite average.")
    lines.append("")
    lines.append("Machine:")
    machine = summary["machine"]
    lines.append(f"- Platform: `{machine['platform']}`")
    lines.append(f"- Python: `{machine['python']}`")
    lines.append(f"- CPU: `{machine['processor'] or 'unknown'}`")
    lines.append(
        f"- Logical CPUs: `{machine['cpu_logical']}`; physical CPUs: `{machine['cpu_physical']}`")
    lines.append(f"- RAM: `{machine['ram_gib']}` GiB")
    lines.append("")
    lines.append("Method:")
    lines.append(
        "- Both flows consumed the same locally generated TAR byte stream per profile.")
    lines.append(
        "- `current` = TAR extraction to staging + `LTOPacker` ZIP_STORED manifests.")
    lines.append(
        "- `stored_tar` = direct Stored TAR `.part` + full validation + sidecar publication.")
    lines.append(
        "- No PostgreSQL, SSH, LTFS, tape, `Z:\\`, `config.ini`, `.env`, or `SUMMARY.csv` writes were used.")
    lines.append("")
    headers = (
        "| Profile | Flow | Files | Logical MiB | Output MiB | Wall s | CPU s | Peak RSS MiB | "
        "Peak staging MiB | Final staging MiB | Peak entries | Final entries | Ready s |")
    lines.append(headers)
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for profile in summary["profiles"]:
        for flow in ("current", "stored_tar"):
            row = profile[flow]
            lines.append(
                f"| {profile['profile']} | {flow} | {row['file_count']} | "
                f"{row['logical_bytes'] / 1024**2:.2f} | "
                f"{row['output_bytes'] / 1024**2:.2f} | "
                f"{row['wall_seconds']:.3f} | {row['cpu_seconds']:.3f} | "
                f"{'' if row['peak_rss_mb'] is None else f'{row['peak_rss_mb']:.2f}'} | "
                f"{row['peak_staging_bytes'] / 1024**2:.2f} | "
                f"{row['final_staging_bytes'] / 1024**2:.2f} | "
                f"{row['peak_entry_count']} | {row['final_entry_count']} | "
                f"{row['time_to_writer_ready_seconds']:.3f} |")
    lines.append("")
    lines.append("Observations:")
    for profile in summary["profiles"]:
        cur = profile["current"]
        tar = profile["stored_tar"]
        delta_wall = cur["wall_seconds"] - tar["wall_seconds"]
        delta_output = cur["output_bytes"] - tar["output_bytes"]
        delta_peak = cur["peak_staging_bytes"] - tar["peak_staging_bytes"]
        lines.append(
            f"- `{profile['profile']}`: Stored TAR changed wall time by `{delta_wall:.3f}` s, "
            f"output size by `{delta_output / 1024**2:.2f}` MiB, and peak staging by "
            f"`{delta_peak / 1024**2:.2f}` MiB relative to the current path.")
    lines.append("")
    lines.append("Decision framing:")
    lines.append(
        "- If the priority is lower staging footprint and faster writer-ready output, compare `Peak staging MiB` and `Ready s` first.")
    lines.append(
        "- If the priority is CPU cost, compare `CPU s` and `Peak RSS MiB`.")
    lines.append(
        "- Sparse behavior matters separately: the `sparse` profile isolates whether preserving sparse TAR structure is worth enabling the TAR writer.")
    lines.append("")
    return "\n".join(lines) + "\n"


def run_benchmark(*, profiles: Sequence[str], output_root: str,
                  threshold_mb: float = 1024.0, max_zip_size_gb: float = 1.0,
                  keep_workspaces: bool = False):
    specs = _profile_specs()
    selected = []
    for name in profiles:
        if name not in specs:
            raise HarnessError(
                f"unknown profile {name!r}; choose from {', '.join(sorted(specs))}")
        selected.append(specs[name])
    run_root = os.path.abspath(os.path.join(output_root, _iso_now()))
    os.makedirs(_long(run_root), exist_ok=True)
    summary = {
        "version": HARNESS_VERSION,
        "run_root": run_root,
        "run_date_local": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": datetime.now().astimezone().tzname(),
        "machine": _machine_info(),
        "profiles": [],
    }
    for spec in selected:
        summary["profiles"].append(run_profile(
            spec, run_root, threshold_mb=threshold_mb,
            max_zip_size_gb=max_zip_size_gb,
            keep_workspaces=keep_workspaces))
    json_path = os.path.join(run_root, "summary.json")
    with open(_long(json_path), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
    md_path = os.path.join(run_root, "summary.md")
    with open(_long(md_path), "w", encoding="utf-8", newline="\n") as handle:
        handle.write(render_markdown(summary))
    return summary


def parse_args(argv: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profiles", nargs="+", default=["small", "medium", "large", "sparse"],
        help="Profiles to run.")
    parser.add_argument(
        "--output-root", default=str(DEFAULT_OUTPUT_ROOT),
        help="Gitignored root for harness output.")
    parser.add_argument(
        "--threshold-mb", type=float, default=1024.0,
        help="Packer threshold; default keeps every profile on the ZIP_STORED path.")
    parser.add_argument(
        "--max-zip-size-gb", type=float, default=1.0,
        help="LTOPacker max ZIP size cap.")
    parser.add_argument(
        "--keep-workspaces", action="store_true",
        help="Keep per-flow workspaces instead of cleaning them after each profile.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None):
    args = parse_args(argv)
    summary = run_benchmark(
        profiles=args.profiles,
        output_root=args.output_root,
        threshold_mb=args.threshold_mb,
        max_zip_size_gb=args.max_zip_size_gb,
        keep_workspaces=args.keep_workspaces,
    )
    print(f"[BENCHMARK] Stored TAR offline benchmark complete: {summary['run_root']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
