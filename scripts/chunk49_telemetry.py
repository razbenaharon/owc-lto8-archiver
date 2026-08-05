"""Passive per-stage telemetry for one bounded Session 37 write group.

STRICTLY PASSIVE. It never touches the tape, never lists the LTFS drive,
never remounts and never talks to the writer. Every number comes from one of
three sources that cost the media nothing:

* **Kernel per-process counters** (``Win32_Process`` I/O + CPU) for the
  archiver and any ``Robocopy`` child -- the same approach as
  ``backup_logs/_tape_sampler.ps1``, which exists because ``du``/``ls`` on the
  LTFS drive causes tape wear and shoe-shining.
* **The archiver's own rotating trace** (``backup_logs/archiver.log``), parsed
  read-only for the FETCH/PACK/TAPE status lines it already prints.
* **The staging directory**, which is ordinary local disk.

``GetDiskFreeSpaceEx`` is used for the tape only if ``--tape-free`` is passed;
it is a single stat call and never walks the filesystem, but it is off by
default so the idle-LTFS rule holds without an argument.

Writes one JSON object per sample to a JSONL artifact, and prints a compact
snapshot on stage transitions and every ``--report-seconds``.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

STAGE_PATTERNS = (
    ("FETCH", re.compile(r"\[FETCH\]", re.I)),
    ("PACK", re.compile(r"\[PACK\]", re.I)),
    ("TAPE", re.compile(r"\[TAPE\]", re.I)),
    ("DB_SYNC", re.compile(r"\[DB.?SYNC\]|db_sync", re.I)),
)
NUMBER = re.compile(r"(\d[\d,]*\.?\d*)")


def _now():
    return datetime.now(timezone.utc)


def _iso(moment):
    return moment.isoformat()


def _free_bytes(path):
    """Local free space. Never used against the LTFS drive by default."""
    free = ctypes.c_ulonglong()
    total = ctypes.c_ulonglong()
    total_free = ctypes.c_ulonglong()
    ok = ctypes.windll.kernel32.GetDiskFreeSpaceExW(
        str(path), ctypes.byref(free), ctypes.byref(total),
        ctypes.byref(total_free))
    if not ok:
        return None, None
    return int(free.value), int(total.value)


def _dir_usage(root):
    """Bytes and file count under a LOCAL staging root."""
    total = 0
    files = 0
    for base, _dirs, names in os.walk(root):
        for name in names:
            try:
                total += os.path.getsize(os.path.join(base, name))
                files += 1
            except OSError:
                pass
    return total, files


def _processes():
    """Kernel I/O/CPU counters for the archiver and any Robocopy child."""
    script = (
        "Get-CimInstance Win32_Process -Filter "
        "\"Name='python.exe' OR Name='Robocopy.exe' OR Name='robocopy.exe'\" "
        "| Select-Object ProcessId,Name,ReadTransferCount,WriteTransferCount,"
        "UserModeTime,KernelModeTime,WorkingSetSize,CommandLine "
        "| ConvertTo-Json -Compress -Depth 3")
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return []
    if out.returncode != 0 or not out.stdout.strip():
        return []
    try:
        data = json.loads(out.stdout)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else [data]


class LogTail:
    """Read-only incremental tail of the archiver's own trace."""

    def __init__(self, path):
        self.path = path
        self.offset = 0
        if os.path.exists(path):
            self.offset = os.path.getsize(path)

    def new_lines(self):
        if not os.path.exists(self.path):
            return []
        size = os.path.getsize(self.path)
        if size < self.offset:          # rotated
            self.offset = 0
        if size == self.offset:
            return []
        with open(self.path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(self.offset)
            chunk = handle.read()
            self.offset = handle.tell()
        return [line for line in chunk.splitlines() if line.strip()]


class Sampler:
    def __init__(self, args):
        self.args = args
        self.started = time.monotonic()
        self.started_at = _now()
        self.stage = "STARTING"
        self.stage_since = self.started
        self.tail = LogTail(args.log)
        self.prev = None
        self.samples = 0
        self.last_report = 0.0
        self.recent_log = []
        self.robocopy_seen = False
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        self.out = open(args.out, "a", encoding="utf-8")

    # -- derivation -----------------------------------------------------
    def _stage_from(self, lines):
        stage = None
        for line in lines:
            for name, pattern in STAGE_PATTERNS:
                if pattern.search(line):
                    stage = name
        return stage

    def _rates(self, sample):
        """Rolling deltas against the previous sample."""
        if self.prev is None:
            return {}
        span = sample["elapsed_seconds"] - self.prev["elapsed_seconds"]
        if span <= 0:
            return {}
        out = {}
        for key in ("proc_read_bytes", "proc_write_bytes",
                    "robocopy_read_bytes", "robocopy_write_bytes",
                    "staging_bytes"):
            before = self.prev.get(key)
            after = sample.get(key)
            if before is None or after is None:
                continue
            out[f"{key}_mbs"] = round(
                (after - before) / span / 1024 / 1024, 3)
        return out

    def sample(self):
        elapsed = time.monotonic() - self.started
        lines = self.tail.new_lines()
        if lines:
            self.recent_log = lines[-6:]
        moved = self._stage_from(lines)

        procs = _processes()
        archiver = [p for p in procs
                    if (p.get("Name") or "").lower() == "python.exe"
                    and "run.py" in (p.get("CommandLine") or "")]
        robocopy = [p for p in procs
                    if (p.get("Name") or "").lower().startswith("robocopy")]
        if robocopy:
            self.robocopy_seen = True

        def total(rows, field):
            return sum(int(r.get(field) or 0) for r in rows) if rows else 0

        staging_bytes, staging_files = (None, None)
        if self.args.staging and os.path.isdir(self.args.staging):
            staging_bytes, staging_files = _dir_usage(self.args.staging)
        staging_free, staging_total = (None, None)
        if self.args.staging:
            staging_free, staging_total = _free_bytes(
                os.path.splitdrive(os.path.abspath(self.args.staging))[0]
                + os.sep)

        sample = {
            "ts": _iso(_now()),
            "elapsed_seconds": round(elapsed, 3),
            "stage": moved or self.stage,
            "archiver_processes": len(archiver),
            "robocopy_processes": len(robocopy),
            "proc_read_bytes": total(archiver, "ReadTransferCount"),
            "proc_write_bytes": total(archiver, "WriteTransferCount"),
            "proc_rss_bytes": total(archiver, "WorkingSetSize"),
            "proc_cpu_100ns": (total(archiver, "UserModeTime")
                               + total(archiver, "KernelModeTime")),
            "robocopy_read_bytes": total(robocopy, "ReadTransferCount"),
            "robocopy_write_bytes": total(robocopy, "WriteTransferCount"),
            "staging_bytes": staging_bytes,
            "staging_files": staging_files,
            "staging_free_bytes": staging_free,
            "staging_total_bytes": staging_total,
            "log": self.recent_log[-2:] if self.recent_log else [],
        }
        if self.args.tape_free:
            free, tot = _free_bytes(self.args.tape_free)
            sample["tape_free_bytes"] = free
            sample["tape_total_bytes"] = tot
        sample.update(self._rates(sample))

        if moved and moved != self.stage:
            sample["stage_transition"] = {"from": self.stage, "to": moved,
                                          "after_seconds": round(elapsed, 1)}
            self.stage = moved
            self.stage_since = time.monotonic()

        self.out.write(json.dumps(sample) + "\n")
        self.out.flush()
        self.prev = sample
        self.samples += 1
        return sample

    # -- reporting ------------------------------------------------------
    @staticmethod
    def _gb(value):
        return "-" if value is None else f"{value / 1024**3:.2f}GB"

    def snapshot(self, sample, reason):
        elapsed = sample["elapsed_seconds"]
        parts = [
            f"[{reason}] t+{int(elapsed // 60)}m{int(elapsed % 60):02d}s",
            f"stage={sample['stage']}",
            f"proc r/w={self._gb(sample['proc_read_bytes'])}/"
            f"{self._gb(sample['proc_write_bytes'])}",
        ]
        if sample.get("proc_read_bytes_mbs") is not None:
            parts.append(f"read={sample['proc_read_bytes_mbs']}MB/s")
        if sample.get("proc_write_bytes_mbs") is not None:
            parts.append(f"write={sample['proc_write_bytes_mbs']}MB/s")
        if sample["robocopy_processes"]:
            parts.append(
                f"ROBOCOPY x{sample['robocopy_processes']} "
                f"w={self._gb(sample['robocopy_write_bytes'])}")
            if sample.get("robocopy_write_bytes_mbs") is not None:
                parts.append(f"tape={sample['robocopy_write_bytes_mbs']}MB/s")
        if sample.get("staging_bytes") is not None:
            parts.append(f"staging={self._gb(sample['staging_bytes'])}"
                         f"/{sample['staging_files']}f")
        print(" | ".join(parts), flush=True)
        if sample.get("stage_transition"):
            print(f"    -> STAGE {sample['stage_transition']['from']} -> "
                  f"{sample['stage_transition']['to']}", flush=True)
        for line in sample.get("log", []):
            print(f"    log: {line[:160]}", flush=True)

    def run(self):
        print(f"telemetry -> {self.args.out} "
              f"(interval {self.args.interval}s, report "
              f"{self.args.report_seconds}s)", flush=True)
        idle_rounds = 0
        try:
            while True:
                sample = self.sample()
                elapsed = sample["elapsed_seconds"]
                due = (elapsed - self.last_report) >= self.args.report_seconds
                if self.samples == 1 or due or sample.get("stage_transition"):
                    self.snapshot(
                        sample,
                        "transition" if sample.get("stage_transition")
                        else "tick")
                    self.last_report = elapsed
                if sample["archiver_processes"] == 0:
                    idle_rounds += 1
                    if idle_rounds >= self.args.exit_after_idle:
                        self.snapshot(sample, "final")
                        print("archiver gone; telemetry stopping", flush=True)
                        break
                else:
                    idle_rounds = 0
                if self.args.max_seconds and elapsed >= self.args.max_seconds:
                    print("max-seconds reached; telemetry stopping", flush=True)
                    break
                time.sleep(self.args.interval)
        finally:
            self.out.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="backup_logs/chunk49_telemetry.jsonl")
    parser.add_argument("--log", default="backup_logs/archiver.log")
    parser.add_argument("--staging", default=r"C:\temp_for_disk\staging")
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--report-seconds", type=float, default=300.0)
    parser.add_argument("--exit-after-idle", type=int, default=3,
                        help="Consecutive samples with no archiver before "
                             "stopping.")
    parser.add_argument("--max-seconds", type=float, default=0.0)
    parser.add_argument("--tape-free", default="",
                        help="Optional LTFS root for a single free-space stat "
                             "per sample. Off by default so idle LTFS stays "
                             "untouched.")
    Sampler(parser.parse_args()).run()


if __name__ == "__main__":
    main()
