"""robocopy execution/parsing and Defender exclusion helpers."""
import os
import time
import threading
import subprocess

from .runtime import CANCEL, _apply_proc_tuning, _progress_done, _progress_line, register_proc, unregister_proc


def _robocopy_file(src, dst, display_name=None):
    """
    Copy a single file using robocopy with unbuffered I/O.
    Shows a simple active heartbeat while copying.
    Returns True on success (robocopy exit code < 8).
    """
    src_dir  = os.path.dirname(os.path.abspath(src))
    dst_dir  = os.path.dirname(os.path.abspath(dst))
    filename = os.path.basename(src)
    os.makedirs(dst_dir, exist_ok=True)

    try:
        fsize = os.path.getsize(src)
    except OSError as e:
        print(f"\n[ERROR] Cannot access source file: {src} ({e})")
        return False
    label = display_name or filename
    disp  = (label[:15] + '..' + label[-5:]) if len(label) > 22 else label

    proc = subprocess.Popen(
        ['robocopy', src_dir, dst_dir, filename,
         '/J',    # unbuffered I/O — optimized for large files / tape
         '/IS',   # include same files (always copy)
         '/IT',   # include tweaked files (always copy)
         '/R:3',  # retry 3 times on failure
         '/W:10', # wait 10 s between retries
         '/NP', '/NDL', '/NJH', '/NJS', '/NFL',
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    register_proc(proc)  # so Ctrl+C can terminate a long restore copy

    stop_evt = threading.Event()

    def _monitor():
        start = time.time()
        while not stop_evt.wait(5):
            try:
                cur_size = os.path.getsize(dst) if os.path.exists(dst) else 0
            except OSError:
                cur_size = 0
            pct     = (cur_size / fsize * 100) if fsize else 100
            elapsed = int(time.time() - start)
            _progress_line(
                f"[COPYING] {disp} | {min(pct, 100):.1f}% | "
                f"elapsed {elapsed}s"
            )

    t = threading.Thread(target=_monitor, daemon=True)
    t.start()
    try:
        proc.wait()
    finally:
        unregister_proc(proc)
    stop_evt.set()
    t.join(timeout=2)
    _progress_done()

    # A robocopy killed by the cancel handler can exit with code 1 (the
    # ordinary "files copied" success code — TerminateProcess on Windows sets
    # exit code 1 when psutil is unavailable), leaving a truncated file that
    # looks successfully copied. Gate on intent: a copy that ended while a
    # cancellation was requested is never a success.
    if CANCEL.is_set():
        return False
    # robocopy exit codes < 8 indicate success (0=nothing done, 1=ok, 2-7=ok+extras)
    return proc.returncode < 8


def _parse_robocopy_bytes(tokens, idx):
    """
    Consume one bytes value from a robocopy summary token list.
    Handles both '4.52 g' (two tokens) and '1234567890' (one token).
    Returns (bytes_int, next_idx).
    """
    if idx >= len(tokens):
        return 0, idx
    val = tokens[idx].replace(',', '')
    idx += 1
    if idx < len(tokens) and tokens[idx].lower() in ('k', 'm', 'g', 't'):
        mult = {'k': 1024, 'm': 1024**2, 'g': 1024**3, 't': 1024**4}[tokens[idx].lower()]
        idx += 1
        try:
            return int(float(val) * mult), idx
        except ValueError:
            return 0, idx
    try:
        return int(float(val)), idx
    except ValueError:
        return 0, idx


def _parse_robocopy_summary(output):
    """
    Parse robocopy's captured stdout and return a dict with:
      files_copied, files_skipped, files_failed,
      bytes_copied, speed_mbs, elapsed, summary_found

    ``summary_found`` records whether the final "Files :" summary line was
    seen; when it is absent (robocopy died before printing its summary) the
    zeroed counters must not be trusted as "no failures".
    """
    result = {
        'files_copied': 0, 'files_skipped': 0, 'files_failed': 0,
        'bytes_copied': 0, 'speed_mbs': 0.0, 'elapsed': '',
        'summary_found': False,
    }
    output = output or ''
    for line in output.splitlines():
        parts = line.split()
        if not parts:
            continue

        # "Files :  5  5  0  0  0  0"  (Total Copied Skipped Mismatch Failed Extras)
        if parts[0] == 'Files' and len(parts) >= 7 and parts[1] == ':':
            try:
                result['files_copied']  = int(parts[3])
                result['files_skipped'] = int(parts[4])
                result['files_failed']  = int(parts[6])
                result['summary_found'] = True
            except (ValueError, IndexError):
                pass

        # "Bytes :  4.52 g  4.52 g  0  0  0  0"

        elif parts[0] == 'Bytes' and len(parts) >= 4 and parts[1] == ':':
            _,          i = _parse_robocopy_bytes(parts, 2)  # total (skip)
            bytes_copied, _ = _parse_robocopy_bytes(parts, i)
            result['bytes_copied'] = bytes_copied

        # "Speed :  59993856 Bytes/Sec."
        elif parts[0] == 'Speed' and len(parts) >= 4 and parts[1] == ':' and 'bytes/sec' in parts[3].lower():
            try:
                result['speed_mbs'] = float(parts[2].replace(',', '')) / 1024**2
            except (ValueError, IndexError):
                pass

        # "Times :  0:01:18  0:01:18  ..."
        elif parts[0] == 'Times' and len(parts) >= 3 and parts[1] == ':':
            result['elapsed'] = parts[2]

    return result


def _run_robocopy_capture(cmd):
    """Run robocopy and decode its localized console output without crashing."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
    )


def _run_robocopy_tuned(cmd, priority=None, affinity=None):
    """Run robocopy as a tracked, cancellable child with optional CPU priority
    and core affinity (the tape-write step). Returns a CompletedProcess with
    .stdout/.returncode so it is a drop-in for _run_robocopy_capture.

    Registering the process lets Ctrl+C terminate the live tape write; the HIGH
    priority + dedicated cores keep the LTO drive streaming without contention."""
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    register_proc(proc)
    _apply_proc_tuning(proc, priority=priority, affinity=affinity, label='robocopy-tape')
    try:
        out, err = proc.communicate()
    finally:
        unregister_proc(proc)
    return subprocess.CompletedProcess(cmd, proc.returncode, out, err)


def _is_admin():
    """True if the current process is running with Administrator privileges.

    Defender's exclusion list is only *readable* from an elevated context —
    Get-MpPreference succeeds for non-elevated callers but returns empty
    exclusion arrays, so we can't trust a negative result from there.
    Checking elevation up-front is the only reliable way to decide whether
    to touch Defender at all.
    """
    try:
        import ctypes
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _run_powershell(ps_command):
    """Invoke a PowerShell command. Returns CompletedProcess.
    Raises FileNotFoundError if PowerShell is missing,
    subprocess.CalledProcessError on non-zero exit."""
    return subprocess.run(
        ['powershell', '-NonInteractive', '-NoProfile', '-Command', ps_command],
        capture_output=True, text=True, check=True,
    )


def _robocopy_already_excluded():
    """Return True if robocopy.exe is already in Defender's ExclusionProcess list.

    Returns False if it isn't excluded, or if the lookup itself failed
    (insufficient privilege, Defender unavailable, etc.) — the caller will then
    attempt to add the exclusion and surface any error from that attempt.
    """
    ps = (
        "$p = (Get-MpPreference).ExclusionProcess; "
        "if ($p -and ($p -contains 'robocopy.exe')) { 'YES' } else { 'NO' }"
    )
    try:
        result = _run_powershell(ps)
    except FileNotFoundError:
        print("[DEFENDER] PowerShell not found — cannot check existing exclusions.")
        return False
    except subprocess.CalledProcessError as e:
        print("[DEFENDER] WARNING: could not query Defender exclusions "
              f"(exit {e.returncode}). Real-time scanning may still be active, "
              "which can trigger LTO shoe-shining due to hardware buffer drops. "
              "Continuing without process exclusion.")
        return False
    return (result.stdout or '').strip().upper() == 'YES'


def _add_robocopy_exclusion():
    """Attempt to add robocopy.exe as a Defender process exclusion.

    Returns True if the exclusion was added by us (and should be removed
    later). Returns False on any failure — typically lack of Administrator
    privileges — after printing a warning.
    """
    try:
        _run_powershell("Add-MpPreference -ExclusionProcess 'robocopy.exe'")
    except FileNotFoundError:
        print("[DEFENDER] PowerShell not found — skipping process exclusion.")
        return False
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or '').strip()
        print("!" * 60)
        print("[DEFENDER] WARNING: failed to add robocopy.exe process exclusion "
              f"(exit {e.returncode}).")
        if stderr:
            print(f"[DEFENDER] stderr: {stderr}")
        print("[DEFENDER] Without Administrator rights, Defender real-time "
              "scanning remains ACTIVE. This can intercept robocopy I/O and "
              "trigger LTO shoe-shining due to hardware buffer drops.")
        print("[DEFENDER] Continuing anyway — backup will proceed at reduced speed.")
        print("!" * 60)
        return False
    print("[DEFENDER] Added temporary process exclusion: robocopy.exe")
    return True


def _remove_robocopy_exclusion():
    """Remove the robocopy.exe process exclusion. Errors are logged, not raised."""
    try:
        _run_powershell("Remove-MpPreference -ExclusionProcess 'robocopy.exe'")
    except FileNotFoundError:
        return
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or '').strip()
        print(f"[DEFENDER] WARNING: failed to remove robocopy.exe exclusion "
              f"(exit {e.returncode}). You may want to remove it manually.")
        if stderr:
            print(f"[DEFENDER] stderr: {stderr}")
        return
    print("[DEFENDER] Removed temporary process exclusion: robocopy.exe")


def _prepare_robocopy_exclusion():
    """Ensure robocopy.exe is excluded from Defender for this run.

    Returns True if WE added the exclusion (caller must remove it in finally).
    Returns False if it was already excluded, if we can't read/modify Defender
    state without admin, or if the add itself failed.
    """
    if not _is_admin():
        print("[DEFENDER] Running without Administrator privileges. Cannot "
              "verify or modify Windows Defender exclusions. If 'robocopy.exe' "
              "is already globally excluded on this system, the backup will "
              "still run at full speed. Otherwise, real-time scanning may "
              "trigger LTO shoe-shining due to hardware buffer drops.")
        return False

    if _robocopy_already_excluded():
        print("[DEFENDER] robocopy.exe is already excluded. "
              "Proceeding to backup at max speed.")
        return False
    return _add_robocopy_exclusion()
