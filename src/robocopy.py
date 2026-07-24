"""robocopy execution/parsing and Defender exclusion helpers."""
import os
import re
import time
import threading
import subprocess
from collections import namedtuple

from .runtime import CANCEL, _apply_proc_tuning, _is_admin, _progress_done, _progress_line, register_proc, unregister_proc


# Robocopy error lines look like "2024/01/01 12:00:00 ERROR 32 (0x00000020)
# Copying File ...". Matching the full shape (instead of the substring
# "ERROR ") keeps a source path that merely contains the word from failing
# the whole run.
_ROBOCOPY_ERROR_RE = re.compile(r'ERROR \d+ \(0x[0-9A-Fa-f]{8}\)')


# Verdict returned by classify_robocopy_result. ``is_success`` is the single
# authoritative gate the tape writer commits on; ``category`` names the failure
# mode for logs/diagnostics; ``detail`` is a human sentence; ``error_lines`` are
# the raw robocopy ERROR lines detected (kept even on success — a valid summary
# with a recovered transient is still success, but the evidence is preserved).
RobocopyVerdict = namedtuple(
    'RobocopyVerdict', 'is_success category detail error_lines')


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
      files_total, files_copied, files_skipped, files_mismatch, files_failed,
      files_extras, bytes_total, bytes_copied, speed_mbs, elapsed,
      summary_found, summary_malformed

    ``summary_found`` records whether the final "Files :" summary line was
    seen *and* fully parsed into its six integer counters; when it is absent
    (robocopy died before printing its summary) the zeroed counters must not be
    trusted as "no failures". ``summary_malformed`` is set when a "Files :"
    header line IS present but its counters could not be parsed — a truncated or
    garbled summary that must likewise never read as success.
    """
    result = {
        'files_total': 0, 'files_copied': 0, 'files_skipped': 0,
        'files_mismatch': 0, 'files_failed': 0, 'files_extras': 0,
        'bytes_total': 0, 'bytes_copied': 0, 'speed_mbs': 0.0, 'elapsed': '',
        'summary_found': False, 'summary_malformed': False,
    }
    output = output or ''
    for line in output.splitlines():
        parts = line.split()
        if not parts:
            continue

        # "Files :  5  5  0  0  0  0"  (Total Copied Skipped Mismatch Failed Extras)
        # robocopy also echoes the file filter in its options header as
        # "Files : *.*"; that is NOT the summary. Only a line whose first field
        # is an integer counter is treated as a summary candidate, so the echo
        # is ignored rather than mistaken for a corrupt summary (which would
        # otherwise mark EVERY real run malformed and reject good writes).
        if (parts[0] == 'Files' and len(parts) >= 2 and parts[1] == ':'
                and len(parts) >= 3 and parts[2].lstrip('-').isdigit()):
            nums = parts[2:8]
            try:
                if len(nums) < 6:
                    raise ValueError("incomplete Files summary")
                total, copied, skipped, mismatch, failed, extras = (
                    int(n) for n in nums)
                result['files_total']    = total
                result['files_copied']   = copied
                result['files_skipped']  = skipped
                result['files_mismatch'] = mismatch
                result['files_failed']   = failed
                result['files_extras']   = extras
                result['summary_found']  = True
            except (ValueError, IndexError):
                # A numeric-leading "Files :" line that will not fully parse is a
                # malformed summary, not an absent one: record it so the write is
                # never trusted.
                result['summary_malformed'] = True

        # "Bytes :  4.52 g  4.52 g  0  0  0  0"

        elif parts[0] == 'Bytes' and len(parts) >= 4 and parts[1] == ':':
            total_b,      i = _parse_robocopy_bytes(parts, 2)  # total
            bytes_copied, _ = _parse_robocopy_bytes(parts, i)  # copied
            result['bytes_total']  = total_b
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


def _run_robocopy_tuned(cmd, priority=None, affinity=None, on_start=None,
                        raw_sink=None):
    """Run robocopy as a tracked, cancellable child with optional CPU priority
    and core affinity (the tape-write step). Returns a CompletedProcess with
    .stdout/.returncode so it is a drop-in for _run_robocopy_capture.

    Registering the process lets Ctrl+C terminate the live tape write; the HIGH
    priority + dedicated cores keep the LTO drive streaming without contention.

    ``on_start`` (optional) is called once with the live Popen right after the
    process is created and tuned, so a passive profiler can read the robocopy
    process's own I/O counters. It must not block or touch the process; any
    exception it raises is swallowed so it can never disturb the tape write.

    ``raw_sink`` (optional) is any object with a ``.write(str)`` method. Every
    stdout/stderr line is written to it *incrementally, as it arrives* (stderr
    lines prefixed ``[stderr]``), so the complete robocopy output is durably
    persisted even if this process is later killed, the console is detached, or
    the summary is never printed. Sink errors are swallowed — logging must never
    disturb the tape write. stdout/stderr are still accumulated and returned in
    full on the CompletedProcess so parsing/classification are unchanged."""
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,   # line-buffered so the sink sees each line as it is emitted
    )
    # Protected: a cooperative Ctrl+C must let the active tape write finish and
    # commit rather than cut it and leave the chunk ambiguously 'backing'. Only a
    # forced second Ctrl+C kills it. (A restore's per-file robocopy is a plain,
    # cancellable child — reads are safe to interrupt.)
    register_proc(proc, protected=True)
    _apply_proc_tuning(proc, priority=priority, affinity=affinity, label='robocopy-tape')
    if on_start is not None:
        try:
            on_start(proc)
        except Exception:
            pass

    out_chunks = []
    err_chunks = []

    def _pump(stream, buf, prefix):
        # Read line by line and tee to the durable sink as each line arrives.
        try:
            for line in iter(stream.readline, ''):
                buf.append(line)
                if raw_sink is not None:
                    try:
                        raw_sink.write(prefix + line if prefix else line)
                    except Exception:
                        pass
        finally:
            try:
                stream.close()
            except Exception:
                pass

    t_out = threading.Thread(
        target=_pump, args=(proc.stdout, out_chunks, ''), daemon=True)
    t_err = threading.Thread(
        target=_pump, args=(proc.stderr, err_chunks, '[stderr] '), daemon=True)
    t_out.start()
    t_err.start()
    try:
        proc.wait()
    finally:
        unregister_proc(proc)
    # The readers exit once the pipes hit EOF (after the process ends).
    t_out.join(timeout=30)
    t_err.join(timeout=30)
    return subprocess.CompletedProcess(
        cmd, proc.returncode, ''.join(out_chunks), ''.join(err_chunks))


def classify_robocopy_result(returncode, rc_sum, rc_output,
                             expected_files=None, expected_bytes=None):
    """Decide whether a robocopy tape write is trustworthy using ONLY the
    robocopy process's own evidence (its return code, complete output, parsed
    summary) plus locally-known source-side expectations. No tape is read.

    Returns a :class:`RobocopyVerdict`. Conservative by construction: a missing
    or malformed summary, or an unexpected zero-copy result, is NEVER success —
    even when the process happened to return 0. The deliberate existing rule is
    preserved: an ERROR line accompanied by a *complete* summary with zero failed
    files is a transient robocopy retried successfully (/R /W) and stays success;
    the summary counters are the authoritative verdict.

    ``expected_files``/``expected_bytes`` are the source-side work submitted to
    this write (counted locally before the copy). They gate the zero-copy sanity
    check only — they are never compared against anything read back from tape.
    """
    output = rc_output or ''
    error_lines = [ln.strip() for ln in output.splitlines()
                   if _ROBOCOPY_ERROR_RE.search(ln)]
    has_error = bool(error_lines)
    summary_found = bool(rc_sum.get('summary_found'))
    summary_malformed = bool(rc_sum.get('summary_malformed'))
    files_failed = int(rc_sum.get('files_failed', 0) or 0)
    files_copied = int(rc_sum.get('files_copied', 0) or 0)
    files_skipped = int(rc_sum.get('files_skipped', 0) or 0)
    bytes_copied = int(rc_sum.get('bytes_copied', 0) or 0)

    # 1. The process never produced a usable return code (launch/interrupt).
    if returncode is None:
        return RobocopyVerdict(
            False, 'interrupted',
            'Robocopy did not complete (no return code)', error_lines)

    # 2. Robocopy's own hard-failure signals.
    if returncode >= 8:
        return RobocopyVerdict(
            False, 'nonzero_return_code',
            f'Robocopy returned failure code {returncode} (>=8)', error_lines)
    if 'RETRY LIMIT EXCEEDED' in output:
        return RobocopyVerdict(
            False, 'retry_limit_exceeded',
            'Robocopy exhausted its retry limit', error_lines)

    # 3. Summary integrity. A missing or malformed final summary is untrusted
    #    regardless of the (possibly 0) return code — this is the exact gap that
    #    let a summary-less exit-0 run masquerade as success.
    if summary_malformed:
        return RobocopyVerdict(
            False, 'malformed_summary',
            'Robocopy summary present but its counters could not be parsed',
            error_lines)
    if not summary_found:
        detail = ('ERROR detected and final summary missing' if has_error
                  else 'final Robocopy summary missing')
        return RobocopyVerdict(False, 'missing_summary', detail, error_lines)

    # 4. Explicit per-file failure inside an otherwise-complete summary.
    if files_failed > 0:
        return RobocopyVerdict(
            False, 'files_failed',
            f'{files_failed} file(s) failed to copy', error_lines)

    # 5. Zero-work sanity vs. the source-side work we submitted. If we staged
    #    files but robocopy neither copied nor skipped (already-on-tape) any,
    #    the write did nothing though work was expected. Requiring BOTH
    #    copied==0 and skipped==0 keeps a legitimate all-already-present resume
    #    (skipped>0) classified as success.
    if (expected_files and expected_files > 0
            and files_copied == 0 and files_skipped == 0):
        return RobocopyVerdict(
            False, 'zero_copy_unexpected',
            f'no files copied or skipped though {expected_files} were '
            'expected from source', error_lines)
    if (expected_bytes and expected_bytes > 0
            and bytes_copied == 0 and files_skipped == 0):
        return RobocopyVerdict(
            False, 'zero_copy_unexpected',
            f'no bytes copied though ~{expected_bytes} were expected from '
            'source', error_lines)

    # Success: complete summary, no failures, work is accounted for. Any ERROR
    # lines here were transient and recovered — the summary is authoritative.
    detail = ('complete summary, no failed files (recovered a transient ERROR)'
              if has_error else 'complete summary, no failed files')
    return RobocopyVerdict(True, 'success', detail, error_lines)


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
