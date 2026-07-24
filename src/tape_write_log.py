"""Durable per-write raw robocopy log.

Every tape-write attempt gets its own append-mode log file, written and flushed
*incrementally* while robocopy runs, so the complete evidence survives a killed
process, a detached-console closure, a Ctrl+C, an exception, or a summary-less
robocopy failure. This module performs NO tape I/O — it only writes to the
local backup-log directory.

Layout (kept out of git via the ``backup_logs/`` .gitignore rule):
    <log_dir>/tape_write/session_<session_id>/chunk_<chunk_index>_<timestamp>.log
"""
import os
import threading
from datetime import datetime


def _sanitize_cmd(cmd):
    """Render the robocopy invocation for the log.

    The tape-write robocopy command carries only source/destination paths and
    flags — no credentials or secrets — but keep this the single choke point so
    any future secret-bearing argument can be redacted here.
    """
    return ' '.join(str(a) for a in (cmd or []))


class TapeWriteRawLog:
    """Append-only raw log for one robocopy tape-write attempt.

    Usable as a context manager. ``write`` is the thread-safe sink handed to
    :func:`src.robocopy._run_robocopy_tuned` (its stdout/stderr pumps call it
    from two threads). ``write_footer`` records the parsed verdict once the
    process has exited. ``close`` is idempotent so a ``finally`` can always call
    it without double-close errors.
    """

    def __init__(self, log_dir, session_id, chunk_index, tape_label,
                 source, dest, cmd, expected_files=None, expected_bytes=None):
        ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
        sid = 'na' if session_id is None else session_id
        cidx = 'na' if chunk_index is None else chunk_index
        sub = os.path.join(log_dir or '.', 'tape_write', f'session_{sid}')
        os.makedirs(sub, exist_ok=True)
        self.path = os.path.join(sub, f'chunk_{cidx}_{ts}.log')
        self._lock = threading.Lock()
        self._closed = False
        self._footer_written = False
        # Line-buffered append; each write is flushed so a crash loses nothing.
        self._fh = open(self.path, 'a', encoding='utf-8', errors='replace')
        self._write_header(session_id, chunk_index, tape_label, source, dest,
                           cmd, expected_files, expected_bytes)

    def _emit(self, text):
        # Caller holds self._lock.
        if self._closed:
            return
        try:
            self._fh.write(text)
            self._fh.flush()
        except Exception:
            pass

    def _write_header(self, session_id, chunk_index, tape_label, source, dest,
                      cmd, expected_files, expected_bytes):
        with self._lock:
            self._emit(
                "===== TAPE-WRITE RAW LOG =====\n"
                f"timestamp        : {datetime.now().isoformat()}\n"
                f"session_id       : {session_id}\n"
                f"chunk_index      : {chunk_index}\n"
                f"tape_label       : {tape_label}\n"
                f"source           : {source}\n"
                f"destination      : {dest}\n"
                f"expected_files   : {expected_files}\n"
                f"expected_bytes   : {expected_bytes}\n"
                f"robocopy_cmd     : {_sanitize_cmd(cmd)}\n"
                "----- robocopy output (streamed live) -----\n")

    def write(self, text):
        """Thread-safe sink for live robocopy stdout/stderr lines."""
        with self._lock:
            self._emit(text)

    def write_footer(self, returncode, rc_sum, verdict):
        """Record the parsed summary and the application's final verdict."""
        rc_sum = rc_sum or {}
        error_lines = getattr(verdict, 'error_lines', None) or []
        with self._lock:
            if self._footer_written:
                return
            self._footer_written = True
            lines = [
                "\n----- classification -----\n",
                f"return_code        : {returncode}\n",
                f"summary_found      : {rc_sum.get('summary_found')}\n",
                f"summary_malformed  : {rc_sum.get('summary_malformed')}\n",
                f"files_total        : {rc_sum.get('files_total')}\n",
                f"files_copied       : {rc_sum.get('files_copied')}\n",
                f"files_skipped      : {rc_sum.get('files_skipped')}\n",
                f"files_mismatch     : {rc_sum.get('files_mismatch')}\n",
                f"files_failed       : {rc_sum.get('files_failed')}\n",
                f"files_extras       : {rc_sum.get('files_extras')}\n",
                f"bytes_total        : {rc_sum.get('bytes_total')}\n",
                f"bytes_copied       : {rc_sum.get('bytes_copied')}\n",
                f"error_lines_count  : {len(error_lines)}\n",
            ]
            for el in error_lines:
                lines.append(f"error_line         : {el}\n")
            if verdict is not None:
                lines.append(
                    f"classification     : "
                    f"{'SUCCESS' if verdict.is_success else 'FAILURE'} "
                    f"({verdict.category}) — {verdict.detail}\n")
            self._emit(''.join(lines))

    @property
    def footer_written(self):
        return self._footer_written

    def note(self, text):
        """Record an out-of-band note (e.g. an exception propagated)."""
        with self._lock:
            self._emit(f"\n----- note -----\n{text}\n")

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._fh.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
