"""Remote find scanners: batch and streaming manifest discovery over SSH."""
import codecs
import posixpath
import re
import shlex
import subprocess
import threading

from .remote_transport import _ssh_run, _ssh_stream_command
from .planning import DirectoryPlanUnit
from .skipped import SkippedFileTracker
from .ui import ConsoleUI


_FIND_WARNING_RE = re.compile(r"^find:\s+[\"'`‘](.*?)[\"'`’]:\s*(.*)$")
_FIND_PATH_QUOTES = "\"'`\u2018\u2019\u201c\u201d\ufffd?"

class RemoteScanner:
    """Run remote find and preserve partial-scan omissions as skipped rows."""

    def __init__(self, remote_user, remote_host, remote_password='',
                 timeout=None, skipped_tracker=None, ui=None):
        self.remote_user = remote_user
        self.remote_host = remote_host
        self.remote_password = remote_password
        self.timeout = timeout
        self.skipped_tracker = skipped_tracker or SkippedFileTracker()
        self.ui = ui or ConsoleUI()

    def _record_find_warnings(self, stderr):
        def clean_find_path(path):
            path = (path or '').strip(_FIND_PATH_QUOTES)
            for marker in ("ג€˜", "ג€™", "ג€�", "ג€?"):
                path = path.removeprefix(marker).removesuffix(marker)
            return path.strip(_FIND_PATH_QUOTES)

        for line in (stderr or '').splitlines():
            line = line.strip()
            if not line:
                continue
            match = _FIND_WARNING_RE.match(line)
            if match:
                path, reason = match.groups()
                path = clean_find_path(path)
            elif line.startswith("find:") and ": " in line:
                path, reason = line[len("find:"):].strip().rsplit(": ", 1)
                path = clean_find_path(path)
            else:
                path, reason = line, "remote find warning"
            self.skipped_tracker.add('remote', path, reason, 'scan')

    def _scan_one(self, scan_path):
        # LC_ALL=C pins find's diagnostics to English so the warning parser
        # stays locale-independent (filenames are bytes and unaffected).
        find_cmd = f"LC_ALL=C find {shlex.quote(scan_path)} -type f -printf '%s %p\\0'"
        result = _ssh_run(
            self.remote_user,
            self.remote_host,
            find_cmd,
            capture=True,
            password=self.remote_password,
            timeout=self.timeout,
        )
        stdout = result.stdout or ''
        stderr = (result.stderr or '').strip()
        if result.returncode == 124:
            if stderr:
                self._record_find_warnings(stderr)
            raise RuntimeError(
                f"[REMOTE] SSH scan timed out while scanning {scan_path!r}. "
                "The partial find output was discarded so an incomplete backup "
                "session cannot be created. Increase "
                "[PERFORMANCE] ssh_command_timeout_seconds or split the "
                "selection into smaller runs, then start a fresh session."
            )
        if result.returncode == 255:
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {result.returncode}):\n{stderr}"
            )
        if result.returncode != 0 and stderr:
            self._record_find_warnings(stderr)
            self.ui.warning(
                f"[REMOTE] Scan completed with warnings (find exit {result.returncode}); "
                "inaccessible paths were recorded in the skipped-file report."
            )
            if not stdout.strip():
                return []
        elif result.returncode != 0 and not stdout.strip():
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {result.returncode}):\n{stderr}"
            )
        if stdout and not stdout.endswith('\0'):
            # find terminates every record with NUL; a different tail means the
            # stream was cut mid-record. The fragment is rejected below (its
            # truncated path cannot sit under a scan root), never planned.
            self.ui.warning(
                "[REMOTE] Scan stream ended mid-record (truncated transfer?); "
                "the partial record was discarded and recorded as skipped."
            )
        root = posixpath.normpath(scan_path.replace('\\', '/').strip())
        manifest = []
        for record in stdout.split('\0'):
            if not record:
                continue
            parts = record.split(' ', 1)
            if len(parts) != 2:
                continue
            size_s, path = parts
            try:
                size = int(size_s)
            except ValueError:
                self.skipped_tracker.add(
                    'remote', path, f"invalid find size token: {size_s}", 'scan')
                continue
            # Linux filenames are bytes; the SSH capture decodes with
            # errors='replace', so U+FFFD here means the original name was not
            # valid UTF-8. Planning it would send a mangled name to the remote
            # tar, which silently reports it "missing" — record the omission
            # loudly at scan time instead so the operator can rename the file.
            if '�' in path:
                self.skipped_tracker.add(
                    'remote', path,
                    "filename is not valid UTF-8 and cannot be fetched "
                    "faithfully; rename it on the source host to archive it",
                    'scan')
                continue
            # Every legitimate record lies under (or is) a scan root. Anything
            # else is a corrupt/truncated record; planning it is dangerous — a
            # path like '/strg' names a directory and would make the fetch tar
            # stream an entire unplanned tree.
            norm = posixpath.normpath(path)
            if not (norm == root or norm.startswith(root + '/')):
                self.skipped_tracker.add(
                    'remote', path,
                    "scan record outside scan roots (truncated stream?)",
                    'scan')
                continue
            manifest.append((path, size))
        return manifest

    def scan(self, scan_paths):
        manifest = []
        for scan_path in scan_paths:
            self.ui.info(f"[REMOTE] Scanning {scan_path} ...")
            manifest.extend(self._scan_one(scan_path))
        return manifest

class StreamingRemoteScanner(RemoteScanner):
    """Yield remote find records as they arrive over a long-lived SSH stream."""

    def __init__(self, remote_user, remote_host, remote_password='',
                 skipped_tracker=None, ui=None, cipher=''):
        super().__init__(
            remote_user,
            remote_host,
            remote_password=remote_password,
            timeout=None,
            skipped_tracker=skipped_tracker,
            ui=ui,
        )
        self.cipher = cipher

    def _parse_record(self, record, root):
        parts = record.split(' ', 1)
        if len(parts) != 2:
            return None
        size_s, path = parts
        try:
            size = int(size_s)
        except ValueError:
            self.skipped_tracker.add(
                'remote', path, f"invalid find size token: {size_s}", 'scan')
            return None
        # See RemoteScanner._scan_one: U+FFFD marks a non-UTF-8 filename that
        # a fetch would silently drop as "source missing" — skip it loudly.
        if '�' in path:
            self.skipped_tracker.add(
                'remote', path,
                "filename is not valid UTF-8 and cannot be fetched "
                "faithfully; rename it on the source host to archive it",
                'scan')
            return None
        norm = posixpath.normpath(path)
        if not (norm == root or norm.startswith(root + '/')):
            self.skipped_tracker.add(
                'remote', path,
                "scan record outside scan roots (truncated stream?)",
                'scan')
            return None
        return path, size

    def iter_scan(self, scan_paths, stop_evt=None):
        for scan_path in scan_paths:
            if stop_evt is not None and stop_evt.is_set():
                return
            yield from self._iter_scan_one(scan_path, stop_evt=stop_evt)

    def _iter_scan_one(self, scan_path, stop_evt=None):
        self.ui.info(f"[REMOTE] Streaming scan {scan_path} ...")
        root = posixpath.normpath(scan_path.replace('\\', '/').strip())
        find_cmd = f"LC_ALL=C find {shlex.quote(scan_path)} -type f -printf '%s %p\\0'"
        ssh_cmd, env, err = _ssh_stream_command(
            self.remote_user,
            self.remote_host,
            find_cmd,
            password=self.remote_password,
            cipher=self.cipher,
        )
        if err:
            raise RuntimeError(f"[REMOTE] SSH scan failed: {err}")
        if ssh_cmd is None:
            raise RuntimeError("[REMOTE] SSH scan failed: no command produced")

        proc = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        stderr_chunks = []

        def _drain_stderr():
            try:
                while True:
                    chunk = proc.stderr.read(65536) if proc.stderr else b''
                    if not chunk:
                        return
                    stderr_chunks.append(chunk)
            except OSError:
                return

        stderr_thread = threading.Thread(
            target=_drain_stderr, name='streaming-find-stderr', daemon=True)
        stderr_thread.start()

        decoder = codecs.getincrementaldecoder('utf-8')('replace')
        buffer = ''
        saw_record = False
        try:
            while True:
                if stop_evt is not None and stop_evt.is_set():
                    proc.terminate()
                    break
                chunk = proc.stdout.read(65536) if proc.stdout else b''
                if not chunk:
                    break
                buffer += decoder.decode(chunk)
                while '\0' in buffer:
                    record, buffer = buffer.split('\0', 1)
                    if not record:
                        continue
                    parsed = self._parse_record(record, root)
                    if parsed is not None:
                        saw_record = True
                        yield parsed
            tail = decoder.decode(b'', final=True)
            if tail:
                buffer += tail
            if buffer:
                self.ui.warning(
                    "[REMOTE] Scan stream ended mid-record (truncated transfer?); "
                    "the partial record was discarded and recorded as skipped."
                )
                self._parse_record(buffer, root)
        finally:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            stderr_thread.join(timeout=5)

        stderr = b''.join(stderr_chunks).decode('utf-8', errors='replace').strip()
        if proc.returncode == 255:
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {proc.returncode}):\n{stderr}"
            )
        if proc.returncode != 0 and stderr:
            self._record_find_warnings(stderr)
            self.ui.warning(
                f"[REMOTE] Streaming scan completed with warnings "
                f"(find exit {proc.returncode}); inaccessible paths were "
                "recorded in the skipped-file report."
            )
        elif proc.returncode != 0 and not saw_record:
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {proc.returncode}):\n{stderr}"
            )


class DirectoryFirstRemoteScanner(RemoteScanner):
    """Directory-level remote scanner for directory-first planning.

    This scanner keeps the high-cardinality tiny-file names on the remote host
    during planning. It emits directory statistics and exposes a separate
    large-file iterator for files that must remain searchable in files_index.
    """

    def __init__(self, remote_user, remote_host, remote_password='',
                 timeout=None, skipped_tracker=None, ui=None, depth=2,
                 large_file_min_mb=10):
        super().__init__(
            remote_user, remote_host, remote_password=remote_password,
            timeout=timeout, skipped_tracker=skipped_tracker, ui=ui)
        self.depth = max(0, int(depth))
        self.large_file_min_mb = float(large_file_min_mb or 10)

    def discover_candidate_dirs(self, scan_path):
        cmd = (
            f"LC_ALL=C find {shlex.quote(scan_path)} -mindepth 0 "
            f"-maxdepth {self.depth} -type d -print0"
        )
        result = _ssh_run(
            self.remote_user, self.remote_host, cmd, capture=True,
            password=self.remote_password, timeout=self.timeout)
        stderr = (result.stderr or '').strip()
        if result.returncode != 0 and stderr:
            self._record_find_warnings(stderr)
        if result.returncode == 255:
            raise RuntimeError(
                f"[REMOTE] SSH directory scan failed:\n{stderr}")
        root = posixpath.normpath(scan_path.replace('\\', '/').strip())
        dirs = []
        for path in (result.stdout or '').split('\0'):
            if not path:
                continue
            norm = posixpath.normpath(path)
            if norm == root or norm.startswith(root + '/'):
                dirs.append(norm)
        return dirs

    def stat_directory(self, dir_path):
        threshold = int(self.large_file_min_mb * 1024 * 1024)
        script = r"""
root=$1
threshold=$2
find "$root" -type f -printf '%s %h\0' |
awk -v RS='\0' -v root="$root" -v threshold="$threshold" '
BEGIN { direct_count=0; direct_bytes=0; rec_count=0; rec_bytes=0;
        small_count=0; small_bytes=0; large_count=0; large_bytes=0; }
NF {
  size=$1; dir=$0; sub(/^[0-9]+ /, "", dir);
  rec_count++; rec_bytes += size;
  if (dir == root) { direct_count++; direct_bytes += size; }
  if (size < threshold) { small_count++; small_bytes += size; }
  else { large_count++; large_bytes += size; }
}
END { printf "%d %d %d %d %d %d %d %d\n",
      direct_count, direct_bytes, rec_count, rec_bytes,
      small_count, small_bytes, large_count, large_bytes; }'
"""
        cmd = (
            "bash -lc " + shlex.quote(script)
            + " _ " + shlex.quote(dir_path) + " " + str(threshold)
        )
        result = _ssh_run(
            self.remote_user, self.remote_host, cmd, capture=True,
            password=self.remote_password, timeout=self.timeout)
        if result.returncode != 0:
            raise RuntimeError(
                f"[REMOTE] Directory stat failed for {dir_path}:\n"
                f"{(result.stderr or '').strip()}")
        parts = (result.stdout or '').strip().split()
        if len(parts) != 8:
            raise RuntimeError(
                f"[REMOTE] Could not parse directory stats for {dir_path}: "
                f"{result.stdout!r}")
        values = [int(part) for part in parts]
        return DirectoryPlanUnit(
            original_dir_path=posixpath.normpath(dir_path),
            direct_file_count=values[0],
            direct_bytes=values[1],
            recursive_file_count=values[2],
            recursive_bytes=values[3],
            small_file_count=values[4],
            small_file_bytes=values[5],
            large_file_count=values[6],
            large_file_bytes=values[7],
            depth=len([p for p in dir_path.strip('/').split('/') if p]),
        )

    def iter_large_files(self, dir_path):
        threshold = int(self.large_file_min_mb)
        cmd = (
            f"LC_ALL=C find {shlex.quote(dir_path)} -type f "
            f"-size +{threshold - 1}M -printf '%s %p\\0'"
        )
        result = _ssh_run(
            self.remote_user, self.remote_host, cmd, capture=True,
            password=self.remote_password, timeout=self.timeout)
        if result.returncode != 0 and (result.stderr or '').strip():
            self._record_find_warnings(result.stderr)
        root = posixpath.normpath(dir_path.replace('\\', '/').strip())
        for record in (result.stdout or '').split('\0'):
            parsed = self._parse_large_record(record, root)
            if parsed is not None:
                yield parsed

    def _parse_large_record(self, record, root):
        if not record:
            return None
        parts = record.split(' ', 1)
        if len(parts) != 2:
            return None
        size_s, path = parts
        try:
            size = int(size_s)
        except ValueError:
            return None
        norm = posixpath.normpath(path)
        if norm == root or norm.startswith(root + '/'):
            return norm, size
        self.skipped_tracker.add(
            'remote', path, "large-file record outside directory root", 'scan')
        return None
