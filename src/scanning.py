"""Remote find scanners: batch and streaming manifest discovery over SSH."""
import codecs
import posixpath
import re
import shlex
import subprocess
import threading
import time

from .remote_transport import _ssh_run, _ssh_stream_command
from .runtime import CANCEL, _kill_proc_tree, register_proc, unregister_proc
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

#: One immediate-child listing plus a mutation-observation token, all
#: NUL-framed so a filename containing a newline cannot break the framing.
#: ``%y`` is a single character (f, d, l, ...), so the ``OBS `` prefix on the
#: first record can never collide with an entry record.
_LIST_DIRECTORY_SCRIPT = (
    'printf "OBS %s\\0" "$(stat -c %Y:%Z:%i -- "$0" 2>/dev/null || '
    'printf unknown)"; '
    'find "$0" -mindepth 1 -maxdepth 1 -printf "%y %s %p\\0"'
)

#: Directory entry kinds we descend into / archive. Anything else (symlink,
#: socket, fifo, device) is recorded as an exceptional entry rather than
#: silently dropped: "we chose not to archive this" is coverage information.
_ENTRY_FILE = 'f'
_ENTRY_DIRECTORY = 'd'


class DirectoryListing:
    """The result of listing ONE directory's immediate children."""

    __slots__ = ("path", "files", "directories", "errors", "observation",
                 "returncode")

    def __init__(self, path, files, directories, errors, observation,
                 returncode):
        self.path = path
        #: ``[(canonical_path, size), ...]`` sorted by canonical path.
        self.files = files
        #: ``[canonical_path, ...]`` sorted by canonical path.
        self.directories = directories
        #: ``[(category, path_or_None, message), ...]`` — exceptional entries.
        self.errors = errors
        #: Lightweight source-mutation token (mtime:ctime:inode), or None.
        self.observation = observation
        self.returncode = returncode

    @property
    def file_count(self):
        return len(self.files)

    @property
    def byte_count(self):
        return sum(size for _path, size in self.files)


class DirectoryFrontierScanner(RemoteScanner):
    """List ONE directory's immediate children at a time.

    Plan 1, Task 2.3. The production scanner enumerates whole roots
    recursively, so a crash replays every file already visited. This one is the
    unit the frontier persists: a single immediate listing, bounded in size,
    that can be committed and never repeated.

    Two properties make the ordering reproducible regardless of which worker
    listed what, or when:

    * a directory's entries are sorted by canonical path **within** the
      listing, so entry ordinals depend on the source, not on ``find``'s
      arbitrary directory order;
    * the listing is bounded (immediate children only), so that sort is
      bounded too — the reason this scanner never sorts a whole tree.

    An unreadable subdirectory, a non-UTF-8 name and a non-regular entry are
    each recorded as an EXCEPTIONAL ENTRY rather than dropped. Missing coverage
    that nobody recorded is indistinguishable from coverage.
    """

    def __init__(self, remote_user, remote_host, remote_password='',
                 timeout=None, skipped_tracker=None, ui=None, metrics=None):
        super().__init__(
            remote_user, remote_host, remote_password=remote_password,
            timeout=timeout, skipped_tracker=skipped_tracker, ui=ui)
        self.metrics = metrics

    def _note(self, method, *args):
        if self.metrics is None:
            return
        try:
            getattr(self.metrics, method)(*args)
        except Exception:               # never fail a scan for a counter
            pass

    def list_directory(self, dir_path):
        """Return a :class:`DirectoryListing` for one directory.

        Never recurses. Never raises for a per-entry problem — those become
        ``errors`` so the caller can decide whether coverage may be final.
        Raises only when the listing as a whole could not be obtained, because
        "we do not know what is in here" must not read as "it is empty".
        """
        self._note('note_listing_start')
        started = time.monotonic()
        root = posixpath.normpath(str(dir_path).replace('\\', '/').strip())
        command = ("LC_ALL=C sh -c " + shlex.quote(_LIST_DIRECTORY_SCRIPT)
                   + " " + shlex.quote(root))
        result = _ssh_run(
            self.remote_user, self.remote_host, command, capture=True,
            password=self.remote_password, timeout=self.timeout)

        stdout = result.stdout or ''
        stderr = (result.stderr or '').strip()
        if result.returncode == 124:
            raise RuntimeError(
                f"[SCAN] Listing {root!r} timed out. The partial output was "
                "discarded so a directory cannot be recorded as covered on "
                "incomplete evidence.")
        if result.returncode == 255:
            raise RuntimeError(
                f"[SCAN] SSH failed while listing {root!r} "
                f"(exit {result.returncode}):\n{stderr}")

        files, directories, errors = [], [], []
        observation = None
        for record in stdout.split('\0'):
            if not record:
                continue
            if record.startswith('OBS '):
                token = record[4:].strip()
                observation = None if token in ('', 'unknown') else token
                continue
            parsed = self._parse_entry(record, root)
            if parsed is None:
                errors.append(('unparsable_entry', None,
                               'listing record could not be parsed'))
                continue
            kind, size, path, problem = parsed
            if problem is not None:
                errors.append(problem)
                continue
            if kind == _ENTRY_FILE:
                files.append((path, size))
            elif kind == _ENTRY_DIRECTORY:
                directories.append(path)
            else:
                errors.append((
                    'non_regular_entry', path,
                    f"entry type {kind!r} is not archived"))

        if result.returncode != 0 and stderr:
            self._record_find_warnings(stderr)
            for line in stderr.splitlines():
                line = line.strip()
                if line:
                    errors.append(('listing_warning', None, line))

        files.sort(key=lambda item: item[0])
        directories.sort()
        self._note('note_enumeration', time.monotonic() - started, len(files))
        return DirectoryListing(root, files, directories, errors, observation,
                                result.returncode)

    def observe(self, dir_path):
        """Re-read a directory's mutation token only — the final sweep.

        Cheap by design: one ``stat``, no listing. A changed token invalidates
        that directory and its ancestors.
        """
        root = posixpath.normpath(str(dir_path).replace('\\', '/').strip())
        command = ("LC_ALL=C sh -c "
                   + shlex.quote('stat -c %Y:%Z:%i -- "$0" 2>/dev/null '
                                 '|| printf unknown')
                   + " " + shlex.quote(root))
        result = _ssh_run(
            self.remote_user, self.remote_host, command, capture=True,
            password=self.remote_password, timeout=self.timeout)
        token = (result.stdout or '').strip()
        if result.returncode != 0 or not token or token == 'unknown':
            return None
        return token

    def _parse_entry(self, record, root):
        """``'<type> <size> <path>'`` -> ``(kind, size, path, problem)``."""
        parts = record.split(' ', 2)
        if len(parts) != 3:
            return None
        kind, size_s, path = parts
        try:
            size = int(size_s)
        except ValueError:
            return kind, 0, path, (
                'invalid_size', path, f"invalid find size token: {size_s}")
        # U+FFFD means the original name was not valid UTF-8. Planning it would
        # send a mangled name to the remote tar, which silently reports it
        # "missing" — record the omission loudly instead.
        if '�' in path:
            self.skipped_tracker.add(
                'remote', path,
                "filename is not valid UTF-8 and cannot be fetched faithfully; "
                "rename it on the source host to archive it", 'scan')
            return kind, size, path, (
                'invalid_utf8_name', None,
                "a child name is not valid UTF-8 and was not archived")
        # An immediate child must lie directly under the directory listed.
        # Anything else is a corrupt record, and planning it could make a fetch
        # stream an entirely unplanned tree.
        normalized = posixpath.normpath(path)
        if posixpath.dirname(normalized) != root:
            return kind, size, path, (
                'entry_outside_directory', path,
                "listing record is not an immediate child (truncated stream?)")
        return kind, size, normalized, None


class StreamingRemoteScanner(RemoteScanner):
    """Yield remote find records as they arrive over a long-lived SSH stream."""

    def __init__(self, remote_user, remote_host, remote_password='',
                 skipped_tracker=None, ui=None, cipher='', metrics=None):
        super().__init__(
            remote_user,
            remote_host,
            remote_password=remote_password,
            timeout=None,
            skipped_tracker=skipped_tracker,
            ui=ui,
        )
        self.cipher = cipher
        # Optional src.pipeline_types.ScanMetrics. Purely observational: a
        # counter must never change what is scanned, planned or written, so
        # every call goes through _note(), which swallows its own failures.
        self.metrics = metrics

    def _note(self, method, *args):
        if self.metrics is None:
            return
        try:
            getattr(self.metrics, method)(*args)
        except Exception:                       # never fail a scan for a counter
            pass

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
        # One listing start per root, every run. On a resumed session this is
        # the replay: the same root is enumerated again from the beginning.
        self._note('note_listing_start')
        listing_started = time.monotonic()
        entries_yielded = 0
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
        # Register so a global Ctrl+C (CANCEL) can reap the ssh/find tree the
        # same way the tar fetch path does.
        register_proc(proc)
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

        # proc.stdout.read() blocks while the remote find is slow, so a
        # top-of-loop stop check alone cannot cancel a stalled scan. This
        # watcher kills the ssh (and thus remote find) tree as soon as stop_evt
        # or CANCEL is set, which unblocks the read and stops the local process.
        aborted = threading.Event()
        watcher_done = threading.Event()

        def _abort_watch():
            while not watcher_done.is_set():
                if (stop_evt is not None and stop_evt.is_set()) or CANCEL.is_set():
                    aborted.set()
                    _kill_proc_tree(proc)
                    return
                watcher_done.wait(0.25)

        stderr_thread = threading.Thread(
            target=_drain_stderr, name='streaming-find-stderr', daemon=True)
        stderr_thread.start()
        abort_thread = threading.Thread(
            target=_abort_watch, name='streaming-find-abort', daemon=True)
        abort_thread.start()

        decoder = codecs.getincrementaldecoder('utf-8')('replace')
        buffer = ''
        saw_record = False
        try:
            while True:
                if (stop_evt is not None and stop_evt.is_set()) or CANCEL.is_set():
                    _kill_proc_tree(proc)
                    aborted.set()
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
                        entries_yielded += 1
                        yield parsed
            tail = decoder.decode(b'', final=True)
            if tail:
                buffer += tail
            if buffer and not aborted.is_set():
                self.ui.warning(
                    "[REMOTE] Scan stream ended mid-record (truncated transfer?); "
                    "the partial record was discarded and recorded as skipped."
                )
                self._note('note_discarded_partial', 1)
                self._parse_record(buffer, root)
        finally:
            watcher_done.set()
            self._note('note_enumeration',
                       time.monotonic() - listing_started, entries_yielded)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                _kill_proc_tree(proc)
                proc.wait()
            unregister_proc(proc)
            stderr_thread.join(timeout=5)
            abort_thread.join(timeout=5)

        if aborted.is_set():
            # Cancelled scans stop quietly; the non-zero ssh exit is expected.
            return

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
