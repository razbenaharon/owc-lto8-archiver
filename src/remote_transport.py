"""SSH/SCP/tar transport over OpenSSH."""
import os
import shutil
import threading
import subprocess
import tempfile
import shlex
import atexit
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from .constants import PROJECT_ROOT
from .paths import _safe_remote_relpath, validate_remote_posix_relpath
from .pipeline_types import SourceDisposition, StoredTarSourceDiagnostic
from .runtime import CANCEL, _apply_proc_tuning, _kill_proc_tree, register_proc, unregister_proc


def _has_command(name):
    return shutil.which(name) is not None


def _hostkey_policy():
    """Host-key trust policy for every SSH/SCP invocation.

    The default, 'accept-new', trusts a host key on first contact (TOFU) —
    adequate for a fixed lab fleet whose keys are already in known_hosts, but
    a first-contact MITM window when they are not. Set
    LTO_SSH_HOSTKEY_POLICY=yes (environment variable or .env) after
    provisioning known_hosts (e.g. via ssh-keyscan) to pin keys strictly.
    """
    value = os.environ.get('LTO_SSH_HOSTKEY_POLICY', '').strip()
    if not value:
        from .config import _load_env_file
        value = _load_env_file(os.path.join(PROJECT_ROOT, '.env')).get(
            'LTO_SSH_HOSTKEY_POLICY', '').strip()
    return value or 'accept-new'


SSH_HOSTKEY_POLICY = _hostkey_policy()

# Keepalive for long streaming fetches: if the peer stops answering SSH-level
# keepalives (a dead/half-open TCP connection), ssh gives up after
# ~ServerAliveInterval * ServerAliveCountMax seconds instead of blocking the
# local tar's communicate() forever. This only detects a dead *peer*; a peer
# that answers keepalives while its tar/mbuffer delivers no data is caught
# instead by the orchestrator's staging stall watchdog (fetch_stall_timeout).
_SSH_STREAM_KEEPALIVE_OPTS = [
    '-o', 'ServerAliveInterval=15',
    '-o', 'ServerAliveCountMax=4',
    '-o', 'ConnectTimeout=30',
]

_TAR_STORE_STDERR_LIMIT = 2 * 1024 * 1024
_SOURCE_EXCEPTION_SUFFIXES = (
    "Warning: Cannot stat: No such file or directory",
    "Warning: Cannot open: Permission denied",
    "Warning: Cannot open: Input/output error",
    "Warning: Cannot read: Input/output error",
)


@dataclass(frozen=True)
class RemoteTarStoreResult:
    """One direct remote-TAR attempt, including explicit trust boundaries."""

    ok: bool
    part_path: str
    archive_size: int
    remote_exit_code: Optional[int]
    stderr: str
    diagnostics: Tuple[StoredTarSourceDiagnostic, ...] = ()
    unresolved: Tuple[str, ...] = ()
    transport_mode: str = "plain_tar"
    tar_exit_verified: bool = True
    cancelled: bool = False
    error: str = ""


def _stored_tar_command(remote_base):
    """The pinned first-version producer dialect; paths arrive only on stdin."""
    return (
        f"LC_ALL=C tar -C {shlex.quote(str(remote_base))} -b 512 "
        "--format=pax --sparse --sparse-version=1.0 --no-recursion "
        "--ignore-failed-read -cf - --null -T -"
    )


def _mbuffer_pipefail_capable(remote_user, remote_host, *, password=''):
    """Prove both tools exist and that bash propagates a failed producer."""
    command = (
        "command -v bash >/dev/null 2>&1 && "
        "command -v mbuffer >/dev/null 2>&1 || exit 1; "
        "bash -o pipefail -c 'false | true'; test $? -ne 0"
    )
    result = _ssh_run(
        remote_user, remote_host, command, capture=True,
        password=password, timeout=30)
    return result.returncode == 0


def _bounded_stderr_reader(pipe, chunks, state):
    """Drain stderr without allowing a hostile diagnostic stream to grow RAM."""
    kept = 0
    try:
        while True:
            block = pipe.read(65536)
            if not block:
                break
            room = _TAR_STORE_STDERR_LIMIT - kept
            if room > 0:
                chunks.append(block[:room])
                kept += min(room, len(block))
            if len(block) > room:
                state["truncated"] = True
    except OSError:
        state["read_error"] = True


def _feed_nul_pairs(pipe, pairs, state):
    """Stream ordinal/path frames without materializing the complete file list."""
    try:
        for ordinal, path in pairs:
            pipe.write(str(int(ordinal)).encode("ascii") + b"\0")
            pipe.write(path.encode("utf-8", "strict") + b"\0")
        pipe.close()
    except (BrokenPipeError, OSError, UnicodeError) as exc:
        state["error"] = str(exc)
        try:
            pipe.close()
        except OSError:
            pass


def _feed_nul_paths(pipe, paths, state):
    """Stream GNU TAR's exact NUL-delimited relative-name list."""
    try:
        for path in paths:
            pipe.write(path.encode("utf-8", "strict") + b"\0")
        pipe.close()
    except (BrokenPipeError, OSError, UnicodeError) as exc:
        state["error"] = str(exc)
        try:
            pipe.close()
        except OSError:
            pass


def _iter_nul_fields(raw, chunk_size=65536):
    pending = bytearray()
    while True:
        block = raw.read(chunk_size)
        if not block:
            break
        pending.extend(block)
        while True:
            marker = pending.find(0)
            if marker < 0:
                break
            yield bytes(pending[:marker])
            del pending[:marker + 1]
    if pending:
        raise ValueError("truncated NUL-framed source-status output")


def _source_status_command(remote_base):
    """Machine-readable, filename-safe status probe used after TAR warnings."""
    script = (
        "while IFS= read -r -d '' ordinal && IFS= read -r -d '' path; do "
        "target=\"./$path\"; "
        "if [[ ! -e \"$target\" && ! -L \"$target\" ]]; "
        "then status=source_missing; evidence=lstat_missing; "
        "elif [[ ! -f \"$target\" ]]; then status=source_changed; evidence=not_regular; "
        "elif [[ ! -r \"$target\" ]]; then status=source_permission_denied; evidence=not_readable; "
        "elif ! dd if=\"$target\" of=/dev/null bs=1 count=1 2>/dev/null; "
        "then status=source_unreadable; evidence=read_probe_failed; "
        "else status=present; evidence=read_probe_ok; fi; "
        "printf '%s\\0%s\\0%s\\0%s\\0' \"$ordinal\" \"$path\" "
        "\"$status\" \"$evidence\"; done"
    )
    return (
        f"cd {shlex.quote(str(remote_base))} && "
        f"LC_ALL=C bash -c {shlex.quote(script)}"
    )


def _remote_source_status_probe(remote_user, remote_host, remote_base, members,
                                *, password='', cipher='', fetch_cores=None,
                                abort_evt=None):
    """Return all non-present source observations using NUL-framed records."""
    command = _source_status_command(remote_base)
    ssh_cmd, ssh_env, error = _ssh_stream_command(
        remote_user, remote_host, command, password=password, cipher=cipher)
    if error:
        return (), (error,)
    output = tempfile.TemporaryFile(mode="w+b")
    proc = None
    stderr_chunks = []
    stderr_state = {}
    feed_state = {}
    try:
        proc = subprocess.Popen(
            ssh_cmd, stdin=subprocess.PIPE, stdout=output,
            stderr=subprocess.PIPE, env=ssh_env)
        register_proc(proc)
        _apply_proc_tuning(proc, affinity=fetch_cores, label='ssh-tar-status')
        stderr_thread = threading.Thread(
            target=_bounded_stderr_reader,
            args=(proc.stderr, stderr_chunks, stderr_state), daemon=True)
        stderr_thread.start()
        feeder = threading.Thread(
            target=_feed_nul_pairs, args=(proc.stdin, members, feed_state),
            daemon=True)
        feeder.start()
        while proc.poll() is None:
            if CANCEL.is_set() or (abort_evt is not None and abort_evt.is_set()):
                _kill_proc_tree(proc)
                break
            time.sleep(0.05)
        rc = proc.wait()
        feeder.join(timeout=2)
        stderr_thread.join(timeout=2)
        if rc != 0 or feed_state.get("error") or stderr_state.get("truncated"):
            detail = b''.join(stderr_chunks).decode(
                'utf-8', errors='replace').strip()
            return (), (f"source status probe failed (exit {rc}): {detail}",)

        output.seek(0)
        fields = iter(_iter_nul_fields(output))
        observed = []
        while True:
            try:
                ordinal_raw = next(fields)
            except StopIteration:
                break
            try:
                path_raw = next(fields)
                status_raw = next(fields)
                evidence_raw = next(fields)
            except StopIteration as exc:
                raise ValueError(
                    "incomplete NUL-framed source-status record") from exc
            ordinal = int(ordinal_raw.decode("ascii", "strict"))
            path = path_raw.decode("utf-8", "strict")
            status = status_raw.decode("ascii", "strict")
            evidence = evidence_raw.decode("ascii", "strict")
            if status == "present":
                continue
            try:
                disposition = SourceDisposition(status)
            except ValueError:
                disposition = SourceDisposition.UNRESOLVED
            observed.append(StoredTarSourceDiagnostic(
                plan_ordinal=ordinal, path=path,
                disposition=disposition, evidence=evidence))
        return tuple(observed), ()
    except (OSError, ValueError, UnicodeError) as exc:
        if proc is not None:
            _kill_proc_tree(proc)
        return (), (f"source status probe failed: {exc}",)
    finally:
        unregister_proc(proc)
        output.close()


def _remote_tar_store(remote_user, remote_host, remote_base, rel_paths,
                      local_part_path, *, plan_ordinals=None, password='',
                      cipher='', use_mbuffer=False, mbuffer_size='2G',
                      fetch_cores=None, abort_evt=None):
    """Stream one GNU Stored TAR directly into a unique local ``.tar.part``.

    The function performs one attempt.  Its caller owns bounded retry policy;
    every retry must supply a new/truncated part and starts at byte zero.
    """
    paths = []
    try:
        for path in rel_paths:
            paths.append(validate_remote_posix_relpath(path))
    except ValueError as exc:
        return RemoteTarStoreResult(
            False, str(local_part_path), 0, None, "", error=str(exc))
    ordinals = (list(range(len(paths))) if plan_ordinals is None
                else [int(value) for value in plan_ordinals])
    if len(ordinals) != len(paths) or len(set(ordinals)) != len(ordinals):
        return RemoteTarStoreResult(
            False, str(local_part_path), 0, None, "",
            error="plan ordinals must be unique and match the path list")
    pairs = tuple(zip(ordinals, paths))
    part_path = os.path.abspath(str(local_part_path))
    if not part_path.endswith('.part'):
        return RemoteTarStoreResult(
            False, part_path, 0, None, "",
            error="direct TAR output must use a unique .part name")
    if CANCEL.is_set():
        return RemoteTarStoreResult(
            False, part_path, 0, None, "", cancelled=True,
            error="cancelled")
    os.makedirs(os.path.dirname(part_path), exist_ok=True)

    tar_core = _stored_tar_command(remote_base)
    transport_mode = "plain_tar"
    if use_mbuffer and _mbuffer_pipefail_capable(
            remote_user, remote_host, password=password):
        pipeline = f"{tar_core} | mbuffer -q -m {shlex.quote(mbuffer_size)}"
        remote_cmd = f"bash -o pipefail -c {shlex.quote(pipeline)}"
        transport_mode = "tar_mbuffer_pipefail"
    else:
        remote_cmd = tar_core
    ssh_cmd, ssh_env, error = _ssh_stream_command(
        remote_user, remote_host, remote_cmd, password=password, cipher=cipher)
    if error:
        return RemoteTarStoreResult(
            False, part_path, 0, None, "", transport_mode=transport_mode,
            error=error)

    proc = None
    stderr_chunks = []
    stderr_state = {}
    feed_state = {}
    archive_size = 0
    try:
        # Exclusive creation means two build owners can never share a byte
        # stream.  Popen writes stdout to the file descriptor directly, so TAR
        # payload size cannot increase Python memory use.
        with open(part_path, "xb") as output:
            proc = subprocess.Popen(
                ssh_cmd, stdin=subprocess.PIPE, stdout=output,
                stderr=subprocess.PIPE, env=ssh_env)
            register_proc(proc)
            _apply_proc_tuning(proc, affinity=fetch_cores, label='ssh-tar-store')
            stderr_thread = threading.Thread(
                target=_bounded_stderr_reader,
                args=(proc.stderr, stderr_chunks, stderr_state), daemon=True)
            stderr_thread.start()
            feeder = threading.Thread(
                target=_feed_nul_paths,
                args=(proc.stdin, paths, feed_state), daemon=True)
            feeder.start()
            while proc.poll() is None:
                if CANCEL.is_set() or (
                        abort_evt is not None and abort_evt.is_set()):
                    _kill_proc_tree(proc)
                    break
                time.sleep(0.05)
            rc = proc.wait()
            feeder.join(timeout=2)
            stderr_thread.join(timeout=2)
            output.flush()
            os.fsync(output.fileno())
            archive_size = output.tell()
        stderr_text = b''.join(stderr_chunks).decode(
            'utf-8', errors='replace').strip()
    except (FileExistsError, OSError) as exc:
        if proc is not None:
            _kill_proc_tree(proc)
        return RemoteTarStoreResult(
            False, part_path, archive_size, None, "",
            transport_mode=transport_mode, error=str(exc))
    finally:
        unregister_proc(proc)

    cancelled = CANCEL.is_set() or (
        abort_evt is not None and abort_evt.is_set())
    unresolved = []
    diagnostics = ()
    if stderr_state.get("truncated"):
        unresolved.append("remote TAR stderr exceeded the bounded capture limit")
    if stderr_state.get("read_error"):
        unresolved.append("remote TAR stderr could not be read completely")
    if feed_state.get("error") and not cancelled:
        unresolved.append(f"remote TAR input failed: {feed_state['error']}")

    diagnostic_groups = []
    for line in stderr_text.splitlines():
        if line.startswith('tar: '):
            diagnostic_groups.append([line])
        elif diagnostic_groups:
            diagnostic_groups[-1].append(line)
    diagnostic_texts = ['\n'.join(group) for group in diagnostic_groups]
    known_exception_warning = any(
        marker in stderr_text for marker in _SOURCE_EXCEPTION_SUFFIXES)
    changed_warning = (
        "file changed as we read it" in stderr_text
        or "File shrank by" in stderr_text
        or "file is on a different filesystem" in stderr_text)
    known_markers = _SOURCE_EXCEPTION_SUFFIXES + (
        "file changed as we read it", "File shrank by",
        "file is on a different filesystem")
    unknown_tar_warning = any(
        not any(marker in group for marker in known_markers)
        for group in diagnostic_texts)
    if changed_warning:
        unresolved.append("remote TAR observed source change while reading")
    if unknown_tar_warning:
        unresolved.append("unclassified remote TAR diagnostic")

    if rc == 0 and known_exception_warning and not unresolved:
        diagnostics, probe_unresolved = _remote_source_status_probe(
            remote_user, remote_host, remote_base, pairs,
            password=password, cipher=cipher, fetch_cores=fetch_cores,
            abort_evt=abort_evt)
        unresolved.extend(probe_unresolved)
        explicit = [item for item in diagnostics if item.disposition in {
            SourceDisposition.SOURCE_MISSING,
            SourceDisposition.SOURCE_PERMISSION_DENIED,
            SourceDisposition.SOURCE_UNREADABLE,
        }]
        non_exception = [item for item in diagnostics
                         if item.disposition not in {
                             SourceDisposition.SOURCE_MISSING,
                             SourceDisposition.SOURCE_PERMISSION_DENIED,
                             SourceDisposition.SOURCE_UNREADABLE}]
        if non_exception:
            unresolved.append(
                "source status probe observed changed or unresolved members")
        if not explicit:
            unresolved.append(
                "TAR reported an absent/unreadable input but the machine "
                "status probe could not attribute it")
        diagnostics = tuple(explicit)

    if cancelled:
        return RemoteTarStoreResult(
            False, part_path, archive_size, rc, stderr_text,
            diagnostics=diagnostics, unresolved=tuple(unresolved),
            transport_mode=transport_mode, cancelled=True, error="cancelled")
    if rc != 0:
        return RemoteTarStoreResult(
            False, part_path, archive_size, rc, stderr_text,
            diagnostics=diagnostics, unresolved=tuple(unresolved),
            transport_mode=transport_mode,
            error=f"remote TAR/SSH exited with status {rc}")
    if unresolved:
        return RemoteTarStoreResult(
            False, part_path, archive_size, rc, stderr_text,
            diagnostics=diagnostics, unresolved=tuple(unresolved),
            transport_mode=transport_mode,
            error="remote TAR outcome is unresolved")
    return RemoteTarStoreResult(
        True, part_path, archive_size, rc, stderr_text,
        diagnostics=diagnostics, transport_mode=transport_mode)


_ASKPASS_HELPERS = set()
_ASKPASS_HELPER_PATH = None
_ASKPASS_LOCK = threading.Lock()


@atexit.register
def _cleanup_askpass_helpers():
    """Remove any SSH askpass helper scripts created during this run."""
    global _ASKPASS_HELPER_PATH
    for helper_path in list(_ASKPASS_HELPERS):
        try:
            os.remove(helper_path)
        except OSError:
            pass
        _ASKPASS_HELPERS.discard(helper_path)
    _ASKPASS_HELPER_PATH = None


def _get_askpass_helper():
    """Return a shared askpass helper script, creating it once per run.

    The helper only reads ``$env:LTO_REMOTE_PASSWORD`` at call time, so a single
    script serves every SSH invocation regardless of the password value. Reusing
    it avoids leaking one temp file per remote fetch during a long archive run.
    """
    global _ASKPASS_HELPER_PATH
    with _ASKPASS_LOCK:
        if _ASKPASS_HELPER_PATH and os.path.exists(_ASKPASS_HELPER_PATH):
            return _ASKPASS_HELPER_PATH
        helper_body = (
            "@echo off\r\n"
            "powershell -NoProfile -ExecutionPolicy Bypass "
            "-Command \"[Console]::Out.Write($env:LTO_REMOTE_PASSWORD)\"\r\n"
        )
        try:
            with tempfile.NamedTemporaryFile(
                    'w', encoding='utf-8', newline='',
                    prefix='lto_ssh_askpass_', suffix='.cmd',
                    delete=False) as f:
                helper_path = f.name
                f.write(helper_body)
        except OSError as e:
            raise RuntimeError(f"Could not create SSH askpass helper: {e}") from e
        _ASKPASS_HELPERS.add(helper_path)
        _ASKPASS_HELPER_PATH = helper_path
        return helper_path


def _openssh_askpass_env(password):
    """Build an environment that lets OpenSSH read a configured password."""
    env = os.environ.copy()
    env['LTO_REMOTE_PASSWORD'] = password
    env['SSH_ASKPASS'] = _get_askpass_helper()
    env['SSH_ASKPASS_REQUIRE'] = 'force'
    env['DISPLAY'] = env.get('DISPLAY') or 'lto-archive-manager'
    return env


def _ssh_run(remote_user, remote_host, command, capture=True, password='',
             timeout=None):
    # type: (...) -> "subprocess.CompletedProcess[str]"
    """Run a command on the remote host.

    Blank password uses normal OpenSSH key auth. A configured password uses
    sshpass when available, or OpenSSH askpass. PuTTY's ``-pw`` fallback is
    intentionally not supported because it exposes the password in process
    arguments.
    """
    password = password or ''
    if password:
        if _has_command('sshpass'):
            ssh_cmd = [
                'sshpass', '-e',
                'ssh',
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
                f'{remote_user}@{remote_host}',
                command,
            ]
            env = os.environ.copy()
            env['SSHPASS'] = password
            if capture:
                try:
                    return subprocess.run(
                        ssh_cmd,
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        env=env,
                        timeout=timeout,
                    )
                except subprocess.TimeoutExpired as e:
                    return subprocess.CompletedProcess(
                        ssh_cmd, 124, (e.stdout if isinstance(e.stdout, str) else ''),
                        f"SSH command timed out after {timeout}s")
            try:
                return subprocess.run(ssh_cmd, env=env, timeout=timeout, text=True)
            except subprocess.TimeoutExpired:
                return subprocess.CompletedProcess(
                    ssh_cmd, 124, '', f"SSH command timed out after {timeout}s")
        if _has_command('ssh'):
            ssh_cmd = [
                'ssh',
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'NumberOfPasswordPrompts=1',
                '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
                f'{remote_user}@{remote_host}',
                command,
            ]
            env = _openssh_askpass_env(password)
            if capture:
                try:
                    return subprocess.run(
                        ssh_cmd,
                        stdin=subprocess.DEVNULL,
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        env=env,
                        timeout=timeout,
                    )
                except subprocess.TimeoutExpired as e:
                    return subprocess.CompletedProcess(
                        ssh_cmd, 124, (e.stdout if isinstance(e.stdout, str) else ''),
                        f"SSH command timed out after {timeout}s")
            try:
                return subprocess.run(
                    ssh_cmd, stdin=subprocess.DEVNULL, env=env, timeout=timeout,
                    text=True)
            except subprocess.TimeoutExpired:
                return subprocess.CompletedProcess(
                    ssh_cmd, 124, '', f"SSH command timed out after {timeout}s")
        return subprocess.CompletedProcess(
            args=['ssh'],
            returncode=255,
            stdout='',
            stderr=(
                "remote_password is set, but no password-capable SSH helper was found. "
                "Install OpenSSH or sshpass; or configure SSH key auth. "
                "PuTTY -pw/pscp password fallbacks are disabled because they expose "
                "passwords in process arguments."
            ),
        )

    ssh_cmd = [
        'ssh',
        '-o', 'BatchMode=yes',
        '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
        f'{remote_user}@{remote_host}',
        command,
    ]
    if capture:
        try:
            return subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            return subprocess.CompletedProcess(
                ssh_cmd, 124, (e.stdout if isinstance(e.stdout, str) else ''),
                f"SSH command timed out after {timeout}s")
    try:
        return subprocess.run(ssh_cmd, timeout=timeout, text=True)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            ssh_cmd, 124, '', f"SSH command timed out after {timeout}s")


def _scp_fetch_file(remote_user, remote_host, remote_file_path, local_dest_path, password=''):
    """Copy a single file from remote_user@remote_host:remote_file_path to
    local_dest_path using SCP.  stdout/stderr are NOT redirected so SCP's
    native progress output is visible in the terminal.
    Returns SCP's exit code (0 = success).
    """
    os.makedirs(os.path.dirname(os.path.abspath(local_dest_path)), exist_ok=True)
    remote_spec = f'{remote_user}@{remote_host}:{remote_file_path}'

    password = password or ''
    if password:
        if _has_command('sshpass'):
            env = os.environ.copy()
            env['SSHPASS'] = password
            proc = subprocess.Popen(
                [
                    'sshpass', '-e', 'scp',
                    '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
                    '-p', remote_spec, local_dest_path,
                ],
                env=env
            )
            return proc.wait()
        if _has_command('scp'):
            env = _openssh_askpass_env(password)
            proc = subprocess.Popen(
                [
                    'scp',
                    '-o', 'BatchMode=no',
                    '-o', 'PubkeyAuthentication=no',
                    '-o', 'NumberOfPasswordPrompts=1',
                    '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
                    '-p',
                    remote_spec,
                    local_dest_path,
                ],
                stdin=subprocess.DEVNULL,
                env=env,
            )
            return proc.wait()
        print("[REMOTE] remote_password is set, but scp/OpenSSH or sshpass was not found.")
        return 255

    proc = subprocess.Popen(
        [
            'scp',
            '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
            '-p', remote_spec, local_dest_path,
        ]
    )
    return proc.wait()


def _ssh_stream_command(remote_user, remote_host, command, password='', cipher=''):
    """Return a command/env pair for an SSH process that streams stdin/stdout.

    cipher: optional OpenSSH cipher name (e.g. aes128-gcm@openssh.com). When set,
            it is requested with SSH-level compression disabled — a fast AES-NI
            cipher keeps the fetch stream from being CPU-bound on incompressible
            media."""
    password = password or ''
    # OpenSSH cipher/compression tuning, inserted right after the 'ssh' binary.
    cipher_opts = (['-c', cipher, '-o', 'Compression=no'] if cipher else [])
    if password:
        if _has_command('sshpass'):
            env = os.environ.copy()
            env['SSHPASS'] = password
            return [
                'sshpass', '-e',
                'ssh', *cipher_opts, *_SSH_STREAM_KEEPALIVE_OPTS,
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
                f'{remote_user}@{remote_host}',
                command,
            ], env, None
        if _has_command('ssh'):
            return [
                'ssh', *cipher_opts, *_SSH_STREAM_KEEPALIVE_OPTS,
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'NumberOfPasswordPrompts=1',
                '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
                f'{remote_user}@{remote_host}',
                command,
            ], _openssh_askpass_env(password), None
        return None, None, (
            "remote_password is set, but no password-capable SSH helper was found. "
            "Install OpenSSH or sshpass; or configure SSH key auth. "
            "PuTTY -pw fallbacks are disabled because they expose passwords "
            "in process arguments."
        )

    if not _has_command('ssh'):
        return None, None, "ssh was not found on PATH."
    return [
        'ssh', *cipher_opts, *_SSH_STREAM_KEEPALIVE_OPTS,
        '-o', 'BatchMode=yes',
        '-o', f'StrictHostKeyChecking={SSH_HOSTKEY_POLICY}',
        f'{remote_user}@{remote_host}',
        command,
    ], None, None


def _is_recoverable_remote_tar_warning(line):
    """Return True for tar warnings the caller can verify per file.

    The fetch caller checks every expected local output after tar exits. These
    warnings identify individual source files that tar could not read, so the
    caller can mark only those absent files as source_missing. Other tar
    warnings stay fatal because they may mean the stream itself is incomplete or
    corrupt.
    """
    if not line.startswith('tar: '):
        return False
    return line.endswith(
        'Warning: Cannot stat: No such file or directory'
    ) or line.endswith(
        'Warning: Cannot open: Permission denied'
    )


def _remote_tar_fetch(remote_user, remote_host, remote_base, rel_paths, local_dest_dir,
                      password='', cipher='', use_mbuffer=False, mbuffer_size='2G',
                      fetch_cores=None, abort_evt=None):
    """Fetch many remote files in one tar stream over SSH.

    rel_paths must be relative to remote_base and use POSIX separators.
    Returns (ok, error_message).

    Performance knobs:
      cipher       — fast OpenSSH cipher for the stream (AES-NI, low CPU).
      use_mbuffer  — wrap the remote tar in mbuffer (if installed remotely) so a
                     large RAM ring smooths the tar->ssh handoff against jitter.
      fetch_cores  — pin the ssh/tar children to these cores, isolating SSH
                     decryption from the tape-writer's cores.
      abort_evt    — optional event; when set (e.g. by the staging watchdog),
                     the ssh and tar process trees are killed so the caller is
                     never left blocked on a wedged stream.
    """
    if not rel_paths:
        return True, ''
    if not _has_command('tar'):
        return False, "local tar executable was not found on PATH"
    if CANCEL.is_set():
        return False, "cancelled"

    os.makedirs(local_dest_dir, exist_ok=True)
    safe_paths = []
    try:
        for rel in rel_paths:
            safe_paths.append(_safe_remote_relpath(rel))
    except ValueError as e:
        return False, str(e)

    # -b 512 -> 256 KiB records: fewer syscalls than tar's tiny default block.
    # --sparse keeps hole regions out of the stream so a sparse remote file
    # is not shipped (and locally written) at its fully-expanded size.
    # --no-recursion: every -T entry is a scanned regular file; without it a
    # corrupt entry that names a directory makes tar mirror that entire tree
    # (a one-line truncated scan record once ballooned a 100 GB chunk to
    # ~900 GB of unplanned data this way).
    # Missing/unreadable inputs are reported on stderr but do not abort the
    # stream; the caller verifies every expected local file and records exact
    # omissions.
    # LC_ALL=C pins tar's diagnostics to English: the recoverable-warning
    # filter below matches literal English diagnostics, and a localized remote
    # would otherwise turn every legitimately-missing/unreadable file into a
    # fatal fetch error.
    tar_core = (
        f"LC_ALL=C tar -C {shlex.quote(remote_base)} -b 512 --sparse "
        "--no-recursion --ignore-failed-read -cf - --null -T -"
    )
    if use_mbuffer:
        # Use mbuffer only if it exists on the remote; otherwise fall back to a
        # plain tar so a missing binary never fails the fetch. stdin (the NUL
        # file list) flows to whichever tar runs.
        remote_cmd = (
            f"if command -v mbuffer >/dev/null 2>&1; then "
            f"{tar_core} | mbuffer -q -m {shlex.quote(mbuffer_size)}; "
            f"else {tar_core}; fi"
        )
    else:
        remote_cmd = tar_core

    ssh_cmd, ssh_env, err = _ssh_stream_command(
        remote_user, remote_host, remote_cmd, password=password, cipher=cipher
    )
    if err:
        return False, err
    assert ssh_cmd is not None

    # Local extract: read the stream as-is. A tar stream is a 512-byte-record
    # byte stream over the pipe, so the remote -b 512 does not need to be matched
    # here — and this stays compatible with Windows' bsdtar (no GNU -b/-B flags).
    tar_cmd = ['tar', '-C', local_dest_dir, '-xf', '-']
    ssh_proc = None
    tar_proc = None
    ssh_stderr = []

    def _drain_stderr(pipe):
        try:
            while True:
                chunk = pipe.read(65536)
                if not chunk:
                    break
                ssh_stderr.append(chunk)
        except OSError:
            pass

    try:
        ssh_proc = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=ssh_env,
        )
        register_proc(ssh_proc)
        _apply_proc_tuning(ssh_proc, affinity=fetch_cores, label='ssh-fetch')
        stderr_thread = threading.Thread(
            target=_drain_stderr, args=(ssh_proc.stderr,), daemon=True
        )
        stderr_thread.start()

        ssh_stdout = ssh_proc.stdout
        assert ssh_stdout is not None
        tar_proc = subprocess.Popen(
            tar_cmd,
            stdin=ssh_stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        register_proc(tar_proc)
        _apply_proc_tuning(tar_proc, affinity=fetch_cores, label='tar-extract')
        ssh_stdout.close()

        if abort_evt is not None:
            # Watchdog: an abort request must be able to unwedge the blocking
            # communicate() below even when the stream itself has stalled
            # (e.g. staging disk exhausted). Exits on its own once both
            # processes finish normally.
            def _abort_watch():
                while not abort_evt.wait(1):
                    if (tar_proc.poll() is not None
                            and ssh_proc.poll() is not None):
                        return
                _kill_proc_tree(tar_proc)
                _kill_proc_tree(ssh_proc)
            threading.Thread(target=_abort_watch, name='fetch-abort-watch',
                             daemon=True).start()

        file_list = ''.join(f'{rel}\0' for rel in safe_paths).encode('utf-8')
        ssh_stdin = ssh_proc.stdin
        assert ssh_stdin is not None
        try:
            ssh_stdin.write(file_list)
            ssh_stdin.close()
        except OSError:
            pass

        _tar_stdout, tar_stderr = tar_proc.communicate()
        ssh_rc = ssh_proc.wait()
        stderr_thread.join(timeout=2)
        tar_rc = tar_proc.returncode
    except OSError as e:
        for proc in (tar_proc, ssh_proc):
            if proc and proc.poll() is None:
                proc.kill()
        return False, str(e)
    finally:
        unregister_proc(ssh_proc)
        unregister_proc(tar_proc)

    if CANCEL.is_set():
        return False, "cancelled"
    if abort_evt is not None and abort_evt.is_set():
        return False, ("fetch aborted by the staging watchdog "
                       "(chunk overran its plan or staging disk space ran low)")

    ssh_err_text = b''.join(ssh_stderr).decode('utf-8', errors='replace').strip()
    tar_err_text = (tar_stderr or b'').decode('utf-8', errors='replace').strip()
    if ssh_rc != 0 or tar_rc != 0:
        parts = []
        if ssh_rc != 0:
            parts.append(f"remote tar/ssh exit {ssh_rc}: {ssh_err_text}")
        if tar_rc != 0:
            parts.append(f"local tar exit {tar_rc}: {tar_err_text}")
        return False, '\n'.join(parts)

    # --ignore-failed-read also suppresses nonzero exits for some failed inputs.
    # Only per-file missing/unreadable warnings are recoverable; the caller then
    # verifies every expected file and records exact omissions. Other GNU tar
    # warnings remain fatal. Non-tar SSH diagnostics retain their previous
    # behavior and are ignored when the SSH process itself succeeded.
    tar_diagnostics = [
        line for line in ssh_err_text.splitlines()
        if line.startswith('tar: ')
    ]
    fatal_warnings = [
        line for line in tar_diagnostics
        if not _is_recoverable_remote_tar_warning(line)
    ]
    if fatal_warnings:
        return False, "remote tar warning:\n" + '\n'.join(fatal_warnings)
    return True, ''
