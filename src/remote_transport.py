"""SSH/SCP/tar transport over OpenSSH."""
import os
import shutil
import threading
import subprocess
import tempfile
import shlex
import atexit

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .paths import _safe_remote_relpath
from .runtime import CANCEL, _apply_proc_tuning, register_proc, unregister_proc


def _has_command(name):
    return shutil.which(name) is not None


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
                '-o', 'StrictHostKeyChecking=accept-new',
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
                        ssh_cmd, 124, e.stdout or '',
                        f"SSH command timed out after {timeout}s")
            try:
                return subprocess.run(ssh_cmd, env=env, timeout=timeout)
            except subprocess.TimeoutExpired:
                return subprocess.CompletedProcess(
                    ssh_cmd, 124, '', f"SSH command timed out after {timeout}s")
        if _has_command('ssh'):
            ssh_cmd = [
                'ssh',
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'NumberOfPasswordPrompts=1',
                '-o', 'StrictHostKeyChecking=accept-new',
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
                        ssh_cmd, 124, e.stdout or '',
                        f"SSH command timed out after {timeout}s")
            try:
                return subprocess.run(
                    ssh_cmd, stdin=subprocess.DEVNULL, env=env, timeout=timeout)
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
        '-o', 'StrictHostKeyChecking=accept-new',
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
                ssh_cmd, 124, e.stdout or '',
                f"SSH command timed out after {timeout}s")
    try:
        return subprocess.run(ssh_cmd, timeout=timeout)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            ssh_cmd, 124, '', f"SSH command timed out after {timeout}s")


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
                'ssh', *cipher_opts,
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'StrictHostKeyChecking=accept-new',
                f'{remote_user}@{remote_host}',
                command,
            ], env, None
        if _has_command('ssh'):
            return [
                'ssh', *cipher_opts,
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'NumberOfPasswordPrompts=1',
                '-o', 'StrictHostKeyChecking=accept-new',
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
        'ssh', *cipher_opts,
        '-o', 'BatchMode=yes',
        '-o', 'StrictHostKeyChecking=accept-new',
        f'{remote_user}@{remote_host}',
        command,
    ], None, None


def _remote_tar_fetch(remote_user, remote_host, remote_base, rel_paths, local_dest_dir,
                      password='', cipher='', use_mbuffer=False, mbuffer_size='2G',
                      fetch_cores=None):
    """Fetch many remote files in one tar stream over SSH.

    rel_paths must be relative to remote_base and use POSIX separators.
    Returns (ok, error_message).

    Performance knobs:
      cipher       — fast OpenSSH cipher for the stream (AES-NI, low CPU).
      use_mbuffer  — wrap the remote tar in mbuffer (if installed remotely) so a
                     large RAM ring smooths the tar->ssh handoff against jitter.
      fetch_cores  — pin the ssh/tar children to these cores, isolating SSH
                     decryption from the tape-writer's cores.
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
    # Missing inputs are reported on stderr but do not abort the stream; the
    # caller verifies every expected local file and records exact omissions.
    tar_core = (
        f"tar -C {shlex.quote(remote_base)} -b 512 "
        "--ignore-failed-read -cf - --null -T -"
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

        tar_proc = subprocess.Popen(
            tar_cmd,
            stdin=ssh_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        register_proc(tar_proc)
        _apply_proc_tuning(tar_proc, affinity=fetch_cores, label='tar-extract')
        ssh_proc.stdout.close()

        file_list = ''.join(f'{rel}\0' for rel in safe_paths).encode('utf-8')
        try:
            ssh_proc.stdin.write(file_list)
            ssh_proc.stdin.close()
        except OSError:
            pass

        tar_stdout, tar_stderr = tar_proc.communicate()
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

    ssh_err_text = b''.join(ssh_stderr).decode('utf-8', errors='replace').strip()
    tar_err_text = (tar_stderr or b'').decode('utf-8', errors='replace').strip()
    if ssh_rc != 0 or tar_rc != 0:
        parts = []
        if ssh_rc != 0:
            parts.append(f"remote tar/ssh exit {ssh_rc}: {ssh_err_text}")
        if tar_rc != 0:
            parts.append(f"local tar exit {tar_rc}: {tar_err_text}")
        return False, '\n'.join(parts)

    # --ignore-failed-read also suppresses nonzero exits for permission/read
    # warnings. Only a genuinely missing input is recoverable; all other GNU
    # tar warnings remain fatal. Non-tar SSH diagnostics retain their previous
    # behavior and are ignored when the SSH process itself succeeded.
    tar_diagnostics = [
        line for line in ssh_err_text.splitlines()
        if line.startswith('tar: ')
    ]
    fatal_warnings = [
        line for line in tar_diagnostics
        if not line.endswith('Warning: Cannot stat: No such file or directory')
    ]
    if fatal_warnings:
        return False, "remote tar warning:\n" + '\n'.join(fatal_warnings)
    return True, ''
