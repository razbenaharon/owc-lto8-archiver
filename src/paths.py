"""Path cleaning, sanitization, hashing and long-path helpers."""
import os
import re
import sys
import time
import queue
import signal
import shutil
import hashlib
import zipfile
import sqlite3
import threading
import configparser
import subprocess
import tempfile
import shlex
import posixpath
import atexit
from datetime import datetime
from collections import defaultdict

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .constants import BUFFER_SIZE


def _safe_log_token(value, default='item'):
    text = str(value or '').strip()
    if text:
        text = os.path.basename(os.path.normpath(text))
    text = re.sub(r'[^A-Za-z0-9._-]+', '_', text or default)
    text = text.strip('._-')
    return (text or default)[:80]


def _unique_path(path):
    if not os.path.exists(path):
        return path
    root, ext = os.path.splitext(path)
    for idx in range(2, 1000):
        candidate = f"{root}_{idx}{ext}"
        if not os.path.exists(candidate):
            return candidate
    return f"{root}_{int(time.time() * 1000)}{ext}"


def _clean_config_path(value):
    """Return a filesystem path from config text, tolerating optional quotes."""
    value = (value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    return os.path.normpath(os.path.expandvars(os.path.expanduser(value)))


def _clean_remote_path(value):
    """Return a POSIX remote path from config text, tolerating optional quotes."""
    value = (value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    value = value.replace('\\', '/').strip()
    return posixpath.normpath(value) if value else ''


def _config_list(value):
    """Parse a newline/comma/semicolon config list without splitting spaces."""
    value = (value or '').replace('\r', '\n')
    items = []
    for line in value.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r'[;,]', line) if not line.startswith(('"', "'")) else [line]
        for part in parts:
            part = part.strip()
            if part:
                items.append(part)
    return items


def _hash_file(path):
    """Compute SHA-256 hash of a file, reading in chunks."""
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            buf = f.read(BUFFER_SIZE)
            if not buf:
                break
            hasher.update(buf)
    return hasher.hexdigest()


def _verify_restored_hash(local_path, record):
    """Verify a restored file against the stored DB hash.
    Runs entirely on local disk after tape transfer is complete — no impact on tape speed."""
    try:
        stored_hash = record['file_hash']
    except (KeyError, IndexError):
        stored_hash = None
    if not stored_hash:
        print(f"[VERIFY] No stored hash for {record['file_name']} — skipping.")
        return
    actual_hash = _hash_file(local_path)
    if actual_hash == stored_hash:
        print(f"[VERIFY] OK  {record['file_name']}")
    else:
        print(f"[VERIFY] FAIL  {record['file_name']}")
        print(f"         expected: {stored_hash}")
        print(f"         got:      {actual_hash}")


def _dir_tree_size(path):
    """Total size in bytes of every file under path (0 if missing/unreadable)."""
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    except OSError:
        pass
    return total


def _safe_remote_relpath(path):
    """Return a tar-safe remote relative path using forward slashes."""
    rel = (path or '').replace('\\', '/')
    if rel.startswith('/') or re.match(r'^[A-Za-z]:/', rel):
        raise ValueError(f"unsafe relative path: {path}")
    rel = rel.strip('/')
    raw_parts = [part for part in rel.split('/') if part]
    if any(part in ('.', '..') for part in raw_parts):
        raise ValueError(f"unsafe relative path: {path}")
    normalized = posixpath.normpath(rel)
    if normalized in ('', '.'):
        raise ValueError("empty relative path")
    parts = normalized.split('/')
    if normalized.startswith('/') or any(part in ('', '.', '..') for part in parts):
        raise ValueError(f"unsafe relative path: {path}")
    return normalized


def _remote_fetch_base_and_rel(configured_remote_path, remote_fpath):
    """Map a scanned remote path to a tar -C base and safe relative path.

    configured_remote_path may be either a directory or a single file.
    """
    remote_root = (configured_remote_path or '').replace('\\', '/').strip()
    scanned_path = (remote_fpath or '').replace('\\', '/').strip()
    if not remote_root:
        raise ValueError("empty configured remote path")
    if not scanned_path:
        raise ValueError("empty scanned remote path")

    remote_root = posixpath.normpath(remote_root)
    scanned_path = posixpath.normpath(scanned_path)

    if scanned_path == remote_root:
        base = posixpath.dirname(remote_root) or '.'
        rel = posixpath.basename(remote_root)
    elif remote_root == '/':
        base = '/'
        rel = scanned_path.lstrip('/')
    elif scanned_path.startswith(remote_root.rstrip('/') + '/'):
        base = remote_root
        rel = scanned_path[len(remote_root.rstrip('/') + '/'):]
    else:
        raise ValueError(f"remote path outside base: {remote_fpath}")

    return base, _safe_remote_relpath(rel)


_WIN_RESERVED_TABLE = {ord(c): '_' for c in '<>:"|?*'}


_WIN_RESERVED_TABLE.update({i: '_' for i in range(32)})  # ASCII control chars


def _winsafe_extracted_rel(rel):
    """Map a POSIX remote rel-path to the on-disk path bsdtar produces on
    Windows. No-op for names that are already NTFS-legal, so chunks without
    reserved characters are byte-for-byte unaffected. On non-Windows hosts the
    extractor keeps names verbatim, so this is a pass-through there."""
    if os.name != 'nt':
        return rel
    out = []
    for part in rel.split('/'):
        if not part:
            continue
        part = part.translate(_WIN_RESERVED_TABLE).rstrip(' .')
        out.append(part or '_')
    return '/'.join(out)


_LEGACY_PATH_LIMIT = 260


_WIN_RESERVED_NAMES = {
    'CON', 'PRN', 'AUX', 'NUL',
    *(f'COM{i}' for i in range(1, 10)),
    *(f'LPT{i}' for i in range(1, 10)),
}


def _long(path):
    r"""Return a Windows extended-length (\\?\) form of an absolute path so
    Python filesystem calls can exceed the legacy MAX_PATH limit. No-op on
    non-Windows, on already-prefixed paths, and when path is empty."""
    if os.name != 'nt' or not path:
        return path
    if path.startswith('\\\\?\\') or path.startswith('\\??\\'):
        return path
    abspath = os.path.abspath(path)
    if abspath.startswith('\\\\'):          # UNC: \\server\share -> \\?\UNC\...
        return '\\\\?\\UNC\\' + abspath[2:]
    return '\\\\?\\' + abspath


def _reserved_name_component(rel):
    """Return the first path component that is a reserved DOS device name
    (e.g. 'NUL', 'con.txt'), or None. Windows-only; pass-through elsewhere."""
    if os.name != 'nt':
        return None
    for part in (rel or '').replace('\\', '/').split('/'):
        if not part:
            continue
        stem = part.split('.', 1)[0].rstrip(' ').upper()
        if stem in _WIN_RESERVED_NAMES:
            return part
    return None


def _exceeds_legacy_path_limit(path):
    """True when path's absolute form reaches the legacy MAX_PATH limit on
    Windows (i.e. a non-long-path-aware extractor cannot write it)."""
    if os.name != 'nt' or not path:
        return False
    return len(os.path.abspath(path)) >= _LEGACY_PATH_LIMIT


def _disambiguate_local_rel(local_rel, claimed):
    """Return a variant of local_rel whose case-folded form is not in `claimed`.

    Two distinct remote names can sanitize to the same on-disk path (e.g.
    "a:b" and "a?b" both become "a_b"); NTFS is also case-insensitive. When
    that happens the second file would overwrite the first, so we insert a
    "~N" tag before the extension of the final component until the full path
    is unique. `claimed` holds the case-folded paths already taken."""
    head, _, tail = local_rel.rpartition('/')
    dot = tail.rfind('.')
    stem, ext = (tail[:dot], tail[dot:]) if dot > 0 else (tail, '')
    n = 1
    while True:
        cand_tail = f"{stem}~{n}{ext}"
        cand = f"{head}/{cand_tail}" if head else cand_tail
        if cand.casefold() not in claimed:
            return cand
        n += 1
