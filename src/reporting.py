"""Backup-log parsing and SUMMARY.md generation."""
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

from .constants import BACKUP_LOG_DIR
from .paths import _safe_log_token, _unique_path


def _write_source_missing_only_log(log_dir, session_id, chunk_index,
                                   tape_label, missing_files):
    """Write an audit log for a chunk with no remaining source files."""
    try:
        log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        tape_token = _safe_log_token(tape_label, 'tape')
        name = (f"{timestamp}_{tape_token}_pack_{chunk_index:03d}"
                "_source_missing.log")
        log_path = _unique_path(os.path.join(log_dir, name))

        def cell(value):
            text = '' if value is None else str(value)
            return text.replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')

        with open(log_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write("Remote Source-Missing Log\n")
            f.write("=" * 60 + "\n")
            f.write(f"Session id                  : {session_id}\n")
            f.write(f"Chunk                       : {chunk_index + 1}\n")
            f.write(f"Tape label                  : {tape_label}\n")
            f.write("Status                      : completed_without_tape_write\n")
            f.write(f"Source-missing files skipped: {len(missing_files)}\n")
            f.write("\nSource-Missing Files Skipped\n")
            f.write("-" * 60 + "\n")
            f.write("manifest_id\tremote_path\tsize\n")
            for item in missing_files:
                f.write('\t'.join(cell(value) for value in (
                    item.get('manifest_id'),
                    item.get('remote_path'),
                    item.get('file_size_bytes'),
                )) + "\n")
        return log_path
    except Exception as e:
        print(f"[LOG] Warning: could not write source-missing log: {e}")
        return None


def _parse_backup_log(path):
    """Parse a per-pack backup .log into a flat {key: value} dict.

    Reads only the header + Summary + Database Records sections and stops at the
    File Manifest table (large, and every row contains colons). Returns None if
    the file is not a recognizable backup log. Keys use the labels written by
    LTOBackup._write_backup_log (e.g. 'Total time', 'Data copied')."""
    fields = {}
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if line.startswith('File Manifest'):
                    break
                key, sep, val = line.partition(':')
                if not sep:
                    continue
                key = key.strip()
                if key and key not in fields:
                    fields[key] = val.strip()
    except OSError:
        return None
    return fields if 'Total time' in fields else None


def _summarize_log(name, fields):
    """Build one cross-pack summary-table row from parsed log fields."""
    def num(text):
        m = re.search(r'[-+]?\d*\.?\d+', text or '')
        return float(m.group()) if m else None

    def bytes_of(text):
        m = re.search(r'\((\d+)\s*bytes\)', text or '')
        return int(m.group(1)) if m else None

    total_min  = num(fields.get('Total time'))                 # "387.3 minutes"
    data_label = (fields.get('Data copied') or '').split('(')[0].strip()
    data_bytes = bytes_of(fields.get('Data copied'))
    avg_speed  = fields.get('Average speed') or ''             # "220.4 MB/s"
    robo       = (fields.get('Robocopy time') or '').strip()

    window, sort_time = '', name
    try:
        ds = datetime.fromisoformat(fields['Started'])
        de = datetime.fromisoformat(fields['Finished'])
        window = f"{ds.strftime('%b %d %H:%M')} → {de.strftime('%b %d %H:%M')}"
        sort_time = de.isoformat()
    except (KeyError, ValueError):
        pass

    total_txt = f"{total_min:.1f} min ({total_min / 60:.1f} h)" if total_min else ''

    robo_short = robo[2:] if robo.startswith('0:') else robo
    tape_txt = avg_speed + (f" ({robo_short})" if robo_short else '')

    e2e_txt = ''
    if data_bytes and total_min:
        e2e_txt = f"~{(data_bytes / 1024**3) / (total_min / 60):.1f} GiB/h"

    rec = fields.get('Packed Records Inserted')
    if rec is None:
        total = sum(int(v) for k, v in fields.items()
                    if k.endswith('Inserted') and v.strip().isdigit())
        rec = str(total) if total else ''
    try:
        records_txt = f"{int(str(rec).strip()):,}" if str(rec).strip() else ''
    except ValueError:
        records_txt = str(rec)

    m = re.search(r'pack[_-]?(\d+)', name, re.IGNORECASE)
    if m:
        pack, sort_pack = m.group(1), (0, int(m.group(1)))
    else:
        pack, sort_pack = os.path.splitext(name)[0], (1, 0)

    return {
        'pack': pack, 'window': window, 'total': total_txt, 'data': data_label,
        'tape': tape_txt, 'e2e': e2e_txt, 'records': records_txt,
        'fetch': fields.get('Fetch speed (remote->PC)') or '',
        'dbsync': fields.get('DB sync time') or '',
        'sort_pack': sort_pack, 'sort_time': sort_time,
    }


def generate_backup_summary(log_dir=None, output_name='SUMMARY.md'):
    """(Re)write a Markdown table summarizing every per-pack log in log_dir.

    Returns the output path, or None if no logs could be summarized. Never
    raises on a single malformed log — it is skipped."""
    log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
    try:
        names = [n for n in os.listdir(log_dir) if n.lower().endswith('.log')]
    except OSError:
        return None

    rows = []
    for name in names:
        fields = _parse_backup_log(os.path.join(log_dir, name))
        if not fields:
            continue
        try:
            rows.append(_summarize_log(name, fields))
        except Exception:
            continue
    if not rows:
        return None

    rows.sort(key=lambda r: (r['sort_pack'], r['sort_time']))
    have_fetch  = any(r['fetch'] for r in rows)
    have_dbsync = any(r['dbsync'] for r in rows)

    headers = ['Pack', 'Window', 'Total job time', 'Data copied',
               'Tape-write speed¹', 'End-to-end speed²', 'Records']
    if have_fetch:
        headers.append('Fetch speed³')
    if have_dbsync:
        headers.append('DB sync')

    out = ['# LTO Backup Summary', '',
           f'_Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} from '
           f'{len(rows)} log(s) in `{log_dir}`._', '',
           '| ' + ' | '.join(headers) + ' |',
           '|' + '|'.join('---' for _ in headers) + '|']
    for r in rows:
        cells = [r['pack'], r['window'], r['total'], r['data'],
                 r['tape'], r['e2e'], r['records']]
        if have_fetch:
            cells.append(r['fetch'])
        if have_dbsync:
            cells.append(r['dbsync'])
        out.append('| ' + ' | '.join(cells) + ' |')
    out += ['',
            '¹ Tape-write speed = data ÷ robocopy time (raw LTO write); '
            'value in parens is robocopy duration.',
            '² End-to-end speed = data ÷ total job time (covers fetch, '
            'pack, DB sync, and tape write).']
    if have_fetch:
        out.append('³ Fetch speed = remote→PC throughput over SSH '
                   '("internet speed").')
    out.append('')

    out_path = os.path.join(log_dir, output_name)
    try:
        with open(out_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write('\n'.join(out))
    except OSError:
        return None
    return out_path
