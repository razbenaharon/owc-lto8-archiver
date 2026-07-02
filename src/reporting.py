"""Unified CSV statistics reporting.

Every backup/tape-write session is appended as one row to a single
``SUMMARY.csv`` in the backup-log directory. No per-run log files and no
per-file manifests are written, so the CSV never contains individual file
names: only run-level aggregate statistics.
"""
import csv
import os
from datetime import datetime

from .constants import BACKUP_LOG_DIR


SUMMARY_CSV = 'SUMMARY.csv'

SUMMARY_COLUMNS = [
    'record_type',          # always 'backup'
    'operation',            # always 'backup'
    'status',
    'source_host',
    'source_path',
    'tape_label',
    'backup_mode',
    'local_session_id',
    'local_chunk_index',
    'started_at',
    'finished_at',
    'total_time_seconds',
    'robocopy_elapsed',
    'fetch_seconds',
    'pack_seconds',
    'db_sync_seconds',
    'copied_bytes',
    'planned_bytes',
    'fetch_bytes',
    'pack_bytes',
    'files_copied',
    'files_skipped',
    'files_failed',
    'already_on_tape',
    'source_missing_files',
    'skipped_files_count',
    'skipped_files_report',
    'records_inserted',
    'records_updated',
    'records_skipped',
    'tape_used_after_bytes',
    'robocopy_exit_code',
    'robocopy_speed_mbs',
]


def _iso(value):
    if hasattr(value, 'isoformat'):
        return value.isoformat(timespec='seconds')
    return value or ''


def _seconds(value):
    if value is None:
        return ''
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return ''


def _blank_row():
    return {column: '' for column in SUMMARY_COLUMNS}


def _append_row(log_dir, row):
    """Write a single fully-keyed row to SUMMARY.csv, creating it if needed."""
    log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, SUMMARY_CSV)
    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return path


def append_backup_summary_row(log_dir=None, details=None, robocopy_result=None):
    """Append one compact backup/chunk/session row to SUMMARY.csv."""
    details = details or {}
    rc_sum = details.get('rc_sum') or {}
    counts = details.get('record_counts') or {}

    inserted = sum(
        int(value or 0) for key, value in counts.items()
        if key.endswith('_inserted'))
    updated = sum(
        int(value or 0) for key, value in counts.items()
        if key.endswith('_updated'))
    skipped = sum(
        int(value or 0) for key, value in counts.items()
        if key.endswith('_skipped') or key.endswith('_skipped_existing'))

    row = _blank_row()
    row.update({
        'record_type': 'backup',
        'operation': 'backup',
        'status': details.get('status', ''),
        'source_host': details.get('source_host', ''),
        'source_path': details.get('source', ''),
        'tape_label': details.get('tape_label', ''),
        'backup_mode': details.get('backup_mode', ''),
        'local_session_id': details.get('local_session_id', ''),
        'local_chunk_index': details.get('local_chunk_index', ''),
        'started_at': _iso(details.get('started_at')),
        'finished_at': _iso(details.get('finished_at') or datetime.now()),
        'total_time_seconds': _seconds(details.get('total_time_seconds')),
        'robocopy_elapsed': rc_sum.get('elapsed', ''),
        'fetch_seconds': _seconds(details.get('fetch_seconds')),
        'pack_seconds': _seconds(details.get('pack_seconds')),
        'db_sync_seconds': _seconds(details.get('db_sync_seconds')),
        'copied_bytes': details.get('copied_bytes', 0),
        'planned_bytes': details.get('total_bytes', 0),
        'fetch_bytes': details.get('fetch_bytes', ''),
        'pack_bytes': details.get('pack_bytes', ''),
        'files_copied': rc_sum.get('files_copied', 0),
        'files_skipped': int(rc_sum.get('files_skipped', 0) or 0) +
                         int(details.get('skipped', 0) or 0),
        'files_failed': rc_sum.get('files_failed', 0),
        'already_on_tape': details.get('skipped', 0),
        'source_missing_files': len(details.get('source_missing_files') or []),
        'skipped_files_count': details.get('skipped_files_count', ''),
        'skipped_files_report': details.get('skipped_files_report', ''),
        'records_inserted': inserted,
        'records_updated': updated,
        'records_skipped': skipped,
        'tape_used_after_bytes': details.get('new_used', ''),
        'robocopy_exit_code': (
            '' if robocopy_result is None else robocopy_result.returncode),
        'robocopy_speed_mbs': rc_sum.get('speed_mbs', ''),
    })
    return _append_row(log_dir, row)


def _write_source_missing_only_log(log_dir, session_id, chunk_index,
                                   tape_label, missing_files,
                                   source_host='', source_path=''):
    details = {
        'status': 'completed_without_tape_write',
        'source_host': source_host,
        'source': source_path,
        'tape_label': tape_label,
        'backup_mode': 'source-missing-only',
        'local_session_id': session_id,
        'local_chunk_index': chunk_index,
        'started_at': datetime.now(),
        'finished_at': datetime.now(),
        'total_time_seconds': 0,
        'total_bytes': 0,
        'copied_bytes': 0,
        'skipped': 0,
        'new_used': '',
        'source_missing_files': missing_files or [],
        'record_counts': {},
        'rc_sum': {},
    }
    return append_backup_summary_row(log_dir, details, None)


def generate_backup_summary(log_dir=None, output_name=SUMMARY_CSV):
    """Ensure the aggregate CSV exists and return its path."""
    log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, output_name)
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8', newline='') as handle:
            csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS).writeheader()
    return path
