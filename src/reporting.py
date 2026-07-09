"""Unified CSV statistics reporting.

Every backup/tape-write session is appended as one row to a single
``SUMMARY.csv`` in the backup-log directory. No per-run log files and no
per-file manifests are written, so the CSV never contains individual file
names: only run-level aggregate statistics.
"""
import csv
import os
from datetime import datetime
from typing import Any, Dict

from .constants import BACKUP_LOG_DIR
from .telegram_notify import notify_backup_summary


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
    'records_inserted',
    'records_updated',
    'records_skipped',
    'tape_used_after_bytes',
    'robocopy_exit_code',
    'robocopy_speed_mbs',
    # Legacy maintenance columns. Keep them in-place so old SUMMARY.csv files
    # can be migrated without shifting later backup fields.
    'before_bytes',
    'after_bytes',
    'reduction_pct',
    # Newer skipped-file report columns are appended only; inserting them in the
    # middle of an existing CSV corrupts positional readers.
    'skipped_files_count',
    'skipped_files_report',
    'fetch_ram_peak_pct',
    'fetch_ram_min_available_gb',
    'fetch_process_peak_mb',
    'pack_ram_peak_pct',
    'pack_ram_min_available_gb',
    'pack_process_peak_mb',
    'tape_ram_peak_pct',
    'tape_ram_min_available_gb',
    'tape_process_peak_mb',
    'db_sync_ram_peak_pct',
    'db_sync_ram_min_available_gb',
    'db_sync_process_peak_mb',
    'governor_wait_seconds',
    'governor_wait_reasons',
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
    # type: () -> Dict[str, Any]
    return {column: '' for column in SUMMARY_COLUMNS}


def _float_or_none(value):
    try:
        return float(str(value).replace(',', ''))
    except (TypeError, ValueError):
        return None


def _int_or_none(value):
    try:
        return int(float(str(value).replace(',', '')))
    except (TypeError, ValueError):
        return None


def _summary_columns(existing_header=None):
    """Canonical columns plus any unknown columns already present on disk."""
    columns = list(SUMMARY_COLUMNS)
    for column in existing_header or []:
        if column and column not in columns:
            columns.append(column)
    return columns


def _looks_like_shifted_skipped_row(header, row):
    """Detect rows written after skipped columns were inserted mid-schema.

    The old on-disk header did not have skipped_files_count/report. Rows written
    by the newer writer shifted records/tape/speed fields two positions right,
    making tape_used_after_bytes appear as robocopy_speed_mbs.
    """
    if 'skipped_files_count' in header or 'skipped_files_report' in header:
        return False
    if row.get('record_type') != 'backup' or row.get('operation') != 'backup':
        return False

    speed = _float_or_none(row.get('robocopy_speed_mbs'))
    exit_code = _int_or_none(row.get('before_bytes'))
    shifted_speed = _float_or_none(row.get('after_bytes'))
    if speed is None or exit_code is None or shifted_speed is None:
        return False
    if not (0 <= exit_code <= 16):
        return False
    return speed > 10000 and 0 <= shifted_speed < 10000


def _repair_shifted_skipped_row(row):
    repaired = dict(row)
    repaired['skipped_files_count'] = row.get('records_inserted', '')
    repaired['skipped_files_report'] = row.get('records_updated', '')
    repaired['records_inserted'] = row.get('records_skipped', '')
    repaired['records_updated'] = row.get('tape_used_after_bytes', '')
    repaired['records_skipped'] = row.get('robocopy_exit_code', '')
    repaired['tape_used_after_bytes'] = row.get('robocopy_speed_mbs', '')
    repaired['robocopy_exit_code'] = row.get('before_bytes', '')
    repaired['robocopy_speed_mbs'] = row.get('after_bytes', '')
    repaired['before_bytes'] = ''
    repaired['after_bytes'] = ''
    repaired['reduction_pct'] = ''
    return repaired


def _migrate_summary_schema(path):
    """Normalize SUMMARY.csv to the current append-safe schema in-place."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return list(SUMMARY_COLUMNS)

    with open(path, encoding='utf-8', newline='') as handle:
        raw_rows = list(csv.reader(handle))
    if not raw_rows:
        return list(SUMMARY_COLUMNS)

    header = raw_rows[0]
    columns = _summary_columns(header)
    out_rows = []
    changed = header != columns
    extra_count = 0

    for raw in raw_rows[1:]:
        row = {column: '' for column in columns}
        for idx, value in enumerate(raw):
            if idx < len(header):
                row[header[idx]] = value
            else:
                extra_count += 1
                extra_col = f'extra_{extra_count}'
                if extra_col not in columns:
                    columns.append(extra_col)
                    for existing in out_rows:
                        existing[extra_col] = ''
                row[extra_col] = value
                changed = True

        if _looks_like_shifted_skipped_row(header, row):
            row = _repair_shifted_skipped_row(row)
            changed = True
        out_rows.append(row)

    if changed:
        tmp_path = path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in out_rows:
                writer.writerow({column: row.get(column, '') for column in columns})
        try:
            os.replace(tmp_path, path)
        except PermissionError as exc:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise PermissionError(
                f"Could not update {path}. Close it in Excel or any other "
                "viewer, then retry."
            ) from exc

    return columns


def _append_row(log_dir, row):
    """Write a single fully-keyed row to SUMMARY.csv, creating it if needed."""
    log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, SUMMARY_CSV)
    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    if write_header:
        columns = list(SUMMARY_COLUMNS)
    else:
        # Fast path: when the on-disk header already matches the canonical
        # schema, skip the full-file migration read — it re-parsed the entire
        # (ever-growing) CSV on every appended row.
        with open(path, encoding='utf-8', newline='') as handle:
            header = next(csv.reader(handle), [])
        columns = (list(SUMMARY_COLUMNS) if header == SUMMARY_COLUMNS
                   else _migrate_summary_schema(path))
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if write_header:
            writer.writeheader()
        writer.writerow({column: row.get(column, '') for column in columns})
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
        'fetch_ram_peak_pct': details.get('fetch_ram_peak_pct', ''),
        'fetch_ram_min_available_gb': details.get(
            'fetch_ram_min_available_gb', ''),
        'fetch_process_peak_mb': details.get('fetch_process_peak_mb', ''),
        'pack_ram_peak_pct': details.get('pack_ram_peak_pct', ''),
        'pack_ram_min_available_gb': details.get(
            'pack_ram_min_available_gb', ''),
        'pack_process_peak_mb': details.get('pack_process_peak_mb', ''),
        'tape_ram_peak_pct': details.get('tape_ram_peak_pct', ''),
        'tape_ram_min_available_gb': details.get(
            'tape_ram_min_available_gb', ''),
        'tape_process_peak_mb': details.get('tape_process_peak_mb', ''),
        'db_sync_ram_peak_pct': details.get('db_sync_ram_peak_pct', ''),
        'db_sync_ram_min_available_gb': details.get(
            'db_sync_ram_min_available_gb', ''),
        'db_sync_process_peak_mb': details.get('db_sync_process_peak_mb', ''),
        'governor_wait_seconds': details.get('governor_wait_seconds', ''),
        'governor_wait_reasons': details.get('governor_wait_reasons', ''),
    })
    return _append_row(log_dir, row)


def _write_source_missing_only_log(log_dir, session_id, chunk_index,
                                   tape_label, missing_files,
                                   source_host='', source_path='',
                                   notifier=None):
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
    path = append_backup_summary_row(log_dir, details, None)
    notify_backup_summary(notifier, details, None)
    return path


def generate_backup_summary(log_dir=None, output_name=SUMMARY_CSV):
    """Ensure the aggregate CSV exists and return its path."""
    log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, output_name)
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8', newline='') as handle:
            csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS).writeheader()
    elif output_name == SUMMARY_CSV:
        _migrate_summary_schema(path)
    return path
