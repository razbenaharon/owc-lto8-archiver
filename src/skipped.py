"""Skipped-file tracking and CSV reporting."""
import csv
import os
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock

from .constants import BACKUP_LOG_DIR


SKIPPED_COLUMNS = [
    'timestamp',
    'source',
    'path',
    'reason',
    'phase',
    'session_id',
    'chunk_index',
]


@dataclass(frozen=True)
class SkippedFile:
    source: str
    path: str
    reason: str
    phase: str
    session_id: object = ''
    chunk_index: object = ''
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat(timespec='seconds'))

    def as_row(self):
        return {
            'timestamp': self.timestamp,
            'source': self.source or '',
            'path': self.path or '',
            'reason': self.reason or '',
            'phase': self.phase or '',
            'session_id': '' if self.session_id is None else self.session_id,
            'chunk_index': '' if self.chunk_index is None else self.chunk_index,
        }


class SkippedFileTracker:
    """Thread-safe accumulator for file-level omissions in one archive run."""

    def __init__(self):
        self._items = []
        self._lock = Lock()
        self._report_path = None

    def add(self, source, path, reason, phase, session_id=None, chunk_index=None):
        item = SkippedFile(
            source=source,
            path=str(path or ''),
            reason=str(reason or ''),
            phase=phase,
            session_id=session_id if session_id is not None else '',
            chunk_index=chunk_index if chunk_index is not None else '',
        )
        with self._lock:
            self._items.append(item)
        return item

    def extend(self, items):
        for item in items or []:
            if isinstance(item, SkippedFile):
                with self._lock:
                    self._items.append(item)
            elif isinstance(item, dict):
                self.add(
                    item.get('source', ''),
                    item.get('path') or item.get('remote_path') or item.get('file_path') or '',
                    item.get('reason') or item.get('error_msg') or '',
                    item.get('phase', ''),
                    item.get('session_id'),
                    item.get('chunk_index'),
                )

    @property
    def report_path(self):
        return self._report_path

    def items(self):
        with self._lock:
            return list(self._items)

    def count(self):
        with self._lock:
            return len(self._items)

    def has_items(self):
        return self.count() > 0

    def write_csv(self, log_dir=None):
        items = self.items()
        if not items:
            return None
        log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
        os.makedirs(log_dir, exist_ok=True)
        path = self._report_path or os.path.join(
            log_dir,
            'skipped_files_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv',
        )
        with open(path, 'w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=SKIPPED_COLUMNS)
            writer.writeheader()
            for item in items:
                writer.writerow(item.as_row())
        self._report_path = path
        return path

    def print_summary(self, ui=print, log_dir=None):
        count = self.count()
        if count <= 0:
            return None
        report = self.write_csv(log_dir)
        by_source = {}
        for item in self.items():
            by_source[item.source or 'unknown'] = by_source.get(item.source or 'unknown', 0) + 1
        summary = ', '.join(f"{source}={total}" for source, total in sorted(by_source.items()))
        emit = ui.info if hasattr(ui, 'info') else ui
        emit(f"[SKIPPED] {count} file/path omission(s) recorded ({summary}).")
        emit(f"[SKIPPED] Detailed report: {report}")
        return report
