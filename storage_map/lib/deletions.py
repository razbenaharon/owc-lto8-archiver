"""Durable record of server directories deleted after being archived to tape.

Deleting a directory from a lab server destroys the last online copy — the
archive tape becomes the only one. That step therefore needs a permanent,
human-readable record of *what* was removed, *when*, and *what evidence* said it
was safe. This module owns that ledger.

The ledger is a JSON file under ``<output_dir>/deletions/ledger.json``, written
atomically and read by the dashboard's "Reclaimed from servers" panel. Each
record is self-contained: the verification numbers are copied in, not
recomputed, so the record still explains itself years later when the catalog has
moved on. A per-deletion ``inventory`` TSV lists every removed file.

This module never deletes anything. It only records deletions that already
happened.
"""
import json
import os
from datetime import datetime

LEDGER_DIRNAME = 'deletions'
LEDGER_FILENAME = 'ledger.json'

# Fields every record must carry for the entry to mean anything later. A record
# missing any of these cannot answer "was this safe to delete?", so it is
# rejected rather than stored as a half-record.
REQUIRED_FIELDS = ('server', 'path', 'deleted_at', 'files', 'bytes',
                   'tape_label', 'verification')


def ledger_dir(smcfg):
    return os.path.join(smcfg.output_dir, LEDGER_DIRNAME)


def ledger_path(smcfg):
    return os.path.join(ledger_dir(smcfg), LEDGER_FILENAME)


def load_ledger(smcfg):
    """Return the recorded deletions, newest first. Missing/corrupt -> []."""
    try:
        with open(ledger_path(smcfg), encoding='utf-8') as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return []
    records = data.get('records') if isinstance(data, dict) else data
    if not isinstance(records, list):
        return []
    return sorted((r for r in records if isinstance(r, dict)),
                  key=lambda r: str(r.get('deleted_at') or ''), reverse=True)


def _write_ledger(smcfg, records):
    os.makedirs(ledger_dir(smcfg), exist_ok=True)
    payload = {
        'schema': 1,
        'updated_at': datetime.now().isoformat(timespec='seconds'),
        'records': records,
    }
    path = ledger_path(smcfg)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    return path


def append_record(smcfg, record):
    """Append one deletion record. Re-recording the same server+path replaces
    the previous entry, so a corrected record never leaves a stale twin."""
    missing = [f for f in REQUIRED_FIELDS if record.get(f) in (None, '')]
    if missing:
        raise ValueError('deletion record is missing required fields: '
                         + ', '.join(missing))
    key = (record['server'], record['path'])
    kept = [r for r in load_ledger(smcfg)
            if (r.get('server'), r.get('path')) != key]
    kept.append(dict(record))
    _write_ledger(smcfg, kept)
    return record


def summarize(records):
    """Totals for the dashboard header."""
    return {
        'count': len(records),
        'files': sum(int(r.get('files') or 0) for r in records),
        'bytes': sum(int(r.get('bytes') or 0) for r in records),
        'servers': sorted({str(r.get('server')) for r in records
                           if r.get('server')}),
    }


def payload(smcfg):
    records = load_ledger(smcfg)
    return {'records': records, 'totals': summarize(records)}
