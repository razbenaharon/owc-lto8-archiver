"""Named background jobs for the storage_map web app.

One operator, one browser tab, three long-ish operations (scan launch, log
fetch, DB coverage refresh) — so this is deliberately a tiny thread-per-job
manager with a busy lock, not a queue. A job that is still ``running`` blocks
a second start of itself (and of any name listed in ``conflicts``), which the
API surfaces as HTTP 409.
"""
import threading
from datetime import datetime


class JobBusy(RuntimeError):
    """Raised by :meth:`JobManager.start` when a conflicting job is running."""


def _now():
    return datetime.now().isoformat(timespec='seconds')


class JobManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs = {}

    def start(self, name, fn, conflicts=()):
        """Run ``fn`` in a daemon thread under ``name``.

        ``fn`` returns a human-readable detail string; an exception marks the
        job ``failed`` with the exception text as detail (never a traceback
        with secrets — job functions must not embed credentials in messages).
        """
        with self._lock:
            for other in (name, *conflicts):
                job = self._jobs.get(other)
                if job and job['state'] == 'running':
                    raise JobBusy(other)
            self._jobs[name] = {
                'state': 'running',
                'started_at': _now(),
                'finished_at': None,
                'detail': '',
            }
        threading.Thread(target=self._run, args=(name, fn), daemon=True,
                         name=f'storage-map-{name}').start()

    def _run(self, name, fn):
        try:
            detail = fn()
            state, detail = 'done', str(detail or '')
        except Exception as exc:  # noqa: BLE001 - report, don't crash the app
            state, detail = 'failed', f'{type(exc).__name__}: {exc}'
        with self._lock:
            job = self._jobs.get(name)
            if job is not None:
                job.update(state=state, detail=detail, finished_at=_now())

    def is_running(self, name):
        with self._lock:
            job = self._jobs.get(name)
            return bool(job and job['state'] == 'running')

    def snapshot(self):
        """A JSON-safe copy of every job's current state."""
        with self._lock:
            return {name: dict(job) for name, job in self._jobs.items()}
