"""Headless tests for the PySide6 inspector (``src/db_inspector_qt.py``).

Everything here runs on Qt's ``offscreen`` platform against a fake database
and a fake repository: no window is shown and no PostgreSQL server is touched.
The module skips cleanly when PySide6 is not installed.
"""
import os
import subprocess
import sys
import threading
import time

import pytest

pytest.importorskip("PySide6.QtWidgets")
os.environ["QT_QPA_PLATFORM"] = "offscreen"

from PySide6.QtWidgets import QApplication  # noqa: E402

import src.db_inspector_qt as inspector  # noqa: E402

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


def pump(qapp, predicate, timeout=5.0):
    """Process events until ``predicate()`` is true; return whether it was."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        qapp.processEvents()
        if predicate():
            return True
        time.sleep(0.001)
    qapp.processEvents()
    return bool(predicate())


class FakeDb:
    def __init__(self):
        self.calls = []
        self.fail_list_tapes = None

    def list_tapes(self):
        if self.fail_list_tapes:
            raise RuntimeError(self.fail_list_tapes)
        return [{"tape_id": 1, "volume_label": "Tape_T", "date_formatted": None,
                 "total_capacity": 12288, "used_space": 0,
                 "status": "active", "status_reason": None}]

    def count_tape_file_records(self, _label):
        return 0

    def recalculate_tape_used_space(self, label):
        self.calls.append(("recalculate", label))
        return 0

    def delete_session(self, kind, session_id):
        self.calls.append(("delete_session", kind, session_id))
        return 1


class FakeRepository:
    sessions = []
    blockers = []
    fail = None

    def __init__(self, _db_path):
        if FakeRepository.fail:
            raise RuntimeError(FakeRepository.fail)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def list_sessions(self):
        return [dict(row) for row in FakeRepository.sessions]

    def unreferenced_remote_data_summary(self):
        return {"supported": 1, "active_sessions": 0, "plans": 0,
                "snapshots": 0}


@pytest.fixture
def fake_repo(monkeypatch):
    FakeRepository.sessions = []
    FakeRepository.blockers = []
    FakeRepository.fail = None
    monkeypatch.setattr(inspector, "InspectorRepository", FakeRepository)
    return FakeRepository


# --------------------------------------------------------------------------
# H15 - worker lifetime: every write gets its reply and the process survives
# --------------------------------------------------------------------------

def test_h15_every_write_action_gets_its_reply_and_process_survives():
    """120 real write actions, in a child process so a hard crash is a
    reportable failure instead of taking the test run down with it."""
    env = dict(os.environ, QT_QPA_PLATFORM="offscreen")
    proc = subprocess.run(
        [sys.executable,
         os.path.join(TESTS_DIR, "inspector_gui_stress_child.py"), "120"],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, (
        f"inspector crashed (exit {proc.returncode}):\n{proc.stderr[-2000:]}")
    assert "replies 120 of 120" in proc.stdout, proc.stdout + proc.stderr[-2000:]


def test_h15_replies_arrive_on_the_gui_thread_for_success_and_failure(qapp):
    runner = inspector.TaskRunner()
    gui_thread = threading.get_ident()
    seen = []

    def boom():
        raise RuntimeError("db is down")

    for i in range(100):
        runner.submit(lambda i=i: (i, threading.get_ident()),
                      on_ok=lambda r: seen.append(
                          ("ok", r[0], r[1], threading.get_ident())))
        runner.submit(boom, on_fail=lambda m: seen.append(
            ("fail", m, None, threading.get_ident())))
    assert pump(qapp, lambda: len(seen) == 200), f"only {len(seen)} of 200"
    assert runner.pending() == 0
    assert {row[3] for row in seen} == {gui_thread}
    oks = [row for row in seen if row[0] == "ok"]
    assert sorted(row[1] for row in oks) == list(range(100))
    assert all(row[2] != gui_thread for row in oks), "job ran on GUI thread"
    assert [row[1] for row in seen if row[0] == "fail"] == ["db is down"] * 100


def test_h15_only_one_write_is_in_flight(qapp, fake_repo, monkeypatch):
    monkeypatch.setattr(inspector.QMessageBox, "critical",
                        staticmethod(lambda *_a, **_k: None))
    widget = inspector.ManageWidget(FakeDb(), "fake-dsn")
    release = threading.Event()
    done = []
    assert widget._run_db_task(lambda: release.wait(10), on_done=done.append)
    assert not widget.rename_btn.isEnabled()
    # A second write while the first is running is refused, not queued.
    assert widget._run_db_task(lambda: done.append("second")) is False
    release.set()
    assert pump(qapp, lambda: bool(done))
    assert done == [True]
    assert widget.rename_btn.isEnabled()
    assert widget._run_db_task(lambda: 7, on_done=done.append)
    assert pump(qapp, lambda: done == [True, 7])
