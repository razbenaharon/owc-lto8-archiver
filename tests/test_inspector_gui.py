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


# --------------------------------------------------------------------------
# M8 - destructive actions, pagination, dead button, database-down refresh
# --------------------------------------------------------------------------

class Dialogs:
    """Records and scripts every modal the inspector can raise."""

    def __init__(self, monkeypatch):
        self.errors = []
        self.warnings = []
        self.typed_prompts = []
        self.typed_answer = ("", False)
        self.question_answer = inspector.QMessageBox.StandardButton.Yes
        box = inspector.QMessageBox
        monkeypatch.setattr(box, "critical", staticmethod(
            lambda _p, _t, message, *_a, **_k: self.errors.append(message)))
        monkeypatch.setattr(box, "warning", staticmethod(
            lambda _p, _t, message, *_a, **_k: self.warnings.append(message)))
        monkeypatch.setattr(box, "information", staticmethod(
            lambda *_a, **_k: None))
        monkeypatch.setattr(box, "question", staticmethod(
            lambda *_a, **_k: self.question_answer))
        monkeypatch.setattr(inspector.QInputDialog, "getText", staticmethod(
            self._get_text))

    def _get_text(self, _parent, _title, label, *_a, **_k):
        self.typed_prompts.append(label)
        return self.typed_answer


@pytest.fixture
def dialogs(monkeypatch):
    return Dialogs(monkeypatch)


def _session_row(session_id, status, kind="remote"):
    return {"kind": kind, "session_id": session_id,
            "session_label": f"S{session_id}", "status": status, "mode": "",
            "created_at": None, "completed_at": None, "chunks": 1,
            "manifest_rows": 0, "manifest_bytes": 0, "file_records": 0}


def _manage_with_sessions(qapp, fake_repo, rows):
    fake_repo.sessions = rows
    fake_repo.deletable_checks = []

    def require_session_deletable(self, kind, session_id):
        fake_repo.deletable_checks.append((kind, session_id))
        if fake_repo.blockers:
            raise RuntimeError("; ".join(fake_repo.blockers))

    fake_repo.require_session_deletable = require_session_deletable
    db = FakeDb()
    widget = inspector.ManageWidget(db, "fake-dsn")
    widget.refresh_sessions()
    assert pump(qapp, lambda: widget.sessions_table.rowCount() == len(rows))
    return db, widget


def _settle(qapp, widget):
    assert pump(qapp, lambda: widget.tasks.pending() == 0)
    qapp.processEvents()


def test_m8_gui_refuses_to_delete_an_active_session(qapp, fake_repo, dialogs):
    db, widget = _manage_with_sessions(qapp, fake_repo, [
        _session_row(36, "abandoned"), _session_row(37, "active")])
    widget.sessions_table.selectRow(1)
    assert widget.selected_session() == ("remote", 37)
    dialogs.typed_answer = ("37", True)
    widget.delete_session()
    _settle(qapp, widget)
    assert db.calls == [], "an active session was deleted from the GUI"
    assert dialogs.typed_prompts == [], "refusal must come before any prompt"
    assert any("active" in message for message in dialogs.errors)


def test_m8_session_delete_needs_typed_confirmation(qapp, fake_repo, dialogs):
    db, widget = _manage_with_sessions(qapp, fake_repo, [
        _session_row(36, "abandoned"), _session_row(37, "active")])
    widget.sessions_table.selectRow(0)

    for wrong in (("", True), ("yes", True), ("37", True), ("36", False)):
        dialogs.typed_answer = wrong
        widget.delete_session()
        _settle(qapp, widget)
    assert db.calls == [], "deleted without the typed confirmation"
    assert all("36" in prompt for prompt in dialogs.typed_prompts)

    dialogs.typed_answer = ("36", True)
    widget.delete_session()
    assert pump(qapp, lambda: db.calls == [("delete_session", "remote", 36)])
    # The repository pre-flight ran on the way.
    assert fake_repo.deletable_checks == [("remote", 36)]


def test_m8_session_delete_stops_when_repository_refuses(qapp, fake_repo,
                                                         dialogs):
    db, widget = _manage_with_sessions(qapp, fake_repo, [
        _session_row(36, "abandoned")])
    fake_repo.blockers = ["the archiver lock is held (1 holder(s))"]
    widget.sessions_table.selectRow(0)
    dialogs.typed_answer = ("36", True)
    widget.delete_session()
    assert pump(qapp, lambda: bool(dialogs.errors))
    _settle(qapp, widget)
    assert db.calls == []
    assert "archiver lock is held" in dialogs.errors[-1]
    assert widget.delete_session_btn.isEnabled()


@pytest.mark.parametrize("action,expected,call", [
    ("delete_tape", "Tape_T", ("delete_tape", "Tape_T")),
    ("wipe_tape_files", "Tape_T", ("delete_files_for_tape", "Tape_T")),
])
def test_m8_tape_destruction_needs_the_label_typed(qapp, fake_repo, dialogs,
                                                   action, expected, call):
    db = FakeDb()
    db.delete_tape = lambda label: db.calls.append(("delete_tape", label))
    db.delete_files_for_tape = lambda label: db.calls.append(
        ("delete_files_for_tape", label))
    widget = inspector.ManageWidget(db, "fake-dsn")
    widget.refresh_tapes()
    widget.tapes_table.selectRow(0)
    dialogs.typed_answer = ("tape_t", True)        # close, but not the label
    getattr(widget, action)()
    _settle(qapp, widget)
    assert db.calls == []
    dialogs.typed_answer = (expected, True)
    getattr(widget, action)()
    assert pump(qapp, lambda: db.calls == [call])
    _settle(qapp, widget)


class PagedRepository(FakeRepository):
    """Three pages of search/browse results; can be told to fail."""
    fail_queries = False

    @staticmethod
    def _page(cursor):
        page = (cursor or {}).get("page", 0)
        if PagedRepository.fail_queries:
            raise RuntimeError("server closed the connection unexpectedly")
        return {"rows": [{"file_id": page * 10 + i, "file_name": f"f{page}_{i}"}
                         for i in range(3)],
                "has_more": page < 2,
                "next_cursor": {"page": page + 1} if page < 2 else None}

    def list_tapes(self):
        return []

    def search_catalog_fts(self, _query, scope=None, cursor=None, limit=None):
        return self._page(cursor)

    def list_directory_files(self, _directory_id, sort="name", cursor=None,
                             limit=None):
        return self._page(cursor)


class ListingDb(FakeDb):
    def list_source_hosts(self):
        return ["source-host.example"]


def _paging_widget(qapp, monkeypatch, kind):
    PagedRepository.fail = None
    PagedRepository.fail_queries = False
    monkeypatch.setattr(inspector, "InspectorRepository", PagedRepository)
    if kind == "search":
        widget = inspector.SearchWidget(ListingDb(), "fake-dsn")
        widget.query.setText("needle")
        widget.new_search()
    else:
        widget = inspector.BrowseWidget(ListingDb(), "fake-dsn")
        widget.current_directory_id = 5
        widget.load_page(None)
    assert pump(qapp, lambda: len(widget.table_model.rows) == 3)
    return widget


def _first_id(widget):
    return widget.table_model.rows[0]["file_id"]


@pytest.mark.parametrize("kind", ["search", "browse"])
def test_m8_failed_page_change_keeps_pagination_state(qapp, monkeypatch,
                                                      dialogs, kind):
    widget = _paging_widget(qapp, monkeypatch, kind)
    widget.next_page()
    assert pump(qapp, lambda: _first_id(widget) == 10)
    assert widget.page_index == 1

    PagedRepository.fail_queries = True
    widget.next_page()
    assert pump(qapp, lambda: bool(dialogs.errors))
    # Still on page 2 of 3, showing page 2's rows, with a working stack.
    assert widget.page_index == 1
    assert _first_id(widget) == 10
    assert widget.next_btn.isEnabled() and widget.prev_btn.isEnabled()
    assert "page 2" in widget.status.text()

    PagedRepository.fail_queries = False
    widget.next_page()
    assert pump(qapp, lambda: _first_id(widget) == 20)
    assert widget.page_index == 2 and not widget.next_btn.isEnabled()
    widget.previous_page()
    assert pump(qapp, lambda: _first_id(widget) == 10)
    widget.previous_page()
    assert pump(qapp, lambda: _first_id(widget) == 0)
    assert widget.page_index == 0 and not widget.prev_btn.isEnabled()


def test_m8_no_button_launches_a_removed_command(qapp, fake_repo, dialogs,
                                                 monkeypatch):
    launched = []
    monkeypatch.setattr(subprocess, "Popen",
                        lambda cmd, *_a, **_k: launched.append(cmd))
    dialogs.question_answer = inspector.QMessageBox.StandardButton.No
    db, widget = _manage_with_sessions(qapp, fake_repo, [
        _session_row(36, "abandoned")])
    widget.refresh_tapes()
    buttons = widget.findChildren(inspector.QPushButton)
    assert buttons
    for button in buttons:
        button.click()
        _settle(qapp, widget)
    assert launched == [], f"a button still shells out: {launched}"
    assert db.calls == []


def test_m8_refresh_with_database_down_shows_an_error(qapp, fake_repo,
                                                      dialogs):
    db = ListingDb()
    fake_repo.require_catalog_v3 = lambda self: None
    fake_repo.list_tapes = lambda self: []
    window = inspector.DBInspectorQtApp(db, "fake-dsn")
    window.tabs.setCurrentWidget(window.manage)
    _settle(qapp, window.manage)
    assert dialogs.errors == []

    db.fail_list_tapes = "connection to server at 127.0.0.1 failed"
    fake_repo.fail = "connection to server at 127.0.0.1 failed"
    window.refresh_all()                      # must not raise
    _settle(qapp, window.manage)
    assert dialogs.errors, "refresh with the database down said nothing"
    assert all("127.0.0.1 failed" in message for message in dialogs.errors)
    assert "database" in dialogs.errors[0].lower()

    # A later refresh, database back, recovers without a restart.
    db.fail_list_tapes = None
    fake_repo.fail = None
    dialogs.errors.clear()
    window.refresh_all()
    _settle(qapp, window.manage)
    assert dialogs.errors == []
    assert window.manage.tapes_table.rowCount() == 1
