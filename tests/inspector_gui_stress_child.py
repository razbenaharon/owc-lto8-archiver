"""Child process for tests/test_inspector_gui_workers.py (audit H15).

Runs N write actions through the real ``ManageWidget`` against a fake database
and a fake repository, headless, and prints how many got their reply. It lives
in its own process because the failure it guards against was a hard crash of
the interpreter, which a same-process test cannot report.

Usage: python inspector_gui_stress_child.py <actions>
"""
import os
import sys
import time

os.environ["QT_QPA_PLATFORM"] = "offscreen"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PySide6.QtWidgets import QApplication, QInputDialog, QMessageBox  # noqa: E402

import src.db_inspector_qt as inspector  # noqa: E402


class FakeDb:
    def __init__(self):
        self.status = "active"
        self.calls = 0

    def list_tapes(self):
        return [{"tape_id": 1, "volume_label": "Tape_T", "date_formatted": None,
                 "total_capacity": 12288, "used_space": 0,
                 "status": self.status, "status_reason": None}]

    def count_tape_file_records(self, _label):
        return 0

    def set_tape_status(self, _label, status, _reason=None):
        self.calls += 1
        self.status = status
        return 1

    def rename_tape(self, _old, _new):
        self.calls += 1
        return 1

    def recalculate_tape_used_space(self, _label):
        self.calls += 1
        return 0


class FakeRepository:
    def __init__(self, _db_path):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def list_sessions(self):
        return []

    def unreferenced_remote_data_summary(self):
        return {"supported": 1, "active_sessions": 0, "plans": 0,
                "snapshots": 0}


def main(actions):
    app = QApplication([])
    inspector.InspectorRepository = FakeRepository
    QMessageBox.question = staticmethod(
        lambda *_a, **_k: QMessageBox.StandardButton.Yes)
    QMessageBox.critical = staticmethod(lambda *_a, **_k: None)
    QInputDialog.getText = staticmethod(lambda *_a, **_k: ("Tape_T", True))

    db = FakeDb()
    widget = inspector.ManageWidget(db, "fake-dsn")
    widget.refresh()
    replies = 0
    for i in range(actions):
        widget.tapes_table.selectRow(0)
        before = db.calls
        action = (widget.toggle_tape_status, widget.rename_tape,
                  widget.recalculate_used)[i % 3]
        action()
        deadline = time.monotonic() + 5.0
        # The reply is what re-enables the write buttons and refreshes.
        while time.monotonic() < deadline:
            app.processEvents()
            if db.calls > before and widget.rename_btn.isEnabled():
                replies += 1
                break
            time.sleep(0.001)
    print(f"replies {replies} of {actions}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(int(sys.argv[1])))
