"""One tape, one usage percentage (audit M7).

The same cartridge used to be reported at 80.5% by the GUI (binary divisor),
86.5% by the text menu (decimal divisor) and was really at 92.5% of the
planning budget the archiver enforces. Every display must now agree with
``tape_budget_bytes``.
"""
import os
import re

import pytest

from src import cli
from src.constants import (LOCAL_TAPE_BUDGET_BYTES, tape_budget_bytes,
                           tape_usage)

CAPACITY_GB = 12288
# 86.5% of the registered decimal capacity: the audit's worked example.
USED_BYTES = int(0.865 * CAPACITY_GB * 1000**3)


class FakeDb:
    def __init__(self, capacity=CAPACITY_GB, used=USED_BYTES, status="active"):
        self.row = {"tape_id": 1, "volume_label": "Tape_T",
                    "date_formatted": None, "total_capacity": capacity,
                    "used_space": used, "status": status,
                    "status_reason": None}

    def list_tapes(self):
        return [dict(self.row)]

    def count_tape_file_records(self, _label):
        return 0


def _cli_percent(db, capsys):
    cli._print_tapes_table(db)
    out = capsys.readouterr().out
    match = re.search(r"(\d+\.\d)%", out)
    assert match, out
    return match.group(1) + "%", out


def test_m7_audit_example_was_three_different_numbers():
    old_gui = USED_BYTES / (CAPACITY_GB * 1024**3) * 100
    old_menu = USED_BYTES / (CAPACITY_GB * 1000**3) * 100
    assert 80.5 <= old_gui < 80.6
    assert f"{old_menu:.1f}" == "86.5"
    assert tape_usage(CAPACITY_GB, USED_BYTES).percent_text == "92.5%"


def test_m7_usage_is_measured_against_the_planning_budget():
    usage = tape_usage(CAPACITY_GB, USED_BYTES)
    budget, available = tape_budget_bytes(CAPACITY_GB, USED_BYTES)
    assert usage.budget_bytes == budget == LOCAL_TAPE_BUDGET_BYTES
    assert usage.used_bytes == USED_BYTES
    assert usage.available_bytes == available
    assert usage.percent == pytest.approx(USED_BYTES / budget * 100)
    # A smaller registered capacity is the budget, exactly as in planning.
    small = tape_usage(1000, 500 * 1000**3)
    assert small.budget_bytes == 1000 * 1000**3
    assert small.percent_text == "50.0%"
    # Over budget is reported as such; only the bar fraction is clamped.
    over = tape_usage(1000, 1500 * 1000**3)
    assert over.percent_text == "150.0%"
    assert over.bar_fraction == 1.0
    # No registered capacity: planning falls back to the default budget.
    assert tape_usage(None, 0).budget_bytes == LOCAL_TAPE_BUDGET_BYTES
    assert tape_usage(0, None).percent_text == "0.0%"


def test_m7_text_menu_shows_the_planning_budget_percentage(capsys):
    percent, out = _cli_percent(FakeDb(), capsys)
    assert percent == "92.5%"
    # Decimal units, labelled as decimal: used / planning budget in GB.
    assert f"{USED_BYTES / 1000**3:.1f}/{LOCAL_TAPE_BUDGET_BYTES / 1000**3:.0f} GB" in out


@pytest.mark.parametrize("capacity,used", [
    (CAPACITY_GB, USED_BYTES),
    (CAPACITY_GB, 5 * 1000**4),
    (1000, 123_456_789_012),
    (None, 3 * 1024**4),
])
def test_m7_gui_and_text_menu_show_the_same_percentage(capacity, used, capsys):
    pytest.importorskip("PySide6.QtWidgets")
    os.environ["QT_QPA_PLATFORM"] = "offscreen"
    from PySide6.QtWidgets import QApplication
    import src.db_inspector_qt as inspector

    app = QApplication.instance() or QApplication([])
    db = FakeDb(capacity, used)
    widget = inspector.ManageWidget(db, "fake-dsn")
    widget.refresh_tapes()
    gui_percent = widget.tapes_table.item(0, 6).text()
    menu_percent, _out = _cli_percent(db, capsys)
    assert gui_percent == menu_percent == tape_usage(capacity, used).percent_text
    # The GUI's byte column stays binary and says so.
    assert widget.tapes_table.item(0, 4).text().endswith(("GiB", "TiB"))
    del widget, app
