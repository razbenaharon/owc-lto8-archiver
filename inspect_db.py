"""Root runner — launch the SQLite database inspector GUI.

Mirrors run.py's path anchoring: the GUI code lives in src/db_inspector.py and
the database lives in the project root, so we chdir to the root before reading
config.ini / the DB.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

import customtkinter as ctk

from src.config import ConfigManager
from src.db import DatabaseManager
from src.db_inspector import DBInspectorApp

if __name__ == "__main__":
    cfg = ConfigManager()
    db = DatabaseManager(cfg.db_path)

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("dark-blue")
    ctk.set_widget_scaling(1.25)

    app = DBInspectorApp(db, cfg.db_path)
    try:
        app.mainloop()
    finally:
        db.close()
