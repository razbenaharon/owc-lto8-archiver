import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox
try:
    import customtkinter as ctk
except ImportError:
    sys.exit("customtkinter is not installed. Run: pip install customtkinter")

APP_FONT_FAMILY = "Roboto"
APP_FONT_SIZE = 18
APP_FONT_SIZE_SMALL = 16
APP_FONT_SIZE_MONO = 15
TREE_FONT_SIZE = 26
TREE_HEADING_SIZE = 24
TREE_ROW_HEIGHT = 48


# ==============================================================================
# HELPERS
# ==============================================================================

def _fmt_bytes(n):
    if n is None:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _bar_str(pct, width=18):
    if pct is None:
        return "░" * width
    pct = max(0.0, min(1.0, pct))
    filled = round(pct * width)
    return "█" * filled + "░" * (width - filled)


def _prepare_database_connection(db):
    if not callable(getattr(db, "_require_updated", None)):
        def _require_updated(cur, message):
            if cur.rowcount == 0:
                raise RuntimeError(message)
        db._require_updated = _require_updated

    db.conn.execute("PRAGMA foreign_keys = ON")
    enabled = db.conn.execute("PRAGMA foreign_keys").fetchone()[0]
    if enabled != 1:
        raise RuntimeError("[DB] Could not enable SQLite foreign key checks.")


def _apply_treeview_dark_style(style):
    style.theme_use("clam")
    style.configure("Treeview",
        background="#212121",
        foreground="#DCE4EE",
        fieldbackground="#212121",
        bordercolor="#474747",
        borderwidth=1,
        rowheight=TREE_ROW_HEIGHT,
        font=(APP_FONT_FAMILY, TREE_FONT_SIZE))
    style.configure("Treeview.Heading",
        background="#1a1a1a",
        foreground="#DCE4EE",
        relief="flat",
        font=(APP_FONT_FAMILY, TREE_HEADING_SIZE, "bold"))
    style.map("Treeview",
        background=[("selected", "#1f538d")],
        foreground=[("selected", "#DCE4EE")])
    style.map("Treeview.Heading",
        background=[("active", "#474747")])


# ==============================================================================
# DIALOGS
# ==============================================================================

class RenameDialog(ctk.CTkToplevel):
    def __init__(self, master, current_label):
        super().__init__(master)
        self.title("Rename Tape")
        self.geometry("380x160")
        self.resizable(False, False)
        self.result = None

        ctk.CTkLabel(self, text=f"Rename tape  '{current_label}'  to:").pack(padx=20, pady=(20, 6))
        self._var = ctk.StringVar()
        self._entry = ctk.CTkEntry(self, textvariable=self._var, width=300)
        self._entry.pack(padx=20, pady=6)
        self._entry.focus_set()

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(padx=20, pady=(10, 20), fill="x")
        ctk.CTkButton(btn_row, text="Rename", width=120,
                      command=self._confirm).pack(side="right", padx=(6, 0))
        ctk.CTkButton(btn_row, text="Cancel", width=120,
                      fg_color="#444", hover_color="#555",
                      command=self.destroy).pack(side="right")

        self._entry.bind("<Return>", lambda _: self._confirm())
        self._entry.bind("<Escape>", lambda _: self.destroy())
        self.grab_set()
        self.focus_set()

    def _confirm(self):
        val = self._var.get().strip()
        if val:
            self.result = val
        self.destroy()


class SetCapacityDialog(ctk.CTkToplevel):
    def __init__(self, master, label):
        super().__init__(master)
        self.title("Set Tape Capacity")
        self.geometry("380x185")
        self.resizable(False, False)
        self.result = None

        ctk.CTkLabel(self, text=f"Set capacity for  '{label}'  (GB):").pack(padx=20, pady=(20, 6))
        self._var = ctk.StringVar()
        self._entry = ctk.CTkEntry(self, textvariable=self._var, width=300,
                                   placeholder_text="e.g. 12000")
        self._entry.pack(padx=20, pady=6)
        self._entry.focus_set()

        self._err_var = ctk.StringVar()
        ctk.CTkLabel(self, textvariable=self._err_var,
                     text_color="#ff6b6b").pack(padx=20)

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(padx=20, pady=(6, 20), fill="x")
        ctk.CTkButton(btn_row, text="Set", width=120,
                      command=self._confirm).pack(side="right", padx=(6, 0))
        ctk.CTkButton(btn_row, text="Cancel", width=120,
                      fg_color="#444", hover_color="#555",
                      command=self.destroy).pack(side="right")

        self._entry.bind("<Return>", lambda _: self._confirm())
        self._entry.bind("<Escape>", lambda _: self.destroy())
        self.grab_set()
        self.focus_set()

    def _confirm(self):
        try:
            val = float(self._var.get().strip())
            if val <= 0:
                raise ValueError
            self.result = val
            self.destroy()
        except ValueError:
            self._err_var.set("Enter a positive number.")


class FileDetailDialog(ctk.CTkToplevel):
    def __init__(self, master, rec):
        super().__init__(master)
        self.title(f"File Details — {rec['file_name']}")
        self.geometry("700x490")
        self.resizable(True, True)

        scroll = ctk.CTkScrollableFrame(self)
        scroll.pack(fill="both", expand=True, padx=10, pady=10)

        fields = [
            ("file_id",           "ID"),
            ("file_name",         "Name"),
            ("original_path",     "Original Path"),
            ("file_size_bytes",   "Size"),
            ("file_hash",         "SHA-256 Hash"),
            ("backup_date",       "Backup Date"),
            ("tape_label",        "Tape"),
            ("is_packed",         "Packed"),
            ("container_name",    "Container"),
            ("stored_path",       "Stored Path"),
            ("local_session_id",  "Session ID"),
            ("local_chunk_index", "Chunk Index"),
        ]

        for i, (col, label) in enumerate(fields):
            ctk.CTkLabel(scroll, text=label + ":", anchor="e", width=140,
                         font=ctk.CTkFont(weight="bold")).grid(
                row=i, column=0, padx=(8, 4), pady=3, sticky="e")

            val = rec[col]
            if col == "file_hash":
                display = str(val) if val else "—"
                lbl = ctk.CTkLabel(scroll, text=display, anchor="w",
                                   font=("Courier New", 11))
            elif col == "file_size_bytes" and val is not None:
                lbl = ctk.CTkLabel(scroll,
                                   text=f"{val:,}  ({_fmt_bytes(val)})",
                                   anchor="w")
            elif col == "is_packed":
                lbl = ctk.CTkLabel(scroll,
                                   text="yes" if val else "no", anchor="w")
            else:
                lbl = ctk.CTkLabel(scroll,
                                   text=str(val) if val is not None else "—",
                                   anchor="w", wraplength=500, justify="left")
            lbl.grid(row=i, column=1, padx=(4, 8), pady=3, sticky="w")

        ctk.CTkButton(self, text="Close", width=100,
                      command=self.destroy).pack(pady=(0, 12))
        self.grab_set()
        self.focus_set()


# ==============================================================================
# TOP BAR
# ==============================================================================

class TopBar(ctk.CTkFrame):
    def __init__(self, master, db_path, on_refresh):
        super().__init__(master, height=44,
                         fg_color=("#1c1c1c", "#1c1c1c"), corner_radius=0)
        self.pack_propagate(False)

        ctk.CTkLabel(self, text="Database:",
                     font=ctk.CTkFont(weight="bold")).pack(
            side="left", padx=(14, 4))
        display = db_path if len(db_path) <= 90 else "…" + db_path[-87:]
        ctk.CTkLabel(self, text=display,
                     text_color="#a0a0a0").pack(side="left", padx=(0, 8))
        ctk.CTkButton(self, text="⟳  Refresh", width=110, height=30,
                      command=on_refresh).pack(side="right", padx=14)


# ==============================================================================
# TAPES PANEL
# ==============================================================================

class TapesPanel(ctk.CTkFrame):
    def __init__(self, master, db, app):
        super().__init__(master, fg_color="transparent")
        self._db  = db
        self._app = app
        self._selected_label = None

        # Toolbar
        tb = ctk.CTkFrame(self, fg_color="transparent")
        tb.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))

        _d = dict(width=148, height=30, state="disabled")
        self._btn_rename   = ctk.CTkButton(tb, text="Rename",
                                           command=self._on_rename, **_d)
        self._btn_capacity = ctk.CTkButton(tb, text="Set Capacity",
                                           command=self._on_capacity, **_d)
        self._btn_recalc   = ctk.CTkButton(tb, text="Recalculate Used",
                                           command=self._on_recalc, **_d)
        self._btn_wipe     = ctk.CTkButton(tb, text="Wipe File Records",
                                           command=self._on_wipe,
                                           fg_color="#6b3030",
                                           hover_color="#7a3838", **_d)
        self._btn_delete   = ctk.CTkButton(tb, text="Delete Tape",
                                           command=self._on_delete_tape,
                                           fg_color="#7a1c1c",
                                           hover_color="#8f2020", **_d)

        for btn in (self._btn_rename, self._btn_capacity, self._btn_recalc,
                    self._btn_wipe, self._btn_delete):
            btn.pack(side="left", padx=(0, 6))

        # Treeview
        tf = ctk.CTkFrame(self, fg_color="#212121")
        tf.grid(row=1, column=0, sticky="nsew", padx=8, pady=(4, 8))
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

        cols = ("tape_id", "volume_label", "date_init",
                "capacity_gb", "used_gb", "pct_bar", "file_count")
        self._tree = ttk.Treeview(tf, columns=cols, show="headings",
                                  selectmode="browse")

        headings = {
            "tape_id":      ("ID",          55,  "center"),
            "volume_label": ("Volume Label", 180, "w"),
            "date_init":    ("Initialized",  155, "w"),
            "capacity_gb":  ("Cap (GB)",      90, "e"),
            "used_gb":      ("Used (GB)",     90, "e"),
            "pct_bar":      ("Space Used",   230, "w"),
            "file_count":   ("Files",         65, "center"),
        }
        for col, (heading, width, anchor) in headings.items():
            self._tree.heading(col, text=heading)
            self._tree.column(col, width=width, anchor=anchor,
                              stretch=(col == "pct_bar"))

        self._tree.tag_configure("odd",  background="#212121")
        self._tree.tag_configure("even", background="#292929")

        vsb = ctk.CTkScrollbar(tf, command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        self._tree.bind("<<TreeviewSelect>>", self._on_select)

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.refresh()

    # ------------------------------------------------------------------

    def refresh(self):
        tapes = self._db.list_tapes()
        self._tree.delete(*self._tree.get_children())
        for i, t in enumerate(tapes):
            label   = t["volume_label"]
            cap_gb  = t["total_capacity"]
            used_b  = t["used_space"] or 0
            used_gb = used_b / 1024**3
            date_s  = (t["date_formatted"] or "")[:19]
            count   = self._db.count_tape_file_records(label)

            if cap_gb:
                pct   = min(used_gb / cap_gb, 1.0)
                pct_s = f"{pct * 100:5.1f}%  {_bar_str(pct)}"
                cap_s = f"{cap_gb:,.0f}"
            else:
                pct_s = f"  —      {_bar_str(None)}"
                cap_s = "—"

            used_s = f"{used_gb:.2f}"
            tag = "odd" if i % 2 == 0 else "even"
            self._tree.insert("", "end", iid=label, values=(
                t["tape_id"], label, date_s, cap_s, used_s, pct_s, count
            ), tags=(tag,))

        self._selected_label = None
        self._update_toolbar()

    def _on_select(self, _event=None):
        sel = self._tree.selection()
        self._selected_label = sel[0] if sel else None
        self._update_toolbar()

    def _update_toolbar(self):
        state = "normal" if self._selected_label else "disabled"
        for btn in (self._btn_rename, self._btn_capacity, self._btn_recalc,
                    self._btn_wipe, self._btn_delete):
            btn.configure(state=state)

    def _tape_labels_from_db(self):
        return [t["volume_label"] for t in self._db.list_tapes()]

    def _show_db_error_and_refresh(self, exc):
        messagebox.showerror(
            "Database Changed",
            f"{exc}\n\nThe database view will refresh.",
            parent=self.winfo_toplevel())
        self._app.refresh_all()

    # ------------------------------------------------------------------

    def _on_rename(self):
        label = self._selected_label
        if not label:
            return
        dlg = RenameDialog(self, label)
        self.wait_window(dlg)
        new_label = dlg.result
        if not new_label:
            return
        if new_label == label:
            return
        if self._db.tape_exists(new_label):
            messagebox.showerror("Error",
                                 f"Label '{new_label}' already exists.",
                                 parent=self.winfo_toplevel())
            return
        try:
            self._db.rename_tape(label, new_label)
        except Exception as e:
            self._show_db_error_and_refresh(e)
            return
        self.refresh()
        self._app.files_panel.refresh(self._tape_labels_from_db())

    def _on_capacity(self):
        label = self._selected_label
        if not label:
            return
        dlg = SetCapacityDialog(self, label)
        self.wait_window(dlg)
        if dlg.result is None:
            return
        try:
            self._db.update_tape_capacity(label, dlg.result)
        except Exception as e:
            self._show_db_error_and_refresh(e)
            return
        self.refresh()

    def _on_recalc(self):
        label = self._selected_label
        if not label:
            return
        if not messagebox.askyesno(
                "Confirm",
                f"Recalculate used space for '{label}'?",
                parent=self.winfo_toplevel()):
            return
        try:
            new_bytes = self._db.recalculate_tape_used_space(label)
        except Exception as e:
            self._show_db_error_and_refresh(e)
            return
        messagebox.showinfo(
            "Done",
            f"Used space for '{label}' updated to {new_bytes / 1024**3:.3f} GB.",
            parent=self.winfo_toplevel())
        self.refresh()

    def _on_wipe(self):
        label = self._selected_label
        if not label:
            return
        count = self._db.count_tape_file_records(label)
        if not messagebox.askyesno(
                "Wipe File Records",
                f"Wipe {count} file record(s) for '{label}'?\n\n"
                f"The tape entry will be kept. This cannot be undone.",
                parent=self.winfo_toplevel()):
            return
        try:
            self._db.delete_files_for_tape(label)
        except Exception as e:
            self._show_db_error_and_refresh(e)
            return
        self.refresh()
        self._app.files_panel.refresh()

    def _on_delete_tape(self):
        label = self._selected_label
        if not label:
            return
        count = self._db.count_tape_file_records(label)
        if not messagebox.askyesno(
                "Delete Tape",
                f"Permanently delete tape '{label}' and "
                f"{count} file record(s)?\n\nThis cannot be undone.",
                parent=self.winfo_toplevel()):
            return
        try:
            self._db.delete_tape(label)
        except Exception as e:
            self._show_db_error_and_refresh(e)
            return
        self.refresh()
        self._app.files_panel.refresh(self._tape_labels_from_db())


# ==============================================================================
# FILES PANEL
# ==============================================================================

class FilesPanel(ctk.CTkFrame):
    def __init__(self, master, db, tape_labels):
        super().__init__(master, fg_color="transparent")
        self._db = db
        self._tape_labels = list(tape_labels)

        # Filter bar
        fb = ctk.CTkFrame(self, fg_color="transparent")
        fb.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))

        ctk.CTkLabel(fb, text="Name:").grid(row=0, column=0,
                                             padx=(0, 4), sticky="w")
        self._name_var = ctk.StringVar()
        ctk.CTkEntry(fb, textvariable=self._name_var, width=200,
                     placeholder_text="search…").grid(row=0, column=1,
                                                       padx=(0, 12))

        ctk.CTkLabel(fb, text="Tape:").grid(row=0, column=2, padx=(0, 4))
        self._tape_var = ctk.StringVar(value="All")
        self._tape_menu = ctk.CTkOptionMenu(
            fb, variable=self._tape_var,
            values=["All"] + self._tape_labels, width=150)
        self._tape_menu.grid(row=0, column=3, padx=(0, 12))

        ctk.CTkLabel(fb, text="From:").grid(row=0, column=4, padx=(0, 4))
        self._date_from_var = ctk.StringVar()
        ctk.CTkEntry(fb, textvariable=self._date_from_var, width=105,
                     placeholder_text="YYYY-MM-DD").grid(row=0, column=5,
                                                          padx=(0, 8))

        ctk.CTkLabel(fb, text="To:").grid(row=0, column=6, padx=(0, 4))
        self._date_to_var = ctk.StringVar()
        ctk.CTkEntry(fb, textvariable=self._date_to_var, width=105,
                     placeholder_text="YYYY-MM-DD").grid(row=0, column=7,
                                                          padx=(0, 12))

        ctk.CTkButton(fb, text="Search", width=90, height=30,
                      command=self._on_search).grid(row=0, column=8,
                                                     padx=(0, 6))
        ctk.CTkButton(fb, text="Clear", width=70, height=30,
                      fg_color="#444", hover_color="#555",
                      command=self._on_clear).grid(row=0, column=9)

        self._status_var = ctk.StringVar(value="")
        ctk.CTkLabel(fb, textvariable=self._status_var,
                     text_color="#a0a0a0").grid(
            row=1, column=0, columnspan=10, sticky="w", pady=(4, 0))

        # Treeview
        tf = ctk.CTkFrame(self, fg_color="#212121")
        tf.grid(row=1, column=0, sticky="nsew", padx=8, pady=(4, 4))
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

        cols = ("file_id", "file_name", "original_path",
                "size", "tape_label", "backup_date", "is_packed")
        self._tree = ttk.Treeview(tf, columns=cols, show="headings",
                                  selectmode="extended")

        headings = {
            "file_id":       ("ID",           60,  "center"),
            "file_name":     ("File Name",    220, "w"),
            "original_path": ("Original Path", 340, "w"),
            "size":          ("Size",          90,  "e"),
            "tape_label":    ("Tape",          90,  "center"),
            "backup_date":   ("Backup Date",  150,  "w"),
            "is_packed":     ("Packed",        60,  "center"),
        }
        for col, (heading, width, anchor) in headings.items():
            self._tree.heading(col, text=heading)
            self._tree.column(col, width=width, anchor=anchor,
                              stretch=(col == "original_path"))

        self._tree.tag_configure("odd",  background="#212121")
        self._tree.tag_configure("even", background="#292929")

        vsb = ctk.CTkScrollbar(tf, command=self._tree.yview)
        hsb = ctk.CTkScrollbar(tf, orientation="horizontal",
                                command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set,
                             xscrollcommand=hsb.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self._tree.bind("<<TreeviewSelect>>", self._on_select)
        self._tree.bind("<Double-Button-1>",
                        lambda _: self._on_view_details())

        # Toolbar
        tb2 = ctk.CTkFrame(self, fg_color="transparent")
        tb2.grid(row=2, column=0, sticky="ew", padx=8, pady=(4, 8))

        self._btn_delete  = ctk.CTkButton(
            tb2, text="Delete Selected", width=145, height=30,
            state="disabled", fg_color="#7a1c1c", hover_color="#8f2020",
            command=self._on_delete)
        self._btn_details = ctk.CTkButton(
            tb2, text="View Details", width=120, height=30,
            state="disabled", command=self._on_view_details)

        self._btn_delete.pack(side="left", padx=(0, 8))
        self._btn_details.pack(side="left")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self._on_search()

    # ------------------------------------------------------------------

    def _on_select(self, _event=None):
        sel = self._tree.selection()
        self._btn_delete.configure(
            state="normal" if sel else "disabled")
        self._btn_details.configure(
            state="normal" if len(sel) == 1 else "disabled")

    def _build_query(self):
        name      = self._name_var.get().strip()
        tape      = self._tape_var.get()
        date_from = self._date_from_var.get().strip()
        date_to   = self._date_to_var.get().strip()

        sql    = "SELECT * FROM files_index WHERE 1=1"
        params = []
        if name:
            sql += " AND file_name LIKE ?"
            params.append(f"%{name}%")
        if tape and tape != "All":
            sql += " AND tape_label = ?"
            params.append(tape)
        if date_from:
            sql += " AND backup_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND backup_date <= ?"
            params.append(date_to + " 23:59:59")
        sql += " ORDER BY backup_date DESC"
        return sql, params

    def _on_search(self):
        sql, params = self._build_query()
        rows  = self._db.conn.execute(sql, params).fetchall()
        MAX   = 2000
        self._populate(rows[:MAX])
        total = len(rows)
        shown = min(total, MAX)
        if total == 0:
            self._status_var.set("No records found.")
        elif total > MAX:
            self._status_var.set(
                f"Showing first {shown:,} of {total:,} records"
                f" — refine filters to see more")
        else:
            self._status_var.set(
                f"Showing {shown:,} record{'s' if shown != 1 else ''}")

    def _on_clear(self):
        self._name_var.set("")
        self._tape_var.set("All")
        self._date_from_var.set("")
        self._date_to_var.set("")
        self._on_search()

    def _populate(self, rows):
        self._tree.delete(*self._tree.get_children())
        for i, r in enumerate(rows):
            size_s = _fmt_bytes(r["file_size_bytes"])
            packed = "yes" if r["is_packed"] else "no"
            date_s = (r["backup_date"] or "")[:19]
            tag    = "odd" if i % 2 == 0 else "even"
            self._tree.insert("", "end", iid=str(r["file_id"]), values=(
                r["file_id"], r["file_name"], r["original_path"],
                size_s, r["tape_label"], date_s, packed
            ), tags=(tag,))

    def _on_delete(self):
        sel = self._tree.selection()
        if not sel:
            return
        if not messagebox.askyesno(
                "Confirm Delete",
                f"Delete {len(sel)} file record(s)? This cannot be undone.",
                parent=self.winfo_toplevel()):
            return
        missing = []
        for iid in sel:
            try:
                self._db.delete_file(int(iid))
            except RuntimeError as e:
                missing.append(str(e))
        self._on_search()
        if missing:
            messagebox.showwarning(
                "Database Changed",
                "Some selected record(s) were no longer present.\n\n"
                + "\n".join(missing[:5]),
                parent=self.winfo_toplevel())

    def _on_view_details(self):
        sel = self._tree.selection()
        if not sel:
            return
        rec = self._db.get_file_by_id(int(sel[0]))
        if rec:
            FileDetailDialog(self, rec)
        else:
            self._on_search()
            messagebox.showinfo(
                "Database Changed",
                "The selected file record is no longer present.",
                parent=self.winfo_toplevel())

    def refresh(self, tape_labels=None):
        if tape_labels is not None:
            self._tape_labels = list(tape_labels)
            self._tape_menu.configure(
                values=["All"] + self._tape_labels)
            if self._tape_var.get() not in (["All"] + self._tape_labels):
                self._tape_var.set("All")
        self._on_search()


# ==============================================================================
# SESSIONS / MANIFESTS PANEL
# ==============================================================================

class SessionsPanel(ctk.CTkFrame):
    def __init__(self, master, db, app):
        super().__init__(master, fg_color="transparent")
        self._db = db
        self._app = app

        tb = ctk.CTkFrame(self, fg_color="transparent")
        tb.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))

        self._btn_delete = ctk.CTkButton(
            tb, text="Delete Selected Session", width=190, height=30,
            state="disabled", fg_color="#7a1c1c", hover_color="#8f2020",
            command=self._on_delete)
        self._btn_vacuum = ctk.CTkButton(
            tb, text="Compact DB", width=120, height=30,
            command=self._on_vacuum)
        self._btn_delete.pack(side="left", padx=(0, 8))
        self._btn_vacuum.pack(side="left")

        self._status_var = ctk.StringVar(value="")
        ctk.CTkLabel(tb, textvariable=self._status_var,
                     text_color="#a0a0a0").pack(side="left", padx=(12, 0))

        tf = ctk.CTkFrame(self, fg_color="#212121")
        tf.grid(row=1, column=0, sticky="nsew", padx=8, pady=(4, 8))
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

        cols = ("kind", "session_id", "label", "status", "mode",
                "created", "completed", "chunks", "manifest_rows",
                "manifest_size", "file_records")
        self._tree = ttk.Treeview(tf, columns=cols, show="headings",
                                  selectmode="browse")

        headings = {
            "kind":          ("Type",           80,  "center"),
            "session_id":    ("ID",             60,  "center"),
            "label":         ("Session Label",  260, "w"),
            "status":        ("Status",         95,  "center"),
            "mode":          ("Mode",           85,  "center"),
            "created":       ("Created",        155, "w"),
            "completed":     ("Completed",      155, "w"),
            "chunks":        ("Chunks",         70,  "center"),
            "manifest_rows": ("Manifest Rows",  120, "e"),
            "manifest_size": ("Manifest Size",  120, "e"),
            "file_records":  ("File Records",   105, "e"),
        }
        for col, (heading, width, anchor) in headings.items():
            self._tree.heading(col, text=heading)
            self._tree.column(col, width=width, anchor=anchor,
                              stretch=(col == "label"))

        self._tree.tag_configure("odd",  background="#212121")
        self._tree.tag_configure("even", background="#292929")

        vsb = ctk.CTkScrollbar(tf, command=self._tree.yview)
        hsb = ctk.CTkScrollbar(tf, orientation="horizontal",
                                command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set,
                             xscrollcommand=hsb.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self._tree.bind("<<TreeviewSelect>>", self._on_select)

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.refresh()

    def _table_exists(self, name):
        return self._db.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (name,)
        ).fetchone() is not None

    def _load_rows(self):
        rows = []
        if self._table_exists("local_sessions"):
            local_rows = self._db.conn.execute("""
                SELECT
                    'local' AS kind,
                    s.session_id,
                    s.session_label,
                    s.status,
                    COALESCE(s.backup_mode, 'auto') AS mode,
                    s.created_at,
                    s.completed_at,
                    s.total_chunks AS chunks,
                    COALESCE((
                        SELECT COUNT(*) FROM local_chunks_manifest m
                        WHERE m.session_id = s.session_id
                    ), 0) AS manifest_rows,
                    COALESCE((
                        SELECT SUM(dir_size_bytes) FROM local_chunks_manifest m
                        WHERE m.session_id = s.session_id
                    ), 0) AS manifest_bytes,
                    COALESCE((
                        SELECT COUNT(*) FROM files_index f
                        WHERE f.local_session_id = s.session_id
                    ), 0) AS file_records
                FROM local_sessions s
                ORDER BY s.session_id
            """).fetchall()
            rows.extend(dict(r) for r in local_rows)

        if self._table_exists("remote_sessions"):
            remote_rows = self._db.conn.execute("""
                SELECT
                    'remote' AS kind,
                    s.session_id,
                    s.session_label,
                    s.status,
                    '' AS mode,
                    s.created_at,
                    s.completed_at,
                    s.chunk_count AS chunks,
                    COALESCE((
                        SELECT COUNT(*) FROM remote_manifest m
                        WHERE m.session_id = s.session_id
                    ), 0) AS manifest_rows,
                    COALESCE((
                        SELECT SUM(file_size_bytes) FROM remote_manifest m
                        WHERE m.session_id = s.session_id
                    ), 0) AS manifest_bytes,
                    0 AS file_records
                FROM remote_sessions s
                ORDER BY s.session_id
            """).fetchall()
            rows.extend(dict(r) for r in remote_rows)

        return sorted(rows, key=lambda r: (str(r["kind"]), int(r["session_id"])))

    def refresh(self):
        rows = self._load_rows()
        self._tree.delete(*self._tree.get_children())

        total_manifest_rows = 0
        total_manifest_bytes = 0
        for i, r in enumerate(rows):
            total_manifest_rows += r["manifest_rows"] or 0
            total_manifest_bytes += r["manifest_bytes"] or 0
            tag = "odd" if i % 2 == 0 else "even"
            iid = f"{r['kind']}:{r['session_id']}"
            self._tree.insert("", "end", iid=iid, values=(
                r["kind"],
                r["session_id"],
                r["session_label"],
                r["status"],
                r["mode"] or "",
                (r["created_at"] or "")[:19],
                (r["completed_at"] or "")[:19],
                r["chunks"] if r["chunks"] is not None else "",
                f"{r['manifest_rows']:,}",
                _fmt_bytes(r["manifest_bytes"] or 0),
                f"{r['file_records']:,}",
            ), tags=(tag,))

        self._status_var.set(
            f"{len(rows):,} session(s), {total_manifest_rows:,} manifest row(s), "
            f"{_fmt_bytes(total_manifest_bytes)} manifest data")
        self._btn_delete.configure(state="disabled")

    def _on_select(self, _event=None):
        self._btn_delete.configure(
            state="normal" if self._tree.selection() else "disabled")

    def _selected_session(self):
        sel = self._tree.selection()
        if not sel:
            return None, None
        kind, session_id = sel[0].split(":", 1)
        return kind, int(session_id)

    def _session_summary(self, kind, session_id):
        manifest_table = "local_chunks_manifest" if kind == "local" else "remote_manifest"
        session_table = "local_sessions" if kind == "local" else "remote_sessions"
        label_row = self._db.conn.execute(
            f"SELECT session_label, status FROM {session_table} WHERE session_id = ?",
            (session_id,)
        ).fetchone()
        count = self._db.conn.execute(
            f"SELECT COUNT(*) FROM {manifest_table} WHERE session_id = ?",
            (session_id,)
        ).fetchone()[0]
        return label_row, count

    def _delete_session(self, kind, session_id):
        manifest_table = "local_chunks_manifest" if kind == "local" else "remote_manifest"
        session_table = "local_sessions" if kind == "local" else "remote_sessions"
        with self._db.lock:
            with self._db.conn:
                self._db.conn.execute(
                    f"DELETE FROM {manifest_table} WHERE session_id = ?",
                    (session_id,)
                )
                cur = self._db.conn.execute(
                    f"DELETE FROM {session_table} WHERE session_id = ?",
                    (session_id,)
                )
                self._db._require_updated(
                    cur, f"[DB] {kind.title()} session not found: {session_id}")

    def _on_delete(self):
        kind, session_id = self._selected_session()
        if not kind:
            return
        label_row, manifest_count = self._session_summary(kind, session_id)
        if not label_row:
            self.refresh()
            return
        label, status = label_row["session_label"], label_row["status"]
        if not messagebox.askyesno(
                "Delete Session",
                f"Delete {kind} session '{label}' (id {session_id})?\n\n"
                f"Status: {status}\n"
                f"Manifest rows to delete: {manifest_count:,}\n\n"
                "Tape records and file index records will be kept. "
                "Deleting an active session removes its resume state.",
                parent=self.winfo_toplevel()):
            return
        try:
            self._delete_session(kind, session_id)
        except Exception as e:
            messagebox.showerror(
                "Delete Failed", str(e), parent=self.winfo_toplevel())
            self.refresh()
            return
        self.refresh()

    def _on_vacuum(self):
        if not messagebox.askyesno(
                "Compact Database",
                "Run SQLite VACUUM to shrink the database file?\n\n"
                "This can take a little while on large databases.",
                parent=self.winfo_toplevel()):
            return
        try:
            self._db.conn.execute("VACUUM")
        except Exception as e:
            messagebox.showerror(
                "Compact Failed", str(e), parent=self.winfo_toplevel())
            return
        messagebox.showinfo(
            "Done", "Database compacted.", parent=self.winfo_toplevel())
        self.refresh()


# ==============================================================================
# MAIN APP
# ==============================================================================

class DBInspectorApp(ctk.CTk):
    def __init__(self, db, db_path):
        super().__init__()
        _prepare_database_connection(db)
        self._db = db

        self.title("LTO Archive — Database Inspector")
        self.geometry("1200x750")
        self.minsize(900, 550)

        style = ttk.Style(self)
        _apply_treeview_dark_style(style)

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        TopBar(self, db_path, on_refresh=self.refresh_all).grid(
            row=0, column=0, sticky="ew")

        tabview = ctk.CTkTabview(self)
        tabview.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        tabview.add("Tapes")
        tabview.add("Files")
        tabview.add("Sessions")

        tape_labels = [t["volume_label"] for t in db.list_tapes()]

        self.tapes_panel = TapesPanel(tabview.tab("Tapes"), db, self)
        self.tapes_panel.pack(fill="both", expand=True)

        self.files_panel = FilesPanel(tabview.tab("Files"), db, tape_labels)
        self.files_panel.pack(fill="both", expand=True)

        self.sessions_panel = SessionsPanel(tabview.tab("Sessions"), db, self)
        self.sessions_panel.pack(fill="both", expand=True)

    def refresh_all(self):
        self.tapes_panel.refresh()
        tape_labels = [t["volume_label"] for t in self._db.list_tapes()]
        self.files_panel.refresh(tape_labels)
        self.sessions_panel.refresh()


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    from lto_archive_manager import ConfigManager, DatabaseManager

    cfg = ConfigManager()
    db  = DatabaseManager(cfg.db_path)

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("dark-blue")
    ctk.set_widget_scaling(1.25)

    app = DBInspectorApp(db, cfg.db_path)
    try:
        app.mainloop()
    finally:
        db.close()
