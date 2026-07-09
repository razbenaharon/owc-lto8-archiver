"""PySide6 lazy PostgreSQL archive inspector."""
import os
import subprocess
import sys
from typing import Any, Dict

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "src"

from PySide6.QtCore import (
    QAbstractItemModel,
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QRunnable,
    QThreadPool,
    Qt,
    Signal,
    Slot,
)
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableView,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

from .db import _fmt_ts
from .inspector_repository import InspectorRepository


PAGE_SIZE = 250


def _fmt_bytes(value):
    if value is None:
        return "-"
    n = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or unit == "PiB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


class WorkerSignals(QObject):
    finished = Signal(object, object)
    failed = Signal(object, str)


class RepositoryWorker(QRunnable):
    def __init__(self, db_path, token, function):
        super().__init__()
        self.db_path = db_path
        self.token = token
        self.function = function
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        try:
            with InspectorRepository(self.db_path) as repo:
                result = self.function(repo)
        except Exception as exc:
            self.signals.failed.emit(self.token, str(exc))
            return
        self.signals.finished.emit(self.token, result)


class _DbTaskSignals(QObject):
    finished = Signal(object)
    failed = Signal(str)


class DbWriteTask(QRunnable):
    """Run a blocking PgDatabaseManager write off the UI thread.

    Heavy maintenance ops (rename_tape rebuilds every record_key of a tape;
    cleanup runs VACUUM) used to freeze the event loop for minutes at catalog
    scale. The manager is connection-pool backed, so it is thread-safe here.
    """

    def __init__(self, fn):
        super().__init__()
        self.fn = fn
        self.signals = _DbTaskSignals()

    @Slot()
    def run(self):
        try:
            result = self.fn()
        except Exception as exc:
            self.signals.failed.emit(str(exc))
            return
        self.signals.finished.emit(result)


class TreeNode:
    def __init__(self, name, kind, parent=None, tape_label=None,
                 directory_id=None, normalized_path=None,
                 recursive_bytes=None, recursive_file_count=None):
        self.name = name
        self.kind = kind
        self.parent = parent
        self.tape_label = tape_label
        self.directory_id = directory_id
        self.normalized_path = normalized_path
        self.recursive_bytes = recursive_bytes
        self.recursive_file_count = recursive_file_count
        self.children = []
        self.cursor = None
        self.has_more = kind in ("tape", "dir")
        self.loading = False

    def row(self):
        if self.parent is None:
            return 0
        return self.parent.children.index(self)


class ArchiveTreeModel(QAbstractItemModel):
    load_error = Signal(str)

    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.root = TreeNode("root", "root")
        self.pool = QThreadPool.globalInstance()
        self._token = 0
        self._load_tapes()

    def _load_tapes(self):
        with InspectorRepository(self.db_path) as repo:
            tapes = repo.list_tapes()
        self.root.children = [
            TreeNode(t["volume_label"], "tape", self.root,
                     tape_label=t["volume_label"],
                     recursive_bytes=t["used_space"] or 0)
            for t in tapes
        ]

    def index(self, row, column, parent=QModelIndex()):
        if column not in (0, 1) or row < 0:
            return QModelIndex()
        parent_node = self.node_from_index(parent)
        if row >= len(parent_node.children):
            return QModelIndex()
        return self.createIndex(row, column, parent_node.children[row])

    def parent(self, index):
        if not index.isValid():
            return QModelIndex()
        node = index.internalPointer()
        parent = node.parent
        if parent is None or parent is self.root:
            return QModelIndex()
        return self.createIndex(parent.row(), 0, parent)

    def rowCount(self, parent=QModelIndex()):
        return len(self.node_from_index(parent).children)

    def columnCount(self, parent=QModelIndex()):
        return 2

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        node = index.internalPointer()
        column = index.column()
        if role == Qt.ItemDataRole.DisplayRole:
            if column == 0:
                return node.name
            if column == 1 and node.kind in ("tape", "dir"):
                return _fmt_bytes(node.recursive_bytes)
            return None
        if role == Qt.ItemDataRole.TextAlignmentRole and column == 1:
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        if role == Qt.ItemDataRole.ToolTipRole:
            if node.kind == "tape":
                return f"Tape: {node.tape_label}"
            if node.kind == "dir":
                path = node.normalized_path or ""
                if node.recursive_file_count is not None:
                    return (f"{path}\n{node.recursive_file_count:,} file(s), "
                            f"{_fmt_bytes(node.recursive_bytes)}")
                return path
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return "Size" if section == 1 else "Archive"
        return None

    def flags(self, index):
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        return Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable

    def hasChildren(self, parent=QModelIndex()):
        node = self.node_from_index(parent)
        return bool(node.children) or node.has_more

    def canFetchMore(self, parent):
        node = self.node_from_index(parent)
        return node.kind in ("tape", "dir") and node.has_more and not node.loading

    def fetchMore(self, parent):
        node = self.node_from_index(parent)
        if not self.canFetchMore(parent):
            return
        node.loading = True
        self._token += 1
        token = (self._token, node)

        def load(repo):
            if node.kind == "tape":
                page = repo.list_child_directories(
                    tape_label=node.tape_label, cursor=node.cursor,
                    limit=PAGE_SIZE)
            else:
                page = repo.list_child_directories(
                    parent_id=node.directory_id, cursor=node.cursor,
                    limit=PAGE_SIZE)
            sizes = repo.subtree_sizes(
                [row["directory_id"] for row in page["rows"]])
            for row in page["rows"]:
                totals = sizes.get(row["directory_id"], {})
                row["recursive_bytes"] = totals.get("recursive_bytes", 0)
                row["recursive_file_count"] = totals.get("recursive_file_count", 0)
            return page

        worker = RepositoryWorker(self.db_path, token, load)
        worker.signals.finished.connect(self._on_loaded)
        worker.signals.failed.connect(self._on_failed)
        self.pool.start(worker)

    def node_from_index(self, index):
        return index.internalPointer() if index.isValid() else self.root

    def index_for_node(self, node):
        if node is self.root or node.parent is None:
            return QModelIndex()
        return self.createIndex(node.row(), 0, node)

    @Slot(object, object)
    def _on_loaded(self, token, result):
        _token_id, node = token
        node.loading = False
        rows = result["rows"]
        if rows:
            parent_index = self.index_for_node(node)
            start = len(node.children)
            self.beginInsertRows(parent_index, start, start + len(rows) - 1)
            for row in rows:
                node.children.append(TreeNode(
                    row["name"], "dir", node,
                    tape_label=row["tape_label"],
                    directory_id=row["directory_id"],
                    normalized_path=row["normalized_path"],
                    recursive_bytes=row.get("recursive_bytes", 0),
                    recursive_file_count=row.get("recursive_file_count", 0)))
            self.endInsertRows()
        node.cursor = result["next_cursor"]
        node.has_more = bool(result["has_more"])

    @Slot(object, str)
    def _on_failed(self, token, message):
        _token_id, node = token
        node.loading = False
        node.has_more = False
        self.load_error.emit(message)


class FileTableModel(QAbstractTableModel):
    columns = [
        ("file_id", "ID"),
        ("file_name", "Name"),
        ("file_size_bytes", "Size"),
        ("tape_label", "Tape"),
        ("source_host", "Source Host"),
        ("backup_date", "Backup Date"),
        ("is_packed", "Packed"),
        ("original_path", "Original Path"),
        ("stored_path", "Stored Path"),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.rows = []

    def set_rows(self, rows):
        self.beginResetModel()
        self.rows = list(rows)
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return len(self.rows)

    def columnCount(self, parent=QModelIndex()):
        return len(self.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        row = self.rows[index.row()]
        key = self.columns[index.column()][0]
        value = row.get(key)
        if role == Qt.ItemDataRole.DisplayRole:
            if key == "file_size_bytes":
                return _fmt_bytes(value)
            if key == "is_packed":
                return "yes" if value else "no"
            if key == "backup_date":
                return _fmt_ts(value)
            return "" if value is None else str(value)
        if role == Qt.ItemDataRole.TextAlignmentRole and key in ("file_id", "file_size_bytes"):
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.columns[section][1]
        return None

    def file_ids(self, indexes):
        result = []
        for index in indexes:
            if index.isValid():
                result.append(self.rows[index.row()]["file_id"])
        return sorted(set(result))


class SimpleRowsTableModel(QAbstractTableModel):
    def __init__(self, columns, parent=None):
        super().__init__(parent)
        self.columns = list(columns)
        self.rows = []

    def set_rows(self, rows):
        self.beginResetModel()
        self.rows = list(rows or [])
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.rows)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
            return None
        key = self.columns[index.column()]
        value = self.rows[index.row()].get(key)
        if key.endswith("bytes") or key in (
                "byte_count", "direct_bytes", "recursive_bytes",
                "small_file_bytes", "large_file_bytes"):
            return _fmt_bytes(value)
        return "" if value is None else str(value)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.columns[section]
        return None


class FileDetailDialog(QDialog):
    def __init__(self, record, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"File Details - {record.get('file_name', '')}")
        self.resize(760, 520)
        layout = QVBoxLayout(self)
        text = QTextEdit(self)
        text.setReadOnly(True)
        lines = []
        for key in (
                "file_id", "file_name", "original_path", "file_size_bytes",
                "source_host", "backup_date", "tape_label", "is_packed",
                "container_name", "stored_path", "local_session_id",
                "local_chunk_index"):
            lines.append(f"{key}: {record.get(key)}")
        text.setPlainText("\n".join(lines))
        layout.addWidget(text)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, self)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)


class BrowseWidget(QWidget):
    def __init__(self, db, db_path, parent=None):
        super().__init__(parent)
        self.db = db
        self.db_path = db_path
        self.pool = QThreadPool.globalInstance()
        self.generation = 0
        self.current_directory_id = None
        self.current_sort = "name"
        self.cursor_stack = [None]
        self.page_index = 0
        self.has_more = False

        layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Horizontal, self)
        layout.addWidget(splitter, 1)

        self.tree = QTreeView(splitter)
        self.tree_model = ArchiveTreeModel(db_path, self)
        self.tree.setModel(self.tree_model)
        self.tree.setHeaderHidden(False)
        tree_header = self.tree.header()
        tree_header.setStretchLastSection(False)
        tree_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        tree_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.tree.selectionModel().selectionChanged.connect(self._on_tree_selection)
        self.tree_model.load_error.connect(self._show_error)
        splitter.addWidget(self.tree)

        right = QWidget(splitter)
        right_layout = QVBoxLayout(right)
        controls = QHBoxLayout()
        self.sort_combo = QComboBox()
        self.sort_combo.addItems(["name", "size", "date"])
        self.sort_combo.currentTextChanged.connect(self._sort_changed)
        self.prev_btn = QPushButton("Previous")
        self.next_btn = QPushButton("Next")
        self.details_btn = QPushButton("Details")
        self.delete_btn = QPushButton("Delete Selected")
        self.refresh_btn = QPushButton("Refresh")
        for widget in (
                QLabel("Sort:"), self.sort_combo, self.prev_btn, self.next_btn,
                self.details_btn, self.delete_btn, self.refresh_btn):
            controls.addWidget(widget)
        controls.addStretch(1)
        right_layout.addLayout(controls)

        self.status = QLabel("Select a directory.")
        right_layout.addWidget(self.status)

        self.table = QTableView()
        self.table_model = FileTableModel(self)
        self.table.setModel(self.table_model)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.doubleClicked.connect(lambda _idx: self.show_details())
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._context_menu)
        right_layout.addWidget(self.table, 1)
        splitter.addWidget(right)
        splitter.setSizes([330, 970])

        self.prev_btn.clicked.connect(self.previous_page)
        self.next_btn.clicked.connect(self.next_page)
        self.details_btn.clicked.connect(self.show_details)
        self.delete_btn.clicked.connect(self.delete_selected)
        self.refresh_btn.clicked.connect(self.reload_page)
        self._update_buttons()

    def _selected_node(self):
        indexes = self.tree.selectionModel().selectedRows()
        if not indexes:
            return None
        return indexes[0].internalPointer()

    def _on_tree_selection(self):
        node = self._selected_node()
        if not node or node.kind != "dir":
            self.current_directory_id = None
            self.table_model.set_rows([])
            self.status.setText("Select a directory.")
            self._update_buttons()
            return
        self.current_directory_id = node.directory_id
        self.cursor_stack = [None]
        self.page_index = 0
        self.load_page(None)

    def _sort_changed(self, value):
        self.current_sort = value
        self.cursor_stack = [None]
        self.page_index = 0
        self.load_page(None)

    def load_page(self, cursor):
        if self.current_directory_id is None:
            return
        self.generation += 1
        generation = self.generation
        directory_id = self.current_directory_id
        sort = self.current_sort
        self.status.setText("Loading...")

        worker = RepositoryWorker(
            self.db_path, generation,
            lambda repo: repo.list_directory_files(
                directory_id, sort=sort, cursor=cursor, limit=PAGE_SIZE))
        worker.signals.finished.connect(self._on_files_loaded)
        worker.signals.failed.connect(lambda _token, msg: self._show_error(msg))
        self.pool.start(worker)

    @Slot(object, object)
    def _on_files_loaded(self, generation, result):
        if generation != self.generation:
            return
        self.table_model.set_rows(result["rows"])
        self.has_more = bool(result["has_more"])
        if self.has_more:
            if len(self.cursor_stack) == self.page_index + 1:
                self.cursor_stack.append(result["next_cursor"])
            else:
                self.cursor_stack[self.page_index + 1] = result["next_cursor"]
        self.status.setText(
            f"{len(result['rows']):,} row(s) on page {self.page_index + 1}")
        self.table.resizeColumnsToContents()
        self._update_buttons()

    def _update_buttons(self):
        has_rows = bool(self.table_model.rows)
        self.prev_btn.setEnabled(self.page_index > 0)
        self.next_btn.setEnabled(self.has_more)
        self.details_btn.setEnabled(has_rows)
        self.delete_btn.setEnabled(has_rows)

    def previous_page(self):
        if self.page_index <= 0:
            return
        self.page_index -= 1
        self.load_page(self.cursor_stack[self.page_index])

    def next_page(self):
        if not self.has_more:
            return
        self.page_index += 1
        self.load_page(self.cursor_stack[self.page_index])

    def reload_page(self):
        if self.current_directory_id is not None:
            self.load_page(self.cursor_stack[self.page_index])

    def selected_file_ids(self):
        return self.table_model.file_ids(self.table.selectionModel().selectedRows())

    def show_details(self):
        ids = self.selected_file_ids()
        if not ids:
            return
        record = self.db.get_file_by_id(ids[0])
        if not record:
            self.reload_page()
            return
        FileDetailDialog(record, self).exec()

    def delete_selected(self):
        ids = self.selected_file_ids()
        if not ids:
            return
        if QMessageBox.question(
                self, "Delete File Records",
                f"Delete {len(ids)} file record(s)? This cannot be undone."
        ) != QMessageBox.StandardButton.Yes:
            return
        # One transaction (also reconciles tapes.used_space) instead of one
        # commit per record.
        self.db.delete_files(ids)
        self.reload_page()

    def _context_menu(self, pos):
        menu = QMenu(self)
        menu.addAction("Details", self.show_details)
        menu.addAction("Delete Selected", self.delete_selected)
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def _show_error(self, message):
        QMessageBox.critical(self, "Database Error", message)


class SearchWidget(QWidget):
    def __init__(self, db, db_path, parent=None):
        super().__init__(parent)
        self.db = db
        self.db_path = db_path
        self.pool = QThreadPool.globalInstance()
        self.generation = 0
        self.cursor_stack = [None]
        self.page_index = 0
        self.has_more = False

        layout = QVBoxLayout(self)
        controls = QHBoxLayout()
        self.query = QLineEdit()
        self.query.setPlaceholderText("FTS search")
        self.tape_combo = QComboBox()
        self.source_combo = QComboBox()
        self.search_btn = QPushButton("Search")
        self.prev_btn = QPushButton("Previous")
        self.next_btn = QPushButton("Next")
        self.details_btn = QPushButton("Details")
        self.delete_btn = QPushButton("Delete Selected")
        for widget in (
                QLabel("Query:"), self.query, QLabel("Tape:"), self.tape_combo,
                QLabel("Source:"), self.source_combo,
                self.search_btn, self.prev_btn, self.next_btn,
                self.details_btn, self.delete_btn):
            controls.addWidget(widget)
        layout.addLayout(controls)
        self.status = QLabel("")
        layout.addWidget(self.status)
        self.table = QTableView()
        self.table_model = FileTableModel(self)
        self.table.setModel(self.table_model)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.doubleClicked.connect(lambda _idx: self.show_details())
        layout.addWidget(self.table, 1)

        self.search_btn.clicked.connect(self.new_search)
        self.query.returnPressed.connect(self.new_search)
        self.prev_btn.clicked.connect(self.previous_page)
        self.next_btn.clicked.connect(self.next_page)
        self.details_btn.clicked.connect(self.show_details)
        self.delete_btn.clicked.connect(self.delete_selected)
        self.refresh_tapes()
        self.refresh_sources()
        self._update_buttons()

    def refresh_tapes(self):
        current = self.tape_combo.currentText()
        self.tape_combo.clear()
        self.tape_combo.addItem("All")
        for tape in self.db.list_tapes():
            self.tape_combo.addItem(tape["volume_label"])
        if current:
            idx = self.tape_combo.findText(current)
            if idx >= 0:
                self.tape_combo.setCurrentIndex(idx)

    def refresh_sources(self):
        current = self.source_combo.currentText()
        self.source_combo.clear()
        self.source_combo.addItem("All")
        for source_host in self.db.list_source_hosts():
            self.source_combo.addItem(source_host)
        if current:
            idx = self.source_combo.findText(current)
            if idx >= 0:
                self.source_combo.setCurrentIndex(idx)

    def new_search(self):
        self.cursor_stack = [None]
        self.page_index = 0
        self.load_page(None)

    def load_page(self, cursor):
        query = self.query.text().strip()
        if not query:
            self.table_model.set_rows([])
            self.status.setText("Enter a search query.")
            self._update_buttons()
            return
        self.generation += 1
        generation = self.generation
        tape = self.tape_combo.currentText()
        scope = {}
        if tape and tape != "All":
            scope["tape_label"] = tape
        source_host = self.source_combo.currentText()
        if source_host and source_host != "All":
            scope["source_host"] = source_host
        self.status.setText("Searching...")
        worker = RepositoryWorker(
            self.db_path, generation,
            lambda repo: repo.search_catalog_fts(
                query, scope=scope, cursor=cursor, limit=PAGE_SIZE))
        worker.signals.finished.connect(self._on_loaded)
        worker.signals.failed.connect(lambda _token, msg: self._show_error(msg))
        self.pool.start(worker)

    @Slot(object, object)
    def _on_loaded(self, generation, result):
        if generation != self.generation:
            return
        self.table_model.set_rows(result["rows"])
        self.has_more = bool(result["has_more"])
        if self.has_more:
            if len(self.cursor_stack) == self.page_index + 1:
                self.cursor_stack.append(result["next_cursor"])
            else:
                self.cursor_stack[self.page_index + 1] = result["next_cursor"]
        self.status.setText(
            f"{len(result['rows']):,} row(s) on page {self.page_index + 1}")
        self.table.resizeColumnsToContents()
        self._update_buttons()

    def _update_buttons(self):
        has_rows = bool(self.table_model.rows)
        self.prev_btn.setEnabled(self.page_index > 0)
        self.next_btn.setEnabled(self.has_more)
        self.details_btn.setEnabled(has_rows)
        self.delete_btn.setEnabled(has_rows)

    def previous_page(self):
        if self.page_index > 0:
            self.page_index -= 1
            self.load_page(self.cursor_stack[self.page_index])

    def next_page(self):
        if self.has_more:
            self.page_index += 1
            self.load_page(self.cursor_stack[self.page_index])

    def selected_file_ids(self):
        return self.table_model.file_ids(self.table.selectionModel().selectedRows())

    def show_details(self):
        ids = self.selected_file_ids()
        if ids:
            record = self.db.get_file_by_id(ids[0])
            if record:
                FileDetailDialog(record, self).exec()

    def delete_selected(self):
        ids = self.selected_file_ids()
        if not ids:
            return
        if QMessageBox.question(
                self, "Delete File Records",
                f"Delete {len(ids)} file record(s)? This cannot be undone."
        ) != QMessageBox.StandardButton.Yes:
            return
        self.db.delete_files(ids)
        self.load_page(self.cursor_stack[self.page_index])

    def _show_error(self, message):
        QMessageBox.critical(self, "Search Error", message)


class DirectoryCatalogWidget(QWidget):
    def __init__(self, db, db_path, parent=None):
        super().__init__(parent)
        self.db = db
        self.db_path = db_path
        self.pool = QThreadPool.globalInstance()
        self.generation = 0

        layout = QVBoxLayout(self)
        controls = QHBoxLayout()
        self.tape_combo = QComboBox()
        self.query = QLineEdit()
        self.query.setPlaceholderText("Directory or small-file search")
        self.bundles_btn = QPushButton("Bundles")
        self.stats_btn = QPushButton("Directory Stats")
        self.dir_btn = QPushButton("Find Directory")
        self.manifest_btn = QPushButton("Manifest Search")
        self.validate_btn = QPushButton("Validate")
        for widget in (
                QLabel("Tape:"), self.tape_combo, self.query,
                self.bundles_btn, self.stats_btn, self.dir_btn,
                self.manifest_btn, self.validate_btn):
            controls.addWidget(widget)
        controls.addStretch(1)
        layout.addLayout(controls)
        self.status = QLabel("")
        layout.addWidget(self.status)
        self.table = QTableView()
        self.model = SimpleRowsTableModel([], self)
        self.table.setModel(self.model)
        layout.addWidget(self.table, 1)

        self.bundles_btn.clicked.connect(self.load_bundles)
        self.stats_btn.clicked.connect(self.load_stats)
        self.dir_btn.clicked.connect(self.find_directory)
        self.manifest_btn.clicked.connect(self.search_manifest)
        self.validate_btn.clicked.connect(self.validate)
        self.refresh_tapes()

    def refresh_tapes(self):
        current = self.tape_combo.currentText()
        self.tape_combo.clear()
        self.tape_combo.addItem("All")
        for tape in self.db.list_tapes():
            self.tape_combo.addItem(tape["volume_label"])
        if current:
            idx = self.tape_combo.findText(current)
            if idx >= 0:
                self.tape_combo.setCurrentIndex(idx)

    def _tape(self):
        value = self.tape_combo.currentText()
        return None if value == "All" else value

    def _run(self, columns, fn, label):
        self.generation += 1
        generation = self.generation
        self.status.setText(f"{label}...")
        worker = RepositoryWorker(self.db_path, generation, fn)
        worker.signals.finished.connect(
            lambda token, result: self._loaded(token, result, columns))
        worker.signals.failed.connect(lambda _token, msg: self._show_error(msg))
        self.pool.start(worker)

    @Slot(object, object)
    def _loaded(self, generation, result, columns):
        if generation != self.generation:
            return
        if isinstance(result, dict) and "rows" in result:
            rows = result["rows"]
        elif isinstance(result, dict) and "warnings" in result:
            rows = result["warnings"]
            if not rows:
                rows = [{"status": "ok", "bundles_checked": result["bundles_checked"]}]
                columns = ["status", "bundles_checked"]
        else:
            rows = result
        self.model = SimpleRowsTableModel(columns, self)
        self.model.set_rows(rows)
        self.table.setModel(self.model)
        self.table.resizeColumnsToContents()
        self.status.setText(f"{len(rows):,} row(s)")

    def load_bundles(self):
        tape = self._tape()
        columns = [
            "bundle_id", "tape_label", "source_host", "original_dir_path",
            "stored_bundle_path", "manifest_path", "file_count", "byte_count",
            "small_file_count", "large_file_count",
        ]
        self._run(
            columns,
            lambda repo: repo.list_directory_bundles(
                tape_label=tape, limit=PAGE_SIZE),
            "Loading bundles",
        )

    def load_stats(self):
        tape = self._tape()
        columns = [
            "stat_id", "tape_label", "source_host", "original_dir_path",
            "recursive_file_count", "recursive_bytes", "small_file_count",
            "small_file_bytes", "large_file_count", "large_file_bytes",
            "stored_root_path",
        ]
        self._run(
            columns,
            lambda repo: repo.list_directory_stats(
                tape_label=tape, limit=PAGE_SIZE),
            "Loading directory stats",
        )

    def find_directory(self):
        query = self.query.text().strip()
        tape = self._tape()
        if not query:
            self.status.setText("Enter a directory path.")
            return
        columns = [
            "dir_id", "tape_label", "source_host", "original_dir_path",
            "parent_original_dir_path", "depth", "recursive_file_count",
            "recursive_bytes", "recursive_small_file_count",
            "recursive_large_file_count", "stored_bundle_path",
            "manifest_path",
        ]
        self._run(
            columns,
            lambda repo: repo.find_directory_tree(
                query, tape_label=tape, limit=PAGE_SIZE),
            "Searching directories",
        )

    def search_manifest(self):
        query = self.query.text().strip()
        tape = self._tape()
        if not query:
            self.status.setText("Enter a small-file search term.")
            return
        columns = [
            "tape_label", "stored_bundle_path", "relative_path", "file_name",
            "size_bytes", "manifest_path",
        ]
        self._run(
            columns,
            lambda repo: repo.search_small_file_manifests(
                query, tape_label=tape, limit=PAGE_SIZE),
            "Streaming manifests",
        )

    def validate(self):
        tape = self._tape()
        columns = [
            "warning", "bundle_id", "tape_label", "stored_bundle_path",
            "file_count", "direct_files", "byte_count", "direct_bytes",
        ]
        self._run(
            columns,
            lambda repo: repo.validate_directory_catalog(tape_label=tape),
            "Validating directory catalog",
        )

    def _show_error(self, message):
        QMessageBox.critical(self, "Directory Catalog Error", message)


class ManageWidget(QWidget):
    def __init__(self, db, db_path, parent=None):
        super().__init__(parent)
        self.db = db
        self.db_path = db_path
        self.pool = QThreadPool.globalInstance()
        self._sessions_token = 0
        self._cleanup_summary = {}
        layout = QVBoxLayout(self)
        tabs = QTabWidget(self)
        layout.addWidget(tabs)
        tabs.addTab(self._build_tapes_tab(), "Tapes")
        tabs.addTab(self._build_sessions_tab(), "Sessions")
        self._loaded = False

    def _build_tapes_tab(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        controls = QHBoxLayout()
        self.rename_btn = QPushButton("Rename")
        self.capacity_btn = QPushButton("Set Capacity")
        self.recalc_btn = QPushButton("Recalculate Used")
        self.wipe_btn = QPushButton("Wipe File Records")
        self.delete_tape_btn = QPushButton("Delete Tape")
        self.refresh_btn = QPushButton("Refresh")
        for button in (
                self.rename_btn, self.capacity_btn, self.recalc_btn,
                self.wipe_btn, self.delete_tape_btn, self.refresh_btn):
            controls.addWidget(button)
        controls.addStretch(1)
        layout.addLayout(controls)
        self.tapes_table = QTableWidget(0, 7)
        self.tapes_table.setHorizontalHeaderLabels([
            "ID", "Volume Label", "Initialized", "Capacity GB",
            "Used", "Files", "Used %"])
        self.tapes_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tapes_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        layout.addWidget(self.tapes_table, 1)
        self.rename_btn.clicked.connect(self.rename_tape)
        self.capacity_btn.clicked.connect(self.set_capacity)
        self.recalc_btn.clicked.connect(self.recalculate_used)
        self.wipe_btn.clicked.connect(self.wipe_tape_files)
        self.delete_tape_btn.clicked.connect(self.delete_tape)
        self.refresh_btn.clicked.connect(self.refresh)
        return page

    def _build_sessions_tab(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        controls = QHBoxLayout()
        self.delete_session_btn = QPushButton("Delete Selected Session")
        self.cleanup_btn = QPushButton("Clean Unused Session Data")
        self.preflight_btn = QPushButton("Open Catalog Preflight")
        for button in (self.delete_session_btn, self.cleanup_btn, self.preflight_btn):
            controls.addWidget(button)
        controls.addStretch(1)
        layout.addLayout(controls)
        self.sessions_table = QTableWidget(0, 11)
        self.sessions_table.setHorizontalHeaderLabels([
            "Type", "ID", "Label", "Status", "Mode", "Created", "Completed",
            "Chunks", "Manifest Rows", "Manifest Size", "File Records"])
        self.sessions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.sessions_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        layout.addWidget(self.sessions_table, 1)
        self.delete_session_btn.clicked.connect(self.delete_session)
        self.cleanup_btn.clicked.connect(self.cleanup_sessions)
        self.preflight_btn.clicked.connect(self.open_preflight_console)
        return page

    def refresh(self):
        self._loaded = True
        self.refresh_tapes()
        self.refresh_sessions()

    def _write_buttons(self):
        return (self.rename_btn, self.capacity_btn, self.recalc_btn,
                self.wipe_btn, self.delete_tape_btn, self.refresh_btn,
                self.delete_session_btn, self.cleanup_btn)

    def _run_db_task(self, fn, on_done=None):
        """Run a DB write in the thread pool with the write buttons disabled."""
        for button in self._write_buttons():
            button.setEnabled(False)

        def _restore_buttons():
            for button in self._write_buttons():
                button.setEnabled(True)

        def _ok(result):
            _restore_buttons()
            if on_done:
                on_done(result)

        def _fail(message):
            _restore_buttons()
            self._show_error(message)

        task = DbWriteTask(fn)
        task.signals.finished.connect(_ok)
        task.signals.failed.connect(_fail)
        self.pool.start(task)

    def selected_tape(self):
        row = self.tapes_table.currentRow()
        if row < 0:
            return None
        item = self.tapes_table.item(row, 1)
        return item.text() if item is not None else None

    def refresh_tapes(self):
        tapes = self.db.list_tapes()
        self.tapes_table.setRowCount(len(tapes))
        for row, tape in enumerate(tapes):
            label = tape["volume_label"]
            used = tape["used_space"] or 0
            cap = tape["total_capacity"] or 0
            pct = (used / (cap * 1024 ** 3) * 100) if cap else 0
            values = [
                tape["tape_id"], label, _fmt_ts(tape["date_formatted"]),
                cap or "", _fmt_bytes(used),
                self.db.count_tape_file_records(label), f"{pct:.1f}%"]
            for col, value in enumerate(values):
                self.tapes_table.setItem(row, col, QTableWidgetItem(str(value)))
        self.tapes_table.resizeColumnsToContents()

    def rename_tape(self):
        label = self.selected_tape()
        if not label:
            return
        new_label, ok = QInputDialog.getText(self, "Rename Tape", "New label:")
        if ok and new_label:
            target = new_label.strip()
            self._run_db_task(
                lambda: self.db.rename_tape(label, target),
                on_done=lambda _result: self.refresh())

    def set_capacity(self):
        label = self.selected_tape()
        if not label:
            return
        value, ok = QInputDialog.getDouble(
            self, "Set Capacity", "Capacity GB:", decimals=2, minValue=0.01)
        if ok:
            self._run_db_task(
                lambda: self.db.update_tape_capacity(label, value),
                on_done=lambda _result: self.refresh())

    def recalculate_used(self):
        label = self.selected_tape()
        if label:
            self._run_db_task(
                lambda: self.db.recalculate_tape_used_space(label),
                on_done=lambda _result: self.refresh())

    def wipe_tape_files(self):
        label = self.selected_tape()
        if not label:
            return
        count = self.db.count_tape_file_records(label)
        if QMessageBox.question(
                self, "Wipe File Records",
                f"Delete {count:,} file record(s) for {label}?") == QMessageBox.StandardButton.Yes:
            self._run_db_task(
                lambda: self.db.delete_files_for_tape(label),
                on_done=lambda _result: self.refresh())

    def delete_tape(self):
        label = self.selected_tape()
        if label and QMessageBox.question(
                self, "Delete Tape",
                f"Delete tape {label} and all file records?") == QMessageBox.StandardButton.Yes:
            self._run_db_task(
                lambda: self.db.delete_tape(label),
                on_done=lambda _result: self.refresh())

    def refresh_sessions(self):
        self._sessions_token += 1
        token = self._sessions_token
        self.sessions_table.setRowCount(0)
        self.cleanup_btn.setEnabled(False)
        worker = RepositoryWorker(
            self.db_path,
            token,
            lambda repo: {
                "rows": repo.list_sessions(),
                "summary": repo.unreferenced_remote_data_summary(),
            },
        )
        worker.signals.finished.connect(self._on_sessions_loaded)
        worker.signals.failed.connect(lambda _token, msg: self._show_error(msg))
        self.pool.start(worker)

    @Slot(object, object)
    def _on_sessions_loaded(self, token, result):
        if token != self._sessions_token:
            return
        rows = result["rows"]
        self._cleanup_summary = result.get("summary") or {}
        self.sessions_table.setRowCount(len(rows))
        for row, item in enumerate(rows):
            values = [
                item["kind"], item["session_id"], item["session_label"],
                item["status"], item["mode"], _fmt_ts(item["created_at"]),
                _fmt_ts(item["completed_at"]), item["chunks"],
                f"{item['manifest_rows']:,}", _fmt_bytes(item["manifest_bytes"] or 0),
                f"{item['file_records']:,}"]
            for col, value in enumerate(values):
                self.sessions_table.setItem(row, col, QTableWidgetItem(str(value)))
        self.sessions_table.resizeColumnsToContents()
        summary = self._cleanup_summary
        self.cleanup_btn.setEnabled(
            bool(summary.get("plans") or summary.get("snapshots"))
            and not summary.get("active_sessions"))

    def selected_session(self):
        row = self.sessions_table.currentRow()
        if row < 0:
            return None, None
        kind_item = self.sessions_table.item(row, 0)
        id_item = self.sessions_table.item(row, 1)
        if kind_item is None or id_item is None:
            return None, None
        return kind_item.text(), int(id_item.text())

    def delete_session(self):
        kind, session_id = self.selected_session()
        if not kind:
            return
        if QMessageBox.question(
                self, "Delete Session",
                f"Delete {kind} session {session_id}?") != QMessageBox.StandardButton.Yes:
            return
        self._run_db_task(
            lambda: self.db.delete_session(kind, session_id),
            on_done=lambda _result: self.refresh_sessions())

    def cleanup_sessions(self):
        summary = self._cleanup_summary or self.db.get_unreferenced_remote_data_summary()
        if QMessageBox.question(
                self, "Clean Unused Session Data",
                f"Delete unused plans/snapshots?\n\n{summary}") != QMessageBox.StandardButton.Yes:
            return

        def done(result):
            QMessageBox.information(self, "Cleanup Complete", str(result))
            self.refresh_sessions()

        # compact=True runs VACUUM (ANALYZE) — never on the UI thread.
        self._run_db_task(
            lambda: self.db.cleanup_unreferenced_remote_data(compact=True),
            on_done=done)

    def _show_error(self, message):
        QMessageBox.critical(self, "Database Error", message)

    def open_preflight_console(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        kwargs: Dict[str, Any] = {"cwd": root}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
        subprocess.Popen(
            [sys.executable, os.path.join(root, "run.py"),
             "--catalog-v3-preflight"],
            **kwargs)


class DBInspectorQtApp(QMainWindow):
    def __init__(self, db, db_path, display_ref=None):
        super().__init__()
        self.db = db
        self.db_path = db_path
        self.setWindowTitle("LTO Archive Inspector")
        self.resize(1500, 900)
        with InspectorRepository(db_path) as repo:
            repo.require_catalog_v3()

        tabs = QTabWidget(self)
        self.tabs = tabs
        self.browse = BrowseWidget(db, db_path, self)
        self.search = SearchWidget(db, db_path, self)
        self.directory_catalog = DirectoryCatalogWidget(db, db_path, self)
        self.manage = ManageWidget(db, db_path, self)
        tabs.addTab(self.browse, "Files")
        tabs.addTab(self.search, "Search")
        tabs.addTab(self.directory_catalog, "Directory Catalog")
        tabs.addTab(self.manage, "Manage")
        tabs.currentChanged.connect(self._tab_changed)
        self.setCentralWidget(tabs)

        refresh = QAction("Refresh", self)
        refresh.triggered.connect(self.refresh_all)
        self.menuBar().addAction(refresh)
        self.statusBar().showMessage(display_ref or db_path)

    def refresh_all(self):
        self.search.refresh_tapes()
        self.directory_catalog.refresh_tapes()
        if self.manage._loaded:
            self.manage.refresh()

    def _tab_changed(self, index):
        if self.tabs.widget(index) is self.manage and not self.manage._loaded:
            self.manage.refresh()


def run_qt_inspector(db, db_path, display_ref=None):
    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("LTO Archive Inspector")
    window = DBInspectorQtApp(db, db_path, display_ref=display_ref)
    window.show()
    return app.exec()


if __name__ == "__main__":
    from .config import ConfigManager
    from .constants import PROJECT_ROOT
    from .db import create_database_manager

    os.chdir(PROJECT_ROOT)
    cfg = ConfigManager()
    db = create_database_manager(cfg)
    try:
        raise SystemExit(run_qt_inspector(
            db, cfg.db_dsn, display_ref=cfg.db_display_ref))
    finally:
        db.close()
