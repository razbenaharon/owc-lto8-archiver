"""LTORetriever restore flows."""
import os
import re
import shutil
import uuid
import zipfile
import posixpath
import ntpath
from collections import defaultdict

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .db import DatabaseManager, _fmt_ts
from .ltfs import get_volume_label
from .packer import StagingSpaceError, ensure_staging_space
from .robocopy import _robocopy_file
from .runtime import CANCEL, _acquire_tape_io_lock, _release_tape_io_lock


RESTORE_PAGE_SIZE = 250


class LTORetriever:
    def __init__(self, db: DatabaseManager, tape_drive: str,
                 staging_dir: str, restore_dir: str):
        self.db          = db
        self.tape_drive  = tape_drive
        self.staging_dir = staging_dir
        self.restore_dir = restore_dir

    @staticmethod
    def _source_path_module(path):
        if re.match(r'^[A-Za-z]:', path or '') or '\\' in (path or ''):
            return ntpath
        return posixpath

    @staticmethod
    def _path_is_under(path, base):
        if not path or not base:
            return False
        mod = LTORetriever._source_path_module(path)
        path_norm = mod.normcase(mod.normpath(path))
        base_norm = mod.normcase(mod.normpath(base))
        return path_norm == base_norm or path_norm.startswith(
            base_norm.rstrip('\\/') + mod.sep)

    @staticmethod
    def _join_source_parts(parts, sep):
        if not parts:
            return ''
        if parts[0].endswith(':'):
            return parts[0] + sep + sep.join(parts[1:])
        return sep.join(parts)

    @staticmethod
    def _infer_directory_root(dir_query, records):
        query = (dir_query or '').strip().strip('"').rstrip('/\\')
        if not query or not records:
            return query

        first_path = records[0].get('original_path') or ''
        mod = LTORetriever._source_path_module(first_path or query)
        query_norm = mod.normpath(query)
        is_abs_query = bool(re.match(r'^[A-Za-z]:', query_norm)) or query_norm.startswith(('/', '\\'))
        if is_abs_query and all(
                LTORetriever._path_is_under(r.get('original_path'), query_norm)
                for r in records):
            return query_norm

        query_leaf = query.replace('\\', '/').strip('/').split('/')[-1].lower()
        for record in records:
            original = record.get('original_path') or ''
            parts = [p for p in re.split(r'[\\/]+', original) if p]
            for i, part in enumerate(parts[:-1]):
                if part.lower() == query_leaf:
                    sep = '\\' if ('\\' in original or re.match(r'^[A-Za-z]:', original)) else '/'
                    return LTORetriever._join_source_parts(parts[:i + 1], sep)

        paths = [r.get('original_path') for r in records if r.get('original_path')]
        if not paths:
            return query_norm
        try:
            return mod.commonpath(paths)
        except ValueError:
            return mod.dirname(paths[0])

    @staticmethod
    def _safe_restore_relpath(rel_path):
        parts = []
        for part in re.split(r'[\\/]+', rel_path or ''):
            if not part or part in ('.', '..') or part.endswith(':'):
                continue
            parts.append(part)
        return os.path.join(*parts) if parts else ''

    def _unique_dest_path(self, candidate):
        """Return a restore path under restore_dir that won't overwrite an
        existing file. Distinct source files that share a basename would
        otherwise silently clobber each other when flattened into restore_dir."""
        base, ext = os.path.splitext(candidate)
        counter = 1
        while os.path.exists(candidate):
            candidate = f"{base}_{counter}{ext}"
            counter += 1
        return candidate

    def _destination_for_record(self, record, restore_base=None):
        rel_path = None
        original = record.get('original_path')
        if restore_base and original and self._path_is_under(original, restore_base):
            mod = self._source_path_module(original)
            rel_path = mod.relpath(original, restore_base)
        safe_rel = self._safe_restore_relpath(rel_path or record['file_name'])
        return self._unique_dest_path(os.path.join(self.restore_dir, safe_rel))

    @staticmethod
    def _copy_file_to(src, dst):
        """Copy ``src`` so it lands exactly at ``dst``, honoring a renamed
        basename.

        robocopy always writes ``dst_dir/<src basename>`` — it cannot rename
        during a copy — so handing it a collision-renamed target used to
        silently overwrite the existing same-named file instead. Copy into a
        scratch subdir of the destination (same volume), then atomically
        move the file to its real name."""
        dst_dir = os.path.dirname(os.path.abspath(dst))
        scratch = os.path.join(dst_dir, f".restore_tmp_{uuid.uuid4().hex[:12]}")
        os.makedirs(scratch, exist_ok=True)
        try:
            landed = os.path.join(scratch, os.path.basename(src))
            if not _robocopy_file(src, landed):
                return False
            if CANCEL.is_set():
                # A cancelled robocopy can exit "successfully" with a partial
                # file — never publish it to the restore directory.
                return False
            os.replace(landed, dst)
            return True
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    @staticmethod
    def _check_cancelled():
        if CANCEL.is_set():
            raise RuntimeError("[RESTORE] Cancelled by user; "
                               "partially restored files were kept.")

    def run(self):
        print("\n--- RETRIEVER: Search & Restore ---")
        print("1. Search by filename / wildcard  (e.g. *.mov, IMG_*)")
        print("2. Search by date range")
        print("3. Search by both")
        print("4. Restore full directory")
        print("5. Restore full backup session")
        opt = input("Option (1-5): ").strip()

        fetch_page = None
        fetch_after = None      # keyset pager for ALL-restores (O(n), not O(n^2))
        total_results = 0
        restore_base = None

        if opt in ('1', '2', '3'):
            name_q = date_from = date_to = None
            if opt in ('1', '3'):
                name_q = input("Filename or pattern: ").strip() or None
            if opt in ('2', '3'):
                date_from = input("Backed-up from (YYYY-MM-DD, blank=any): ").strip() or None
                date_to   = input("Backed-up to   (YYYY-MM-DD, blank=any): ").strip() or None
            total_results = self.db.count_search_files(name_q, date_from, date_to)
            fetch_page = lambda offset: self.db.search_files(
                name_q, date_from, date_to,
                limit=RESTORE_PAGE_SIZE, offset=offset)
            fetch_after = lambda after_id: self.db.search_files(
                name_q, date_from, date_to,
                limit=RESTORE_PAGE_SIZE, after_id=after_id)

        elif opt == '4':
            dir_q = input("Original directory path (partial ok): ").strip()
            if not dir_q:
                return
            total_results = self.db.count_by_directory(dir_q)
            fetch_page = lambda offset: self.db.search_by_directory(
                dir_q, limit=RESTORE_PAGE_SIZE, offset=offset)
            fetch_after = lambda after_id: self.db.search_by_directory(
                dir_q, limit=RESTORE_PAGE_SIZE, after_id=after_id)
            first_page = fetch_page(0)
            if first_page:
                directory_root = self._infer_directory_root(dir_q, first_page)
                mod = self._source_path_module(directory_root)
                restore_base = mod.dirname(directory_root)

        elif opt == '5':
            sessions = self.db.list_backup_sessions()
            if not sessions:
                print("[RETRIEVER] No backup sessions found.")
                return
            print(f"\n{'#':>3}  {'Date':<12}  {'Tape':<25}  {'Files':>6}  Size")
            print("-" * 65)
            for i, s in enumerate(sessions, 1):
                size_s = f"{(s['total_bytes'] or 0) / 1024**3:.2f} GB"
                print(f"{i:>3}  {str(s['session_date']):<12}  {s['tape_label']:<25}  {s['file_count']:>6}  {size_s}")
            print()
            try:
                idx = int(input("Select session # (0 = cancel): ").strip())
            except ValueError:
                return
            if idx < 1 or idx > len(sessions):
                return
            s = sessions[idx - 1]
            total_results = self.db.count_by_session(s['session_date'], s['tape_label'])
            fetch_page = lambda offset: self.db.search_by_session(
                s['session_date'], s['tape_label'],
                limit=RESTORE_PAGE_SIZE, offset=offset)
            fetch_after = lambda after_id: self.db.search_by_session(
                s['session_date'], s['tape_label'],
                limit=RESTORE_PAGE_SIZE, after_id=after_id)

        else:
            return

        if not fetch_page or total_results <= 0:
            print("[RETRIEVER] No matching files found.")
            return

        offset = 0
        while True:
            results = fetch_page(offset)
            if not results and offset:
                offset = max(0, offset - RESTORE_PAGE_SIZE)
                results = fetch_page(offset)
            self._print_results_page(results, offset, total_results)
            sel_raw = input(
                "Enter file ID, ALL, N=next, P=previous, or 0=cancel: "
            ).strip()
            if sel_raw == '0' or not sel_raw:
                return
            if sel_raw.upper() == 'N':
                if offset + RESTORE_PAGE_SIZE < total_results:
                    offset += RESTORE_PAGE_SIZE
                else:
                    print("[RETRIEVER] Already on last page.")
                continue
            if sel_raw.upper() == 'P':
                offset = max(0, offset - RESTORE_PAGE_SIZE)
                continue
            os.makedirs(self.restore_dir, exist_ok=True)
            if sel_raw.upper() == 'ALL':
                if total_results > RESTORE_PAGE_SIZE:
                    confirm = input(
                        f"Restore all {total_results:,} matching file(s)? "
                        "Type RESTORE ALL to confirm: "
                    ).strip()
                    if confirm != 'RESTORE ALL':
                        print("[ABORTED]")
                        continue
                self._restore_all_pages(fetch_after, total_results,
                                        restore_base=restore_base)
                return
            try:
                sel = int(sel_raw)
            except ValueError:
                print("[RETRIEVER] Invalid input.")
                continue
            record = self.db.get_file_by_id(sel)
            if not record:
                print("[RETRIEVER] File ID not found.")
                continue
            self._verify_tape(record['tape_label'])
            if record['is_packed']:
                self._restore_packed(record, restore_base=restore_base)
            else:
                self._restore_loose(record, restore_base=restore_base)
            return

    @staticmethod
    def _print_results_page(results, offset, total_results):
        page_start = offset + 1 if results else 0
        page_end = offset + len(results)
        print(f"\n{'ID':>7}  {'Filename':<42}  {'Size':>10}  "
              f"{'Backup Date':<20}  {'Host':<8}  Tape")
        print("-" * 112)
        for row in results:
            size_s = f"{(row['file_size_bytes'] or 0)/1024**2:.1f} MB"
            date_s = _fmt_ts(row['backup_date'])
            host_s = row.get('source_host') or 'so02'
            print(f"{row['file_id']:>7}  {row['file_name']:<42}  "
                  f"{size_s:>10}  {date_s:<20}  {host_s:<8}  "
                  f"{row['tape_label']}")
        print(f"\nShowing {page_start:,}-{page_end:,} of "
              f"{total_results:,} matching file(s)")

    def _restore_all_pages(self, fetch_after, total_results, restore_base=None):
        # Keyset pagination on file_id: OFFSET paging re-scans every skipped
        # row, which turns a large ALL-restore into an O(n^2) query series.
        after_id = 0
        restored = 0
        while True:
            self._check_cancelled()
            page = fetch_after(after_id)
            if not page:
                break
            self._restore_many(page, restore_base=restore_base)
            restored += len(page)
            after_id = page[-1]['file_id']
        print(f"\n[RESTORE] Requested {total_results:,}; processed "
              f"{restored:,} file(s).")

    def _restore_many(self, records, restore_base=None):
        total = len(records)
        done  = 0

        # Group by tape so we only ask for each tape once
        by_tape = defaultdict(list)
        for r in records:
            by_tape[r['tape_label']].append(r)

        for tape_label, tape_records in by_tape.items():
            self._check_cancelled()
            self._verify_tape(tape_label)

            loose  = [r for r in tape_records if not r['is_packed']]
            packed = [r for r in tape_records if r['is_packed']]

            for record in loose:
                self._check_cancelled()
                self._restore_loose(record, restore_base=restore_base)
                done += 1
                print(f"[RESTORE] Progress: {done}/{total}")

            # Group packed files by ZIP bundle so each bundle is copied only once
            by_container = defaultdict(list)
            for r in packed:
                by_container[r['container_name']].append(r)

            for container_path, container_records in by_container.items():
                self._check_cancelled()
                self._restore_packed_bulk(container_path, container_records,
                                          restore_base=restore_base)
                done += len(container_records)
                print(f"[RESTORE] Progress: {done}/{total}")

        print(f"\n[RESTORE] Complete. {total} file(s) restored to: {self.restore_dir}")

    def _verify_tape(self, required_label):
        while True:
            mounted = get_volume_label(self.tape_drive)
            if mounted and mounted.upper() == required_label.upper():
                return

            current = mounted or "not detected"
            print(f"\n[TAPE] Required: {required_label}  |  Currently mounted: {current}")
            choice = input(
                f"Insert tape '{required_label}', then press Enter to re-check "
                "or type CANCEL to abort: "
            ).strip()
            if choice.upper() == 'CANCEL':
                raise RuntimeError(
                    f"[RESTORE] Cancelled: tape '{required_label}' was not mounted.")

    def _bundle_staging_space_ok(self, tape_zip_path):
        """Check the staging disk can hold a bundle ZIP before reading it off
        tape. The size probe is an LTFS metadata read, so it takes the tape
        lock like every other tape access."""
        _acquire_tape_io_lock(f"stat {os.path.basename(tape_zip_path)}")
        try:
            try:
                bundle_size = os.path.getsize(tape_zip_path)
            except OSError:
                bundle_size = 0
        finally:
            _release_tape_io_lock()
        try:
            ensure_staging_space(
                self.staging_dir, bundle_size,
                context=f"restore bundle {os.path.basename(tape_zip_path)}")
        except StagingSpaceError as e:
            print(f"[ERROR] {e}")
            return False
        return True

    def _restore_loose(self, record, restore_base=None):
        src = record['stored_path']
        dst = self._destination_for_record(record, restore_base=restore_base)
        print(f"\n[RESTORE] Copying loose file: {record['file_name']}")
        os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
        _acquire_tape_io_lock(f"restore {record['file_name']}")
        try:
            ok = self._copy_file_to(src, dst)
        finally:
            _release_tape_io_lock()
        self._check_cancelled()
        if ok:
            print(f"[RESTORE] Saved to: {dst}")
        else:
            print(f"[ERROR] Restore failed: robocopy error")

    @staticmethod
    def _resolve_zip_entry(zip_names, stored_in_zip, file_name):
        """Choose which ZIP entry to extract for a catalog record.

        Prefer the exact stored_path. Only fall back to a basename match when it
        is UNIQUE in the ZIP (returning a warning); refuse an ambiguous basename
        so a wrong same-named entry is never extracted. Returns
        (entry_name, warning, error) — entry_name is None when error is set."""
        if stored_in_zip and stored_in_zip in zip_names:
            return stored_in_zip, None, None
        base_matches = [n for n in zip_names
                        if os.path.basename(n) == file_name]
        if len(base_matches) == 1:
            warn = (f"exact stored path '{stored_in_zip}' not in ZIP; using the "
                    f"unique basename match '{base_matches[0]}'.")
            return base_matches[0], warn, None
        if not base_matches:
            return None, None, (f"'{file_name}' not found inside ZIP "
                                f"(stored path: {stored_in_zip}).")
        return None, None, (f"'{file_name}' is ambiguous inside ZIP "
                            f"({len(base_matches)} entries share this name) and "
                            f"the stored path '{stored_in_zip}' is absent — "
                            "refusing to guess.")

    def _restore_packed(self, record, restore_base=None):
        tape_zip_path = record['container_name']   # full path of ZIP on tape
        stored_in_zip = record['stored_path']       # relative path inside the ZIP
        local_zip     = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))

        print(f"\n[RESTORE] Packed file inside {os.path.basename(tape_zip_path)}")
        print(f"[RESTORE] Step 1/3: Copying ZIP from tape to staging...")

        os.makedirs(self.staging_dir, exist_ok=True)
        if not self._bundle_staging_space_ok(tape_zip_path):
            return
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print(f"[ERROR] Could not copy ZIP from tape: robocopy error")
            return

        print(f"[RESTORE] Step 2/3: Extracting '{record['file_name']}' from ZIP...")
        dst = self._destination_for_record(record, restore_base=restore_base)
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                entry, warn, err = self._resolve_zip_entry(
                    zf.namelist(), stored_in_zip, record['file_name'])
                if err:
                    print(f"[ERROR] {err}")
                    return
                if warn:
                    print(f"[WARN] {warn}")
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with zf.open(entry) as zf_src, open(dst, 'wb') as out:
                    shutil.copyfileobj(zf_src, out)
            print(f"[RESTORE] Saved to: {dst}")
        except Exception as e:
            print(f"[ERROR] Extraction failed: {e}")
        finally:
            print("[RESTORE] Step 3/3: Removing staging ZIP...")
            try:
                os.remove(local_zip)
            except OSError:
                pass

    def _restore_packed_bulk(self, tape_zip_path, records, restore_base=None):
        """Extract multiple files from a single ZIP bundle in one pass."""
        local_zip = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))
        print(f"\n[RESTORE] Copying {os.path.basename(tape_zip_path)} from tape to staging...")
        os.makedirs(self.staging_dir, exist_ok=True)
        if not self._bundle_staging_space_ok(tape_zip_path):
            return
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print(f"[ERROR] Could not copy ZIP from tape: robocopy error")
            return
        print(f"[RESTORE] Extracting {len(records)} file(s)...")
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                zip_names = zf.namelist()
                for record in records:
                    stored_in_zip = record['stored_path']
                    dst = self._destination_for_record(record,
                                                       restore_base=restore_base)
                    entry, warn, err = self._resolve_zip_entry(
                        zip_names, stored_in_zip, record['file_name'])
                    if err:
                        print(f"[ERROR] {err}")
                        continue
                    if warn:
                        print(f"[WARN] {warn}")
                    try:
                        os.makedirs(os.path.dirname(dst), exist_ok=True)
                        with zf.open(entry) as zf_src, open(dst, 'wb') as out:
                            shutil.copyfileobj(zf_src, out)
                        print(f"[OK] {record['file_name']}")
                    except Exception as e:
                        print(f"[ERROR] {record['file_name']}: {e}")
        except Exception as e:
            print(f"[ERROR] ZIP extraction failed: {e}")
        finally:
            try:
                os.remove(local_zip)
            except OSError:
                pass
