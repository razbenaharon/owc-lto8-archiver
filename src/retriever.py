"""LTORetriever restore flows."""
import os
import re
import shutil
import tarfile
import uuid
import zipfile
import posixpath
import ntpath
from collections import defaultdict
from typing import TYPE_CHECKING

from .archive_artifacts import (TAR_SIDECAR_VERSION, ArtifactError,
                                is_ltfs_locator,
                                resolve_local_metadata_locator,
                                search_tar_sidecar)
from .db import _fmt_ts
from .ltfs import get_volume_label
from .local_manifest_archive import find_manifest_record, search_manifests
from .packer import StagingSpaceError, ensure_staging_space
from .robocopy import _robocopy_file
from .runtime import CANCEL, _acquire_tape_io_lock, _release_tape_io_lock
from .pipeline_types import ContainerFormat
from .tar_container import (STORED_TAR_DIALECT, STORED_TAR_FORMAT_VERSION,
                            StoredTarError, StoredTarReader,
                            validate_tar_member_name)

if TYPE_CHECKING:
    from .pg_db import PgDatabaseManager


RESTORE_PAGE_SIZE = 250


class LTORetriever:
    def __init__(self, db: "PgDatabaseManager", tape_drive: str,
                 staging_dir: str, restore_dir: str,
                 manifest_archive_root: str = None):
        self.db          = db
        self.tape_drive  = tape_drive
        self.staging_dir = staging_dir
        self.restore_dir = restore_dir
        self.manifest_archive_root = manifest_archive_root

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

    def _resolve_tape_path(self, path):
        """Remap the catalog's stored drive letter to the configured LTFS drive.

        ``stored_path``/``container_name`` were recorded as absolute paths on
        whatever letter the LTFS volume had at archive time. The mount letter
        is an OS assignment detail — the same cartridge can come back as E:\\
        years later — so restores resolve against the drive configured now.
        """
        drive, rest = os.path.splitdrive(str(path or ''))
        if not drive or not rest:
            return path
        current = os.path.splitdrive(os.path.abspath(self.tape_drive))[0]
        if not current or drive.upper() == current.upper():
            return path
        return current + rest

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
    def _route_container_format(record):
        """Return persisted container format, ``None`` for a loose file.

        The one compatibility exception is an unlinked legacy packed row.  It
        is adapted explicitly to ZIP only when it has the historical bundle
        locator; extensions are never inspected.
        """
        value = record.get("container_format")
        if value:
            try:
                return ContainerFormat(value)
            except ValueError as exc:
                raise RuntimeError(
                    f"[RESTORE] Unsupported persisted container format: {value!r}") from exc
        if record.get("container_id") is not None:
            raise RuntimeError(
                "[RESTORE] Linked container has no persisted format; refusing "
                "to guess from packing state or filename")
        if record.get("is_packed") is True and (
                record.get("container_name")
                or record.get("tape_container_locator")):
            return ContainerFormat.ZIP
        if record.get("is_packed") is False:
            return None
        raise RuntimeError(
            "[RESTORE] Record has neither a container format nor an explicit "
            "loose-file identity")

    def _local_container_path(self, record):
        locator = record.get("local_container_locator")
        if not locator or is_ltfs_locator(
                locator, tape_root=self.tape_drive,
                tape_locator=record.get("tape_container_locator")):
            return None
        return os.path.abspath(str(locator))

    def _record_needs_tape(self, record):
        route = self._route_container_format(record)
        if route is None or route is ContainerFormat.ZIP:
            return True
        # TAR routing first proves local sidecar availability.  If the data
        # container is not local, _restore_container verifies the tape only
        # after that proof; callers must not probe LTFS earlier.
        return False

    def _restore_record(self, record, restore_base=None):
        route = self._route_container_format(record)
        if route is None:
            return self._restore_loose(record, restore_base=restore_base)
        if route is ContainerFormat.ZIP:
            return self._restore_packed(record, restore_base=restore_base)
        return self._restore_container([record], restore_base=restore_base)

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
        print("4. Restore full directory  (indexed files >= threshold only)")
        print("5. Restore full backup session")
        print("6. Restore full directory — COMPLETE  (whole bundles; includes "
              "the small files that have no individual DB row)")
        print("7. Search/restore pruned small files from local manifests")
        opt = input("Option (1-7): ").strip()

        if opt == '7':
            self._run_manifest_search()
            return

        if opt == '6':
            dir_q = input("Original directory path: ").strip()
            self._restore_directory_complete(dir_q)
            return

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
            fetch_after = lambda after_id, tape: self.db.search_catalog(
                name_query=name_q, date_from=date_from, date_to=date_to,
                tape_label=tape, limit=RESTORE_PAGE_SIZE, after_id=after_id)

        elif opt == '4':
            dir_q = input("Original directory path (partial ok): ").strip()
            if not dir_q:
                return
            total_results = self.db.count_by_directory(dir_q)
            fetch_page = lambda offset: self.db.search_by_directory(
                dir_q, limit=RESTORE_PAGE_SIZE, offset=offset)
            fetch_after = lambda after_id, tape: self.db.search_by_directory(
                dir_q, limit=RESTORE_PAGE_SIZE, after_id=after_id,
                tape_label=tape)
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
            # A session is bound to one tape; return nothing for the others.
            fetch_after = lambda after_id, tape: (
                [] if tape and tape != s['tape_label']
                else self.db.search_by_session(
                    s['session_date'], s['tape_label'],
                    limit=RESTORE_PAGE_SIZE, after_id=after_id))

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
            if self._record_needs_tape(record):
                self._verify_tape(record['tape_label'])
            self._restore_record(record, restore_base=restore_base)
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
            host_s = row.get('source_host') or 'srv02'
            print(f"{str(row['file_id']):>7}  {row['file_name']:<42}  "
                  f"{size_s:>10}  {date_s:<20}  {host_s:<8}  "
                  f"{row['tape_label']}")
        print(f"\nShowing {page_start:,}-{page_end:,} of "
              f"{total_results:,} matching file(s)")

    def _run_manifest_search(self):
        if not self.manifest_archive_root:
            print("[RETRIEVER] Local manifest archive is not configured.")
            return
        query = input("Filename or pattern: ").strip() or "*"
        allowed_paths = self.db.list_pruned_manifest_segments()
        rows = search_manifests(
            self.manifest_archive_root, query, limit=500,
            allowed_paths=allowed_paths)
        if not rows:
            print("[RETRIEVER] No matching local-manifest files found.")
            return
        self._print_results_page(rows, 0, len(rows))
        print("[RETRIEVER] Results are capped at 500; narrow the pattern if needed.")
        selection = input(
            "Enter manifest ID (M:<id>), ALL, or 0=cancel: ").strip()
        if not selection or selection == "0":
            return
        os.makedirs(self.restore_dir, exist_ok=True)
        if selection.upper() == "ALL":
            confirm = input(
                f"Restore all {len(rows):,} displayed file(s)? "
                "Type RESTORE ALL to confirm: ").strip()
            if confirm == "RESTORE ALL":
                self._restore_many(rows)
            return
        try:
            record = find_manifest_record(
                self.manifest_archive_root, selection,
                allowed_paths=allowed_paths)
        except (TypeError, ValueError):
            record = None
        if not record:
            print("[RETRIEVER] Manifest file ID not found.")
            return
        if self._record_needs_tape(record):
            self._verify_tape(record["tape_label"])
        self._restore_record(record)

    def _restore_all_pages(self, fetch_after, total_results, restore_base=None):
        # One tape at a time: a multi-tape restore used to interleave tapes
        # page by page, prompting a cartridge swap every RESTORE_PAGE_SIZE
        # files. Within each tape, keyset pagination on file_id (OFFSET paging
        # re-scans every skipped row — O(n^2) over a large result).
        restored = 0
        for tape in [t['volume_label'] for t in self.db.list_tapes()]:
            after_id = 0
            while True:
                self._check_cancelled()
                page = fetch_after(after_id, tape)
                if not page:
                    break
                self._restore_many(page, restore_base=restore_base)
                restored += len(page)
                after_id = page[-1]['file_id']
        if restored < total_results:
            print(f"\n[RESTORE] Note: {total_results - restored:,} matching "
                  "record(s) reference tapes that are no longer registered "
                  "and were not restored.")
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
            if any(self._record_needs_tape(r) for r in tape_records):
                self._verify_tape(tape_label)

            loose = []
            contained = []
            for record in tape_records:
                route = self._route_container_format(record)
                (loose if route is None else contained).append(record)

            for record in loose:
                self._check_cancelled()
                self._restore_loose(record, restore_base=restore_base)
                done += 1
                print(f"[RESTORE] Progress: {done}/{total}")

            # Group by persisted format + durable identity so one request can
            # span legacy ZIP, Stored TAR and loose records safely.
            by_container = defaultdict(list)
            for r in contained:
                route = self._route_container_format(r)
                identity = (r.get("container_id")
                            or r.get("tape_container_locator")
                            or r.get("container_name"))
                by_container[(route.value, identity)].append(r)

            for (_format, _identity), container_records in by_container.items():
                self._check_cancelled()
                if self._route_container_format(
                        container_records[0]) is ContainerFormat.ZIP:
                    path = (container_records[0].get("tape_container_locator")
                            or container_records[0].get("container_name"))
                    self._restore_packed_bulk(
                        path, container_records, restore_base=restore_base)
                else:
                    self._restore_container(
                        container_records, restore_base=restore_base)
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
        """Check staging can hold a container before reading it off
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
        src = self._resolve_tape_path(record['stored_path'])
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
            print("[ERROR] Restore failed: robocopy error")

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

    def _sidecar_locator_for_record(self, record):
        locator = (record.get("local_sidecar_locator")
                   or record.get("permanent_local_metadata_locator"))
        if locator:
            return locator, record.get("sidecar_artifact_version")
        container_id = record.get("container_id")
        finder = getattr(self.db, "find_container_restore_sidecars", None)
        if container_id is not None and finder is not None:
            rows = finder([container_id])
            if rows:
                row = rows[0]
                return (row.get("local_locator")
                        or row.get("permanent_local_metadata_locator"),
                        row.get("artifact_version"))
        return None, None

    def _load_tar_sidecar(self, record, *, directory=None, limit=1_000_000):
        locator, artifact_version = self._sidecar_locator_for_record(record)
        path = resolve_local_metadata_locator(
            self.manifest_archive_root, locator,
            tape_root=self.tape_drive,
            tape_locator=(record.get("tape_sidecar_locator")
                          or record.get("tape_container_locator")
                          or record.get("container_name")))
        # The artifact identity and JSON header carry the same contract
        # version. Legacy/synthetic callers without the identity still have to
        # satisfy the one Phase-1 sidecar version.
        expected_version = artifact_version or TAR_SIDECAR_VERSION
        try:
            return search_tar_sidecar(
                path, directory=directory, limit=limit,
                expected_version=expected_version,
                expected_container_id=record.get("container_id"),
                expected_member_count=record.get("expected_member_count"))
        except ArtifactError:
            raise
        except Exception as exc:
            raise ArtifactError(
                "local TAR sidecar is unreadable or structurally invalid; "
                "restore will not fall back to scanning tape") from exc

    @staticmethod
    def _publish_temp_no_clobber(temp_path, candidate):
        """Atomically publish a same-volume temp without replacing a file."""
        base, ext = os.path.splitext(candidate)
        counter = 0
        while True:
            target = candidate if counter == 0 else f"{base}_{counter}{ext}"
            try:
                os.link(temp_path, target)
            except FileExistsError:
                counter += 1
                continue
            except OSError as exc:
                raise RuntimeError(
                    "[RESTORE] Atomic no-clobber publication is unavailable "
                    f"for {target!r}") from exc
            os.remove(temp_path)
            return target

    def _copy_tar_stream(self, source, output, expected_size):
        written = 0
        while True:
            self._check_cancelled()
            chunk = source.read(min(1024 * 1024, expected_size - written + 1))
            if not chunk:
                break
            output.write(chunk)
            written += len(chunk)
            if written > expected_size:
                raise StoredTarError("extracted TAR member exceeds expected size")
        if written != expected_size:
            raise StoredTarError(
                f"extracted TAR member size mismatch: {written} != {expected_size}")
        return written

    def _restore_tar_members(self, local_tar, records, restore_base=None,
                             sidecar=None):
        """Validate the full TAR, then extract selected members atomically."""
        if not records:
            return 0
        first = records[0]
        sidecar = sidecar or self._load_tar_sidecar(first)
        format_version = first.get("format_version") or STORED_TAR_FORMAT_VERSION
        tar_dialect = first.get("tar_dialect") or STORED_TAR_DIALECT
        with open(local_tar, "rb") as tar_stream:
            validated = StoredTarReader(
                tar_stream, tar_dialect=tar_dialect,
                format_version=format_version).validate(
                    sidecar.expected_members,
                    expected_member_count=first.get("expected_member_count"),
                    expected_logical_bytes=first.get("expected_logical_bytes"))
            expected_by_name = {item.name: item for item in validated.members}
            wanted = {}
            for record in records:
                name = validate_tar_member_name(
                    record.get("member_name") or record.get("stored_path"))
                member = expected_by_name.get(name)
                if member is None:
                    raise StoredTarError(
                        "selected TAR member is absent from validated sidecar: "
                        f"{name!r}")
                size = record.get("file_size_bytes")
                if size is not None and int(size) != member.logical_size:
                    raise StoredTarError(
                        f"catalog/sidecar size mismatch for {name!r}")
                wanted[name] = (record, member.logical_size)

            restored = 0
            seen = set()
            tar_stream.seek(0)
            tf = tarfile.open(fileobj=tar_stream, mode="r:")
            try:
                for info in tf:
                    normalized = validate_tar_member_name(info.name)
                    if normalized not in wanted:
                        continue
                    if not info.isfile():
                        raise StoredTarError(
                            "selected TAR member is not a regular file: "
                            f"{info.name!r}")
                    record, expected_size = wanted[normalized]
                    if int(info.size) != int(expected_size):
                        raise StoredTarError(
                            f"TAR extraction size disagrees for {info.name!r}")
                    source = tf.extractfile(info)
                    if source is None:
                        raise StoredTarError(
                            f"TAR member cannot be opened: {info.name!r}")
                    dst = self._destination_for_record(
                        record, restore_base=restore_base)
                    dst_dir = os.path.dirname(os.path.abspath(dst))
                    os.makedirs(dst_dir, exist_ok=True)
                    temp_path = os.path.join(
                        dst_dir, f".restore_tar_{uuid.uuid4().hex}.part")
                    try:
                        with source, open(temp_path, "xb") as output:
                            self._copy_tar_stream(source, output, expected_size)
                        self._check_cancelled()
                        published = self._publish_temp_no_clobber(temp_path, dst)
                        print(f"[OK] {record['file_name']} -> {published}")
                        restored += 1
                        seen.add(normalized)
                    finally:
                        try:
                            os.remove(temp_path)
                        except FileNotFoundError:
                            pass
            finally:
                tf.close()
        missing = set(wanted) - seen
        if missing:
            raise StoredTarError(
                f"validated TAR extraction did not encounter: {sorted(missing)!r}")
        return restored

    def _restore_container(self, records, restore_base=None, sidecar=None):
        """Restore one explicitly selected persisted container."""
        if not records:
            return 0
        route = self._route_container_format(records[0])
        if route is ContainerFormat.ZIP:
            locator = (records[0].get("tape_container_locator")
                       or records[0].get("container_name"))
            return self._restore_packed_bulk(
                locator, records, restore_base=restore_base)
        if route is not ContainerFormat.STORED_TAR:
            raise RuntimeError("[RESTORE] A loose file is not a container")

        record = records[0]
        try:
            sidecar = sidecar or self._load_tar_sidecar(record)
        except ArtifactError as exc:
            print(f"[ERROR] Stored TAR restore refused: {exc}")
            return 0
        local_tar = self._local_container_path(record)
        owns_staged_copy = False
        if not (local_tar and os.path.isfile(local_tar)):
            self._verify_tape(record["tape_label"])
            tape_path = self._resolve_tape_path(
                record.get("tape_container_locator")
                or record.get("container_name"))
            if not tape_path:
                raise RuntimeError(
                    "[RESTORE] Stored TAR has neither an available local "
                    "locator nor a tape locator")
            os.makedirs(self.staging_dir, exist_ok=True)
            local_tar = os.path.join(
                self.staging_dir,
                f"restore_{uuid.uuid4().hex}_{os.path.basename(tape_path)}")
            if not self._bundle_staging_space_ok(tape_path):
                return 0
            _acquire_tape_io_lock(f"restore {os.path.basename(tape_path)}")
            try:
                ok = _robocopy_file(tape_path, local_tar)
            finally:
                _release_tape_io_lock()
            if not ok:
                print("[ERROR] Could not copy Stored TAR from tape: robocopy error")
                return 0
            owns_staged_copy = True
        try:
            return self._restore_tar_members(
                local_tar, records, restore_base=restore_base,
                sidecar=sidecar)
        except (ArtifactError, StoredTarError, tarfile.TarError,
                OSError, RuntimeError) as exc:
            if CANCEL.is_set():
                raise
            print(f"[ERROR] Stored TAR restore refused: {exc}")
            return 0
        finally:
            if owns_staged_copy:
                try:
                    os.remove(local_tar)
                except OSError:
                    pass

    def _restore_packed(self, record, restore_base=None):
        # Full path of the ZIP on tape, remapped to the current drive letter.
        tape_zip_path = self._resolve_tape_path(record['container_name'])
        stored_in_zip = record['stored_path']       # relative path inside the ZIP
        local_zip     = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))

        print(f"\n[RESTORE] Packed file inside {os.path.basename(tape_zip_path)}")
        print("[RESTORE] Step 1/3: Copying ZIP from tape to staging...")

        os.makedirs(self.staging_dir, exist_ok=True)
        if not self._bundle_staging_space_ok(tape_zip_path):
            return
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print("[ERROR] Could not copy ZIP from tape: robocopy error")
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
                if entry is None:
                    print("[ERROR] ZIP entry could not be resolved.")
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
        tape_zip_path = self._resolve_tape_path(tape_zip_path)
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
            print("[ERROR] Could not copy ZIP from tape: robocopy error")
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
                    if entry is None:
                        print(f"[ERROR] {record['file_name']}: ZIP entry could not be resolved.")
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

    # ------------------------------------------------------------------
    # Bundle-complete directory restore (includes small files with no
    # individual files_index row). Driven by the 007 directory catalog +
    # the ZIP's own entry list — never by a per-small-file lookup.
    # ------------------------------------------------------------------

    @staticmethod
    def _canonical_from_zip_entry(base_path, entry_name):
        """Full SOURCE path of a file = its bundle base + the ZIP entry path."""
        entry = str(entry_name).replace("\\", "/").lstrip("/")
        base = str(base_path or "").replace("\\", "/").rstrip("/")
        return (base + "/" + entry) if base else entry

    @staticmethod
    def _entry_under_directory(canonical, dir_path):
        """True if a reconstructed file path lies within the requested dir."""
        c = str(canonical).replace("\\", "/")
        d = str(dir_path).replace("\\", "/").rstrip("/")
        return bool(d) and (c == d or c.startswith(d + "/"))

    def _restore_directory_complete(self, dir_q):
        """Restore a SOURCE directory across ZIP and Stored TAR containers.

        ZIP keeps its historical entry-list routing. Stored TAR uses only its
        selected permanent local sidecar and therefore also restores members
        that have no permanent ``files_index`` row.
        """
        needle = (dir_q or "").strip().strip('"').replace("\\", "/").rstrip("/")
        if not needle:
            return
        try:
            bundles = self.db.find_directory_restore_bundles(needle)
        except RuntimeError as e:
            print(str(e))
            return
        if not bundles:
            print("[RETRIEVER] No directory-catalog bundle found for that path. "
                  "(Bundle-complete restore needs the 007 directory catalog; "
                  "for loose/large files use option 4.)")
            return
        restore_base = posixpath.dirname(needle)
        os.makedirs(self.restore_dir, exist_ok=True)
        by_tape = defaultdict(list)
        for bundle in bundles:
            if not bundle.get("container_format"):
                if bundle.get("container_id") is not None:
                    print("[ERROR] Linked directory container has no persisted "
                          "format; refusing to guess.")
                    return
                bundle = dict(bundle)
                bundle.update({
                    "container_format": ContainerFormat.ZIP.value,
                    "format_version": "legacy-zip-v1",
                    "is_packed": True,
                    "container_name": bundle.get("stored_bundle_path"),
                    "tape_container_locator": bundle.get(
                        "stored_bundle_path"),
                })
            by_tape[bundle["tape_label"]].append(bundle)
        print(f"\n[RESTORE] {len(bundles)} bundle(s) across "
              f"{len(by_tape)} tape(s) cover '{needle}'.")
        total = 0
        for tape_label, tape_bundles in by_tape.items():
            self._check_cancelled()
            if any(self._record_needs_tape(b) for b in tape_bundles):
                self._verify_tape(tape_label)
            for bundle in tape_bundles:
                total += self._extract_container_subtree(
                    bundle, needle, restore_base)
        print(f"\n[RESTORE] Directory restore complete: {total} file(s) "
              f"extracted to {self.restore_dir}")

    def _extract_container_subtree(self, bundle, dir_path, restore_base):
        route = self._route_container_format(bundle)
        if route is ContainerFormat.ZIP:
            return self._extract_bundle_subtree(
                bundle.get("tape_container_locator")
                or bundle["stored_bundle_path"],
                bundle.get("source_base_path") or bundle.get("base_path"),
                dir_path, restore_base)
        if route is not ContainerFormat.STORED_TAR:
            raise RuntimeError("[RESTORE] Directory route is not a container")
        try:
            sidecar = self._load_tar_sidecar(
                bundle, directory=dir_path, limit=1_000_000)
        except ArtifactError as exc:
            print(f"[ERROR] Stored TAR directory restore refused: {exc}")
            return 0
        if not sidecar.matches:
            return 0
        records = []
        for match in sidecar.matches:
            record = dict(bundle)
            record.update(match)
            record["is_packed"] = True
            record["container_name"] = (
                bundle.get("tape_container_locator")
                or bundle.get("stored_bundle_path"))
            records.append(record)
        return self._restore_container(
            records, restore_base=restore_base, sidecar=sidecar)

    def _extract_bundle_subtree(self, tape_zip_path, base_path, dir_path,
                                restore_base):
        """Copy one bundle ZIP from tape and extract only the entries whose
        reconstructed source path is under ``dir_path``. Returns the count."""
        tape_zip_path = self._resolve_tape_path(tape_zip_path)
        local_zip = os.path.join(
            self.staging_dir, os.path.basename(tape_zip_path))
        print(f"\n[RESTORE] Copying {os.path.basename(tape_zip_path)} "
              "from tape to staging...")
        os.makedirs(self.staging_dir, exist_ok=True)
        if not self._bundle_staging_space_ok(tape_zip_path):
            return 0
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print("[ERROR] Could not copy ZIP from tape: robocopy error")
            return 0
        extracted = 0
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                for entry in zf.namelist():
                    if entry.endswith("/"):
                        continue
                    canonical = self._canonical_from_zip_entry(base_path, entry)
                    if not self._entry_under_directory(canonical, dir_path):
                        continue
                    self._check_cancelled()
                    rel = (canonical[len(restore_base):]
                           if restore_base and canonical.startswith(restore_base)
                           else canonical)
                    dst = self._unique_dest_path(os.path.join(
                        self.restore_dir, self._safe_restore_relpath(rel)))
                    os.makedirs(os.path.dirname(os.path.abspath(dst)),
                                exist_ok=True)
                    with zf.open(entry) as zsrc, open(dst, "wb") as out:
                        shutil.copyfileobj(zsrc, out)
                    extracted += 1
                    if extracted % 500 == 0:
                        print(f"[RESTORE] {extracted} file(s) extracted...")
        except Exception as e:
            print(f"[ERROR] Extraction failed for "
                  f"{os.path.basename(tape_zip_path)}: {e}")
        finally:
            try:
                os.remove(local_zip)
            except OSError:
                pass
        print(f"[RESTORE] {os.path.basename(tape_zip_path)}: "
              f"{extracted} file(s) extracted.")
        return extracted
