"""Versioned local scan-segment artifacts (``scan-segment-v1``).

Plan 1, Task 2.2. A frontier scan discovers tens of millions of files. Writing
one PostgreSQL row per discovered file to say "this succeeded" is the cost the
frontier exists to remove, so the per-file detail lives here instead: a
``JSONL.zst`` file under the permanent local manifest root, with only the
range, the aggregate counts and a root-relative locator persisted in the
catalog.

Four rules make these artifacts safe to depend on for recovery:

1. **Publication is atomic.** Records are written to a uniquely-named ``.part``
   file and only ``os.replace``-d into their final name once the stream is
   flushed and closed. A crash therefore leaves an orphan ``.part``, never a
   truncated artifact that a reader would happily parse as complete.

2. **A ready locator is never a ``.part``.** Enforced here, in
   :class:`~src.pg_scan.PgScanMixin` and by the schema's own CHECK — three
   layers, because a locator naming a file still being written is a promise the
   reader cannot keep.

3. **Publication never clobbers.** If the final name already exists, the writer
   refuses and reports it. Two workers producing "the same" segment is a
   frontier bug to surface, not a race to let the later one win.

4. **No content hashes.** The frontier records path, size, ordinal and counts.
   Hashing would mean reading every byte of the source over SSH, which is the
   fetch this pipeline is trying to schedule, not a scan. The residual risk —
   a same-size content replacement is undetectable — is documented rather than
   papered over with a hash nobody can afford to compute.

Artifacts live in their own namespaced subdirectory under
``ConfigManager.local_manifest_archive_root`` so they cannot collide with the
migration-010 small-file manifest exports that share the same root.
"""
import fnmatch
import io
import json
import os
import posixpath
import shutil
import uuid
from dataclasses import dataclass

try:
    import zstandard as zstd
except ImportError:                # pragma: no cover - requirements ships it
    zstd = None

from .logsetup import get_logger
from .paths import _long
from .pipeline_types import SourceDisposition, StoredTarSourceDiagnostic
from .tar_container import (
    STORED_TAR_DIALECT,
    STORED_TAR_FORMAT_VERSION,
    StoredTarError,
    validate_stored_tar_part,
    validate_tar_member_name,
)

#: Artifact format version. A reader refuses anything it does not know.
ARTIFACT_VERSION = "scan-segment-v1"

# Phase 1 establishes the consumer contract before any TAR producer is enabled.
TAR_SIDECAR_VERSION = "tar-sidecar-v1"
TAR_SIDECAR_NAMESPACE = "tar_sidecars"
MAX_TAR_SIDECAR_RECORDS = 1_000_000

#: Namespaced subdirectory under the local manifest archive root. Keeping
#: operational scan artifacts out of the migration-010 export namespace means a
#: metadata prune of one can never take the other with it.
ARTIFACT_NAMESPACE = "scan_segments"

# Plan 3, Task 1.2. Four more versioned artifacts share one publication
# protocol. Each gets its own namespace for the same reason the scan segments
# have one: pruning the metadata of a superseded generation must never be able
# to take a different artifact kind with it.
PLAN_MANIFEST_VERSION = "plan-manifest-v1"
TERMINAL_STATE_VERSION = "terminal-state-v1"
SESSION_DESCRIPTOR_VERSION = "session-descriptor-v1"
SCAN_STATE_SEGMENT_VERSION = "scan-state-segment-v1"

PLAN_MANIFEST_NAMESPACE = "plan_manifests"
TERMINAL_STATE_NAMESPACE = "terminal_states"
SESSION_DESCRIPTOR_NAMESPACE = "session_descriptors"
SCAN_STATE_SEGMENT_NAMESPACE = "scan_states"

_TRAILER_KIND = "trailer"

_HEADER_KIND = "header"
_RECORD_KIND = "entry"


class ArtifactError(RuntimeError):
    """An artifact operation that must not be papered over."""


class ArtifactConflict(ArtifactError):
    """The final artifact name is already taken."""


@dataclass(frozen=True)
class TarSidecarSearchResult:
    """Validated local TAR-sidecar contents and a bounded route selection."""

    header: dict
    expected_members: tuple
    matches: tuple
    footer: dict


@dataclass(frozen=True)
class StoredTarPairPublication:
    """Ready local data/metadata pair after the owner-checked DB commit."""

    tar_path: str
    sidecar_path: str
    sidecar_locator: str
    tar_size: int
    sidecar_size: int
    disposition_counts: dict
    pack_sidecar_path: str = ""
    source_diagnostics: tuple = ()


def _require_zstd():
    if zstd is None:
        raise ArtifactError(
            "[SCAN] zstandard is required for .jsonl.zst scan segments. "
            "Run `python -m pip install -r requirements.txt`.")


def artifact_root(archive_root):
    """The namespaced directory scan segments live in."""
    return os.path.join(os.path.abspath(str(archive_root)), ARTIFACT_NAMESPACE)


def segment_locator(session_id, scan_directory_id, first_scan_ordinal):
    """Root-relative locator for a segment, as stored in PostgreSQL.

    Deliberately relative (and POSIX-separated): the catalog must survive the
    archive root being moved to different storage, which it cannot if it holds
    absolute Windows paths.
    """
    return posixpath.join(
        ARTIFACT_NAMESPACE,
        f"session_{int(session_id):06d}",
        f"dir_{int(scan_directory_id):09d}",
        f"seg_{int(first_scan_ordinal):012d}.jsonl.zst")


def resolve_locator(archive_root, locator):
    """Absolute path of a stored locator, with containment enforced.

    A locator that escapes the archive root (``..``, an absolute path, a drive
    letter) is refused: locators come from the database, and a database is not
    a trust boundary for filesystem paths.
    """
    root = os.path.abspath(str(archive_root))
    text = str(locator or "").strip()
    if not text:
        raise ArtifactError("empty artifact locator")
    if text.startswith(("/", "\\")) or os.path.splitdrive(text)[0]:
        raise ArtifactError(f"artifact locator must be relative: {locator!r}")
    candidate = os.path.abspath(os.path.join(root, text.replace("/", os.sep)))
    try:
        contained = os.path.commonpath([root, candidate]) == root
    except ValueError:                       # different volumes
        contained = False
    if not contained:
        raise ArtifactError(
            f"artifact locator escapes the archive root: {locator!r}")
    return candidate


def is_ltfs_locator(locator, *, tape_root=None, tape_locator=None):
    """Classify a locator lexically, without performing tape I/O."""
    text = str(locator or "").strip()
    if not text:
        return False
    drive = os.path.splitdrive(text)[0].casefold()
    if drive == "z:":
        return True
    tape_drive = os.path.splitdrive(str(tape_root or ""))[0].casefold()
    if drive and tape_drive and drive == tape_drive:
        return True
    if tape_locator:
        left = os.path.normcase(os.path.abspath(text))
        right = os.path.normcase(os.path.abspath(str(tape_locator)))
        if left == right:
            return True
    return False


def resolve_local_metadata_locator(archive_root, locator, *, tape_root=None,
                                   tape_locator=None):
    """Resolve local metadata, refusing LTFS locators before any probe."""
    if is_ltfs_locator(locator, tape_root=tape_root,
                       tape_locator=tape_locator):
        raise ArtifactError(
            "local restore metadata points at LTFS; copy/rebuild the sidecar "
            "on local disk and retry (the tape will not be scanned)")
    text = str(locator or "").strip()
    if not text:
        raise ArtifactError(
            "local TAR sidecar metadata is missing; restore cannot scan tape "
            "to reconstruct it")
    if os.path.isabs(text) or os.path.splitdrive(text)[0]:
        return os.path.abspath(text)
    if not archive_root:
        raise ArtifactError(
            "relative TAR sidecar locator has no configured local metadata root")
    return resolve_locator(archive_root, text)


def _sidecar_source_path(header, record, member_name):
    original = (record.get("canonical_source_path")
                or record.get("original_path") or record.get("source_path"))
    if original:
        return str(original)
    base = str(header.get("source_base_path") or "").rstrip("/")
    return f"{base}/{member_name}" if base else member_name


def search_tar_sidecar(path, *, directory=None, query=None, limit=10_000,
                       expected_version=TAR_SIDECAR_VERSION,
                       expected_container_id=None,
                       expected_member_count=None,
                       max_records=MAX_TAR_SIDECAR_RECORDS):
    """Validate and sequentially search one explicitly selected JSONL.zst.

    Search output and total input records are both bounded.  The complete
    expectation tuple is retained because a restore validates the whole TAR,
    not only the selected members.
    """
    _require_zstd()
    if str(path).endswith(".part"):
        raise ArtifactError("refusing to read an unpublished TAR sidecar .part")
    limit = max(1, int(limit))
    max_records = max(1, int(max_records))
    header = None
    footer = None
    expected = []
    matches = []
    logical_bytes = 0
    previous_ordinal = None
    member_names = set()
    folded_names = set()
    directory_norm = (str(directory or "").replace(
        "\\", "/").rstrip("/") or None)
    query_text = str(query or "").strip() or None

    try:
        raw = open(_long(path), "rb")
    except OSError as exc:
        raise ArtifactError(
            f"local TAR sidecar is unavailable at {path!r}; restore will not "
            "fall back to scanning tape") from exc
    with raw:
        reader = zstd.ZstdDecompressor().stream_reader(raw)
        text = io.TextIOWrapper(reader, encoding="utf-8")
        try:
            for line_number, line in enumerate(text, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except (TypeError, json.JSONDecodeError) as exc:
                    raise ArtifactError(
                        f"invalid TAR sidecar JSON at line {line_number}") from exc
                kind = record.get("kind") or record.get("record_type")
                if header is None:
                    if kind != "header":
                        raise ArtifactError("TAR sidecar must begin with a header")
                    header = record
                    if (expected_version
                            and record.get("version") != expected_version):
                        raise ArtifactError(
                            "unsupported TAR sidecar version "
                            f"{record.get('version')!r}")
                    if (expected_container_id is not None
                            and record.get("container_id") is not None
                            and int(record["container_id"])
                            != int(expected_container_id)):
                        raise ArtifactError(
                            "TAR sidecar container identity mismatch")
                    continue
                if footer is not None:
                    raise ArtifactError("records follow the TAR sidecar footer")
                if kind == "footer":
                    footer = record
                    continue
                if kind not in ("member", "entry", "source_exception"):
                    raise ArtifactError(
                        f"unrecognized TAR sidecar record kind {kind!r}")
                if len(expected) >= max_records:
                    raise ArtifactError(
                        f"TAR sidecar exceeds the bounded "
                        f"{max_records:,}-record reader")

                member_name = record.get("member_name", record.get("name"))
                source_exception = record.get("source_exception")
                if kind == "source_exception" and source_exception is None:
                    source_exception = (record.get("disposition")
                                        or record.get("status"))
                if kind == "source_exception" and member_name is not None:
                    raise ArtifactError(
                        "source-exception sidecar record invents a member name")
                if member_name is not None:
                    try:
                        member_name = validate_tar_member_name(str(member_name))
                    except ValueError as exc:
                        raise ArtifactError(
                            f"unsafe TAR sidecar member at line {line_number}") \
                            from exc
                try:
                    size = int(record.get(
                        "logical_size", record.get(
                            "expected_size", record.get(
                                "file_size_bytes", record.get("size")))))
                    ordinal = int(record.get(
                        "ordinal", record.get(
                            "plan_ordinal", record.get("scan_ordinal"))))
                except (TypeError, ValueError) as exc:
                    raise ArtifactError(
                        f"TAR sidecar member at line {line_number} lacks "
                        "integer size/ordinal") from exc
                if size < 0 or ordinal < 0:
                    raise ArtifactError(
                        "TAR sidecar size/ordinal must be non-negative")
                if previous_ordinal is not None and ordinal <= previous_ordinal:
                    raise ArtifactError(
                        "TAR sidecar ordinals must be strictly ascending")
                previous_ordinal = ordinal
                if member_name is not None:
                    if (member_name in member_names
                            or member_name.casefold() in folded_names):
                        raise ArtifactError(
                            "duplicate or case-fold-colliding TAR sidecar member")
                    member_names.add(member_name)
                    folded_names.add(member_name.casefold())
                item = {
                    "name": member_name,
                    "logical_size": size,
                    "ordinal": ordinal,
                    "source_exception": source_exception,
                }
                expected.append(item)
                if source_exception is not None:
                    allowed_exceptions = {
                        SourceDisposition.SOURCE_MISSING.value,
                        SourceDisposition.SOURCE_PERMISSION_DENIED.value,
                        SourceDisposition.SOURCE_UNREADABLE.value,
                    }
                    if str(source_exception) not in allowed_exceptions:
                        raise ArtifactError(
                            "TAR sidecar contains a blocking source disposition")
                    source_path = _sidecar_source_path(
                        header, record, member_name)
                    if (not source_path or "\0" in source_path
                            or any(part in (".", "..")
                                   for part in source_path.split("/"))):
                        raise ArtifactError(
                            "unsafe source-exception path in TAR sidecar")
                    continue
                observed_size = record.get("observed_archived_size")
                if observed_size is not None and int(observed_size) != size:
                    raise ArtifactError(
                        "TAR sidecar expected/observed member size mismatch")
                logical_bytes += size
                original_path = _sidecar_source_path(
                    header, record, member_name)
                if "\0" in original_path or any(
                        part in (".", "..")
                        for part in original_path.split("/")):
                    raise ArtifactError(
                        f"unsafe canonical source path in TAR sidecar: "
                        f"{original_path!r}")
                source_base = str(header.get("source_base_path") or "").rstrip("/")
                expected_source = (f"{source_base}/{member_name}"
                                   if source_base else None)
                if expected_source is not None and original_path != expected_source:
                    raise ArtifactError(
                        "TAR sidecar source path disagrees with its canonical "
                        "source root/member name")
                selected = True
                if directory_norm:
                    selected = (original_path == directory_norm
                                or original_path.startswith(
                                    directory_norm + "/"))
                if selected and query_text:
                    selected = fnmatch.fnmatchcase(
                        posixpath.basename(original_path).casefold(),
                        query_text.casefold())
                if selected:
                    if len(matches) >= limit:
                        raise ArtifactError(
                            f"TAR sidecar route matches more than {limit:,} "
                            "members; select a narrower directory/container route")
                    matches.append({
                        "member_name": member_name,
                        "stored_path": member_name,
                        "original_path": original_path,
                        "file_name": posixpath.basename(original_path),
                        "file_size_bytes": size,
                        "ordinal": ordinal,
                    })
        finally:
            text.close()

    if header is None or footer is None:
        raise ArtifactError("TAR sidecar is truncated (header/footer missing)")
    present_count = sum(1 for item in expected
                        if item["source_exception"] is None)
    declared_count = footer.get("member_count", header.get("member_count"))
    declared_bytes = footer.get("logical_bytes", header.get("logical_bytes"))
    if declared_count is None or int(declared_count) != present_count:
        raise ArtifactError("TAR sidecar member-count aggregate mismatch")
    if declared_bytes is None or int(declared_bytes) != logical_bytes:
        raise ArtifactError("TAR sidecar logical-byte aggregate mismatch")
    if (expected_member_count is not None
            and int(expected_member_count) != present_count):
        raise ArtifactError("catalog/sidecar member-count mismatch")
    return TarSidecarSearchResult(
        header=dict(header), expected_members=tuple(expected),
        matches=tuple(matches), footer=dict(footer))


class JsonlZstArtifactWriter:
    """Write one scan segment, then publish it atomically.

    Usage::

        with JsonlZstArtifactWriter(root, locator, scope=...) as writer:
            writer.add(path="/vault/a/f", size=10, ordinal=0)
        writer.published            # -> True
        writer.file_count, writer.byte_count

    Leaving the block without an exception publishes; leaving it *with* one
    leaves the ``.part`` behind untouched, so a reconciliation can see that a
    write was interrupted rather than finding nothing at all.
    """

    def __init__(self, archive_root, locator, *, scope=None,
                 scan_directory_id=None, session_id=None,
                 version=ARTIFACT_VERSION):
        _require_zstd()
        self.archive_root = os.path.abspath(str(archive_root))
        self.locator = str(locator)
        self.final_path = resolve_locator(self.archive_root, self.locator)
        # A UNIQUE .part name per attempt: two workers that both believe they
        # own this segment must not write into the same temporary file, or the
        # loser corrupts the winner's stream.
        self.part_path = f"{self.final_path}.{uuid.uuid4().hex[:12]}.part"
        self.scope = scope
        self.scan_directory_id = scan_directory_id
        self.session_id = session_id
        self.version = version

        self.file_count = 0
        self.byte_count = 0
        self.first_ordinal = None
        self.last_ordinal = None
        self.first_path = None
        self.last_path = None
        self.published = False
        self._closed = False
        self._raw = None
        self._compressor = None
        self._text = None

    # -- lifecycle --------------------------------------------------------
    def open(self):
        if self._text is not None:
            return self
        os.makedirs(_long(os.path.dirname(self.final_path)), exist_ok=True)
        self._raw = open(_long(self.part_path), "wb")
        self._compressor = zstd.ZstdCompressor().stream_writer(self._raw)
        self._text = io.TextIOWrapper(self._compressor, encoding="utf-8")
        self._emit({
            "kind": _HEADER_KIND,
            "version": self.version,
            "session_id": self.session_id,
            "scan_directory_id": self.scan_directory_id,
            "scope": self.scope,
        })
        return self

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            # Interrupted: close the stream but leave the .part in place as
            # evidence. Reconciliation decides whether to remove it.
            self.close(publish=False)
            return False
        self.close(publish=True)
        return False

    def _emit(self, payload):
        if self._text is None:
            raise ArtifactError("artifact writer is not open")
        self._text.write(json.dumps(payload, ensure_ascii=False,
                                    separators=(",", ":")))
        self._text.write("\n")

    def add(self, *, path, size, ordinal, entry_hint=None, storage_hint=None):
        """Append one discovered entry.

        ``ordinal`` is the STABLE scan ordinal — the identity a chunk later
        consumes a range of. It must ascend; a caller that supplies a
        descending or duplicate ordinal is refused, because the whole
        consumption model rests on ranges being contiguous and ordered.
        """
        if self._text is None:
            self.open()
        ordinal = int(ordinal)
        if self.last_ordinal is not None and ordinal <= self.last_ordinal:
            raise ArtifactError(
                f"scan ordinals must ascend: {ordinal} follows "
                f"{self.last_ordinal}")
        record = {
            "kind": _RECORD_KIND,
            "path": str(path),
            "size": int(size),
            "ordinal": ordinal,
        }
        if entry_hint is not None:
            record["entry_hint"] = entry_hint
        if storage_hint is not None:
            record["storage_hint"] = storage_hint
        self._emit(record)

        self.file_count += 1
        self.byte_count += int(size)
        if self.first_ordinal is None:
            self.first_ordinal = ordinal
            self.first_path = str(path)
        self.last_ordinal = ordinal
        self.last_path = str(path)
        return ordinal

    def flush(self):
        if self._text is not None:
            self._text.flush()

    def close(self, publish=True):
        """Close the stream, and (by default) publish atomically."""
        if self._closed:
            return self.published
        self._closed = True
        if self._text is not None:
            try:
                self._emit({
                    "kind": "footer",
                    "file_count": self.file_count,
                    "byte_count": self.byte_count,
                    "first_scan_ordinal": self.first_ordinal,
                    "last_scan_ordinal": self.last_ordinal,
                })
                self._text.flush()
                self._text.detach()
                self._compressor.close()
            finally:
                self._raw.close()
                self._text = None
        if not publish:
            return False
        return self.publish()

    def publish(self):
        """Atomically move the ``.part`` into its final name.

        Refuses to clobber an existing final artifact. Two workers producing
        "the same" segment is a frontier defect that must surface, not a race
        whose loser silently overwrites the winner.
        """
        if self.published:
            return True
        if not os.path.exists(_long(self.part_path)):
            raise ArtifactError(
                f"nothing to publish: {self.part_path} is missing")
        if os.path.exists(_long(self.final_path)):
            raise ArtifactConflict(
                f"refusing to overwrite an existing scan segment: "
                f"{self.locator}. Two workers produced the same segment; "
                "reconcile the frontier rather than letting one win.")
        os.replace(_long(self.part_path), _long(self.final_path))
        self.published = True
        get_logger().info(
            "scan_segment_published: locator=%s files=%d bytes=%d "
            "ordinals=%s..%s", self.locator, self.file_count, self.byte_count,
            self.first_ordinal, self.last_ordinal)
        return True

    def abandon(self):
        """Discard an unpublished attempt. Never touches a published file."""
        self.close(publish=False)
        try:
            os.remove(_long(self.part_path))
        except FileNotFoundError:
            pass
        return True


def parse_jsonl_zst_artifact(archive_root, locator, *,
                             expected_version=ARTIFACT_VERSION):
    """Read a published artifact back into ``(header, entries, totals)``.

    Validates the footer's aggregates against what was actually read. A
    mismatch raises: a segment whose counts disagree with its contents cannot
    be used to decide what a chunk already covers.
    """
    _require_zstd()
    path = resolve_locator(archive_root, locator)
    if str(locator).endswith(".part"):
        raise ArtifactError(
            f"refusing to read a .part artifact as ready: {locator!r}")
    header = None
    footer = None
    entries = []
    with open(_long(path), "rb") as raw:
        reader = zstd.ZstdDecompressor().stream_reader(raw)
        text = io.TextIOWrapper(reader, encoding="utf-8")
        for line in text:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            kind = record.get("kind")
            if kind == _HEADER_KIND:
                header = record
            elif kind == _RECORD_KIND:
                entries.append(record)
            elif kind == "footer":
                footer = record

    if header is None:
        raise ArtifactError(f"artifact has no header: {locator!r}")
    if expected_version and header.get("version") != expected_version:
        raise ArtifactError(
            f"unsupported artifact version {header.get('version')!r} "
            f"(expected {expected_version!r}): {locator!r}")
    if footer is None:
        # No footer means the stream was cut before close(); such a file should
        # never have been published, so treat it as unusable rather than
        # partially trusting it.
        raise ArtifactError(
            f"artifact is truncated (no footer): {locator!r}")

    totals = {
        "file_count": len(entries),
        "byte_count": sum(int(e["size"]) for e in entries),
        "first_scan_ordinal": entries[0]["ordinal"] if entries else None,
        "last_scan_ordinal": entries[-1]["ordinal"] if entries else None,
    }
    for key, actual in totals.items():
        declared = footer.get(key)
        if declared != actual:
            raise ArtifactError(
                f"artifact aggregate mismatch in {locator!r}: {key} "
                f"declared={declared!r} actual={actual!r}")
    ordinals = [e["ordinal"] for e in entries]
    if ordinals != sorted(set(ordinals)):
        raise ArtifactError(
            f"artifact ordinals are not strictly ascending: {locator!r}")
    return header, entries, totals


def find_orphan_parts(archive_root):
    """Every ``.part`` under the artifact namespace, absolute paths.

    Orphans are EVIDENCE, not garbage: an interrupted publication is exactly
    what a reconciliation wants to know about. They are removed only when their
    owning attempt is proven absent or expired, which is Task 3.2's job — this
    function only finds them.
    """
    root = artifact_root(archive_root)
    found = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name.endswith(".part"):
                found.append(os.path.join(dirpath, name))
    return sorted(found)


def publish_no_clobber(part_path, final_path):
    """Atomically publish ``part_path`` without ever replacing ``final_path``.

    ``os.replace`` is intentionally forbidden here: its successful race outcome
    is data loss.  Windows rename is already no-clobber; elsewhere a same-volume
    hard-link creation provides the atomic create-if-absent primitive.
    """
    part = os.path.abspath(str(part_path))
    final = os.path.abspath(str(final_path))
    if not os.path.isfile(_long(part)):
        raise ArtifactError(f"publication part is missing: {part}")
    os.makedirs(_long(os.path.dirname(final)), exist_ok=True)
    try:
        if os.name == "nt":
            os.rename(_long(part), _long(final))
        else:  # pragma: no cover - production is Windows; keeps tests portable
            os.link(_long(part), _long(final))
            os.unlink(_long(part))
    except FileExistsError as exc:
        raise ArtifactConflict(
            f"refusing to overwrite published artifact: {final}") from exc
    return final


# ---------------------------------------------------------------------------
# Plan 3, Task 1.2 - the shared header/detail/trailer artifact protocol
# ---------------------------------------------------------------------------
#
# The scan-segment writer above publishes on close.  That was enough for Plan 1,
# where a segment is re-derivable from the source.  A Plan 3 artifact is not:
# a plan manifest IS the membership of a chunk that owns no per-file rows, so a
# file that parses today and not tomorrow loses the chunk.  Publication here is
# therefore two-phase and paranoid in a specific order:
#
#   unique .part -> flush -> close -> fsync (file AND parent directory)
#                -> REOPEN AND PARSE THE WHOLE FILE
#                -> validate counts, bytes, ordinals, cross-artifact identity
#                -> atomic no-clobber rename -> persist locator/size/readiness
#
# The reopen is the point.  A writer validating its own in-memory totals proves
# nothing about the bytes that reached the disk; only reading them back does.


def _safe_label(value, *, what="label"):
    """A filesystem-safe form of a stable logical identity.

    Session labels come from the database and end up in a path, so they get the
    same treatment as any other untrusted path component: a fixed alphabet, no
    separators, no traversal, bounded length.
    """
    text = str(value or "").strip()
    if not text:
        raise ArtifactError(f"empty {what}")
    safe = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in text)
    safe = safe.strip("._-") or "_"
    if len(safe) > 96:
        safe = safe[:96]
    return safe


def plan3_artifact_locator(namespace, session_label, *, chunk_index=None,
                           kind="artifact", version="v1", generation=0,
                           container_ordinal=None):
    """Root-relative locator for a Plan 3 artifact.

    Deterministic and organized by STABLE logical identity - session label and
    chunk index - never by a generated numeric id, because the rebuild in Task
    4.2 regenerates those ids and must still find these files.  The filename
    carries the artifact kind, the schema version and the generation (plus the
    container ordinal where a chunk has several containers), so append-only
    generations never collide.
    """
    parts = [namespace, f"session_{_safe_label(session_label, what='session label')}"]
    if chunk_index is not None:
        parts.append(f"chunk_{int(chunk_index):06d}")
    name = f"{kind}.{version}.g{int(generation):04d}"
    if container_ordinal is not None:
        name += f".c{int(container_ordinal):04d}"
    parts.append(name + ".jsonl.zst")
    return posixpath.join(*parts)


def _fsync_path(path):
    """Best-effort durability for a file and the directory naming it."""
    try:
        fd = os.open(_long(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:                          # pragma: no cover - platform dep.
        pass
    finally:
        os.close(fd)


def _fsync_dir(path):
    if os.name == "nt":
        # Windows has no directory handle to fsync through os.open; the rename
        # itself is the atomic barrier there.
        return
    try:                                     # pragma: no cover - POSIX only
        fd = os.open(_long(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


@dataclass(frozen=True)
class PublishedPlan3Artifact:
    """What the caller persists after a validated publication."""

    locator: str
    path: str
    version: str
    size_bytes: int
    record_count: int
    byte_total: int
    header: dict
    trailer: dict


class Plan3ArtifactWriter:
    """Write one header/detail/trailer artifact and publish it atomically.

    ``validate_record`` is called for every detail record as it is added, and
    the whole file is validated again after being read back from disk.  Both
    matter: the first rejects a bad record while the caller still has context,
    the second proves the published bytes are the ones that were checked.
    """

    def __init__(self, archive_root, locator, *, version, header,
                 validate_record=None, validate_artifact=None,
                 size_field="expected_size", ordinal_field="plan_ordinal",
                 path_field="canonical_path", require_unique_paths=True):
        _require_zstd()
        self.archive_root = os.path.abspath(str(archive_root))
        self.locator = str(locator)
        self.final_path = resolve_locator(self.archive_root, self.locator)
        self.part_path = f"{self.final_path}.{uuid.uuid4().hex[:12]}.part"
        self.version = str(version)
        self.header = dict(header or {})
        self.header.setdefault("kind", _HEADER_KIND)
        self.header["version"] = self.version
        self._validate_record = validate_record
        self._validate_artifact = validate_artifact
        self._size_field = size_field
        self._ordinal_field = ordinal_field
        self._path_field = path_field
        self._require_unique_paths = require_unique_paths

        self.record_count = 0
        self.byte_total = 0
        self.first_ordinal = None
        self.last_ordinal = None
        self.published = False
        self._published = None
        self._closed = False
        self._seen_paths = set()
        self._raw = None
        self._compressor = None
        self._text = None

    # -- lifecycle --------------------------------------------------------
    def open(self):
        if self._text is not None:
            return self
        os.makedirs(_long(os.path.dirname(self.final_path)), exist_ok=True)
        self._raw = open(_long(self.part_path), "wb")
        self._compressor = zstd.ZstdCompressor().stream_writer(self._raw)
        self._text = io.TextIOWrapper(self._compressor, encoding="utf-8")
        self._emit(self.header)
        return self

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.close(publish=False)
            return False
        self.close(publish=True)
        return False

    def _emit(self, payload):
        if self._text is None:
            raise ArtifactError("artifact writer is not open")
        self._text.write(json.dumps(payload, ensure_ascii=False,
                                    separators=(",", ":"), sort_keys=True))
        self._text.write("\n")

    def add(self, record):
        """Append one validated detail record."""
        if self._text is None:
            self.open()
        if not isinstance(record, dict):
            raise ArtifactError("artifact detail record must be a mapping")
        payload = dict(record)
        payload["kind"] = _RECORD_KIND
        if self._validate_record is not None:
            self._validate_record(payload, self.header)

        ordinal = payload.get(self._ordinal_field)
        if ordinal is not None:
            ordinal = int(ordinal)
            if self.last_ordinal is not None and ordinal <= self.last_ordinal:
                raise ArtifactError(
                    f"{self._ordinal_field} must ascend: {ordinal} follows "
                    f"{self.last_ordinal} in {self.locator}")
            if self.first_ordinal is None:
                self.first_ordinal = ordinal
            self.last_ordinal = ordinal

        if self._require_unique_paths and self._path_field:
            path = payload.get(self._path_field)
            if path is not None:
                if path in self._seen_paths:
                    raise ArtifactError(
                        f"duplicate {self._path_field} {path!r} in "
                        f"{self.locator}")
                self._seen_paths.add(path)

        self._emit(payload)
        self.record_count += 1
        size = payload.get(self._size_field) if self._size_field else None
        if size is not None:
            self.byte_total += int(size)
        return self.record_count

    def close(self, publish=True):
        if self._closed:
            return self.published
        self._closed = True
        if self._text is not None:
            try:
                self._emit({
                    "kind": _TRAILER_KIND,
                    "record_count": self.record_count,
                    "byte_total": self.byte_total,
                    "first_ordinal": self.first_ordinal,
                    "last_ordinal": self.last_ordinal,
                })
                self._text.flush()
                self._text.detach()
                # Closing the zstd stream writer also closes the raw file it
                # wraps, so durability is taken on the path in publish() rather
                # than on a handle that may already be gone.
                self._compressor.close()
            finally:
                if not self._raw.closed:
                    self._raw.flush()
                    os.fsync(self._raw.fileno())
                    self._raw.close()
                self._text = None
        if not publish:
            return False
        return self.publish()

    def publish(self):
        """Reopen, parse, validate, then atomically claim the final name."""
        if self.published:
            return self._published
        if not os.path.isfile(_long(self.part_path)):
            raise ArtifactError(f"nothing to publish: {self.part_path}")
        _fsync_path(self.part_path)

        # Phase two: the bytes on disk are the evidence, not our counters.
        header, records, trailer = _parse_plan3_stream(
            self.part_path, expected_version=self.version, allow_part=True)
        self._validate_parsed(header, records, trailer)

        final = publish_no_clobber(self.part_path, self.final_path)
        _fsync_dir(os.path.dirname(final))
        self.published = True
        self._published = PublishedPlan3Artifact(
            locator=self.locator, path=final, version=self.version,
            size_bytes=os.path.getsize(_long(final)),
            record_count=len(records),
            byte_total=int(trailer.get("byte_total") or 0),
            header=header, trailer=trailer)
        get_logger().info(
            "plan3_artifact_published: kind=%s locator=%s records=%d bytes=%d",
            self.version, self.locator, len(records),
            self._published.size_bytes)
        return self._published

    def _validate_parsed(self, header, records, trailer):
        if len(records) != self.record_count:
            raise ArtifactError(
                f"{self.locator}: wrote {self.record_count} records but read "
                f"back {len(records)}")
        if int(trailer.get("record_count", -1)) != len(records):
            raise ArtifactError(
                f"{self.locator}: trailer record_count "
                f"{trailer.get('record_count')!r} != {len(records)}")
        if self._size_field:
            actual = sum(int(r[self._size_field]) for r in records
                         if r.get(self._size_field) is not None)
            if int(trailer.get("byte_total", -1)) != actual:
                raise ArtifactError(
                    f"{self.locator}: trailer byte_total "
                    f"{trailer.get('byte_total')!r} != {actual}")
        if self._ordinal_field:
            ordinals = [int(r[self._ordinal_field]) for r in records
                        if r.get(self._ordinal_field) is not None]
            if ordinals != sorted(set(ordinals)):
                raise ArtifactError(
                    f"{self.locator}: {self._ordinal_field} values are not "
                    "strictly ascending and unique")
        if self._require_unique_paths and self._path_field:
            paths = [r.get(self._path_field) for r in records
                     if r.get(self._path_field) is not None]
            if len(paths) != len(set(paths)):
                raise ArtifactError(
                    f"{self.locator}: duplicate {self._path_field} on reparse")
        if self._validate_artifact is not None:
            self._validate_artifact(header, records, trailer)

    def abandon(self):
        """Discard an unpublished attempt; never touches a published file."""
        self.close(publish=False)
        try:
            os.remove(_long(self.part_path))
        except FileNotFoundError:
            pass
        return True


def _parse_plan3_stream(path, *, expected_version, allow_part=False):
    """Read one artifact file into ``(header, records, trailer)``."""
    _require_zstd()
    text_path = str(path)
    if not allow_part and text_path.endswith(".part"):
        raise ArtifactError(
            f"refusing to read a .part artifact as ready: {text_path!r}")
    header = None
    trailer = None
    records = []
    try:
        with open(_long(text_path), "rb") as raw:
            reader = zstd.ZstdDecompressor().stream_reader(raw)
            stream = io.TextIOWrapper(reader, encoding="utf-8")
            for line in stream:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                kind = record.get("kind")
                if kind == _HEADER_KIND:
                    header = record
                elif kind == _RECORD_KIND:
                    records.append(record)
                elif kind == _TRAILER_KIND:
                    trailer = record
    except (zstd.ZstdError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactError(
            f"artifact is unreadable (truncated or corrupt): "
            f"{text_path!r}: {exc}") from exc

    if header is None:
        raise ArtifactError(f"artifact has no header: {text_path!r}")
    if expected_version and header.get("version") != expected_version:
        raise ArtifactError(
            f"unsupported artifact version {header.get('version')!r} "
            f"(expected {expected_version!r}): {text_path!r}")
    if trailer is None:
        raise ArtifactError(f"artifact is truncated (no trailer): {text_path!r}")
    return header, records, trailer


def read_plan3_artifact(archive_root, locator, *, expected_version,
                        validate_artifact=None):
    """Parse a published Plan 3 artifact and re-check its own aggregates.

    Every consumer goes through here, so an artifact whose trailer disagrees
    with its contents is refused at read time as well as at publication time.
    """
    path = resolve_locator(os.path.abspath(str(archive_root)), locator)
    header, records, trailer = _parse_plan3_stream(
        path, expected_version=expected_version)
    declared = trailer.get("record_count")
    if declared is not None and int(declared) != len(records):
        raise ArtifactError(
            f"artifact aggregate mismatch in {locator!r}: record_count "
            f"declared={declared!r} actual={len(records)}")
    if validate_artifact is not None:
        validate_artifact(header, records, trailer)
    return header, records, trailer


def tar_sidecar_locator(session_id, chunk_index, container_ordinal):
    return posixpath.join(
        TAR_SIDECAR_NAMESPACE,
        f"session_{int(session_id):06d}",
        f"chunk_{int(chunk_index):06d}",
        f"container_{int(container_ordinal):04d}.jsonl.zst")


def _record_value(record, *names, default=None):
    if isinstance(record, dict):
        for name in names:
            if name in record:
                return record[name]
        return default
    for name in names:
        if hasattr(record, name):
            return getattr(record, name)
    return default


def _coerce_sidecar_plan(plan_members, container_ordinal):
    entries = []
    ordinals = set()
    names = set()
    for raw in plan_members:
        ordinal = int(_record_value(
            raw, "plan_ordinal", "ordinal", "scan_ordinal"))
        assigned = _record_value(
            raw, "container_ordinal", default=container_ordinal)
        if assigned is None or int(assigned) != int(container_ordinal):
            raise ArtifactError(
                f"plan ordinal {ordinal} belongs to another container")
        name = _record_value(raw, "member_name", "name", "remote_path")
        size = _record_value(
            raw, "expected_size", "expected_logical_bytes", "logical_size",
            "file_size_bytes", "size")
        canonical = _record_value(
            raw, "canonical_source_path", "source_path", "original_path",
            default=name)
        if name is None or size is None or canonical is None:
            raise ArtifactError("sidecar plan entry lacks name/size/source path")
        name = validate_tar_member_name(str(name))
        canonical = str(canonical)
        if "\0" in canonical or any(
                component in (".", "..")
                for component in canonical.split("/")):
            raise ArtifactError(
                f"unsafe canonical source path: {canonical!r}")
        if ordinal in ordinals:
            raise ArtifactError(f"duplicate plan ordinal {ordinal}")
        if name in names:
            raise ArtifactError(f"duplicate planned member {name!r}")
        ordinals.add(ordinal)
        names.add(name)
        entries.append({
            "plan_ordinal": ordinal,
            "member_name": name,
            "canonical_source_path": canonical,
            "expected_size": int(size),
            "container_ordinal": int(container_ordinal),
        })
    entries.sort(key=lambda item: item["plan_ordinal"])
    return tuple(entries)


def _coerce_sidecar_diagnostics(source_diagnostics):
    result = {}
    for raw in source_diagnostics or ():
        if isinstance(raw, StoredTarSourceDiagnostic):
            item = raw
        else:
            try:
                item = StoredTarSourceDiagnostic(
                    plan_ordinal=int(_record_value(
                        raw, "plan_ordinal", "ordinal")),
                    path=str(_record_value(raw, "path", "member_name", "name")),
                    disposition=SourceDisposition(_record_value(
                        raw, "disposition", "status")),
                    evidence=str(_record_value(raw, "evidence", default="")),
                )
            except (TypeError, ValueError) as exc:
                raise ArtifactError("invalid source diagnostic") from exc
        if item.plan_ordinal in result:
            raise ArtifactError(
                f"duplicate source diagnostic ordinal {item.plan_ordinal}")
        result[item.plan_ordinal] = item
    return result


def _sidecar_records(plan, validation, diagnostics, *, session_id,
                     chunk_index, container_id, container_ordinal,
                     tar_size):
    members = {item.ordinal: item for item in validation.members}
    records = [{
        "record_type": "header",
        "version": TAR_SIDECAR_VERSION,
        "session_id": int(session_id),
        "chunk_index": int(chunk_index),
        "container_id": int(container_id),
        "container_ordinal": int(container_ordinal),
        "format_version": STORED_TAR_FORMAT_VERSION,
        "tar_dialect": STORED_TAR_DIALECT,
        "tar_size_bytes": int(tar_size),
        "plan_ordinal_count": len(plan),
    }]
    logical_bytes = 0
    disposition_counts = {item.value: 0 for item in SourceDisposition}
    for expected in plan:
        ordinal = expected["plan_ordinal"]
        diagnostic = diagnostics.get(ordinal)
        member = members.get(ordinal)
        identity = {
            "session_id": int(session_id),
            "chunk_index": int(chunk_index),
            "container_id": int(container_id),
            "container_ordinal": int(container_ordinal),
            "plan_ordinal": ordinal,
            "ordinal": ordinal,
            "canonical_source_path": expected["canonical_source_path"],
            "expected_size": expected["expected_size"],
            "logical_size": expected["expected_size"],
        }
        if diagnostic is not None:
            if member is not None:
                raise ArtifactError(
                    f"ordinal {ordinal} is both archived and a source exception")
            if diagnostic.path != expected["member_name"]:
                raise ArtifactError(
                    f"source diagnostic path mismatch at ordinal {ordinal}")
            if diagnostic.disposition not in {
                    SourceDisposition.SOURCE_MISSING,
                    SourceDisposition.SOURCE_PERMISSION_DENIED,
                    SourceDisposition.SOURCE_UNREADABLE}:
                raise ArtifactError(
                    f"{diagnostic.disposition.value} blocks sidecar readiness")
            records.append({
                "record_type": "source_exception",
                **identity,
                "disposition": diagnostic.disposition.value,
                "source_exception": diagnostic.disposition.value,
                "evidence": diagnostic.evidence,
                "diagnostic_path": diagnostic.path,
            })
            disposition_counts[diagnostic.disposition.value] += 1
            continue
        if member is None:
            raise ArtifactError(
                f"plan ordinal {ordinal} has neither TAR member nor diagnostic")
        if (member.name != expected["member_name"]
                or member.logical_size != expected["expected_size"]):
            disposition_counts[SourceDisposition.SOURCE_CHANGED.value] += 1
            raise ArtifactError(
                f"source_changed at plan ordinal {ordinal}; TAR is rejected")
        records.append({
            "record_type": "member",
            **identity,
            "member_name": member.name,
            "observed_archived_size": member.logical_size,
            "stored_size": member.stored_size,
            "disposition": SourceDisposition.ARCHIVED.value,
            "sparse": bool(member.sparse),
            "sparse_extent_count": int(member.sparse_extent_count),
        })
        disposition_counts[SourceDisposition.ARCHIVED.value] += 1
        logical_bytes += member.logical_size
    if sum(disposition_counts.values()) != len(plan):
        raise ArtifactError("sidecar records do not account for every plan ordinal")
    records.append({
        "record_type": "footer",
        "plan_ordinal_count": len(plan),
        "member_count": validation.member_count,
        "logical_bytes": logical_bytes,
        "tar_size_bytes": int(tar_size),
        "disposition_counts": disposition_counts,
    })
    return records, disposition_counts


def _write_tar_sidecar_part(part_path, records):
    _require_zstd()
    os.makedirs(_long(os.path.dirname(part_path)), exist_ok=True)
    raw = open(_long(part_path), "xb")
    compressed = None
    try:
        compressed = zstd.ZstdCompressor().stream_writer(raw, closefd=False)
        text = io.TextIOWrapper(compressed, encoding="utf-8", newline="\n")
        try:
            for record in records:
                text.write(json.dumps(
                    record, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")))
                text.write("\n")
            text.flush()
            text.detach()
            compressed.close()
            raw.flush()
            os.fsync(raw.fileno())
        finally:
            if compressed is not None and not compressed.closed:
                compressed.close()
    finally:
        raw.close()
    return os.path.getsize(_long(part_path))


def _read_sidecar_records(path, *, allow_part=False):
    _require_zstd()
    if str(path).endswith(".part") and not allow_part:
        raise ArtifactError("refusing to read an unpublished TAR sidecar .part")
    records = []
    try:
        with open(_long(path), "rb") as raw:
            reader = zstd.ZstdDecompressor().stream_reader(raw)
            text = io.TextIOWrapper(reader, encoding="utf-8")
            try:
                for line_number, line in enumerate(text, 1):
                    if not line.strip():
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        raise ArtifactError(
                            f"invalid TAR sidecar JSON at line {line_number}") \
                            from exc
            finally:
                text.close()
    except (OSError, zstd.ZstdError) as exc:
        raise ArtifactError(f"cannot read TAR sidecar {path!r}") from exc
    return records


def validate_tar_sidecar(path, plan_members, validation, source_diagnostics,
                         *, session_id, chunk_index, container_id,
                         container_ordinal, tar_size, allow_part=False):
    """Prove a complete sidecar is exactly equivalent to plan + TAR parse."""
    plan = _coerce_sidecar_plan(plan_members, container_ordinal)
    diagnostics = _coerce_sidecar_diagnostics(source_diagnostics)
    expected, counts = _sidecar_records(
        plan, validation, diagnostics, session_id=session_id,
        chunk_index=chunk_index, container_id=container_id,
        container_ordinal=container_ordinal, tar_size=tar_size)
    actual = _read_sidecar_records(path, allow_part=allow_part)
    if actual != expected:
        raise ArtifactError("TAR sidecar is not equivalent to its plan/TAR")
    return counts


def read_tar_sidecar_diagnostics(path, *, session_id, chunk_index,
                                 container_id, container_ordinal):
    """Recover only the trusted source-exception evidence from a final sidecar.

    This is intentionally not an inference from missing TAR members.  The
    records are read from the already-published permanent sidecar and its
    container identity is checked before the caller uses them to perform the
    full plan/TAR/sidecar equivalence validation.
    """
    if str(path).endswith(".part"):
        raise ArtifactError(
            "refusing to recover diagnostics from an unpublished sidecar")
    records = _read_sidecar_records(path)
    if not records or records[0].get("record_type") != "header":
        raise ArtifactError("TAR sidecar must begin with a header")
    header = records[0]
    expected_identity = {
        "session_id": int(session_id),
        "chunk_index": int(chunk_index),
        "container_id": int(container_id),
        "container_ordinal": int(container_ordinal),
    }
    mismatch = [
        key for key, value in expected_identity.items()
        if header.get(key) is None or int(header[key]) != value]
    if (header.get("version") != TAR_SIDECAR_VERSION or mismatch):
        raise ArtifactError(
            "TAR sidecar header identity/version mismatch"
            + (f": {', '.join(mismatch)}" if mismatch else ""))

    diagnostics = []
    for record in records[1:]:
        if record.get("record_type") != "source_exception":
            continue
        try:
            diagnostics.append(StoredTarSourceDiagnostic(
                plan_ordinal=int(record["plan_ordinal"]),
                path=str(record["diagnostic_path"]),
                disposition=SourceDisposition(record["disposition"]),
                evidence=str(record.get("evidence") or ""),
            ))
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactError(
                "invalid source-exception evidence in TAR sidecar") from exc
    return tuple(diagnostics)


def _files_equal(left, right, chunk_size=1024 * 1024):
    try:
        if os.path.getsize(_long(left)) != os.path.getsize(_long(right)):
            return False
        with open(_long(left), "rb") as one, open(_long(right), "rb") as two:
            while True:
                a = one.read(chunk_size)
                b = two.read(chunk_size)
                if a != b:
                    return False
                if not a:
                    return True
    except OSError:
        return False


def _copy_ready_sidecar_to_pack(sidecar_path, pack_dir):
    if not pack_dir:
        return ""
    os.makedirs(_long(pack_dir), exist_ok=True)
    final = os.path.join(pack_dir, os.path.basename(sidecar_path))
    if os.path.exists(_long(final)):
        if not _files_equal(sidecar_path, final):
            raise ArtifactConflict(
                f"pack sidecar conflicts with permanent copy: {final}")
        return final
    part = f"{final}.{uuid.uuid4().hex[:12]}.part"
    try:
        with open(_long(sidecar_path), "rb") as source, \
                open(_long(part), "xb") as target:
            shutil.copyfileobj(source, target, 1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        try:
            publish_no_clobber(part, final)
        except ArtifactConflict:
            if not _files_equal(sidecar_path, final):
                raise
            os.remove(_long(part))
    except Exception:
        # The authoritative permanent sidecar is never removed by pack cleanup
        # or by a failed co-location attempt.
        try:
            os.remove(_long(part))
        except OSError:
            pass
        raise
    return final


def publish_stored_tar_pair(
        archive_root, tar_part_path, final_tar_path, plan_members,
        source_diagnostics, *, validation=None, session_id, chunk_index,
        container_id, container_ordinal, owner_token, db, pack_dir=None,
        crash_hook=None):
    """Sidecar-first filesystem publication followed by one paired DB CAS."""
    hook = crash_hook or (lambda _stage: None)
    archive_root = os.path.abspath(str(archive_root))
    tar_part_path = os.path.abspath(str(tar_part_path))
    final_tar_path = os.path.abspath(str(final_tar_path))
    sidecar_locator = tar_sidecar_locator(
        session_id, chunk_index, container_ordinal)
    sidecar_path = resolve_locator(archive_root, sidecar_locator)
    sidecar_existed = os.path.isfile(_long(sidecar_path))
    tar_final_existed = os.path.isfile(_long(final_tar_path))

    diagnostics = tuple(source_diagnostics or ())
    # A final TAR without its sidecar has already crossed the ordering boundary.
    # Reconstruct metadata only for an all-present exact plan, never from an
    # exception set whose sole trusted durable copy should have been the sidecar.
    if tar_final_existed and not sidecar_existed and diagnostics:
        raise ArtifactError(
            "final TAR has no sidecar and source exceptions existed; refuse "
            "to infer or reconstruct exception evidence")

    validation_source = (
        tar_part_path if os.path.isfile(_long(tar_part_path))
        else final_tar_path)
    if not os.path.isfile(_long(validation_source)):
        raise ArtifactError("neither validated TAR part nor final TAR exists")
    try:
        current_validation = validate_stored_tar_part(
            validation_source, plan_members,
            container_ordinal=container_ordinal,
            source_diagnostics=diagnostics,
            require_part=validation_source.endswith(".part"))
    except StoredTarError as exc:
        raise ArtifactError(f"Stored TAR revalidation failed: {exc}") from exc
    if validation is not None and current_validation != validation:
        raise ArtifactError("Stored TAR validation summary changed before publish")
    validation = current_validation
    tar_size = validation.archive_size

    if sidecar_existed:
        counts = validate_tar_sidecar(
            sidecar_path, plan_members, validation, diagnostics,
            session_id=session_id, chunk_index=chunk_index,
            container_id=container_id, container_ordinal=container_ordinal,
            tar_size=tar_size)
    else:
        plan = _coerce_sidecar_plan(plan_members, container_ordinal)
        diag_map = _coerce_sidecar_diagnostics(diagnostics)
        records, counts = _sidecar_records(
            plan, validation, diag_map, session_id=session_id,
            chunk_index=chunk_index, container_id=container_id,
            container_ordinal=container_ordinal, tar_size=tar_size)
        sidecar_part = f"{sidecar_path}.{uuid.uuid4().hex[:12]}.part"
        _write_tar_sidecar_part(sidecar_part, records)
        validate_tar_sidecar(
            sidecar_part, plan_members, validation, diagnostics,
            session_id=session_id, chunk_index=chunk_index,
            container_id=container_id, container_ordinal=container_ordinal,
            tar_size=tar_size, allow_part=True)
        hook("after_sidecar_validation")
        try:
            publish_no_clobber(sidecar_part, sidecar_path)
        except ArtifactConflict:
            validate_tar_sidecar(
                sidecar_path, plan_members, validation, diagnostics,
                session_id=session_id, chunk_index=chunk_index,
                container_id=container_id, container_ordinal=container_ordinal,
                tar_size=tar_size)
            try:
                os.remove(_long(sidecar_part))
            except OSError:
                pass
        hook("after_sidecar_publication")

    # The permanent sidecar exists and is fully equivalent before the data name
    # can become final.  A racing existing final is reused only after a bounded
    # byte-for-byte comparison and a fresh semantic parse.
    if os.path.isfile(_long(final_tar_path)):
        if os.path.isfile(_long(tar_part_path)):
            if not _files_equal(tar_part_path, final_tar_path):
                raise ArtifactConflict(
                    "same-name final TAR conflicts with the validated part")
        validate_stored_tar_part(
            final_tar_path, plan_members, container_ordinal=container_ordinal,
            source_diagnostics=diagnostics, require_part=False)
    else:
        try:
            publish_no_clobber(tar_part_path, final_tar_path)
        except ArtifactConflict:
            if not _files_equal(tar_part_path, final_tar_path):
                raise
            validate_stored_tar_part(
                final_tar_path, plan_members,
                container_ordinal=container_ordinal,
                source_diagnostics=diagnostics, require_part=False)
            try:
                os.remove(_long(tar_part_path))
            except OSError:
                pass
        hook("after_tar_publication")

    sidecar_size = os.path.getsize(_long(sidecar_path))
    pack_sidecar_path = _copy_ready_sidecar_to_pack(sidecar_path, pack_dir)
    hook("before_paired_db_commit")
    row = db.publish_stored_tar_pair(
        container_id=int(container_id), owner_token=str(owner_token),
        sidecar_locator=sidecar_locator,
        sidecar_version=TAR_SIDECAR_VERSION,
        sidecar_size_bytes=sidecar_size,
        temporary_data_locator=final_tar_path,
        tar_size_bytes=tar_size,
        observed_member_count=validation.member_count,
        observed_logical_bytes=validation.logical_bytes,
        disposition_counts=counts,
    )
    if not row:
        raise ArtifactError("owner-checked paired TAR publication was refused")
    hook("after_paired_db_commit")
    return StoredTarPairPublication(
        tar_path=final_tar_path, sidecar_path=sidecar_path,
        sidecar_locator=sidecar_locator, tar_size=tar_size,
        sidecar_size=sidecar_size, disposition_counts=dict(counts),
        pack_sidecar_path=pack_sidecar_path,
        source_diagnostics=tuple(diagnostics))


__all__ = [
    "ARTIFACT_NAMESPACE", "ARTIFACT_VERSION", "TAR_SIDECAR_VERSION",
    "ArtifactConflict", "ArtifactError", "JsonlZstArtifactWriter",
    "StoredTarPairPublication", "TarSidecarSearchResult", "artifact_root",
    "find_orphan_parts",
    "is_ltfs_locator", "parse_jsonl_zst_artifact",
    "publish_no_clobber", "publish_stored_tar_pair",
    "read_tar_sidecar_diagnostics",
    "resolve_local_metadata_locator", "resolve_locator",
    "search_tar_sidecar", "segment_locator", "tar_sidecar_locator",
    "validate_tar_sidecar",
]
