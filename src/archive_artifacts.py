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
import io
import json
import os
import posixpath
import uuid

try:
    import zstandard as zstd
except ImportError:                # pragma: no cover - requirements ships it
    zstd = None

from .logsetup import get_logger
from .paths import _long

#: Artifact format version. A reader refuses anything it does not know.
ARTIFACT_VERSION = "scan-segment-v1"

#: Namespaced subdirectory under the local manifest archive root. Keeping
#: operational scan artifacts out of the migration-010 export namespace means a
#: metadata prune of one can never take the other with it.
ARTIFACT_NAMESPACE = "scan_segments"

_HEADER_KIND = "header"
_RECORD_KIND = "entry"


class ArtifactError(RuntimeError):
    """An artifact operation that must not be papered over."""


class ArtifactConflict(ArtifactError):
    """The final artifact name is already taken."""


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


__all__ = [
    "ARTIFACT_NAMESPACE", "ARTIFACT_VERSION", "ArtifactConflict",
    "ArtifactError", "JsonlZstArtifactWriter", "artifact_root",
    "find_orphan_parts", "parse_jsonl_zst_artifact", "resolve_locator",
    "segment_locator",
]
