"""Typed state shared across the archive pipeline.

These replace string-keyed dicts whose typos were silent ``None``s at
runtime: :class:`StagedChunk` crosses the producer -> tape-writer thread
boundary, :class:`StreamState` is the streaming session's shared counters,
and :class:`FileRecord` annotates the packer/catalog metadata records
(annotation only — the records stay plain dicts because the DB layer
consumes them via ``.get()``).
"""
from dataclasses import dataclass, field
from typing import List, Optional, TypedDict


class FileRecord(TypedDict, total=False):
    """One packer/catalog metadata record (see LTOPacker._pack_entries)."""
    file_name: str
    original_path: str
    file_size_bytes: int
    is_packed: bool
    container_name: Optional[str]
    stored_path: str
    canonical_source_path: Optional[str]


@dataclass
class StagedChunk:
    """A fetched-and-packed chunk, queued for the tape writer.

    ``fetch_seconds``/``pack_seconds`` are producer-side timings that overlap
    the previous chunk's tape write (see the SUMMARY.csv notes in AGENTS.md).
    ``skip_tape`` marks a chunk whose every source file went missing — it is
    logged and marked done without any tape I/O.
    """
    chunk_index: int
    fetch_dir: str
    pack_dir: str
    metadata: List[FileRecord]
    staged_bytes: int = 0
    fetch_seconds: Optional[float] = None
    fetch_bytes: Optional[int] = None
    pack_seconds: Optional[float] = None
    pack_bytes: Optional[int] = None
    source_missing_files: list = field(default_factory=list)
    skip_tape: bool = False


@dataclass
class StreamState:
    """Shared counters of a streaming remote session.

    Mutated by the scanner thread and read by the pipeline; every access to
    ``remaining_bytes``/``next_chunk_index`` happens under the session's
    ``remaining_lock`` (see RemoteOrchestrator._run_streaming_session).
    """
    remaining_bytes: int = 0
    next_chunk_index: int = 0
    chunks: int = 0
    files: int = 0
    bytes: int = 0
    scan_error: Optional[str] = None
