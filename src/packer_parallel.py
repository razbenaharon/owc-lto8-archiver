"""Thread-parallel PACK path for :class:`~src.packer.LTOPacker` (opt-in).

The default serial packer (``LTOPacker._pack_entries``) is single-threaded and,
for chunks of hundreds of thousands of ~KB files, is per-file-latency bound: the
box sits mostly idle while one thread opens/reads/zips each file in turn. This
module keeps that exact per-file logic — the same truncation-safe spool, the
same manifest writer, the same StagingSpaceBudget accounting, the same governor
checkpoints — and merely fans it out across a small pool of worker threads, each
owning a **disjoint shard** of the file list and its **own uniquely-named**
bundle(s)/manifest(s). Nothing about integrity, restore, or the tape path
changes; only dispatch and the shared-resource plumbing live here.

Safety invariants (verified against resource_governor.ResourceGovernor and
packer.LTOPacker):

* No shared mutable ``LTOPacker`` state is written — every ``_pack_entries``
  call uses only local ``zipf``/manifest/metadata/counter variables and
  read-only ``self`` config, so concurrent calls on one instance are safe.
* Each worker uses a distinct ``bundle_prefix`` (``Bundle_wNN``) → distinct ZIP
  and manifest filenames → no path collisions; ``container_name`` in the merged
  metadata still points at the real ZIP, so restore is unaffected.
* One shared, thread-safe ``StagingSpaceBudget`` backs all workers, so the disk
  free-space guard accounts for every worker's writes (no independent
  over-commit).
* ``SkippedFileTracker`` is already lock-guarded; workers share it.
* Every worker passes through the shared ``ResourceGovernor`` at each
  ``pack_file_batch_size`` checkpoint via ``wait_or_pause("pack", "continue")``.
  A pending/active tape write therefore pauses **all** workers exactly as it
  pauses the serial packer — the tape's exclusivity and RAM reserve are intact.
* A worker that raises (StagingSpaceError, or any unexpected error the per-file
  loop does not already funnel into the skipped tracker) aborts the whole chunk
  by re-raising, so the caller cleans staging and the chunk stays resumable —
  identical to the serial contract.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from .packer import StagingSpaceBudget
from .skipped import SkippedFileTracker


def _shard(entries, workers):
    """Round-robin split so loose (large) files and directory locality spread
    evenly across workers instead of piling onto one shard."""
    buckets = [[] for _ in range(workers)]
    for index, entry in enumerate(entries):
        buckets[index % workers].append(entry)
    return [bucket for bucket in buckets if bucket]


def pack_entries_parallel(packer, dest, threshold_mb, entries, *, workers,
                          source_root=None, bundle_prefix="Bundle",
                          skipped_tracker=None, source_name='local',
                          session_id=None, chunk_index=None, governor=None,
                          pack_file_batch_size=10000):
    """Pack ``entries`` into ``dest`` using ``workers`` worker threads.

    Returns the merged metadata list (concatenation of each worker's records).
    Order is not significant downstream: ``db._apply_canonical_remote_paths``
    matches by ``stored_path`` and the DB sync is set-based.
    """
    workers = max(1, int(workers or 1))
    entries = list(entries)
    skipped_tracker = skipped_tracker or SkippedFileTracker()

    shards = _shard(entries, workers)
    # Degenerate cases (1 shard, e.g. workers==1 or a single file) → serial path
    # so we never spin up a pool for nothing.
    if len(shards) <= 1:
        return packer._pack_entries(
            dest, threshold_mb, entries,
            source_root=source_root,
            bundle_prefix=bundle_prefix,
            skipped_tracker=skipped_tracker,
            source_name=source_name,
            session_id=session_id,
            chunk_index=chunk_index,
            governor=governor,
            pack_file_batch_size=pack_file_batch_size,
            heading="Offline phase - tape idle",
            done_label="Offline phase done",
        )

    # One shared free-space budget for the whole chunk (all shards write to the
    # same disk). Sized from the full chunk, not a shard, so the reserve/overhead
    # is enforced once against real free space.
    total_bytes = sum(int(entry.get('size') or 0) for entry in entries)
    budget = StagingSpaceBudget(
        dest, total_bytes, context="Parallel pack (tape idle)")

    print(f"\n[PACKER] Parallel pack: {len(entries):,} file(s) across "
          f"{len(shards)} worker(s). "
          f"(Threshold: {threshold_mb:.0f} MB | "
          f"Max ZIP: {packer.max_zip_size_gb:.0f} GB)")

    def _run_shard(worker_id, shard):
        return packer._pack_entries(
            dest, threshold_mb, shard,
            source_root=source_root,
            bundle_prefix=f"{bundle_prefix}_w{worker_id:02d}",
            skipped_tracker=skipped_tracker,
            source_name=source_name,
            session_id=session_id,
            chunk_index=chunk_index,
            governor=governor,
            pack_file_batch_size=pack_file_batch_size,
            heading=f"Parallel pack worker {worker_id:02d}",
            done_label=f"Parallel pack worker {worker_id:02d} done",
            budget=budget,
            quiet_progress=True,
        )

    results = [None] * len(shards)
    errors = []
    with ThreadPoolExecutor(max_workers=len(shards),
                            thread_name_prefix="pack") as pool:
        future_to_id = {
            pool.submit(_run_shard, worker_id, shard): worker_id
            for worker_id, shard in enumerate(shards)
        }
        for future in as_completed(future_to_id):
            worker_id = future_to_id[future]
            try:
                results[worker_id] = future.result()
            except Exception as exc:  # noqa: BLE001 - surfaced below
                errors.append((worker_id, exc))

    if errors:
        # Match the serial packer's fail-hard contract: a worker that raised
        # (e.g. StagingSpaceError, or a genuine bug — not a per-file skip, which
        # is already recorded) fails the chunk so the caller cleans staging and
        # the chunk stays resumable. Never return partial metadata.
        detail = "; ".join(
            f"worker {wid}: {type(exc).__name__}: {exc}" for wid, exc in errors)
        raise RuntimeError(f"[PACKER] parallel pack worker(s) failed: {detail}")

    metadata = []
    for shard_metadata in results:
        if shard_metadata:
            metadata.extend(shard_metadata)

    total_packed = sum(1 for m in metadata if m.get('is_packed'))
    total_loose = len(metadata) - total_packed
    print(f"\n[PACKER] Parallel pack done: {total_packed:,} packed | "
          f"{total_loose:,} loose across {len(shards)} worker(s).")
    return metadata
