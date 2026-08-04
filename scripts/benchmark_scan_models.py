"""Compare the three candidate scan models with measurements, not argument.

Plan 1, Task 0.2. The decision this harness has to inform is whether to keep
the current whole-root replay scanner, require a full inventory before any
chunk is released, or persist an incremental directory frontier. Each model is
scored on four independent axes, reported separately so one cannot hide inside
another:

  exploration        time and entries spent listing the source
  database membership round trips, rows and elapsed time spent proving a
                     rediscovered path is already known
  replay             entries re-enumerated after a restart that were already
                     known — the cost the frontier exists to remove
  time-to-first      seconds until the first sealed chunk, so a model that is
                     cheaper overall but starves the tape is visible as such

Two modes:

  --replay LISTING   Offline. Feeds a recorded ``find`` listing through each
                     model with an increasing known-path set, simulating N
                     restarts. Touches no network, no PostgreSQL, no tape.

  --pg-benchmark     Isolated PostgreSQL only. Populates snapshot/plan rows at
                     increasing cardinality and measures the real membership
                     query, reporting catalog cardinality, SQL executions, rows
                     and elapsed time as four separate figures.

The PostgreSQL mode refuses to run against anything but an explicitly named
test database: it creates and drops fixtures, and must never see production.

    python scripts/benchmark_scan_models.py --replay listing.txt --restarts 3
    python scripts/benchmark_scan_models.py --replay listing.txt --synthetic 200000
    python scripts/benchmark_scan_models.py --pg-benchmark \\
        --dsn "postgresql://user@localhost/lto_archive_test" \\
        --cardinalities 1000,10000,100000
"""
import argparse
import json
import os
import posixpath
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline_types import ScanMetrics          # noqa: E402
from src.planning import StreamingChunkBuilder      # noqa: E402

#: A test database must say so in its name. The benchmark creates and drops
#: tables, so an ambiguous DSN is refused rather than guessed at.
_REQUIRED_DSN_MARKERS = ("test", "bench", "scratch")


# =============================================================================
# Listing input
# =============================================================================
def parse_listing(path):
    """Read a recorded listing into ``[(remote_path, size), ...]``.

    Accepts the NUL-framed ``%s %p\\0`` output of the production find command
    and the newline-framed equivalent, so an operator can capture either.
    """
    with open(path, "rb") as handle:
        raw = handle.read()
    separator = b"\0" if b"\0" in raw else b"\n"
    entries = []
    for record in raw.split(separator):
        if not record.strip():
            continue
        text = record.decode("utf-8", errors="replace")
        size_s, _, file_path = text.partition(" ")
        if not file_path:
            continue
        try:
            entries.append((file_path.strip(), int(size_s)))
        except ValueError:
            continue
    return entries


def synthetic_listing(count, root="/vault/synthetic", seed=7):
    """A deterministic stand-in listing when no real capture is available.

    Deliberately lumpy: a long tail of tiny files with occasional large ones,
    which is what makes chunk boundaries move between runs.
    """
    entries = []
    value = seed
    for index in range(count):
        value = (value * 1103515245 + 12345) & 0x7FFFFFFF
        depth = value % 4
        directory = posixpath.join(root, *[f"d{(value >> (3 * i)) % 40}"
                                           for i in range(depth + 1)])
        size = 4096 if value % 97 else 512 * 1024 * 1024
        entries.append((posixpath.join(directory, f"f{index}"), size))
    return entries


def directory_of(file_path):
    return posixpath.dirname(file_path) or "/"


# =============================================================================
# Model results
# =============================================================================
class ModelResult:
    """One model's score on the four axes, reported separately."""

    def __init__(self, name, criterion):
        self.name = name
        self.criterion = criterion
        self.metrics = ScanMetrics()
        self.entries_replayed = 0
        self.directories_replayed = 0
        self.chunks_sealed = 0
        self.restarts = 0
        self.wall_seconds = 0.0

    def as_dict(self):
        snapshot = self.metrics.snapshot()
        return {
            "model": self.name,
            "decision_criterion": self.criterion,
            "restarts": self.restarts,
            "wall_seconds": round(self.wall_seconds, 4),
            "exploration": {
                "listing_starts": snapshot["scan_listing_starts"],
                "entries_seen": snapshot["scan_entries_seen"],
                "enumeration_seconds": snapshot["scan_enumeration_seconds"],
            },
            "database_membership": {
                "sql_executions": snapshot["scan_sql_executions"],
                "sql_rows": snapshot["scan_sql_rows"],
                "membership_query_count":
                    snapshot["scan_membership_query_count"],
                "membership_query_paths":
                    snapshot["scan_membership_query_paths"],
                "membership_query_seconds":
                    snapshot["scan_membership_query_seconds"],
                "plan_insert_calls": snapshot["scan_plan_insert_calls"],
                "plan_insert_rows": snapshot["scan_plan_insert_rows"],
            },
            "replay": {
                "entries_replayed": self.entries_replayed,
                "directories_replayed": self.directories_replayed,
                "duplicate_entries": snapshot["scan_entries_duplicate"],
            },
            "time_to_first": {
                "first_sealed_chunk_seconds":
                    snapshot["scan_seconds_to_first_sealed_chunk"],
                "chunks_sealed": self.chunks_sealed,
            },
        }


# =============================================================================
# Model 1 — current root replay
# =============================================================================
def run_current_root_replay(entries, restarts, budget_bytes, max_files,
                            interrupt_at):
    """Today's behaviour: every run re-enumerates every root from the start.

    Recovery is by replaying visited files. The membership filter runs once per
    sealed chunk (bulk), and rediscovered paths have already moved the chunk
    boundary before they are dropped.
    """
    result = ModelResult(
        "current_root_replay",
        "establish repeat enumeration and database cost; the membership filter "
        "is a chunk-bulk query, NOT one round trip per file")
    known = set()
    started = time.perf_counter()

    for attempt in range(restarts + 1):
        result.restarts = attempt
        result.metrics.note_listing_start()
        listing_started = time.perf_counter()
        builder = StreamingChunkBuilder(budget_bytes, alloc_unit=4096,
                                        padding_factor=1.0,
                                        max_files=max_files)
        seen = 0
        limit = (interrupt_at if attempt < restarts else len(entries))

        def publish(chunk):
            query_started = time.perf_counter()
            paths = [path for path, _ in chunk]
            fresh = [(path, size) for path, size in chunk if path not in known]
            result.metrics.note_membership_query(
                time.perf_counter() - query_started, len(paths),
                len(paths) - len(fresh))
            if not fresh:
                return
            insert_started = time.perf_counter()
            known.update(path for path, _ in fresh)
            result.metrics.note_plan_insert(
                time.perf_counter() - insert_started, len(fresh))
            result.metrics.mark_first_sealed_chunk()
            result.chunks_sealed += 1

        for file_path, size in entries[:limit]:
            seen += 1
            if file_path in known:
                result.entries_replayed += 1
            for chunk in builder.add(file_path, size):
                publish(chunk)
        if attempt == restarts:
            for chunk in builder.flush():
                publish(chunk)
        result.metrics.note_enumeration(
            time.perf_counter() - listing_started, seen)

    result.wall_seconds = time.perf_counter() - started
    return result


# =============================================================================
# Model 2 — full scan before processing
# =============================================================================
def run_full_scan_before_processing(entries, restarts, budget_bytes, max_files,
                                    interrupt_at):
    """Persist the complete inventory before releasing ANY chunk.

    Reject this model if time-to-first-write or metadata footprint is worse
    without a compensating safety benefit — an interrupted full scan has
    published nothing, so the whole inventory is re-enumerated.
    """
    result = ModelResult(
        "full_scan_before_processing",
        "reject if time-to-first-write or metadata footprint is worse without "
        "a compensating safety benefit")
    started = time.perf_counter()
    inventory = []

    for attempt in range(restarts + 1):
        result.restarts = attempt
        result.metrics.note_listing_start()
        listing_started = time.perf_counter()
        limit = (interrupt_at if attempt < restarts else len(entries))
        # An interrupted full scan keeps nothing: the next attempt starts over.
        result.entries_replayed += len(inventory)
        inventory = list(entries[:limit])
        result.metrics.note_enumeration(
            time.perf_counter() - listing_started, len(inventory))

    # Only now may chunks be sealed, so the first tape-ready chunk cannot
    # appear before the entire source has been listed.
    builder = StreamingChunkBuilder(budget_bytes, alloc_unit=4096,
                                    padding_factor=1.0, max_files=max_files)
    sealed = []
    for file_path, size in inventory:
        sealed.extend(builder.add(file_path, size))
    sealed.extend(builder.flush())
    for chunk in sealed:
        insert_started = time.perf_counter()
        result.metrics.note_plan_insert(
            time.perf_counter() - insert_started, len(chunk))
        result.metrics.mark_first_sealed_chunk()
        result.chunks_sealed += 1

    result.wall_seconds = time.perf_counter() - started
    return result


# =============================================================================
# Model 3 — persistent directory frontier
# =============================================================================
def run_persistent_directory_frontier(entries, restarts, budget_bytes,
                                      max_files, interrupt_at):
    """Resume from committed directory state while earlier chunks stage.

    Select this model when it eliminates completed-directory replay and retains
    the scanner/stager/writer overlap: a crash replays at most the one
    directory that was mid-listing.
    """
    result = ModelResult(
        "persistent_directory_frontier",
        "select when it eliminates completed-directory replay and retains "
        "scan/stage/write overlap")
    started = time.perf_counter()

    # Group into immediate-directory listings, the frontier's unit of work.
    by_directory = {}
    order = []
    for file_path, size in entries:
        directory = directory_of(file_path)
        if directory not in by_directory:
            by_directory[directory] = []
            order.append(directory)
        by_directory[directory].append((file_path, size))

    complete = set()            # durable: directories whose listing is final
    partial = None              # at most ONE directory may be replayed
    known = set()
    builder = StreamingChunkBuilder(budget_bytes, alloc_unit=4096,
                                    padding_factor=1.0, max_files=max_files)

    def publish(chunk):
        query_started = time.perf_counter()
        fresh = [(path, size) for path, size in chunk if path not in known]
        result.metrics.note_membership_query(
            time.perf_counter() - query_started, len(chunk),
            len(chunk) - len(fresh))
        if not fresh:
            return
        insert_started = time.perf_counter()
        known.update(path for path, _ in fresh)
        result.metrics.note_plan_insert(
            time.perf_counter() - insert_started, len(fresh))
        result.metrics.mark_first_sealed_chunk()
        result.chunks_sealed += 1

    consumed = 0
    for attempt in range(restarts + 1):
        result.restarts = attempt
        listing_started = time.perf_counter()
        seen = 0
        limit = (interrupt_at if attempt < restarts else len(entries))
        if partial is not None:
            # The bounded replay: exactly one directory, never a whole root.
            result.directories_replayed += 1
            result.entries_replayed += len(by_directory[partial])
            complete.discard(partial)

        for directory in order:
            if directory in complete:
                continue                    # never re-enumerated
            result.metrics.note_listing_start()
            files = by_directory[directory]
            for file_path, size in files:
                seen += 1
                consumed += 1
                for chunk in builder.add(file_path, size):
                    publish(chunk)
                if consumed >= limit and attempt < restarts:
                    partial = directory
                    break
            else:
                complete.add(directory)
                continue
            break
        else:
            partial = None
        result.metrics.note_enumeration(
            time.perf_counter() - listing_started, seen)
        if partial is None:
            break
        consumed = 0

    for chunk in builder.flush():
        publish(chunk)
    result.wall_seconds = time.perf_counter() - started
    return result


MODELS = (
    run_current_root_replay,
    run_full_scan_before_processing,
    run_persistent_directory_frontier,
)


# =============================================================================
# Isolated-PostgreSQL cardinality benchmark
# =============================================================================
def _assert_isolated_dsn(dsn):
    lowered = (dsn or "").lower()
    if not any(marker in lowered for marker in _REQUIRED_DSN_MARKERS):
        raise SystemExit(
            "[BENCH] Refusing to run: the DSN does not name a test database.\n"
            f"        Its name must contain one of {_REQUIRED_DSN_MARKERS}.\n"
            "        This benchmark creates and drops fixture tables and must "
            "never touch the production catalog.")


def run_pg_benchmark(dsn, cardinalities, batch=5000):
    """Measure the real membership query at increasing catalog cardinality.

    Reports catalog cardinality, SQL executions, rows and elapsed time as four
    separate figures, so a bulk query is never reported as if it were one round
    trip per file.
    """
    _assert_isolated_dsn(dsn)
    try:
        import psycopg
    except ImportError:
        raise SystemExit("[BENCH] psycopg is required for --pg-benchmark.")

    rows = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS bench_snapshot_files")
        conn.execute(
            """CREATE TABLE bench_snapshot_files (
                   snapshot_file_id BIGINT GENERATED BY DEFAULT AS IDENTITY
                       PRIMARY KEY,
                   snapshot_id      BIGINT NOT NULL,
                   remote_path      TEXT NOT NULL,
                   file_size_bytes  BIGINT NOT NULL,
                   UNIQUE (snapshot_id, remote_path))""")
        try:
            populated = 0
            for target in cardinalities:
                while populated < target:
                    step = min(batch, target - populated)
                    with conn.cursor() as cur:
                        cur.executemany(
                            "INSERT INTO bench_snapshot_files "
                            "(snapshot_id, remote_path, file_size_bytes) "
                            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                            [(1, f"/vault/bench/f{i}", 4096)
                             for i in range(populated, populated + step)])
                    populated += step

                # One chunk-bulk membership query, exactly as production issues.
                probe = [f"/vault/bench/f{i}"
                         for i in range(0, min(populated, 200000), 7)]
                started = time.perf_counter()
                found = conn.execute(
                    "SELECT remote_path FROM bench_snapshot_files "
                    "WHERE snapshot_id=%s AND remote_path = ANY(%s)",
                    (1, probe)).fetchall()
                elapsed = time.perf_counter() - started
                rows.append({
                    "catalog_cardinality": populated,
                    "sql_executions": 1,
                    "probe_paths": len(probe),
                    "rows_returned": len(found),
                    "elapsed_seconds": round(elapsed, 6),
                })
        finally:
            conn.execute("DROP TABLE IF EXISTS bench_snapshot_files")
    return rows


# =============================================================================
# CLI
# =============================================================================
def _print_report(results):
    print("\n" + "=" * 78)
    print("SCAN MODEL COMPARISON — exploration / database / replay / latency")
    print("=" * 78)
    for result in results:
        data = result.as_dict()
        print(f"\n{data['model']}")
        print(f"  criterion : {data['decision_criterion']}")
        print(f"  restarts  : {data['restarts']}   "
              f"wall {data['wall_seconds']:.4f}s")
        exploration = data["exploration"]
        print(f"  EXPLORATION  listing_starts={exploration['listing_starts']} "
              f"entries_seen={exploration['entries_seen']} "
              f"seconds={exploration['enumeration_seconds']}")
        database = data["database_membership"]
        print(f"  DATABASE     sql_executions={database['sql_executions']} "
              f"sql_rows={database['sql_rows']} "
              f"membership_seconds={database['membership_query_seconds']}")
        replay = data["replay"]
        print(f"  REPLAY       entries_replayed={replay['entries_replayed']} "
              f"directories_replayed={replay['directories_replayed']} "
              f"duplicates={replay['duplicate_entries']}")
        latency = data["time_to_first"]
        print("  LATENCY      first_sealed_chunk="
              f"{latency['first_sealed_chunk_seconds']}s "
              f"chunks_sealed={latency['chunks_sealed']}")
    print()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--replay", metavar="LISTING",
                        help="recorded find listing to replay offline")
    source.add_argument("--synthetic", type=int, metavar="N",
                        help="generate N synthetic entries instead")
    parser.add_argument("--restarts", type=int, default=2,
                        help="simulated interruptions before completion")
    parser.add_argument("--interrupt-at", type=int, default=None,
                        help="entries consumed before each simulated crash")
    parser.add_argument("--chunk-budget-gb", type=float, default=12.0)
    parser.add_argument("--chunk-max-files", type=int, default=200000)
    parser.add_argument("--json", metavar="PATH",
                        help="also write the full report as JSON")
    parser.add_argument("--pg-benchmark", action="store_true",
                        help="run the isolated-PostgreSQL cardinality benchmark")
    parser.add_argument("--dsn", default=os.environ.get("BENCH_DSN", ""))
    parser.add_argument("--cardinalities", default="1000,10000,100000")
    args = parser.parse_args(argv)

    report = {}

    if args.replay or args.synthetic:
        entries = (parse_listing(args.replay) if args.replay
                   else synthetic_listing(args.synthetic))
        if not entries:
            raise SystemExit("[BENCH] The listing produced no entries.")
        interrupt_at = args.interrupt_at or max(1, len(entries) // 2)
        budget = int(args.chunk_budget_gb * 1024 ** 3)
        results = [model(entries, args.restarts, budget, args.chunk_max_files,
                         interrupt_at)
                   for model in MODELS]
        _print_report(results)
        report["entries"] = len(entries)
        report["models"] = [r.as_dict() for r in results]

    if args.pg_benchmark:
        cardinalities = [int(v) for v in args.cardinalities.split(",") if v]
        rows = run_pg_benchmark(args.dsn, cardinalities)
        print("\nISOLATED POSTGRESQL — membership query by catalog cardinality")
        for row in rows:
            print(f"  cardinality={row['catalog_cardinality']:>9,}  "
                  f"sql_executions={row['sql_executions']}  "
                  f"probe_paths={row['probe_paths']:>7,}  "
                  f"rows={row['rows_returned']:>7,}  "
                  f"elapsed={row['elapsed_seconds']:.6f}s")
        report["postgres"] = rows

    if not report:
        parser.error("choose --replay, --synthetic and/or --pg-benchmark")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(f"[BENCH] JSON report written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
