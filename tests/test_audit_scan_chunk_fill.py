"""Regression tests for audit H10: chunks must fill across directories.

The frontier publishes one ready segment per directory and the coordinator
sealed after every directory, so every directory that held a file became its
own chunk. These tests run the real ``FrontierScanCoordinator`` (real segment
artifacts in a temp dir, real publisher and chunk builder) over a synthetic
tree and count the chunks, then check the invariants the deferral must not
disturb.
"""
import tempfile
import threading
import unittest

from lto_fakes import FakeFrontierCatalog
from src import scan_frontier as sf
from src.pipeline_types import ScanMetrics
from src.planning import StreamingChunkBuilder
from src.scanning import DirectoryListing


class _Catalog(FakeFrontierCatalog):
    """The frontier fake plus the plan-insert half, recording every seal."""

    def __init__(self):
        super().__init__()
        self.chunks = {}                # chunk_index -> [(path, size), ...]
        self.seal_log = []              # chunk indices, in sealing order
        self.scan_complete = False
        self.directories_listed_at_seal = []
        self.frontier = None

    def append_remote_streaming_chunk(self, _session_id, chunk_index, rows,
                                      **_kwargs):
        if chunk_index in self.chunks:
            raise AssertionError(f"chunk {chunk_index} was reopened")
        members = [(row[1], row[3]) for row in rows]
        planned = {path for chunk in self.chunks.values() for path, _ in chunk}
        duplicate = planned.intersection(path for path, _ in members)
        if duplicate:
            raise AssertionError(f"planned twice: {sorted(duplicate)[:3]}")
        self.chunks[chunk_index] = members
        self.seal_log.append(chunk_index)
        if self.frontier is not None:
            self.directories_listed_at_seal.append(
                self.frontier.directories_listed)
        return {"inserted_files": len(members),
                "inserted_bytes": sum(size for _path, size in members)}

    def mark_remote_scan_complete(self, _session_id):
        self.scan_complete = True

    def mark_remote_scan_error(self, _session_id, _message):
        pass


class _TreeScanner:
    """``tree`` maps a directory to ``(files, child_directories)``."""

    def __init__(self, tree):
        self.tree = tree
        self.listed = []

    def list_directory(self, path):
        self.listed.append(path)
        files, children = self.tree.get(path, ((), ()))
        return DirectoryListing(path, sorted(files), sorted(children), [],
                                "obs:1", 0)

    def observe(self, _path):
        return "obs:1"


class _State:
    def __init__(self):
        self.metrics = ScanMetrics()
        self.remaining_bytes = 10 ** 15
        self.next_chunk_index = 0
        self.chunks = self.files = self.bytes = 0
        self.scan_error = None


def small_directory_tree(directories, files_per_directory=3, size=100):
    tree = {"/src": ((), [f"/src/d{n:04d}" for n in range(directories)])}
    for n in range(directories):
        tree[f"/src/d{n:04d}"] = (
            [(f"/src/d{n:04d}/f{k}", size)
             for k in range(files_per_directory)], ())
    return tree


class _ScanCase(unittest.TestCase):
    def run_scan(self, tree, *, budget, max_files=None, db=None,
                 publication_gate=None, stop_after_directories=None):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        db = db or _Catalog()
        scanner = _TreeScanner(tree)
        stop = threading.Event()
        state = _State()
        state.next_chunk_index = len(db.chunks)     # a resumed session
        coordinator = sf.FrontierScanCoordinator(
            db=db, session_id=1, scan_paths=["/src"],
            archive_root=getattr(self, "archive_root", None) or root.name,
            state=state, remaining_lock=threading.Lock(), stop_event=stop,
            scanner_factory=lambda _metrics: scanner,
            builder_factory=lambda: StreamingChunkBuilder(
                budget, max_files=max_files),
            publication_gate=publication_gate,
            max_directories=stop_after_directories)
        db.frontier = coordinator.frontier
        coordinator.run()
        self.assertIsNone(coordinator.state.scan_error)
        return db, coordinator, scanner


class ChunksFillAcrossDirectoriesTests(_ScanCase):
    def test_two_hundred_small_directories_become_one_chunk(self):
        db, coordinator, _ = self.run_scan(
            small_directory_tree(200), budget=10 ** 9)
        self.assertEqual(len(db.chunks), 1,
                         f"{len(db.chunks)} chunks for 200 small directories")
        self.assertEqual(len(db.chunks[0]), 600)
        self.assertEqual(coordinator.state.chunks, 1)
        self.assertTrue(db.scan_complete)
        self.assertEqual(db.get_ready_segments(1, limit=10 ** 6), [])

    def test_chunks_fill_to_the_target_size_not_to_a_directory(self):
        # 200 directories x 300 B against a 6000 B target. Whole directories
        # only, and the fit is judged conservatively (one allocation unit per
        # file), so 19 directories - 95% of the target - make a chunk.
        db, _coordinator, _ = self.run_scan(
            small_directory_tree(200), budget=6000)
        sizes = [sum(size for _p, size in db.chunks[index])
                 for index in sorted(db.chunks)]
        self.assertEqual(sizes, [5700] * 10 + [3000])
        self.assertEqual(sorted(db.chunks), list(range(11)))
        self.assertTrue(all(size <= 6000 for size in sizes))

    def test_file_count_limit_also_fills_across_directories(self):
        db, _coordinator, _ = self.run_scan(
            small_directory_tree(200), budget=10 ** 9, max_files=150)
        self.assertEqual([len(db.chunks[i]) for i in sorted(db.chunks)],
                         [150] * 4)
        self.assertEqual(sum(len(chunk) for chunk in db.chunks.values()), 600)

    def test_a_full_chunk_is_sealed_while_traversal_continues(self):
        # The stager must not wait for the whole scan: each chunk is sealed as
        # soon as the directories discovered so far fill it.
        db, coordinator, _ = self.run_scan(
            small_directory_tree(200), budget=6000)
        total = coordinator.frontier.directories_listed
        self.assertLess(db.directories_listed_at_seal[0], total // 4)
        self.assertEqual(db.directories_listed_at_seal,
                         sorted(db.directories_listed_at_seal))

    def test_every_file_is_planned_exactly_once_in_discovery_order(self):
        tree = small_directory_tree(57, files_per_directory=4, size=70)
        db, _coordinator, _ = self.run_scan(tree, budget=1000)
        planned = [path for index in sorted(db.chunks)
                   for path, _size in db.chunks[index]]
        expected = sorted(path for files, _children in tree.values()
                          for path, _size in files)
        self.assertEqual(planned, expected)
        for index in sorted(db.chunks)[:-1]:
            self.assertGreater(
                sum(size for _p, size in db.chunks[index]), 1000 - 4 * 70 - 1)

    def test_a_directory_larger_than_a_chunk_is_still_split(self):
        tree = {"/src": ((), ["/src/big", "/src/small"]),
                "/src/big": ([(f"/src/big/f{n:02d}", 100) for n in range(25)],
                             ()),
                "/src/small": ([("/src/small/a", 100)], ())}
        db, _coordinator, _ = self.run_scan(tree, budget=1000)
        self.assertEqual(sum(len(chunk) for chunk in db.chunks.values()), 26)
        self.assertTrue(all(
            sum(size for _p, size in chunk) <= 1000
            for chunk in db.chunks.values()))


class DeferredSealingKeepsTheInvariantsTests(_ScanCase):
    def test_interrupted_scan_loses_nothing_and_resumes_into_full_chunks(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.archive_root = root.name
        tree = small_directory_tree(40)                 # 40 x 300 B
        db = _Catalog()
        # First run stops after 11 listings (the root + 10 directories), with
        # less than a chunk discovered.
        self.run_scan(tree, budget=6000, db=db, stop_after_directories=11)
        first_run = {index: list(chunk) for index, chunk in db.chunks.items()}
        self.assertFalse(db.scan_complete)
        # Whatever was sealed stays exactly as it was; nothing is lost.
        self.run_scan(tree, budget=6000, db=db)
        for index, members in first_run.items():
            self.assertEqual(db.chunks[index], members)
        planned = sorted(path for chunk in db.chunks.values()
                         for path, _size in chunk)
        self.assertEqual(planned, sorted(
            path for files, _c in tree.values() for path, _s in files))
        self.assertTrue(db.scan_complete)

    def test_segments_waiting_from_an_earlier_run_join_later_directories(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.archive_root = root.name
        tree = small_directory_tree(40)
        db = _Catalog()
        stop_now = threading.Event()

        # Run 1 is cancelled mid-traversal: ready segments, nothing sealed.
        scanner = _TreeScanner(tree)
        original = scanner.list_directory

        def cancelling(path):
            if len(scanner.listed) >= 6:
                stop_now.set()
            return original(path)

        scanner.list_directory = cancelling
        coordinator = sf.FrontierScanCoordinator(
            db=db, session_id=1, scan_paths=["/src"], archive_root=root.name,
            state=_State(), remaining_lock=threading.Lock(),
            stop_event=stop_now, scanner_factory=lambda _m: scanner,
            builder_factory=lambda: StreamingChunkBuilder(6000))
        coordinator.run()
        self.assertEqual(db.chunks, {})
        waiting = len(db.get_ready_segments(1, limit=1000))
        self.assertGreater(waiting, 0)

        db2, _coordinator, _ = self.run_scan(tree, budget=6000, db=db)
        sizes = [sum(size for _p, size in db2.chunks[index])
                 for index in sorted(db2.chunks)]
        self.assertEqual(sizes, [5700, 5700, 600])

    def test_a_closed_publication_gate_still_halts_without_sealing(self):
        db, coordinator, _ = self.run_scan(
            small_directory_tree(40), budget=6000,
            publication_gate=lambda: False)
        self.assertEqual(db.chunks, {})
        self.assertFalse(db.scan_complete)
        self.assertTrue(coordinator.publisher.halted)
        self.assertGreater(len(db.get_ready_segments(1, limit=1000)), 0)

    def test_scan_finality_still_requires_the_closing_publish(self):
        db, _coordinator, _ = self.run_scan(
            small_directory_tree(5), budget=10 ** 9)
        self.assertTrue(db.scan_complete)
        self.assertEqual(len(db.chunks), 1)
        self.assertTrue(all(scope["coverage_state"] == "final"
                            for scope in db.get_scan_scopes(1)))


if __name__ == "__main__":
    unittest.main()
