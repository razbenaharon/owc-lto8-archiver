"""Chunk planners: bin-pack scanned manifests into staging-sized chunks."""
from dataclasses import dataclass, field


@dataclass
class DirectoryPlanUnit:
    original_dir_path: str
    recursive_file_count: int
    recursive_bytes: int
    direct_file_count: int = 0
    direct_bytes: int = 0
    small_file_count: int = 0
    small_file_bytes: int = 0
    large_file_count: int = 0
    large_file_bytes: int = 0
    depth: int = 0
    children: list = field(default_factory=list)


class DirectoryUnitPlanner:
    """Choose whole directory containers, descending when thresholds overflow."""

    def __init__(self, max_bytes, max_files):
        self.max_bytes = int(max_bytes)
        self.max_files = int(max_files)

    def fits(self, unit):
        return (
            int(unit.recursive_bytes) <= self.max_bytes and
            int(unit.recursive_file_count) <= self.max_files
        )

    def plan(self, roots):
        planned = []
        for unit in roots:
            self._append(unit, planned)
        return planned

    def _append(self, unit, planned):
        if self.fits(unit) or not unit.children:
            planned.append(unit)
            return
        for child in sorted(
                unit.children,
                key=lambda item: item.original_dir_path.lower()):
            self._append(child, planned)


class ChunkPlanner:
    """Greedy largest-first chunk planner.

    Bin-packs on each file's estimated on-disk footprint — logical size
    rounded up to the staging volume's allocation unit, times a safety
    padding factor — so a chunk's *physical* staging footprint stays within
    budget even when logical sizes understate "size on disk" (cluster
    rounding on small files, filesystem metadata, files that grow after the
    scan). The manifest's logical sizes are preserved in the plan."""

    def __init__(self, budget_bytes, alloc_unit=1, padding_factor=1.0,
                 max_files=None):
        self.budget_bytes = budget_bytes
        self.alloc_unit = max(1, int(alloc_unit))
        self.padding_factor = max(1.0, padding_factor)
        self.max_files = int(max_files) if max_files else None

    def footprint(self, fsize):
        """Estimated bytes a file of logical size fsize allocates on disk."""
        clusters = max(1, -(-int(fsize) // self.alloc_unit))
        return int(clusters * self.alloc_unit * self.padding_factor)

    def plan(self, manifest):
        chunks = []
        current = []
        cur_fp = 0
        for remote_path, fsize in sorted(manifest, key=lambda x: x[1], reverse=True):
            fp = self.footprint(fsize)
            if fp > self.budget_bytes:
                chunks.append([(remote_path, fsize)])
                continue
            if (current and
                    (cur_fp + fp > self.budget_bytes or
                     (self.max_files and len(current) >= self.max_files))):
                chunks.append(current)
                current = []
                cur_fp = 0
            current.append((remote_path, fsize))
            cur_fp += fp
        if current:
            chunks.append(current)
        return chunks

class StreamingChunkBuilder:
    """Build threshold-sized chunks in discovery order."""

    def __init__(self, budget_bytes, alloc_unit=1, padding_factor=1.0,
                 max_files=None):
        self.planner = ChunkPlanner(
            budget_bytes, alloc_unit=alloc_unit,
            padding_factor=padding_factor, max_files=max_files)
        self.current = []
        self.current_fp = 0

    def add(self, remote_path, fsize):
        fp = self.planner.footprint(fsize)
        if fp > self.planner.budget_bytes:
            chunks = []
            if self.current:
                chunks.append(self.current)
                self.current = []
                self.current_fp = 0
            chunks.append([(remote_path, fsize)])
            return chunks
        if (self.current and
                (self.current_fp + fp > self.planner.budget_bytes or
                 (self.planner.max_files and
                  len(self.current) >= self.planner.max_files))):
            ready = self.current
            self.current = [(remote_path, fsize)]
            self.current_fp = fp
            return [ready]
        self.current.append((remote_path, fsize))
        self.current_fp += fp
        return []

    def flush(self):
        if not self.current:
            return []
        ready = self.current
        self.current = []
        self.current_fp = 0
        return [ready]
