"""Chunk planners: bin-pack scanned manifests into staging-sized chunks."""


class ChunkPlanner:
    """Greedy largest-first chunk planner.

    Bin-packs on each file's estimated on-disk footprint — logical size
    rounded up to the staging volume's allocation unit, times a safety
    padding factor — so a chunk's *physical* staging footprint stays within
    budget even when logical sizes understate "size on disk" (cluster
    rounding on small files, filesystem metadata, files that grow after the
    scan). The manifest's logical sizes are preserved in the plan."""

    def __init__(self, budget_bytes, alloc_unit=1, padding_factor=1.0):
        self.budget_bytes = budget_bytes
        self.alloc_unit = max(1, int(alloc_unit))
        self.padding_factor = max(1.0, padding_factor)

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
            if cur_fp + fp > self.budget_bytes and current:
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

    def __init__(self, budget_bytes, alloc_unit=1, padding_factor=1.0):
        self.planner = ChunkPlanner(
            budget_bytes, alloc_unit=alloc_unit, padding_factor=padding_factor)
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
        if self.current and self.current_fp + fp > self.planner.budget_bytes:
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
