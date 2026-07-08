"""Local and remote archive orchestrators (facade module).

The implementation is split by concern into sibling modules with strictly
downward dependencies:

- :mod:`src.scanning`            — remote ``find`` scanners (batch/streaming)
- :mod:`src.planning`            — chunk bin-packing planners
- :mod:`src.local_orchestrator`  — persistent local multi-tape workflow
- :mod:`src.remote_orchestrator` — streaming remote -> staging -> tape pipeline

This module re-exports the public classes so existing imports keep working.
Note for tests: ``mock.patch`` targets must name the module a symbol is *used*
in (e.g. ``src.scanning._ssh_run``, ``src.remote_orchestrator._dir_tree_size``),
not this facade.
"""
from .local_orchestrator import LocalOrchestrator
from .planning import ChunkPlanner, StreamingChunkBuilder
from .remote_orchestrator import RemoteOrchestrator
from .scanning import RemoteScanner, StreamingRemoteScanner

__all__ = [
    "ChunkPlanner", "LocalOrchestrator", "RemoteOrchestrator",
    "RemoteScanner", "StreamingChunkBuilder", "StreamingRemoteScanner",
]
