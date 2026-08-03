"""Local and remote archive orchestrators (facade module).

The implementation is split by concern into sibling modules with strictly
downward dependencies:

- :mod:`src.scanning`            — remote ``find`` scanners (batch/streaming)
- :mod:`src.planning`            — chunk bin-packing planners
- :mod:`src.local_orchestrator`  — persistent local multi-tape workflow
- :mod:`src.remote_orchestrator` — streaming remote -> staging -> tape pipeline

Plan 1 also split the remote pipeline itself, so behaviour lives in focused
modules rather than one 3.6k-line class:

- :mod:`src.scan_frontier`   — scan-model gate + discovery/publication
- :mod:`src.remote_staging`  — fetch + pack onto local staging (no tape)
- :mod:`src.remote_writer`   — the finite write group (the only tape path)
- :mod:`src.remote_pipeline` — the single scheduling loop over the three

This module re-exports the public classes so existing imports keep working.
Note for tests: ``mock.patch`` targets must name the module a symbol is *used*
in (e.g. ``src.scanning._ssh_run``, ``src.remote_staging._dir_tree_size``,
``src.scan_frontier.StreamingRemoteScanner``), not this facade.

Removed from this facade at Plan 1 completion: ``RemoteScanner`` and
``StreamingRemoteScanner``. The frontier is the only scanner a production
run may build, so the public facade must not advertise the legacy ones. The
classes still exist in ``src.scanning`` — the plan forbids deleting the
legacy PlanSource and later plans need it — and the tests that characterise
their behaviour import them from there.

Removed by Plan 1 Task 1.6 after a repository-wide audit found no production
caller: ``DirectoryFirstRemoteScanner``, ``DirectoryUnitPlanner`` and
``DirectoryPlanUnit``. They were never wired into the new-session path, and two
defects made reviving them as-is unsafe — ``stat_directory`` ran a full
recursive ``find`` per directory (so planning a tree walked it once per node),
and ``iter_large_files`` compared its threshold in **megabytes** while
``stat_directory`` compared the same setting in **bytes**. The directory-boundary
scanner in Plan 1 Task 2.3 replaces them, deliberately from scratch.
"""
from .local_orchestrator import LocalOrchestrator
from .planning import ChunkPlanner, StreamingChunkBuilder
from .remote_orchestrator import RemoteOrchestrator
from .resource_governor import ResourceGovernor

__all__ = [
    "ChunkPlanner",
    "LocalOrchestrator", "RemoteOrchestrator",
    "ResourceGovernor",
    "StreamingChunkBuilder",
]
