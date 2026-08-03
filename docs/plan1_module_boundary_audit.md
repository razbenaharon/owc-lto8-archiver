# Plan 1 — module boundary and dead-path audit (Task 1.6)

The Task 1.6 acceptance gate asks for two things that are easy to assert and
easy to fudge: that **every retained scanner/planner has a production caller or
a documented compatibility purpose**, and that a **before/after report** shows
`remote_orchestrator.py` materially reduced with no duplicate scheduling
implementation and no new wrapper-only module. This file is that report.

## 1. Symbol audit

Repository-wide search over `*.py`, `*.ini` and `*.md` (excluding
`__pycache__` and the plan documents themselves).

| Symbol | Production caller | Verdict |
|---|---|---|
| `scanning.StreamingRemoteScanner` | **Yes** — `scan_frontier.build_legacy_scanner_factory` builds it for every remote session | Retained. It is still *the* production scanner; Task 2.3's frontier replaces it only after the schema and rehearsal gates pass. |
| `scanning.RemoteScanner` | **Yes, indirectly** — base class of `StreamingRemoteScanner`; owns `_record_find_warnings` and the record-validation rules both scanners rely on | Retained. Its own `scan()`/`_scan_one()` batch entry has no production caller but is exercised by `RemoteScannerTests`, which is where the find-warning and truncated-record parsing is proved. Documented compatibility purpose. |
| `planning.ChunkPlanner` | **Yes** — `StreamingChunkBuilder` composes it for `footprint()` | Retained. |
| `planning.StreamingChunkBuilder` | **Yes** — `RemoteScanCoordinator.run()` | Retained. |
| `scanning.DirectoryFirstRemoteScanner` | **No** | **Removed.** |
| `planning.DirectoryUnitPlanner` | **No** | **Removed.** |
| `planning.DirectoryPlanUnit` | **No** (only `DirectoryFirstRemoteScanner.stat_directory` constructed it) | **Removed.** |
| `config.remote_scan_mode` | **No reader anywhere** | **Removed**, with its `config.example.ini` key. |
| `config.remote_scan_depth` | **No reader** (only `DirectoryFirstRemoteScanner`'s `depth`) | **Removed.** |
| `config.directory_chunk_max_gb` | **No reader** | **Removed.** |
| `config.directory_chunk_max_files` | **No reader** | **Removed.** |
| `config.large_file_min_mb` | **No reader** today | **Retained** — it is a real archival policy figure (loose vs packed), defaults from `index_min_file_mb`, and Plans 2–3 size stored-TAR/loose behaviour on it. Documented as currently unread. |

An existing `config.ini` may still contain the removed keys. `configparser`
ignores keys nothing asks for, so no operator action is required.

### Why the directory-first code was not revived instead

Plan 1 warns: *"Do not reuse the dormant directory-first code without proving it
avoids recursive duplicate walks and uses the real loose threshold."* It fails
both:

1. **Recursive duplicate walks.** `stat_directory()` ran
   `find "$root" -type f -printf '%s %h\0'` — a *full recursive* listing — once
   per candidate directory. `DirectoryUnitPlanner._append()` then descends into
   children when a unit overflows, so planning a tree of depth *d* re-walked
   every file up to *d* times. On the source this pipeline actually archives
   (~82 M files still to go at the time of writing) that is not a tuning
   problem, it is a different order of magnitude.
2. **Two different thresholds from one setting.** `stat_directory()` computes
   `threshold = int(large_file_min_mb * 1024 * 1024)` — **bytes** — while
   `iter_large_files()` computes `threshold = int(large_file_min_mb)` and passes
   it to `find -size +{threshold-1}M` — **megabytes**. The same configured value
   therefore meant two different sizes in the same class, so its "large file"
   accounting and its large-file listing could never agree.

Task 2.3 builds the directory-boundary scanner from scratch, on an
immediate-child listing with persisted per-directory state, for exactly these
reasons.

## 2. Before / after module report

Counted with `ast`: `lines` is physical lines, `defs` is function/method
definitions, `branch` counts `if`/`for`/`while`/`try`/`except`/`with` nodes as a
proxy for control-flow density.

| module | lines | defs | branches |
|---|---:|---:|---:|
| `remote_orchestrator.py` **before Plan 1** | 3657 | 78 | 378 |
| `remote_orchestrator.py` after | 2330 | 74 | 158 |
| `scan_frontier.py` | 347 | 13 | 29 |
| `remote_staging.py` | 1137 | 21 | 128 |
| `remote_writer.py` | 351 | 6 | 30 |
| `remote_pipeline.py` | 407 | 16 | 40 |
| `scanning.py` | 340 | 12 | 53 |
| `planning.py` | 86 | 6 | 8 |

**Control flow, not just line count, is what moved.** `remote_orchestrator.py`
lost **58% of its branches** (378 → 158) while losing 36% of its lines: what
left is decision-making, not boilerplate. Its remaining `def` count barely
changed because Task 1.2 deliberately kept the façade's public API — the
delegating one-liners are the API surface, not the behaviour.

The five-module total (4572) exceeds the original 3657 because each new module
carries its own imports and a docstring explaining the invariant it protects.
That is the intended trade: the number that matters operationally is that the
single largest file dropped from 3657 to 2330, and that the code deciding
whether a tape write may start is now 351 reviewable lines in one place.

## 3. No duplicate scheduling implementation

- **One scan/publication implementation**: `scan_frontier.RemoteScanCoordinator`.
  `remote_orchestrator.py` contains no `StreamingChunkBuilder`, no `iter_scan`,
  no `_append_chunk` and no `_scanner_planner`
  (`tests/test_scan_frontier.py::OrchestratorIsWiringTests`).
- **One scheduling loop**: `remote_pipeline.RemotePipelineCoordinator`, used by
  *both* `_run_streaming_session` and `_run_session`. The scan-complete resume
  path's group-of-one bypass is gone
  (`tests/test_remote_failure_hardening.py::SingleGateStructureTests`).
- **One tape path**: `remote_writer.RemoteChunkWriter._write_chunk_group`, which
  is the only caller of `_pre_write_safety_gate`
  (`tests/test_finite_group_only_tape_path.py`).

## 4. No wrapper-only module

Each new module owns behaviour rather than forwarding:

| module | owns |
|---|---|
| `scan_frontier` | the scan-mode activation gate, discovery, chunk sealing/publication, the publication gate |
| `remote_staging` | SSH/tar fetch, retry classification, the staging watchdog, packing, preserve/discard/resume-pack rules |
| `remote_writer` | ownership acquisition, the single safety gate, per-chunk failure isolation, the `backing` transition |
| `remote_pipeline` | authoritative work selection, backlog fairness, producer/writer lifecycle, group settlement |

`RemoteOrchestrator` retains delegating methods, but they are the *façade's*
public API — the surface callers and the existing test suite address — not a
second implementation. The behaviour they call into exists once.
