"""Tape-coverage matching: server directories (du) vs the archive DB.

Pure logic — no SSH, no database connection. The SQL text lives here so it can
be unit-tested as a string; :mod:`storage_map.webapp.repository` executes it.

The comparison stays at *directory* level: each mount plus the top layer below
it, with one extra level inside ``shared-data`` (for example
``/strg/D/shared-data/op``). The servers hold many TB
and ``files_index`` millions of rows, so neither side ever enumerates files
here. The DB is aggregated once into per-prefix byte totals; the du scan is
already directory-granular.
"""
import posixpath

from storage_map.lib.core import SHARED_DATA_DEPTH, SHARED_DATA_DIR, TOP_LAYER_DEPTH

# A directory is certified "full" per directory by TWO signals together — count
# and size — never the raw byte ratio (du reports block-allocated size; the DB
# stores apparent sizes, so the ratio alone is unreliable for many tiny files
# and can look ~100% while content is missing). Both epsilons below are derived,
# not magic numbers.

# Count epsilon: ``du --inodes`` also counts subdirectories, so the backup file
# count sits just below the source inode count. 1% absorbs the subdir fraction.
COUNT_MATCH_TOLERANCE = 0.01

# Size epsilon has two parts:
#   * physics — du(allocated) exceeds apparent by at most files×block (each file
#     wastes under one block), so the source's apparent size, and thus a complete
#     backup's apparent bytes, must be >= du − files×block. This term SCALES with
#     the file count: many-tiny-file dirs get the slack they physically need,
#     few-big-file dirs stay tight.
#   * safety — a small flat margin for sparse files, directory metadata and
#     tail-packing filesystems.
SIZE_SAFETY_MARGIN = 0.01
# ext4/xfs default; the safety margin absorbs the occasional 1 KiB/8 KiB block.
DEFAULT_BLOCK_BYTES = 4096

UNKNOWN_HOST = '(unknown host)'
UNMAPPED_MOUNT = '(outside configured mounts)'

# Directory-level catalog totals, merged into per-prefix byte/file totals.
#
# Packed files smaller than ``index_min_file_mb`` (``%(threshold_bytes)s``) are
# deliberately NOT written to ``files_index`` — they live only in the directory
# catalog (``directory_tree_index``). Coverage therefore has to add the
# catalog's per-directory small-file totals on top of ``files_index``, or every
# small-file-heavy directory looks nearly empty even when fully archived.
#
#   per_file  — dedups re-archived files (no unique constraint on
#               (source_host, original_path)) and keeps only POSIX rows, so
#               Windows-path local sessions never pollute server coverage.
#   idx       — per-directory ``files_index`` totals plus the packed-small
#               subset (``ps_*``): pre-cutover small files (and the legacy
#               backfill of ``directory_tree_index``) are represented on BOTH
#               sides, so the catalog contribution below subtracts ``ps_*`` to
#               avoid double counting. Post-cutover ``manifest_only`` small
#               files have no ``files_index`` row, so ``ps_*`` is 0 and their
#               catalog bytes are added in full.
#   dir_small — per-directory small-file totals from the directory catalog.
#   merged    — files_index totals + GREATEST(0, catalog_small - ps) per dir.
#   outer     — truncates each dirname to at most %(max_segs)s path segments;
#               deeper directories roll up into that ancestor.
COVERAGE_SQL = """
WITH per_file AS (
    SELECT lower(split_part(source_host, '.', 1))       AS host,
           regexp_replace(original_path, '/[^/]*$', '') AS dirname,
           MAX(file_size_bytes)                         AS bytes,
           bool_or(is_packed)                           AS is_packed,
           MAX(catalog_backup_date)                     AS last_backup
    FROM files_index
    WHERE original_path LIKE '/%%'
    GROUP BY 1, source_host, original_path, 2
),
idx AS (
    SELECT host, dirname,
           SUM(bytes)                                              AS bytes,
           COUNT(*)                                                AS files,
           COALESCE(SUM(bytes) FILTER (
               WHERE is_packed AND bytes < %(threshold_bytes)s), 0) AS ps_bytes,
           COALESCE(COUNT(*)   FILTER (
               WHERE is_packed AND bytes < %(threshold_bytes)s), 0) AS ps_files,
           MAX(last_backup)                                        AS last_backup
    FROM per_file
    GROUP BY 1, 2
),
dir_small AS (
    SELECT lower(split_part(source_host, '.', 1))    AS host,
           original_dir_path                         AS dirname,
           SUM(direct_small_file_bytes)::bigint      AS bytes,
           SUM(direct_small_file_count)::bigint      AS files,
           MAX(backup_date)                          AS last_backup
    FROM directory_tree_index
    WHERE original_dir_path LIKE '/%%'
    GROUP BY 1, 2
),
merged AS (
    SELECT COALESCE(i.host, d.host)       AS host,
           COALESCE(i.dirname, d.dirname) AS dirname,
           COALESCE(i.bytes, 0)
             + GREATEST(0, COALESCE(d.bytes, 0) - COALESCE(i.ps_bytes, 0))
                                          AS bytes,
           COALESCE(i.files, 0)
             + GREATEST(0, COALESCE(d.files, 0) - COALESCE(i.ps_files, 0))
                                          AS files,
           GREATEST(i.last_backup, d.last_backup) AS last_backup
    FROM idx i
    FULL OUTER JOIN dir_small d USING (host, dirname)
)
SELECT host,
       COALESCE(NULLIF('/' || array_to_string(
           (string_to_array(trim(LEADING '/' FROM dirname), '/'))[1:%(max_segs)s],
           '/'), '/'), '/')       AS dir_prefix,
       SUM(bytes)::bigint         AS tape_bytes,
       SUM(files)::bigint         AS tape_files,
       MAX(last_backup)           AS last_backup
FROM merged
GROUP BY 1, 2
"""

def norm(path):
    """The same normalization rule as ``core._build_tree``."""
    path = (path or '').rstrip('/')
    return path or '/'


def segments(path):
    """``/strg/D/op`` -> ``('strg', 'D', 'op')``; ``/`` -> ``()``."""
    path = norm(path)
    if path == '/':
        return ()
    return tuple(path.strip('/').split('/'))


def _join(segs):
    return '/' + '/'.join(segs) if segs else '/'


def max_segments(mounts, match_depth):
    """Truncation limit for the SQL: deepest configured mount + match_depth."""
    deepest = max((len(segments(m)) for m in mounts), default=0)
    return deepest + match_depth


def _visible_depth_limit(path_segs, mount_segs, match_depth):
    rel = path_segs[len(mount_segs):] if mount_segs else path_segs
    requested = min(match_depth, SHARED_DATA_DEPTH)
    if SHARED_DATA_DIR in mount_segs or (rel and rel[0] == SHARED_DATA_DIR):
        return requested
    return min(requested, TOP_LAYER_DEPTH)


def resolve_host_map(servers, override=None):
    """Map DB ``source_host`` values (short, lowercase) to server names.

    A server matches its configured name and the short form of its SSH host
    (``so02.iem.technion.ac.il`` -> ``so02``); explicit ``host_map`` config
    entries win over both.
    """
    mapping = {}
    for srv in servers:
        mapping[srv.name.lower()] = srv.name
        short = srv.host.split('.')[0].strip().lower()
        if short:
            mapping.setdefault(short, srv.name)
    for db_host, server_name in (override or {}).items():
        mapping[db_host.strip().lower()] = server_name.strip()
    return mapping


def _assign_mount(prefix_segs, mount_segs):
    """Longest configured mount that is a path-prefix of ``prefix_segs``."""
    best = None
    for m_segs, m_path in mount_segs:
        if prefix_segs[:len(m_segs)] == m_segs:
            if best is None or len(m_segs) > len(best[0]):
                best = (m_segs, m_path)
    return best


def _source_file_count(server_files, dir_count, baseline_files):
    """Best per-directory source *file* count and whether it is exact.

    Priority: an explicit ``find`` baseline; else ``du --inodes`` minus the
    ``find -type d`` directory count (exact file count without enumerating
    files); else the raw inode count (approximate — it still includes subdirs).
    Returns ``(count, exact)`` or ``(None, False)`` when no count exists.
    """
    if baseline_files is not None and baseline_files > 0:
        return baseline_files, True
    if server_files and dir_count is not None:
        return max(1, server_files - dir_count), True
    if server_files:
        return server_files, False
    return None, False


def _row_status(server_bytes, tape_bytes, server_files=None, tape_files=0,
                baseline_files=None, dir_count=None,
                block_bytes=DEFAULT_BLOCK_BYTES):
    """(coverage_pct, status) for one directory row — combined count + size.

    Compares the directory as a whole (never per file). It is ``full`` only when
    BOTH signals agree:

    * count — the backup holds ~every source file. Exact when the source file
      count is known precisely (``find`` baseline, or ``du --inodes`` minus the
      ``find -type d`` directory count) → equality; otherwise the raw inode count
      within :data:`COUNT_MATCH_TOLERANCE` (it also counts subdirectories).
    * size — the backup's apparent bytes fall inside the source's block-
      allocation band ``[du − files×block − m , du + m]`` where ``m`` is
      :data:`SIZE_SAFETY_MARGIN`·du. du measures allocated blocks, so a complete
      backup's apparent bytes can be below du by at most the block waste
      (``files×block``) — the band width is the derived size epsilon and scales
      with the file count.

    With no file count at all (a byte-only scan) a directory is never certified
    ``full`` — the byte ratio alone is not accurate enough.
    """
    if server_bytes is None or server_bytes <= 0:
        # Not seen by du (or empty on disk) — the tape copy is all there is.
        return None, ('tape_only' if tape_bytes else 'none')
    pct = tape_bytes / server_bytes * 100.0

    source_files, exact = _source_file_count(
        server_files, dir_count, baseline_files)
    if source_files is None:
        return pct, ('partial' if tape_bytes else 'none')
    count_ok = (tape_files >= source_files if exact
                else tape_files >= source_files * (1.0 - COUNT_MATCH_TOLERANCE))

    # Block-waste term uses the inode count (files AND directories each allocate
    # blocks); fall back to the file count when no inode count exists. The wider
    # bound just avoids false 'partial's — the safety margin covers dir blocks.
    block_count = server_files or source_files
    margin = SIZE_SAFETY_MARGIN * server_bytes
    lower = server_bytes - block_count * block_bytes - margin
    size_ok = lower <= tape_bytes <= server_bytes + margin
    if count_ok and size_ok:
        return 100.0, 'full'
    return pct, ('partial' if tape_bytes else 'none')


def build_coverage(scan_results, db_rows, mounts_by_server, match_depth,
                   host_map, db_generated_at=None, default_mounts=None,
                   baseline_by_server=None):
    """Merge du scan trees with aggregated DB rows into the coverage report.

    ``db_rows`` are dicts from :data:`COVERAGE_SQL` (``host``, ``dir_prefix``,
    ``tape_bytes``, ``tape_files``, ``last_backup``). Rows for a host that maps
    to no configured server appear under a synthetic server section, and
    prefixes under no configured mount under a synthetic mount — archived data
    is never silently hidden.

    ``baseline_by_server`` maps a server name to ``{path: exact_file_count}``
    from :mod:`storage_map.lib.baseline`; where a path has an exact count it
    overrides the du-inode count in the status decision.
    """
    scan_by_server = {res.server: res for res in scan_results}
    baseline_by_server = baseline_by_server or {}

    # --- group DB rows by resolved server name -----------------------------
    db_by_server = {}
    for row in db_rows:
        host = str(row.get('host') or '').strip().lower()
        server = host_map.get(host) or (
            f'{host} (not in config)' if host else UNKNOWN_HOST)
        db_by_server.setdefault(server, []).append(row)

    # Configured servers first (in config order), then any DB-only sections.
    names = list(mounts_by_server)
    names += [n for n in sorted(db_by_server) if n not in mounts_by_server]

    servers_out = []
    for name in names:
        mounts = [norm(m) for m in
                  (mounts_by_server.get(name) or default_mounts or [])]
        mount_segs = [(segments(m), m) for m in mounts]
        res = scan_by_server.get(name)
        baseline = baseline_by_server.get(name, {})

        # --- cumulative DB totals per mount, keyed by truncated path -------
        # Each SQL row covers a disjoint set of files, so adding it to every
        # ancestor between the mount root and match_depth yields correct
        # cumulative totals without double counting.
        db_acc = {}  # mount -> {path -> [tape_bytes, tape_files, last_backup]}
        for row in db_by_server.get(name, []):
            p_segs = segments(row['dir_prefix'])
            assigned = _assign_mount(p_segs, mount_segs)
            if assigned is not None:
                m_segs, bucket = assigned
            else:
                m_segs, bucket = (), UNMAPPED_MOUNT
            per_mount = db_acc.setdefault(bucket, {})
            deepest = min(len(p_segs) - len(m_segs),
                          _visible_depth_limit(p_segs, m_segs, match_depth))
            for depth in range(deepest + 1):
                key = _join(p_segs[:len(m_segs) + depth])
                entry = per_mount.setdefault(key, [0, 0, None])
                entry[0] += int(row.get('tape_bytes') or 0)
                entry[1] += int(row.get('tape_files') or 0)
                last = row.get('last_backup')
                if last and (entry[2] is None or last > entry[2]):
                    entry[2] = last

        # --- du sizes + inode/dir counts per mount, down to match_depth -----
        du_acc = {}     # mount -> {path -> size}
        du_counts = {}  # mount -> {path -> inode count or None}
        du_dirs = {}    # mount -> {path -> directory count or None}
        if res is not None:
            for mt in res.mounts:
                sizes = du_acc.setdefault(norm(mt.mount), {})
                counts = du_counts.setdefault(norm(mt.mount), {})
                dirs = du_dirs.setdefault(norm(mt.mount), {})
                stack = [(mt.root, 0)]
                while stack:
                    node, depth = stack.pop()
                    sizes[norm(node.path)] = node.size
                    counts[norm(node.path)] = node.count
                    dirs[norm(node.path)] = node.dir_count
                    if depth < _visible_depth_limit(
                            segments(node.path), segments(mt.mount),
                            match_depth):
                        stack.extend((c, depth + 1) for c in node.children)

        # --- union both sides into ordered rows -----------------------------
        mount_order = []
        for m in list(du_acc) + mounts + [UNMAPPED_MOUNT]:
            if m not in mount_order and (m in du_acc or m in db_acc):
                mount_order.append(m)

        mounts_out = []
        for mount in mount_order:
            sizes = du_acc.get(mount, {})
            counts = du_counts.get(mount, {})
            dirs = du_dirs.get(mount, {})
            tape = db_acc.get(mount, {})
            base = 0 if mount == UNMAPPED_MOUNT else len(segments(mount))
            rows_out = []
            for path in sorted(set(sizes) | set(tape), key=segments):
                server_bytes = sizes.get(path)
                server_files = counts.get(path)
                dir_count = dirs.get(path)
                baseline_files = baseline.get(path)
                tape_entry = tape.get(path)
                tape_bytes = tape_entry[0] if tape_entry else 0
                tape_files = tape_entry[1] if tape_entry else 0
                pct, state = _row_status(
                    server_bytes, tape_bytes, server_files, tape_files,
                    baseline_files, dir_count)
                source_files, exact = _source_file_count(
                    server_files, dir_count, baseline_files)
                rows_out.append({
                    'path': path,
                    'name': posixpath.basename(path.rstrip('/')) or path,
                    'depth': max(0, len(segments(path)) - base),
                    'server_bytes': server_bytes,
                    'server_files': server_files,
                    'source_files': source_files,
                    'source_files_exact': exact,
                    'baseline_files': baseline_files,
                    'tape_bytes': tape_bytes,
                    'tape_files': tape_files,
                    'last_backup': tape_entry[2] if tape_entry else None,
                    'coverage_pct': pct,
                    'status': state,
                })
            mounts_out.append({'mount': mount, 'rows': rows_out})

        servers_out.append({
            'name': name,
            'in_config': name in mounts_by_server,
            'scanned_at': res.generated_at if res else None,
            'mounts': mounts_out,
        })

    return {
        'generated_at_db': db_generated_at,
        'match_depth': match_depth,
        'servers': servers_out,
    }
