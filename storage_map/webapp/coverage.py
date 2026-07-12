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

# A row's coverage counts as "full" from 95%: du reports allocated blocks while
# the DB stores apparent file sizes, so even a perfectly archived directory
# rarely lands on exactly 100%.
FULL_THRESHOLD_PCT = 95.0

UNKNOWN_HOST = '(unknown host)'
UNMAPPED_MOUNT = '(outside configured mounts)'

# One sequential scan of files_index, grouped into directory prefixes.
#   per_file  — dedups re-archived files (no unique constraint on
#               (source_host, original_path)) and keeps only POSIX rows, so
#               Windows-path local sessions never pollute server coverage.
#   outer     — truncates each file's dirname to at most %(max_segs)s path
#               segments; deeper directories roll up into that ancestor.
COVERAGE_SQL = """
WITH per_file AS (
    SELECT lower(split_part(source_host, '.', 1))       AS host,
           regexp_replace(original_path, '/[^/]*$', '') AS dirname,
           MAX(file_size_bytes)                         AS bytes,
           MAX(catalog_backup_date)                     AS last_backup
    FROM files_index
    WHERE original_path LIKE '/%%'
    GROUP BY 1, source_host, original_path, 2
)
SELECT host,
       COALESCE(NULLIF('/' || array_to_string(
           (string_to_array(trim(LEADING '/' FROM dirname), '/'))[1:%(max_segs)s],
           '/'), '/'), '/')       AS dir_prefix,
       SUM(bytes)::bigint         AS tape_bytes,
       COUNT(*)                   AS tape_files,
       MAX(last_backup)           AS last_backup
FROM per_file
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


def _row_status(server_bytes, tape_bytes):
    """(coverage_pct, status) for one directory row."""
    if server_bytes is None or server_bytes <= 0:
        # Not seen by du (or empty on disk) — the tape copy is all there is.
        return None, ('tape_only' if tape_bytes else 'none')
    pct = tape_bytes / server_bytes * 100.0
    if pct >= FULL_THRESHOLD_PCT:
        return 100.0, 'full'
    return pct, ('partial' if tape_bytes else 'none')


def build_coverage(scan_results, db_rows, mounts_by_server, match_depth,
                   host_map, db_generated_at=None, default_mounts=None):
    """Merge du scan trees with aggregated DB rows into the coverage report.

    ``db_rows`` are dicts from :data:`COVERAGE_SQL` (``host``, ``dir_prefix``,
    ``tape_bytes``, ``tape_files``, ``last_backup``). Rows for a host that maps
    to no configured server appear under a synthetic server section, and
    prefixes under no configured mount under a synthetic mount — archived data
    is never silently hidden.
    """
    scan_by_server = {res.server: res for res in scan_results}

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

        # --- du sizes per mount, down to match_depth ------------------------
        du_acc = {}  # mount -> {path -> size}
        if res is not None:
            for mt in res.mounts:
                sizes = du_acc.setdefault(norm(mt.mount), {})
                stack = [(mt.root, 0)]
                while stack:
                    node, depth = stack.pop()
                    sizes[norm(node.path)] = node.size
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
            tape = db_acc.get(mount, {})
            base = 0 if mount == UNMAPPED_MOUNT else len(segments(mount))
            rows_out = []
            for path in sorted(set(sizes) | set(tape), key=segments):
                server_bytes = sizes.get(path)
                tape_entry = tape.get(path)
                tape_bytes = tape_entry[0] if tape_entry else 0
                pct, state = _row_status(server_bytes, tape_bytes)
                rows_out.append({
                    'path': path,
                    'name': posixpath.basename(path.rstrip('/')) or path,
                    'depth': max(0, len(segments(path)) - base),
                    'server_bytes': server_bytes,
                    'tape_bytes': tape_bytes,
                    'tape_files': tape_entry[1] if tape_entry else 0,
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
