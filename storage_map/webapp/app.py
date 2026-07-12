"""FastAPI application for the interactive Storage Map dashboard.

Serves a single-page frontend plus a small JSON API around the v1 engine
(:mod:`storage_map.lib.core`): live overview from the fetched raw
logs, in-app scan/status/fetch actions, and the tape-coverage view backed by
one read-only PostgreSQL aggregation. Every endpoint is a sync ``def`` on
purpose — SSH status checks block for up to a minute and must run in the
threadpool, never on the event loop.
"""
import json
import os
from datetime import datetime

try:
    from fastapi import Body, FastAPI, HTTPException
    from fastapi.responses import FileResponse, JSONResponse, Response
    from fastapi.staticfiles import StaticFiles
except ImportError as exc:  # pragma: no cover - dependency hint
    raise RuntimeError(
        "The Storage Map app needs FastAPI and uvicorn: "
        "pip install fastapi uvicorn") from exc

from src.config import ConfigManager
from src.telegram_notify import TelegramNotifier
from storage_map.lib import core
from storage_map.lib import baseline as baseline_lib
from storage_map.lib.dashboard import _server_leaves
from storage_map.webapp import coverage as cov
from storage_map.webapp.jobs import JobBusy, JobManager
from storage_map.webapp.repository import CoverageRepository
from storage_map.webapp.settings import load_webapp_config

_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
_DOCS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'docs')
def _cache_path(smcfg):
    return os.path.join(smcfg.output_dir, 'coverage_cache.json')


def _load_cache(smcfg):
    try:
        with open(_cache_path(smcfg), encoding='utf-8') as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get('rows'), list):
            return data
    except (OSError, ValueError):
        pass
    return None


def _write_cache(smcfg, data):
    os.makedirs(smcfg.output_dir, exist_ok=True)
    tmp = _cache_path(smcfg) + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(data, fh)
    os.replace(tmp, _cache_path(smcfg))


def _overview_payload(smcfg, results):
    servers = []
    for res in results:
        mounts = []
        for mt in sorted(res.mounts, key=lambda m: m.total, reverse=True):
            used = mt.used_bytes
            if used is None and mt.has_capacity:
                used = max(0, mt.capacity_bytes - mt.free_bytes)
            mounts.append({
                'mount': mt.mount,
                'total': mt.total,
                'capacity_bytes': mt.capacity_bytes,
                'used_bytes': used,
                'free_bytes': mt.free_bytes,
                'free_percent': mt.free_percent,
            })
        top = [{'name': node.name, 'path': node.path, 'mount': mount,
                'size': node.size}
               for node, mount in _server_leaves(res)[:12]]
        rawlog = core._latest_path(smcfg, res.server)
        try:
            fetched_at = datetime.fromtimestamp(
                os.path.getmtime(rawlog)).isoformat(timespec='seconds')
        except OSError:
            fetched_at = None
        servers.append({
            'name': res.server,
            'host': res.host,
            'generated_at': res.generated_at,
            'fetched_at': fetched_at,
            'total': res.total,
            'mounts': mounts,
            'top_folders': top,
        })
    return {'servers': servers,
            'grand_total': sum(r.total for r in results)}


def create_app(cfg=None):
    cfg = cfg or ConfigManager()
    smcfg = core.load_storage_map_config(cfg)
    webcfg = load_webapp_config(cfg, smcfg)
    jobs = JobManager()

    mounts_by_server = {srv.name: list(srv.mounts) for srv in smcfg.servers}
    all_mounts = [m for srv in smcfg.servers for m in srv.mounts]
    max_segs = cov.max_segments(all_mounts, webcfg.match_depth)
    host_map = cov.resolve_host_map(smcfg.servers, webcfg.host_map)

    app = FastAPI(title='Storage Map', docs_url=None, redoc_url=None)
    app.state.webcfg = webcfg
    app.state.smcfg = smcfg

    @app.middleware('http')
    async def _api_guard(request, call_next):
        if request.url.path.startswith('/api/'):
            # CSRF: a cross-origin "simple" POST (text/plain, empty body)
            # would otherwise fire actions like /api/scan from any web page
            # the operator has open. Requiring application/json forces a CORS
            # preflight, which same-origin app.js already satisfies.
            if request.method == 'POST':
                ctype = request.headers.get('content-type', '')
                if not ctype.lower().startswith('application/json'):
                    return JSONResponse(
                        {'detail': 'Content-Type must be application/json'},
                        status_code=415)
        return await call_next(request)

    def _servers(requested=None):
        if not requested:
            return smcfg.servers
        if (not isinstance(requested, list)
                or not all(isinstance(n, str) for n in requested)):
            raise HTTPException(status_code=400,
                                detail="'servers' must be a list of names")
        try:
            return core._select_servers(smcfg, ','.join(requested))
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def _results():
        return core._load_results(smcfg, smcfg.servers)

    def _coverage_payload():
        cache = _load_cache(smcfg)
        report = cov.build_coverage(
            _results(),
            (cache or {}).get('rows', []),
            mounts_by_server,
            webcfg.match_depth,
            host_map,
            db_generated_at=(cache or {}).get('generated_at'),
            default_mounts=all_mounts,
            baseline_by_server=baseline_lib.load_baseline(),
        )
        report['stale'] = cache is None
        return report

    # ------------------------------------------------------------- pages --
    @app.get('/', include_in_schema=False)
    def index():
        return FileResponse(
            os.path.join(_STATIC_DIR, 'index.html'),
            headers={'Cache-Control': 'no-store, max-age=0'})

    # --------------------------------------------------------------- data --
    @app.get('/api/overview')
    def api_overview():
        return _overview_payload(smcfg, _results())

    @app.get('/api/coverage')
    def api_coverage():
        return _coverage_payload()

    @app.post('/api/export/html')
    def api_export_html(payload: dict = Body(default=None)):
        html = (payload or {}).get('html')
        if not isinstance(html, str) or not html.strip():
            raise HTTPException(status_code=400,
                                detail="'html' snapshot content is required.")
        os.makedirs(_DOCS_DIR, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'storage_map_{stamp}.html'
        path = os.path.join(_DOCS_DIR, filename)
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(html)
        return {'saved': filename, 'path': path}

    # ------------------------------------------------------------ actions --
    def _start(name, fn, conflicts=()):
        try:
            jobs.start(name, fn, conflicts=conflicts)
        except JobBusy as exc:
            raise HTTPException(
                status_code=409,
                detail=f"A '{exc}' job is already running.") from exc
        return Response(status_code=202,
                        content=json.dumps({'started': name}),
                        media_type='application/json')

    @app.post('/api/scan')
    def api_scan(payload: dict = Body(default=None)):
        servers = _servers((payload or {}).get('servers'))
        notifier = TelegramNotifier.from_config(cfg)

        def run():
            rc = core.scan(smcfg, servers, notifier=notifier)
            names = ', '.join(s.name for s in servers)
            if rc:
                raise RuntimeError(f'launch failed on at least one of {names}')
            return f'scan launched on {names}'

        return _start('scan', run, conflicts=('fetch',))

    @app.get('/api/scan/status')
    def api_scan_status():
        states = {}
        for srv in smcfg.servers:
            states[srv.name] = {
                'state': core._remote_status(srv),
                'started_at': core._manifest_started(smcfg, srv),
            }
        return {'servers': states}

    @app.post('/api/fetch')
    def api_fetch(payload: dict = Body(default=None)):
        servers = _servers((payload or {}).get('servers'))
        notifier = TelegramNotifier.from_config(cfg)

        def run():
            rc = core.fetch(smcfg, servers, notifier=notifier)
            if rc:
                raise RuntimeError('no completed scans were retrieved '
                                   '(still pending or unreachable)')
            return 'raw logs fetched; dashboard data refreshed'

        return _start('fetch', run, conflicts=('scan',))

    @app.post('/api/coverage/refresh')
    def api_coverage_refresh():
        def run():
            with CoverageRepository(cfg.db_dsn) as repo:
                rows = repo.fetch_coverage_rows(
                    max_segs, int(cfg.index_min_file_mb * 1024 * 1024))
            _write_cache(smcfg, {
                'generated_at': datetime.now().isoformat(timespec='seconds'),
                'max_segments': max_segs,
                'rows': rows,
            })
            return f'aggregated {len(rows)} directory prefixes from the DB'

        return _start('coverage', run)

    # Exact-count baseline (find -type f): a deliberate, one-time refresh of the
    # committed baseline file. Launch walks every inode, so run it in a quiet
    # window — never while an archive is fetching from the same server.
    @app.post('/api/coverage/baseline/launch')
    def api_baseline_launch(payload: dict = Body(default=None)):
        servers = _servers((payload or {}).get('servers'))

        def run():
            rc = baseline_lib.launch_baseline(smcfg, servers)
            names = ', '.join(s.name for s in servers)
            if rc:
                raise RuntimeError(f'baseline launch failed on one of {names}')
            return f'exact-count baseline launched on {names}'

        return _start('baseline', run, conflicts=('fetch', 'scan'))

    @app.post('/api/coverage/baseline/collect')
    def api_baseline_collect(payload: dict = Body(default=None)):
        servers = _servers((payload or {}).get('servers'))

        def run():
            result = baseline_lib.collect_baseline(smcfg, servers)
            if not result['fetched']:
                raise RuntimeError('no completed baseline runs were retrieved '
                                   '(still pending or unreachable)')
            return f"baseline updated for {', '.join(result['fetched'])}"

        return _start('baseline', run)

    @app.get('/api/coverage/baseline/status')
    def api_baseline_status():
        states = {srv.name: baseline_lib._baseline_status(srv)
                  for srv in smcfg.servers}
        return {'servers': states}

    @app.get('/api/jobs')
    def api_jobs():
        return jobs.snapshot()

    app.mount('/static', StaticFiles(directory=_STATIC_DIR), name='static')

    return app
