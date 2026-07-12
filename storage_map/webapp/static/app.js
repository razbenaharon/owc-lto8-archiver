/* Storage Map frontend — plain JS, no build step.
 * Data: /api/overview and /api/coverage. Actions POST to /api/*
 * and are tracked by polling /api/jobs while anything runs. */
'use strict';

/* Categorical mount hues — the validated palette, light and dark steps,
 * matching MOUNT_HUES in storage_map/lib. */
const HUES_LIGHT = ['#2a78d6', '#1baf7a', '#eda100', '#008300',
                    '#4a3aa7', '#e34948', '#e87ba4', '#eb6834'];
const HUES_DARK = ['#3987e5', '#199e70', '#c98500', '#008300',
                   '#9085e9', '#e66767', '#d55181', '#d95926'];
const BADGE_LABELS = {full: 'full', partial: 'partial',
                      none: 'not on tape', tape_only: 'tape only'};
const ARCHIVED_ONLY_KEY = 'storageMapArchivedOnly';
const DB_FRESH_HOURS = 24;

/* Last payload of each endpoint, kept so the KPI row (which mixes overview and
 * coverage numbers) and the archived-only toggle can re-render without a
 * refetch. */
let lastOverview = null;
let lastCoverage = null;
let autoRefreshedOnce = false;

const $ = (sel) => document.querySelector(sel);
const darkMode = () => matchMedia('(prefers-color-scheme: dark)').matches;
const hues = () => (darkMode() ? HUES_DARK : HUES_LIGHT);

function esc(text) {
  const div = document.createElement('div');
  div.textContent = text == null ? '' : String(text);
  return div.innerHTML;
}

function human(bytes) {
  if (bytes == null) return '—';
  let v = Number(bytes);
  for (const unit of ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']) {
    if (v < 1024 || unit === 'PiB') return `${v.toFixed(1)} ${unit}`;
    v /= 1024;
  }
}

async function authFetch(url, options) {
  return fetch(url, options);
}

async function getJSON(url) {
  const resp = await authFetch(url);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    throw new Error(body.detail || `${resp.status} ${resp.statusText}`);
  }
  return resp.json();
}

function setStatus(text, isError = false) {
  $('#statusline').innerHTML =
    isError ? `<span class="err">${esc(text)}</span>` : esc(text);
}

/* ------------------------------------------------------------ overview -- */
function kpi(value, label, accent = false) {
  return `<div class="kpi${accent ? ' kpi-accent' : ''}">
    <div class="kpi-value">${esc(value)}</div>
    <div class="kpi-label">${esc(label)}</div></div>`;
}

function barRow(label, valueText, pctWidth, hue, title) {
  const pct = Math.max(0, Math.min(100, pctWidth || 0));
  return `<div class="bar-row">
    <div class="bar-label" title="${esc(title || label)}">${esc(label)}</div>
    <div class="bar-track"><div class="bar-fill"
      style="width:${pct.toFixed(2)}%;background:${hue}"></div></div>
    <div class="bar-value">${esc(valueText)}</div></div>`;
}

function coverageTapeTotal(report) {
  // Each mount's depth-0 row carries that mount's cumulative tape bytes, and
  // every DB prefix lands in exactly one mount bucket — so summing the roots
  // counts every archived file once.
  let bytes = 0;
  for (const srv of report.servers || [])
    for (const m of srv.mounts)
      for (const r of m.rows)
        if (r.depth === 0) bytes += r.tape_bytes || 0;
  return bytes;
}

function renderKpis() {
  const data = lastOverview || {servers: [], grand_total: 0};
  const servers = data.servers || [];
  const nMounts = servers.reduce((n, s) => n + s.mounts.length, 0);
  const lastScan = servers.map((s) => s.generated_at).filter(Boolean).sort().pop();
  let tapeKpis = '';
  if (lastCoverage) {
    const tape = coverageTapeTotal(lastCoverage);
    const pct = data.grand_total ? (tape / data.grand_total) * 100 : null;
    tapeKpis =
      kpi(human(tape), 'on tape (archive DB)') +
      kpi(pct == null ? '—' : `${pct.toFixed(1)}%`, 'overall coverage');
  }
  $('#kpis').innerHTML =
    kpi(human(data.grand_total), 'total used (all servers)', true) +
    tapeKpis +
    kpi(String(servers.length), 'servers') +
    kpi(String(nMounts), 'mounts scanned') +
    kpi(lastScan ? lastScan.replace('T', ' ') : '—', 'last scan');
}

function renderOverview(data) {
  lastOverview = data;
  renderKpis();
  const servers = data.servers || [];

  $('#server-panels').innerHTML = servers.map((srv) => {
    // Hue is assigned by mount order within the server (fixed, never cycled
    // mid-render) so a mount keeps its color across panels.
    const hueOf = {};
    srv.mounts.forEach((mt, i) => { hueOf[mt.mount] = hues()[i % hues().length]; });
    const mountBars = srv.mounts.map((mt) => {
      const used = mt.used_bytes != null ? mt.used_bytes : mt.total;
      const text = mt.free_bytes != null
        ? `${human(used)} used · ${human(mt.free_bytes)} left · ` +
          `${mt.free_percent.toFixed(1)}% left`
        : `${human(mt.total)} used · left n/a`;
      const width = mt.capacity_bytes
        ? (used / mt.capacity_bytes) * 100
        : (mt.total / (srv.total || 1)) * 100;
      return barRow(mt.mount, text, width, hueOf[mt.mount]);
    }).join('');
    const topSize = srv.top_folders.length ? srv.top_folders[0].size : 1;
    const topBars = srv.top_folders.map((f) =>
      barRow(f.name, `${human(f.size)} · ${((f.size / (srv.total || 1)) * 100).toFixed(1)}%`,
             (f.size / (topSize || 1)) * 100,
             hueOf[f.mount] || hues()[0], f.path)).join('');
    return `<section class="panel">
      <div class="panel-head"><div>
        <h2>${esc(srv.name)}</h2>
        <div class="panel-sub">${esc(srv.host || 'unknown host')} ·
          scanned ${esc(srv.generated_at || 'n/a')}</div>
      </div>
      <div class="panel-sub">total used <strong>${esc(human(srv.total))}</strong></div></div>
      <div class="panel-grid">
        <div class="chart-block"><h3>Usage by mount</h3>
          <div class="bars">${mountBars}</div></div>
        <div class="chart-block"><h3>Top folders <span class="muted">(largest 12)</span></h3>
          <div class="bars">${topBars}</div></div>
      </div></section>`;
  }).join('') || '<section class="panel"><div class="placeholder">' +
    'No scan data yet — Start scan, wait for it to finish, then Fetch &amp; rebuild.' +
    '</div></section>';
}

/* ------------------------------------------------------------ coverage -- */
function covBar(pct, status) {
  // Fill color repeats the status-badge hue so the table scans at a glance.
  const cls = status === 'full' ? ' s-full'
    : status === 'partial' ? ' s-partial' : ' s-none';
  const shown = status === 'full' ? 100 : pct;
  return `<span class="cov-bar"><span class="cov-track"><span class="cov-fill${cls}"
       style="width:${Math.max(0, Math.min(100, shown)).toFixed(1)}%"></span></span>
     <span${pct > 100 ? ' title="More bytes on tape than on the server — the directory shrank since it was archived"' : ''}>
       ${shown.toFixed(1)}%</span></span>`;
}

function covRow(row) {
  const pct = row.coverage_pct;
  const pctCell = pct == null
    ? '<span class="muted">—</span>' : covBar(pct, row.status);
  const last = row.last_backup ? row.last_backup.slice(0, 10) : '—';
  return `<tr class="depth-${row.depth}">
    <td class="dir" style="padding-left:${10 + row.depth * 22}px"
        title="${esc(row.path)}">${esc(row.depth ? row.name : row.path)}</td>
    <td class="num">${esc(human(row.server_bytes))}</td>
    <td class="num">${esc(human(row.tape_bytes || 0))}</td>
    <td class="num">${pctCell}</td>
    <td class="num">${Number(row.tape_files || 0).toLocaleString()}</td>
    <td class="num muted">${esc(last)}</td>
    <td><span class="badge ${esc(row.status)}">${esc(BADGE_LABELS[row.status] || row.status)}</span></td>
  </tr>`;
}

function ageHoursOf(iso) {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  return Number.isFinite(t) ? (Date.now() - t) / 3600000 : null;
}

function mountHead(m) {
  // Depth-0 is the mount root: du total vs cumulative tape bytes for the
  // whole mount, i.e. exactly the one-line summary an operator scans for.
  const root = m.rows.find((r) => r.depth === 0 && r.path === m.mount);
  let sum = '';
  if (root && root.coverage_pct != null) {
    sum = `<span class="cov-mount-sum">Hot DB: ${esc(human(root.tape_bytes))},
      ${Number(root.tape_files || 0).toLocaleString()} files ·
      ${esc(human(root.server_bytes))} on server ${covBar(root.coverage_pct, root.status)}</span>`;
  } else if (root && root.tape_bytes) {
    sum = `<span class="cov-mount-sum">Hot DB: ${esc(human(root.tape_bytes))},
      ${Number(root.tape_files || 0).toLocaleString()} files</span>`;
  }
  return `<div class="cov-mount-head"><h3>${esc(m.mount)}</h3>${sum}</div>`;
}

function renderCoverage(report) {
  lastCoverage = report;
  renderKpis();

  const age = ageHoursOf(report.generated_at_db);
  const ageBadge = report.stale
    ? ' <span class="age-badge warn">no DB snapshot</span>'
    : (age != null && age > DB_FRESH_HOURS
       ? ` <span class="age-badge warn">DB snapshot ${Math.floor(age / 24)}d ` +
         `${Math.floor(age % 24)}h old</span>`
       : '');
  $('#coverage-sub').innerHTML = report.stale
    ? 'No database snapshot yet — aggregating the tape catalog…' + ageBadge
    : 'Server directories vs the archive database · DB aggregated ' +
      `${esc((report.generated_at_db || '').replace('T', ' '))} · ` +
      `top layer, shared-data depth ${esc(String(report.match_depth))}` + ageBadge;

  const archivedOnly = $('#cov-archived-only').checked;
  $('#coverage').innerHTML = (report.servers || []).map((srv) => {
    // Tape bytes accumulate into every ancestor row, so filtering on
    // tape_bytes > 0 never drops a parent whose child is archived.
    const mounts = srv.mounts
      .map((m) => ({mount: m.mount,
                    rows: archivedOnly
                      ? m.rows.filter((r) => r.tape_bytes > 0) : m.rows}))
      .filter((m) => m.rows.length);
    if (!mounts.length) return '';
    const lastBk = srv.mounts
      .flatMap((m) => m.rows.map((r) => r.last_backup))
      .filter(Boolean).sort().pop();
    let note = srv.in_config
      ? (srv.scanned_at ? `scanned ${srv.scanned_at}` : 'no scan data — DB side only')
      : 'appears in the database but not in [STORAGE_MAP] config';
    note += ` · last backup ${lastBk ? lastBk.slice(0, 10) : 'never'}`;
    const tables = mounts.map((m) => `<div class="cov-mount">
      ${mountHead(m)}
      <div class="cov-scroll"><table class="cov">
        <thead><tr>
          <th>Directory</th><th class="num">On server</th>
          <th class="num" title="Recursive file size recorded in files_index">Hot DB size</th>
          <th class="num">Coverage</th>
          <th class="num" title="Recursive file count recorded in files_index">Hot DB files</th>
          <th class="num">Last backup</th><th>Status</th>
        </tr></thead>
        <tbody>${m.rows.map(covRow).join('')}</tbody>
      </table></div></div>`).join('');
    return `<div class="cov-server">
      <div class="panel-head" style="margin-top:16px">
        <h2 style="font-size:16px;margin:0">${esc(srv.name)}</h2>
        <div class="panel-sub">${esc(note)}</div>
      </div>${tables}</div>`;
  }).join('') || `<div class="placeholder">${archivedOnly
    ? 'Nothing on tape yet for the configured servers.'
    : 'Nothing to show yet.'}</div>`;
}

function maybeAutoRefreshCoverage(report) {
  /* A snapshot older than DB_FRESH_HOURS silently hides everything archived
   * since, so re-aggregate once per page load instead of waiting for a manual
   * click. Job completion re-renders through the /api/jobs poller. */
  if (autoRefreshedOnce) return;
  const age = ageHoursOf(report.generated_at_db);
  if (report.stale || age == null || age > DB_FRESH_HOURS) {
    autoRefreshedOnce = true;
    postAction('/api/coverage/refresh',
               'DB snapshot is stale — re-aggregating the tape catalog…');
  }
}

/* -------------------------------------------------------------- actions -- */
const JOB_BUTTONS = {scan: '#btn-scan', fetch: '#btn-fetch', coverage: '#btn-coverage'};
let prevJobs = {};
let pollTimer = null;

async function refreshJobs() {
  let jobs = {};
  try { jobs = await getJSON('/api/jobs'); } catch (err) { return; }
  let anyRunning = false;
  const notes = [];
  for (const [name, sel] of Object.entries(JOB_BUTTONS)) {
    const job = jobs[name];
    const running = !!(job && job.state === 'running');
    $(sel).disabled = running;
    if (running) { anyRunning = true; notes.push(`${name}: running…`); }
    const before = prevJobs[name];
    if (before && before.state === 'running' && job && job.state !== 'running') {
      notes.push(`${name}: ${job.state}${job.detail ? ` — ${job.detail}` : ''}`);
      if (job.state === 'done') {
        if (name === 'fetch') { loadOverview(); loadCoverage(); }
        if (name === 'coverage') loadCoverage();
      }
    }
  }
  prevJobs = jobs;
  if (notes.length) setStatus(notes.join(' · '),
                              notes.some((n) => n.includes('failed')));
  if (anyRunning && !pollTimer) {
    pollTimer = setInterval(refreshJobs, 2000);
  } else if (!anyRunning && pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

async function postAction(url, startNote) {
  setStatus(startNote);
  try {
    const resp = await authFetch(url, {method: 'POST',
                                       headers: {'Content-Type': 'application/json'}});
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new Error(body.detail || `${resp.status} ${resp.statusText}`);
    }
  } catch (err) {
    setStatus(err.message, true);
    return;
  }
  await refreshJobs();
}

async function checkStatus() {
  const btn = $('#btn-status');
  btn.disabled = true;
  setStatus('Asking servers for scan status…');
  try {
    const data = await getJSON('/api/scan/status');
    const parts = Object.entries(data.servers).map(([name, st]) =>
      `${name}: ${st.state}${st.started_at ? ` (launched ${st.started_at})` : ''}`);
    setStatus(parts.join(' · '));
  } catch (err) {
    setStatus(err.message, true);
  } finally {
    btn.disabled = false;
  }
}

/* --------------------------------------------------------------- export -- */
async function exportHtml() {
  setStatus('Building static HTML snapshot...');
  try {
    const css = await fetch('/static/style.css').then(resp => resp.text());
    const copy = document.querySelector('.wrap').cloneNode(true);
    copy.querySelector('.actions')?.remove();
    copy.querySelector('.cov-toggle')?.remove();
    copy.querySelector('#statusline')?.remove();
    const generated = new Date().toLocaleString();
    const source = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Storage Map snapshot</title><style>${css}</style></head><body>
${copy.outerHTML}<div class="snapshot-note">Static snapshot generated ${esc(generated)}</div>
</body></html>`;
    const response = await authFetch('/api/export/html', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({html: source}),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.detail || `${response.status} ${response.statusText}`);
    }
    setStatus(`Static HTML snapshot saved to ${body.path || body.saved}.`);
  } catch (err) {
    setStatus(`HTML export failed: ${err.message}`, true);
  }
}

/* ----------------------------------------------------------------- init -- */
async function loadOverview() {
  try {
    renderOverview(await getJSON('/api/overview'));
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function loadCoverage() {
  try {
    const report = await getJSON('/api/coverage');
    renderCoverage(report);
    maybeAutoRefreshCoverage(report);
  } catch (err) {
    $('#coverage').innerHTML = `<div class="placeholder">${esc(err.message)}</div>`;
  }
}

$('#btn-scan').addEventListener('click', () =>
  postAction('/api/scan', 'Launching remote scans…'));
$('#btn-fetch').addEventListener('click', () =>
  postAction('/api/fetch', 'Fetching finished scan logs…'));
$('#btn-coverage').addEventListener('click', () =>
  postAction('/api/coverage/refresh', 'Aggregating the archive database…'));
$('#btn-status').addEventListener('click', checkStatus);
$('#btn-export-html').addEventListener('click', exportHtml);
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
  loadOverview();
});

const archivedOnlyBox = $('#cov-archived-only');
archivedOnlyBox.checked = localStorage.getItem(ARCHIVED_ONLY_KEY) === '1';
archivedOnlyBox.addEventListener('change', () => {
  localStorage.setItem(ARCHIVED_ONLY_KEY, archivedOnlyBox.checked ? '1' : '0');
  if (lastCoverage) renderCoverage(lastCoverage);
});

loadOverview();
loadCoverage();
refreshJobs();
