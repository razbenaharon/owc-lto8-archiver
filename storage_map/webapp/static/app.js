/* Storage Map v2 frontend — plain JS, no build step.
 * Data: /api/overview, /api/treemap, /api/coverage. Actions POST to /api/*
 * and are tracked by polling /api/jobs while anything runs. */
'use strict';

/* Categorical mount hues — the validated palette, light and dark steps,
 * matching MOUNT_HUES in storage_map/lib (so bars match treemap tiles). */
const HUES_LIGHT = ['#2a78d6', '#1baf7a', '#eda100', '#008300',
                    '#4a3aa7', '#e34948', '#e87ba4', '#eb6834'];
const HUES_DARK = ['#3987e5', '#199e70', '#c98500', '#008300',
                   '#9085e9', '#e66767', '#d55181', '#d95926'];
const BADGE_LABELS = {full: 'full', partial: 'partial',
                      none: 'not on tape', tape_only: 'tape only'};

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

async function getJSON(url) {
  const resp = await fetch(url);
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

function renderOverview(data) {
  const servers = data.servers || [];
  const nMounts = servers.reduce((n, s) => n + s.mounts.length, 0);
  const lastScan = servers.map((s) => s.generated_at).filter(Boolean).sort().pop();
  $('#kpis').innerHTML =
    kpi(human(data.grand_total), 'total used (all servers)', true) +
    kpi(String(servers.length), 'servers') +
    kpi(String(nMounts), 'mounts scanned') +
    kpi(lastScan ? lastScan.replace('T', ' ') : '—', 'last scan');

  $('#server-panels').innerHTML = servers.map((srv) => {
    // Hue is assigned by mount order within the server (fixed, never cycled
    // mid-render) so a mount keeps its color across panels and the treemap.
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

/* ------------------------------------------------------------- treemap -- */
async function loadTreemap() {
  const el = $('#treemap');
  try {
    const fig = await getJSON('/api/treemap');
    el.innerHTML = '';
    Plotly.newPlot(el, fig.data, fig.layout,
                   {displayModeBar: false, displaylogo: false, responsive: true});
  } catch (err) {
    el.innerHTML = `<div class="placeholder">${esc(err.message)}</div>`;
  }
}

/* ------------------------------------------------------------ coverage -- */
function covRow(row) {
  const pct = row.coverage_pct;
  const pctCell = pct == null
    ? '<span class="muted">—</span>'
    : `<span class="cov-bar"><span class="cov-track"><span class="cov-fill"
         style="width:${Math.max(0, Math.min(100, pct)).toFixed(1)}%"></span></span>
       <span${pct > 100 ? ' title="More bytes on tape than on the server — the directory shrank since it was archived"' : ''}>
         ${pct.toFixed(1)}%</span></span>`;
  const last = row.last_backup ? row.last_backup.slice(0, 10) : '—';
  return `<tr class="depth-${row.depth}">
    <td class="dir" style="padding-left:${10 + row.depth * 22}px"
        title="${esc(row.path)}">${esc(row.depth ? row.name : row.path)}</td>
    <td class="num">${esc(human(row.server_bytes))}</td>
    <td class="num">${row.tape_bytes ? esc(human(row.tape_bytes)) : '<span class="muted">—</span>'}</td>
    <td class="num">${pctCell}</td>
    <td class="num">${row.tape_files ? row.tape_files.toLocaleString() : '<span class="muted">—</span>'}</td>
    <td class="num muted">${esc(last)}</td>
    <td><span class="badge ${esc(row.status)}">${esc(BADGE_LABELS[row.status] || row.status)}</span></td>
  </tr>`;
}

function renderCoverage(report) {
  $('#coverage-sub').textContent = report.stale
    ? 'No database snapshot yet — click "Refresh DB coverage" to aggregate the tape catalog.'
    : `Server directories vs the archive database · DB aggregated ` +
      `${(report.generated_at_db || '').replace('T', ' ')} · ` +
      `top layer, shared-data depth ${report.match_depth}`;

  $('#coverage').innerHTML = (report.servers || []).map((srv) => {
    const mounts = srv.mounts.filter((m) => m.rows.length);
    if (!mounts.length) return '';
    const note = srv.in_config
      ? (srv.scanned_at ? `scanned ${srv.scanned_at}` : 'no scan data — DB side only')
      : 'appears in the database but not in [STORAGE_MAP] config';
    const tables = mounts.map((m) => `<div class="cov-mount">
      <h3>${esc(m.mount)}</h3>
      <div class="cov-scroll"><table class="cov">
        <thead><tr>
          <th>Directory</th><th class="num">On server</th>
          <th class="num">On tape</th><th class="num">Coverage</th>
          <th class="num">Files on tape</th><th class="num">Last backup</th><th>Status</th>
        </tr></thead>
        <tbody>${m.rows.map(covRow).join('')}</tbody>
      </table></div></div>`).join('');
    return `<div class="cov-server">
      <div class="panel-head" style="margin-top:16px">
        <h2 style="font-size:16px;margin:0">${esc(srv.name)}</h2>
        <div class="panel-sub">${esc(note)}</div>
      </div>${tables}</div>`;
  }).join('') || '<div class="placeholder">Nothing to show yet.</div>';
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
        if (name === 'fetch') { loadOverviewAndTreemap(); loadCoverage(); }
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
    const resp = await fetch(url, {method: 'POST',
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

/* ----------------------------------------------------------------- init -- */
async function loadOverviewAndTreemap() {
  try {
    renderOverview(await getJSON('/api/overview'));
  } catch (err) {
    setStatus(err.message, true);
  }
  await loadTreemap();
}

async function loadCoverage() {
  try {
    renderCoverage(await getJSON('/api/coverage'));
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
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
  loadOverviewAndTreemap();
});

loadOverviewAndTreemap();
loadCoverage();
refreshJobs();
