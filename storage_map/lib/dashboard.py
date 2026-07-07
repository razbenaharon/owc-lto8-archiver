"""Storage Map dashboard renderer — turn parsed scans into one HTML page.

This is the presentation layer for :mod:`storage_map.lib.core`. It takes the
already parsed :class:`~storage_map.lib.core.ScanResult` objects (mount ->
immediate child, plus one extra level inside shared-data) and emits a single,
self-contained ``index.html``: no external assets, no network needed to view it
(the Plotly runtime is embedded).

Design follows the project's data-viz guidance:
  * Disk usage is a *magnitude*, so the bar marks use one flat sequential hue —
    length carries the value, never a rainbow of colours.
  * The treemap's colour encodes *identity* (which mount) with the categorical
    palette; area already carries magnitude.
  * Colours, ink and surfaces are the validated palette, exposed as CSS custom
    properties so light/dark mode swap in one place.

Only the local, in-memory scan results are read here — no disk or SSH access.
"""
import html
import os
import sys
from datetime import datetime

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from storage_map.lib.core import human
else:
    from .core import human

# --------------------------------------------------------------------------- #
# Palette (validated data-viz reference instance) — light/dark pairs.         #
# Kept here as the single source for the inlined CSS custom properties.        #
# --------------------------------------------------------------------------- #
SERIES = '#2a78d6'  # sequential blue — the one hue every magnitude bar uses.

# One categorical hue per top-level mount, mirroring MOUNT_HUES in storage_map
# so a mount's chip on the dashboard matches its tile in the treemap.
MOUNT_HUES = ['#2a78d6', '#1baf7a', '#eda100', '#008300',
              '#4a3aa7', '#e34948', '#e87ba4', '#eb6834']


def _e(text):
    """HTML-escape an arbitrary string (folder names can contain anything)."""
    return html.escape(str(text), quote=True)


def _leaves(node, mount, acc):
    """Collect visible folders under ``node``, tagged w/ mount."""
    if not node.children:
        acc.append((node, mount))
        return
    for child in node.children:
        _leaves(child, mount, acc)


def _server_leaves(res):
    """All leaf folders across one server's mounts, biggest first."""
    acc = []
    for mt in res.mounts:
        for child in mt.root.children:
            _leaves(child, mt.mount, acc)
    acc.sort(key=lambda nm: nm[0].size, reverse=True)
    return acc


def _bar_row(label, sublabel, value_text, pct_width, hue=SERIES):
    """One horizontal magnitude bar (label · track · direct value label)."""
    pct = max(0.0, min(100.0, pct_width))
    sub = f'<span class="row-sub">{_e(sublabel)}</span>' if sublabel else ''
    return (
        '<div class="bar-row">'
        f'<div class="bar-label" title="{_e(label)}">{_e(label)}{sub}</div>'
        '<div class="bar-track">'
        f'<div class="bar-fill" style="width:{pct:.2f}%;background:{hue}"></div>'
        '</div>'
        f'<div class="bar-value">{_e(value_text)}</div>'
        '</div>'
    )


def _kpi(value, label, accent=False):
    cls = 'kpi kpi-accent' if accent else 'kpi'
    return (f'<div class="{cls}"><div class="kpi-value">{_e(value)}</div>'
            f'<div class="kpi-label">{_e(label)}</div></div>')


def _mount_value_text(mt, fallback_total):
    if mt.has_capacity:
        used_bytes = mt.used_bytes
        if used_bytes is None:
            used_bytes = max(0, mt.capacity_bytes - mt.free_bytes)
        return (
            f'{human(used_bytes)} used · '
            f'{human(mt.free_bytes)} left · {mt.free_percent:.1f}% left'
        )
    return f'{human(mt.total)} used · left n/a'


def _mount_bar_width(mt, fallback_total):
    if mt.used_percent is not None:
        return mt.used_percent
    return mt.total / (fallback_total or 1) * 100


def _server_panel(res):
    """Render one server: sub-KPIs, mount-usage bars, top-consumer bars."""
    total = res.total or 1
    mounts = sorted(res.mounts, key=lambda m: m.total, reverse=True)
    # Stable hue per mount by config order (matches the treemap tiles).
    hue_by_mount = {mt.mount: MOUNT_HUES[i % len(MOUNT_HUES)]
                    for i, mt in enumerate(res.mounts)}

    mount_bars = ''.join(
        _bar_row(mt.mount, None, _mount_value_text(mt, total),
                 _mount_bar_width(mt, total), hue=hue_by_mount[mt.mount])
        for mt in mounts)

    leaves = _server_leaves(res)[:12]
    top_size = leaves[0][0].size if leaves else 1
    consumer_bars = ''.join(
        _bar_row(node.name, mount,
                 f'{human(node.size)} · {node.size / total * 100:.1f}%',
                 node.size / (top_size or 1) * 100,
                 hue=hue_by_mount.get(mount, SERIES))
        for node, mount in leaves)

    biggest_mount = mounts[0] if mounts else None
    scanned = res.generated_at or 'n/a'
    return f"""
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>{_e(res.server)}</h2>
          <div class="panel-sub">{_e(res.host or 'unknown host')} · scanned {_e(scanned)}</div>
        </div>
        <div class="panel-kpis">
          {_kpi(human(res.total), 'total used', accent=True)}
          {_kpi(biggest_mount.mount if biggest_mount else '—', 'largest mount')}
          {_kpi(str(len(res.mounts)), 'mounts')}
        </div>
      </div>
      <div class="panel-grid">
        <div class="chart-block">
          <h3>Usage by mount</h3>
          <div class="bars">{mount_bars}</div>
        </div>
        <div class="chart-block">
          <h3>Top folders <span class="muted">(largest 12)</span></h3>
          <div class="bars">{consumer_bars}</div>
        </div>
      </div>
    </section>
    """


def render_dashboard(results, fig, out_dir):
    """Write the dashboard HTML for ``results`` and return its path.

    ``fig`` is the shared Plotly treemap figure (or ``None`` when Plotly is not
    installed, in which case the treemap section is simply omitted). The page is
    written to ``out_dir/index.html`` as the stable, always-current dashboard.
    """
    os.makedirs(out_dir, exist_ok=True)

    grand_total = sum(r.total for r in results)
    n_mounts = sum(len(r.mounts) for r in results)
    all_leaves = [nm for r in results for nm in _server_leaves(r)]
    biggest = max(all_leaves, key=lambda nm: nm[0].size, default=None)
    biggest_txt = (f'{biggest[0].name} · {human(biggest[0].size)}'
                   if biggest else '—')

    global_kpis = ''.join([
        _kpi(human(grand_total), 'total used (all servers)', accent=True),
        _kpi(str(len(results)), 'servers'),
        _kpi(str(n_mounts), 'mounts scanned'),
        _kpi(biggest_txt, 'largest single folder'),
    ])

    panels = ''.join(_server_panel(r) for r in results)

    if fig is not None:
        treemap_div = fig.to_html(full_html=False, include_plotlyjs=True,
                                  config={'displayModeBar': False,
                                          'displaylogo': False,
                                          'responsive': True})
        treemap_section = f"""
        <section class="panel">
          <div class="panel-head"><div><h2>Interactive treemap</h2>
            <div class="panel-sub">Click any block to drill in · click the crumb to zoom out</div>
          </div></div>
          <div class="treemap">{treemap_div}</div>
        </section>
        """
    else:
        treemap_section = ('<section class="panel"><div class="panel-head"><div>'
                           '<h2>Interactive treemap</h2><div class="panel-sub">'
                           'Install <code>plotly</code> to enable the treemap '
                           '(pip install plotly).</div></div></div></section>')

    generated = datetime.now().strftime('%Y-%m-%d %H:%M')
    servers_label = ', '.join(r.server for r in results)

    page = _PAGE_TEMPLATE.format(
        css=_CSS,
        servers=_e(servers_label),
        generated=_e(generated),
        global_kpis=global_kpis,
        panels=panels,
        treemap=treemap_section,
    )

    index_path = os.path.join(out_dir, 'index.html')
    with open(index_path, 'w', encoding='utf-8') as fh:
        fh.write(page)
    return index_path


# --------------------------------------------------------------------------- #
# Static assets                                                               #
# --------------------------------------------------------------------------- #
_CSS = """
:root {
  --plane:#f9f9f7; --surface:#fcfcfb;
  --ink:#0b0b0b; --ink-2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --border:rgba(11,11,11,0.10);
  --series:#2a78d6; --track:#ecebe4;
}
@media (prefers-color-scheme: dark) {
  :root {
    --plane:#0d0d0d; --surface:#1a1a19;
    --ink:#ffffff; --ink-2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --border:rgba(255,255,255,0.10);
    --series:#3987e5; --track:#26261f;
  }
}
* { box-sizing:border-box; }
body {
  margin:0; background:var(--plane); color:var(--ink);
  font-family:system-ui,-apple-system,"Segoe UI",sans-serif;
  font-size:15px; line-height:1.45;
}
.wrap { max-width:1120px; margin:0 auto; padding:32px 24px 64px; }
header.top { margin-bottom:24px; }
header.top h1 { margin:0 0 4px; font-size:26px; letter-spacing:-0.01em; }
header.top .sub { color:var(--ink-2); font-size:14px; }
.kpi-row { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
  gap:14px; margin:20px 0 8px; }
.kpi { background:var(--surface); border:1px solid var(--border);
  border-radius:12px; padding:16px 18px; }
.kpi-accent { border-color:var(--series); }
.kpi-value { font-size:22px; font-weight:650; letter-spacing:-0.01em; }
.kpi-label { color:var(--muted); font-size:12.5px; text-transform:uppercase;
  letter-spacing:0.04em; margin-top:2px; }
.panel { background:var(--surface); border:1px solid var(--border);
  border-radius:16px; padding:22px 24px; margin-top:22px; }
.panel-head { display:flex; justify-content:space-between; align-items:flex-start;
  gap:20px; flex-wrap:wrap; margin-bottom:6px; }
.panel-head h2 { margin:0; font-size:19px; }
.panel-sub { color:var(--ink-2); font-size:13px; margin-top:2px; }
.panel-kpis { display:flex; gap:10px; flex-wrap:wrap; }
.panel-kpis .kpi { padding:8px 14px; border-radius:10px; min-width:96px; }
.panel-kpis .kpi-value { font-size:16px; }
.panel-kpis .kpi-label { font-size:11px; }
.panel-grid { display:grid; grid-template-columns:1fr 1fr; gap:28px; margin-top:16px; }
@media (max-width:760px){ .panel-grid { grid-template-columns:1fr; } }
.chart-block h3 { margin:0 0 12px; font-size:14px; font-weight:600; }
.chart-block h3 .muted { color:var(--muted); font-weight:400; }
.bars { display:flex; flex-direction:column; gap:9px; }
.bar-row { display:grid; grid-template-columns:150px 1fr auto; align-items:center;
  gap:12px; }
.bar-label { font-size:13px; overflow:hidden; text-overflow:ellipsis;
  white-space:nowrap; }
.bar-label .row-sub { color:var(--muted); font-size:11px; margin-left:6px; }
.bar-track { height:12px; background:var(--track); border-radius:6px; overflow:hidden; }
.bar-fill { height:100%; border-radius:6px; min-width:3px; }
.bar-value { font-size:12.5px; color:var(--ink-2); white-space:nowrap;
  font-variant-numeric:tabular-nums; }
.treemap { margin-top:8px; }
.muted { color:var(--muted); }
code { background:var(--track); padding:1px 6px; border-radius:5px; font-size:13px; }
footer { color:var(--muted); font-size:12.5px; margin-top:28px; text-align:center; }
"""

_PAGE_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Storage Map — {servers}</title>
<style>{css}</style>
</head>
<body>
<div class="wrap">
  <header class="top">
    <h1>Storage Map</h1>
    <div class="sub">{servers} · generated {generated}</div>
  </header>
  <div class="kpi-row">{global_kpis}</div>
  {panels}
  {treemap}
  <footer>Storage Map &amp; Analytics · lab disk-usage dashboard</footer>
</div>
</body>
</html>
"""


if __name__ == "__main__":
    from storage_map.lib.core import main

    raise SystemExit(main(["dashboard", *sys.argv[1:]]))
