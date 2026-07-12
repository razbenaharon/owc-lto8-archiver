"""Static snapshot exporters for the Storage Map web application."""
from io import BytesIO
from html import escape

from storage_map.lib.core import human


def _percent(value):
    return 'n/a' if value is None else f'{value:.1f}%'


def build_pdf(overview, coverage, generated_at):
    """Return a paginated PDF snapshot as bytes."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_RIGHT
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import (KeepTogether, PageBreak, Paragraph,
                                       SimpleDocTemplate, Spacer, Table,
                                       TableStyle)
    except ImportError as exc:  # pragma: no cover - dependency hint
        raise RuntimeError('PDF export needs reportlab (pip install reportlab)') from exc

    out = BytesIO()
    page_size = landscape(A4)
    doc = SimpleDocTemplate(
        out, pagesize=page_size, leftMargin=13 * mm, rightMargin=13 * mm,
        topMargin=14 * mm, bottomMargin=14 * mm,
        title='Storage Map state', author='LTO Storage Map')
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Meta', parent=styles['Normal'],
                              fontSize=8.5, leading=11,
                              textColor=colors.HexColor('#52514e')))
    styles.add(ParagraphStyle(name='Cell', parent=styles['Normal'],
                              fontSize=7.4, leading=9))
    styles.add(ParagraphStyle(name='CellRight', parent=styles['Cell'],
                              alignment=TA_RIGHT))
    styles['Title'].textColor = colors.HexColor('#0b0b0b')
    styles['Heading2'].spaceBefore = 10
    styles['Heading2'].spaceAfter = 5

    def footer(canvas, current_doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 7.5)
        canvas.setFillColor(colors.HexColor('#898781'))
        canvas.drawString(13 * mm, 7 * mm, f'Generated {generated_at}')
        canvas.drawRightString(page_size[0] - 13 * mm, 7 * mm,
                               f'Page {current_doc.page}')
        canvas.restoreState()

    story = [Paragraph('Storage Map state', styles['Title']),
             Paragraph(
                 f"Generated {generated_at} | {len(overview.get('servers', []))} "
                 f"servers | {human(overview.get('grand_total', 0))} scanned",
                 styles['Meta']), Spacer(1, 5 * mm)]
    overview_by_name = {s['name']: s for s in overview.get('servers', [])}
    coverage_servers = coverage.get('servers', [])
    for server_index, server in enumerate(coverage_servers):
        ov = overview_by_name.get(server['name'], {})
        story.append(KeepTogether([
            Paragraph(escape(str(server['name'])), styles['Heading2']),
            Paragraph(
                f"{escape(str(ov.get('host', 'unknown host')))} | scanned "
                f"{escape(str(server.get('scanned_at') or 'n/a'))} | "
                f"{human(ov.get('total', 0))} used",
                styles['Meta']), Spacer(1, 2 * mm)]))

        for mount in server.get('mounts', []):
            story.append(Paragraph(
                f"Mount: {escape(str(mount['mount']))}", styles['Heading3']))
            data = [[Paragraph(v, styles['Cell']) for v in
                     ('Directory', 'On server', 'Hot DB size', 'Coverage',
                      'Hot DB files', 'Last backup', 'Status')]]
            for row in mount.get('rows', []):
                indent = '&nbsp;' * min(12, row.get('depth', 0) * 3)
                data.append([
                    Paragraph(indent + escape(str(row.get('name') or row['path'])),
                              styles['Cell']),
                    Paragraph(human(row.get('server_bytes'))
                              if row.get('server_bytes') is not None else 'n/a',
                              styles['CellRight']),
                    Paragraph(human(row.get('tape_bytes', 0)), styles['CellRight']),
                    Paragraph(_percent(row.get('coverage_pct')), styles['CellRight']),
                    Paragraph(str(row.get('tape_files', 0)), styles['CellRight']),
                    Paragraph(escape(str(row.get('last_backup') or 'n/a')),
                              styles['Cell']),
                    Paragraph(escape(str(row.get('status', 'n/a'))
                                     .replace('_', ' ')),
                              styles['Cell']),
                ])
            table = Table(data, repeatRows=1,
                          colWidths=[72 * mm, 25 * mm, 25 * mm, 20 * mm,
                                     18 * mm, 39 * mm, 24 * mm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2a78d6')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#e1e0d9')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1),
                 [colors.white, colors.HexColor('#f5f6f8')]),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            story.extend([table, Spacer(1, 3 * mm)])
        if server_index < len(coverage_servers) - 1:
            story.append(PageBreak())

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return out.getvalue()
