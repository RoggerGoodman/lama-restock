# LamApp/supermarkets/margin_views.py
"""Views for "Analisi Margini" (see margin_analysis.py)."""
import logging
import re
from datetime import date, timedelta
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_POST

from . import margin_analysis as ma
from .models import MarginExtra, MarginLine, MarginPeriod, MarginReport, Supermarket

logger = logging.getLogger(__name__)

SESSION_KEY = 'margin_upload'
CELL_RE = re.compile(r'^x_(\d+)_(\d+)_(\d+)$')


def _owned_report(request, pk):
    return get_object_or_404(MarginReport.objects.select_related('supermarket'),
                             pk=pk, supermarket__owner=request.user)


def _detail_url(report, department='', period=''):
    url = reverse('margin-report-detail', kwargs={'pk': report.pk})
    params = [f"{k}={v}" for k, v in (('reparto', department), ('periodo', period)) if v not in ('', None)]
    return f"{url}?{'&'.join(params)}" if params else url


def _span(report):
    periods = list(report.periods.all())
    return (periods[0].start, periods[-1].end) if periods else (None, None)


@login_required
def margin_report_list_view(request):
    supermarkets = []
    for sm in Supermarket.objects.filter(owner=request.user).prefetch_related('margin_reports__periods'):
        reports = []
        for report in sm.margin_reports.all():
            start, end = _span(report)
            reports.append({'report': report, 'start': start, 'end': end,
                            'total': ma.build_report(report)['store_total'] if start else None})
        reports.sort(key=lambda r: r['start'] or date.min)
        # A report can absorb the one that starts the day after it ends
        for current, following in zip(reports, reports[1:]):
            if current['end'] and following['start'] == current['end'] + timedelta(days=1):
                current['merge_with'] = following['report']
        supermarkets.append({'supermarket': sm, 'reports': reports,
                             'full': len(reports) >= MarginReport.MAX_PER_SUPERMARKET})
    return render(request, 'margins/list.html', {
        'supermarkets': supermarkets,
        'max_reports': MarginReport.MAX_PER_SUPERMARKET,
    })


@login_required
@require_POST
def margin_report_upload(request):
    sm = get_object_or_404(Supermarket, pk=request.POST.get('supermarket'), owner=request.user)
    pdf = request.FILES.get('pdf_file')
    if not pdf:
        messages.error(request, "Seleziona il PDF \"Analisi Vendite per Reparto\".")
        return redirect('margin-report-list')

    if sm.margin_reports.count() >= MarginReport.MAX_PER_SUPERMARKET:
        messages.error(request, f"{sm.name} ha già {MarginReport.MAX_PER_SUPERMARKET} analisi salvate: "
                                "eliminane una o uniscine due prima di caricarne un'altra.")
        return redirect('margin-report-list')

    try:
        parsed = ma.parse_sales_pdf(pdf)
    except ma.SalesReportError as e:
        messages.error(request, str(e))
        return redirect('margin-report-list')
    except Exception:
        logger.exception("Margin analysis: unreadable PDF")
        messages.error(request, "Impossibile leggere il PDF.")
        return redirect('margin-report-list')

    errors = ma.period_errors(parsed['start'], parsed['end'])
    overlap = MarginPeriod.objects.filter(report__supermarket=sm, start__lte=parsed['end'], end__gte=parsed['start']).first()
    if overlap:
        errors.append(f"Il periodo si sovrappone a uno già salvato ({overlap}).")
    if errors:
        for e in errors:
            messages.error(request, e)
        return redirect('margin-report-list')

    request.session[SESSION_KEY] = {
        'supermarket_id': sm.pk,
        'filename': pdf.name[:255],
        'start': parsed['start'].isoformat(),
        'end': parsed['end'].isoformat(),
        'departments': {str(code): {'name': d['name'], 'till_cost': str(d['till_cost']), 'sales': str(d['sales'])}
                        for code, d in parsed['departments'].items()},
    }
    departments = sorted(parsed['departments'].items())
    return render(request, 'margins/preview.html', {
        'supermarket': sm,
        'start': parsed['start'],
        'end': parsed['end'],
        'departments': departments,
        'total_sales': sum(d['sales'] for _, d in departments),
        'total_cost': sum(d['till_cost'] for _, d in departments),
    })


@login_required
@require_POST
def margin_report_confirm(request):
    data = request.session.pop(SESSION_KEY, None)
    if not data:
        messages.error(request, "Caricamento scaduto: carica di nuovo il PDF.")
        return redirect('margin-report-list')
    sm = get_object_or_404(Supermarket, pk=data['supermarket_id'], owner=request.user)
    start, end = date.fromisoformat(data['start']), date.fromisoformat(data['end'])

    with transaction.atomic():
        # Re-checked: another tab could have saved in the meantime
        if sm.margin_reports.select_for_update().count() >= MarginReport.MAX_PER_SUPERMARKET or \
                MarginPeriod.objects.filter(report__supermarket=sm, start__lte=end, end__gte=start).exists():
            messages.error(request, "Limite di analisi raggiunto o periodo già salvato.")
            return redirect('margin-report-list')
        report = MarginReport.objects.create(supermarket=sm)
        period = MarginPeriod.objects.create(report=report, start=start, end=end, source_filename=data['filename'])
        MarginLine.objects.bulk_create([
            MarginLine(period=period, department_code=int(code), department_name=d['name'][:80],
                       sales=Decimal(d['sales']), till_cost=Decimal(d['till_cost']))
            for code, d in data['departments'].items()
        ])

    try:
        ma.apply_purchases(period, ma.fetch_purchases(sm, start, end))
    except Exception:
        logger.exception(f"Margin analysis: Dropzone read failed for {sm.name} {start}–{end}")
        messages.warning(request, "Vendite salvate, ma Dropzone non ha risposto: premi \"Ricalcola\" più tardi.")
    return redirect('margin-report-detail', pk=report.pk)


@login_required
def margin_report_detail_view(request, pk):
    report = _owned_report(request, pk)
    periods = list(report.periods.all())
    department = request.GET.get('reparto')
    department = int(department) if department and department.isdigit() else None
    # The department table is editable for one period at a time; a one-period report
    # is always that period
    selected = request.GET.get('periodo')
    selected = next((p for p in periods if str(p.pk) == selected), None)
    if selected is None and len(periods) == 1:
        selected = periods[0]
    selected_id = selected.pk if selected else None
    data = ma.build_report(report, department, selected_id)
    if department is not None and department not in dict(data['department_choices']):
        department, data = None, ma.build_report(report, None, selected_id)
    return render(request, 'margins/detail.html', {
        'report': report,
        'department': department,
        'selected_period': selected,
        'chain_editable': bool(selected and data['columns']),
        'late_days': ma.LATE_DOCUMENTS_DAYS,
        **data,
    })


@login_required
@require_POST
def margin_report_refresh(request, pk):
    report = _owned_report(request, pk)
    recent = [p for p in report.periods.all() if p.end >= date.today() - timedelta(days=ma.REFRESH_WINDOW_DAYS)
              or p.purchases_fetched_at is None]
    if not recent:
        messages.info(request, "Tutti i periodi sono completi: non c'è nulla da aggiornare.")
        return redirect('margin-report-detail', pk=pk)
    try:
        for period in recent:
            ma.apply_purchases(period, ma.fetch_purchases(report.supermarket, period.start, period.end))
        count = f"{len(recent)} periodo" if len(recent) == 1 else f"{len(recent)} periodi"
        messages.success(request, f"Acquisti aggiornati da Dropzone ({count}).")
    except Exception:
        logger.exception(f"Margin analysis: refresh failed for report {pk}")
        messages.error(request, "Dropzone non ha risposto: riprova più tardi.")
    return redirect('margin-report-detail', pk=pk)


@login_required
@require_POST
def margin_report_merge(request, pk):
    report = _owned_report(request, pk)
    other = _owned_report(request, request.POST.get('other'))
    a_start, a_end = _span(report)
    b_start, b_end = _span(other)
    if other.supermarket_id != report.supermarket_id or other.pk == report.pk or \
            not a_end or b_start != a_end + timedelta(days=1):
        messages.error(request, "Si possono unire solo due analisi dello stesso punto vendita consecutive.")
        return redirect('margin-report-list')
    with transaction.atomic():
        other.periods.update(report=report)
        report.extra_columns = report.extra_columns + [c for c in other.extra_columns if c not in report.extra_columns]
        report.save(update_fields=['extra_columns'])
        other.delete()
    messages.success(request, f"Analisi unite: {a_start:%d/%m/%Y} – {b_end:%d/%m/%Y}.")
    return redirect('margin-report-detail', pk=report.pk)


@login_required
@require_POST
def margin_report_delete(request, pk):
    report = _owned_report(request, pk)
    report.delete()
    messages.success(request, "Analisi eliminata.")
    return redirect('margin-report-list')


@login_required
@require_POST
def margin_report_columns(request, pk):
    report = _owned_report(request, pk)
    action = request.POST.get('action')
    name = (request.POST.get('name') or '').strip()[:60]
    columns = list(report.extra_columns)
    back = redirect(_detail_url(report, request.POST.get('reparto', ''), request.POST.get('periodo', '')))

    if action == 'add':
        if not name or name in columns:
            messages.error(request, "Serve un nome nuovo per la colonna.")
            return back
        columns.append(name)
    elif action in ('rename', 'delete'):
        old = request.POST.get('column')
        if old not in columns:
            return back
        if action == 'rename':
            if not name or name in columns:
                messages.error(request, "Serve un nome nuovo per la colonna.")
                return back
            columns[columns.index(old)] = name
            MarginExtra.objects.filter(period__report=report, column=old).update(column=name)
        else:
            columns.remove(old)
            MarginExtra.objects.filter(period__report=report, column=old).delete()
    report.extra_columns = columns
    report.save(update_fields=['extra_columns'])
    return back


@login_required
@require_POST
def margin_report_extras(request, pk):
    """
    Save extra amounts. Each cell is posted as x_<period>_<department>_<column index>;
    only the cells on the page are touched, and an emptied cell is cleared.
    """
    report = _owned_report(request, pk)
    columns = list(report.extra_columns)
    periods = {p.pk: p for p in report.periods.all()}
    bad = []
    with transaction.atomic():
        for key, raw in request.POST.items():
            m = CELL_RE.match(key)
            if not m:
                continue
            period = periods.get(int(m.group(1)))
            index = int(m.group(3))
            if period is None or index >= len(columns):
                continue
            cell = dict(period=period, column=columns[index], department_code=int(m.group(2)))
            raw = raw.strip()
            if not raw:
                MarginExtra.objects.filter(**cell).delete()
                continue
            try:
                amount = ma.it_decimal(raw)
            except ValueError:
                bad.append(raw)
                continue
            MarginExtra.objects.update_or_create(**cell, defaults={'amount': amount})
    if bad:
        messages.error(request, f"Importi non validi, non salvati: {', '.join(bad)}")
    else:
        messages.success(request, "Spese extra salvate.")
    return redirect(_detail_url(report, request.POST.get('reparto', ''), request.POST.get('periodo', '')))
