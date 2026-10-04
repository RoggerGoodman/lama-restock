# LamApp/supermarkets/margin_analysis.py
"""
"Analisi Margini": till sales by department (the "Analisi Vendite per Reparto" PDF)
against what Dropzone says was bought in the same Monday–Sunday period.

Purchases come from one Scorporo Amministrativo request per period: each row is one
document × one department, amounts without IVA, credit notes already negative.
Invoices (FAT) are left out: they only summarise the DDTs already counted.
"""
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import pdfplumber

logger = logging.getLogger(__name__)

PURCHASE_TYPES = ('BOL', 'BRF', 'NAC', 'NAD')
# Departments the store does not want in the analysis at all
EXCLUDED_DEPARTMENTS = {890}
# Shown on the line of the department they belong with: 315 "Gastronomia calda centrale"
# with 940 "Gastronomia calda", 993 "Take away" with 310 "Banco gastronomia"
MERGED_DEPARTMENTS = {315: 940, 993: 310}
# Bought to run the store, not to resell: shown in their own block, still in the store total
RUNNING_COST_DEPARTMENTS = {900, 920, 950}
# Direct-supplier DDTs (BRF) reach Dropzone up to ~2 months late; 97% of their value is
# there after 45 days, all of it after ~62
LATE_DOCUMENTS_DAYS = 45
REFRESH_WINDOW_DAYS = 70

PERIOD_RE = re.compile(r"Periodo dal (\d{2}/\d{2}/\d{4}) al (\d{2}/\d{2}/\d{4})")
LINE_RE = re.compile(
    r"^(\d{3})\s*(.+?)\s+\d{2}/\d{2}/\d{4}\s*-\s*\d{2}/\d{2}/\d{4}\s+(-?[\d.]+,\d{2})\s+(-?[\d.]+,\d{2})"
)
TOTAL_RE = re.compile(r"^TOTALE GENERALE\s+(-?[\d.]+,\d{2})\s+(-?[\d.]+,\d{2})")


class SalesReportError(ValueError):
    pass


def it_decimal(text) -> Decimal:
    """'1.234,56' (as printed) or '1234.56' / '1234,56' (as typed) → Decimal."""
    s = str(text).strip().replace(' ', '').replace('€', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return Decimal(s)
    except InvalidOperation:
        raise ValueError(f"Importo non valido: {text!r}")


def parse_sales_pdf(file) -> dict:
    """
    Read the till's "Analisi Vendite per Reparto" (Venduto senza IVA).
    Returns {start, end, departments: {code: {name, till_cost, sales}}}.
    Refuses a PDF whose department rows don't add up to its own TOTALE GENERALE.
    """
    with pdfplumber.open(file) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    period = PERIOD_RE.search(text)
    if not period:
        raise SalesReportError("Periodo non trovato: è il report \"Analisi Vendite per Reparto\"?")
    start = datetime.strptime(period.group(1), "%d/%m/%Y").date()
    end = datetime.strptime(period.group(2), "%d/%m/%Y").date()

    departments, total = {}, None
    for line in text.splitlines():
        line = line.strip()
        m = LINE_RE.match(line)
        if m:
            departments[int(m.group(1))] = {
                'name': m.group(2).strip(),
                'till_cost': it_decimal(m.group(3)),
                'sales': it_decimal(m.group(4)),
            }
            continue
        m = TOTAL_RE.match(line)
        if m:
            total = (it_decimal(m.group(1)), it_decimal(m.group(2)))

    if not departments:
        raise SalesReportError("Nessun reparto trovato nel PDF.")
    if total is None:
        raise SalesReportError("Riga \"TOTALE GENERALE\" non trovata nel PDF.")
    read = (sum(d['till_cost'] for d in departments.values()), sum(d['sales'] for d in departments.values()))
    if abs(read[0] - total[0]) > Decimal('0.10') or abs(read[1] - total[1]) > Decimal('0.10'):
        raise SalesReportError(
            f"Lettura incompleta: i reparti sommano {read[1]} di venduto, il PDF dice {total[1]}."
        )
    return {'start': start, 'end': end, 'departments': departments}


def period_errors(start, end) -> list:
    errors = []
    if start.weekday() != 0:
        errors.append(f"Il periodo deve iniziare di lunedì ({start:%d/%m/%Y} non lo è).")
    if end.weekday() != 6:
        errors.append(f"Il periodo deve finire di domenica ({end:%d/%m/%Y} non lo è).")
    if end < start:
        errors.append("La data di fine è precedente a quella di inizio.")
    return errors


def fetch_purchases(supermarket, start, end) -> dict:
    """
    {department_code: {name, BOL, BRF, NAC, NAD}} for documents dated start..end
    ("Data documento riferimento": the DDT's own date, not the invoice's).
    """
    from .scripts.dropzone_client import DropzoneClient

    client = DropzoneClient(supermarket.username, supermarket.password)
    client.login()
    try:
        x5cper = supermarket.x5cper or client.fetch_x5cper()
        rows = client.fetch_document_headers(x5cper, start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    finally:
        client.session.close()

    out = defaultdict(lambda: {'name': '', **{t: Decimal('0') for t in PURCHASE_TYPES}})
    for row in rows:
        kind = row.get("X5CNAT")
        if kind not in PURCHASE_TYPES:
            continue
        dept = out[int(row["X5CRGM"])]
        dept['name'] = dept['name'] or (row.get("X5XDES") or "").strip()
        dept[kind] += Decimal(str(row["X5VI01"]))
    return dict(out)


def apply_purchases(period, purchases):
    """Write fetched purchases onto the period's lines, adding departments the till never sold."""
    from django.utils import timezone
    from .models import MarginLine

    lines = {l.department_code: l for l in period.lines.all()}
    for code, p in purchases.items():
        line = lines.get(code)
        if line is None:
            line = MarginLine(period=period, department_code=code, department_name=p['name'])
            lines[code] = line
        line.bol, line.brf, line.nac, line.nad = p['BOL'], p['BRF'], p['NAC'], p['NAD']
    for code, line in lines.items():
        if code not in purchases:
            line.bol = line.brf = line.nac = line.nad = Decimal('0')
        line.save()
    period.purchases_fetched_at = timezone.now()
    period.save(update_fields=['purchases_fetched_at'])


def group_key(code):
    if code in EXCLUDED_DEPARTMENTS:
        return None
    return MERGED_DEPARTMENTS.get(code, code)


def report_columns(report) -> list:
    """
    The report's extra items: [{'name', 'department'}], department being a group code.
    Reports saved before items had a department stored bare names: those are resolved
    from the departments their amounts were entered on.
    """
    from .models import MarginExtra

    out = []
    for column in report.extra_columns:
        if isinstance(column, dict):
            out.append({'name': column['name'], 'department': int(column['department'])})
            continue
        departments = sorted(set(
            MarginExtra.objects.filter(period__report=report, column=column)
            .values_list('department_code', flat=True)
        ))
        out.extend({'name': column, 'department': d} for d in departments)
    return out


def _metrics(sales, till_cost, purchases, extras):
    total_cost = purchases + extras
    margin = sales - total_cost
    return {
        'sales': sales,
        'till_cost': till_cost,
        'purchases': purchases,
        'extras': extras,
        'total_cost': total_cost,
        'margin': margin,
        'margin_pct': (margin / sales * 100) if sales else None,
        'till_margin_pct': ((sales - till_cost) / sales * 100) if sales else None,
    }


class _Acc:
    def __init__(self):
        self.sales = self.till_cost = self.extras = Decimal('0')
        self.by_type = {t: Decimal('0') for t in PURCHASE_TYPES}

    def add_line(self, line):
        self.sales += line.sales
        self.till_cost += line.till_cost
        self.by_type['BOL'] += line.bol
        self.by_type['BRF'] += line.brf
        self.by_type['NAC'] += line.nac
        self.by_type['NAD'] += line.nad

    def add(self, other):
        self.sales += other.sales
        self.till_cost += other.till_cost
        self.extras += other.extras
        for t in PURCHASE_TYPES:
            self.by_type[t] += other.by_type[t]

    def row(self, **extra):
        purchases = sum(self.by_type.values(), Decimal('0'))
        return {**_metrics(self.sales, self.till_cost, purchases, self.extras), 'by_type': dict(self.by_type), **extra}


def build_report(report) -> dict:
    """
    Everything the page shows, in one pass:
      departments / running_costs: one row per department over the whole chain, with
        subtotals and the store total;
      per_period: for the whole store and for every department, one row per period
        (the page switches between them without reloading);
      extras: the grid of extra amounts, one row per period, one column per item.
    """
    columns = report_columns(report)
    col_index = {(c['name'], c['department']): i for i, c in enumerate(columns)}
    periods = list(report.periods.prefetch_related('lines', 'extras'))

    cells = defaultdict(_Acc)       # (group code, period index) -> _Acc
    names = {}
    grid = [[None] * len(columns) for _ in periods]
    for pi, period in enumerate(periods):
        for line in period.lines.all():
            key = group_key(line.department_code)
            if key is None:
                continue
            if line.department_code == key or key not in names:
                names[key] = line.department_name
            cells[(key, pi)].add_line(line)
        for extra in period.extras.all():
            key = group_key(extra.department_code)
            index = col_index.get((extra.column, key))
            if index is None:
                continue
            cells[(key, pi)].extras += extra.amount
            grid[pi][index] = (grid[pi][index] or Decimal('0')) + extra.amount

    keys = sorted({k for k, _ in cells} | {c['department'] for c in columns})

    def label(key):
        merged = sorted(c for c, k in MERGED_DEPARTMENTS.items() if k == key)
        suffix = f" (+{', '.join(map(str, merged))})" if merged else ""
        return f"{key} {names.get(key, '')}{suffix}".strip()

    def chain(key):
        acc = _Acc()
        for pi in range(len(periods)):
            acc.add(cells.get((key, pi), _Acc()))
        return acc

    dept_rows, cost_rows = [], []
    dept_total, cost_total, store_total = _Acc(), _Acc(), _Acc()
    for key in keys:
        acc = chain(key)
        row = acc.row(code=key, label=label(key))
        if key in RUNNING_COST_DEPARTMENTS:
            cost_rows.append(row)
            cost_total.add(acc)
        else:
            dept_rows.append(row)
            dept_total.add(acc)
    store_total.add(dept_total)
    store_total.add(cost_total)

    from datetime import date
    late_before = date.today() - timedelta(days=LATE_DOCUMENTS_DAYS)

    def period_table(selected_keys):
        rows, total = [], _Acc()
        for pi, period in enumerate(periods):
            acc = _Acc()
            for key in selected_keys:
                acc.add(cells.get((key, pi), _Acc()))
            rows.append(acc.row(period=period, number=pi + 1, may_grow=period.end > late_before))
            total.add(acc)
        return {'rows': rows, 'total': total.row()}

    per_period = [{'code': '', 'label': 'Tutti i reparti', **period_table(keys)}]
    per_period += [{'code': str(k), 'label': label(k), **period_table([k])} for k in keys]

    extras_columns = [{**c, 'index': i, 'label': label(c['department']),
                       'total': sum((row[i] or Decimal('0') for row in grid), Decimal('0'))}
                      for i, c in enumerate(columns)]
    extras_rows = [{'period': p, 'number': pi + 1, 'cells': grid[pi],
                    'total': sum((v or Decimal('0') for v in grid[pi]), Decimal('0'))}
                   for pi, p in enumerate(periods)]

    return {
        'departments': dept_rows,
        'departments_total': dept_total.row(),
        'running_costs': cost_rows,
        'running_costs_total': cost_total.row(),
        'store_total': store_total.row(),
        'department_choices': [(k, label(k)) for k in keys],
        'per_period': per_period,
        'periods': periods,
        'late_periods': [p for p in periods if p.end > late_before],
        'extras_columns': extras_columns,
        'extras_rows': extras_rows,
        'extras_total': sum((c['total'] for c in extras_columns), Decimal('0')),
    }
