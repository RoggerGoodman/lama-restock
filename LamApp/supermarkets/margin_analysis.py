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
EXCLUDED_DEPARTMENTS = {890, 993}
# Shown as one line: 315 "Gastronomia calda centrale" belongs with 940 "Gastronomia calda"
MERGED_DEPARTMENTS = {315: 940}
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


def _metrics(sales, till_cost, purchases, extras):
    total_cost = purchases + sum(extras, Decimal('0'))
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
    def __init__(self, columns):
        self.sales = self.till_cost = Decimal('0')
        self.by_type = {t: Decimal('0') for t in PURCHASE_TYPES}
        self.extras = {c: Decimal('0') for c in columns}

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
        for t in PURCHASE_TYPES:
            self.by_type[t] += other.by_type[t]
        for c in self.extras:
            self.extras[c] += other.extras[c]

    def row(self, columns, **extra):
        purchases = sum(self.by_type.values(), Decimal('0'))
        return {**_metrics(self.sales, self.till_cost, purchases, [self.extras[c] for c in columns]),
                'by_type': dict(self.by_type), **extra}


def build_report(report, department=None, period_id=None) -> dict:
    """
    Everything the page shows:
      departments / running_costs: one row per department, summed over the whole chain,
      or over the single period `period_id`; subtotals and the store total;
      per_period: one row per period for `department` (a group code) or the whole store.
    """
    columns = list(report.extra_columns)
    periods = list(report.periods.prefetch_related('lines', 'extras'))

    by_dept = {}            # group code -> _Acc over the whole chain
    names = {}
    per_period = []         # (period, _Acc) for the selected department / store
    for period in periods:
        in_chain_view = period_id is None or period.pk == period_id
        period_acc = _Acc(columns)
        for line in period.lines.all():
            key = group_key(line.department_code)
            if key is None:
                continue
            if line.department_code == key or key not in names:
                names[key] = line.department_name
            dept = by_dept.setdefault(key, _Acc(columns))
            if in_chain_view:
                dept.add_line(line)
            if department is None or department == key:
                period_acc.add_line(line)
        for extra in period.extras.all():
            key = group_key(extra.department_code)
            if key is None or extra.column not in columns:
                continue
            if in_chain_view:
                by_dept.setdefault(key, _Acc(columns)).extras[extra.column] += extra.amount
            if department is None or department == key:
                period_acc.extras[extra.column] += extra.amount
        per_period.append((period, period_acc))

    def label(key):
        merged = sorted(c for c, k in MERGED_DEPARTMENTS.items() if k == key)
        suffix = f" (+{', '.join(map(str, merged))})" if merged else ""
        return f"{key} {names.get(key, '')}{suffix}".strip()

    dept_rows, cost_rows = [], []
    dept_total, cost_total = _Acc(columns), _Acc(columns)
    for key in sorted(by_dept):
        acc = by_dept[key]
        row = acc.row(columns, code=key, label=label(key))
        if key in RUNNING_COST_DEPARTMENTS:
            cost_rows.append(row)
            cost_total.add(acc)
        else:
            dept_rows.append(row)
            dept_total.add(acc)
    store_total = _Acc(columns)
    store_total.add(dept_total)
    store_total.add(cost_total)

    from datetime import date
    late_before = date.today() - timedelta(days=LATE_DOCUMENTS_DAYS)
    period_rows = []
    chain_total = _Acc(columns)
    for period, acc in per_period:
        period_rows.append(acc.row(columns, period=period, may_grow=period.end > late_before))
        chain_total.add(acc)

    return {
        'columns': columns,
        'departments': dept_rows,
        'departments_total': dept_total.row(columns),
        'running_costs': cost_rows,
        'running_costs_total': cost_total.row(columns),
        'store_total': store_total.row(columns),
        'department_choices': [(k, label(k)) for k in sorted(by_dept)],
        'selected_label': label(department) if department is not None else None,
        'per_period': period_rows,
        'per_period_total': chain_total.row(columns),
        'periods': periods,
        'late_periods': [p for p in periods if p.end > late_before],
    }
