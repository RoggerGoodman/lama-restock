"""Order comparison ("Valutazione ordine") and its saved snapshots."""

from django.utils import timezone
import json
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from pathlib import Path
from django.conf import settings

from ..models import Storage, RestockLog


@login_required
def order_comparison_view(request, storage_id):
    """
    Compare the machine-computed order against the operator-edited order.

    Both now live on the same RestockLog: results['machine_orders'] is the
    immutable snapshot the decision maker produced, results['orders'] is what the
    operator sent after reviewing. Products PAC2000A refused during "Invia" are in
    results['order_skipped_products']. No CSV export from PAC2000A is needed.
    """
    from ..models import OrderCalibrationReport

    storage = get_object_or_404(Storage, pk=storage_id, supermarket__owner=request.user)

    available_logs = (
        RestockLog.objects
        .filter(storage=storage, operation_type='full_restock',
                status__in=['completed', 'awaiting_review', 'discarded'])
        .order_by('-started_at')[:30]
    )
    available_calibrations = storage.calibration_reports.all()[:10]

    # GET-driven read (no mutation, no upload): stays reachable for the read-only
    # demo account. Accept POST too for backward-compatible links.
    params = request.GET if request.GET.get('log_id') else request.POST
    log_id_str = (params.get('log_id') or '').strip()

    if not log_id_str:
        return render(request, 'storages/valutazione_ordine.html', {
            'storage': storage,
            'available_logs': available_logs,
            'available_calibrations': available_calibrations,
        })

    # --- Load the selected order log (holds both machine + operator orders) ---
    machine_log = None
    try:
        machine_log = RestockLog.objects.filter(
            id=int(log_id_str),
            storage=storage,
            operation_type='full_restock',
        ).first()
    except (ValueError, TypeError):
        pass

    if machine_log is None:
        return render(request, 'storages/valutazione_ordine.html', {
            'storage': storage,
            'available_logs': available_logs,
            'available_calibrations': available_calibrations,
            'comparison_error': "Ordine non trovato.",
        })

    results = machine_log.get_results()

    # machine_orders = original computed snapshot. Logs created before the review
    # feature have no snapshot, so fall back to the final order (no diff to show).
    machine_src = results.get('machine_orders')
    if machine_src is None:
        machine_src = results.get('orders', [])
    machine_orders = {(o['cod'], o['var']): o['qty'] for o in machine_src}

    # human_orders = the order as the operator edited/sent it.
    human_orders = {(o['cod'], o['var']): o['qty'] for o in results.get('orders', [])}

    # Products PAC2000A rejected during submission — the website equivalent of the
    # old CSV 'Errore=Si' rows. Shown separately, excluded from the main diff.
    skipped = results.get('order_skipped_products', [])
    error_keys = {(s['cod'], s['var']) for s in skipped if 'cod' in s and 'var' in s}
    error_orders = {
        (s['cod'], s['var']): (human_orders.get((s['cod'], s['var'])) or s.get('qty', 0))
        for s in skipped if 'cod' in s and 'var' in s
    }

    # --- Merge all product keys (exclude PAC-rejected rows) ---
    all_keys = (set(human_orders.keys()) | set(machine_orders.keys())) - error_keys

    # --- Load product descriptions from DB ---
    desc_map = {}  # (cod, var) -> descrizione
    blue_dot_keys = set()
    promo_keys = set()
    all_desc_keys = all_keys | error_keys
    if all_desc_keys:
        from ..scripts.DatabaseManager import DatabaseManager
        db = DatabaseManager(supermarket_name=storage.supermarket.name)
        try:
            cur = db.cursor()
            cur.execute(
                "SELECT cod, v, descrizione FROM products WHERE settore = %s",
                (storage.settore,)
            )
            for r in cur.fetchall():
                key = (r['cod'], r['v'])
                if key in all_desc_keys:
                    desc_map[key] = r['descrizione']
            cur.execute("SELECT cod, v, internal FROM extra_losses WHERE internal IS NOT NULL")
            blue_dot_keys = set()
            for _el in cur.fetchall():
                for _entry in (_el['internal'] or [])[:2]:
                    _qty = _entry[0] if isinstance(_entry, list) else _entry
                    if _qty:
                        blue_dot_keys.add((_el['cod'], _el['v']))
                        break
            if machine_log:
                ref_date = machine_log.started_at.date()
                cur.execute(
                    "SELECT cod, v FROM economics"
                    " WHERE sale_start IS NOT NULL AND sale_end IS NOT NULL"
                    " AND sale_start <= %s AND sale_end >= %s",
                    (ref_date, ref_date)
                )
                promo_keys = {(r['cod'], r['v']) for r in cur.fetchall()}
        finally:
            db.conn.close()

    # --- Calibration report: user-selected or most recent ---
    calib_report_id_str = (params.get('calib_report_id') or '').strip()
    calibration = None
    if calib_report_id_str:
        try:
            calibration = storage.calibration_reports.filter(pk=int(calib_report_id_str)).first()
        except (ValueError, TypeError):
            pass
    if calibration is None:
        calibration = storage.calibration_reports.first()

    # Build calib_map: (cod, v) -> calibration data for display + JS burn
    calib_map = {}
    if calibration:
        cal_results = calibration.get_results()
        for outcome in ('critical', 'understocked', 'overstocked', 'ok'):
            for p in cal_results.get(outcome, []):
                stock    = p.get('stock', 0) or 0
                floor_v  = p.get('floor', 0) or 0
                eff_min  = p.get('eff_min', 0) or 0
                pkg      = p.get('package_size', 1) or 1
                avg      = p.get('avg_daily_sales', 0) or 0
                if outcome == 'overstocked':
                    excess_deficit = stock - eff_min - pkg
                    excess_days    = round(excess_deficit / avg, 1) if avg > 0 else None
                elif outcome in ('critical', 'understocked'):
                    excess_deficit = stock - eff_min   # negative
                    excess_days    = None
                else:
                    excess_deficit = None
                    excess_days    = None
                calib_map[(p['cod'], p['v'])] = {
                    'outcome': 'critical' if stock <= 0 else outcome,
                    'stock': stock,
                    'avg_daily': avg,
                    'floor': floor_v,
                    'eff_min': eff_min,
                    'package_size': pkg,
                    'excess_deficit': excess_deficit,
                    'excess_days': excess_days,
                    'shelf_life_days': p.get('shelf_life_days'),
                }

    # --- Categorise ---
    agreed = []
    human_more = []
    human_less = []
    human_zeroed = []
    human_added = []

    for key in sorted(all_keys):
        cod, var = key
        h = human_orders.get(key, 0)
        m = machine_orders.get(key, 0)
        descrizione = desc_map.get(key, f"{cod}.{var}")
        calib = calib_map.get(key)

        if key not in machine_orders:
            comp_cat = 'human_added'
        elif h == 0 and m > 0:
            comp_cat = 'human_zeroed'
        elif h == m:
            comp_cat = 'agreed'
        elif h > m:
            comp_cat = 'human_more'
        else:
            comp_cat = 'human_less'

        entry = {
            'cod': cod, 'v': var, 'descrizione': descrizione,
            'machine_qty': m, 'human_qty': h, 'diff': h - m,
            'comp_cat': comp_cat,
            'calib': calib,
            'has_blue_dot': (cod, var) in blue_dot_keys,
            'has_promo': (cod, var) in promo_keys,
        }

        if comp_cat == 'human_added':
            human_added.append(entry)
        elif comp_cat == 'human_zeroed':
            human_zeroed.append(entry)
        elif comp_cat == 'agreed':
            agreed.append(entry)
        elif comp_cat == 'human_more':
            human_more.append(entry)
        else:
            human_less.append(entry)

    # --- Products in calibration critical/understocked absent from both orders ---
    both_missed = []
    if calibration:
        cal_results = calibration.get_results()
        for outcome in ('critical', 'understocked'):
            for p in cal_results.get(outcome, []):
                key = (p['cod'], p['v'])
                if key in all_keys:
                    continue
                if key in error_keys:  # attempted but rejected by PAC2000A — separate section
                    continue
                calib = calib_map.get(key)
                if calib is None:
                    continue
                both_missed.append({
                    'cod': p['cod'], 'v': p['v'],
                    'descrizione': p.get('descrizione', f"{p['cod']}.{p['v']}"),
                    'machine_qty': 0, 'human_qty': 0, 'diff': 0,
                    'comp_cat': 'both_missed',
                    'calib': calib,
                    'has_blue_dot': (p['cod'], p['v']) in blue_dot_keys,
                    'has_promo': (p['cod'], p['v']) in promo_keys,
                })

    # --- Products rejected by PAC2000A (Errore='Si' in the CSV) ---
    pac_rejected = []
    for key in sorted(error_keys):
        cod, var = key
        machine_qty = machine_orders.get(key, 0)
        human_qty = error_orders.get(key, 0)
        descrizione = desc_map.get(key, f"{cod}.{var}")
        calib = calib_map.get(key)
        pac_rejected.append({
            'cod': cod, 'v': var, 'descrizione': descrizione,
            'machine_qty': machine_qty, 'human_qty': human_qty,
            'diff': human_qty - machine_qty,
            'comp_cat': 'pac_rejected',
            'calib': calib,
            'has_blue_dot': key in blue_dot_keys,
            'has_promo': key in promo_keys,
        })

    context = {
        'storage': storage,
        'machine_log': machine_log,
        'calibration': calibration,
        'available_calibrations': available_calibrations,
        'comparison': {
            'agreed': agreed,
            'human_more': human_more,
            'human_less': human_less,
            'human_zeroed': human_zeroed,
            'human_added': human_added,
            'both_missed': both_missed,
            'pac_rejected': pac_rejected,
            'total_human': len(human_orders),
            'total_machine': len(machine_orders),
        },
    }
    return render(request, 'storages/valutazione_ordine.html', context)


def _build_snapshot_html(storage, rows, date_str):
    from html import escape
    from collections import defaultdict

    SECTION_ORDER = ['pac_rejected', 'both_missed', 'human_more', 'human_less', 'zeroed', 'human_added', 'concordi']
    SECTION_NAMES = {
        'pac_rejected': 'Rifiutati da PAC2000A',
        'both_missed':  'Ignorati da entrambi',
        'human_more':   'Umano → Macchina (umano >)',
        'human_less':   'Umano → Macchina (umano <)',
        'zeroed':       'Azzerati',
        'human_added':  "Aggiunti dall'operatore",
        'concordi':     'Concordi',
    }
    OUTCOME_BADGE = {
        'critical':     '<span class="badge b-crit">Critico</span>',
        'understocked': '<span class="badge b-under">Sotto</span>',
        'overstocked':  '<span class="badge b-over">Sovra</span>',
        'ok':           '<span class="badge b-ok">OK</span>',
    }
    OUTCOME_SORT = {'critical': 0, 'understocked': 1, 'ok': 2, 'overstocked': 3, '': 4}

    grouped = defaultdict(list)
    for r in rows:
        grouped[r.get('section', '')].append(r)
    for sec in grouped:
        grouped[sec].sort(key=lambda r: OUTCOME_SORT.get(r.get('outcome', ''), 4))

    sections_html = []
    for key in SECTION_ORDER:
        if key not in grouped:
            continue
        sec_rows = grouped[key]
        title = SECTION_NAMES.get(key, key)
        trs = []
        for r in sec_rows:
            desc = escape(r.get('desc', ''))
            badge = OUTCOME_BADGE.get(r.get('outcome', ''), '<span class="b-none">—</span>')
            try:
                sv = float(r.get('stock', ''))
                stock_cls = ' class="stock stock-zero"' if sv <= 0 else ' class="stock"'
                stock_txt = str(int(sv)) if sv == int(sv) else str(sv)
            except (ValueError, TypeError):
                stock_cls = ' class="stock"'
                stock_txt = str(r.get('stock', '')) or '—'
            trs.append(
                f'<tr>'
                f'<td class="desc">{desc}</td>'
                f'<td>{badge}</td>'
                f'<td{stock_cls}>{stock_txt}</td>'
                f'<td class="corr-cell"><input type="number" class="ci" inputmode="numeric" onchange="onCorr(this)" placeholder="—"></td>'
                f'</tr>'
            )
        sections_html.append(
            f'<div class="section"><div class="section-title">{escape(title)}'
            f' <span class="sec-n">({len(sec_rows)})</span></div>'
            f'<table><thead><tr><th>Prodotto</th><th>Calib.</th><th>Giac.</th><th>Corr.</th></tr></thead>'
            f'<tbody>{"".join(trs)}</tbody></table></div>'
        )

    total = sum(len(v) for v in grouped.values())
    css = (
        '*{box-sizing:border-box;margin:0;padding:0}'
        'body{font-family:system-ui,sans-serif;font-size:14px;background:#f0f0f0;padding-bottom:80px}'
        'header{background:#1e293b;color:#fff;padding:12px 14px 10px}'
        'header h1{font-size:1rem;font-weight:700}'
        'header p{font-size:.72rem;color:#94a3b8;margin-top:2px}'
        '.total{font-size:.72rem;color:#64748b;padding:5px 12px 0}'
        '.section{margin:8px 10px 0}'
        '.section-title{font-size:.68rem;font-weight:700;color:#64748b;text-transform:uppercase;'
        'letter-spacing:.05em;padding:8px 2px 3px}'
        '.sec-n{font-weight:400}'
        'table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;'
        'box-shadow:0 1px 2px rgba(0,0,0,.06)}'
        'th{font-size:.65rem;color:#9ca3af;font-weight:600;padding:6px 8px;text-align:left;'
        'border-bottom:1px solid #f3f4f6}'
        'th:nth-child(2),th:nth-child(3),th:nth-child(4){text-align:right}'
        'td{padding:7px 8px;border-bottom:1px solid #f9fafb;vertical-align:middle}'
        'td:nth-child(2),td:nth-child(3){text-align:right;white-space:nowrap}'
        'tr:last-child td{border-bottom:none}'
        'tr.edited{background:#fffbeb}'
        '.desc{font-size:.82rem;line-height:1.3}'
        '.badge{display:inline-block;font-size:.6rem;font-weight:700;padding:2px 5px;border-radius:3px}'
        '.b-crit{background:#dc2626;color:#fff}'
        '.b-under{background:#f59e0b;color:#000}'
        '.b-over{background:#d97706;color:#000}'
        '.b-ok{background:#16a34a;color:#fff}'
        '.b-none{color:#9ca3af;font-size:.75rem}'
        '.stock{font-weight:600;font-size:.88rem}'
        '.stock-zero{color:#dc2626}'
        '.corr-cell{text-align:right;padding:4px 6px}'
        '.ci{width:54px;border:1px solid #e5e7eb;border-radius:6px;padding:4px 6px;'
        'font-size:.85rem;text-align:center;background:#f9fafb;-moz-appearance:textfield}'
        '.ci::-webkit-inner-spin-button,.ci::-webkit-outer-spin-button{-webkit-appearance:none}'
        '.ci:focus{outline:none;border-color:#6366f1;background:#fff}'
        '.ci.has-val{background:#fef3c7;border-color:#f59e0b;font-weight:700;color:#92400e}'
        '.fab{position:fixed;bottom:16px;right:16px;z-index:99;border:none;border-radius:24px;'
        'padding:12px 20px;font-size:.85rem;font-weight:700;cursor:pointer;'
        'box-shadow:0 3px 10px rgba(0,0,0,.25);transition:background .15s}'
        '.fab-off{background:#6b7280;color:#fff}'
        '.fab-on{background:#f59e0b;color:#000}'
    )
    js = (
        'function onCorr(inp){'
        '  var tr=inp.closest("tr");'
        '  var filled=inp.value!==""&&inp.value!==null;'
        '  inp.classList.toggle("has-val",filled);'
        '  tr.classList.toggle("edited",filled);'
        '  if(document.getElementById("fab").dataset.on==="1")applyFilter();'
        '  updateFab();'
        '}'
        'function updateFab(){'
        '  var n=document.querySelectorAll("tr.edited").length;'
        '  var btn=document.getElementById("fab");'
        '  btn.textContent=btn.dataset.on==="1"'
        '    ?"Mostra tutti":"Solo modificati"+(n>0?" ("+n+")":"");'
        '}'
        'function toggleFilter(){'
        '  var btn=document.getElementById("fab");'
        '  var on=btn.dataset.on!=="1";'
        '  btn.dataset.on=on?"1":"0";'
        '  btn.className="fab "+(on?"fab-on":"fab-off");'
        '  applyFilter();updateFab();'
        '}'
        'function applyFilter(){'
        '  var on=document.getElementById("fab").dataset.on==="1";'
        '  document.querySelectorAll("tbody tr").forEach(function(tr){'
        '    tr.style.display=(!on||tr.classList.contains("edited"))?"":"none";'
        '  });'
        '  document.querySelectorAll(".section").forEach(function(sec){'
        '    var visible=sec.querySelectorAll("tbody tr:not([style*=none])").length;'
        '    sec.style.display=visible>0?"":"none";'
        '  });'
        '}'
    )
    name_esc = escape(storage.name)
    sm_esc   = escape(storage.supermarket.name)
    return (
        f'<!DOCTYPE html><html lang="it"><head>'
        f'<meta charset="utf-8">'
        f'<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<title>{name_esc}</title>'
        f'<style>{css}</style></head><body>'
        f'<header><h1>{name_esc}</h1><p>{sm_esc} — {date_str}</p></header>'
        f'<div class="total">{total} prodotti visibili</div>'
        f'{"".join(sections_html)}'
        f'<button id="fab" class="fab fab-off" data-on="0" onclick="toggleFilter()">Solo modificati</button>'
        f'<script>{js}</script>'
        f'</body></html>'
    )


@login_required
@require_POST
def save_comparison_snapshot_view(request, storage_id):
    storage = get_object_or_404(Storage, pk=storage_id)
    try:
        payload = json.loads(request.body)
        rows = payload.get('rows', [])
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    now = timezone.localtime(timezone.now())
    date_str = now.strftime('%d/%m/%Y %H:%M')
    html = _build_snapshot_html(storage, rows, date_str)

    snapshots_dir = Path(settings.BASE_DIR) / 'snapshots'
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    (snapshots_dir / f'storage_{storage_id}.html').write_text(html, encoding='utf-8')

    url = request.build_absolute_uri(
        reverse('serve-comparison-snapshot', kwargs={'storage_id': storage_id})
    )
    return JsonResponse({'url': url, 'count': len(rows)})


@login_required
def serve_comparison_snapshot_view(request, storage_id):
    from django.http import Http404, HttpResponse
    storage = get_object_or_404(Storage, pk=storage_id)
    if storage.supermarket.owner != request.user:
        from django.core.exceptions import PermissionDenied
        raise PermissionDenied
    snapshot_path = Path(settings.BASE_DIR) / 'snapshots' / f'storage_{storage_id}.html'
    if not snapshot_path.exists():
        raise Http404
    return HttpResponse(snapshot_path.read_text(encoding='utf-8'), content_type='text/html; charset=utf-8')
