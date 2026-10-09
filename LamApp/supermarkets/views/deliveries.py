"""DDT upload and delivery check."""

from django.utils import timezone
from datetime import date, timedelta
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.core.cache import cache
from pathlib import Path
from django.conf import settings
import logging

from ..models import Storage, RestockLog
from ..forms import DDTUploadForm
from ..services import RestockService

logger = logging.getLogger(__name__)


def _ddt_already_loaded(storage, number):
    """
    Where this storage's DDT `number` already went, or None.
    {'kind': 'pending' | 'applied' | 'manual', 'when': date or datetime}
    """
    from ..scripts.DatabaseManager import DatabaseManager

    from ..document_import import LEDGER_RETENTION_DAYS

    since = date.today() - timedelta(days=LEDGER_RETENTION_DAYS)
    try:
        db = DatabaseManager(supermarket_name=storage.supermarket.name)
        try:
            row = db.find_ddt(storage.settore, number, since)
        finally:
            db.close()
    except Exception:
        logger.exception(f"DDT duplicate check: ledger unavailable for {storage.name}")
        row = None

    if row and row['status'] == 'pending':
        return {'kind': 'pending', 'when': row['delivery_date']}
    if row and row['status'] == 'manual':
        return {'kind': 'manual', 'when': row['recorded_at']}
    if row and row['status'] == 'applied':
        return {'kind': 'applied', 'when': row['applied_at']}

    # Before the ledger (and for 'legacy' rows) the logs are the record
    for log in RestockLog.objects.filter(
        storage=storage, operation_type='ddt_import', status='completed',
    ).order_by('-started_at')[:50]:
        stored = [str(inv).lstrip('0') or '0' for inv in log.get_results().get('invoices', [])]
        if number in stored:
            return {'kind': 'applied', 'when': log.started_at}
    return None


@login_required
def upload_ddt_view(request, storage_id):
    """
    Upload DDT (delivery document) to add received stock.
    """
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    duplicate_warning = None

    if request.method == 'POST':
        form = DDTUploadForm(request.POST, request.FILES)

        if form.is_valid():
            pdf_file = request.FILES['pdf_file']
            invoice_number = form.cleaned_data.get('invoice_number', '').strip()

            if invoice_number:
                duplicate_warning = _ddt_already_loaded(storage, invoice_number.lstrip('0') or '0')

            # If duplicate found and user hasn't confirmed, show warning
            if duplicate_warning and 'confirm_duplicate' not in request.POST:
                return render(request, 'storages/upload_ddt.html', {
                    'storage': storage,
                    'form': form,
                    'duplicate_warning': duplicate_warning,
                    'duplicate_invoice': invoice_number,
                })

            try:
                # Save file temporarily
                temp_dir = Path(settings.BASE_DIR) / 'temp_ddt'
                temp_dir.mkdir(exist_ok=True)

                timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
                file_path = temp_dir / f"ddt_{timestamp}_{pdf_file.name}"

                with open(file_path, 'wb+') as destination:
                    for chunk in pdf_file.chunks():
                        destination.write(chunk)

                # ✅ DISPATCH TO CELERY
                from ..tasks import process_ddt_task

                result = process_ddt_task.apply_async(
                    args=[storage_id, str(file_path)],
                    kwargs={'invoice_number': invoice_number},
                    retry=True
                )

                messages.info(
                    request,
                    f"Processing DDT for {storage.name}. This may take a few minutes."
                )

                return redirect('task-progress', task_id=result.id, storage_id=storage_id)

            except Exception as e:
                logger.exception("Error saving DDT file")
                messages.error(request, f"Errore: {str(e)}")
                return redirect('upload-ddt', storage_id=storage_id)
    else:
        form = DDTUploadForm()

    return render(request, 'storages/upload_ddt.html', {
        'storage': storage,
        'form': form,
        'duplicate_warning': None,
        'duplicate_invoice': None,
    })


@login_required
def delivery_check_view(request, storage_id):
    """
    Delivery verification page. The user scans barcodes with the laser gun;
    the page builds a list of scanned products client-side.
    """
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    return render(request, 'storages/delivery_check.html', {'storage': storage})


@login_required
def delivery_check_lookup_ean_ajax(request, storage_id):
    """
    AJAX endpoint: receive an EAN barcode, return product info.
    POST JSON: {"ean": "1234567890123"}
    Returns JSON: {"found": true, "product": {...}} or {"found": false}
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )

    try:
        data = json.loads(request.body)
        ean_raw = data.get('ean', '').strip()
        if not ean_raw:
            return JsonResponse({'found': False, 'error': 'EAN vuoto'}, status=400)

        try:
            ean = int(ean_raw)
        except ValueError:
            return JsonResponse({'found': False, 'error': 'EAN non valido'}, status=400)

        with RestockService(storage) as service:
            row = service.db.get_product_by_ean(ean)

        if row is None:
            return JsonResponse({'found': False})

        return JsonResponse({
            'found': True,
            'product': {
                'cod': row['cod'],
                'var': row['v'],
                'descrizione': row['descrizione'],
                'pz_x_collo': row['pz_x_collo'],
                'settore': row['settore'],
            }
        })

    except Exception:
        logger.exception("Error in delivery_check_lookup_ean_ajax")
        return JsonResponse({'found': False, 'error': 'Errore interno'}, status=500)


@login_required
def delivery_check_parse_ddt_ajax(request, storage_id):
    """
    AJAX: upload DDT PDF, parse it, enrich with descriptions, return expected delivery list.
    POST multipart: pdf_file
    Returns JSON: {"entries": [{cod, var, descrizione, qty}, ...]}
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )

    pdf_file = request.FILES.get('pdf_file')
    if not pdf_file:
        return JsonResponse({'error': 'Nessun file caricato'}, status=400)

    temp_path = None
    try:
        temp_dir = Path(settings.BASE_DIR) / 'temp_ddt'
        temp_dir.mkdir(exist_ok=True)
        timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
        temp_path = temp_dir / f"ddt_check_{timestamp}_{pdf_file.name}"

        with open(temp_path, 'wb+') as f:
            for chunk in pdf_file.chunks():
                f.write(chunk)

        from ..scripts.ddt_parser import parse_ddt_pdf
        raw_entries = parse_ddt_pdf(str(temp_path))

        entries = []
        with RestockService(storage) as service:
            cur = service.db.cursor()
            for cod, var, qty in raw_entries:
                cur.execute(
                    "SELECT descrizione, ean FROM products WHERE cod=%s AND v=%s",
                    (cod, var)
                )
                row = cur.fetchone()
                descrizione = row['descrizione'] if row else f"{cod}.{var}"
                ean = row['ean'] if row else None
                entries.append({'cod': cod, 'var': var, 'descrizione': descrizione, 'qty': qty, 'ean': ean})

        return JsonResponse({'entries': entries})

    except Exception:
        logger.exception("Error parsing DDT for delivery check")
        return JsonResponse({'error': 'Errore nel parsing del DDT'}, status=500)

    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


@login_required
def delivery_check_fetch_ean_ajax(request, storage_id):
    """
    Trigger a Celery task to fetch and store the EAN for a single product.
    POST JSON: {"cod": 1234, "var": 1}
    Returns JSON: {"task_id": "..."}
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)

    data = json.loads(request.body)
    cod = int(data['cod'])
    var = int(data['var'])

    from ..tasks import fetch_single_ean
    result = fetch_single_ean.apply_async(args=[storage.id, cod, var])
    return JsonResponse({'task_id': result.id})


@login_required
def delivery_check_sync_scan_ajax(request, storage_id):
    """
    Server-side persistence for the delivery scan list (cross-device sync).
    GET  → return stored scan data for this storage
    POST → save scan data  (body: {scanList, notFoundList})
    DELETE → clear stored scan data
    Cache key: delivery_scan_{storage_id}  (24h TTL)
    """
    get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)
    cache_key = f'delivery_scan_{storage_id}'

    if request.method == 'GET':
        data = cache.get(cache_key) or {}
        return JsonResponse({'scanList': data.get('scanList', {}), 'notFoundList': data.get('notFoundList', [])})

    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            cache.set(cache_key, {'scanList': body.get('scanList', {}), 'notFoundList': body.get('notFoundList', [])}, timeout=86400)
            return JsonResponse({'ok': True})
        except Exception:
            return JsonResponse({'error': 'Dati non validi'}, status=400)

    if request.method == 'DELETE':
        cache.delete(cache_key)
        return JsonResponse({'ok': True})

    return JsonResponse({'error': 'Method not allowed'}, status=405)
