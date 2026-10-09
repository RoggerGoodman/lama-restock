"""Recording and editing losses."""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from pathlib import Path
from django.conf import settings
from psycopg2.extras import Json
import logging

from ..models import Supermarket, Storage, RestockLog
from ..forms import RecordLossesForm
from ..services import RestockService

logger = logging.getLogger(__name__)


@login_required
def record_losses_unified_view(request):
    supermarkets = Supermarket.objects.filter(owner=request.user)

    if request.method == 'POST':
        form = RecordLossesForm(request.POST, request.FILES)
        supermarket_id = request.POST.get('supermarket_id')

        if not supermarket_id:
            messages.error(request, "Seleziona un punto vendita")
            return redirect('record-losses-unified')

        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)

        if form.is_valid():
            loss_type = form.cleaned_data['loss_type']
            csv_file = request.FILES['csv_file']

            try:
                import tempfile
                losses_folder = Path(settings.LOSSES_FOLDER)
                losses_folder.mkdir(exist_ok=True)

                with tempfile.NamedTemporaryFile(
                    dir=str(losses_folder), suffix='.csv', delete=False, mode='wb'
                ) as tmp:
                    for chunk in csv_file.chunks():
                        tmp.write(chunk)
                    tmp_path = tmp.name

                logger.info(f"Saved uploaded loss CSV: {tmp_path}")

                storage = supermarket.storages.first()
                if not storage:
                    messages.error(request, f"Nessun reparto trovato per {supermarket.name}")
                    Path(tmp_path).unlink(missing_ok=True)
                    return redirect('record-losses-unified')

                with RestockService(storage) as service:
                    from ..scripts.inventory_reader import process_loss_csv_dropzone
                    result = process_loss_csv_dropzone(service.db, tmp_path, loss_type)

                    if result['success']:
                        messages.success(
                            request,
                            f"Elaborato {loss_type}: {result['processed']} perdite registrate, "
                            f"{result['total_losses']} unita totali"
                        )
                        if result['absent'] > 0:
                            messages.info(request, f"{result['absent']} EAN non trovati in database (saltati)")
                        if result['errors'] > 0:
                            messages.warning(request, f"{result['errors']} errori durante elaborazione")
                    else:
                        messages.error(request, f"Errore: {result.get('error', 'Errore sconosciuto')}")

                Path(tmp_path).unlink(missing_ok=True)
                return redirect('record-losses-unified')

            except Exception as e:
                logger.exception("Error processing uploaded loss CSV")
                messages.error(request, f"Errore: {str(e)}")
    else:
        form = RecordLossesForm()
    return render(request, 'inventory/record_losses_unified.html', {
        'supermarkets': supermarkets,
        'form': form
    })


@login_required
def edit_losses_view(request):
    """
    UPDATED: View to edit recorded losses WITHOUT affecting stock.
    Shows both quantity and cost snapshot for each month.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    # Get filters
    supermarket_id = request.GET.get('supermarket_id')
    if not supermarket_id and supermarkets.count() == 1:
        supermarket_id = str(supermarkets.first().id)
    storage_id = request.GET.get('storage_id')
    
    # Build scope
    if supermarket_id:
        supermarkets_filter = Supermarket.objects.filter(id=supermarket_id, owner=request.user)
    else:
        supermarkets_filter = supermarkets
    
    # Get storages
    if storage_id:
        storages = Storage.objects.filter(id=storage_id)
    else:
        storages = Storage.objects.filter(supermarket__in=supermarkets_filter)
    
    # Group by supermarket
    supermarkets_to_process = {}
    for storage in storages:
        if storage.supermarket.id not in supermarkets_to_process:
            supermarkets_to_process[storage.supermarket.id] = {
                'supermarket': storage.supermarket,
                'storages': [],
                'settores': set()
            }
        supermarkets_to_process[storage.supermarket.id]['storages'].append(storage)
        supermarkets_to_process[storage.supermarket.id]['settores'].add(storage.settore)
    
    products_with_losses = []
    
    # Process each supermarket's database
    for sm_id, sm_data in supermarkets_to_process.items():
        try:
            first_storage = sm_data['storages'][0]
            with RestockService(first_storage) as service:
                cursor = service.db.cursor()
                
                # Build WHERE clause
                if storage_id:
                    settore_filter = f"WHERE p.settore = '{first_storage.settore}'"
                elif len(sm_data['settores']) < len(sm_data['supermarket'].storages.all()):
                    settores_list = "', '".join(sm_data['settores'])
                    settore_filter = f"WHERE p.settore IN ('{settores_list}')"
                else:
                    settore_filter = ""
                
                # Also get current cost for reference
                query = f"""
                    SELECT 
                        el.cod, el.v,
                        el.broken, el.broken_updated,
                        el.expired, el.expired_updated,
                        el.internal, el.internal_updated,
                        el.shrinkage, el.shrinkage_updated,
                        p.descrizione,
                        p.settore,
                        p.rapp,
                        e.cost_std as current_cost
                    FROM extra_losses el
                    LEFT JOIN products p ON el.cod = p.cod AND el.v = p.v
                    LEFT JOIN economics e ON el.cod = e.cod AND el.v = e.v
                    {settore_filter}
                    ORDER BY p.descrizione
                """
                
                cursor.execute(query)
                
                for row in cursor.fetchall():
                    cod = row['cod']
                    v = row['v']
                    description = row['descrizione'] or f"Product {cod}.{v}"
                    # cost_std is per collo; losses count pieces, so divide by rapp.
                    rapp = int(row['rapp'] or 1) or 1
                    current_cost = (row['current_cost'] or 0.0) / rapp

                    # Process arrays - convert to format with cost info
                    def process_loss_array(loss_json, fallback_cost):
                        """Convert array to list of {qty, cost, value} dicts"""
                        if not loss_json:
                            return []

                        result = []
                        for item in loss_json:
                            if isinstance(item, list) and len(item) == 2:
                                # New format: [qty, cost]
                                qty, cost = item
                                cost = (cost or 0.0) / rapp
                                result.append({
                                    'qty': qty,
                                    'cost': cost,
                                    'value': qty * cost
                                })
                            else:
                                # Old format: just qty
                                qty = item
                                result.append({
                                    'qty': qty,
                                    'cost': fallback_cost,
                                    'value': qty * fallback_cost
                                })
                        return result

                    broken_data = process_loss_array(row['broken'], current_cost)
                    expired_data = process_loss_array(row['expired'], current_cost)
                    internal_data = process_loss_array(row['internal'], current_cost)
                    shrinkage_data = process_loss_array(row['shrinkage'], current_cost)

                    product = {
                        'cod': cod,
                        'var': v,
                        'description': description,
                        'settore': row['settore'],
                        'supermarket_name': sm_data['supermarket'].name,
                        'supermarket_id': sm_data['supermarket'].id,
                        'current_cost': current_cost,
                        'broken': broken_data,
                        'broken_updated': row['broken_updated'],
                        'expired': expired_data,
                        'expired_updated': row['expired_updated'],
                        'internal': internal_data,
                        'internal_updated': row['internal_updated'],
                        'shrinkage': shrinkage_data,
                        'shrinkage_updated': row['shrinkage_updated'],
                    }

                    # Only include if has at least one loss recorded
                    if broken_data or expired_data or internal_data or shrinkage_data:
                        products_with_losses.append(product)
        except Exception as e:
            logger.exception(f"Error loading losses for supermarket {sm_id}")
            continue
    
    context = {
        'supermarkets': supermarkets,
        'storages': Storage.objects.filter(supermarket__owner=request.user),
        'products': products_with_losses,
        'selected_supermarket': supermarket_id or '',
        'selected_storage': storage_id or '',
    }
    
    return render(request, 'inventory/edit_losses.html', context)


@login_required
@require_POST
def edit_loss_ajax_view(request):
    """
    UPDATED: AJAX endpoint to edit a specific loss value.
    Now handles new format: [[qty, cost], [qty, cost], ...]
    Updates extra_losses table WITHOUT affecting stock.
    
    When editing, we preserve the original cost snapshot but allow changing quantity.
    """
    try:
        data = json.loads(request.body)
        
        supermarket_id = int(data['supermarket_id'])
        cod = int(data['cod'])
        var = int(data['var'])
        loss_type = data['loss_type']  # 'broken', 'expired', 'internal', or 'shrinkage'
        month_index = int(data['month_index'])  # 0 = most recent month
        new_value = int(data['new_value'])  # New quantity

        # Validate loss type
        if loss_type not in ['broken', 'expired', 'internal', 'shrinkage']:
            return JsonResponse({
                'success': False,
                'message': 'Invalid loss type'
            }, status=400)
        
        # Get supermarket and storage
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = supermarket.storages.first()
        
        if not storage:
            return JsonResponse({
                'success': False,
                'message': 'No storage found'
            }, status=400)
        
        with RestockService(storage) as service:
            cursor = service.db.cursor()
            
            # Get current array
            cursor.execute(f"""
                SELECT {loss_type}, {loss_type}_updated
                FROM extra_losses
                WHERE cod = %s AND v = %s
            """, (cod, var))
            
            row = cursor.fetchone()
            
            if not row:
                return JsonResponse({
                    'success': False,
                    'message': 'Product not found in extra_losses'
                }, status=404)
            
            current_array = row[loss_type] or []
            
            # Validate month index
            if month_index >= len(current_array):
                return JsonResponse({
                    'success': False,
                    'message': f'Month index {month_index} out of range (array length: {len(current_array)})'
                }, status=400)
            
            # Handle both old and new formats
            item = current_array[month_index]
            
            if isinstance(item, list) and len(item) == 2:
                # New format: [qty, cost]
                old_qty = item[0]
                stored_cost = item[1]
                current_array[month_index] = [new_value, stored_cost]  # Keep cost, update qty
            else:
                # Old format: just qty
                old_qty = item
                # Get current cost from economics as fallback
                cursor.execute("""
                    SELECT cost_std FROM economics WHERE cod = %s AND v = %s
                """, (cod, var))
                cost_row = cursor.fetchone()
                current_cost = float(cost_row['cost_std']) if cost_row and cost_row['cost_std'] else 0.0
                
                # Convert to new format with current cost as snapshot
                current_array[month_index] = [new_value, current_cost]
            
            # Calculate stock adjustment needed
            stock_delta = old_qty - new_value  # Positive = add back to stock, negative = remove more
            
            # Update database - ONLY the extra_losses table (no stock adjustment per requirement)
            cursor.execute(f"""
                UPDATE extra_losses
                SET {loss_type} = %s
                WHERE cod = %s AND v = %s
            """, (Json(current_array), cod, var))
            
            service.db.conn.commit()
            
            logger.info(
                f"Loss edited: {supermarket.name} - Product {cod}.{var} - "
                f"{loss_type}[{month_index}]: {old_qty} → {new_value} "
                f"(stock NOT adjusted, cost snapshot preserved)"
            )
            
            return JsonResponse({
                'success': True,
                'message': f'Updated {loss_type} month {month_index}: {old_qty} → {new_value}',
                'old_value': old_qty,
                'new_value': new_value,
                'stock_delta_not_applied': stock_delta
            })          
    except Exception as e:
        logger.exception("Error editing loss value")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


@login_required
def loss_log_fetch_ean_ajax(request, log_id):
    """
    Trigger fetch_product_from_ean for an absent EAN found in a loss_recording log.
    POST JSON: {"ean": "1234567890123"}
    Returns JSON: {"task_id": "..."}
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    log = get_object_or_404(RestockLog, id=log_id, storage__supermarket__owner=request.user)
    data = json.loads(request.body)
    ean = data['ean']
    qty = data.get('qty')
    loss_type = data.get('loss_type')

    from ..tasks import fetch_product_from_ean
    result = fetch_product_from_ean.apply_async(args=[log.storage.id, ean, qty, loss_type])
    return JsonResponse({'task_id': result.id})
