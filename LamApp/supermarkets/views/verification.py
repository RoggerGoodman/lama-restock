"""Stock verification and pending verifications."""

from django.utils import timezone
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from pathlib import Path
from django.conf import settings
import logging

from ..models import Supermarket, Storage, BlacklistEntry
from ..services import RestockService

logger = logging.getLogger(__name__)


@login_required
@require_POST
def auto_add_product_view(request):
    """
    FIXED: Now returns task_id for async tracking instead of blocking.
    Frontend can track progress properly.
    """
    try:
        data = json.loads(request.body)
        cod = int(data['cod'])
        var = int(data['var'])
        supermarket_id = int(data['supermarket_id'])
        storage_id = int(data['storage_id'])

        products_list = [(cod, var)]
        
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = get_object_or_404(Storage, id=storage_id, supermarket=supermarket)
        
        logger.info(f"Auto-adding product {cod}.{var} to {storage.name}")
        
        # ✅ FIX: Dispatch async and return task_id (don't block with .get())
        from ..tasks import add_products_unified_task
        
        result = add_products_unified_task.apply_async(
            args=[storage_id, products_list, storage.settore],
            retry=True
        )
        
        # Return task_id for frontend to track
        return JsonResponse({
            'success': True,
            'task_id': result.id,
            'message': f'Auto-adding product {cod}.{var}...'
        })
            
    except Exception as e:
        logger.exception("Error in auto_add_product_view")
        return JsonResponse({
            'success': False,
            'message': f'Error: {str(e)}'
        }, status=500)


@login_required
def verify_stock_unified_enhanced_view(request):
    """
    FIXED: Now properly dispatches Celery task for auto-add verification.
    Handles PDF files and automatically adds missing products.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    if request.method == 'POST':
        supermarket_id = request.POST.get('supermarket_id')
        storage_id = request.POST.get('storage_id')
        cluster = request.POST.get('cluster', '').strip().upper()
        
        if not supermarket_id or not storage_id:
            messages.error(request, "Seleziona sia il punto vendita che il magazzino")
            return redirect('verify-stock-unified-enhanced')
        
        storage = get_object_or_404(
            Storage,
            id=storage_id,
            supermarket_id=supermarket_id,
            supermarket__owner=request.user
        )
        
        if 'pdf_file' not in request.FILES:
            messages.error(request, "Nessun file caricato")
            return redirect('verify-stock-unified-enhanced')
        
        pdf_file = request.FILES['pdf_file']
        
        if not pdf_file.name.endswith('.pdf'):
            messages.error(request, "Il file deve essere in formato .pdf")
            return redirect('verify-stock-unified-enhanced')
        
        try:
            # Save file to INVENTORY_FOLDER
            inventory_folder = Path(settings.INVENTORY_FOLDER)
            inventory_folder.mkdir(exist_ok=True)
            
            timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            file_path = inventory_folder / f"verify_auto_{timestamp}_{pdf_file.name}"
            
            with open(file_path, 'wb+') as destination:
                for chunk in pdf_file.chunks():
                    destination.write(chunk)
            
            # ✅ DISPATCH TO NEW CELERY TASK WITH AUTO-ADD
            from ..tasks import verify_stock_with_auto_add_task
            
            result = verify_stock_with_auto_add_task.apply_async(
                args=[storage_id, str(file_path), cluster or None],
                retry=True
            )
            
            cluster_msg = f" (Cluster: {cluster})" if cluster else ""
            messages.info(
                request,
                f"Stock verification with auto-add started for {storage.name}{cluster_msg}. "
                f"Missing products will be automatically fetched and added. "
                f"This may take 10-20 minutes."
            )
            
            return redirect('task-progress', task_id=result.id, storage_id=storage_id)
            
        except Exception as e:
            logger.exception("Error starting verification")
            messages.error(request, f"Errore: {str(e)}")
            return redirect('verify-stock-unified-enhanced')
    
    # GET request - load existing clusters for dropdown
    clusters_by_storage = {}
    for sm in supermarkets:
        for storage in sm.storages.all():
            with RestockService(storage) as service:
                cursor = service.db.cursor()
                cursor.execute("""
                    SELECT DISTINCT cluster 
                    FROM products 
                    WHERE cluster IS NOT NULL AND cluster != '' 
                        AND settore = %s 
                    ORDER BY cluster ASC
                """, (storage.settore,))
                clusters = [row['cluster'] for row in cursor.fetchall()]
                clusters_by_storage[storage.id] = clusters
    return render(request, 'inventory/verify_stock_unified.html', {
        'supermarkets': supermarkets,
        'clusters_by_storage': json.dumps(clusters_by_storage)
    })


@login_required
def verification_report_unified_view(request):
    """
    UPDATED: Now properly displays report from Celery task result.
    Shows comprehensive report with verified, added, and failed products.
    """
    from celery.result import AsyncResult
    from LamApp.celery import app as celery_app
    
    # Try to get from Celery task result first
    task_id = request.GET.get('task_id')
    
    report = None
    
    if task_id:
        task = AsyncResult(task_id, app=celery_app)
        
        if task.ready() and task.successful():
            result = task.result
            
            # ✅ Convert task result to report format
            report = {
                'total_products': result.get('total_products', 0),
                'products_verified': result.get('existing_verified', 0),
                'products_added': result.get('products_added', 0),
                'stock_changes': result.get('stock_changes', []),
                'added_products': result.get('added_products', []),
                'failed_additions': result.get('failed_additions', []),
                'cluster': result.get('cluster'),
                'storage_name': result.get('storage_name'),
            }
        else:
            # Task not ready or failed
            messages.warning(request, "Verifica ancora in corso o non riuscita")
            return redirect('inventory-search')
    else:
        # Fallback to session (for backward compatibility)
        report = request.session.get('verification_report')
    
    if not report:
        messages.warning(request, "Nessun report di verifica disponibile")
        return redirect('inventory-search')
    
    # Calculate statistics
    total_difference = 0
    total_stock_after = 0
    
    if report.get('stock_changes'):
        total_difference = sum(
            change.get('difference', 0) 
            for change in report['stock_changes']
        )
        total_stock_after = sum(
            change.get('new_stock', 0)
            for change in report['stock_changes']
        )
    
    # Add auto-added products to totals
    if report.get('added_products'):
        total_difference += sum(
            product.get('qty', 0) 
            for product in report['added_products']
        )
        total_stock_after += sum(
            product.get('qty', 0)
            for product in report['added_products']
        )
    
    report['total_difference'] = total_difference
    report['total_stock_after'] = total_stock_after
    
    return render(request, 'inventory/verification_report_unified.html', {
        'report': report
    })


@login_required
@require_POST
def verify_product_ajax_view(request):
    """
    Unified AJAX endpoint for verifying a single product.

    Accepts either:
      - storage_id (direct storage reference)
      - supermarket_id + settore (lookup storage by these)

    Optional parameters:
      - stock: New stock value (required in most cases)
      - cluster: Cluster assignment (optional, 'NONE' to clear)
      - package_size: Package size update (optional)
    """
    try:
        data = json.loads(request.body)

        cod = int(data['cod'])
        var = int(data['var'])
        stock = data.get('stock')  # Can be None for just marking verified
        cluster = data.get('cluster', '').strip().upper() if data.get('cluster') else None
        package_size = data.get('package_size')

        # Resolve storage: either by storage_id or by supermarket_id + settore
        storage_id = data.get('storage_id')
        supermarket_id = data.get('supermarket_id')
        settore = data.get('settore')

        if storage_id:
            storage = get_object_or_404(
                Storage,
                id=storage_id,
                supermarket__owner=request.user
            )
        elif supermarket_id and settore:
            storage = get_object_or_404(
                Storage,
                supermarket_id=supermarket_id,
                settore=settore,
                supermarket__owner=request.user
            )
        else:
            return JsonResponse({
                'success': False,
                'message': 'Must provide either storage_id or supermarket_id + settore'
            }, status=400)

        with RestockService(storage) as service:
            # Update package size if provided
            if package_size is not None:
                cursor = service.db.cursor()
                cursor.execute("""
                    UPDATE products
                    SET pz_x_collo = %s
                    WHERE cod = %s AND v = %s
                """, (int(package_size), cod, var))
                service.db.conn.commit()
                logger.info(f"Updated package size for {cod}.{var} to {package_size}")

            # Handle cluster update (including clearing with 'NONE')
            cluster_to_set = None
            if cluster:
                if cluster == 'NONE':
                    # Clear cluster
                    cursor = service.db.cursor()
                    cursor.execute("""
                        UPDATE products
                        SET cluster = NULL
                        WHERE cod = %s AND v = %s
                    """, (cod, var))
                    service.db.conn.commit()
                else:
                    cluster_to_set = cluster

            # Verify stock (this also marks as verified)
            try:
                current_stock = service.db.get_stock(cod, var)
            except ValueError:
                current_stock = None  # New product, no product_stats row yet
            if stock is not None:
                service.db.verify_stock(cod, var, int(stock), cluster_to_set)
            else:
                service.db.verify_stock(cod, var, new_stock=None, cluster=cluster_to_set)

            message = f'Product {cod}.{var} verified successfully!'
            if package_size:
                message += f' Package size updated to {package_size}.'

            logger.info(
                f"[VERIFY] {storage.supermarket.name} - {storage.settore} - "
                f"Product {cod}.{var}: {current_stock} → {stock if stock is not None else '(unchanged)'}"
                + (f" (package: {package_size})" if package_size else "")
                + (f" (cluster: {cluster})" if cluster else "")
            )

            return JsonResponse({
                'success': True,
                'message': message,
                'cluster_updated': bool(cluster)
            })
    except KeyError as e:
        return JsonResponse({
            'success': False,
            'message': f'Missing required field: {e}'
        }, status=400)
    except Exception as e:
        logger.exception("Error verifying product")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


@login_required
def pending_verifications_view(request):
    """
    Show all products that need verification across all supermarkets.
    These are products that have been ordered but not yet verified.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    all_pending = []
    
    # Group by supermarket to minimize DB connections
    for sm in supermarkets:
        if not sm.storages.exists():
            continue
        
        try:
            storage = sm.storages.first()
            # Map settore -> storage id so each row can target the right storage
            # (e.g. for the "Non gestiti" action).
            storage_by_settore = {s.settore: s.id for s in sm.storages.all()}
            blacklisted = set(
                BlacklistEntry.objects.filter(
                    blacklist__storage__supermarket=sm
                ).values_list('product_code', 'product_var')
            )

            with RestockService(storage) as service:
                cursor = service.db.cursor()

                # Get all settores for this supermarket
                settores = list(sm.storages.values_list('settore', flat=True).distinct())

                if not settores:
                    continue

                settore_placeholders = ','.join(['%s'] * len(settores))
                
                # ✅ FIXED: Add type check to prevent "non-array" error
                query = f"""
                    SELECT 
                        p.cod, p.v, p.descrizione, p.pz_x_collo, p.settore,
                        ps.stock, ps.bought_last_24,
                        ps.last_update_bought
                    FROM product_stats ps
                    JOIN products p ON ps.cod = p.cod AND ps.v = p.v
                    WHERE ps.verified = FALSE
                    AND p.purge_flag = FALSE
                    AND ps.stock <> 0
                    AND ps.bought_last_24 IS NOT NULL
                    AND jsonb_typeof(ps.bought_last_24) = 'array'
                    AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(ps.bought_last_24) WITH ORDINALITY AS elem(val, idx)
                        WHERE idx <= 4
                        AND jsonb_typeof(elem.val) = 'number'
                        AND (elem.val)::text::numeric <> 0
                    )
                    AND p.settore IN ({settore_placeholders})
                    ORDER BY ps.last_update_bought DESC
                """
                
                cursor.execute(query, settores)
                
                for row in cursor.fetchall():
                    if (row['cod'], row['v']) in blacklisted:
                        continue
                    bought = row['bought_last_24'] or []
                    sold = []  # They haven't sold any yet
                    
                    # Only include if bought but not sold
                    if bought and (not sold or all(s == 0 for s in sold)):
                        all_pending.append({
                            'supermarket_name': sm.name,
                            'supermarket_id': sm.id,
                            'storage_id': storage_by_settore.get(row['settore']),
                            'settore': row['settore'],
                            'cod': row['cod'],
                            'var': row['v'],
                            'name': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                            'package_size': row['pz_x_collo'] or 12,
                            'stock': row['stock'] or 0,
                            'last_update_bought': row['last_update_bought']
                        })
        except Exception as e:
            logger.exception(f"Error loading pending verifications for {sm.name}")
            continue
    
    # Sort by last_update_bought (most recent first)
    all_pending.sort(key=lambda x: x['last_update_bought'] if x['last_update_bought'] else timezone.now(), reverse=True)
    
    context = {
        'pending_products': all_pending,
        'total_pending': len(all_pending)
    }
    
    return render(request, 'inventory/pending_verifications.html', context)
