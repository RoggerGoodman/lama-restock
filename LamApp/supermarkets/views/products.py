"""Adding, purging and linking products."""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
import logging

from ..models import Supermarket, Storage, RestockLog, ProductLinkNotification
from ..forms import PurgeProductsForm, AddProductsForm
from ..services import RestockService

logger = logging.getLogger(__name__)


@login_required
def add_products_view(request, storage_id):
    """
    UPDATED: Now uses unified task with gather_missing_product_data.
    Replaces old Scrapper-based approach for consistency.
    """
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    if request.method == 'POST':
        form = AddProductsForm(storage, request.POST)
        
        if form.is_valid():
            products_list = form.cleaned_data['products']
            settore = form.cleaned_data['settore']
            
            # ✅ DISPATCH TO UNIFIED TASK (uses gather_missing_product_data)
            from ..tasks import add_products_unified_task
            
            result = add_products_unified_task.apply_async(
                args=[storage_id, products_list, settore],
                retry=True
            )
            
            est_time = len(products_list) * 20  # 20 seconds per product
            messages.info(
                request,
                f"Adding {len(products_list)} products using auto-fetch. "
                f"Estimated time: {est_time // 60} minutes. "
                f"Track progress on the next page."
            )
            
            return redirect('task-progress', task_id=result.id, storage_id=storage_id)
    else:
        form = AddProductsForm(storage)
    
    return render(request, 'storages/add_products.html', {
        'storage': storage,
        'form': form
    })


@login_required
def purge_products_view(request, storage_id):
    """View to flag/purge products"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )    
    if request.method == 'POST':
        form = PurgeProductsForm(request.POST)    
        if form.is_valid():
            products_list = form.cleaned_data['products']           
            try:
                with RestockService(storage) as service:               
                    flagged = []
                    purged = []
                    errors = []                   
                    for cod, var in products_list:
                        try:
                            result = service.db.flag_for_purge(cod, var)
                            
                            if result['action'] == 'flagged':
                                flagged.append(result)
                            elif result['action'] == 'purged':
                                purged.append(result)
                        
                        except ValueError as e:
                            errors.append(f"Product {cod}.{var}: {str(e)}")
                        except Exception as e:
                            logger.exception(f"Error processing {cod}.{var}")
                            errors.append(f"Product {cod}.{var}: {str(e)}")                    
                    # Show results
                    if purged:
                        messages.success(
                            request,
                            f"Immediately purged {len(purged)} products with zero stock"
                        )
                    if flagged:
                        messages.warning(
                            request,
                            f"Flagged {len(flagged)} products for purging (they have stock > 0). "
                            f"They will be automatically purged when stock reaches zero."
                        )
                    if errors:
                        for error in errors[:5]:
                            messages.error(request, error)
                    return redirect('storage-detail', pk=storage_id)               
            except Exception as e:
                logger.exception("Error in purge operation")
                messages.error(request, f"Errore: {str(e)}")
    else:        
        form = PurgeProductsForm()    
    # Get pending purges
    with RestockService(storage) as service:
        pending_purges = service.db.get_purge_pending()    
    return render(request, 'storages/purge_products.html', {
        'storage': storage,
        'form': form,
        'pending_purges': pending_purges
    })


@login_required
@require_POST
def check_purge_flagged_view(request, storage_id):
    """Check and purge all flagged products with zero stock"""
    storage = get_object_or_404(
        Storage,
        id=storage_id,
        supermarket__owner=request.user
    )
    
    try:
        with RestockService(storage) as service:
            purged = service.db.check_and_purge_flagged()        
        if purged:
            messages.success(
                request,
                f"Automatically purged {len(purged)} flagged products that reached zero stock"
            )
        else:
            messages.info(request, "Nessun prodotto segnalato pronto per l'eliminazione")
        
    except Exception as e:
        logger.exception("Error checking flagged products")
        messages.error(request, f"Errore: {str(e)}")
    
    return redirect('purge-products', storage_id=storage_id)


@login_required
@require_POST
def flag_products_for_purge_view(request, log_id):
    """Flag multiple skipped products for purging"""
    log = get_object_or_404(
        RestockLog,
        id=log_id,
        storage__supermarket__owner=request.user
    )
    
    try:
        data = json.loads(request.body)
        products = data.get('products', [])
        
        if not products:
            return JsonResponse({'success': False, 'message': 'No products provided'}, status=400)
        
        with RestockService(log.storage) as service:
            flagged_count = 0
            purged_count = 0
            errors = []
            
            for product in products:
                cod = product['cod']
                var = product['var']
                
                try:
                    result = service.db.flag_for_purge(cod, var)
                    
                    if result['action'] == 'flagged':
                        flagged_count += 1
                    elif result['action'] == 'purged':
                        purged_count += 1
                except Exception as e:
                    logger.warning(f"Error flagging {cod}.{var}: {e}")
                    errors.append(f"{cod}.{var}: {str(e)}")
        return JsonResponse({
            'success': True,
            'flagged': flagged_count,
            'purged': purged_count,
            'errors': errors
        })
        
    except Exception as e:
        logger.exception("Error flagging products")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)


@login_required
@require_POST
def dismiss_product_link_notification(request, pk):
    """Mark a single product link notification as read."""
    notif = get_object_or_404(
        ProductLinkNotification, pk=pk, supermarket__owner=request.user
    )
    notif.is_read = True
    notif.save(update_fields=['is_read'])

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True})

    return redirect('dashboard')


@login_required
@require_POST
def dismiss_all_product_link_notifications(request):
    """Mark all product link notifications as read for the current user."""
    updated = ProductLinkNotification.objects.filter(
        supermarket__owner=request.user,
        is_read=False,
    ).update(is_read=True)

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True, 'dismissed': updated})

    return redirect('dashboard')


@login_required
def product_links_view(request):
    """
    Per-supermarket product link management.
    Allows creating and deleting links between a primary product (the one to keep
    ordering) and a secondary product (being phased out). A 'propagate to all'
    option creates the same link for every supermarket owned by the user.
    """
    from ..models import ChainLinkOptOut, ChainProductLink, ProductLink

    user_supermarkets = list(Supermarket.objects.filter(owner=request.user).order_by('name'))
    if not user_supermarkets:
        messages.error(request, "Nessun punto vendita associato al tuo account.")
        return redirect('inventory-search')

    # Supermarket selector (via GET param, default to first)
    selected_id = request.GET.get('supermarket_id') or request.POST.get('supermarket_id')
    try:
        selected_id = int(selected_id)
        selected_sm = next(sm for sm in user_supermarkets if sm.id == selected_id)
    except (TypeError, ValueError, StopIteration):
        selected_sm = user_supermarkets[0]
        selected_id = selected_sm.id

    error = None
    success = None

    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'create':
            try:
                primary_cod = int(request.POST['primary_cod'])
                primary_v = int(request.POST.get('primary_v') or 0)
                secondary_cod = int(request.POST['secondary_cod'])
                secondary_v = int(request.POST.get('secondary_v') or 0)
                notes = request.POST.get('notes', '').strip()
                propagate = request.POST.get('propagate') == '1'
                purge_on_removal = request.POST.get('purge_on_removal') == '1'

                if primary_cod == secondary_cod and primary_v == secondary_v:
                    error = "Il prodotto subentrante e il prodotto sostituito non possono essere lo stesso articolo."
                else:
                    targets = user_supermarkets if propagate else [selected_sm]
                    created, skipped = 0, 0
                    for sm in targets:
                        _, was_created = ProductLink.objects.get_or_create(
                            supermarket=sm,
                            primary_cod=primary_cod,
                            primary_v=primary_v,
                            defaults={
                                'secondary_cod': secondary_cod,
                                'secondary_v': secondary_v,
                                'notes': notes,
                                'purge_on_removal': purge_on_removal,
                                'created_by': request.user,
                            }
                        )
                        if was_created:
                            created += 1
                            # Re-created by hand: lift a previous opt-out from the chain link
                            ChainLinkOptOut.objects.filter(
                                supermarket=sm,
                                chain_link__primary_cod=primary_cod, chain_link__primary_v=primary_v,
                                chain_link__secondary_cod=secondary_cod, chain_link__secondary_v=secondary_v,
                            ).delete()
                            ProductLinkNotification.objects.create(
                                supermarket=sm,
                                primary_cod=primary_cod,
                                primary_v=primary_v,
                                secondary_cod=secondary_cod,
                                secondary_v=secondary_v,
                                created_by=request.user,
                            )
                        else:
                            skipped += 1

                    if propagate:
                        success = f"Collegamento propagato: {created} punto/i vendita aggiornato/i."
                        if skipped:
                            success += f" {skipped} già esistente/i (saltati)."
                    else:
                        if created:
                            success = f"Collegamento creato: {primary_cod}.{primary_v} ← {secondary_cod}.{secondary_v}"
                        else:
                            error = "Uno dei prodotti selezionati fa già parte di un collegamento per questo punto vendita."
            except ValueError:
                error = "Codici prodotto non validi. Inserire numeri interi."
            except Exception as e:
                error = f"Errore durante la creazione: {e}"

        elif action == 'delete':
            try:
                link_id = int(request.POST['link_id'])
                link = ProductLink.objects.filter(id=link_id, supermarket__owner=request.user).first()
                if link:
                    # Deleting a chain link by hand opts this store out of it for good
                    chain_link = ChainProductLink.objects.filter(
                        primary_cod=link.primary_cod, primary_v=link.primary_v,
                        secondary_cod=link.secondary_cod, secondary_v=link.secondary_v,
                    ).first()
                    if chain_link:
                        ChainLinkOptOut.objects.get_or_create(supermarket=link.supermarket, chain_link=chain_link)
                    link.delete()
                success = "Collegamento eliminato."
            except Exception as e:
                error = f"Errore durante l'eliminazione: {e}"

        elif action == 'toggle_purge':
            link = ProductLink.objects.filter(
                id=request.POST.get('link_id'), supermarket__owner=request.user
            ).first()
            if link:
                link.purge_on_removal = request.POST.get('purge_on_removal') == '1'
                link.save(update_fields=['purge_on_removal'])

        elif action == 'invert':
            try:
                link_id = int(request.POST['link_id'])
                link = ProductLink.objects.get(id=link_id, supermarket__owner=request.user)
                link.primary_cod, link.secondary_cod = link.secondary_cod, link.primary_cod
                link.primary_v, link.secondary_v = link.secondary_v, link.primary_v
                link.save()
                success = f"Ruoli invertiti: {link.primary_cod}.{link.primary_v} e' ora il subentrante."
            except ProductLink.DoesNotExist:
                error = "Collegamento non trovato."
            except Exception as e:
                error = f"Errore durante l'inversione: {e}"

        return redirect(f"{request.path}?supermarket_id={selected_id}")

    links = list(
        ProductLink.objects
        .filter(supermarket=selected_sm)
        .select_related('created_by')
        .order_by('-created_at')
    )

    # Resolve cod.v to product descriptions
    name_map = {}
    if links:
        keys = set()
        for link in links:
            keys.add((link.primary_cod, link.primary_v))
            keys.add((link.secondary_cod, link.secondary_v))
        from ..scripts.DatabaseManager import DatabaseManager
        try:
            db = DatabaseManager(supermarket_name=selected_sm.name)
            try:
                cur = db.cursor()
                cur.execute(
                    "SELECT cod, v, descrizione FROM products WHERE (cod, v) IN %s",
                    (tuple(keys),)
                )
                for r in cur.fetchall():
                    name_map[(r['cod'], r['v'])] = r['descrizione']
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"Could not load product names for {selected_sm.name}: {e}")
    for link in links:
        link.primary_name = name_map.get((link.primary_cod, link.primary_v))
        link.secondary_name = name_map.get((link.secondary_cod, link.secondary_v))

    return render(request, 'inventory/product_links.html', {
        'links': links,
        'supermarkets': user_supermarkets,
        'selected_sm': selected_sm,
        'error': error,
        'success': success,
    })
