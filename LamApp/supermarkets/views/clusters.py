"""Clusters: assignment, minimum stock, order preview."""

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

from ..models import Supermarket, Storage, ClusterSetting, Blacklist, BlacklistEntry
from ..services import RestockService

logger = logging.getLogger(__name__)


@login_required
def cluster_order_preview_view(request):
    """Run the decision maker for specific clusters and render a printable order preview.
    No order is actually placed."""
    from ..scripts.decision_maker import DecisionMaker

    supermarket_id = request.GET.get('supermarket_id')
    settore = request.GET.get('settore')
    cluster_param = request.GET.get('clusters', '')
    coverage_str = request.GET.get('coverage', '')

    clusters = [c.strip() for c in cluster_param.split(',') if c.strip()]

    try:
        coverage = int(coverage_str)
        if coverage < 1:
            raise ValueError
    except (ValueError, TypeError):
        messages.error(request, "Copertura non valida — inserire un numero intero >= 1")
        return redirect('inventory-search')

    supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
    storage = supermarket.storages.filter(settore=settore).first()
    if not storage:
        messages.error(request, f"Magazzino non trovato per settore: {settore}")
        return redirect('inventory-search')

    orders = []
    try:
        with RestockService(storage) as service:
            from ..models import ProductLink
            blacklist = service.get_blacklist_set()
            dm = DecisionMaker(
                service.db, service.helper,
                blacklist_set=blacklist,
                product_links=ProductLink.build_pairs(supermarket),
            )
            today_date = timezone.now().date()
            lead_days = storage.schedule.calculate_lead_days(
                today_date.weekday(), reference_date=today_date
            ) if hasattr(storage, 'schedule') else 0.0
            dm.decide_orders_for_settore(
                settore, coverage, storage.minimum_stock, lead_days=lead_days,
                cluster_minimum_stock=storage.cluster_minimum_stocks(),
                day_weights=[supermarket.get_day_weight(d) for d in range(7)],
            )

            if dm.orders_list:
                cur = service.db.cursor()
                cur.execute("""
                    SELECT p.cod, p.v, p.descrizione, p.pz_x_collo, p.rapp, p.cluster, ps.stock
                    FROM products p
                    LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                    WHERE p.settore = %s
                """, (settore,))
                product_lookup = {(row['cod'], row['v']): dict(row) for row in cur.fetchall()}

                for (cod, var, qty_packages, discount) in dm.orders_list:
                    product = product_lookup.get((cod, var))
                    if not product:
                        continue
                    product_cluster = product.get('cluster') or ''
                    if clusters and product_cluster not in clusters:
                        continue
                    rapp = product.get('rapp') or 1
                    pz_x_collo = product.get('pz_x_collo') or 1
                    package_size = pz_x_collo * rapp
                    orders.append({
                        'cod': cod,
                        'var': var,
                        'descrizione': product.get('descrizione', ''),
                        'cluster': product_cluster,
                        'stock': product.get('stock', 0),
                        'pz_x_collo': pz_x_collo,
                        'rapp': rapp,
                        'package_size': package_size,
                        'qty_packages': qty_packages,
                        'qty_units': qty_packages * package_size,
                        'discount': discount,
                    })
    except Exception as e:
        logger.exception("Error in cluster order preview")
        messages.error(request, f"Errore nel calcolo dell'ordine: {e}")
        return redirect('inventory-search')

    # Sort by cluster then description for readability
    orders.sort(key=lambda o: (o['cluster'], o['descrizione']))

    total_packages = sum(o['qty_packages'] for o in orders)
    total_units = sum(o['qty_units'] for o in orders)

    context = {
        'supermarket': supermarket,
        'settore': settore,
        'clusters': clusters,
        'coverage': coverage,
        'orders': orders,
        'total_packages': total_packages,
        'total_units': total_units,
        'generated_at': timezone.now(),
    }
    return render(request, 'inventory/cluster_order_preview.html', context)


@login_required
def get_clusters_for_settore_view(request, supermarket_id, settore):
    """AJAX endpoint to get clusters for a settore"""
    try:
        supermarket = get_object_or_404(Supermarket, id=supermarket_id, owner=request.user)
        storage = supermarket.storages.filter(settore=settore).first()
        
        if not storage:
            logger.warning(f"No storage found for settore: {settore}")
            return JsonResponse({'clusters': []})
        
        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute("""
                SELECT DISTINCT cluster
                FROM products
                WHERE settore = %s 
                AND cluster IS NOT NULL 
                AND cluster != ''
                ORDER BY cluster
            """, (settore,))
            
            clusters = [row['cluster'] for row in cur.fetchall()]
            logger.info(f"API: Loaded {len(clusters)} clusters for settore {settore}")
            
            return JsonResponse({'clusters': clusters})
    
    except Exception as e:
        logger.exception("Error loading clusters")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_POST
def create_blacklist_from_cluster_view(request):
    """AJAX endpoint to create a blacklist containing all products from a given cluster."""
    try:
        data = json.loads(request.body)
        storage_id = data.get('storage_id')
        cluster_name = data.get('cluster')

        if not storage_id or not cluster_name:
            return JsonResponse({'error': 'Missing storage_id or cluster'}, status=400)

        storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)

        blacklist_name = f"Cluster {cluster_name} bloccato"

        # Fetch all cod.v from the cluster
        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute("""
                SELECT cod, v
                FROM products
                WHERE settore = %s AND cluster = %s
                ORDER BY cod, v
            """, (storage.settore, cluster_name))
            products = cur.fetchall()

        if not products:
            return JsonResponse({'error': f'Nessun prodotto trovato nel cluster {cluster_name}'}, status=400)

        blacklist, created = Blacklist.objects.get_or_create(
            storage=storage,
            name=blacklist_name,
            defaults={'description': f'Prodotti del cluster {cluster_name}'},
        )

        added = 0
        for row in products:
            _, entry_created = BlacklistEntry.objects.get_or_create(
                blacklist=blacklist,
                product_code=row['cod'],
                product_var=row['v'],
            )
            if entry_created:
                added += 1

        return JsonResponse({
            'success': True,
            'blacklist_id': blacklist.id,
            'blacklist_name': blacklist_name,
            'created': created,
            'added': added,
            'total': blacklist.entries.count(),
        })

    except Exception as e:
        logger.exception("Error creating blacklist from cluster")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
def assign_clusters_view(request):
    """
    UPDATED: Now handles PDF files instead of CSV.
    User provides cluster name, not derived from filename.
    """
    supermarkets = Supermarket.objects.filter(owner=request.user)
    
    if request.method == 'POST':
        supermarket_id = request.POST.get('supermarket_id')
        storage_id = request.POST.get('storage_id')
        cluster = request.POST.get('cluster', '').strip().upper()
        
        if not supermarket_id or not storage_id:
            messages.error(request, "Seleziona sia il punto vendita che il magazzino")
            return redirect('assign-clusters')
        
        if not cluster:
            messages.error(request, "Inserisci un nome per il cluster")
            return redirect('assign-clusters')
        
        storage = get_object_or_404(
            Storage,
            id=storage_id,
            supermarket_id=supermarket_id,
            supermarket__owner=request.user
        )
        
        if 'pdf_file' not in request.FILES:
            messages.error(request, "Nessun file caricato")
            return redirect('assign-clusters')
        
        pdf_file = request.FILES['pdf_file']
        
        if not pdf_file.name.endswith('.pdf'):
            messages.error(request, "Il file deve essere in formato .pdf (non CSV)")
            return redirect('assign-clusters')
        
        try:
            # Save file
            inventory_folder = Path(settings.INVENTORY_FOLDER)
            inventory_folder.mkdir(exist_ok=True)
            
            timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            file_path = inventory_folder / f"cluster_{timestamp}_{pdf_file.name}"
            
            with open(file_path, 'wb+') as destination:
                for chunk in pdf_file.chunks():
                    destination.write(chunk)
            
            # ✅ DISPATCH TO CELERY with explicit cluster name
            from ..tasks import assign_clusters_task
            
            result = assign_clusters_task.apply_async(
                args=[storage_id, str(file_path), cluster],
                retry=True
            )
            
            messages.info(
                request,
                f"Assigning cluster '{cluster}' to products. This may take a few minutes."
            )
            
            return redirect('task-progress', task_id=result.id, storage_id=storage_id)
            
        except Exception as e:
            logger.exception("Error assigning clusters")
            messages.error(request, f"Errore: {str(e)}")
            return redirect('assign-clusters')

    clusters_by_storage, _ = _load_clusters_by_storage(supermarkets)
    return render(request, 'inventory/assign_clusters.html', {
        'supermarkets': supermarkets,
        'clusters_by_storage': json.dumps(clusters_by_storage),
    })


def _load_clusters_by_storage(supermarkets):
    """Return ({storage_id: [clusters]}, [storages that have at least one cluster])."""
    clusters_by_storage = {}
    storages_with_clusters = []
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
            if clusters:
                storages_with_clusters.append({'id': storage.id, 'label': f"{sm.name} — {storage.settore}"})
    return clusters_by_storage, storages_with_clusters


@login_required
def cluster_management_view(request):
    """Rename/move/delete clusters, set a cluster's Giacenza minima, block a cluster via blacklist."""
    supermarkets = Supermarket.objects.filter(owner=request.user)
    clusters_by_storage, all_storages = _load_clusters_by_storage(supermarkets)

    storage_minimums = dict(
        Storage.objects.filter(supermarket__owner=request.user).values_list('id', 'minimum_stock')
    )
    cluster_minimums = {}
    for storage_id, name, value in ClusterSetting.objects.filter(
        storage__supermarket__owner=request.user, minimum_stock__isnull=False
    ).values_list('storage_id', 'name', 'minimum_stock'):
        if name in clusters_by_storage.get(storage_id, []):
            cluster_minimums.setdefault(storage_id, {})[name] = value

    return render(request, 'inventory/cluster_management.html', {
        'clusters_by_storage': json.dumps(clusters_by_storage),
        'all_storages': all_storages,
        'storage_minimums': json.dumps(storage_minimums),
        'cluster_minimums': json.dumps(cluster_minimums),
    })


@login_required
@require_POST
def cluster_set_minimum_stock_view(request):
    """Set or clear a cluster's Giacenza minima. Empty value = back to the storage default."""
    storage_id = request.POST.get('storage_id')
    cluster = request.POST.get('cluster', '').strip().upper()
    raw = request.POST.get('minimum_stock', '').strip()

    if not storage_id or not cluster:
        messages.error(request, "Seleziona un magazzino e un cluster.")
        return redirect('cluster-management')

    storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)

    if raw == '':
        ClusterSetting.objects.filter(storage=storage, name=cluster).update(minimum_stock=None)
        messages.success(
            request,
            f"Cluster '{cluster}': giacenza minima rimossa, si usa il default del magazzino ({storage.minimum_stock})."
        )
        return redirect('cluster-management')

    try:
        value = int(raw)
        if value < 0:
            raise ValueError
    except ValueError:
        messages.error(request, "La giacenza minima deve essere un numero intero >= 0.")
        return redirect('cluster-management')

    ClusterSetting.objects.update_or_create(storage=storage, name=cluster, defaults={'minimum_stock': value})
    messages.success(request, f"Cluster '{cluster}': giacenza minima impostata a {value}.")
    return redirect('cluster-management')


@login_required
@require_POST
def manage_cluster_view(request):
    """Handle cluster rename/move and delete operations from the cluster management page."""
    storage_id = request.POST.get('storage_id')
    source = request.POST.get('source_cluster', '').strip().upper()
    action = request.POST.get('action')

    if not storage_id or not source:
        messages.error(request, "Seleziona un magazzino e un cluster.")
        return redirect('cluster-management')

    storage = get_object_or_404(Storage, id=storage_id, supermarket__owner=request.user)

    if action in ('rename', 'move'):
        new_name = request.POST.get('new_name', '').strip().upper()
        if not new_name:
            messages.error(request, "Inserisci il nuovo nome del cluster.")
            return redirect('cluster-management')
        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute(
                "UPDATE products SET cluster = %s WHERE settore = %s AND cluster = %s",
                (new_name, storage.settore, source)
            )
            count = cur.rowcount
        # Products joining an existing cluster take its settings; otherwise the settings follow the rename
        if new_name != source:
            if ClusterSetting.objects.filter(storage=storage, name=new_name).exists():
                ClusterSetting.objects.filter(storage=storage, name=source).delete()
            else:
                ClusterSetting.objects.filter(storage=storage, name=source).update(name=new_name)
        if action == 'move':
            messages.success(request, f"{count} prodotti spostati da '{source}' a '{new_name}'.")
        else:
            messages.success(request, f"Cluster '{source}' rinominato in '{new_name}' ({count} prodotti).")

    elif action == 'delete':
        with RestockService(storage) as service:
            cur = service.db.cursor()
            cur.execute(
                "SELECT cod, v FROM products WHERE settore = %s AND cluster = %s",
                (storage.settore, source)
            )
            for row in cur.fetchall():
                cur.execute(
                    "UPDATE product_stats SET stock = 0 WHERE cod = %s AND v = %s",
                    (row['cod'], row['v'])
                )
                service.db.purge_product(row['cod'], row['v'])
            cur.execute(
                "UPDATE products SET cluster = NULL WHERE settore = %s AND cluster = %s",
                (storage.settore, source)
            )
        ClusterSetting.objects.filter(storage=storage, name=source).delete()
        messages.success(request, f"Cluster '{source}' eliminato e prodotti purgati.")

    else:
        messages.error(request, "Azione non valida.")

    return redirect('cluster-management')
