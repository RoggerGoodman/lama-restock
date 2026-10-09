"""Dashboard and landing page."""

from django.utils import timezone
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
import logging

from ..models import (
    Supermarket, RestockSchedule, BlacklistEntry, RestockLog, RecipeCostAlert,
    ProductLinkNotification, CreditNote,
)
from ..services import RestockService

logger = logging.getLogger(__name__)


@login_required
def dashboard_view(request):
    from django.db.models import Prefetch

    # The "ultima operazione" column reads storage.restock_logs.first. RestockLog has
    # no default ordering, and loss_recording logs are supermarket-wide despite being
    # pinned to an arbitrary storage — so filter them out and order explicitly here.
    supermarkets = Supermarket.objects.filter(owner=request.user).prefetch_related(
        'storages__schedule',
        Prefetch(
            'storages__restock_logs',
            queryset=RestockLog.objects.exclude(
                operation_type='loss_recording'
            ).order_by('-started_at')
        ),
        'storages__blacklists'
    )
    
    # ✅ FIXED: Only show KEY operations (full_restock and order_execution)
    recent_logs = RestockLog.objects.filter(
        storage__supermarket__owner=request.user,
        operation_type__in=['full_restock', 'order_execution']  # ← FILTER KEY OPERATIONS ONLY
    ).select_related('storage', 'storage__supermarket').order_by('storage__name', '-started_at')

    # Group recent logs by storage, limit 5 per storage
    from collections import OrderedDict
    recent_logs_by_storage = OrderedDict()
    for log in recent_logs:
        storage_key = (log.storage.id, log.storage.name, log.storage.supermarket.name)
        if storage_key not in recent_logs_by_storage:
            recent_logs_by_storage[storage_key] = []
        # Limit to 5 most recent operations per storage
        if len(recent_logs_by_storage[storage_key]) < 5:
            recent_logs_by_storage[storage_key].append(log)
    
    # Get failed logs that need attention (last 24h, not dismissed)
    from datetime import timedelta
    last_24h = timezone.now() - timedelta(hours=24)
    failed_logs = RestockLog.objects.filter(
        storage__supermarket__owner=request.user,
        status='failed',
        started_at__gte=last_24h,
        is_dismissed=False
    ).select_related('storage', 'storage__supermarket').order_by('-started_at')[:5]
    
    # Pending verifications count (efficient)
    pending_verifications = 0
    
    # Group storages by supermarket to minimize DB connections
    supermarkets_with_storages = {}
    for sm in supermarkets:
        if sm.storages.exists():
            supermarkets_with_storages[sm.id] = sm
    
    # Process each supermarket's database once.
    # Count ALL pending verifications across every supermarket (drives the
    # dashboard "Attenzione" card, which only shows when the count is > 0),
    # and collect up to 5 sample products for the preview.
    top_pending_products = []
    for sm_id, sm in supermarkets_with_storages.items():
        try:
            storage = sm.storages.first()
            with RestockService(storage) as service:
                cursor = service.db.cursor()
                settores = list(sm.storages.values_list('settore', flat=True).distinct())

                if not settores:
                    continue

                settore_placeholders = ','.join(['%s'] * len(settores))

                # Keep this WHERE clause in sync with pending_verifications_view()
                pending_where = f"""
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
                """

                # Blacklist lives in the Django DB, so filter in Python
                blacklisted = set(
                    BlacklistEntry.objects.filter(
                        blacklist__storage__supermarket=sm
                    ).values_list('product_code', 'product_var')
                )
                cursor.execute(
                    f"SELECT p.cod, p.v, p.descrizione, ps.stock {pending_where}",
                    settores,
                )
                for row in cursor.fetchall():
                    if (row['cod'], row['v']) in blacklisted:
                        continue
                    pending_verifications += 1
                    # Collect a few samples for the dashboard preview
                    if len(top_pending_products) < 5:
                        top_pending_products.append({
                            'supermarket': sm.name,
                            'cod': row['cod'],
                            'var': row['v'],
                            'name': row['descrizione'] or f"Product {row['cod']}.{row['v']}",
                            'stock': row['stock'] or 0
                        })
        except Exception as e:
            logger.warning(f"Could not load pending verifications for {sm.name}: {e}")
            continue

    logger.info(f"Dashboard: {pending_verifications} total pending verifications across {len(supermarkets_with_storages)} supermarkets")

    # Get notification counts for each storage
    storage_notifications = {}
    for sm in supermarkets:
        for storage in sm.storages.all():
            try:
                with RestockService(storage) as service:
                    cursor = service.db.cursor()

                    # Count negative stock products
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND ps.verified = TRUE
                            AND ps.stock < 0
                    """, (storage.settore,))
                    negative_count = cursor.fetchone()['cnt']

                    # Count out of stock products
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND p.purge_flag = FALSE
                            AND ps.verified = TRUE
                            AND ps.stock = 0
                            AND p.disponibilita = 'Si'
                            AND EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements_text(ps.sales_sets) WITH ORDINALITY AS s(val, idx)
                                WHERE s.idx <= 7 AND s.val::numeric <> 0
                            )
                    """, (storage.settore,))
                    out_of_stock_count = cursor.fetchone()['cnt']

                    # Count brand-new available products added within the last 7 days
                    cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM products p
                        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                        WHERE p.settore = %s
                            AND p.purge_flag = FALSE
                            AND ps.verified IS NOT TRUE
                            AND p.disponibilita != 'No'
                            AND NOT EXISTS (
                                SELECT 1 FROM jsonb_array_elements_text(COALESCE(ps.bought_last_24, '[]'::jsonb)) WITH ORDINALITY AS b(val, idx)
                                WHERE b.idx <= 6 AND b.val::numeric <> 0
                            )
                            AND NOT EXISTS (
                                SELECT 1 FROM jsonb_array_elements_text(COALESCE(ps.sold_last_24, '[]'::jsonb)) WITH ORDINALITY AS s(val, idx)
                                WHERE s.idx <= 6 AND s.val::numeric <> 0
                            )
                            AND p.first_added_at >= CURRENT_DATE - INTERVAL '7 days'
                    """, (storage.settore,))
                    new_available_count = cursor.fetchone()['cnt']

                    storage_notifications[storage.id] = {
                        'negative': negative_count,
                        'out_of_stock': out_of_stock_count,
                        'new_available': new_available_count,
                    }
            except Exception as e:
                logger.warning(f"Could not load notifications for storage {storage.name}: {e}")
                storage_notifications[storage.id] = {
                    'negative': 0,
                    'out_of_stock': 0,
                    'new_available': 0,
                }

    # Get unread recipe cost alerts for this user's supermarkets
    recipe_cost_alerts = RecipeCostAlert.objects.filter(
        recipe__supermarket__owner=request.user,
        is_read=False
    ).select_related('recipe', 'recipe__supermarket').order_by('-created_at')[:10]

    unread_alerts_count = RecipeCostAlert.objects.filter(
        recipe__supermarket__owner=request.user,
        is_read=False
    ).count()

    # Get unread product link notifications for this user's supermarkets
    product_link_notifications = list(ProductLinkNotification.objects.filter(
        supermarket__owner=request.user,
        is_read=False,
    ).select_related('supermarket', 'created_by').order_by('-created_at')[:20])

    # Resolve cod.v to product descriptions, one DB connection per supermarket.
    from ..scripts.DatabaseManager import DatabaseManager
    notifs_by_sm = {}
    for notif in product_link_notifications:
        notifs_by_sm.setdefault(notif.supermarket, []).append(notif)
    for supermarket, notifs in notifs_by_sm.items():
        keys = set()
        for n in notifs:
            keys.add((n.primary_cod, n.primary_v))
            keys.add((n.secondary_cod, n.secondary_v))
        name_map = {}
        try:
            db = DatabaseManager(supermarket_name=supermarket.name)
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
            logger.warning(f"Could not load product names for {supermarket.name}: {e}")
        for n in notifs:
            n.primary_name = name_map.get((n.primary_cod, n.primary_v))
            n.secondary_name = name_map.get((n.secondary_cod, n.secondary_v))

    pending_credit_notes = list(
        CreditNote.objects.filter(
            storage__supermarket__owner=request.user,
            status=CreditNote.STATUS_PENDING,
        ).select_related('storage').order_by('doc_date', 'id')
    )

    context = {
        'supermarkets': supermarkets,
        'recent_logs': recent_logs,  # ← NOW ONLY ORDERS
        'recent_logs_by_storage': recent_logs_by_storage,  # ← GROUPED BY STORAGE
        'failed_logs': failed_logs,
        'pending_verifications': pending_verifications,
        'top_pending_products': top_pending_products,
        'total_supermarkets': supermarkets.count(),
        'total_storages': sum(s.storages.count() for s in supermarkets),
        'active_schedules': RestockSchedule.objects.filter(
            storage__supermarket__owner=request.user
        ).count(),
        'recipe_cost_alerts': recipe_cost_alerts,
        'unread_alerts_count': unread_alerts_count,
        'storage_notifications': storage_notifications,
        'product_link_notifications': product_link_notifications,
        'pending_credit_notes': pending_credit_notes,
    }

    return render(request, 'dashboard.html', context)


def home_view(request):
    """Landing page"""
    if request.user.is_authenticated:
        return redirect('dashboard')
    return render(request, 'home.html')
