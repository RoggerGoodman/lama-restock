"""Web views, one module per area.

Everything is re-exported here so urls.py can keep using `views.<name>`.
margins, credit_notes and sync are imported by urls.py as modules.
"""

from .common import net_price_of, parse_shelf_barcode
from .auth import signup, UsernameChangeForm, account_view
from .dashboard import dashboard_view, home_view
from .supermarkets import (
    SupermarketListView, SupermarketDetailView, SupermarketCreateView,
    SupermarketUpdateView, SupermarketDeleteView, closure_calendar_view,
    closure_api_view, get_storages_for_supermarket_ajax_view,
)
from .storages import (
    StorageDetailView, storage_set_minimum_stock_view, calibration_report_view,
    StorageDeleteView, manual_list_update_view,
)
from .order_comparison import (
    order_comparison_view, save_comparison_snapshot_view,
    serve_comparison_snapshot_view,
)
from .schedules import (
    RestockScheduleListView, RestockScheduleView, RestockScheduleDeleteView,
    schedule_exceptions_api,
)
from .restock import (
    run_restock_view, retry_restock_view, RestockLogDetailView,
    RestockLogDeleteView, dismiss_failed_log,
)
from .order_review import (
    order_review_search, order_review_edit, order_submit, order_recalc,
    order_discard,
)
from .blacklists import (
    BlacklistListView, BlacklistDetailView, BlacklistCreateView,
    BlacklistDeleteView, BlacklistEntryCreateView, BlacklistEntryDeleteView,
    blacklist_entry_reintegrate_view,
)
from .products import (
    add_products_view, purge_products_view, check_purge_flagged_view,
    flag_products_for_purge_view, dismiss_product_link_notification,
    dismiss_all_product_link_notifications, product_links_view,
)
from .analytics import (
    stock_value_unified_view, create_stock_snapshot_view,
    delete_stock_snapshot_view, losses_analytics_unified_view,
)
from .profit import stock_profit_view
from .promos_equipment import (
    promo_products_view, order_promo_products_view, equipment_order_view,
    order_equipment_view,
)
from .inventory import (
    inventory_search_view, fermi_products_api_view, inventory_results_view,
    inventory_product_not_found_view, get_settores_for_supermarket_view,
    inventory_flag_for_purge_ajax_view, fermi_blacklist_view,
    inventory_adjust_stock_ajax_view,
)
from .clusters import (
    cluster_order_preview_view, get_clusters_for_settore_view,
    create_blacklist_from_cluster_view, assign_clusters_view,
    cluster_management_view, cluster_set_minimum_stock_view, manage_cluster_view,
)
from .verification import (
    auto_add_product_view, verify_stock_unified_enhanced_view,
    verification_report_unified_view, verify_product_ajax_view,
    pending_verifications_view,
)
from .losses import (
    record_losses_unified_view, edit_losses_view, edit_loss_ajax_view,
    loss_log_fetch_ean_ajax,
)
from .deliveries import (
    upload_ddt_view, delivery_check_view, delivery_check_lookup_ean_ajax,
    delivery_check_parse_ddt_ajax, delivery_check_fetch_ean_ajax,
    delivery_check_sync_scan_ajax,
)
from .progress import (
    task_progress_view, task_status_ajax_view, restock_task_progress_view,
)
from .recipes import (
    RecipeListView, RecipeDetailView, RecipeDeleteView, dismiss_recipe_cost_alert,
    dismiss_all_recipe_cost_alerts, recipe_create_view, recipe_update_view,
    recipe_product_search_view, recipe_get_base_items_view,
)
