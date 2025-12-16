# supermarkets/urls.py - CLEANED UP VERSION

from django.contrib.auth import views as auth_views
from django.contrib.auth.views import LogoutView
from django.urls import path
from . import views

urlpatterns = [
    # ============ Home & Dashboard ============
    path('', views.home_view, name='home'),
    path('dashboard/', views.dashboard_view, name='dashboard'),
    
    # ============ Authentication ============
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('signup/', views.signup, name='signup'),
    path('logout/', LogoutView.as_view(next_page='home'), name='logout'),
    
    # ============ Supermarkets ============
    path('supermarkets/', views.SupermarketListView.as_view(), name='supermarket-list'),
    path('supermarkets/add/', views.SupermarketCreateView.as_view(), name='supermarket-add'),
    path('supermarkets/<int:pk>/', views.SupermarketDetailView.as_view(), name='supermarket-detail'),
    path('supermarkets/<int:pk>/edit/', views.SupermarketUpdateView.as_view(), name='supermarket-edit'),
    path('supermarkets/<int:pk>/delete/', views.SupermarketDeleteView.as_view(), name='supermarket-delete'),
    path('supermarkets/<int:pk>/sync/', views.sync_storages_view, name='sync-storages'),
    path('supermarkets/<int:supermarket_id>/upload-promos/', views.upload_promos_view, name='upload-promos'),
    
    # ============ Storages ============
    path('storages/<int:pk>/', views.StorageDetailView.as_view(), name='storage-detail'),
    path('storages/<int:pk>/delete/', views.StorageDeleteView.as_view(), name='storage-delete'),
    path('storages/<int:storage_id>/restock/', views.run_restock_view, name='run-restock'),
    path('storages/<int:storage_id>/update-stats/', views.update_stats_only_view, name='update-stats-only'),
    path('storages/<int:storage_id>/add-products/', views.add_products_view, name='add-products'),
    path('storages/<int:storage_id>/purge/', views.purge_products_view, name='purge-products'),
    path('storages/<int:storage_id>/check-purge/', views.check_purge_flagged_view, name='check-purge-flagged'),
    
    # ============ List Updates ============
    path('storages/<int:storage_id>/configure-updates/', views.configure_list_updates_view, name='configure-list-updates'),
    path('storages/<int:storage_id>/update-list/', views.manual_list_update_view, name='manual-list-update'),
    
    # ============ Restock Schedules ============
    path('schedules/', views.RestockScheduleListView.as_view(), name='restock_schedule_list'),
    path('schedules/<int:storage_id>/edit/', views.RestockScheduleView.as_view(), name='restock_schedule'),
    path('schedules/<int:pk>/delete/', views.RestockScheduleDeleteView.as_view(), name='schedule-delete'),
    
    # ============ Restock Logs ============
    path('logs/<int:pk>/', views.RestockLogDetailView.as_view(), name='restock-log-detail'),
    path('logs/<int:log_id>/retry/', views.retry_restock_view, name='retry-restock'),
    path('logs/<int:log_id>/progress/', views.restock_progress_view, name='restock-progress'),
    path('logs/<int:log_id>/flag-products/', views.flag_products_for_purge_view, name='flag-products-for-purge'),
    
    # ============ Blacklists (accessed from dashboard/storage) ============
    path('blacklists/', views.BlacklistListView.as_view(), name='blacklist-list'),
    path('blacklists/add/', views.BlacklistCreateView.as_view(), name='blacklist-add'),
    path('blacklists/<int:pk>/', views.BlacklistDetailView.as_view(), name='blacklist-detail'),
    path('blacklists/<int:pk>/delete/', views.BlacklistDeleteView.as_view(), name='blacklist-delete'),
    path('blacklists/<int:blacklist_pk>/entries/add/', views.BlacklistEntryCreateView.as_view(), name='blacklistentry-add'),
    path('blacklists/entries/<int:pk>/delete/', views.BlacklistEntryDeleteView.as_view(), name='blacklistentry-delete'),
    
    # ============ Inventory Panel (Unified Operations) ============
    path('inventory/', views.inventory_search_view, name='inventory-search'),
    path('inventory/results/<str:search_type>/', views.inventory_results_view, name='inventory-results'),
    path('inventory/not-found/<int:cod>/<int:var>/', views.inventory_product_not_found_view, name='inventory-product-not-found'),
    
    # NEW: Unified inventory operations
    path('inventory/verify-stock/', views.verify_stock_unified_view, name='verify-stock-unified'),
    path('inventory/assign-clusters/', views.assign_clusters_view, name='assign-clusters'),
    path('inventory/record-losses/', views.record_losses_unified_view, name='record-losses-unified'),
    path('inventory/verification-report/', views.verification_report_unified_view, name='verification-report-unified'),
    path('inventory/stock-value/', views.stock_value_unified_view, name='stock-value-unified'),
    path('inventory/losses-analytics/', views.losses_analytics_unified_view, name='losses-analytics-unified'),
    
    # Inventory AJAX endpoints
    path('inventory/api/settores/<int:supermarket_id>/', views.get_settores_for_supermarket_view, name='api-settores'),
    path('inventory/api/clusters/<int:supermarket_id>/<str:settore>/', views.get_clusters_for_settore_view, name='api-clusters'),
    path('inventory/api/storages/<int:supermarket_id>/', views.get_storages_for_supermarket_ajax_view, name='api-storages'),
    path('inventory/adjust-stock/', views.inventory_adjust_stock_ajax_view, name='inventory-adjust-stock'),
    path('inventory/flag-for-purge/', views.inventory_flag_for_purge_ajax_view, name='inventory-flag-purge'),
    path('inventory/verify-single/', views.verify_single_product_ajax_view, name='inventory-verify-single'),
]