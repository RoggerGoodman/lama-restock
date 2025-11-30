# LamApp/supermarkets/urls.py
from django.contrib.auth import views as auth_views
from django.contrib.auth.views import LogoutView
from django.urls import path
from . import views

urlpatterns = [
    # Home & Dashboard
    path('', views.home_view, name='home'),
    path('dashboard/', views.dashboard_view, name='dashboard'),
    
    # Authentication
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('signup/', views.signup, name='signup'),
    path('logout/', LogoutView.as_view(next_page='home'), name='logout'),
    
    # Supermarkets
    path('supermarkets/', views.SupermarketListView.as_view(), name='supermarket-list'),
    path('supermarkets/add/', views.SupermarketCreateView.as_view(), name='supermarket-add'),
    path('supermarkets/<int:pk>/', views.SupermarketDetailView.as_view(), name='supermarket-detail'),
    path('supermarkets/<int:pk>/edit/', views.SupermarketUpdateView.as_view(), name='supermarket-edit'),
    path('supermarkets/<int:pk>/delete/', views.SupermarketDeleteView.as_view(), name='supermarket-delete'),
    path('supermarkets/<int:pk>/sync/', views.sync_storages_view, name='sync-storages'),
    # MOVED: Promos are now supermarket-level (not storage-level)
    path('supermarkets/<int:supermarket_id>/upload-promos/', views.upload_promos_view, name='upload-promos'),
    
    # Storages
    path('storages/<int:pk>/', views.StorageDetailView.as_view(), name='storage-detail'),
    path('storages/<int:pk>/delete/', views.StorageDeleteView.as_view(), name='storage-delete'),
    path('storages/<int:storage_id>/restock/', views.run_restock_view, name='run-restock'),
    
    # List Updates
    path('storages/<int:storage_id>/configure-updates/', views.configure_list_updates_view, name='configure-list-updates'),
    path('storages/<int:storage_id>/update-list/', views.manual_list_update_view, name='manual-list-update'),
    
    # Stock Verification
    path('storages/<int:storage_id>/verify/', views.verify_stock_view, name='verify-stock'),
    path('storages/<int:storage_id>/verification-report/', views.verification_report_view, name='verification-report'),
    
    # Stock Value Analysis
    path('storages/<int:storage_id>/stock-value/', views.stock_value_view, name='stock-value'),
    path('supermarkets/<int:supermarket_id>/stock-value/', views.supermarket_stock_value_view, name='supermarket-stock-value'),
    
    # Stock Adjustments
    path('storages/<int:storage_id>/adjust-stock/', views.adjust_stock_view, name='adjust-stock'),
    path('storages/<int:storage_id>/bulk-adjust-stock/', views.bulk_adjust_stock_view, name='bulk-adjust-stock'),
    
    # Restock Schedules
    path('schedules/', views.RestockScheduleListView.as_view(), name='restock_schedule_list'),
    path('schedules/<int:storage_id>/edit/', views.RestockScheduleView.as_view(), name='restock_schedule'),
    path('schedules/<int:pk>/delete/', views.RestockScheduleDeleteView.as_view(), name='schedule-delete'),
    
    # Restock Logs
    path('logs/<int:pk>/', views.RestockLogDetailView.as_view(), name='restock-log-detail'),
    path('logs/<int:log_id>/execute/', views.execute_order_view, name='execute-order'),
    
    # Blacklists
    path('blacklists/', views.BlacklistListView.as_view(), name='blacklist-list'),
    path('blacklists/add/', views.BlacklistCreateView.as_view(), name='blacklist-add'),
    path('blacklists/<int:pk>/', views.BlacklistDetailView.as_view(), name='blacklist-detail'),
    path('blacklists/<int:pk>/delete/', views.BlacklistDeleteView.as_view(), name='blacklist-delete'),
    path('blacklists/<int:blacklist_pk>/entries/add/', views.BlacklistEntryCreateView.as_view(), name='blacklistentry-add'),
    path('blacklists/entries/<int:pk>/delete/', views.BlacklistEntryDeleteView.as_view(), name='blacklistentry-delete'),
]
