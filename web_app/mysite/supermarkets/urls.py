from django.contrib.auth import views as auth_views
from django.contrib.auth.views import LogoutView
from django.urls import path
from . import views  # Import your views if needed

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('', views.home_view, name='home'),  # Home view remains at the root
    path("login/", auth_views.LoginView.as_view(template_name="registration/login.html"), name="login"),
    path("signup/", views.signup, name="signup"),
    # Change the supermarket list route from '' to 'supermarkets/'
    path('supermarkets/', views.SupermarketListView.as_view(), name='supermarket-list'),
    path('supermarkets/add/', views.SupermarketCreateView.as_view(), name='supermarket-add'),
    path('supermarkets/<int:pk>/', views.SupermarketDetailView.as_view(), name='supermarket-detail'),
    path('supermarkets/<int:pk>/edit/', views.SupermarketUpdateView.as_view(), name='supermarket-edit'),
    path("logout/", LogoutView.as_view(), name="logout"),
    path('blacklists/', views.BlacklistListView.as_view(), name='blacklist-list'),
    path('blacklists/add/', views.BlacklistCreateView.as_view(), name='blacklist-add'),
    path('blacklists/<int:pk>/', views.BlacklistDetailView.as_view(), name='blacklist-detail'),
    path('blacklists/<int:blacklist_pk>/entries/add/', views.BlacklistEntryCreateView.as_view(), name='blacklistentry-add'),
    path('restock-schedules/', views.RestockScheduleListView.as_view(), name='restock_schedule_list'),
    path('restock-schedules/<int:category_id>/edit/', views.RestockScheduleView.as_view(), name='restock_schedule'),
]
