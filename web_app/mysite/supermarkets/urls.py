from django.contrib.auth import views as auth_views
from django.contrib.auth.views import LogoutView
from django.urls import path
from . import views  # Import your views if needed

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('', views.home_view, name='home'),  # This defines the 'home' route
    path("login/", auth_views.LoginView.as_view(template_name="registration/login.html"), name="login"),
    path("signup/", views.signup, name="signup"),  # Signup page (we'll add this later)
    path('supermarkets/', views.supermarket_list, name='supermarket_list'),
    path("supermarket/<int:supermarket_id>/restock/", views.restock_schedule, name="restock_schedule"),
    path("logout/", LogoutView.as_view(), name="logout"),
]
