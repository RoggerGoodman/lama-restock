from django.shortcuts import render, redirect
from .models import Supermarket, RestockSchedule
from .forms import RestockScheduleForm
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required, permission_required
from django.shortcuts import get_object_or_404

@login_required
def dashboard_view(request):
    return render(request, 'dashboard.html')

def home_view(request):
    return render(request, 'home.html')  # Make sure you have a home.html

def signup(request):
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)  # Automatically log in the user after signup
            return redirect("home")  # Redirect to your home page
    else:
        form = UserCreationForm()

    return render(request, "registration/signup.html", {"form": form})


@login_required
@permission_required('supermarkets.change_restockschedule', raise_exception=True)
def restock_schedule(request, supermarket_id):
    supermarket = Supermarket.objects.get(id=supermarket_id)

    try:
        schedule = RestockSchedule.objects.get(supermarket=supermarket)
    except RestockSchedule.DoesNotExist:
        schedule = None

    if request.method == "POST":
        form = RestockScheduleForm(request.POST, instance=schedule)
        if form.is_valid():
            schedule = form.save(commit=False)
            schedule.supermarket = supermarket
            schedule.save()
            return redirect("supermarket_detail", supermarket_id=supermarket.id)  # Redirect to supermarket page

    else:
        form = RestockScheduleForm(instance=schedule)

    return render(request, "restock_schedule.html", {"form": form, "supermarket": supermarket})

@login_required  # Requires login
@permission_required('supermarkets.view_supermarket', raise_exception=True)  # Requires permission
def supermarket_list(request):
    supermarkets = Supermarket.objects.all()
    return render(request, 'supermarkets/list.html', {'supermarkets': supermarkets})
