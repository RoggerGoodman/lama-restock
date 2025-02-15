from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse_lazy
from django.views.generic import ListView, DetailView, CreateView, UpdateView
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth.mixins import LoginRequiredMixin

from .models import Supermarket, RestockSchedule, Blacklist, BlacklistEntry, Category
from .forms import RestockScheduleForm, BlacklistForm, BlacklistEntryForm


@login_required
def dashboard_view(request):
    """
    Render the user dashboard.
    """
    return render(request, 'dashboard.html')


def home_view(request):
    """
    Render the home page.
    """
    return render(request, 'home.html')


def signup(request):
    """
    Handle user signup using Django's built-in UserCreationForm.
    Automatically logs in the user after a successful signup.
    """
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect("home")
    else:
        form = UserCreationForm()

    return render(request, "registration/signup.html", {"form": form})


class RestockScheduleListView(LoginRequiredMixin, ListView):
    """
    List all categories for the logged-in user's supermarkets.
    For each category, the user can add or edit its restock schedule.
    """
    model = Category
    template_name = "restock_schedule_list.html"
    context_object_name = "categories"
    
    def get_queryset(self):
        # Only categories belonging to supermarkets owned by the logged-in user.
        return Category.objects.filter(supermarket__owner=self.request.user).order_by("supermarket__name", "name")


class RestockScheduleView(LoginRequiredMixin, UpdateView):
    """
    Upsert view for a RestockSchedule attached to a specific Category.
    If no schedule exists for the category, one is created.
    """
    model = RestockSchedule
    form_class = RestockScheduleForm
    template_name = "restock_schedule.html"
    
    def get_object(self, queryset=None):
        """
        Retrieve the schedule for the given category.
        Create one if it doesn't exist.
        """
        category = get_object_or_404(
            Category, 
            id=self.kwargs.get("category_id"),
            supermarket__owner=self.request.user
        )
        schedule, created = RestockSchedule.objects.get_or_create(category=category)
        return schedule

    def get_context_data(self, **kwargs):
        """
        Add the Category object to the context.
        """
        context = super().get_context_data(**kwargs)
        context["category"] = get_object_or_404(
            Category,
            id=self.kwargs.get("category_id"),
            supermarket__owner=self.request.user
        )
        return context

    def get_success_url(self):
        """
        After saving, redirect to the list of restock schedules.
        """
        return reverse_lazy("restock_schedule_list")


# List all supermarkets owned by the logged-in user.
class SupermarketListView(LoginRequiredMixin, ListView):
    model = Supermarket
    template_name = 'supermarkets/list.html'
    context_object_name = 'supermarkets'

    def get_queryset(self):
        # Only show supermarkets for the current user.
        return Supermarket.objects.filter(owner=self.request.user).order_by('name')


# Display the details for a single supermarket.
class SupermarketDetailView(LoginRequiredMixin, DetailView):
    model = Supermarket
    template_name = 'supermarkets/detail.html'
    context_object_name = 'supermarket'

    def get_queryset(self):
        # Ensure users can only see their own supermarkets.
        return Supermarket.objects.filter(owner=self.request.user)


# Allow the user to add a new supermarket.
class SupermarketCreateView(LoginRequiredMixin, CreateView):
    model = Supermarket
    # You can either specify fields directly or use a custom form.
    fields = ['name', 'username', 'password']
    template_name = 'supermarkets/form.html'

    def form_valid(self, form):
        # Set the owner to the logged-in user.
        form.instance.owner = self.request.user
        return super().form_valid(form)

    def get_success_url(self):
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.pk})


# Allow the user to edit an existing supermarket.
class SupermarketUpdateView(LoginRequiredMixin, UpdateView):
    model = Supermarket
    fields = ['name', 'username', 'password']
    template_name = 'supermarkets/form.html'

    def get_queryset(self):
        # Only allow editing of supermarkets owned by the current user.
        return Supermarket.objects.filter(owner=self.request.user)

    def get_success_url(self):
        return reverse_lazy('supermarket-detail', kwargs={'pk': self.object.pk})


class BlacklistListView(LoginRequiredMixin, ListView):
    """
    List all blacklists.
    """
    model = Blacklist
    template_name = 'blacklists/list.html'
    context_object_name = 'blacklists'
    ordering = ['name']


class BlacklistDetailView(LoginRequiredMixin, DetailView):
    """
    Display the details of a single blacklist along with its entries.
    """
    model = Blacklist
    template_name = 'blacklists/detail.html'
    context_object_name = 'blacklist'


class BlacklistCreateView(LoginRequiredMixin, CreateView):
    """
    Allow users to create a new blacklist.
    """
    model = Blacklist
    form_class = BlacklistForm
    template_name = 'blacklists/form.html'
    
    def get_success_url(self):
        return reverse_lazy('blacklist-detail', kwargs={'pk': self.object.pk})


class BlacklistEntryCreateView(LoginRequiredMixin, CreateView):
    """
    Allow users to add a new entry (product code and var) to a specific blacklist.
    The URL must pass a `blacklist_pk` parameter.
    """
    model = BlacklistEntry
    form_class = BlacklistEntryForm
    template_name = 'blacklists/entries/form.html'
    
    def dispatch(self, request, *args, **kwargs):
        # Retrieve and store the parent blacklist once.
        self.blacklist = get_object_or_404(Blacklist, pk=self.kwargs.get('blacklist_pk'))
        return super().dispatch(request, *args, **kwargs)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['blacklist'] = self.blacklist
        return context
    
    def form_valid(self, form):
        # Associate the new entry with the parent blacklist.
        form.instance.blacklist = self.blacklist
        return super().form_valid(form)
    
    def get_success_url(self):
        return reverse_lazy('blacklist-detail', kwargs={'pk': self.blacklist.pk})
