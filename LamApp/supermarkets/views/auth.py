"""Signup and account pages."""

from django.shortcuts import render, redirect
from django.contrib.auth.forms import UserCreationForm, PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django import forms as django_forms
from django.contrib.auth.models import User as AuthUser
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.conf import settings


def signup(request):
    """
    User registration with optional closure control.
    Set REGISTRATION_CLOSED=True in settings to disable public registration.
    """
    # Check if registration is closed
    if getattr(settings, 'REGISTRATION_CLOSED', False):
        messages.error(
            request,
            "Registration is currently closed. Please contact the administrator for access."
        )
        return redirect('login')
    
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, "Account creato con successo!")
            return redirect("dashboard")
    else:
        form = UserCreationForm()
    
    return render(request, "registration/signup.html", {"form": form})


class UsernameChangeForm(django_forms.ModelForm):
    class Meta:
        model = AuthUser
        fields = ['username']


@login_required
def account_view(request):
    username_form = UsernameChangeForm(instance=request.user)
    password_form = PasswordChangeForm(request.user)

    if request.method == 'POST':
        if 'change_username' in request.POST:
            username_form = UsernameChangeForm(request.POST, instance=request.user)
            if username_form.is_valid():
                username_form.save()
                messages.success(request, "Username aggiornato.")
                return redirect('account')
        elif 'change_password' in request.POST:
            password_form = PasswordChangeForm(request.user, request.POST)
            if password_form.is_valid():
                password_form.save()
                update_session_auth_hash(request, password_form.user)
                messages.success(request, "Password aggiornata.")
                return redirect('account')

    return render(request, 'registration/account.html', {
        'username_form': username_form,
        'password_form': password_form,
    })
