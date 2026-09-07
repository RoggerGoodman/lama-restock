from django.contrib import messages
from django.http import JsonResponse
from django.shortcuts import redirect
from django.urls import reverse

from .demo import is_demo_user

# GET/HEAD/OPTIONS never mutate. Every dangerous action in the app is a POST
# (orders, restock, sync, list-update, deletes...), so blocking non-safe methods
# for the demo user is a single fail-safe choke point: any future mutation is
# denied by default too.
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})

DEMO_MESSAGE = "Questa è una demo di sola lettura: l'azione è disabilitata."


class DemoReadOnlyMiddleware:
    """Reject any state-changing request from the demo account."""

    def __init__(self, get_response):
        self.get_response = get_response
        self._logout_path = None

    def _is_logout(self, request):
        # Logout is a POST in modern Django and must stay available to the demo.
        if self._logout_path is None:
            self._logout_path = reverse("logout")
        return request.path == self._logout_path

    def __call__(self, request):
        if (
            request.method not in SAFE_METHODS
            and is_demo_user(getattr(request, "user", None))
            and not self._is_logout(request)
        ):
            wants_json = (
                request.headers.get("X-Requested-With") == "XMLHttpRequest"
                or "application/json" in request.headers.get("Accept", "")
            )
            if wants_json:
                return JsonResponse({"success": False, "error": DEMO_MESSAGE}, status=403)
            messages.warning(request, DEMO_MESSAGE)
            return redirect(request.META.get("HTTP_REFERER") or "dashboard")

        return self.get_response(request)
