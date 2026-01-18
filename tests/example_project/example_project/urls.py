from __future__ import annotations

from django.urls import path

urlpatterns = [path("healthz/", lambda r: None)]
