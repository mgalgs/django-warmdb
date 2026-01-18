from __future__ import annotations

from django.test import TestCase

from .models import Widget


class WidgetTests(TestCase):
    def test_create_widget(self):
        Widget.objects.create(name="x")
        assert Widget.objects.count() == 1
