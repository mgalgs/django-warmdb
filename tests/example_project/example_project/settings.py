from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "django-warmdb-example-secret"
DEBUG = True

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "example_app",
    "warmdb",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "example_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

WSGI_APPLICATION = "example_project.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("WARMDB_PGDATABASE", "postgres"),
        "USER": os.environ.get("WARMDB_PGUSER", "postgres"),
        "PASSWORD": os.environ.get("WARMDB_PGPASSWORD", "postgres"),
        "HOST": os.environ.get("WARMDB_PGHOST", "localhost"),
        "PORT": os.environ.get("WARMDB_PGPORT", "5432"),
    }
}

# Let the test runner override settings; but make warmdb the default runner here.
TEST_RUNNER = "warmdb.runner.WarmDBDiscoverRunner"

USE_TZ = True

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
