from __future__ import annotations

from typing import Any

from django.db import connections

from .exceptions import WarmDBUnsupported


def _ensure_postgres(alias: str) -> None:
    engine = connections[alias].settings_dict.get("ENGINE", "")
    if "postgres" not in engine:
        raise WarmDBUnsupported(f"warmdb only supports Postgres (ENGINE={engine!r})")


def terminate_sessions(alias: str, dbname: str) -> None:
    _ensure_postgres(alias)

    with connections[alias]._nodb_cursor() as cursor:
        cursor.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s AND pid <> pg_backend_pid();
            """,
            [dbname],
        )


def drop_database(alias: str, dbname: str) -> None:
    _ensure_postgres(alias)
    terminate_sessions(alias, dbname)

    with connections[alias]._nodb_cursor() as cursor:
        qn = connections[alias].ops.quote_name(dbname)
        cursor.execute(f"DROP DATABASE IF EXISTS {qn};")


def create_database(alias: str, dbname: str) -> None:
    _ensure_postgres(alias)

    with connections[alias]._nodb_cursor() as cursor:
        qn = connections[alias].ops.quote_name(dbname)
        cursor.execute(f"CREATE DATABASE {qn};")


def create_database_from_template(
    alias: str, dbname: str, template_dbname: str
) -> None:
    _ensure_postgres(alias)

    with connections[alias]._nodb_cursor() as cursor:
        qn = connections[alias].ops.quote_name
        cursor.execute(f"CREATE DATABASE {qn(dbname)} TEMPLATE {qn(template_dbname)};")
