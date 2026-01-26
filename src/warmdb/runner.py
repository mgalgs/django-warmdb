from __future__ import annotations

import sys

from django.conf import settings
from django.core.management import CommandError
from django.db import connections
from django.db.migrations.executor import MigrationExecutor
from django.test.runner import DiscoverRunner

from .core import allocate_clone
from .exceptions import WarmDBNoReadyDB, WarmDBNotInitialized, WarmDBSchemaChanged
from .postgres import drop_database
from .state import WarmDBState


class WarmDBDiscoverRunner(DiscoverRunner):
    """DiscoverRunner that allocates a pre-migrated Postgres clone DB."""

    _warmdb_allocated_name: str | None = None

    def setup_databases(self, **kwargs):
        parallel = kwargs.get("parallel", 1)
        if parallel and int(parallel) != 1:
            raise RuntimeError("warmdb does not support --parallel yet")

        # Allocate clone and re-point test DB.
        try:
            allocated, _template = allocate_clone(alias="default")
        except WarmDBNoReadyDB as e:
            raise CommandError(str(e)) from None
        except WarmDBNotInitialized as e:
            raise CommandError(str(e)) from None

        self._warmdb_allocated_name = allocated

        try:
            # Point the default connection at the allocated clone.
            db = connections["default"]
            settings.DATABASES["default"]["NAME"] = allocated
            db.settings_dict["NAME"] = allocated

            # Ensure the connection uses the new name.
            db.close()

            # Definitive check: ensure no unapplied migrations.
            executor = MigrationExecutor(db)
            plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
            if plan:
                raise CommandError(
                    "Schema changed since warmdb init.\nRun: manage.py warmdb refresh"
                )
        except Exception:
            # Clean up the allocated clone so it doesn't stay stuck as in-use.
            self._consume_allocated_clone()
            raise

        # Return the config Django expects for teardown.  The format is a list
        # of (connection, old_name, destroy) tuples.  We set destroy=False
        # because warmdb owns the lifecycle.
        return [(db, allocated, False)]

    def teardown_databases(self, old_config, **kwargs):
        self.keepdb = True
        kwargs.pop("keepdb", None)

        try:
            super().teardown_databases(old_config, **kwargs)
        finally:
            self._consume_allocated_clone()

    def _consume_allocated_clone(self) -> None:
        name = self._warmdb_allocated_name
        if not name:
            return

        # Ensure we are disconnected from the clone before dropping.
        connections["default"].close()

        state = WarmDBState(self._warmdb_state_path())
        try:
            drop_database("default", name)
            state.mark_consumed(name)
        except Exception as e:
            state.mark_error(name, str(e))
            print(f"warmdb consume failed for {name}: {e}", file=sys.stderr)

    def _warmdb_state_path(self):
        # Local import to avoid importing settings at module import time.
        from .core import state_path

        return state_path()
