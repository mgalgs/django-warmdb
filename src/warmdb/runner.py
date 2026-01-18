from __future__ import annotations

import sys

from django.db import connections
from django.db.migrations.executor import MigrationExecutor
from django.test.runner import DiscoverRunner

from .core import allocate_clone
from .exceptions import WarmDBSchemaChanged
from .postgres import create_database_from_template, drop_database
from .state import WarmDBState


class WarmDBDiscoverRunner(DiscoverRunner):
    """DiscoverRunner that allocates a pre-migrated Postgres clone DB."""

    _warmdb_allocated_name: str | None = None
    _warmdb_template_name: str | None = None

    def setup_databases(self, **kwargs):
        parallel = kwargs.get("parallel", 1)
        if parallel and int(parallel) != 1:
            raise RuntimeError("warmdb does not support --parallel yet")

        # Allocate clone and re-point test DB.
        allocated, template = allocate_clone(alias="default")
        self._warmdb_allocated_name = allocated
        self._warmdb_template_name = template

        db = connections["default"]
        db.settings_dict.setdefault("TEST", {})
        db.settings_dict["TEST"]["NAME"] = allocated
        db.settings_dict["TEST"]["MIGRATE"] = False

        # Force keepdb semantics; warmdb owns lifecycle.
        # Note: Django's internal setup_databases plumbing passes `keepdb` separately,
        # so we must set the runner attribute (and avoid passing `keepdb` via kwargs).
        self.keepdb = True
        kwargs.pop("keepdb", None)

        old_config = super().setup_databases(**kwargs)

        # Definitive check: ensure no unapplied migrations.
        executor = MigrationExecutor(db)
        plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
        if plan:
            raise WarmDBSchemaChanged(
                "Schema changed since warmdb init.\nRun: manage.py warmdb invalidate && manage.py warmdb init"
            )

        return old_config

    def teardown_databases(self, old_config, **kwargs):
        self.keepdb = True
        kwargs.pop("keepdb", None)

        try:
            super().teardown_databases(old_config, **kwargs)
        finally:
            self._recycle_allocated_clone()

    def _recycle_allocated_clone(self) -> None:
        name = self._warmdb_allocated_name
        template = self._warmdb_template_name
        if not name or not template:
            return

        # Ensure we are disconnected from the clone before dropping.
        connections["default"].close()

        state = WarmDBState(self._warmdb_state_path())
        try:
            drop_database("default", name)
            create_database_from_template("default", name, template)
            state.mark_ready(name)
        except Exception as e:
            state.mark_error(name, str(e))
            print(f"warmdb recycle failed for {name}: {e}", file=sys.stderr)

    def _warmdb_state_path(self):
        # Local import to avoid importing settings at module import time.
        from .core import state_path

        return state_path()
