from __future__ import annotations

from django.core.management.base import BaseCommand

from warmdb.core import init_pool, invalidate_pool
from warmdb.state import WarmDBState


class Command(BaseCommand):
    help = "Manage warm Postgres DB pool for Django tests"

    def add_arguments(self, parser):
        parser.add_argument(
            "subcommand",
            choices=["init", "status", "invalidate"],
            help="Subcommand to run",
        )

        parser.add_argument("--pool-size", type=int, default=5)
        parser.add_argument("--force", action="store_true")
        parser.add_argument("--prefix", default="warmdb")

    def handle(self, *args, **options):
        subcommand = options["subcommand"]

        if subcommand == "init":
            init_pool(
                pool_size=int(options["pool_size"]),
                force=bool(options["force"]),
                prefix=str(options["prefix"]),
            )
            self.stdout.write(self.style.SUCCESS("warmdb init complete"))
            return

        if subcommand == "status":
            from warmdb.core import state_path

            state = WarmDBState(state_path())
            if not state.exists():
                self.stdout.write(
                    "warmdb is not initialized. Run: manage.py warmdb init"
                )
                return

            schema_hash = state.get_meta("schema_hash")
            template = state.get_meta("template_db_name")

            self.stdout.write(f"template: {template}")
            self.stdout.write(f"schema_hash: {schema_hash}")
            for r in state.list_dbs():
                extra = ""
                if r.status == "in-use":
                    extra = f" pid={r.allocated_to_pid} at={r.allocated_at}"
                if r.status == "error" and r.last_error:
                    extra = f" error={r.last_error}"
                self.stdout.write(f"{r.name}: {r.status}{extra}")
            return

        if subcommand == "invalidate":
            invalidate_pool()
            self.stdout.write(self.style.SUCCESS("warmdb invalidated"))
            return

        raise RuntimeError(f"Unknown subcommand: {subcommand}")
