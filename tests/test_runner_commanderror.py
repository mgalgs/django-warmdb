from __future__ import annotations

from unittest import mock

from django.core.management import CommandError
from warmdb.exceptions import WarmDBNoReadyDB, WarmDBNotInitialized
from warmdb.runner import WarmDBDiscoverRunner


def test_warmdb_no_ready_db_converts_to_commanderror():
    """WarmDBNoReadyDB is converted to CommandError for clean output."""
    runner = WarmDBDiscoverRunner(verbosity=0, interactive=False)

    # Mock allocate_clone to raise WarmDBNoReadyDB
    with mock.patch("warmdb.runner.allocate_clone") as mock_allocate:
        mock_allocate.side_effect = WarmDBNoReadyDB(
            "No warmdb databases are ready. Pool exhausted. Run: manage.py warmdb refresh"
        )

        # Should raise CommandError (not the original WarmDBNoReadyDB)
        try:
            runner.setup_databases()
            assert False, "Should have raised CommandError"
        except CommandError as e:
            # Verify the message is preserved
            assert "No warmdb databases are ready" in str(e)
        except Exception as e:
            assert (
                False
            ), f"Should have raised CommandError, got {type(e).__name__}: {e}"


def test_warmdb_not_initialized_converts_to_commanderror():
    """WarmDBNotInitialized is converted to CommandError for clean output."""
    runner = WarmDBDiscoverRunner(verbosity=0, interactive=False)

    # Mock allocate_clone to raise WarmDBNotInitialized
    with mock.patch("warmdb.runner.allocate_clone") as mock_allocate:
        mock_allocate.side_effect = WarmDBNotInitialized(
            "warmdb is not initialized. Run: manage.py warmdb init"
        )

        try:
            runner.setup_databases()
            assert False, "Should have raised CommandError"
        except CommandError as e:
            assert "warmdb is not initialized" in str(e)
        except Exception as e:
            assert (
                False
            ), f"Should have raised CommandError, got {type(e).__name__}: {e}"
