import importlib
import os
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def app_module():
    project_root = Path(__file__).resolve().parents[1]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    os.environ.setdefault(
        "ADMIN_SECRET",
        "test-admin-secret-32chars-minimum-1234567890",
    )
    os.environ.setdefault("ADMIN_PASSWORD", "test-admin-password")
    os.environ.setdefault("PUBLIC_BASE_URL", "https://example.com")
    os.environ.setdefault("APP_SKIP_DB_INIT", "true")
    os.environ.setdefault("BACKGROUND_WORKERS_ENABLED", "false")
    os.environ.setdefault("BACKUP_WORKER_ENABLED", "false")
    os.environ.setdefault("OBS_ALERTS_ENABLED", "false")

    if "app" in sys.modules:
        del sys.modules["app"]

    module = importlib.import_module("app")
    module.app.config["TESTING"] = True
    return module


@pytest.fixture(autouse=True)
def reset_app_state(app_module):
    app_module._request_rate_limit.clear()
    app_module._failed_login_attempts.clear()
    app_module._online_sessions.clear()
    app_module.OBS_COUNTERS.clear()
    app_module.OBS_INCIDENTS.clear()
    app_module.OBS_ALERT_LAST_SENT.clear()


@pytest.fixture()
def client(app_module):
    return app_module.app.test_client()
