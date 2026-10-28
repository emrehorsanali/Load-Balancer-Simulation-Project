"""Test setup. Tests run from the repository checkout or from the test image."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
# The sample data and validators sit at the repository root in a checkout and
# next to the app package in the test image.
DATA_DIR = next(
    directory for directory in (BACKEND_DIR, BACKEND_DIR.parent)
    if (directory / "validate_run.py").is_file()
)
for path in (BACKEND_DIR, DATA_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture
def api(tmp_path, monkeypatch):
    """An HTTP client for a fresh API instance whose state lives in ``tmp_path``."""
    from fastapi.testclient import TestClient

    monkeypatch.setenv("STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("INITIAL_SERVERS_FILE", str(DATA_DIR / "servers.json"))
    monkeypatch.setenv("DEFAULT_REQUESTS_FILE", str(DATA_DIR / "requests.csv"))
    import app.main

    module = importlib.reload(app.main)
    with TestClient(module.app) as client:
        client.state_dir = tmp_path / "state"
        yield client
