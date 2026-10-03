"""Shared pytest fixtures."""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "fixtures"))

from build_fixtures import build_masterinput  # noqa: E402


@pytest.fixture(scope="session")
def masterinput_db(tmp_path_factory):
    """MasterInput database built once per session from tests/fixtures/sources.

    Treat it as read-only; use ``masterinput_copy`` in tests that write.
    """
    return build_masterinput(tmp_path_factory.mktemp("fixtures") / "MasterInput.db")


@pytest.fixture
def masterinput_copy(masterinput_db, tmp_path):
    """Private, writable copy of the MasterInput fixture for one test."""
    return Path(shutil.copy2(masterinput_db, tmp_path / "MasterInput.db"))
