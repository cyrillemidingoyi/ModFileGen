"""Shared pytest fixtures."""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "fixtures"))

from build_fixtures import (  # noqa: E402
    build_celsius_v32_template,
    build_masterinput,
    build_modelsdictionary,
)


@pytest.fixture(scope="session")
def masterinput_db(tmp_path_factory):
    """MasterInput database built once per session from tests/fixtures/sources.

    Treat it as read-only; use ``masterinput_copy`` in tests that write.
    """
    return build_masterinput(tmp_path_factory.mktemp("fixtures") / "MasterInput.db")


@pytest.fixture(scope="session")
def modelsdictionary_db(tmp_path_factory):
    """ModelsDictionary (``Variables`` table only) built once per session. Read-only."""
    return build_modelsdictionary(
        tmp_path_factory.mktemp("fixtures") / "ModelsDictionary.db"
    )


@pytest.fixture(scope="session")
def celsius_v32_template_db(tmp_path_factory):
    """CELSIUS V32 template database built once per session. Read-only."""
    return build_celsius_v32_template(
        tmp_path_factory.mktemp("fixtures") / "celsius_model_input.db"
    )


@pytest.fixture
def masterinput_copy(masterinput_db, tmp_path):
    """Private, writable copy of the MasterInput fixture for one test."""
    return Path(shutil.copy2(masterinput_db, tmp_path / "MasterInput.db"))


@pytest.fixture(autouse=True)
def _apsim_tests_write_in_tmp_path(request, tmp_path, monkeypatch):
    """APSIM tests write their .apsimx/.met outputs in the current directory.

    Run them from a temporary directory so they never litter the repository.
    """
    if "apsim" in Path(str(request.node.fspath)).parts:
        monkeypatch.chdir(tmp_path)
