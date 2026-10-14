import pytest


@pytest.fixture(autouse=True, scope="session")
def _outside_the_project(tmp_path_factory):
    """MLflow's SQLite store writes model files to ./mlruns, so tests run from a temp folder."""

    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(tmp_path_factory.mktemp("cwd"))
        yield
