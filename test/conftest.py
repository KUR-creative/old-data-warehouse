import pytest

def pytest_addoption(parser):
    parser.addoption("--conn", action="store", default=None)

#@fixture()
@pytest.fixture(scope="module")
def conn(request): return request.config.getoption("--conn")
