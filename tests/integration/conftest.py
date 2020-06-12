from pytest import fixture

def pytest_addoption(parser):
    parser.addoption("--root", action="store", default=None)
    parser.addoption("--conn", action="store", default=None)
    parser.addoption("--yaml", action="store", default=None)

@fixture()
def root(request): return request.config.getoption("--root")
@fixture()
def conn(request): return request.config.getoption("--conn")
@fixture()
def yaml(request): return request.config.getoption("--yaml")
