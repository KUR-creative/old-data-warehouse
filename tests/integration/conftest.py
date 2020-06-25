from pytest import fixture

def pytest_addoption(parser):
    parser.addoption("--snet_root", action="store", default=None)
    parser.addoption("--m109_root", action="store", default=None)
    parser.addoption("--conn", action="store", default=None)
    parser.addoption("--yaml", action="store", default=None)

@fixture()
def snet_root(request): return request.config.getoption("--snet_root")
@fixture()
def m109_root(request): return request.config.getoption("--m109_root")
@fixture()
def conn(request): return request.config.getoption("--conn")
@fixture()
def yaml(request): return request.config.getoption("--yaml")
