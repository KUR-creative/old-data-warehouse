from pytest import fixture

def pytest_addoption(parser):
    parser.addoption(
        "--conn",
        action="store",
        default=None
    )

@fixture()
def conn(request):
    return request.config.getoption("--conn")
