import pytest

#@pytest.fixture()
def test_sk(conn):
    if conn is None: pytest.skip('db test')
    assert conn == 'db'
    
#def test_(conn): assert conn == 'db'
