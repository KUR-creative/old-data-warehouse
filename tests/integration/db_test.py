from pathlib import Path

from parse import parse
import pytest

from dw.data_source import old_snet
from dw.log import get_cli_cmds

#@pytest.fixture()
def test_old_snet_add_data(root, conn):
    if not conn:
        pytest.skip('this tests old_snet.add_data(root, conn)')
        
    # TODO: check integrity of connection string(with refactoring cli)
    db_parsed = parse('{}:{}@{}:{}/{}', conn)
    if not db_parsed:
        assert False, f'Invalid connection string:\n{conn}'
        
    if root is None:
        # It is just for convenient testing. It could be crashed.
        #root_parsed = parse('pythona main.py add old_snet {} <connection>',
        root_parsed = parse('python main.py add old_snet {} <connection>',
              get_cli_cmds(db_parsed, False)[1]['command'])
        root = root_parsed[0] if root_parsed else None
    if root is None or (not Path(root).exists()):
        assert False, f'Invalid root: {root}'

    print(root)
    print(conn)
    assert False
    #result = old_snet.add_data(root, db_parsed)
    
#def test_(conn): assert conn == 'db'
