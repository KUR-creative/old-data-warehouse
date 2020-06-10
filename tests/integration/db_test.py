from pathlib import Path

from parse import parse
import pytest

from dw.data_source import old_snet
from dw import db

#@pytest.fixture()
def test_old_snet_add_data(root, conn):
    if not conn:
        pytest.skip('this tests old_snet.add_data(root, conn)')
        
    # TODO: check integrity of connection string(with refactoring cli)
    db_parsed = parse('{}:{}@{}:{}/{}', conn)
    if not db_parsed:
        assert False, f'Invalid connection string:\n{conn}'
        
    if root is None or (not Path(root).exists()):
        assert False, f'Invalid root: {root}'

    print(root)
    print(conn)
    
    # REINIT
    db.run(db.DROP_ALL_QUERY, *db_parsed)
    schema = './dw/schema/szmc_0.1.0.sql' # TODO: use latest schema file finder(#39)
    with open(schema, 'r') as s:
        db.init(s.read(), *db_parsed)
        
    # Add 
    result = old_snet.add_data(root, db_parsed)
    assert result is None

    # Check properties of DB: What props to check? Think it!
    num_imgs = len(list(Path(root, 'image').glob('*')))
    print(len(num_imgs))
    assert False
    # row-count(file) = 3 * count(img in snet285/image): img, rbk, wk
    # no row in dataset
    # 2 row in executed_command
    
#def test_(conn): assert conn == 'db'
