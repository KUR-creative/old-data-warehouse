from pathlib import Path

from pypika import Table
from pypika import functions as fn
from parse import parse
import pytest

from dw.data_source import old_snet
from dw import db


def test_old_snet_add_data(root, conn):
    #### GIVEN #####################################################
    if not conn:
        pytest.skip('this tests old_snet.add_data(root, conn)')
    db_parsed = parse('{}:{}@{}:{}/{}', conn) # TODO: check integrity of connection string(with refactoring cli)
    if not db_parsed:
        assert False, f'Invalid connection string:\n{conn}'
    if root is None or (not Path(root).exists()):
        assert False, f'Invalid root: {root}'

    #### WHEN #####################################################
    # Reinit
    db.run(db.DROP_ALL_QUERY, *db_parsed)
    schema = './dw/schema/szmc_0.1.0.sql' # TODO: use latest schema file finder(#39)
    with open(schema, 'r') as s:
        db.init(s.read(), *db_parsed)
        
    # Add 
    result = old_snet.add_data(root, db_parsed)
    assert result is None

    #### THEN #####################################################
    # Check properties of DB
    num_imgs = len(list(Path(root, 'image').glob('*')))
    
    num_files = db.get(
        Table('file').select(fn.Count('*')), *db_parsed
    )[0]['count']
    assert num_files == 3 * num_imgs, 'DB has img, wk, rbk images: 3 * num_img'
    
    num_masks = db.get(
        Table('mask').select(fn.Count('*')), *db_parsed
    )[0]['count']
    assert num_masks == 2 * num_imgs, 'DB has wk, rbk masks: 2 * num_img'
    
    num_datasets = db.get(
        Table('dataset').select(fn.Count('*')), *db_parsed
    )[0]['count']
    assert num_datasets == 0, 'DB has no dataset now.'
