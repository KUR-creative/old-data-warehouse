from pathlib import Path

from pypika import Table
from parse import parse
import pytest

from dw.data_source import old_snet
from dw import db


def test_old_snet_add_data(conn, root, yaml):
    '''
    If you don't know how to pass args, run `python main.py log <testdb>`
    
    args: 
        conn: string 'id:pw@host:port/dbname' format.
        root: root directory path string of old snet dataset. (src)
        yaml: file path of train/valid/split specified yaml (legacy)
    '''
    #### GIVEN #####################################################
    if not conn:
        pytest.skip('this tests old_snet.add_data(root, conn, yaml)')
    db_parsed = parse('{}:{}@{}:{}/{}', conn) # TODO: check integrity of connection string(with refactoring cli)
    if not db_parsed:
        assert False, f'Invalid connection string:\n{conn}'
    if root is None or (not Path(root).exists()):
        assert False, f'Invalid root: {root}'
    if yaml is None or (not Path(yaml).exists()):
        assert False, f'Yaml file not exists: {yaml}'

    # Reinit
    db.run(db.DROP_ALL_QUERY, *db_parsed)
    schema = './dw/schema/szmc_0.1.0.sql' # TODO: use latest schema file finder(#39)
    with open(schema, 'r') as s:
        db.init(s.read(), *db_parsed)
        
    #### WHEN #####################################################
    # Add old_snet data
    result = old_snet.add_data(root, db_parsed)
    assert result is None

    #--- THEN -----------------------------------------------------
    # Check properties of DB
    num_imgs = len(list(Path(root, 'image').glob('*')))
    
    num_files = db.count_rows(Table('file'), *db_parsed)
    assert num_files == 3 * num_imgs, 'DB has img, wk, rbk images: 3 * num_img'
    num_masks = db.count_rows(Table('mask'), *db_parsed)
    assert num_masks == 2 * num_imgs, 'DB has wk, rbk masks: 2 * num_img'
    num_datasets = db.count_rows(Table('dataset'), *db_parsed)
    assert num_datasets == 0, 'DB has no dataset now.'
    
    #### AND WHEN #################################################
    # Create old_snet dataset
    result = old_snet.create(yaml, db_parsed)
    assert result is None
    
    #--- THEN -----------------------------------------------------
    # Check properties of DB
    num_datasets = db.count_rows(Table('dataset'), *db_parsed)
    assert num_datasets == 1
    num_relations = db.count_rows(Table('dataset_annotation'), *db_parsed)
    assert num_relations == 2 * num_imgs
