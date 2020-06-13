'''
behavior test for old_snet.
This test is written to cope with DB schema changing.
'''
from pathlib import Path
import shutil

from pypika import Table
from parse import parse
import pytest

from dw.data_source import old_snet
from dw import db
from dw import common
from dw.tasks import generate


def test_program_behavior(conn, root, yaml):
    '''
    If you don't know how to pass args, run `python main.py log <testdb>`
    
    args: 
        conn: string 'id:pw@host:port/dbname' format.
        root: root directory path string of old snet dataset. (src)
        yaml: file path of train/valid/split specified yaml (legacy)
    '''
    conn_str = conn
    #### GIVEN #####################################################
    if not conn_str:
        pytest.skip('this tests old_snet.add_data(root, conn, yaml)')
    conn = db.connection(conn_str)
    if not conn:
        assert False, f'Invalid connection string:\n{conn}'
    if root is None or (not Path(root).exists()):
        assert False, f'Invalid root: {root}'
    if yaml is None or (not Path(yaml).exists()):
        assert False, f'Yaml file not exists: {yaml}'

    # Reinit
    db.run(db.DROP_ALL_QUERY, conn)
    schema = './dw/schema/szmc_0.1.0.sql' # TODO: use latest schema file finder(#39)
    with open(schema, 'r') as s:
        db.init(s.read(), conn)
        
    #### WHEN #####################################################
    # Add old_snet data
    result = old_snet.add_data(root, conn)
    assert result is None

    #--- THEN -----------------------------------------------------
    # Check properties of DB
    num_imgs = len(list(Path(root, 'image').glob('*')))
    
    num_files = db.count_rows(Table('file'), conn)
    assert num_files == 3 * num_imgs, 'DB has img, wk, rbk images: 3 * num_img'
    num_masks = db.count_rows(Table('mask'), conn)
    assert num_masks == 2 * num_imgs, 'DB has wk, rbk masks: 2 * num_img'
    num_datasets = db.count_rows(Table('dataset'), conn)
    assert num_datasets == 0, 'DB has no dataset now.'
    
    #### AND WHEN #################################################
    # Create old_snet dataset
    result = old_snet.create(yaml, conn)
    assert result is None
    
    #--- THEN -----------------------------------------------------
    # Check properties of DB
    num_datasets = db.count_rows(Table('dataset'), conn)
    assert num_datasets == 1
    num_relations = db.count_rows(Table('dataset_annotation'), conn)
    assert num_relations == 2 * num_imgs

    #### AND WHEN #################################################
    # Generate easy_only dataset
    dset = common.Dataset('old_snet', 'full')
    mask_dir_relpath = 'easy_only' #'tmp_dirpath' - IT SUCKS!!!!!
    mask_dir_abspath = generate.generate(
        conn, dset, 'easy_only', mask_dir_relpath)
    #--- THEN -----------------------------------------------------
    # It just check validity of code after schema change..
    # So no crash = success. Too many tests are not effective.
    # NOTE: In future more test could be needed.. but NOT now!
    assert num_imgs == len(list(Path(mask_dir_abspath).glob('*')))
    shutil.rmtree(mask_dir_abspath)
    
    #### AND WHEN #################################################
    # Export easy_only dataset
    # Export old_snet wk dataset
    # Export old_snet rbk dataset
    #--- THEN -----------------------------------------------------
    # Check properties of DB
    # Check properties of exported datasets
