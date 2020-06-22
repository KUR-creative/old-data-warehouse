'''
behavior test for old_snet.
This test is written to cope with DB schema changing.
'''
from pathlib import Path
import shutil

from parse import parse
import pytest

from dw.data_source import old_snet
from dw import db
from dw import common
from dw.tasks import generate
from dw.schema import schema as S, Any
from dw.schema.gen_schema import latest as latest_schema


def test_program_behavior(conn, snet_root, yaml) -> Any:
    '''
    If you don't know how to pass args, run `python main.py log <testdb>`
    
    args: 
        conn: string 'id:pw@host:port/dbname' format.
        snet_root: root directory path string of old snet dataset. (src)
        yaml: file path of train/valid/split specified yaml (legacy)
    '''
    conn_str = conn
    root = snet_root
    #### GIVEN #####################################################
    if not conn_str or root is None:
        pytest.skip('this tests old_snet.add_data(root, conn, yaml)')
    conn = db.connection(conn_str)
    if not conn:
        assert False, f'Invalid connection string:\n{conn}'
    if (not Path(root).exists()):
        assert False, f'Invalid root: {root}'
    if yaml is None or (not Path(yaml).exists()):
        assert False, f'Yaml file not exists: {yaml}'

    db.reinit(conn, latest_schema())
        
    #### WHEN #####################################################
    # Add old_snet data
    result = old_snet.add_data(root, conn)
    assert result is None

    #--- THEN -----------------------------------------------------
    # Check properties of DB
    num_imgs = len(list(Path(root, 'image').glob('*')))
    
    num_files = db.count_rows(S.file._, conn)
    assert num_files == 3 * num_imgs, 'DB has img, wk, rbk images: 3 * num_img'
    num_masks = db.count_rows(S.mask._, conn)
    assert num_masks == 2 * num_imgs, 'DB has wk, rbk masks: 2 * num_img'
    num_datasets = db.count_rows(S.dataset._, conn)
    assert num_datasets == 0, 'DB has no dataset now.'
    
    #### AND WHEN #################################################
    # Create old_snet dataset
    result = old_snet.create(yaml, conn)
    assert result is None
    
    #--- THEN -----------------------------------------------------
    # Check properties of DB
    num_datasets = db.count_rows(S.dataset._, conn)
    assert num_datasets == 1
    num_relations = db.count_rows(S.dataset_annotation._, conn)
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
