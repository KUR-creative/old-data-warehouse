'''
behavior test for manga109.
This test is written to add manga109 data.
'''
from pathlib import Path

import pytest

from dw import db
from dw.utils import file_utils as fu
from dw.data_source import manga109
from dw.schema import schema as S, Any
from dw.schema.gen_schema import latest as latest_schema

def test_program_behavior(conn, m109_root) -> Any:
    '''
    args: 
        conn: string 'id:pw@host:port/dbname' format.
        snet_root: root directory path string of old snet dataset. (src)
    '''
    conn_str = conn
    root = m109_root
    #### GIVEN #####################################################
    if not conn_str or root is None:
        pytest.skip('this tests manga109.add_data(root, conn)')
    conn = db.connection(conn_str)
    if not conn:
        assert False, f'Invalid connection string:\n{conn}'
    if not Path(root).exists():
        assert False, f'Invalid root: {root}'
    db.reinit(conn, latest_schema())

    #### WHEN #####################################################
    # Add manga109 data
    result = manga109.add_data(root, conn)
    assert result is None
    
    #### THEN #####################################################
    # number of added files are same to file system.
    num_imgs = len(fu.descendants(Path(root) / 'images'))
    num_xmls = len(fu.descendants(Path(root) / 'manga109-annotations'))
    num_files = num_imgs + num_xmls
    num_rows = db.count_rows(S.file._, conn)
    assert num_files == num_rows
