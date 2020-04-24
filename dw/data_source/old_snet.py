'''
Logics for old snet dataset

old snet dataset is directory of files.
root direcory must be satisfy following structure.
'''

import os
from pathlib import Path
import json

import funcy as F
from pypika import Table

from dw.utils import file_utils as fu
from dw.utils import fp
from dw import db


def is_valid(root):
    root_dir = Path(root)
    img_dir = Path(root, 'image')
    rbk_dir = Path(root, 'clean_rbk')
    wk_dir = Path(root, 'clean_wk')
    map_json = Path(root, 'map.json')
    if not (root_dir.exists() and
            img_dir.exists() and
            rbk_dir.exists() and
            wk_dir.exists() and
            map_json):
        print('Old Snet root directory structure is invalid')
        return False
    
    stem = lambda p: Path(p).stem
    sorted_children = fp.pipe(fu.children, fu.human_sorted)
    img_stems = fp.lmap(stem, sorted_children(img_dir))
    rbk_stems = fp.lmap(stem, sorted_children(rbk_dir))
    wk_stems = fp.lmap(stem, sorted_children(wk_dir))
    ids = fp.go(
        map_json.read_text(),
        json.loads, fp.unzip, F.second, fp.lmap(stem)
    )
    
    eq_len = (
        len(img_stems) == len(rbk_stems) == len(wk_stems) == len(ids))
    if not eq_len:
        print('No title or annotation')
        return False
    
    eq_stems = bool(sum(
        map(fp.equal, img_stems, rbk_stems, wk_stems, ids)
    ))
    if not eq_stems:
        print('Some xml names are mismatched')
        return False
    
    return True

def save(root, connection):
    ''' Save old snet dataset(root) to DB(connection) '''
    if not is_valid(root):
        return 'Invalid old snet dataset'
    print(is_valid(root))

    root_dir = Path(root)
    img_dir = Path(root, 'image')
    rbk_dir = Path(root, 'clean_rbk')
    wk_dir = Path(root, 'clean_wk')
    map_json = Path(root, 'map.json')
    
    # Get {img,rbk,wk} paths
    relpath = F.partial(os.path.relpath, start=root)
    sorted_children = fp.pipe(fu.children, fu.human_sorted)
    
    img_paths = fp.lmap(relpath, sorted_children(img_dir))
    rbk_paths = fp.lmap(relpath, sorted_children(rbk_dir))
    wk_paths = fp.lmap(relpath, sorted_children(wk_dir))

    # Get img names. NOTE: map_json must be sorted in id
    names, ids = fp.go(
        map_json.read_text(),
        json.loads,
        fp.map(fp.lmap(lambda p: Path(p).stem)),
        fp.unzip
    )
    
    # Run queries.
    tab_name = 'old_snet_data_raw'
    query = db.multi_query(
        Table(tab_name).insert(
            *zip(ids, names, img_paths, rbk_paths, wk_paths)),
        Table('raw_table_root').insert(
            tab_name, root)
    )
    
    db.run(query, *connection)
    
    # None means success.
