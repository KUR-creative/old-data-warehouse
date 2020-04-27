'''
Logics for old snet dataset

old snet dataset is directory of files.
root direcory must be satisfy following structure.
'''

import os
from pathlib import Path
import json
import uuid

import funcy as F
from pypika import Table, Query
#from pypika import functions as fn

from dw.utils import file_utils as fu
from dw.utils import fp, etc
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
        print('Some file names are mismatched')
        return False
    
    return True

def save(root, connection):
    ''' Save old snet dataset(root) to DB(connection) '''
    if not is_valid(root):
        return 'Invalid old snet dataset'
    print(is_valid(root))

    root_dir = Path(root)
    #map_json = Path(root, 'map.json')
    
    # Get {img,rbk,wk} paths.
    relpath = F.partial(os.path.relpath, start=root)
    sorted_children = fp.pipe(fu.children, fu.human_sorted)
    
    img_abspaths = sorted_children(Path(root, 'image'))
    rbk_abspaths = sorted_children(Path(root, 'clean_rbk'))
    wk_abspaths = sorted_children(Path(root, 'clean_wk'))
    img_relpaths = fp.lmap(relpath, img_abspaths)
    rbk_relpaths = fp.lmap(relpath, rbk_abspaths)
    wk_relpaths = fp.lmap(relpath, wk_abspaths)
    
    # Generate uuids.
    def uuids(length):
        return list(F.repeatedly(
            lambda: str(uuid.uuid4()), length))
    img_uuids = uuids(len(img_abspaths))
    rbk_uuids = uuids(len(rbk_abspaths))
    wk_uuids = uuids(len(wk_abspaths))
    
    all_uuids = img_uuids + rbk_uuids + wk_uuids
    abspaths = img_abspaths + rbk_abspaths + wk_abspaths
    relpaths = fp.lmap(relpath, abspaths)
    
    # Run queries.
    old_snet, rbk, wk = 'old_snet', 'rbk', 'wk'
    query = db.multi_query(
        Table('file_source').insert(
            old_snet, str(root_dir), etc.host_ip()),
        Query.into('file')
            .columns('uuid', 'source', 'relpath', 'abspath')
            .insert(*zip(
                all_uuids, F.repeat(old_snet),
                abspaths, relpaths
            )),
        Table('image').insert(
            *fp.map(lambda x: (x,), img_uuids)),
        Table('mask_scheme').insert(
            (rbk, 'red, blue, black 3 class dataset'),
            ( wk, 'white, black 2 class dataset')),
        Table('mask_scheme_content').insert(
            (rbk, '#FF0000', 'easy text'), 
            (rbk, '#0000FF', 'hard text'), 
            (rbk, '#000000', 'background'),
            ( wk, '#FFFFFF', 'text'), 
            ( wk, '#000000', 'background')), 
        Table('mask').insert(*F.concat(
            zip(rbk_uuids, F.repeat(rbk)),
            zip(wk_uuids, F.repeat(wk))
        ))
    )
    db.run(query, *connection)

    #TODO: Validation(Fact check).
    # count(image) + count(mask) = count(file)
    # count(rbk) = count(wk)
    # rbk mask scheme then path has 'rbk'
    # wk mask scheme then path has 'wk'
    
    # None means success.
