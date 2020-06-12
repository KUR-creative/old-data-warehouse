'''
Logics for old snet dataset

old snet dataset is directory of files.
root direcory must be satisfy following structure.
'''

import os
from pathlib import Path
import json

import funcy as F
from pypika import Table, Query
import yaml
#from pypika import functions as fn

from dw.utils import file_utils as fu
from dw.utils import fp, etc
from dw import db

def is_valid_directory(root):
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

def add_data(root, connection):
    ''' Add old snet data to DB(connection) '''
    if not is_valid_directory(root):
        return 'Invalid old snet dataset'

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
    
    img_uuids = etc.uuid4strs(len(img_abspaths))
    rbk_uuids = etc.uuid4strs(len(rbk_abspaths))
    wk_uuids = etc.uuid4strs(len(wk_abspaths))
    
    all_uuids = img_uuids + rbk_uuids + wk_uuids
    abspaths = img_abspaths + rbk_abspaths + wk_abspaths
    relpaths = fp.lmap(relpath, abspaths)
    
    # Has db 'mask' type?
    mask = 'mask'
    tab_annotation_type = Table('annotation_type')
    new_annotation_type = (db.count_rows(
        tab_annotation_type, *connection,
        tab_annotation_type.name == mask
    ) == 0)
    
    # Run queries.
    old_snet, rbk, wk = 'old_snet', 'rbk', 'wk'
    query = db.multi_query(
        Table('file_source').insert(
            old_snet, str(root_dir), etc.host_ip()),
        Query.into('file')
            .columns('uuid', 'source', 'relpath', 'abspath')
            .insert(*zip(
                all_uuids, F.repeat(old_snet),
                relpaths, abspaths
            )),
        # Add images
        Table('image').insert(
            *fp.map(lambda x: (x,), img_uuids)),
        # Add masks
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
        )),
        # Add annotation relation
        tab_annotation_type.insert(
            (mask, 'image that has same height,width of input')
        ) if new_annotation_type else '',
        Table('annotation').insert(*F.concat(
            zip(img_uuids, rbk_uuids, F.repeat(mask)),
            zip(img_uuids, wk_uuids, F.repeat(mask)),
        ))
    )
    db.run(query, *connection)

    #TODO: Validation(Fact check).
    # count(image) + count(mask) = count(file)
    # count(rbk) = count(wk)
    # rbk mask scheme then path has 'rbk'
    # wk mask scheme then path has 'wk'
    
    # None means success.

def is_valid_yaml(split_yaml_path):
    return True

def create(split_yaml, connection):
    ''' Create old snet dataset from old snet data in db(connection) '''
    if not is_valid_yaml(split_yaml):
        return 'Invalid split yaml'

    with open(split_yaml) as f:
        ids = yaml.safe_load(f.read())
        
    # Get data from DB
    annotation = Table('annotation')
    file, mask = Table('file'), Table('mask')
    rbk_rows, wk_rows = F.lsplit(
        lambda row: row['scheme'] == 'rbk',
        db.get(
            Query.from_(annotation)
                 .from_(mask).from_(file)
                 .select(annotation.input,
                         annotation.output,
                         file.relpath,
                         mask.scheme)
                 .where(annotation.output == mask.uuid)
                 .where(mask.uuid == file.uuid),
            *connection
        )
    )
    
    # Group rows by rbk/wk, train/valid/test
    train, valid, test = 'train', 'valid', 'test'
    num_train = len(ids[train])
    num_valid = len(ids[valid])
    num_test  = len(ids[test])
    
    def where(row):
        id = int(Path(row['relpath']).stem)
        return(train if id in ids[train]
          else valid if id in ids[valid] else test)
    rbk = F.group_by(where, rbk_rows)
    wk = F.group_by(where, wk_rows)

    assert len(rbk[train]) == len(wk[train]) == num_train
    assert len(rbk[valid]) == len(wk[valid]) == num_valid
    assert len(rbk[test])  == len(wk[test])  == num_test
    
    # Build rows of dataset_annotation relation.
    dset_info = [
        'old_snet', 'full', num_train, num_valid, num_test]
    def dset_anno_rows(dset_info, row_dict, usage):
        return fp.lmap(
            lambda row: (
                *dset_info,
                str(row['input']),
                str(row['output']),
                usage
            ),
            row_dict[usage]
        )

    # Build query.
    description =(
        'Old snet dataset. 동일한 데이터에 어노테이션만 다른 '
      + 'rbk / wk 데이터를 가지고 있음. split=full은 easy, hard '
      + '모두 포함하는 데이터라는 뜻')
    query = db.multi_query(
        Table('dataset').insert(
            *dset_info, description
        ),
        Table('dataset_annotation').insert(*F.concat(
            dset_anno_rows(dset_info, rbk, train),
            dset_anno_rows(dset_info, rbk, valid),
            dset_anno_rows(dset_info, rbk, test),
            dset_anno_rows(dset_info, wk, train),
            dset_anno_rows(dset_info, wk, valid),
            dset_anno_rows(dset_info, wk, test)
        ))
    )

    db.run(query, *connection)

    # None means success.
