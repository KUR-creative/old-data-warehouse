'''
Logics for old snet dataset

old snet dataset is directory of files.
root direcory must be satisfy following structure.
'''

import os
from pathlib import Path
import json

import funcy as F
from pypika import Query
import yaml
import imagesize
#from pypika import functions as fn

from dw import db
from dw import query as Q
from dw.utils import file_utils as fu
from dw.utils import fp, etc
from dw.schema import schema as S, Any


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

def add_data(root, connection) -> Any:
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
    all_abspaths = img_abspaths + rbk_abspaths + wk_abspaths
    all_relpaths = fp.lmap(relpath, all_abspaths)
    
    file_query = Q.insert_files(
        all_uuids, all_relpaths, all_abspaths,
        'old_snet', root_dir
    )
    
    # othes
    img_sizeseq = fp.map(
        fp.pipe(
            imagesize.get,
            fp.tup(lambda w,h: (0,0, h,w, h,w))
        ),
        img_abspaths
    )
    img_rows = fp.lmap(
        lambda uuid, size_info: (uuid, *size_info),
        img_uuids, img_sizeseq)
    
    output_rows = F.lconcat(
        zip(rbk_uuids, F.repeat('mask')),
        zip(wk_uuids, F.repeat('mask')),
    )               
    annotation_rows = F.lmap(
        lambda img_info, out_info: img_info[:5] + out_info,
        img_rows * 2, output_rows)#uuid,y,x,h,w          
    
    # Has db 'mask' type?
    annotation_type = S.annotation_type._
    has_mask_type = db.contains(
        annotation_type, 'name', 'mask', connection)
    
    # Run queries.
    query = db.multi_query(
        file_query,
        # Add images
        S.image._.insert(*img_rows),
        # Add masks
        S.mask_scheme._.insert(
            ('rbk', 'red, blue, black 3 class dataset'),
            ( 'wk', 'white, black 2 class dataset')),
        S.mask_scheme_content._.insert(
            ('rbk', '#FF0000', 'easy text'), 
            ('rbk', '#0000FF', 'hard text'), 
            ('rbk', '#000000', 'background'),
            ( 'wk', '#FFFFFF', 'text'), 
            ( 'wk', '#000000', 'background')), 
        S.mask._.insert(*F.concat(
            zip(rbk_uuids, F.repeat('rbk')),
            zip(wk_uuids, F.repeat('wk'))
        )),
        # Add annotation relation
        annotation_type.insert(
            ('mask', 'image that has same height,width of input')
        ) if not has_mask_type else '',
        S.annotation._.insert(*annotation_rows)
    )
    db.run(query, connection)
    
    #TODO: Validation(Fact check).
    # count(image) + count(mask) = count(file)
    # count(rbk) = count(wk)
    # rbk mask scheme then path has 'rbk'
    # wk mask scheme then path has 'wk'
    
    # None means success.

def is_valid_yaml(split_yaml_path):
    return True

def create(split_yaml, connection) -> Any:
    ''' Create old snet dataset from old snet data in db(connection) '''
    if not is_valid_yaml(split_yaml):
        return 'Invalid split yaml'

    with open(split_yaml) as f:
        ids = yaml.safe_load(f.read())
        
    # Get data from DB
    rbk_rows, wk_rows = F.lsplit(
        lambda row: row['scheme'] == 'rbk',
        db.get(
            Query.from_(S.annotation._)
                 .from_(S.mask._).from_(S.file._)
                 .select(S.annotation.input,
                         S.annotation.output,
                         S.file.relpath,
                         S.mask.scheme)
                 .where(S.annotation.output == S.mask.uuid)
                 .where(S.mask.uuid == S.file.uuid),
            connection
        )
    )
    
    # Group rows by rbk/wk, train/valid/test
    num_train = len(ids['train'])
    num_valid = len(ids['valid'])
    num_test  = len(ids['test'])
    
    def where(row):
        id = int(Path(row['relpath']).stem)
        return('train' if id in ids['train']
          else 'valid' if id in ids['valid'] else 'test')
    rbk = F.group_by(where, rbk_rows)
    wk = F.group_by(where, wk_rows)

    assert len(rbk['train']) == len(wk['train']) == num_train
    assert len(rbk['valid']) == len(wk['valid']) == num_valid
    assert len(rbk['test'])  == len(wk['test'])  == num_test
    
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
        S.dataset._.insert(
            *dset_info, description
        ),
        S.dataset_annotation._.insert(*F.concat(
            dset_anno_rows(dset_info, rbk, 'train'),
            dset_anno_rows(dset_info, rbk, 'valid'),
            dset_anno_rows(dset_info, rbk, 'test'),
            dset_anno_rows(dset_info, wk, 'train'),
            dset_anno_rows(dset_info, wk, 'valid'),
            dset_anno_rows(dset_info, wk, 'test')
        ))
    )

    db.run(query, connection)


    # None means success.
