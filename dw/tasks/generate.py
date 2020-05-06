import os
from pathlib import Path

import cv2
from pypika import Table, Query, Order
import funcy as F
from tqdm import tqdm

from dw import common
from dw.utils import fp
from dw import db


@fp.multi
def generate(connection, src_dataset, out_form, option):
    return src_dataset, out_form, option

@fp.mmethod(generate, (common.Dataset('old_snet', 'full'), 'easy_only', 'easy_only'))
def generate(connection, src_dataset, out_form, option):
    ''' train / valid / test not specified: means getting biggest dataset '''
    generate_snet_easy(connection, src_dataset, out_form, option)

def generate_snet_easy(connection, src_dataset, out_form, mask_dir_relpath):
    # Get biggest dataset
    train, valid, test = 'train', 'valid', 'test'
    tvt = db.get(
        Table('dataset')
            .select('train', 'valid', 'test')
            .orderby('train', 'valid', 'test', order=Order.desc),
        *connection
    )[0].as_dict()

    # Get root path of file source of biggest dataset
    file_source = Table('file_source')
    root = db.get(
        file_source
            .select('root_path')
            .where(file_source.name == src_dataset.name),
        *connection
    )[0]['root_path']
    
    # Get mask paths of 'rbk' dataset
    dataset_annotation = Table('dataset_annotation')
    mask_file = Table('file').as_('mask_file')
    mask_row = Table('mask')
    rows = db.get(
        Query.from_(mask_file).from_(dataset_annotation).from_(mask_row)
             .select(mask_file.abspath, mask_file.relpath, dataset_annotation.usage)
             .where(
                 # dataset
                 (dataset_annotation.name == src_dataset.name) &
                 (dataset_annotation.split == src_dataset.split) &
                 (dataset_annotation.train == tvt[train]) &
                 (dataset_annotation.valid == tvt[valid]) &
                 (dataset_annotation.test == tvt[test]) &
                 # others
                 (dataset_annotation.output == mask_file.uuid) &
                 (dataset_annotation.output == mask_row.uuid) &
                 (mask_row.scheme == 'rbk')),
        *connection)

    # Get red mask sequence
    red_maskseq = fp.map(
        fp.pipe(
            lambda row: row.abspath,
            cv2.imread, lambda mask: mask[:,:,2]),
        rows
    )

    # Get destination paths
    dstpaths = fp.lmap(
        fp.pipe(
            lambda r: r.relpath,
            lambda p: Path(p).name,
            lambda p: Path(root, mask_dir_relpath, p)),
        rows,
    )

    # Save easy only masks to destination paths
    os.makedirs(Path(root, mask_dir_relpath), exist_ok=True)
    print('Generate & Save easy text only masks...')
    for mask, dstpath in tqdm(zip(red_maskseq, dstpaths), total=len(dstpaths)):
        #cv2.imshow('mask',mask); cv2.waitKey(0)
        cv2.imwrite(str(dstpath), mask)
    print('Done!')

    # Save data to file, mask_scheme, mask_scheme_content,
    #   mask(easy_only scheme), snet_annotation, dataset_annotation
    #   TODO: This procedure can be extracted as function: 'save_annotations'
    #         Some parts of proc could be duplicated with data_source.save

    '''
    query = db.multi_query(
        Query.into('file')

    for r, rm, dp in zip(rows, red_maskseq, dst_paths):
        print(r['abspath'], dp)
    print(mask_dir_relpath)
    
    for r in rows: print(r)

    #for mp in red_maskseq: print(mp)
    for m in red_maskseq:
        cv2.imshow('m',m)
        cv2.waitKey(0)

    
    mask_paths = fp.group_by(lambda row: row['usage'], rows)
    mask_abspaths = fp.go(
        mask_paths,
        fp.walk_values(fp.lmap(lambda row: row.abspath)),
        dict
    )
    mask_relpaths = fp.go(
        mask_paths,
        fp.walk_values(fp.lmap(lambda row: row.relpath)),
        dict
    )

    from pprint import pprint
    #pprint(mask_paths)
    print(type(mask_paths))
    #pprint(mask_abspaths)
    #pprint(mask_relpaths)

    # Save to mask_dir_relpath/same_name_src_masks
    maskseq = fp.walk_values(fp.map(path2red_mask), mask_abspaths)
    '''
    
