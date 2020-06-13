import os
from pathlib import Path

import cv2
from pypika import Table, Query, Order
import funcy as F
from tqdm import tqdm

from dw import common
from dw.utils import fp, etc
from dw import db


@fp.multi
def generate(connection, src_dataset, out_form, option):
    return src_dataset, out_form, option

@fp.mmethod(generate, (common.Dataset('old_snet', 'full'), 'easy_only', 'easy_only'))
def generate(connection, src_dataset, out_form, option):
    ''' train / valid / test not specified: means getting biggest dataset '''
    return generate_snet_easy(connection, src_dataset, out_form, option)

def generate_snet_easy(connection, src_dataset, out_form, mask_dir_relpath):
    '''
    Generate snet easy_only dataset from DB
    1. Get data from Db.
    2. Generate target data from 1.data.
    3. save Generated data to file system and DB.
    
    return: path of directory that contains easy-only mask files
    '''
    #-----------------------------------------------------------------------
    # 1. Get data from Db
    #-----------------------------------------------------------------------
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
             .select(
                 dataset_annotation.input,
                 dataset_annotation.usage,
                 mask_file.abspath,
                 mask_file.relpath)
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

    #-----------------------------------------------------------------------
    # 2. Generate target data from 1.data
    #-----------------------------------------------------------------------
    # Get red mask sequence
    red_maskseq = fp.map(
        fp.pipe(
            lambda row: row.abspath,
            cv2.imread, lambda mask: mask[:,:,2]),
        rows
    )

    # Get destination paths
    relpaths = fp.lmap(
        fp.pipe(
            lambda row: row.relpath,
            lambda path: Path(path).name,
            lambda name: Path(mask_dir_relpath, name),
            str
        ),
        rows
    )
    abspaths = fp.lmap(
        lambda path: str(Path(root, path)), relpaths
    )

    #-----------------------------------------------------------------------
    # 3. save Generated data
    #-----------------------------------------------------------------------
    # Save easy only masks to destination paths
    mask_dirpath = Path(root, mask_dir_relpath)
    os.makedirs(mask_dirpath, exist_ok=True)
    print('Generate & Save easy text only masks...')
    for mask, dstpath in tqdm(zip(red_maskseq, abspaths), total=len(abspaths)):
        #cv2.imshow('mask',mask); cv2.waitKey(0) # look & feel check
        cv2.imwrite(str(dstpath), mask)
    print('Done!')

    # Has db easy only scheme?
    easy_only = 'easy_only'
    mask_scheme = Table('mask_scheme')
    has_easy_only_scheme = db.get(
        mask_scheme.select('*').where(mask_scheme.name == easy_only),
        *connection
    ).as_dict()
    
    # If not, add new mask scheme: easy_only
    if not has_easy_only_scheme:
        query = db.multi_query(
            Table('mask_scheme').insert(
                easy_only, 'white, black 2class, easy-text only dataset'),
            Table('mask_scheme_content').insert(
                (easy_only, '#FFFFFF', 'text'),
                (easy_only, '#000000', 'background')), 
        )
        db.run(query, *connection)

    # Save generated mask files, mask, annotation, dataset_annotation
    # This procedure similar to 'add' command, but image source is implicit.
    img_uuids = fp.lmap(lambda r: str(r.input), rows)
    mask_uuids = etc.uuid4strs(len(abspaths))
    insert_masks_query = db.multi_query(
        Query.into('file')
            .columns('uuid', 'source', 'relpath', 'abspath')
            .insert(*zip(
                mask_uuids, F.repeat(src_dataset.name), relpaths, abspaths
            )),
        Table('mask').insert(*zip(
            mask_uuids, F.repeat(easy_only)
        )),
        Table('annotation').insert(*zip(
            img_uuids, mask_uuids, F.repeat('mask')
        ))
    )
    
    # Save easy_only dataset
    # This procedure similar to 'create' command, but split is implicit.
    num_train = len(fp.lfilter(lambda r: r.usage == 'train', rows))
    num_valid = len(fp.lfilter(lambda r: r.usage == 'valid', rows))
    num_test = len(fp.lfilter(lambda r: r.usage == 'test', rows))
    dset_info = [
        src_dataset.name, easy_only, num_train, num_valid, num_test]
    description =(
        'old snet easy only dataset. split=easy_only는 '
      + 'easy text만 포함하는 데이터라는 뜻')
    insert_dataset_query = db.multi_query(
        Table('dataset').insert(
            *dset_info, description
        ),
        Table('dataset_annotation').insert(*fp.map(
            lambda row, output: (
                *dset_info, str(row.input), output, row.usage
            ),
            rows, mask_uuids
        ))
    )
    
    db.run(db.multi_query(
        insert_masks_query,
        insert_dataset_query
    ), *connection)

    return str(mask_dirpath)
