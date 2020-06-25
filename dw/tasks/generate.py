import os
from pathlib import Path

import cv2
from pypika import Query, Order
import funcy as F
from tqdm import tqdm

from dw import common
from dw import query as Q
from dw.utils import fp, etc
from dw import db
from dw.schema import schema as S, Any


@fp.multi
def generate(connection, src_dataset, out_form, option):
    return src_dataset, out_form, option

@fp.mmethod(generate, (common.Dataset('old_snet', 'full'), 'easy_only', 'easy_only')) # type: ignore[no-redef]
def generate(connection, src_dataset, out_form, option): 
    ''' train / valid / test not specified: means getting biggest dataset '''
    return generate_snet_easy(connection, src_dataset, out_form, option)

def generate_snet_easy(connection, src_dataset, out_form, mask_dir_relpath) -> Any:
    '''
    Generate snet easy_only dataset from DB
    1. Get data from Db.
    2. Generate target data from 1.data.
    3. save Generated data to file system and DB.
    
    WARNING: 
    In szmc-0.1.0, input-annotation relation is 
    {input=(uuid, y, x, h, w) : (uuid)=output}. 
    But this function is defined based on relation: {input_uuid : output_uuid}
    This assumtion could be PROBLEMATIC when INPUT IS NOT WHOLE IMAGE.
    BUT not now..
    If you want to make input as crop of image, change `dataset_annotation` schema first.
    
    return: path of directory that contains easy-only mask files
    '''
    #-----------------------------------------------------------------------
    # 1. Get data from DB
    #-----------------------------------------------------------------------
    # Get biggest dataset
    train, valid, test = 'train', 'valid', 'test'
    tvt = db.get(
        S.dataset._
            .select('train', 'valid', 'test')
            .orderby('train', 'valid', 'test', order=Order.desc),
        connection
    )[0].as_dict()

    # Get root path of file source of biggest dataset
    file_source = S.file_source._
    root = db.get(
        file_source
            .select('root_path')
            .where(file_source.name == src_dataset.name),
        connection
    )[0]['root_path']
    
    # Get mask paths of 'rbk' dataset
    dataset_annotation = S.dataset_annotation._
    mask_file = S.file._.as_('mask_file')
    mask_row = S.mask._
    rows = db.get(
        Query.from_(mask_file).from_(dataset_annotation)
             .from_(mask_row).from_(S.image._)
             .select(
                 dataset_annotation.input,
                 #dataset_annotation.y, dataset_annotation.x, dataset_annotation.h, dataset_annotation.w,
                 S.image.y, S.image.x,
                 S.image.h, S.image.w,
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
                 # image
                 (dataset_annotation.input == S.image.uuid) &
                 # others
                 (dataset_annotation.output == mask_file.uuid) &
                 (dataset_annotation.output == mask_row.uuid) &
                 (mask_row.scheme == 'rbk')),
        connection)

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

    # Add new scheme (If already defined, it just be skipped).
    db.run(
        Q.insert_new_mask_scheme(
            'easy_only',
            'white, black 2class, easy-text only dataset',
            connection,
            ('easy_only', '#FFFFFF', 'text'),
            ('easy_only', '#000000', 'background')
        ),
        connection
    )

    # Save generated mask files, mask, annotation, dataset_annotation
    # This procedure similar to 'add' command, but image source is implicit.
    mask_uuids = etc.uuid4strs(len(abspaths))
    annotation_rows = fp.map(
        lambda row, out_uuid: 
        (str(row.input), row.y, row.x, row.h, row.w, out_uuid, 'mask'),
        rows, mask_uuids)
    insert_masks_query = db.multi_query(
        Query.into(S.file._)
            .columns(S.file.uuid, S.file.source,
                     S.file.relpath, S.file.abspath)
            .insert(*zip(
                mask_uuids, F.repeat(src_dataset.name),
                relpaths, abspaths
            )),
        S.mask._.insert(*zip(
            mask_uuids, F.repeat('easy_only')
        )),
        S.annotation._.insert(*annotation_rows)
    )
    
    # Save easy_only dataset
    # This procedure similar to 'create' command, but split is implicit.
    num_train = len(fp.lfilter(lambda r: r.usage == 'train', rows))
    num_valid = len(fp.lfilter(lambda r: r.usage == 'valid', rows))
    num_test = len(fp.lfilter(lambda r: r.usage == 'test', rows))
    dset_info = [
        src_dataset.name, 'easy_only', num_train, num_valid, num_test]
    description =(
        'old snet easy only dataset. split=easy_only는 '
      + 'easy text만 포함하는 데이터라는 뜻')
    insert_dataset_query = db.multi_query(
        S.dataset._.insert(
            *dset_info, description
        ),
        S.dataset_annotation._.insert(*fp.map(
            lambda row, output: (
                *dset_info, str(row.input), output, row.usage
            ),
            rows, mask_uuids
        ))
    )
    
    db.run(db.multi_query(
        insert_masks_query, insert_dataset_query
    ), connection)

    return str(mask_dirpath)
