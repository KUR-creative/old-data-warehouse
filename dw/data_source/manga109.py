'''
Logics for Manga109 dataset

Manga109 dataset is directory of files.
root directory must be satisfy following structure.

root
├── images
│   ├── AisazuNihaIrarenai
│   │   ├── AisazuNihaIrarenai_0.jpg
│   │   ├── ...
│   │   └── AisazuNihaIrarenai_100.jpg
│   ├── AkkeraKanjinchou
│   ├── ...
│   └── YumeNoKayoiji
└── manga109-annotations
    ├── AisazuNihaIrarenai.xml
    ├── Akuhamu.xml
    ├── ...
    └── YumeNoKayoiji.xml
'''
import os
from pathlib import Path

import funcy as F
from pypika import Query

from dw import db
from dw import query as Q
from dw.utils import file_utils as fu
from dw.utils import fp, etc
from dw.schema import schema as S, Any
from dw.data_source import common


def is_valid(root):
    rdir = Path(root)
    idir = Path(root, 'images')
    adir = Path(root, 'manga109-annotations')
    if not (rdir.exists() and idir.exists() and adir.exists()):
        print('Manga109 root directory structure is invalid')
        return False
    
    stem = lambda p: Path(p).stem
    titles = fp.lmap(stem, fu.human_sorted(fu.children(idir)))
    xml_names = fp.lmap(stem, fu.human_sorted(fu.children(adir)))
    
    eq_len = (len(titles) == len(xml_names)) 
    if not eq_len:
        print('No title or annotation')
        return False
    
    eq_names = bool(sum(
        map(fp.equal, titles, xml_names)
    ))
    if not eq_names:
        print('Some xml names are mismatched')
        return False
    
    return True

def add_data(root, connection) -> Any:
    ''' Add Manga109 data to DB(connection) '''
    if not is_valid(root):
        return 'Invalid Manga109 dataset'

    # Insert file information
    img_abspaths = fu.descendants(Path(root, 'images'))
    xml_abspaths = fu.descendants(Path(root, 'manga109-annotations'))
    all_abspaths = img_abspaths + xml_abspaths
    
    relpath = F.partial(os.path.relpath, start=root)
    img_relpaths = fp.lmap(relpath, img_abspaths)
    xml_relpaths = fp.lmap(relpath, xml_abspaths)
    all_relpaths = img_relpaths + xml_relpaths
    
    img_uuids = etc.uuid4strs(len(img_abspaths))
    xml_uuids = etc.uuid4strs(len(xml_abspaths))
    all_uuids = img_uuids + xml_uuids
    
    file_query = Q.insert_files(
        all_uuids, all_relpaths, all_abspaths,
        'manga109', root
    )

    # Insert image information
    img_rows = common.full_sized_image_rows(
        img_uuids, img_abspaths)
    
    # Insert xml annotation type
    annotation_name = 'manga109xml'
    new_annotation_type_query = Q.insert_new_annotation_type(
        annotation_name,
        'xml file for a manga title',
        connection)
    
    # Build annotation_rows: [img_pk, xml_uuid]
    title2xml_uuid = F.zipdict(
        F.map(lambda p: Path(p).stem, xml_abspaths),
        xml_uuids)
    def title(img_path):
        return Path(img_path).parents[0].parts[-1] #m109 specific
    img_primary_keys = F.lmap(
        fp.tup(lambda inp, y,x, h,w, *_: (inp, y,x, h,w)),
        img_rows)
    img_xml_uuids = F.lmap(
        fp.pipe(title, title2xml_uuid), img_abspaths)
    annotation_rows = F.lmap(
        lambda img_pk, xml_uuid:
        (*img_pk, xml_uuid, annotation_name),
        img_primary_keys, img_xml_uuids)
        
    # Run query
    query = db.multi_query(
        file_query, 
        S.image._.insert(*img_rows),
        new_annotation_type_query,
        S.annotation._.insert(*annotation_rows)
    )
    db.run(query, connection)
    
    # None means success.
