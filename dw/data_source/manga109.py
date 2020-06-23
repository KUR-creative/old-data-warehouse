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
    # TODO: Rewrite this
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

    # Run query
    query = db.multi_query(file_query)
    db.run(query, connection)
    '''
    # Get images (path, title, no).
    relpath = F.partial(os.path.relpath, start=root)
    sorted_children = fp.pipe(fu.children, fu.human_sorted)
    title_dirpaths = sorted_children(Path(root, 'images'))
    stem = lambda p: Path(p).stem
    imgpaths = fp.go(
        title_dirpaths,
        fp.mapcat(sorted_children),
        fp.lmap(relpath)
    )
    multiplied_titles, nos = fp.go(
        imgpaths,
        fp.map(stem),
        fp.map(lambda s: s.rsplit('_', 1)),
        fp.map(fp.tup(lambda title, no: [title, int(no)])),
        fp.unzip
    )

    # Get metadata xml files
    titles = fp.lmap(stem, title_dirpaths)
    xmlseq = fp.go(
        Path(root, 'manga109-annotations'),
        sorted_children,
        fp.map(lambda p: Path(p).read_text())
    )
    
    # Run queries.
    #tab_name = ''
    query = db.multi_query(
        S.manga109_raw._.insert(
            *zip(multiplied_titles, nos, imgpaths)),
        S.manga109_xml._.insert(
            *zip(titles, xmlseq)),
        S.raw_table_root._.insert(
            'manga109_raw', root)
    )
    db.run(query, connection)
    '''
    
    # None means success.
