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
from pypika import Table

from dw.utils import file_utils as fu
from dw.utils import fp
from dw import db


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

def add_data(root, connection):
    ''' Add Manga109 data to DB(connection) '''
    # TODO: Rewrite this
    if not is_valid(root):
        return 'Invalid Manga109 dataset'

    return None
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
    tab_name = 'manga109_raw'
    query = db.multi_query(
        Table(tab_name).insert(
            *zip(multiplied_titles, nos, imgpaths)),
        Table('manga109_xml').insert(
            *zip(titles, xmlseq)),
        Table('raw_table_root').insert(
            tab_name, root)
    )
    db.run(query, *connection)
    
    # None means success.
