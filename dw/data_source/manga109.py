'''
Logics for Manga109 dataset

Manga109 dataset is directory of files.
root direcory must be satisfy following structure.

root
├── images
│   ├── AisazuNihaIrarenai
│   │   ├── AisazuNihaIrarenai_0.jpg
│   │   ├── ...
│   │   └── AisazuNihaIrarenai_100.jpg
│   ├── AkkeraKanjinchou
│   ├── ...
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

from dw.utils import file_utils as fu
from dw.utils import fp

def file_name(path):
    return Path(path).stem

def is_valid(root):
    rdir = Path(root)
    idir = Path(root, 'images')
    adir = Path(root, 'manga109-annotations')
    if not (rdir.exists() and idir.exists() and adir.exists()):
        print('Manga109 root directory structure is invalid')
        return False
    
    titles = fp.lmap(file_name, fu.human_sorted(fu.children(idir)))
    xml_names = fp.lmap(file_name, fu.human_sorted(fu.children(adir)))
    
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

def save(root, connection):
    ''' Save Manga109 dataset(root) to DB(connection) '''
    if not is_valid(root):
        return 'Invalid Manga109 dataset'

    # get images (path, title, no).
    relpath = F.partial(os.path.relpath, start=root)
    sorted_children = fp.pipe(fu.children, fu.human_sorted)
    title_dirpaths = sorted_children(Path(root, 'images'))
    imgpaths = fp.go(
        title_dirpaths,
        fp.mapcat(sorted_children),
        fp.lmap(relpath)
    )
    titles, nos = fp.go(
        imgpaths,
        fp.map(file_name),
        fp.map(lambda s: s.rsplit('_', 1)),
        fp.map(fp.tup(lambda title, no: [title, int(no)])),
        fp.unzip
    )
    print(nos)
    print(titles)

    xmls = fp.go(
        Path(root, 'manga109-annotations'),
        sorted_children,
        #fp.lmap(lambda p: Path(p).read_text())
        fp.map(lambda p: Path(p).read_text())
    )
    #print(imgpaths, len(imgpaths))
    #print(xmls[0])
    '''
    print(fp.lmap(
        rel,
        fu.children(Path(root,'images', dirs[0]))))
    print()
    #print(os.listdir(Path(root,'manga109-annotations')))
    print(connection)
    '''
