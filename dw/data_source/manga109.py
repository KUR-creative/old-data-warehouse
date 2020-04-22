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
from pathlib import Path
from dw.utils import file_utils as fu
from dw.utils import fp

def is_valid(root):
    rdir = Path(root)
    idir = Path(root, 'images')
    adir = Path(root, 'manga109-annotations')
    if not (rdir.exists() and idir.exists() and adir.exists()):
        print('Manga109 root directory structure is invalid')
        return False
    
    file_name = lambda p: Path(p).stem
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
