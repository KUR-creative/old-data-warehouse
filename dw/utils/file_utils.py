'''
Utils for File Processing
'''
import os
import re
from pathlib import PurePosixPath, Path

import chardet
import funcy as F


def children(dirpath):
    ''' Return children file path list of `dirpath` '''
    parent = Path(dirpath)
    return list(map(
        lambda child_path: str(parent / child_path.name),
        parent.iterdir()
    ))

def descendants(root_dirpath):
    ''' Return descendants file path list of `root_dirpath` ''' 
    fpaths = []
    it = os.walk(root_dirpath)
    for root,dirs,files in it:
        for path in map(lambda name:PurePosixPath(root) / name,files):
            fpaths.append(str(path))
    return fpaths

@F.autocurry
def replace1(old, new, path):
    parts = list(Path(path).parts) # NOTE: because of set_in implementation..
    idx = parts.index(old)
    return str(Path(*F.set_in(parts, [idx], new)))

def human_sorted(iterable):
    ''' Sorts the given iterable in the way that is expected. '''
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(iterable, key = alphanum_key)

def write_text(path, text, mode=0o777, exist_ok=True):
    path = Path(PurePosixPath(path))
    os.makedirs(path.parent, mode, exist_ok)
    path.write_text(text)

def read_text(path, encoding=None, errors=None):
    with open(path, 'rb') as f:
        rawdata = f.read()
        encoding = chardet.detect(rawdata)['encoding']
        #print('path->',path)
        return Path(path).read_text(encoding=encoding, errors=errors)
