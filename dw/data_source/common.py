'''
Common logic for data processing of modules in data_source

Query building functions may neend to be here... (See dw.query)
'''
from pathlib import Path
from typing import Union, List

import imagesize

from dw.utils import fp


def full_sized_image_rows(uuids: List[str],
                          abspaths: List[Union[Path, str]]):
    ''' 
    Return rows for image table. 
    Use this when all input images are full sized. (not crops)
    '''
    sizeseq = fp.map(
        fp.pipe(
            imagesize.get,
            fp.tup(lambda w,h: (0,0, h,w, h,w))
        ),
        abspaths
    )
    return fp.lmap(
        lambda uuid, size_info: (uuid, *size_info),
        uuids, sizeseq)
