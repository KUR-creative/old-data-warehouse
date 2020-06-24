'''
Pre-defined query building functions
These codes can be changed according to schema changes.

Maybe I need better code partitioning..

'''
from typing import Union, List
from pathlib import Path

import funcy as F
from pypika import Query

from dw import db
from dw.utils import fp, etc
from dw.schema import schema as S, Any


def insert_files(all_uuids: List[str],
                 relpaths: List[Union[Path, str]],
                 abspaths: List[Union[Path, str]],
                 source_name: str,
                 root_dir: Union[Path, str, None] = None):
    '''
    Build inserting file information to file_source, file table.
    If root_dir = None, do not insert file_source info.
    It just used in dw.data_source.modules
    '''
    uuid, source, relpath, abspath = (
        S.file.uuid, S.file.source, S.file.relpath, S.file.abspath)
    return db.multi_query(
        S.file_source._.insert(
            source_name, str(root_dir), etc.host_ip()
        ) if root_dir is not None else '',
        Query.into(S.file._)
            .columns(uuid, source, relpath, abspath)
            .insert(*zip(
                all_uuids, F.repeat(source_name),
                relpaths, abspaths
            ))
    )
