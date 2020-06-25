'''
Pre-defined query building functions
These codes can be changed according to schema changes.

Maybe I need better code partitioning..

'''
from typing import Union, List, Tuple
from pathlib import Path

import funcy as F
from pypika import Query, Table

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

def insert_new_annotation_type(name:str,
                               description:str,
                               conn:db.Connection):
    ''' If the type(annotation_type.name = name) already exists,
    it return empty string(''), else return db query string. '''
    return insert_if_not_exists(
        S.annotation_type._, name, description, conn)

def insert_new_mask_scheme(
        name:str,
        description:str,
        conn:db.Connection,
        *scheme_contents:Tuple[str, str, str]):
    ''' If the scheme(mask_scheme.name = name) already exists,
    it return empty string(''), else return db query string. '''
    new_scheme_query = insert_if_not_exists(
        S.mask_scheme._, name, description, conn)
    return db.multi_query(
        new_scheme_query,
        S.mask_scheme_content._.insert(*scheme_contents)
    ) if new_scheme_query else ''
        
    
def insert_if_not_exists(table:Table,
                         name:str,
                         description:str,
                         conn:db.Connection):
    ''' 
    If object(table.name = name) already exists in the table,
    it return empty string(''), else return db query string.
    
    This is possible because the schema of mask_scheme and
    annotation_type are the same...
    '''
    exists = db.contains(table, 'name', name, conn)
    return table.insert(
        (name, description)
    ) if not exists else ''
