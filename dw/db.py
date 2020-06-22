from collections import namedtuple

import psycopg2 as pg
import records
from pypika import Table
from pypika import functions as fn
from parse import parse

from dw.utils import fp


#-----------------------------------------------------------------------------
Connection = namedtuple(
    'Connection', 'user password host port dbname'
)

def connection(conn_str):
    parsed = parse('{}:{}@{}:{}/{}', conn_str)
    return Connection(*parsed) if parsed else None
    
#-----------------------------------------------------------------------------
DROP_ALL_QUERY = 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'

#-----------------------------------------------------------------------------
def count_rows(table:Table, conn:Connection, where=None):
    query =(table.select(fn.Count('*')).where(where) if where
       else table.select(fn.Count('*')))
    return get(query, conn)[0]['count']

def contains(table, prop, query_val, conn:Connection):
    ''' 
    Does table contains table.prop == query_val?
    
    Convenient function for set-like tables
    (ex: mask_scheme, annotation_type, ...)
    '''
    # Check table type
    table =(table if type(table) == Table else
            Table(table) if type(table) == str else
            None)
    if table is None:
        assert False, 'table type must be pypika.Table or str'
            
    dics = get(
        table.select('*').where(
            fp.prop(prop, table) == query_val),
        conn
    ).as_dict()

    return (len(dics) > 0)
    
def reinit(conn:Connection, schema_path):
    global DROP_ALL_QUERY
    run(DROP_ALL_QUERY, conn)
    with open(schema_path, 'r') as s:
        init(s.read(), conn)
#-----------------------------------------------------------------------------
def multi_query(*queries):
    ''' 
    Queries is list<pypika.queries.QueryBuilder> 
    if query is empty string(''), then it will skip.
    '''
    return ';'.join(map(str, queries))

def init(schema:str, conn:Connection): # TODO: namedtuple is type? I dunno
    with pg.connect(**conn._asdict()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema)
    return 'Init success'

def get(query:str, conn:Connection):
    ''' No side effect! Just read data from DB. 
    Get data with python-records'''
    # TODO: Cacheing and refactoring arg signature
    conn_str = '{user}:{password}@{host}:{port}/{dbname}'.format(
        **conn._asdict())
    db = records.Database('postgresql+psycopg2://' + conn_str)
    return db.query(str(query))

def get_pg(query:str, conn:Connection):
    ''' 
    Get data WITHOUT python-records.
    
    It is needed because of limitation of python-records. 
    (maybe problem with duplicated column name)
    
    It only work with postgresql db.
    ''' 
    with pg.connect(**conn._asdict()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(str(query))
            return cursor.fetchall()

def run(query:str, conn:Connection):
    ''' Query(or Execution) to Side effect. '''
    with pg.connect(**conn._asdict()) as connection:
        with connection.cursor() as cursor:
            return cursor.execute(str(query))
