import psycopg2 as pg
import records
from pypika import Query, Table
from pypika import functions as fn

DROP_ALL_QUERY = 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'

def count_rows(table: Table, user, pw, host, port, dbname, where=None):
    query =(table.select(fn.Count('*')).where(where) if where
       else table.select(fn.Count('*')))
    return get(query, user, pw, host, port, dbname)[0]['count']

#-----------------------------------------------------------------------------
def init(schema, user, pw, host, port, dbname):
    with pg.connect(user=user, password=pw, host=host, dbname=dbname) as conn:
        with conn.cursor() as cursor:
            cursor.execute(schema)
    return 'Init success'

def get(query, user, pw, host, port, dbname):
    ''' No side effect! Just read data from DB. 
    Get data with python-records'''
    # TODO: Cacheing and refactoring arg signature
    connection = f'{user}:{pw}@{host}:{port}/{dbname}'
    conn_url = f'postgresql+psycopg2://{connection}'
    db = records.Database(conn_url)
    return db.query(str(query))

def get_pg(query, user, pw, host, port, dbname):
    ''' Get data without python-records. It sucks... 
    It only work with postgresql db.''' 
    with pg.connect(user=user, password=pw, host=host, port=port, dbname=dbname) as conn:
        with conn.cursor() as cursor:
            cursor.execute(str(query))
            return cursor.fetchall()

def run(query, user, pw, host, port, dbname):
    ''' Query(or Execution) to Side effect. '''
    with pg.connect(user=user, password=pw, host=host, port=port, dbname=dbname) as conn:
        with conn.cursor() as cursor:
            return cursor.execute(str(query))

def multi_query(*queries):
    ''' 
    Queries is list<pypika.queries.QueryBuilder> 
    if query is empty string(''), then it will skip.
    '''
    return ';'.join(map(str, queries))
