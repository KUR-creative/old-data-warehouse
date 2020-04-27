import psycopg2 as pg
import records
from pypika import Query

def init(schema, user, pw, host, port, dbname):
    with pg.connect(user=user, password=pw, host=host, dbname=dbname) as conn:
        with conn.cursor() as cursor:
            cursor.execute(schema)
    return 'Init success'
    '''
    conn_str = f'postgresql+psycopg2://{connection}'
    db = records.Database(conn_str)
    sql = Query.from_('cars').select('*').get_sql()
    print(sql)
    rows = db.query(sql)
    for r in rows:
        print(r)
        print(r.as_dict())
    print(rows.all())
    '''

def run(query, user, pw, host, port, dbname):
    with pg.connect(user=user, password=pw, host=host, port=port, dbname=dbname) as conn:
        with conn.cursor() as cursor:
            return cursor.execute(query)

def multi_query(*queries):
    ''' Queries is list<pypika.queries.QueryBuilder> '''
    return ';'.join(map(str, queries))
