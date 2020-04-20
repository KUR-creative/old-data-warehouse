import psycopg2 as pg
import records
from pypika import Query

db = None
def init(connection=None):
    '''
    Args:
        connection (str): "id:pw@host:port/dbname" format
    '''
    global db
    
    if connection == None:
        return 'Please call with connection information'
    conn_str = f'postgresql+psycopg2://{connection}'
    # TODO:
    # CREATE tmp_manga109 table with sql(ddl) file 
    db = records.Database(conn_str)
    sql = Query.from_('cars').select('*').get_sql()
    print(sql)
    rows = db.query(sql)
    for r in rows:
        print(r)
        print(r.as_dict())
    print(rows.all())
    
    return conn_str
