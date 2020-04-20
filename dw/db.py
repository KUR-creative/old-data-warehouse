def init(connection=None):
    if connection == None:
        return 'Please call with connection information'
    conn_str = f'postgresql+psycopg2://{connection}'
    return conn_str
