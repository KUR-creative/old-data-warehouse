from dw.const import types
from dw.db import orm
from dw.db import query as Q

def test_tmp(conn):
    orm.init(types.connection(conn))
    Q.create_tables()
