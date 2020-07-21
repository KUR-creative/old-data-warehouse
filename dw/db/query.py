from dw.db.schema import Base
from dw.db import orm

#---------------------------------------------------------------
# Commands without session
def create_tables():
    assert orm.engine is not None, 'orm.init first.'
    Base.metadata.create_all(orm.engine)
    
# ** DANGER! **
#def DROP_ALL(sess): sess.
