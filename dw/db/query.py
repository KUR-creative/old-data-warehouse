from dw.db.schema import Base
from dw.db import orm

#---------------------------------------------------------------
# Commands without session
def CREATE_TABLES():
    assert orm.engine is not None, 'orm.init first.'
    Base.metadata.create_all(orm.engine)
    
# ** DANGER! **
def DROP_ALL():
    assert orm.engine is not None, 'orm.init first.'
    orm.sess_factory.close_all() # need to close all
    Base.metadata.drop_all(orm.engine)
