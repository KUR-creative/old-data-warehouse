from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dw.const.types import Connection

# DO NOT CHANGE MANUALLY
engine = None
sess_factory = None

def init(conn: Connection, echo=True):
    ''' Call only one time '''
    global engine, sess_factory
    
    if engine is None:
        conn_str = 'postgresql://{}:{}@{}:{}/{}'.format(*conn)
        engine = create_engine(conn_str, echo=echo)
    if sess_factory is None:
        sess_factory = sessionmaker(bind=engine)

@contextmanager
def session():
    '''Provide a transactional scope around a series of operations.'''
    assert sess_factory is not None, 'orm.init first.'

    session = sess_factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
