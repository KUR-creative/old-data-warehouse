from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dw.const.types import Connection

# DO NOT CHANGE MANUALLY
engine = None
_make_session = None

def init(conn: Connection, echo=True):
    ''' Call only one time '''
    global engine, _make_session
    
    if engine is None:
        conn_str = 'postgresql://{}:{}@{}:{}/{}'.format(*conn)
        engine = create_engine(conn_str, echo=echo)
    if _make_session is None:
        _make_session = sessionmaker(bind=engine)

@contextmanager
def session():
    '''Provide a transactional scope around a series of operations.'''
    assert _make_session is not None, 'orm.init first.'

    session = _make_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
