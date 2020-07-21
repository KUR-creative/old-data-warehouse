from uuid import uuid4

from hypothesis import given, settings
from hypothesis import strategies as st
import funcy as F

from dw.db import orm
from dw.db import schema as S
from dw.db import query as Q
from dw.const import types
from dw.api import put

@given(datums=st.lists(st.builds(types.Data)))
@settings(max_examples=20)
def test_insert(datums, conn):
    # insert generated canonical forms of data
    orm.init(types.connection(conn))
    Q.CREATE_TABLES()
    with orm.session() as sess:
        sess.add_all(F.lmap(S.Data, datums))
        # Be Careful!! sess must be in ctx manager!!!
        for inp, out in zip(datums, sess.query(S.Data).all()):
            assert inp.uuid == out.uuid
    Q.DROP_ALL()
