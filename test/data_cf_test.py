from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st
import funcy as F

from dw.db import orm
from dw.db import schema as S
from dw.db import query as Q
from dw.const import types
from dw.api import put

#@given(dat=st.lists(builds(types.Data)))
#@given(dat_seq=st.iterables(st.builds(types.Data),min_size=1))
@given(dat_seq=st.lists(st.builds(types.Data),min_size=4))
def test_insert(dat_seq, conn):
    # insert generated canonical forms of data
    
    #orm.init(types.connection(conn))
    #Q.CREATE_TABLES()
    #with orm.session() as sess:
        #sess.add_all(F.lmap(S.Data,dat_seq)
    #lst = F.lmap(fp.tup(S.Data), dat_seq)
    dat = dat_seq[0]
    #print(S.Data(uuid=dat.uuid, type=dat.type.value))
    #print(S.Data(dat))
    from pprint import pprint
    pprint(F.lmap(S.Data, dat_seq))
    #assert len(lst) <= 2
    assert False
    #Q.DROP_ALL()
