from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from dw.const import types


@given(dat=st.builds(types.Data))
def test_insert(dat, conn):
    # insert generated canonical forms of data
    print(dat)
    assert False
