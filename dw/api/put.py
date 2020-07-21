from typing import Iterable

from dw.db import orm
from dw.const import types


def data(dat_seq: Iterable[types.Data]):
    ''' put.data(canonical_form) to initialized db '''
    assert orm.engine is not None, 'orm.init first.'
