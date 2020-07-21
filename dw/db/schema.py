from enum import Enum
from uuid import uuid4

from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from dw.const import types


def nt_dic(nt_obj):
    ''' namedtuple obj to db dic ''' 
    dic = nt_obj._asdict()
    for k,v in dic.items():
        if isinstance(v, Enum):
            dic[k] = v.value
    return dic
'''
data는 서로 관계를 가질 수 있는 정보의 단위이다.
named_dat_rel 은 이름 붙여진 데이터 사이의 관계이다.
dataset은 named_dat_rel 3개로 이루어진다.
'''

#---------------------------------------------------------------
class Data(Base):
    __tablename__ = 'data'
    uuid = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid4)
    type = Column(String)

    def __init__(self, nt_obj):
        super(Data, self).__init__(**nt_dic(nt_obj)) # type: ignore
    def __repr__(self):
        return "<Data(uuid='%s', type='%s')>" % (
            self.uuid, self.type)
