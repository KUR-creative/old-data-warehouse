from typing import NamedTuple, Mapping, Any
from enum import Enum, auto
import uuid

from parse import parse

#---------------------------------------------------------------
# Common types
class Connection(NamedTuple):
    user: str
    password: str
    host: str
    port: int
    dbname: str

def connection(conn_str: str):
    parsed = parse('{}:{}@{}:{}/{}', conn_str)
    return Connection(*parsed) if parsed else None

#---------------------------------------------------------------
# Types for DB
class _AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name
class DataType(_AutoName):
    img = auto()
    crop = auto()
    mask = auto()
    
class Data(NamedTuple):
    uuid: uuid.UUID
    type: DataType
