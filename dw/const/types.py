from parse import parse

from typing import NamedTuple, Mapping, Any


class Connection(NamedTuple):
    user: str
    password: str
    host: str
    port: int
    dbname: str

def connection(conn_str: str):
    parsed = parse('{}:{}@{}:{}/{}', conn_str)
    print(parsed)
    return Connection(*parsed) if parsed else None

class Data(NamedTuple):
    uuid: str
    type: str
    value: Mapping[int, Any]
