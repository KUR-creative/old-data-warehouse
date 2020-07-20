from typing import NamedTuple, Mapping, Any

class Data(NamedTuple):
    uuid: str
    type: str
    value: Mapping[int, Any]
