from collections import namedtuple

Dataset = namedtuple(
    'Dataset',
    'name split train valid test',
    defaults=[None, None, None]
)
