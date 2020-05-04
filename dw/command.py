from collections import namedtuple
from dw.utils import fp

Dataset = namedtuple(
    'Dataset',
    'name split train valid test',
    defaults=[None, None, None]
)

@fp.multi
def export(connection, out_path, out_form, dataset, option=None):
    return out_form, dataset, option

@fp.mmethod(export, ('tfrecord', Dataset('old_snet', 'full'), 'rbk'))
def export(connection, out_path, out_form, dataset, option):
    _export(connection, out_path, out_form, dataset, option)
@fp.mmethod(export, ('tfrecord', Dataset('old_snet', 'full'), 'wk'))
def export(connection, out_path, out_form, dataset, option):
    _export(connection, out_path, out_form, dataset, option)

def _export(connection, out_path, out_form, dataset, option):
    print(option)
