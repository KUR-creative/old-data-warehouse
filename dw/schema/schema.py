'''
Automatically generated DB schema constraints.

If you want some functions to be statically checked
 1) Import Schema, and use.
from dw.schema import schema as S
 2) Set return type of function as Any 
def fn_to_type_check(..) -> Any:
 3) run mypy
$ mypy                 # Just run mypy or
$ python main.py mypy  # Run with updating schema.py
'''

from pypika import Table

class executed_command:
    _           = Table('executed_command')
    command     = _.command
    timestamp   = _.timestamp
    git_hash    = _.git_hash
    note        = _.note
class file_source:
    _           = Table('file_source')
    name        = _.name
    root_path   = _.root_path
    host        = _.host
class file:
    _           = Table('file')
    uuid        = _.uuid
    source      = _.source
    relpath     = _.relpath
    abspath     = _.abspath
    type        = _.type
    md5         = _.md5
    size        = _.size
class image:
    _           = Table('image')
    uuid        = _.uuid
    y           = _.y
    x           = _.x
    h           = _.h
    w           = _.w
    full_h      = _.full_h
    full_w      = _.full_w
class image_metadata:
    _           = Table('image_metadata')
    uuid        = _.uuid
    y           = _.y
    x           = _.x
    h           = _.h
    w           = _.w
    depth       = _.depth
    hash        = _.hash
class mask_scheme:
    _           = Table('mask_scheme')
    name        = _.name
    description = _.description
class mask_scheme_content:
    _           = Table('mask_scheme_content')
    name        = _.name
    color       = _.color
    description = _.description
class mask:
    _           = Table('mask')
    uuid        = _.uuid
    scheme      = _.scheme
class annotation_type:
    _           = Table('annotation_type')
    name        = _.name
    description = _.description
class annotation:
    _           = Table('annotation')
    input       = _.input
    y           = _.y
    x           = _.x
    h           = _.h
    w           = _.w
    output      = _.output
    type        = _.type
class dataset:
    _           = Table('dataset')
    name        = _.name
    split       = _.split
    train       = _.train
    valid       = _.valid
    test        = _.test
    description = _.description
class dataset_annotation:
    _           = Table('dataset_annotation')
    name        = _.name
    split       = _.split
    train       = _.train
    valid       = _.valid
    test        = _.test
    input       = _.input
    output      = _.output
    usage       = _.usage
