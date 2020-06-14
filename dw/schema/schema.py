'''
Schema constraints for mypy static type checking.

CAUTION: IT CAN MODIFIES ITSELF WHEN IMPORTED!

It is very hacky solution for DB schema constraints...
'''
from pathlib import Path

from pglast import parse_sql
import funcy as F

from dw.utils import fp


TABLES_LINE  = '# --------------------------- Tables ---------------------------'
COLUMNS_LINE = '# --------------------------- Columns --------------------------'
END_LINE = '# --------------------------------------------------------------'

def latest(dbname=None):
    ''' latest schema file. TODO: not implemented '''
    return 'dw/schema/szmc_0.1.0.sql'

def schema_dic(sql_path):
    ''' Generate Entity and Attribute from sql file. '''
    with open(sql_path, 'r') as f:
        sql = f.read()
    
    def is_create_table(stmt):
        keys = F.get_in(stmt, ['RawStmt', 'stmt']).keys()
        assert len(keys) == 1
        return list(keys)[0] == 'CreateStmt'
    create_table_stmts = fp.go(
        parse_sql(sql),
        fp.filter(is_create_table),
        fp.lmap(lambda coll: F.get_in(
            coll, ['RawStmt', 'stmt', 'CreateStmt']
        ))
    )

    tables = fp.map(
        lambda coll: F.get_in(
            coll, ['relation', 'RangeVar', 'relname']
        ), 
        create_table_stmts
    )
    columns = fp.map(
        fp.pipe(
            lambda dic: dic['tableElts'],
            fp.lmap(lambda coll: F.get_in(
                coll, ['ColumnDef', 'colname']
            )),
            F.compact
        ),
        create_table_stmts
    )
    
    return F.zipdict(tables, columns)

def tables_string(schema_dic):
    tab_names = schema_dic.keys()
    max_len_tab_name = max(map(len, tab_names))
    tab_line = "    {:<%d} = Table('{}')" % (max_len_tab_name,)
    return '\n'.join([
        'class Tables:',
        *(tab_line.format(name, name) for name in tab_names)
    ])

def columns_string(schema_dic):
    col_names_lst = schema_dic.values()
    max_len_col_name = max(map(len, F.flatten(col_names_lst)))
    tab_line = '    class {}:'
    col_line = '        {:<%d} = Tables.{}.{}' % (max_len_col_name,)
    return '\n'.join([
        'class Columns:',
        *F.mapcat(
            fp.tup(lambda tab, cols: [
                tab_line.format(tab),
                *(col_line.format(col, tab, col)
                  for col in cols)
            ]),
            schema_dic.items())
    ])
    
def lines_parts(code_lines):
    top, _tab_, mid, _col_, bottom = F.lpartition_by(
        lambda line: {TABLES_LINE:0, COLUMNS_LINE:1}.get(line),
        code_lines
    )
    upto_tab_ = top + _tab_
    upto_col_ = mid + _col_
    
    '''
    '''
    print('################## top #######################')
    for line in top:
        print(repr(line))
    print('################## _tab_ #######################')
    for line in _tab_:
        print(repr(line))
    print('################## MID #######################')
    for line in mid:
        print(repr(line))
    print('################## _col_ #####################')
    for line in _col_:
        print(repr(line))
    print('################## bottom #####################')
    for line in bottom:
        print(repr(line))

    return upto_tab_, upto_col_, bottom

def old_tab_col_lines(upto_col_lines, bottom_lines):
    old_tab_lines,_ = F.lsplit_by(
        lambda line: line != END_LINE, upto_col_lines)
    old_col_lines,_ = F.lsplit_by(
        lambda line: line != END_LINE, bottom_lines)
    return old_tab_lines, old_col_lines
    
def modify(schema_dic, py_path=Path(__file__).absolute()):
    ''' CAUTION: Modifies THIS file if latest schema is changed! '''
    with open(py_path, 'r') as f:
        old_code = f.read()
        code_lines = old_code.split('\n')
    tab_lines = tables_string(schema_dic).split('\n')
    col_lines = columns_string(schema_dic).split('\n')

    upto_tab_, upto_col_, bottom = lines_parts(code_lines)
    old_tab_lines, old_col_lines = old_tab_col_lines(
        upto_col_, bottom)
    
    tab_same = (
        all(map(fp.equal, old_tab_lines, tab_lines)) and
        len(old_tab_lines) == len(tab_lines))
    col_same = (
        all(map(fp.equal, old_col_lines, col_lines)) and
        len(old_col_lines) == len(col_lines))
    
    if tab_same and col_same:
        print('No change!')
        #return None
    
    new_code = '\n'.join(
        upto_tab_ +    # ------- TAB -------
        (old_tab_lines if tab_same else tab_lines) +
        [END_LINE] +     # -------------------
        [COLUMNS_LINE] + # ------- COL -------
        (old_col_lines if col_same else col_lines) +
        [END_LINE]
    )

    with open(py_path, 'w') as f:
        f.write(new_code)

    '''
    #new_code = schema_added_code(code_lines, tab_lines, col_lines)
    print('###########_upto_tab_###################################')
    for line in upto_tab_:
        print(repr(line))
    print('########### upto_col ###################################')
    for line in upto_col_:
        print(repr(line))
    print('############# bottom #################################')
    for line in bottom:
        print(repr(line))
    print('##############################################')
    print('##############################################')
    for line in old_tab_lines:
        print(repr(line))
    print('##############################################')
    print('##############################################')
    for line in old_col_lines:
        print(repr(line))
    print('##############################################')
    print('##############################################')

    
    if old_code != new_code:
        with open(py_path, 'w') as f:
            f.write(new_code)
    else:
        print('no change!')

    '''
    #print(new_code)
            
modify(schema_dic(latest()))

'''
When this module imported, generate Entity and Attribute 
if latest schema file changed.

Generated Entity/Attribute class code is written between comment.

# ----- Entity/Attribute ------
class Entity:                  
    class a_table_name:        <- here
        ...
# -----------------------------

It is necessary to check the change of the automatically
generated code to detect the latest schema change.

So DO NOT MANUALLY MODIFY AFTER THIS LINE! '''

from pypika import Table

# --------------------------- Tables ---------------------------
class Tables:
    executed_command    = Table('executed_command')
    file_source         = Table('file_source')
    file                = Table('file')
    image               = Table('image')
    mask_scheme         = Table('mask_scheme')
    mask_scheme_content = Table('mask_scheme_content')
    mask                = Table('mask')
    annotation_type     = Table('annotation_type')
    annotation          = Table('annotation')
    dataset             = Table('dataset')
    dataset_annotation  = Table('dataset_annotation')
    image_metadata      = Table('image_metadata')
# --------------------------------------------------------------
# --------------------------- Columns --------------------------
class Columns:
    class executed_command:
        command     = Tables.executed_command.command
        timestamp   = Tables.executed_command.timestamp
        git_hash    = Tables.executed_command.git_hash
        note        = Tables.executed_command.note
    class file_source:
        name        = Tables.file_source.name
        root_path   = Tables.file_source.root_path
        host        = Tables.file_source.host
    class file:
        uuid        = Tables.file.uuid
        source      = Tables.file.source
        relpath     = Tables.file.relpath
        abspath     = Tables.file.abspath
        type        = Tables.file.type
        md5         = Tables.file.md5
        size        = Tables.file.size
    class image:
        uuid        = Tables.image.uuid
    class mask_scheme:
        name        = Tables.mask_scheme.name
        description = Tables.mask_scheme.description
    class mask_scheme_content:
        name        = Tables.mask_scheme_content.name
        color       = Tables.mask_scheme_content.color
        description = Tables.mask_scheme_content.description
    class mask:
        uuid        = Tables.mask.uuid
        scheme      = Tables.mask.scheme
    class annotation_type:
        name        = Tables.annotation_type.name
        description = Tables.annotation_type.description
    class annotation:
        input       = Tables.annotation.input
        output      = Tables.annotation.output
        type        = Tables.annotation.type
    class dataset:
        name        = Tables.dataset.name
        split       = Tables.dataset.split
        train       = Tables.dataset.train
        valid       = Tables.dataset.valid
        test        = Tables.dataset.test
        description = Tables.dataset.description
    class dataset_annotation:
        name        = Tables.dataset_annotation.name
        split       = Tables.dataset_annotation.split
        train       = Tables.dataset_annotation.train
        valid       = Tables.dataset_annotation.valid
        test        = Tables.dataset_annotation.test
        input       = Tables.dataset_annotation.input
        output      = Tables.dataset_annotation.output
        usage       = Tables.dataset_annotation.usage
    class image_metadata:
        uuid        = Tables.image_metadata.uuid
        size        = Tables.image_metadata.size
        height      = Tables.image_metadata.height
        width       = Tables.image_metadata.width
        depth       = Tables.image_metadata.depth
# --------------------------------------------------------------
