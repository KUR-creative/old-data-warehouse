'''
Generate schema constraints for mypy static type checking.
It is somewhat hacky solution for DB schema constraints...
'''

from pathlib import Path

from pglast import parse_sql
import funcy as F

from dw.utils import fp


def latest(dbname=None):
    ''' latest schema file. TODO: not implemented '''
    return 'dw/schema/szmc_0.1.0.sql'

def schema_dic(sql_path):
    ''' Generate Tables and Columns from sql file. '''
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

def schema_string(schema_dic):
    col_names_lst = schema_dic.values()
    max_len_col_name = max(map(len, F.flatten(col_names_lst)))
    cls_line = 'class {}:'
    tab_line = "    {:<%d} = Table('{}')" % (max_len_col_name,)
    col_line = "    {:<%d} = _.{}" % (max_len_col_name,)
    return '\n'.join([
        *F.mapcat(
            fp.tup(lambda tab, cols: [
                cls_line.format(tab),
                tab_line.format('_', tab), 
                *(col_line.format(col, col)
                  for col in cols)
            ]),
            schema_dic.items())
    ])
        
PY_TEMPLATE ="""'''
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

{}
"""
def generate(inp_sql_path=latest(),
             out_py_path='dw/schema/schema.py'):
    ''' 
    Generate schema.py from inp_sql_path 
    and save to out_py_path 
    If new and old schema.py are same then no update.
    '''
    schema_str = schema_string(schema_dic(inp_sql_path))
    py_str = PY_TEMPLATE.format(schema_str)
    
    out_path = Path(out_py_path)
    old_py_str =(out_path.read_text() if out_path.exists()
                 else None)
    if py_str != old_py_str:
        with open(out_path, 'w') as f:
            f.write(py_str)
        print('dw/schema/schema.py updated!')
