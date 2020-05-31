import re
import sys
import subprocess

from pypika import Table, Query

from dw import db


def git_hash():
    return (subprocess
       .check_output(['git', 'rev-parse', 'HEAD'])
       .strip().decode('utf8'))

connect_re = re.compile('.+:.+@.+:[0-9]+\/.+') # very generous
def log_cli_cmd(connection, description):
    ''' 
    Note - It uses command from sys.argv.
    So it doesn't need explicit command information in args.
    
    args:
    connection: db spec, string 'id:pw@host:port/dbname' format
    description: optional note for running command. it will be logged with command.
    '''
    safe_argv = [
        '<connection>' if connect_re.match(arg) else arg
        for arg in sys.argv
    ]
    cmd = ' '.join([
        'python',
        *map(lambda arg:
             repr(arg) if ' ' in arg else arg, safe_argv
        )]
    )
    
    query = db.multi_query(
        Query.into('executed_command').columns(
            'command', 'git_hash', 'note'
        ).insert(
            cmd, git_hash(), description
        )
    )
    
    db.run(query, *connection)

def get_cli_cmds(connection, full_table:bool):
    cols = '*' if full_table else 'command'
    return db.get(Table('executed_command').select(cols), *connection)
