''' CLI interface of data warehouse '''
# This module is fire cli spec, therefore
# you must import inside of (command) function!

class init(object):
    ''' Initialize something. These commands need to be called only once. '''
    def test_db(self, connection):
        ''' For test
        args: connection: string 'id:pw@host:port/dbname' format
        '''
        from parse import parse
        from dw import db
        
        parsed = parse('{}:{}@{}:{}/{}', connection)
        return(db.init(*parsed) if parsed
          else f'invalid connection string:\n{connection}')
