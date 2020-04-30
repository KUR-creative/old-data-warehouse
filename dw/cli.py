''' CLI interface of data warehouse '''
# This module is fire cli spec, therefore
# you must import modules inside of (command) function!

class init(object):
    ''' Initialize something. These commands need to be called only once. '''
    def szmc_db(self, connection, schema='./dw/schema/szmc_0.1.0.sql'):
        '''
        args: 
        schema: schema sql file path. default: './dw/schema/szmc_0.1.0.sql'
        connection: string 'id:pw@host:port/dbname' format
        '''
        from parse import parse
        from dw import db
        
        parsed = parse('{}:{}@{}:{}/{}', connection)
        with open(schema, 'r') as s:
            return(db.init(s.read(), *parsed) if parsed
              else f'invalid connection string:\n{connection}')

class add(object):
    ''' Add something(s) '''
    def old_snet(self, connection, root):
        '''
        Add old snet data(not dataset!) into db.

        Old snet data is directory of files.
        ROOT direcory must satisfy following structure.
        map.json must be list of [old_name, some_path]
        
        root
        ├── image
        │   ├── 0.png
        │   ├── ...
        │   └── 284.png
        ├── clean_rbk
        │   ├── 0.png
        │   ├── ...
        │   └── 284.png
        ├── clean_wk
        │   ├── 0.png
        │   ├── ...
        │   └── 284.png
        └── map.json
        
        args: 
            root: root directory path string of old snet dataset. (src)
            connection: string 'id:pw@host:port/dbname' format. (dst)
        '''
        from parse import parse
        from dw.data_source import old_snet

        parsed = parse('{}:{}@{}:{}/{}', connection)
        result = old_snet.save(root, parsed) if parsed else 'conn_parse_error'
        return('Add success' if result == None
          else f'invalid connection string:\n{connection}' if parsed == None 
          else result) # some db error

class create(object):
    ''' Create something(s) '''
    def old_snet(self, connection, split_yaml):
        '''
        Create old snet dataset from old snet data in db(connection)

        Old snet dataset is consist of <image,mask> pairs that splitted in train/valid/test.
        split_yaml must be dict of id list
        ex) {train: [0,1, ...], valid: [2,3, ..], test: [6,7, ..]} 
        id is name of image/mask files in old snet (old snet specific data).
        
        args: 
            split_yaml: file path of train/valid/split specified yaml (legacy)
            connection: string 'id:pw@host:port/dbname' format.
        '''
        from parse import parse
        from dw.data_source import old_snet

        parsed = parse('{}:{}@{}:{}/{}', connection)
        result = old_snet.create(connection, split_yaml) if parsed else 'conn_parse_error'
        return('Create success' if result == None
          else f'invalid connection string:\n{connection}' if parsed == None 
          else result) # some db error
        print(root, connection)
