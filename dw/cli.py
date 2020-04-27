''' CLI interface of data warehouse '''
# This module is fire cli spec, therefore
# you must import modules inside of (command) function!

class init(object):
    ''' Initialize something. These commands need to be called only once. '''
    def szmc_db(self, connection, schema='./dw/schema/szmc_0.1.0.sql'):
        ''' For test
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
    def old_snet(self, root, connection):
        '''
        Add old snet dataset into db.

        Old snet dataset is directory of files.
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
        print(root, connection)
        
    def tmp_m109(self, root, connection):
        ''' Add Manga109 dataset into DB.
        
        (temporary implementation)
        Save Manga109 dataset to db specified in connection.
        
        Manga109 dataset is directory of files.
        ROOT direcory must be satisfy following structure.
        
        ROOT
        ├── images
        │   ├── AisazuNihaIrarenai
        │   │   ├── AisazuNihaIrarenai_0.jpg
        │   │   ├── ...
        │   │   └── AisazuNihaIrarenai_100.jpg
        │   ├── AkkeraKanjinchou
        │   ├── ...
        │   └── YumeNoKayoiji
        └── manga109-annotations
            ├── AisazuNihaIrarenai.xml
            ├── Akuhamu.xml
            ├── ...
            └── YumeNoKayoiji.xml
        
        args: 
            root: root directory path string of manga109 dataset. (src)
            connection: string 'id:pw@host:port/dbname' format. (dst)
        '''
        from parse import parse
        from dw.data_source import manga109

        parsed = parse('{}:{}@{}:{}/{}', connection)
        result = manga109.save(root, parsed) if parsed else 'conn_parse_error'
        return('Add success' if result == None
          else f'invalid connection string:\n{connection}' if parsed == None 
          else result) # some db error

    def tmp_old_snet(self, root, connection):
        '''
        Add old snet dataset into db.

        Old snet dataset is directory of files.
        ROOT direcory must be satisfy following structure.
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
        print(root, connection)
