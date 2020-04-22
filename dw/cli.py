''' CLI interface of data warehouse '''
# This module is fire cli spec, therefore
# you must import modules inside of (command) function!

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

class add(object):
    ''' Add something(s) '''
    def tmp_m109(self, root, connection):
        ''' Add Manga109 dataset into DB.
        
        (temporary implementation)
        Save Manga109 dataset to db specified in connection.
        
        Manga109 dataset is directory of files.
        ROOT direcory must be satisfy following structure.
        
        ROOT
        ├── images
        │   ├── AisazuNihaIrarenai
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
        from dw import db
        from dw.data_source import manga109

        parsed = parse('{}:{}@{}:{}/{}', connection)
        result = manga109.save(root, parsed) if parsed else 'conn_parse_error'
        return('Add success' if result == None
          else f'invalid connection string:\n{connection}' if parsed == None 
          else result) # some db error
