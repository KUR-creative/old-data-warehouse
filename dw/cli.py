''' 
CLI interface of data warehouse.
All successful commands are logged in connected DB.

The functions defined in this module prepare for 'task'.
ex) Parsing argument. Reporting error when arg parse fail..
All other jobs are executed by functions in 'tasks' package.
'''
# This module is fire cli spec, therefore
# you must import modules inside of (command) function!

def DROP_ALL(connection, note=None):
    ''' 
    DROP ALL of tables in DB. Be careful!
    
    args: 
    connection: string 'id:pw@host:port/dbname' format
    note: note for running command. it will be logged with command.
    '''
    from parse import parse
    from dw import db
    import sys
    print(sys.argv)
    
    print('This command DROP ALL of tables in DB.')
    ans = input('Do you really want? [yes/no] \n')
    if ans == 'yes':
        parsed = parse('{}:{}@{}:{}/{}', connection)
        query = 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'
        if parsed:
            db.run(query, *parsed)
            return 'All tables are dropped'
        else:
            return f'invalid connection string:\n{connection}'
    elif ans == 'no':
        return 'Nothing happened!'
    else:
        return 'Please answer: yes or no.'
        
def REINIT(connection, schema='./dw/schema/szmc_0.1.0.sql', note=None): 
    ''' 
    DROP ALL of tables in DB and then re-initialize DB. 
    Be careful!
    
    args: 
    schema: schema sql file path. default: './dw/schema/szmc_0.1.0.sql'
    connection: string 'id:pw@host:port/dbname' format
    note: note for running command. it will be logged with command.
    '''
    ret = DROP_ALL(connection)
    if ret == 'All tables are dropped':
        return init().szmc_db(connection, schema, note)
    else:
        return ret

def log(connection):
    '''
    Print command log of DB. This command will not logged.
   
    args:
    connection: string 'id:pw@host:port/dbname' format
    '''
    from parse import parse
    from dw import log
    
    parsed = parse('{}:{}@{}:{}/{}', connection)
    if parsed:
        print(str(log.get_cli_cmds(parsed).dataset)
              .replace('<connection>', connection))
        return None
    else:
        return f'invalid connection string:\n{connection}'
    
def tmp_tfrecord_test(tfrecord_path='/home/kur/dev/szmc/nn-lab/dataset/snet285wk.tfrecords'):
    ''' temporary implementation for tfrecord exporting test. This command will not logged.  '''
    from tests import export_test
    export_test.tmp_dset_testing(tfrecord_path)
    
class init(object):
    ''' Initialize something. These commands need to be called only once. '''
    def szmc_db(self, connection, schema='./dw/schema/szmc_0.1.0.sql', note=None):
        '''
        Initialize szmc DB

        args: 
        schema: schema sql file path. default: './dw/schema/szmc_0.1.0.sql'
        connection: string 'id:pw@host:port/dbname' format
        note: note for running command. it will be logged with command.
        '''
        from parse import parse
        from dw import db
        from dw import log
        
        parsed = parse('{}:{}@{}:{}/{}', connection)
        with open(schema, 'r') as s:
            if parsed:
                ret = db.init(s.read(), *parsed)
                log.log_cli_cmd(parsed, note) # must be called after init!
                return ret
            else:
                return f'invalid connection string:\n{connection}'

class add(object):
    ''' Add something(s) '''
    def old_snet(self, root, connection, note=None):
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
        note: note for running command. it will be logged with command.
        '''
        from parse import parse
        from dw.data_source import old_snet
        from dw import log

        parsed = parse('{}:{}@{}:{}/{}', connection)
        result = old_snet.save(root, parsed) if parsed else 'conn_parse_error'
        if parsed == None:
            return f'invalid connection string:\n{connection}' 
        elif result == None:
            log.log_cli_cmd(parsed, note)
            return 'Add success'
        else:
            return result

class create(object):
    ''' Create something(s) '''
    def old_snet(self, split_yaml, connection, note=None):
        '''
        Create old snet dataset from old snet data in db(connection)

        Old snet dataset is consist of <image,mask> pairs that splitted in train/valid/test.
        split_yaml must be dict of id list
        ex) {train: [0,1, ...], valid: [2,3, ..], test: [6,7, ..]} 
        id is name of image/mask files in old snet (id is old snet specific data).
        
        Dataset old_snet.a.199.58.28 will be created.
                (name.split.train.valid.test)
        the dataset is row of 'dataset' table. 
        And the contents of dataset is implicitly saved 
        in 'dataset_annotation' table.
        
        args: 
        split_yaml: file path of train/valid/split specified yaml (legacy)
        connection: string 'id:pw@host:port/dbname' format.
        note: note for running command. it will be logged with command.
        '''
        from parse import parse
        from dw.data_source import old_snet
        from dw import log

        parsed = parse('{}:{}@{}:{}/{}', connection)
        result = old_snet.create(split_yaml, parsed) if parsed else 'conn_parse_error'
        if parsed == None:
            return f'invalid connection string:\n{connection}' 
        elif result == None:
            log.log_cli_cmd(parsed, note)
            return 'Create success'
        else:
            return result
        
class generate(object):
    ''' Generate something(s) '''
    def easy_only(self, connection, src_dataset, mask_dir_relpath='easy_only', note=None):
        '''
        Create EASY ONLY dataset from src_dataset in db(connection).
        src_dataset must have rbk scheme data.
        
        ----
        It generate and save easy-only mask(red mask from rbk scheme) files. 
        And it creates dataset: "src_dataset_name.easy_only.train.valid.test"
          Name of generated dataset = src_dataset_name (same)
          Split of generated dataset = easy_only (same dataset different split)
        Generated easy-only mask files are saved in root_path/mask_dir_relpath, 
        where root_path from file_source table.
        Names of mask files are same with names of src_dataset mask files.

        src_dataset(rbk) => generated dataset(wk)
        
        new mask scheme "easy_only" will be created if not exists.
        EASY ONLY dataset is (old_snet, wk) dataset where
        white masks are only easy-text(red masks in rbk scheme)
        
        ----
        src_dataset must be consist of <image,mask> pairs that splitted in train/valid/test.
        ex) {train: [0,1, ...], valid: [2,3, ..], test: [6,7, ..]} 
        id is name of image/mask files in old snet (id is old snet specific data).

        the dataset is row of 'dataset' table. 
        And the contents of dataset is implicitly saved 
        in 'dataset_annotation' table.
        
        args: 
        connection: string 'id:pw@host:port/dbname' format
        src_dataset: string 'name.split.train.valid.test' format.
        'name.split' (tvt omit) then choose biggest dataset.
        mask_dir_relpath: directory path to save generated easy text only masks.
        It relative to root_path of src_dataset. Default = 'easy_only'
        note: note for running command. it will be logged with command.
        '''
        from parse import parse
        from dw.tasks import generate
        from dw import log
        from dw import common

        # connection, src_dataset, mask_dir_relpath, note=None
        db_parsed = parse('{}:{}@{}:{}/{}', connection)
        if not db_parsed:
            return f'Invalid connection string:\n{connection}'
        
        dset_parsed = parse('{}.{}', src_dataset)
        if not dset_parsed:
            return f'Invalid src_dataset specification:\n{dset_parsed}'
        else:
            parsed = parse('{}.{}.{}.{}.{}', src_dataset)
            if parsed:
                dset = common.Dataset(*parsed)
            else:
                name, split = dset_parsed
                assert '.' not in split
                dset = common.Dataset(name, split)

        # Didn't check mask_dir_relpath validity. But maybe it's okay..
                
        generate.generate(db_parsed, dset, 'easy_only', mask_dir_relpath)

class export(object):
    ''' Export something(s) to something(s) '''
    def tfrecord(self, connection, out_path, dataset, option=None):
        ''' 
        Export dataset to tfrecord dataset saved in out_path
        
        Currently, dataset.name = old_snet, option= rbk or wk.

        args: 
        connection: string 'id:pw@host:port/dbname' format
        out_path: file path to save tfrecord dataset.
        dataset: string 'name.split.train.valid.test' format.
        'name.split' (tvt omit) then choose biggest dataset.
        option: dataset specific options. rbk/wk for old snet.
        '''
        from dw.tasks import export
        from dw import common
        from parse import parse
        
        db_parsed = parse('{}:{}@{}:{}/{}', connection)
        if not db_parsed:
            return f'Invalid connection string:\n{connection}'
        
        dset_parsed = parse('{}.{}', dataset)
        if not dset_parsed:
            return f'Invalid dataset specification:\n{dset_parsed}'
        else:
            parsed = parse('{}.{}.{}.{}.{}', dataset)
            if parsed:
                dset = common.Dataset(*parsed)
            else:
                name, split = dset_parsed
                assert '.' not in split
                dset = common.Dataset(name, split)
                
        export.export(
            db_parsed, out_path, 'tfrecord', dset, option)
