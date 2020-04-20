'''
Docstring description here
'''
# This is fire cli spec, therefore
# imported modules also will be parts of command! (groups)

def init(what, *args):
    ''' 
    Initialize something. These commands need to be called only once.
    
    Args:
        what (str): something to initialize. Call matched function
        args: arguments of matched function of what
    '''
    from dw import db
    
    do = {
        'test_db': db.init,
    }.get(what)

    return do(*args) if do else f'invalid argument: {what}'
