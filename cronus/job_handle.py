
class JobHandle(object):
    ''' Abstract Base class for job handles
    '''

    def __init__(self, config):
        self._config = config

    def __iter__(self):
        ''' iterate through jobs 
        '''
        pass
