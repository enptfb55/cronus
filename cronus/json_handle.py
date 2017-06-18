import json
from job_handle import JobHandle


class JsonHandle(JobHandle):
    def __init__(self, config):
        super(JsonHandle, self).__init__(config)
        
        fpath = self._config.get('Source', 'file_path')
        with open(fpath) as f:
            self.json = json.load(f)

    def __iter__(self):
        ''' iterate through jobs in json file
        '''
        for job in self.json:
            yield job
