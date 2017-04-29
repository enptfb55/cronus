import json

class JsonHandle(object):
    def __init__(self, fpath):
        with open(fpath) as f:
            self.json = json.load(f)

    def iter(self):
        ''' iterate through jobs in json file
        '''

        for job in self.json:
            yield job
