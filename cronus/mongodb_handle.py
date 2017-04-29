
from pymongo import MongoClient

class MongoDBHandle(object):
    def __init__(self, host, port, db, collection):
        client = MongoClient(host, int(port))
        self.collection = client[db][collection]

    def iter(self):
        ''' iterate through the collection
        '''

        for job in self.collection.find({}):
            yield job
