from django.conf import settings
from pymongo import MongoClient

client = MongoClient(settings.MONGO_DB_HOST, settings.MONGO_DB_PORT, connect=False)


class MongoConnection(object):
    def __init__(self):
        self.db = client[settings.MONGO_DB_NAME]

    def get_collection(self, name):
        self.collection = self.db[name]


class BaseCollection(MongoConnection):
    def create(self, document):
        inserted_id = self.collection.insert_one(document).inserted_id
        return inserted_id

    def create_many(self, documents):
        inserted_ids = self.collection.insert_many(documents).inserted_ids
        return inserted_ids

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)
