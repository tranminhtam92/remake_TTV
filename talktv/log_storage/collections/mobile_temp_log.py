from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection


class MobileTempLogCollection(BaseCollection):
    def __init__(self):
        super(MobileTempLogCollection, self).__init__()
        self.get_collection('temp_log_mobile')
        self.collection.create_index([
            ('uin', DESCENDING),
            ('login_ip', ASCENDING),
            ('platform', ASCENDING),
            ('room_mode', ASCENDING),
            ('room_id', ASCENDING),
            ('start_time', DESCENDING),
            ('stop_time', ASCENDING),
        ])

    def pop_all_temp_log(self):
        cursor = self.collection.find({})
        docs = list(cursor)
        self.collection.delete_many({})
        return docs
