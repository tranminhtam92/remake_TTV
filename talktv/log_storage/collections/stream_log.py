from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection


class StreamLogCollection(BaseCollection):
    def __init__(self):
        super(StreamLogCollection, self).__init__()
        self.get_collection('log_stream')
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('streamer_uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('category', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('sub_category', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('room_id', ASCENDING),
        ])
        self.collection.create_index([
            ('_table', ASCENDING),
        ])

    def is_existed(self, room_id, streamer_uin, start_time, stop_time):
        result = self.collection.find({
            'room_id': room_id,
            'streamer_uin': streamer_uin,
            'start_time': {'$lte': stop_time},
            'stop_time': {'$gte': start_time}
        })
        return len(result) > 0
