from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection


class ChatLogCollection(BaseCollection):
    def __init__(self):
        super(ChatLogCollection, self).__init__()
        self.get_collection('log_chat')
        self.collection.create_index([
            ('event_time', DESCENDING),
            ('room_id', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('_log_file', DESCENDING),
            ('_row', ASCENDING),
        ])

    def get_total_comments(self, dt_from, delta):
        dt_to = dt_from + delta
        return self.collection.count({
            'event_time': {'$gte': dt_from, '$lte': dt_to}
        })

    def get_total_comments_by_room(self, dt_from, delta):
        dt_to = dt_from + delta
        cursor = self.collection.aggregate([
            {'$match': {
                'event_time': {'$gte': dt_from, '$lte': dt_to}
            }},
            {'$group': {
                "_id": '$room_id',
                "total_comments": {"$sum": 1}
            }}
        ], allowDiskUse=True)
        data = list(cursor)
        result = {}
        for entity in data:
            result[entity['_id']] = entity['total_comments']
        return result
