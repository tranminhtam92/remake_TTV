from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection


class ViewLogCollection(BaseCollection):
    def __init__(self):
        super(ViewLogCollection, self).__init__()
        self.get_collection('log_view')
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('category', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('sub_category', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('platform', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('platform', ASCENDING),
            ('category', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('platform', ASCENDING),
            ('sub_category', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('streamer_uin', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('is_game', ASCENDING),
            ('streamer_uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('streamer_uin', ASCENDING),
            ('room_id', ASCENDING),
            ('stream_table', ASCENDING),
            ('stream_id', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('stop_time', DESCENDING),
            ('start_time', DESCENDING),
            ('room_id', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('_log_file', DESCENDING),
            ('_row', ASCENDING),
        ])

    # get active_users and view_length

    def get_users_and_view_length(self, dt_from, delta, fields=None):
        dt_to = dt_from + delta

        match = {
            "start_time": {"$lte": dt_to},
            "stop_time": {"$gte": dt_from},
        }
        if fields is not None:
            for field in fields:
                match[field] = {"$ne": None}

        group = {
            "_id": {field: "${}".format(field) for field in fields} if fields is not None else None,
            "users": {"$addToSet": "$uin"},
            "users_3min": {"$addToSet": {"$cond": [{"$gte": ["$view_length", 180]}, "$uin", None]}},
            "view_length": {"$sum": {
                "$divide": [{"$subtract": [
                    {"$cond": [{"$lt": ["$stop_time", dt_to]}, "$stop_time", dt_to]},
                    {"$cond": [{"$gt": ["$start_time", dt_from]}, "$start_time", dt_from]},
                ]}, 1000]
            }}
        }

        project = {
            '_id': False,
            'users': {"$setDifference": ["$users", [None, ]]},
            'users_3min': {"$setDifference": ["$users_3min", [None, ]]},
            'view_length': True,
        }
        if fields is not None:
            for field in fields:
                project[field] = "$_id.{}".format(field)

        pipeline = [
            {"$match": match},
            {"$group": group},
            {"$project": project}
        ]
        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
        data = list(cursor)
        return data

    def get_users_and_view_length_by_streaming_event(self, stream_ids):
        pipeline = [
            {'$match': {
                "stream_id": {"$in": stream_ids},
            }},
            {"$group": {
                "_id": {
                    "streamer_uin": "$streamer_uin",
                    "room_id": "$room_id",
                    "stream_table": "$stream_table",
                    "stream_id": "$stream_id",
                },
                "users": {"$addToSet": "$uin"},
                "users_3min": {"$addToSet": {"$cond": [{"$gte": ["$view_length", 180]}, "$uin", None]}},
                "view_length": {"$sum": "$view_length"},
            }},
            {"$project": {
                '_id': False,
                "streamer_uin": "$_id.streamer_uin",
                "room_id": "$_id.room_id",
                "stream_table": "$_id.stream_table",
                "stream_id": "$_id.stream_id",
                'users': {"$setDifference": ["$users", [None, ]]},
                'users_3min': {"$setDifference": ["$users_3min", [None, ]]},
                'view_length': True,
            }}
        ]
        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
        data = list(cursor)
        return data

    def get_users_and_view_length_for_streamer_class(self, dt_from, delta, streamer_uins, is_game):
        dt_to = dt_from + delta

        match = {
            "start_time": {"$lte": dt_to},
            "stop_time": {"$gte": dt_from},
            "streamer_uin": {"$in": streamer_uins},
            "is_game": is_game
        }

        group = {
            "_id": None,
            "users": {"$addToSet": "$uin"},
            "users_3min": {"$addToSet": {"$cond": [{"$gte": ["$view_length", 180]}, "$uin", None]}},
            "view_length": {"$sum": {
                "$divide": [{"$subtract": [
                    {"$cond": [{"$lt": ["$stop_time", dt_to]}, "$stop_time", dt_to]},
                    {"$cond": [{"$gt": ["$start_time", dt_from]}, "$start_time", dt_from]},
                ]}, 1000]
            }}
        }

        project = {
            '_id': False,
            'users': {"$setDifference": ["$users", [None, ]]},
            'users_3min': {"$setDifference": ["$users_3min", [None, ]]},
            'view_length': True,
        }

        pipeline = [
            {"$match": match},
            {"$group": group},
            {"$project": project}
        ]

        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
        data = list(cursor)
        return data
