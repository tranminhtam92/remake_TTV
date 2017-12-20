from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection


class MobileLogCollection(BaseCollection):
    def __init__(self):
        super(MobileLogCollection, self).__init__()
        self.get_collection('log_mobile')
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
            ('room_id', ASCENDING),
            ('uin', ASCENDING),
        ])
        self.collection.create_index([
            ('_log_file', DESCENDING),
            ('_row', ASCENDING),
        ])

    # get_active_users

    def get_active_users(self, dt_from, delta, is_3min=False):
        dt_to = dt_from + delta
        if is_3min:
            conditions = {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "view_length": {"$gte": 180},
            }
        else:
            conditions = {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
            }
        data = self.collection.distinct('uin', filter=conditions, allowDiskUse=True)
        return len(data)

    def get_active_users_by_platform(self, dt_from, delta, is_3min=False):
        dt_to = dt_from + delta
        if is_3min:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "view_length": {"$gte": 180},
            }}
        else:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
            }}
        pipeline = [
            conditions,
            # Sort to improve the performance
            {"$sort": {
                "platform": ASCENDING,
                "uin": ASCENDING
            }},
            # Count all occurrences
            {"$group": {
                "_id": {
                    "platform": "$platform",
                    "uin": "$uin"
                },
                "count": {"$sum": 1}
            }},
            # Sum all occurrences and count distinct
            {"$group": {
                "_id": {
                    "platform": "$_id.platform",
                },
                "active_users": {"$sum": 1}
            }}
        ]
        data = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = {}
        for obj in data:
            result[obj['_id']['platform']] = obj['active_users']
        return result

    def get_active_users_by_category(self, dt_from, delta, is_3min=False):
        dt_to = dt_from + delta
        if is_3min:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "view_length": {"$gte": 180},
            }}
        else:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
            }}
        pipeline = [
            conditions,
            # Sort to improve the performance
            {"$sort": {
                "category": ASCENDING,
                "uin": ASCENDING
            }},
            # Count all occurrences
            {"$group": {
                "_id": {
                    "category": "$category",
                    "uin": "$uin"
                },
                "count": {"$sum": 1}
            }},
            # Sum all occurrences and count distinct
            {"$group": {
                "_id": {
                    "category": "$_id.category",
                },
                "active_users": {"$sum": 1}
            }}
        ]
        data = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = {}
        for obj in data:
            result[obj['_id']['category']] = obj['active_users']
        return result

    def get_active_users_by_sub_category(self, dt_from, delta, is_3min=False):
        dt_to = dt_from + delta
        if is_3min:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "view_length": {"$gte": 180},
                "sub_category": {"$ne": None},
            }}
        else:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "sub_category": {"$ne": None},
            }}
        pipeline = [
            conditions,
            # Sort to improve the performance
            {"$sort": {
                "sub_category": ASCENDING,
                "uin": ASCENDING
            }},
            # Count all occurrences
            {"$group": {
                "_id": {
                    "sub_category": "$sub_category",
                    "uin": "$uin"
                },
                "count": {"$sum": 1}
            }},
            # Sum all occurrences and count distinct
            {"$group": {
                "_id": {
                    "sub_category": "$_id.sub_category",
                },
                "active_users": {"$sum": 1}
            }}
        ]
        data = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = {}
        for obj in data:
            result[obj['_id']['sub_category']] = obj['active_users']
        return result

    def get_active_users_by_platform_and_category(self, dt_from, delta, is_3min=False):
        dt_to = dt_from + delta
        if is_3min:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "view_length": {"$gte": 180},
            }}
        else:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
            }}
        pipeline = [
            conditions,
            # Sort to improve the performance
            {"$sort": {
                "platform": ASCENDING,
                "category": ASCENDING,
                "uin": ASCENDING
            }},
            # Count all occurrences
            {"$group": {
                "_id": {
                    "platform": "$platform",
                    "category": "$category",
                    "uin": "$uin"
                },
                "count": {"$sum": 1}
            }},
            # Sum all occurrences and count distinct
            {"$group": {
                "_id": {
                    "platform": "$_id.platform",
                    "category": "$_id.category",
                },
                "active_users": {"$sum": 1}
            }}
        ]
        data = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = {}
        for obj in data:
            result[(obj['_id']['platform'], obj['_id']['category'])] = obj['active_users']
        return result

    def get_active_users_by_platform_and_sub_category(self, dt_from, delta, is_3min=False):
        dt_to = dt_from + delta
        if is_3min:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "view_length": {"$gte": 180},
                "sub_category": {"$ne": None},
            }}
        else:
            conditions = {'$match': {
                "start_time": {"$lte": dt_to},
                "stop_time": {"$gte": dt_from},
                "sub_category": {"$ne": None},
            }}
        pipeline = [
            conditions,
            # Sort to improve the performance
            {"$sort": {
                "platform": ASCENDING,
                "sub_category": ASCENDING,
                "uin": ASCENDING
            }},
            # Count all occurrences
            {"$group": {
                "_id": {
                    "platform": "$platform",
                    "sub_category": "$sub_category",
                    "uin": "$uin"
                },
                "count": {"$sum": 1}
            }},
            # Sum all occurrences and count distinct
            {"$group": {
                "_id": {
                    "platform": "$_id.platform",
                    "sub_category": "$_id.sub_category",
                },
                "active_users": {"$sum": 1}
            }}
        ]
        data = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = {}
        for obj in data:
            result[(obj['_id']['platform'], obj['_id']['sub_category'])] = obj['active_users']
        return result

    # total view length

    def get_total_view_length(self, dt_from, delta):
        dt_to = dt_from + delta
        # case 0:
        # start_time in range(dt_from, dt_to)
        # stop_time in range(dt_from, dt_to)
        conditions = {'$match': {
            "start_time": {"$gte": dt_from, "$lte": dt_to},
            "stop_time": {"$gte": dt_from, "$lte": dt_to},
        }}
        sum_step = {"$group": {
            "_id": None,
            "total_view_length": {"$sum": "$view_length"}
        }}
        data = self.collection.aggregate([conditions, sum_step], allowDiskUse=True)

        # case 1:
        # start_time < dt_from
        # stop_time in range(dt_from, dt_to)
        conditions1 = {'$match': {
            "start_time": {"$lt": dt_from},
            "stop_time": {"$gte": dt_from, "$lte": dt_to},
        }}
        sum_step1 = {"$group": {
            "_id": None,
            "total_view_length": {"$sum": {"$divide": [{"$subtract": ["$stop_time", dt_from]}, 1000]}}
        }}
        data1 = self.collection.aggregate([conditions1, sum_step1], allowDiskUse=True)

        # case 2:
        # start_time in range(dt_from, dt_to)
        # stop_time > dt_to
        conditions2 = {'$match': {
            "start_time": {"$gte": dt_from, "$lte": dt_to},
            "stop_time": {"$gt": dt_to},
        }}
        sum_step2 = {"$group": {
            "_id": None,
            "total_view_length": {"$sum": {"$divide": [{"$subtract": [dt_to, "$start_time"]}, 1000]}}
        }}
        data2 = self.collection.aggregate([conditions2, sum_step2], allowDiskUse=True)

        # case 3:
        # start_time < dt_from
        # stop_time > dt_to
        conditions3 = {'$match': {
            "start_time": {"$lt": dt_from},
            "stop_time": {"$gt": dt_to},
        }}
        sum_step3 = {"$group": {
            "_id": None,
            "total_view_length": {"$sum": int((dt_to - dt_from).total_seconds())}
        }}
        data3 = self.collection.aggregate([conditions3, sum_step3], allowDiskUse=True)

        data = list(data)
        data.extend(list(data1))
        data.extend(list(data2))
        data.extend(list(data3))
        result = 0
        for i in data:
            result = result + int(i['total_view_length'])
        return result

    def get_total_view_length_by_id(self, dt_from, delta, fields):
        _id = {field: "${}".format(field) for field in fields}
        have_sub = True if 'sub_category' in _id else False
        dt_to = dt_from + delta
        sort_step = {"$sort": {field: ASCENDING for field in fields}}

        # case 0:
        # start_time in range(dt_from, dt_to)
        # stop_time in range(dt_from, dt_to)
        if have_sub:
            conditions = {'$match': {
                "start_time": {"$gte": dt_from, "$lte": dt_to},
                "stop_time": {"$gte": dt_from, "$lte": dt_to},
                "sub_category": {"$ne": None},
            }}
        else:
            conditions = {'$match': {
                "start_time": {"$gte": dt_from, "$lte": dt_to},
                "stop_time": {"$gte": dt_from, "$lte": dt_to},
            }}
        sum_step = {"$group": {
            "_id": _id.copy(),
            "total_view_length": {"$sum": "$view_length"}
        }}
        data = self.collection.aggregate([conditions, sort_step, sum_step], allowDiskUse=True)

        # case 1:
        # start_time < dt_from
        # stop_time in range(dt_from, dt_to)
        if have_sub:
            conditions1 = {'$match': {
                "start_time": {"$lt": dt_from},
                "stop_time": {"$gte": dt_from, "$lte": dt_to},
                "sub_category": {"$ne": None},
            }}
        else:
            conditions1 = {'$match': {
                "start_time": {"$lt": dt_from},
                "stop_time": {"$gte": dt_from, "$lte": dt_to},
            }}
        sum_step1 = {"$group": {
            "_id": _id.copy(),
            "total_view_length": {"$sum": {"$divide": [{"$subtract": ["$stop_time", dt_from]}, 1000]}}
        }}
        data1 = self.collection.aggregate([conditions1, sort_step, sum_step1], allowDiskUse=True)

        # case 2:
        # start_time in range(dt_from, dt_to)
        # stop_time > dt_to
        if have_sub:
            conditions2 = {'$match': {
                "start_time": {"$gte": dt_from, "$lte": dt_to},
                "stop_time": {"$gt": dt_to},
                "sub_category": {"$ne": None},
            }}
        else:
            conditions2 = {'$match': {
                "start_time": {"$gte": dt_from, "$lte": dt_to},
                "stop_time": {"$gt": dt_to},
            }}
        sum_step2 = {"$group": {
            "_id": _id.copy(),
            "total_view_length": {"$sum": {"$divide": [{"$subtract": [dt_to, "$start_time"]}, 1000]}}
        }}
        data2 = self.collection.aggregate([conditions2, sort_step, sum_step2], allowDiskUse=True)

        # case 3:
        # start_time < dt_from
        # stop_time > dt_to
        if have_sub:
            conditions3 = {'$match': {
                "start_time": {"$lt": dt_from},
                "stop_time": {"$gt": dt_to},
                "sub_category": {"$ne": None},
            }}
        else:
            conditions3 = {'$match': {
                "start_time": {"$lt": dt_from},
                "stop_time": {"$gt": dt_to},
            }}
        sum_step3 = {"$group": {
            "_id": _id.copy(),
            "total_view_length": {"$sum": int((dt_to - dt_from).total_seconds())}
        }}
        data3 = self.collection.aggregate([conditions3, sort_step, sum_step3], allowDiskUse=True)

        data = list(data)
        data.extend(list(data1))
        data.extend(list(data2))
        data.extend(list(data3))
        return data

    def get_total_view_length_by_platform(self, dt_from, delta):
        fields = ["platform", ]
        data = self.get_total_view_length_by_id(dt_from, delta, fields)
        result = {}
        for obj in data:
            k = obj['_id']['platform']
            v = obj['total_view_length']
            if k in result:
                result[k] = int(result[k] + v)
            else:
                result[k] = int(v)
        return result

    def get_total_view_length_by_category(self, dt_from, delta):
        fields = ["category", ]
        data = self.get_total_view_length_by_id(dt_from, delta, fields)
        result = {}
        for obj in data:
            k = obj['_id']['category']
            v = obj['total_view_length']
            if k in result:
                result[k] = int(result[k] + v)
            else:
                result[k] = int(v)
        return result

    def get_total_view_length_by_sub_category(self, dt_from, delta):
        fields = ["sub_category", ]
        data = self.get_total_view_length_by_id(dt_from, delta, fields)
        result = {}
        for obj in data:
            k = obj['_id']['sub_category']
            v = obj['total_view_length']
            if k in result:
                result[k] = int(result[k] + v)
            else:
                result[k] = int(v)
        return result

    def get_total_view_length_by_platform_and_category(self, dt_from, delta):
        fields = ["platform", "category", ]
        data = self.get_total_view_length_by_id(dt_from, delta, fields)
        result = {}
        for obj in data:
            k = (obj['_id']['platform'], obj['_id']['category'])
            v = obj['total_view_length']
            if k in result:
                result[k] = int(result[k] + v)
            else:
                result[k] = int(v)
        return result

    def get_total_view_length_by_platform_and_sub_category(self, dt_from, delta):
        fields = ["platform", "sub_category", ]
        data = self.get_total_view_length_by_id(dt_from, delta, fields)
        result = {}
        for obj in data:
            k = (obj['_id']['platform'], obj['_id']['sub_category'])
            v = obj['total_view_length']
            if k in result:
                result[k] = int(result[k] + v)
            else:
                result[k] = int(v)
        return result
