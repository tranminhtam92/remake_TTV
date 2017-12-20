from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection


class RevenueLogCollection(BaseCollection):
    def __init__(self):
        super(RevenueLogCollection, self).__init__()
        self.get_collection('log_revenue')
        self.collection.create_index([
            ('commit_time', DESCENDING),
            ('r_user_id', ASCENDING),
        ])
        self.collection.create_index([
            ('commit_time', DESCENDING),
            ('g_user_id', ASCENDING),
        ])
        self.collection.create_index([
            ('commit_time', DESCENDING),
            ('g_user_id', ASCENDING),
            ('client_type', ASCENDING),
        ])
        self.collection.create_index([
            ('commit_time', DESCENDING),
            ('client_type', ASCENDING),
        ])
        self.collection.create_index([
            ('_table', ASCENDING),
        ])


# db.getCollection('log_revenue').aggregate(
#     {
#         '$match': {
#          'commit_time': {"$gte": new ISODate("2017-07-02 00:00:00"), "$lte": new ISODate("2017-13-02 23:59:59")},
#          'client_type' : {"$in": [200,201]}
#         }
#     },
#     {
#         "$project": {
#             "ymd": {
#                 $dateToString: { format: "%Y-%m-%d", date: "$commit_time" }
#             },
#              "client_type": "$client_type",
#             "amount": "$amount",
#         }
#     },
#     {
#         "$group": {
#             "_id": {
#                 "client_type": "$client_type",
#                 "ymd": "$ymd",
#             },
#             ymd : { $first: '$ymd' },
#             client_type : { $first: '$client_type' },
#             amount: {
#                 "$sum": "$amount"
#             }
#         }
#     },
#     {
#         $sort: {
#             "_id.ymd": 1
#         }
#     }
# )

# db.getCollection('log_revenue').aggregate(
#     {
#         '$match': {
#          'commit_time': {"$gte": new ISODate("2017-07-16 00:00:00"), "$lte": new ISODate("2017-12-16 23:59:59")},
#          'client_type' : {"$in": [200,201]}
#         }
#     },
#     {
#         "$project": {
#             "ymd": {
#                 $dateToString: { format: "%Y-%m-%d", date: "$commit_time" }
#             },
#              "client_type": "$client_type",
#             "g_user_id": "$g_user_id",
#             "amount": "$amount",
#         }
#     },
#     {
#         "$group": {
#             "_id": {
#                 "client_type": "$client_type",
#                 "ymd": "$ymd",
#             },
#             ymd : { $first: '$ymd' },
#             client_type : { $first: '$client_type' },
#             give_users: { $addToSet: '$g_user_id'},
#             amount: {
#                 "$sum": "$amount"
#             }
#         }
#     },
#     {
#         $project: {
#             "ymd": "$ymd",
#             "client_type": "$client_type",
#             "num_pay_user": {
#                 $size: "$give_users"
#             },
#             "revenue": "$amount"
#         }
#     },
#     {
#         $sort: {
#             "_id.ymd": 1
#         }
#     }
# )