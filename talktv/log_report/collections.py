from datetime import datetime

from pymongo import ASCENDING, DESCENDING

from talktv.base_collection import BaseCollection
from talktv.constants import ReportType


class ReportCollection(BaseCollection):
    def __init__(self):
        super(ReportCollection, self).__init__()
        self.get_collection('report')
        self.collection.create_index([
            # BASIC
            ('report_type', DESCENDING),
            ('report_date', DESCENDING),
            ('platform', ASCENDING),
            ('category', ASCENDING),
            ('sub_category', ASCENDING),
            ('room_id', ASCENDING),
            # STREAMING REPORT
            ('streamer_uin', ASCENDING),
            ('stream_start', ASCENDING),
            ('stream_stop', ASCENDING),
            # STREAMER CLASS REPORT
            ('streamer_class', ASCENDING),
            ('is_game', ASCENDING),
        ], name='report_index', unique=True)

    def _update_report(self, keys, data):
        spec = {
            'report_type': None,
            'report_date': None,
            'platform': None,
            'category': None,
            'sub_category': None,
            'room_id': None,
            'streamer_uin': None,
            'stream_start': None,
            'stream_stop': None,
            'streamer_class': None,
            'is_game': None
        }
        spec.update(keys)
        response = self.collection.update(
            spec=spec,
            document={'$set': data},
            upsert=True,
        )
        return response

    def update_streaming_report(self, room_id, streamer_uin, stream_start, stream_stop,
                                users=None, users_3min=None, view_length=None):

        report_date = datetime.combine(stream_start.date(), datetime.min.time())

        data = {}
        if users is not None:
            data['users'] = len(users)
            data['users_list'] = users
        if users_3min is not None:
            data['users_3min'] = len(users_3min)
            data['users_3min_list'] = users_3min
        if view_length is not None:
            data['view_length'] = view_length

        return self._update_report(
            keys={
                'report_type': ReportType.STREAMING,
                'report_date': report_date,
                'room_id': room_id,
                'streamer_uin': streamer_uin,
                'stream_start': stream_start,
                'stream_stop': stream_stop,
            },
            data=data
        )

    def update_hourly_report(self, report_date,
                             platform=None, category=None, sub_category=None, is_game=None,
                             room_id=None, streamer_uin=None, streamer_class=None,
                             users=None, users_3min=None, view_length=None, comments=None,
                             users_list=None, users_3min_list=None):
        if sub_category:
            category = sub_category / 100

        data = {}
        if users is not None:
            data['users'] = users
        if users_list is not None:
            data['users_list'] = users_list

        if users_3min is not None:
            data['users_3min'] = users_3min
        if users_3min_list is not None:
            data['users_3min_list'] = users_3min_list

        if view_length is not None:
            data['view_length'] = view_length
        if comments is not None:
            data['comments'] = comments

        return self._update_report(
            keys={
                'report_type': ReportType.HOURLY_REPORT,
                'report_date': report_date,
                'platform': platform,
                'category': category,
                'sub_category': sub_category,
                'room_id': room_id,
                'streamer_uin': streamer_uin,
                'streamer_class': streamer_class,
                'is_game': is_game
            },
            data=data,
        )

    def get_hourly_report(self, dt_from, delta, fields):
        dt_to = dt_from + delta

        params = {
            "platform": None,
            "category": None,
            "sub_category": None,
            "is_game": None,
            "room_id": None,
            "streamer_uin": None,
            "streamer_class": None,
        }

        limits = {
            '_id': False,
            'stream_start': False,
            'stream_stop': False,
            'report_type': False,
            'report_date': False,
            'users': False,
            'users_3min': False,
        }

        if fields is not None:
            for field in fields:
                params[field] = {'$ne': None}
            if 'sub_category' in fields:
                params['category'] = {'$ne': None}

        for k, v in params.iteritems():
            if v is None:
                limits[k] = False

        params['report_type'] = ReportType.HOURLY_REPORT
        params['report_date'] = {"$gte": dt_from, "$lt": dt_to}

        cursor = self.collection.find(params.copy(), limits.copy())
        result = list(cursor)
        return result

    def get_users(self, dt_from, delta,
                  platform=False, category=False,
                  sub_category=False, is_game=False,
                  room_id=False, streamer_uin=False,
                  streamer_class=False):
        pre_pipeline = [
            {'$unwind': {
                'path': "$users_list",
            }},
        ]
        group = {
            "users_list": {"$addToSet": "$users_list"},
        }
        project_params = {
            "users_list": {'$setDifference': ["$users_list", [None, ]]},
        }
        return self._group_hourly_report(dt_from, delta, group,
                                         pre_pipeline=pre_pipeline,
                                         project_params=project_params,
                                         platform=platform,
                                         category=category,
                                         sub_category=sub_category,
                                         is_game=is_game,
                                         room_id=room_id,
                                         streamer_uin=streamer_uin,
                                         streamer_class=streamer_class)

    def get_comments(self, dt_from, delta, platform=False, category=False, sub_category=False,
                     is_game=False, room_id=False, streamer_uin=False, streamer_class=False):
        group = {
            "comments": {"$sum": {"$ifNull": ["$comments", 0]}},
        }
        return self._group_hourly_report(dt_from, delta, group,
                                         platform=platform, category=category,
                                         sub_category=sub_category, is_game=is_game, room_id=room_id,
                                         streamer_uin=streamer_uin, streamer_class=streamer_class)

    def _group_hourly_report(self, dt_from, delta, group, pre_pipeline=None, project_params=None,
                             platform=False, category=False, sub_category=False,
                             is_game=False, room_id=False, streamer_uin=False, streamer_class=False):
        dt_to = dt_from + delta

        match = {
            'report_type': ReportType.HOURLY_REPORT,
            'report_date': {"$gte": dt_from, "$lt": dt_to},
            'platform': None if platform is False else {"$ne": None},
            'category': None if category is False else {"$ne": None},
            'sub_category': None if sub_category is False else {"$ne": None},
            'room_id': None if room_id is False else {"$ne": None},
            'streamer_uin': None if streamer_uin is False else {"$ne": None},
            'streamer_class': None if streamer_class is False else {"$ne": None},
            'is_game': None if is_game is False else {"$ne": None}
        }

        group_id = {}
        project_fields = {
            "_id": False,
            "comments": True,
            'view_length': True,
            "users_list": True,
            "users_3min_list": True,
        }
        if project_params is not None:
            project_fields.update(project_params)

        if platform is True:
            group_id['platform'] = "$platform"
            project_fields['platform'] = "$_id.platform"
        if category is True:
            group_id['category'] = "$category"
            project_fields['category'] = "$_id.category"
        if sub_category is True:
            group_id['sub_category'] = "$sub_category"
            project_fields['sub_category'] = "$_id.sub_category"
        if room_id is True:
            group_id['room_id'] = "$room_id"
            project_fields['room_id'] = "$_id.room_id"
        if streamer_uin is True:
            group_id['streamer_uin'] = "$streamer_uin"
            project_fields['streamer_uin'] = "$_id.streamer_uin"
        if streamer_class is True:
            group_id['streamer_class'] = "$streamer_class"
            project_fields['streamer_class'] = "$_id.streamer_class"
        if is_game is True:
            group_id['is_game'] = "$is_game"
            project_fields['is_game'] = "$_id.is_game"

        if len(group_id) == 0:
            group_id = None

        group['_id'] = group_id

        pipeline = [{"$match": match}, ]
        if pre_pipeline is not None:
            pipeline = pipeline + pre_pipeline
        pipeline = pipeline + [{"$group": group}, {"$project": project_fields}, ]

        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
        result = list(cursor)

        return result

    def update_daily_report(self, report_date,
                            platform=None, category=None, sub_category=None, is_game=None,
                            room_id=None, streamer_uin=None, streamer_class=None,

                            a1=None, a1_3min=None, v1=None, c1=None,
                            a7=None, a7_3min=None, v7=None, c7=None,
                            a30=None, a30_3min=None, v30=None, c30=None,

                            rr1=None, oa1=None, rr1_3min=None, oa1_3min=None,
                            rr7=None, oa7=None, rr7_3min=None, oa7_3min=None,
                            rr30=None, oa30=None, rr30_3min=None, oa30_3min=None,

                            rrn1=None, lnu1=None, onu1=None, rrn1_3min=None, lnu1_3min=None, onu1_3min=None,
                            rrn7=None, lnu7=None, onu7=None, rrn7_3min=None, lnu7_3min=None, onu7_3min=None,
                            rrn30=None, lnu30=None, onu30=None, rrn30_3min=None, lnu30_3min=None, onu30_3min=None, ):
        if sub_category:
            category = sub_category / 100

        data = {}

        if a30 is not None:
            report_type = ReportType.REPORT_30_DAYS

            data['v30'] = v30
            data['c30'] = c30
            data['a30'] = a30
            data['a30_3min'] = a30_3min

            data['oa30'] = oa30
            data['oa30_3min'] = oa30_3min
            data['rr30'] = rr30
            data['rr30_3min'] = rr30_3min

            data['lnu30'] = lnu30
            data['lnu30_3min'] = lnu30_3min
            data['onu30'] = onu30
            data['onu30_3min'] = onu30_3min
            data['rrn30'] = rrn30
            data['rrn30_3min'] = rrn30_3min

        elif a7 is not None:
            report_type = ReportType.REPORT_7_DAYS

            data['v7'] = v7
            data['c7'] = c7
            data['a7'] = a7
            data['a7_3min'] = a7_3min

            data['oa7'] = oa7
            data['oa7_3min'] = oa7_3min
            data['rr7'] = rr7
            data['rr7_3min'] = rr7_3min
            
            data['lnu7'] = lnu7
            data['lnu7_3min'] = lnu7_3min
            data['onu7'] = onu7
            data['onu7_3min'] = onu7_3min
            data['rrn7'] = rrn7
            data['rrn7_3min'] = rrn7_3min

        else:
            report_type = ReportType.REPORT_1_DAY

            data['v1'] = v1
            data['c1'] = c1
            data['a1'] = a1
            data['a1_3min'] = a1_3min

            data['oa1'] = oa1
            data['oa1_3min'] = oa1_3min
            data['rr1'] = rr1
            data['rr1_3min'] = rr1_3min

            data['lnu1'] = lnu1
            data['lnu1_3min'] = lnu1_3min
            data['onu1'] = onu1
            data['onu1_3min'] = onu1_3min
            data['rrn1'] = rrn1
            data['rrn1_3min'] = rrn1_3min

        return self._update_report(
            keys={
                'report_type': report_type,
                'report_date': report_date,
                'platform': platform,
                'category': category,
                'sub_category': sub_category,
                'room_id': room_id,
                'streamer_uin': streamer_uin,
                'streamer_class': streamer_class,
                'is_game': is_game
            },
            data=data,
        )
