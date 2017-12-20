import traceback
from datetime import datetime, timedelta

import pandas as pd
from dateutil import parser
from decouple import config
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from log_report.collections import ReportCollection
from log_storage.collections import ViewLogCollection
from log_storage.support_models import StreamTimeDBTalkTV, StreamTimeNewDBTalkTV, StreamerInfo
from talktv import utils
from talktv.constants import Platform
from talktv.custom import CustomBaseCommand

GA_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
GA_KEY_FILE = config('GA_KEY_FILE')
GA_VIEW_ID = config('GA_VIEW_ID')


class Command(CustomBaseCommand):
    command_name = 'daily_report'
    help = 'Daily report at 3:00 AM'
    streamer_uins_in_class = {}
    ga = False

    # Set up arguments

    def add_arguments(self, parser):
        parser.add_argument(
            '--from',
            dest='datetime',
            default=None,
            type=str,
            help='start time by datetime (e.g. 2017-06-23 00:00:00)',
        )
        parser.add_argument(
            '--to',
            dest='to_datetime',
            default=None,
            type=str,
            help='stop time by datetime (e.g. 2017-06-23 00:00:00)',
        )
        parser.add_argument(
            '--ga',
            dest='ga',
            action='store_true',
            default=False,
            help='get reports from Google Analytics',
        )

    def read_arguments(self, options):
        """
        Read arguments from `self.options` and store them to variables
        :param options: set by function `add_arguments`
        :return: `dt_from` and `dt_to` to specific the name of log files
        """
        self.ga = options['ga']

        if options['datetime']:
            dt_from = parser.parse(options['datetime'])
        else:
            dt_from = datetime.now() - timedelta(days=1)
        dt_from = dt_from.replace(hour=0, minute=0, second=0, microsecond=0)

        if options['to_datetime']:
            dt_to = parser.parse(options['to_datetime'])
        else:
            dt_to = dt_from
        dt_to = dt_to.replace(hour=0, minute=0, second=0, microsecond=0)

        return dt_from, dt_to

    def handle(self, *args, **options):
        dt_from, dt_to = self.read_arguments(options=options)

        if self.ga:
            self.google_analytics_reports(dt_from - timedelta(days=3), dt_to)
            self._info_log(dt_from, dt_to, 'google_analytics_reports', 'done')
            return

        for streamer_class in range(1, 5):
            self.get_streamer_uins_in_class(streamer_class)

        params = []
        dt_time = dt_from
        while dt_time <= dt_to:
            params.append(dt_time)
            dt_time = dt_time + timedelta(days=1)
        utils.parallel_map_no_return(self.report_daily, params)

    def report_daily(self, dt_time):
        log = ViewLogCollection()
        report = ReportCollection()

        self._info_log(dt_time.date(), '1 day', 'START')
        self.report_users_view_length_and_comments(report, dt_time, 1, None)
        self.report_users_view_length_and_comments(report, dt_time, 1, ['category'])
        self.report_users_view_length_and_comments(report, dt_time, 1, ['sub_category'])
        self.report_users_view_length_and_comments(report, dt_time, 1, ['platform'])
        self.report_users_view_length_and_comments(report, dt_time, 1, ['platform', 'category'])
        self.report_users_view_length_and_comments(report, dt_time, 1, ['platform', 'sub_category'])
        self.report_users_view_length_and_comments(report, dt_time, 1, ['room_id'])
        self._info_log(dt_time.date(), '1 day', '50/100')
        self.streamer_class_reports(log=log, report=report, dt_time=dt_time, days=1, is_game=False)
        self.streamer_class_reports(log=log, report=report, dt_time=dt_time, days=1, is_game=True)
        self._info_log(dt_time.date(), '1 day', 'DONE')

        self._info_log(dt_time.date(), '7 days', 'START')
        self.report_users_view_length_and_comments(report, dt_time, 7, None)
        self.report_users_view_length_and_comments(report, dt_time, 7, ['category'])
        self.report_users_view_length_and_comments(report, dt_time, 7, ['sub_category'])
        self.report_users_view_length_and_comments(report, dt_time, 7, ['platform'])
        self.report_users_view_length_and_comments(report, dt_time, 7, ['platform', 'category'])
        self.report_users_view_length_and_comments(report, dt_time, 7, ['platform', 'sub_category'])
        self._info_log(dt_time.date(), '7 days', '50/100')
        self.streamer_class_reports(log=log, report=report, dt_time=dt_time, days=7, is_game=False)
        self.streamer_class_reports(log=log, report=report, dt_time=dt_time, days=7, is_game=True)
        self._info_log(dt_time.date(), '7 days', 'DONE')

        self._info_log(dt_time.date(), '30 days', 'START')
        self.report_users_view_length_and_comments(report, dt_time, 30, None)
        self.report_users_view_length_and_comments(report, dt_time, 30, ['category'])
        self.report_users_view_length_and_comments(report, dt_time, 30, ['sub_category'])
        self.report_users_view_length_and_comments(report, dt_time, 30, ['platform'])
        self.report_users_view_length_and_comments(report, dt_time, 30, ['platform', 'category'])
        self.report_users_view_length_and_comments(report, dt_time, 30, ['platform', 'sub_category'])
        self._info_log(dt_time.date(), '30 days', '50/100')
        self.streamer_class_reports(log=log, report=report, dt_time=dt_time, days=30, is_game=False)
        self.streamer_class_reports(log=log, report=report, dt_time=dt_time, days=30, is_game=True)
        self._info_log(dt_time.date(), '30 days', 'DONE')

        self.streaming_report(log=log, report=report, dt_time=dt_time, days=1)
        self._info_log(dt_time.date(), 'SUCCESS')

    @staticmethod
    def report_comments(report_col, dt_time, days):
        delta = timedelta(days=days)
        dt_from = dt_time + timedelta(days=1) - delta

        def create_params(_data, _fields=None):
            _comments = _data['comments']
            if delta == timedelta(days=30):
                _params = {
                    'c30': _comments,
                }
            elif delta == timedelta(days=7):
                _params = {
                    'c7': _comments,
                }
            else:
                _params = {
                    'c1': _comments,
                }
            if _fields is not None:
                for _f in _fields:
                    _params[_f] = _data[_f]
            return _params

        # all
        result = report_col.get_comments(dt_from=dt_from, delta=delta)
        params = create_params(result[0])
        report_col.update_daily_report(dt_time, **params)

        # by room
        result = report_col.get_comments(dt_from=dt_from, delta=delta,
                                         room_id=True)
        for data in result:
            params = create_params(data, ['room_id'])
            report_col.update_daily_report(dt_time, **params)

    @staticmethod
    def report_users_view_length_and_comments(report, dt_time, days, fields=None):
        delta = timedelta(days=days)
        dt_from = dt_time + timedelta(days=1) - delta

        df = report.get_hourly_report(dt_from=dt_from, delta=delta, fields=fields)
        df = pd.DataFrame(df)
        if 'comments' in df.columns:
            df['comments'].fillna(0, inplace=True)
        else:
            df['comments'] = [0] * len(df)
        df['view_length'].fillna(0, inplace=True)
        df['users_list'] = df['users_list'].apply(lambda c: c if isinstance(c, list) else [])
        df['users_3min_list'] = df['users_3min_list'].apply(lambda c: c if isinstance(c, list) else [])

        if fields is None:
            df = df.sum()
            result = [df.to_dict(), ]
        else:
            df = df.groupby(fields) \
                .agg({'comments': 'sum',
                      'users_list': 'sum',
                      'users_3min_list': 'sum',
                      'view_length': 'sum'}) \
                .reset_index()
            result = df.T.to_dict().values()

        if (delta != timedelta(days=30)) and \
                ((fields is None) or ('room_id' not in fields and 'streamer_uin' not in fields)):
            # Old Data for Retention Rate
            if fields is None:
                old_df = report.get_hourly_report(dt_from=dt_from - delta, delta=delta, fields=fields)
                old_df = pd.DataFrame(old_df)
                old_df = old_df[['users_list', 'users_3min_list']]
                old_df['users_list'] = old_df['users_list'].apply(lambda c: c if isinstance(c, list) else [])
                old_df['users_3min_list'] = old_df['users_3min_list'].apply(lambda c: c if isinstance(c, list) else [])
                old_df = old_df.sum().to_dict()
            else:
                old_df = report.get_hourly_report(dt_from=dt_from - delta, delta=delta, fields=fields)
                old_df = pd.DataFrame(old_df)
                old_df = old_df[fields + ['users_list', 'users_3min_list']]
                old_df['users_list'] = old_df['users_list'].apply(lambda c: c if isinstance(c, list) else [])
                old_df['users_3min_list'] = old_df['users_3min_list'].apply(lambda c: c if isinstance(c, list) else [])
                old_df = old_df.groupby(fields) \
                    .agg({'users_list': 'sum',
                          'users_3min_list': 'sum'}) \
                    .reset_index()

            # Old Data 2 for Retention Rate New User
            if fields is None:
                old_df2 = report.get_hourly_report(dt_from=dt_from - delta - delta, delta=delta, fields=fields)
                old_df2 = pd.DataFrame(old_df2)
                old_df2 = old_df2[['users_list', 'users_3min_list']]
                old_df2['users_list'] = old_df2['users_list'].apply(lambda c: c if isinstance(c, list) else [])
                old_df2['users_3min_list'] = old_df2['users_3min_list'].apply(
                    lambda c: c if isinstance(c, list) else [])
                old_df2 = old_df2.sum().to_dict()
            else:
                old_df2 = report.get_hourly_report(dt_from=dt_from - delta - delta, delta=delta, fields=fields)
                old_df2 = pd.DataFrame(old_df2)
                old_df2 = old_df2[fields + ['users_list', 'users_3min_list']]
                old_df2['users_list'] = old_df2['users_list'].apply(lambda c: c if isinstance(c, list) else [])
                old_df2['users_3min_list'] = old_df2['users_3min_list'].apply(
                    lambda c: c if isinstance(c, list) else [])
                old_df2 = old_df2.groupby(fields) \
                    .agg({'users_list': 'sum',
                          'users_3min_list': 'sum'}) \
                    .reset_index()

            if delta == timedelta(days=7):
                oa_key = 'oa7'  # Old Active User
                rr_key = 'rr7'  # Retention Rate

                onu_key = 'onu7'  # New User in Old Data
                lnu_key = 'lnu7'  # Old New User in Old Data
                rrn_key = 'rrn7'
            else:
                oa_key = 'oa1'  # Old Active User
                rr_key = 'rr1'  # Retention Rate

                onu_key = 'onu1'  # New User in Old Data
                lnu_key = 'lnu1'  # Old New User in Old Data
                rrn_key = 'rrn1'

        for data in result:
            comments = data['comments']
            view_length = data['view_length']
            users_set = set(data['users_list'])
            users_3min_set = set(data['users_3min_list'])

            if delta == timedelta(days=30):
                params = {
                    'c30': comments,
                    'v30': view_length,
                    'a30': len(users_set),
                    'a30_3min': len(users_3min_set),
                }
            elif delta == timedelta(days=7):
                params = {
                    'c7': comments,
                    'v7': view_length,
                    'a7': len(users_set),
                    'a7_3min': len(users_3min_set),
                }
            else:
                params = {
                    'c1': comments,
                    'v1': view_length,
                    'a1': len(users_set),
                    'a1_3min': len(users_3min_set),
                }

            # Retention Rate
            if (delta != timedelta(days=30)) and \
                    ((fields is None) or ('room_id' not in fields and 'streamer_uin' not in fields)):
                if fields is None:
                    old_data = old_df
                    old_data2 = old_df2
                else:
                    query_params = []
                    for field in fields:
                        query_params.append("{} == {}".format(field, data[field]))
                    query_params = " and ".join(query_params)

                    old_data = old_df.query(query_params)
                    if len(old_data) == 1:
                        old_data = old_data.to_dict(orient='records')[0]
                    else:
                        old_data = {}

                    old_data2 = old_df2.query(query_params)
                    if len(old_data2) == 1:
                        old_data2 = old_data2.to_dict(orient='records')[0]
                    else:
                        old_data2 = {}

                # Retention Rate
                if len(old_data) > 0:
                    params[oa_key] = len(set(old_data['users_list']) & users_set)
                    params[oa_key + '_3min'] = len(set(old_data['users_3min_list']) & users_3min_set)
                else:
                    params[oa_key] = 0
                    params[oa_key + '_3min'] = 0

                params[rr_key] = round(params[oa_key] * 100.0 / len(users_set), 2) if len(users_set) > 0 else 0.0
                params[rr_key + '_3min'] = round(params[oa_key + '_3min'] * 100.0 / len(users_3min_set), 2) \
                    if len(users_3min_set) > 0 else 0.0

                # Retention Rate New User
                if len(old_data) > 0:
                    if len(old_data2) > 0:
                        last_new_user = set(old_data['users_list']) - set(old_data2['users_list'])
                        last_new_user_3min = set(old_data['users_3min_list']) - set(old_data2['users_3min_list'])
                    else:
                        last_new_user = set(old_data['users_list'])
                        last_new_user_3min = set(old_data['users_3min_list'])
                else:
                    last_new_user = set()
                    last_new_user_3min = set()

                old_new_user = last_new_user & users_set
                old_new_user_3min = last_new_user_3min & users_3min_set

                params[onu_key] = len(old_new_user)
                params[lnu_key] = len(last_new_user)
                params[rrn_key] = params[onu_key] * 100.0 / params[lnu_key] if params[lnu_key] > 0 else 0.0
                params[onu_key + '_3min'] = len(old_new_user_3min)
                params[lnu_key + '_3min'] = len(last_new_user_3min)
                params[rrn_key + '_3min'] = params[onu_key + '_3min'] * 100.0 / params[lnu_key + '_3min'] \
                    if params[lnu_key + '_3min'] > 0 else 0.0

            if fields is not None:
                for field in fields:
                    params[field] = data[field]
            report.update_daily_report(dt_time, **params)

    # Streamer Class Reports

    def get_streamer_uins_in_class(self, streamer_class):
        if streamer_class not in self.streamer_uins_in_class:
            rank_ids = StreamerInfo.objects.get_streamer_uins_in_class(streamer_class)
            self.streamer_uins_in_class[streamer_class] = rank_ids
        return self.streamer_uins_in_class[streamer_class]

    def streamer_class_reports(self, log, report, dt_time, days, is_game):
        delta = timedelta(days=days)
        dt_from = dt_time + timedelta(days=1) - delta

        for streamer_class in range(1, 5):
            streamer_uins = self.get_streamer_uins_in_class(streamer_class)
            data = log.get_users_and_view_length_for_streamer_class(dt_from, delta, streamer_uins, is_game)
            if len(data) == 0:
                continue
            data = data[0]
            users = data['users']
            users_3min = data['users_3min']
            view_length = data['view_length']

            if delta == timedelta(days=30):
                report.update_daily_report(dt_time, is_game=is_game, streamer_class=streamer_class,
                                           a30=len(users), rr30=users,
                                           a30_3min=len(users_3min), rr30_3min=users_3min,
                                           v30=view_length)
            elif delta == timedelta(days=7):
                report.update_daily_report(dt_time, is_game=is_game, streamer_class=streamer_class,
                                           a7=len(users), rr7=users,
                                           a7_3min=len(users_3min), rr7_3min=users_3min,
                                           v7=view_length)
            else:
                report.update_daily_report(dt_time, is_game=is_game, streamer_class=streamer_class,
                                           a1=len(users), rr1=users,
                                           a1_3min=len(users_3min), rr1_3min=users_3min,
                                           v1=view_length)

    # streaming reports

    def streaming_report(self, log, report, dt_time, days):
        delta = timedelta(days=days)
        dt_to = dt_time + timedelta(days=1)
        dt_from = dt_to - delta

        stream_ids = StreamTimeNewDBTalkTV.objects.get_ids(dt_from=dt_from, dt_to=dt_to)
        stream_ids = stream_ids + StreamTimeDBTalkTV.objects.get_ids(dt_from=dt_from, dt_to=dt_to)
        stream_ids = list(set(stream_ids))

        result = log.get_users_and_view_length_by_streaming_event(stream_ids)
        for data in result:
            try:
                streamer_uin = data['streamer_uin']
                room_id = data['room_id']
                stream_table = data['stream_table']
                stream_id = data['stream_id']
                if stream_table == 'stream_time_new':
                    info = StreamTimeNewDBTalkTV.objects.get(id=stream_id)
                else:
                    info = StreamTimeDBTalkTV.objects.get(id=stream_id)

                report.update_streaming_report(room_id, streamer_uin, info.start_time, info.stop_time,
                                               users_3min=data['users_3min'],
                                               users=data['users'],
                                               view_length=data['view_length'])
            except:
                self._info_log(traceback.format_exc())

    # Google Analytics

    @staticmethod
    def report_ga(analytics, date_ranges, metrics, days):
        report_col = ReportCollection()
        response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': GA_VIEW_ID,
                        'dateRanges': date_ranges,
                        'metrics': metrics,
                        'dimensions': [{'name': 'ga:date'}]
                    }]
            }
        ).execute()

        for report in response.get('reports', []):
            for row in report.get('data', {}).get('rows', []):
                report_date = row.get('dimensions', [])[0]
                report_date = parser.parse(report_date)
                v = int(row.get('metrics', [])[0].get('values', [])[0])
                if days == 30:
                    report_col.update_daily_report(report_date, platform=Platform.GOOGLE_ANALYTICS, a30=v)
                elif days == 7:
                    report_col.update_daily_report(report_date, platform=Platform.GOOGLE_ANALYTICS, a7=v)
                else:
                    report_col.update_daily_report(report_date, platform=Platform.GOOGLE_ANALYTICS, a1=v)

    def google_analytics_reports(self, dt_from, dt_to):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GA_KEY_FILE, GA_SCOPES)
        analytics = build('analyticsreporting', 'v4', credentials=credentials)
        date_ranges = [{'startDate': dt_from.strftime('%Y-%m-%d'), 'endDate': dt_to.strftime('%Y-%m-%d')}]
        self.report_ga(analytics, date_ranges, [{'expression': 'ga:1dayUsers'}], 1)
        self.report_ga(analytics, date_ranges, [{'expression': 'ga:7dayUsers'}], 7)
        self.report_ga(analytics, date_ranges, [{'expression': 'ga:30dayUsers'}], 30)
