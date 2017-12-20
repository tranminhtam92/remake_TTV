from datetime import datetime, timedelta

from dateutil import parser

from log_report.collections import ReportCollection
from log_storage.collections import ViewLogCollection, ChatLogCollection
from talktv import utils
from talktv.custom import CustomBaseCommand


class Command(CustomBaseCommand):
    command_name = 'hourly_report'
    help = 'Report hourly active users and total view length'

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

    def read_arguments(self, options):
        """
        Read arguments from `self.options` and store them to variables
        :param options: set by function `add_arguments`
        :return: `dt_from` and `dt_to` to specific the name of log files
        """
        if options['datetime']:
            dt_from = parser.parse(options['datetime'])
        else:
            dt_from = datetime.now() - timedelta(hours=2)
        dt_from = dt_from.replace(minute=0, second=0, microsecond=0)

        if options['to_datetime']:
            dt_to = parser.parse(options['to_datetime'])
        else:
            dt_to = dt_from
        dt_to = dt_to.replace(minute=0, second=0, microsecond=0)

        return dt_from, dt_to

    def handle(self, *args, **options):
        dt_from, dt_to = self.read_arguments(options=options)

        params = []
        dt_time = dt_from
        while dt_time <= dt_to:
            params.append(dt_time)
            dt_time = dt_time + timedelta(hours=1)
        utils.parallel_map_no_return(self.hourly_report, params)
        self._info_log(dt_from, dt_to, 'DONE')

    def hourly_report(self, dt_time):
        log = ViewLogCollection()
        report = ReportCollection()
        self._info_log('hourly_report', dt_time)
        self.report_comments(dt_time)
        self.report_users_and_view_length(dt_time, log, report)

    @staticmethod
    def report_users_and_view_length(dt_time, log, report):
        delta = timedelta(hours=1)

        def report_users_by(fields=None):
            result = log.get_users_and_view_length(dt_from=dt_time, delta=delta, fields=fields)
            for data in result:
                view_length = data['view_length']
                users_list = data['users']
                users_3min_list = data['users_3min']
                params = {
                    'view_length': view_length,
                    'users_3min': len(users_3min_list),
                    'users_3min_list': users_3min_list,
                    'users': len(users_list),
                    'users_list': users_list
                }
                if fields is not None:
                    for field in fields:
                        params[field] = data[field]
                report.update_hourly_report(dt_time, **params)

        report_users_by(None)
        report_users_by(['category'])
        report_users_by(['sub_category'])
        report_users_by(['platform'])
        report_users_by(['platform', 'category'])
        report_users_by(['platform', 'sub_category'])
        report_users_by(['room_id'])
        report_users_by(['room_id', 'platform'])
        report_users_by(['streamer_uin'])
        report_users_by(['streamer_uin', 'platform'])

    @staticmethod
    def report_comments(dt_time):
        log = ChatLogCollection()
        report = ReportCollection()
        delta = timedelta(hours=1)

        # all
        result = log.get_total_comments(dt_from=dt_time, delta=delta)
        report.update_hourly_report(dt_time, comments=result)

        # by room
        result = log.get_total_comments_by_room(dt_from=dt_time, delta=delta)
        for k, v in result.iteritems():
            report.update_hourly_report(dt_time, comments=v, room_id=k)
