from datetime import datetime, timedelta

from dateutil import parser
from dateutil.relativedelta import relativedelta

from log_report.collections import ActiveUsers3MinReportCollection
from log_report.collections import ActiveUsersReportCollection
from log_report.collections import TotalViewLengthReportCollection
from log_storage.collections import ViewLogCollection
from talktv.constants import ReportType
from talktv.custom import CustomBaseCommand


class Command(CustomBaseCommand):
    help = 'Report monthly active users and total view length'
    skip_platform = False

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
            '--skip_platform',
            dest='skip_platform',
            action='store_true',
            default=False,
            help='only google analytics reports',
        )

    def read_arguments(self, options):
        """
        Read arguments from `self.options` and store them to variables
        :param options: set by function `add_arguments`
        :return: `dt_from` and `dt_to` to specific the name of log files
        """
        self.skip_platform = options['skip_platform']

        if options['datetime']:
            dt_from = parser.parse(options['datetime'])
        else:
            dt_from = datetime.now() - relativedelta(months=1)
        dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if options['to_datetime']:
            dt_to = parser.parse(options['to_datetime'])
        else:
            dt_to = dt_from
        dt_to = dt_to.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        return dt_from, dt_to

    def handle(self, *args, **options):
        dt_from, dt_to = self.read_arguments(options=options)
        dt_time = dt_from
        while dt_time <= dt_to:
            self.report_monthly(dt_time)
            dt_time = dt_time + timedelta(days=1)

    def report_monthly(self, dt_time):
        self._immediate_log('report_total_view_length', dt_time)
        self.report_total_view_length(dt_time)
        self._immediate_log('report_active_users', dt_time)
        self.report_active_users(dt_time, False)
        self._immediate_log('report_active_users_3min', dt_time)
        self.report_active_users(dt_time, True)
        self._immediate_log('DONE', dt_time)

    def report_active_users(self, dt_time, is_3min):
        log = ViewLogCollection()

        if is_3min:
            report = ActiveUsers3MinReportCollection()
        else:
            report = ActiveUsersReportCollection()

        delta = relativedelta(months=1)

        result = log.get_active_users_by_streamer(dt_from=dt_time, delta=delta, is_3min=is_3min)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, streamer_uin=k)

        result = log.get_active_users_by_room(dt_from=dt_time, delta=delta, is_3min=is_3min)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, room_id=k)

        if self.skip_platform:
            return

        result = log.get_active_users_by_streamer_and_platform(dt_from=dt_time, delta=delta, is_3min=is_3min)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, streamer_uin=k[0], platform=k[1])

        result = log.get_active_users_by_room_and_platform(dt_from=dt_time, delta=delta, is_3min=is_3min)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, room_id=k[0], platform=k[1])

    def report_total_view_length(self, dt_time):
        log = ViewLogCollection()

        report = TotalViewLengthReportCollection()

        delta = relativedelta(months=1)

        result = log.get_total_view_length_by_streamer(dt_from=dt_time, delta=delta)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, streamer_uin=k)

        result = log.get_total_view_length_by_room(dt_from=dt_time, delta=delta)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, room_id=k)

        if self.skip_platform:
            return

        result = log.get_total_view_length_by_streamer_and_platform(dt_from=dt_time, delta=delta)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, streamer_uin=k[0], platform=k[1])

        result = log.get_total_view_length_by_room_and_platform(dt_from=dt_time, delta=delta)
        for k, v in result.iteritems():
            report.create_report(ReportType.REPORT_30_DAY, dt_time, v, room_id=k[0], platform=k[1])
