from datetime import datetime, timedelta

from dateutil import parser

from log_storage.collections import RevenueLogCollection
from log_storage.serializers import BillingLogSerializer
from log_storage.support_models import ZTalkTransactionLogDB, ZTalkBillingLogDB
from talktv.custom import CustomBaseCommand


class Command(CustomBaseCommand):
    help = 'Get revenue from MySQL dbs and store data to Mongo database'

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
            dt_from = datetime.now() - timedelta(hours=1)
        dt_from = dt_from.replace(minute=0, second=0, microsecond=0)

        if options['to_datetime']:
            dt_to = parser.parse(options['to_datetime'])
        else:
            dt_to = dt_from
        dt_to = dt_to.replace(minute=59, second=59, microsecond=999999)
        return dt_from, dt_to

    # Main flow

    def handle(self, *args, **options):
        dt_from, dt_to = self.read_arguments(options=options)
        print dt_from, dt_to
        col = RevenueLogCollection()
        # # Revenue
        billing_list = ZTalkBillingLogDB.objects.get_billing_log(dt_from, dt_to)
        # print billing_list

        for billing_info in billing_list:
            print type(billing_info.commit_time)
            document = BillingLogSerializer(billing_info).data
            print type(document["commit_time"])

            client_type_log = ZTalkTransactionLogDB.objects.get_client_type(document['bill_id'])
            document["commit_time"] = billing_info.commit_time
            document["client_type"] = None
            if client_type_log is not None:
                print 'client_type_log', client_type_log.client_type
                document["client_type"] = client_type_log.client_type

            document["_id"] = document["bill_id"]
            try:
                col.create(document)
            except Exception, e:
                self._error_log('Parse revenue log', e)
                # raise e

        self._info_log('Parse revenue log', dt_from, dt_to)

