import os
import re
from datetime import datetime, timedelta

from dateutil import parser
from decouple import config

from log_storage.collections import ChatLogCollection
from log_storage.models import Result
from log_storage.serializers import ChatLogSerializer
from talktv import utils
from talktv.constants import LOG_CHAT_KEYS
from talktv.custom import CustomBaseCommand

LOG_65535_DIR = config('LOG_65535_DIR')
LOG_CHAT_DIR = config('LOG_CHAT_DIR')


class Command(CustomBaseCommand):
    help = 'Parse chat log files and store data to Mongo database'
    skip_convert = False

    log_65535_dir = LOG_65535_DIR
    log_chat_dir = LOG_CHAT_DIR

    result = None
    documents = []
    success_rows = 0

    # following attributes are used for cache

    room_extend = {}

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
            '--skip_convert',
            dest='skip_convert',
            action='store_true',
            default=False,
            help='Skip converting 65535 files',
        )

    def read_arguments(self, options):
        """
        Read arguments from `self.options` and store them to variables
        :param options: set by function `add_arguments`
        :return: `dt_from` and `dt_to` to specific the name of log files
        """
        self.skip_convert = options['skip_convert']

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
        log_files = self.check_log_files(dt_from=dt_from, dt_to=dt_to)
        log_files.sort()
        utils.parallel_map(self.parse_log_file, log_files)

    def check_log_files(self, dt_from, dt_to):

        """
        Check and return the path of files which will be parsed
        :return: list of the full path of files
        """
        params = []
        dt_temp = dt_from
        while dt_temp <= dt_to:
            params.append(dt_temp)
            dt_temp = dt_temp + timedelta(hours=1)
        result = utils.parallel_map(self.check_log_file, params=params)
        log_files = []
        for file_path in result:
            if file_path:
                log_files.append(file_path)
        return log_files

    def check_log_file(self, dt_time):
        """
        Convert file or check file at `dt_time`
        :return: the full path of file if success, None if fail
        """
        log_65535 = '65535_{}{}{}_{}00.log'.format(dt_time.year,
                                                   str(dt_time.month).zfill(2),
                                                   str(dt_time.day).zfill(2),
                                                   str(dt_time.hour).zfill(2))
        log_65535 = os.path.join(self.log_65535_dir, log_65535)
        log_chat = 'Chat_{}{}{}_{}00.txt'.format(dt_time.year,
                                                 str(dt_time.month).zfill(2),
                                                 str(dt_time.day).zfill(2),
                                                 str(dt_time.hour).zfill(2))
        log_chat = os.path.join(self.log_chat_dir, log_chat)
        try:
            if self.skip_convert:
                if os.path.isfile(log_chat):
                    return log_chat
                else:
                    return None

            read_file = open(log_65535, 'r')
            content = read_file.readlines()
            read_file.close()
            if len(content) == 0:
                result = Result.objects.create_result_for_log_chat(file_path=log_chat, total_rows=0)
                result.set_system_log(log_65535, 'is empty')
                result.finish_log_chat()
                self._error_log(log_65535, 'is empty')
                return None
            content = [r.strip() for r in content]
            content = list(set(content))
            write_content = []
            for row in content:
                if 'Chat' in row:
                    write_content.append(row + '\n')
            write_file = open(log_chat, 'w+')
            write_file.writelines(write_content)
            write_file.close()
            return log_chat
        except Exception, e:
            result = Result.objects.create_result_for_log_chat(file_path=log_chat, total_rows=0)
            result.set_system_log(log_65535, type(e), e)
            result.finish_log_chat()
            self._error_log(log_65535, type(e), e)
            return None

    def parse_log_file(self, log_file):
        '''
        Parse log data from `log_file`
        '''
        log_data = self.get_log_data(log_file=log_file)
        if not log_data:
            return

        tmp_time = datetime.now()
        self.result = Result.objects.create_result_for_log_chat(file_path=log_file, total_rows=len(log_data))

        log_file = os.path.basename(log_file)

        self.parse_documents_from_log_data(log_file=log_file, log_data=log_data)

        tmp_time = datetime.now() - tmp_time
        messages = [log_file, 'FINISH', round(self.success_rows * 100.0 / len(log_data), 4),
                    'in', tmp_time, '(', self.success_rows, '/', len(log_data), 'rows)']

        self.result.finish_log_chat(success_rows=self.success_rows, task_time=int(tmp_time.total_seconds()))

    def get_log_data(self, log_file):
        try:
            f = open(log_file, 'r')
            content = f.readlines()
            f.close()
            content = [r.strip() for r in content]
            if len(content) == 0:
                result = Result.objects.create_result_for_log_chat(file_path=log_file, total_rows=0)
                result.set_system_log(log_file, 'is empty')
                result.finish_log_chat()
                self._warning_log(log_file, 'is empty')
                return None
            return content
        except Exception, e:
            result = Result.objects.create_result_for_log_chat(file_path=log_file, total_rows=0)
            result.set_system_log(log_file, type(e), e)
            result.finish_log_chat()
            self._error_log(log_file, type(e), e)
            return None

    def parse_documents_from_log_data(self, log_file, log_data):
        """
        Load documents from log data and save them to `self.documents`
        """
        self.documents = []
        self.success_rows = 0
        for row, row_data in enumerate(log_data):
            params = (log_file, row, row_data)
            self.try_catch_parse_data(params)
        self.success_rows = self.save_documents(self.documents)

    def save_documents(self, documents):
        collection = ChatLogCollection()
        inserted_id = collection.create_many(documents)
        return len(inserted_id)

    def try_catch_parse_data(self, params, retry=False):
        """
        This function is used for try-catch exception and retry if 'MySQL server has gone away'
        """
        log_file, row, row_data = params
        try:
            return self.parse_data(log_file, row, row_data)
        except Exception as e:
            if not retry:
                return self.try_catch_parse_data(params, True)
            else:
                return None

    def parse_data(self, log_file, row, row_data):
        """
        Parse data when parse multi rows in parallel (no cache)
        """
        head_data = row_data.strip().split('|', 12)[:12]
        tail_data = row_data.strip().split('|', 12)[12].rsplit("|", 5)[1:]
        mid_data = row_data.strip().split('|', 12)[12].rsplit("|", 5)[0]

        name_in_room, content = re.split(r"\|\d+\|\d+\.\d+\.\d+\.\d+\|\d+\|\d+\-\d+\-\d+\ \d+\:\d+\:\d+\|", mid_data)
        mid_data = mid_data.replace(name_in_room, "").replace(content, "")
        mid_data = mid_data[1:-1].split("|")

        data = head_data + [name_in_room] + mid_data + [content] + tail_data
        data = dict(zip(LOG_CHAT_KEYS, data))
        serializer = ChatLogSerializer(data=data)
        if serializer.is_valid():
            valid_data = serializer.validated_data
        else:
            self._warning_log(log_file, row, serializer.errors)
            return

        valid_data['_row'] = row
        valid_data['_log_file'] = os.path.basename(log_file)
        self.documents.append(valid_data)
