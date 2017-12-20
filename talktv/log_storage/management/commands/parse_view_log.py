import json
import os
import traceback
from datetime import datetime, timedelta

from bson import json_util
from dateutil import parser
from decouple import config
from django.db import close_old_connections

from log_storage.collections import MobileLogCollection
from log_storage.collections import MobileTempLogCollection
from log_storage.collections import ViewLogCollection
from log_storage.models import Result, ErrorDetail
from log_storage.serializers import MobileEnterLogSerializer, MobileQuitLogSerializer, LeaveRoomLogSerializer
from log_storage.support_models import get_RoomShow_model, RoomExtend, StreamTimeNewDBTalkTV, StreamTimeDBTalkTV
from talktv import utils
from talktv.constants import CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS
from talktv.constants import CONVERT_IDOL_CATEGORY_IDS
from talktv.constants import CONVERT_LM_CATEGORY_IDS
from talktv.constants import Category
from talktv.constants import DANCE_ROOM_IDS
from talktv.constants import LOG_LEAVEROOM_KEYS
from talktv.constants import LOG_MOBILE_ENTER_KEYS
from talktv.constants import LOG_MOBILE_KEYS
from talktv.constants import LOG_MOBILE_QUIT_KEYS
from talktv.constants import MAX_STREAM_LENGTH
from talktv.constants import PCGameID
from talktv.constants import SubCategory
from talktv.custom import CustomBaseCommand

LOG_65535_DIR = config('LOG_65535_DIR')
LOG_LEAVEROOM_DIR = config('LOG_LEAVEROOM_DIR')

LOG_MOBILEPROXY_DIR = config('LOG_MOBILEPROXY_DIR')
LOG_MOBILE_DIR = config('LOG_MOBILE_DIR')

TRACKING_PARSE_LOG_DIR = config('TRACKING_PARSE_LOG_DIR')


def remove_tracking_file(log_file):
    path = os.path.join(TRACKING_PARSE_LOG_DIR, log_file)
    if os.path.exists(path):
        os.remove(path)


def tracking_parse_log(log_file, row):
    with open(os.path.join(TRACKING_PARSE_LOG_DIR, log_file), 'a+') as f:
        f.write(str(row) + '\n')


class Command(CustomBaseCommand):
    help = 'Parse log files and store data to Mongo database'
    skip_convert = False
    mobile_proxy = False

    result = None
    documents = None
    skip_rows = None

    log_base_dir = LOG_65535_DIR
    log_dist_dir = LOG_LEAVEROOM_DIR
    log_type = Result.LOG_VIEW

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
            '--mobile_proxy',
            dest='mobile_proxy',
            action='store_true',
            default=False,
            help='parse mobile proxy logs',
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
        self.mobile_proxy = options['mobile_proxy']
        self.skip_convert = options['skip_convert']

        if self.mobile_proxy:
            self.log_base_dir = LOG_MOBILEPROXY_DIR
            self.log_dist_dir = LOG_MOBILE_DIR
            self.log_type = Result.LOG_MOBILE

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
        for log_file in log_files:
            self.parse_log_file(log_file)

    def check_log_files(self, dt_from, dt_to):
        """
        Check and return the path of files which will be parsed
        :return: list of the full path of files
        """
        if self.mobile_proxy:
            result = []
            dt_time = dt_from
            while dt_time <= dt_to:
                file_path = self.check_log_file(dt_time)
                dt_time = dt_time + timedelta(hours=1)
                if file_path:
                    result.append(file_path)
            return result
        else:
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
            log_files.sort()
            return log_files

    def check_log_file(self, dt_time):
        """
        Convert file or check file at `dt_time`
        :return: the full path of file if success, None if fail
        """
        log_base = '65535_{}{}{}_{}00.log'.format(dt_time.year,
                                                  str(dt_time.month).zfill(2),
                                                  str(dt_time.day).zfill(2),
                                                  str(dt_time.hour).zfill(2))
        log_base = os.path.join(self.log_base_dir, log_base)
        if self.mobile_proxy:
            log_dist = 'Mobile_{}{}{}_{}00.txt'.format(dt_time.year,
                                                       str(dt_time.month).zfill(2),
                                                       str(dt_time.day).zfill(2),
                                                       str(dt_time.hour).zfill(2))
        else:
            log_dist = 'LeaveRoom_{}{}{}_{}00.txt'.format(dt_time.year,
                                                          str(dt_time.month).zfill(2),
                                                          str(dt_time.day).zfill(2),
                                                          str(dt_time.hour).zfill(2))
        log_dist = os.path.join(self.log_dist_dir, log_dist)
        try:
            if self.skip_convert:
                if os.path.isfile(log_dist):
                    return log_dist
                else:
                    return None

            read_file = open(log_base, 'r')
            content = read_file.readlines()
            read_file.close()
            if len(content) == 0:
                result = Result.objects.create_result_for_log(log_type=self.log_type, file_path=log_dist, total_rows=0)
                result.set_system_log(log_base, 'is empty')
                result.finish()
                self._error_log(log_base, 'is empty')
                return None
            content = [r.strip() for r in content]
            write_content = []
            if self.mobile_proxy:
                for row in content:
                    if 'QTXEnterRoom' in row or 'QTXQuitRoom' in row:
                        write_content.append(row)
                write_content = self.build_content_for_mobile_proxy_log(write_content, log_base)
                if write_content:
                    write_file = open(log_dist, 'w+')
                    write_file.writelines(write_content)
                    write_file.close()
                    return log_dist
                return None
            else:
                for row in content:
                    if 'LeaveRoom2' in row:
                        write_content.append(str(row) + '\n')
                write_file = open(log_dist, 'w+')
                write_file.writelines(write_content)
                write_file.close()
                return log_dist
        except Exception, e:
            result = Result.objects.create_result_for_log(log_type=self.log_type, file_path=log_dist, total_rows=0)
            result.set_system_log(log_base, type(e), e, traceback.format_exc())
            result.finish()
            self._error_log(log_base, type(e), e)
            return None

    @staticmethod
    def build_content_for_mobile_proxy_log(content, log_file):
        temp_col = MobileTempLogCollection()
        temp_logs = temp_col.pop_all_temp_log()
        temp_dict = {}
        for temp_log in temp_logs:
            k = "|".join([
                str(temp_log['uin']),
                str(temp_log['login_ip']),
                str(temp_log['platform']),
                str(temp_log['room_id']),
                str(temp_log['room_mode']),
            ])
            temp_dict[k] = temp_log

        new_content = []
        for row_data in content:
            data = row_data.split('|')
            # if this is EnterRoom, save data in temp collection
            if 'QTXEnterRoom' in row_data:
                data = dict(zip(LOG_MOBILE_ENTER_KEYS, data))
                serializer = MobileEnterLogSerializer(data=data)
                if serializer.is_valid():
                    valid_data = serializer.validated_data
                    valid_data['_file_enter'] = os.path.basename(log_file)
                    k = "|".join([
                        str(valid_data['uin']),
                        str(valid_data['login_ip']),
                        str(valid_data['platform']),
                        str(valid_data['room_id']),
                        str(valid_data['room_mode']),
                    ])
                    temp_dict[k] = valid_data
            else:
                data = dict(zip(LOG_MOBILE_QUIT_KEYS, data))
                serializer = MobileQuitLogSerializer(data=data)
                if serializer.is_valid():
                    valid_data = serializer.validated_data
                    k = "|".join([
                        str(valid_data['uin']),
                        str(valid_data['login_ip']),
                        str(valid_data['platform']),
                        str(valid_data['room_id']),
                        str(valid_data['room_mode']),
                    ])
                    if k not in temp_dict:
                        continue
                    enter_log = temp_dict.pop(k)
                    valid_data['start_time'] = enter_log['start_time']
                    valid_data['_file_enter'] = enter_log['_file_enter']
                    valid_data['_file_quit'] = os.path.basename(log_file)
                    new_row_data = "Mobile|" + "|".join([
                        str(valid_data['uin']),
                        str(valid_data['login_ip']),
                        str(valid_data['platform']),
                        str(valid_data['start_time']),
                        str(valid_data['stop_time']),
                        str(valid_data['room_id']),
                        str(valid_data['room_mode']),
                        str(valid_data['_file_enter']),
                        str(valid_data['_file_quit']),
                    ]) + "\n"
                    new_content.append(new_row_data)
        if len(temp_dict) > 0:
            temp_col.create_many(temp_dict.values())
        return new_content

    def parse_log_file(self, log_file):
        """
        Parse log data from `log_file`
        """
        log_data = self.get_log_data(log_file=log_file)
        if not log_data:
            return
        tmp_time = datetime.now()
        self.result = Result.objects.create_result_for_log(log_type=self.log_type, file_path=log_file,
                                                           total_rows=len(log_data))

        log_file = os.path.basename(log_file)
        self._info_log(log_file, 'START', '(', len(log_data), 'rows)')
        remove_tracking_file(log_file)
        self.load_documents_from_log_data(log_file=log_file, log_data=log_data)
        self._info_log(log_file, 'Skip', len(self.skip_rows), 'rows')
        self._info_log(log_file, 'Save', len(self.documents), 'documents')
        success_rows = self.save_documents_to_database(lod=len(log_data))
        remove_tracking_file(log_file)
        close_old_connections()
        tmp_time = datetime.now() - tmp_time

        messages = [log_file, 'FINISH', round(success_rows * 100.0 / len(log_data), 4),
                    'in', tmp_time, '(', success_rows, '/', len(log_data), 'rows)']

        self.result.finish(success_rows=success_rows, task_time=int(tmp_time.total_seconds()))
        self._info_log(*messages)

    def get_log_data(self, log_file):
        try:
            f = open(log_file, 'r')
            content = f.readlines()
            f.close()
            content = [r.strip() for r in content]
            if len(content) == 0:
                result = Result.objects.create_result_for_log_view(file_path=log_file, total_rows=0)
                result.set_system_log(log_file, 'is empty')
                result.finish()
                self._warning_log(log_file, 'is empty')
                return None
            return content
        except Exception, e:
            result = Result.objects.create_result_for_log_view(file_path=log_file, total_rows=0)
            result.set_system_log(log_file, type(e), e)
            result.finish()
            self._error_log(log_file, type(e), e)
            return None

    def load_documents_from_log_data(self, log_file, log_data):
        """
        Load documents from log data and save them to `self.documents`
        """
        self.documents = []
        self.skip_rows = []

        params = []
        for row, row_data in enumerate(log_data):
            params.append((log_file, row, row_data))
        result = utils.parallel_map(func=self.try_catch_parse_data, params=params)
        for data in result:
            if type(data) == int:
                self.skip_rows.append(data)
            elif type(data) == list:
                for i in data:
                    self.documents.append(i)
        self.documents = list(set(self.documents))
        self.skip_rows = list(set(self.skip_rows))

    def try_catch_parse_data(self, params, retry=False):
        """
        This function is used for try-catch exception and retry if 'MySQL server has gone away'
        """
        log_file, row, row_data = params
        try:
            data = self.parse_data(log_file, row, row_data)
            tracking_parse_log(log_file, row)
            return data
        except Exception as e:
            close_old_connections()
            if not retry:
                return self.try_catch_parse_data(params, True)
            else:
                ErrorDetail.objects.save_error_detail(self.result, row, log_file, type(e), e, traceback.format_exc())
                self._info_log(log_file, row, e)
                tracking_parse_log(log_file, row)
                return None

    def parse_data(self, log_file, row, row_data):
        """
        Parse data when parse multi rows in parallel (no cache)
        """
        data = row_data.strip().split('|')
        if self.mobile_proxy:
            data = dict(zip(LOG_MOBILE_KEYS, data))
        else:
            if len(data) == 14:
                data.append(0)  # NOTE: if room_mode not found, set room_mode = 0
            data = dict(zip(LOG_LEAVEROOM_KEYS, data))

        serializer = LeaveRoomLogSerializer(data=data)
        if serializer.is_valid():
            valid_data = serializer.validated_data
        else:
            self._warning_log(log_file, row, serializer.errors)
            return None

        if valid_data['view_length'] == 0 or valid_data['platform'] == 999:
            return row

        valid_data['_row'] = row
        valid_data['_log_file'] = os.path.basename(log_file)
        room_id = valid_data['room_id']
        self.documents = []

        # get channel_name
        room_extend = RoomExtend.objects.get_room(room_id=room_id)
        if room_extend:
            valid_data['channel_name'] = room_extend.channel_name
        else:
            valid_data['channel_name'] = str(room_id)

        tmp_start = valid_data['start_time'].replace(minute=0, second=0, microsecond=0)
        tmp_stop = valid_data['stop_time'].replace(minute=59, second=59, microsecond=999999)

        if valid_data['room_mode'] == 1:
            valid_data['category'] = Category.IDOL_ROOM
            room_info_list = StreamTimeNewDBTalkTV.objects.get_room_info(room_id, tmp_start, tmp_stop)
            if not room_info_list:
                RoomShow = get_RoomShow_model(room_id=room_id)
                room_info_list = RoomShow.objects.get_room_info(room_id, tmp_start, tmp_stop)
            self.parse_data_room_idol(valid_data=valid_data, room_info_list=room_info_list)
        else:
            if room_extend and valid_data['room_mode'] != 0:
                valid_data['streamer_uin'] = room_extend.streamer_uin
            else:
                valid_data['streamer_uin'] = 0

            room_info = StreamTimeNewDBTalkTV.objects.get_first_room_info(room_id, tmp_start, tmp_stop)
            if room_info:
                category_id = room_info.category_id
                valid_data['streamer_uin'] = room_info.streamer_uin
                valid_data['stream_id'] = room_info.id
                valid_data['stream_table'] = 'stream_time_new'
            else:
                category_id = None
            room_info = StreamTimeDBTalkTV.objects.get_first_room_info(valid_data['channel_name'], tmp_start, tmp_stop)
            if room_info:
                valid_data['stream_id'] = room_info.id
                valid_data['stream_table'] = 'stream_time'
            # Game
            if valid_data['is_game']:
                if room_extend:
                    valid_data['game_id'] = CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS.get(category_id, room_extend.game_id)
                else:
                    valid_data['game_id'] = CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS.get(category_id, PCGameID.OTHERS)
            # Other
            elif valid_data['room_mode'] == 104:
                valid_data['category'] = Category.LIVE_MOBILE
                valid_data['sub_category'] = CONVERT_LM_CATEGORY_IDS.get(category_id, SubCategory.LIVE_MOBILE_OTHERS)
            self.create_document(valid_data=valid_data)
        return self.documents

    def parse_data_room_idol(self, valid_data, room_info_list):
        """
        Support function for function `parse_data_per_file`
        """
        used_time_list = []
        for room_info in room_info_list:
            if room_info.stream_length < MAX_STREAM_LENGTH:
                temp_data = valid_data.copy()
                temp_data['start_time'] = max(room_info.start_time, temp_data['start_time'])
                temp_data['stop_time'] = min(room_info.stop_time, temp_data['stop_time'])
                if temp_data['stop_time'] <= temp_data['start_time']:
                    continue
                temp_data['stream_id'] = room_info.id
                temp_data['stream_table'] = 'stream_time_new'
                temp_data['streamer_uin'] = room_info.streamer_uin
                temp_data['view_length'] = int((temp_data['stop_time'] - temp_data['start_time']).total_seconds())

                if isinstance(room_info, StreamTimeNewDBTalkTV) and valid_data['room_id'] not in DANCE_ROOM_IDS:
                    temp_data['sub_category'] = CONVERT_IDOL_CATEGORY_IDS.get(room_info.category_id,
                                                                              SubCategory.IDOL_OTHERS)

                self.create_document(valid_data=temp_data)
                used_time_list.append((temp_data['start_time'], temp_data['stop_time']))

        used_time_list = list(set(used_time_list))
        left_time_list = [(valid_data['start_time'], valid_data['stop_time'])]

        for used_time in used_time_list:
            tmp_list = []
            while len(left_time_list) > 0:
                left_time = left_time_list.pop()
                if used_time[1] > left_time[0] and used_time[0] < left_time[1]:
                    if left_time[0] < used_time[0]:
                        tmp_list.append((left_time[0], used_time[0]))
                    if left_time[1] > used_time[1]:
                        tmp_list.append((used_time[1], left_time[1]))
            left_time_list = tmp_list

        for left_time in left_time_list:
            temp_data = valid_data.copy()
            temp_data['start_time'] = left_time[0]
            temp_data['stop_time'] = left_time[1]
            if temp_data['stop_time'] <= temp_data['start_time']:
                continue
            temp_data['streamer_uin'] = 0
            self.create_document(valid_data=temp_data)

    def create_document(self, valid_data):
        document = LeaveRoomLogSerializer.create_document(valid_data)
        data = json.dumps(document, default=json_util.default)
        self.documents.append(data)

    def save_documents_to_database(self, lod):
        self.documents = [json.loads(i, object_hook=json_util.object_hook) for i in self.documents]
        if self.mobile_proxy:
            collection = MobileLogCollection()
        else:
            collection = ViewLogCollection()
        inserted_ids = collection.create_many(documents=self.documents)
        success_rows = collection.find({"_id": {"$in": inserted_ids}}).distinct("_row")
        failed_rows = []
        for i in xrange(lod):
            if i not in success_rows and i not in self.skip_rows:
                failed_rows.append(i)
        if len(failed_rows) > 0:
            self.result.set_system_log(failed_rows)
        return len(success_rows) + len(self.skip_rows)
