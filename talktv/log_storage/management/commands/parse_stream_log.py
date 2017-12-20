from datetime import datetime, timedelta

from dateutil import parser

from log_storage.collections import StreamLogCollection
from log_storage.serializers import StreamTimeNewLogSerializer, StreamTimeLogSerializer
from log_storage.support_models import StreamTimeNewDBTalkTV, RoomExtend, get_RoomShow_model, StreamTimeDBTalkTV
from talktv.constants import CONVERT_CATEGORY_IDS
from talktv.constants import CONVERT_GAME_CATEGORY_IDS
from talktv.constants import CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS
from talktv.constants import Category
from talktv.constants import MOBILE_GAME_IDS
from talktv.constants import SubCategory
from talktv.custom import CustomBaseCommand


class Command(CustomBaseCommand):
    help = 'Get stream log from MySQL dbs and store data to Mongo database'

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
        col = StreamLogCollection()

        # StreamTime
        stream_info_list = StreamTimeDBTalkTV.objects.get_stream_info(dt_from, dt_to)
        for stream_info in stream_info_list:
            document = StreamTimeLogSerializer(stream_info).data
            document['_table'] = 'stream_time'

            room_extend = RoomExtend.objects.get_room(document['room_id'])
            if room_extend:
                document['streamer_uin'] = room_extend.streamer_uin
                if room_extend.room_mode == 1:
                    document['category'] = Category.IDOL_ROOM
                    if document['room_id'] in (69, 96):
                        document['sub_category'] = SubCategory.IDOL_DANCE
                    else:
                        document['sub_category'] = SubCategory.IDOL_SING
                elif room_extend.room_mode == 101 or room_extend.room_mode == 4:
                    if room_extend.game_id in MOBILE_GAME_IDS:
                        document['category'] = Category.MOBILE_GAME
                    else:
                        document['category'] = Category.PC_GAME
                    document['sub_category'] = CONVERT_GAME_CATEGORY_IDS.get(room_extend.game_id)
                elif room_extend.room_mode == 104:
                    document['category'] = Category.LIVE_MOBILE
                    document['sub_category'] = SubCategory.LIVE_MOBILE_OTHERS
            else:
                document['streamer_uin'] = None
                document['category'] = None
                document['sub_category'] = None

            col.create(document)

        # StreamTimeNew
        stream_info_list = StreamTimeNewDBTalkTV.objects.get_stream_info(dt_from, dt_to)
        for stream_info in stream_info_list:
            document = StreamTimeNewLogSerializer(stream_info).data
            document['_table'] = 'stream_time_new'

            room_extend = RoomExtend.objects.get_room(document['room_id'])
            if room_extend:
                document['channel_name'] = room_extend.channel_name
                if room_extend.room_mode == 1:
                    document['category'] = Category.IDOL_ROOM
                elif room_extend.room_mode == 101 or room_extend.room_mode == 4:
                    game_id = CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS.get(stream_info.category_id)
                    if game_id is None:
                        game_id = room_extend.game_id
                    if game_id in MOBILE_GAME_IDS:
                        document['category'] = Category.MOBILE_GAME
                    else:
                        document['category'] = Category.PC_GAME
                elif room_extend.room_mode == 104:
                    document['category'] = Category.LIVE_MOBILE
            else:
                document['channel_name'] = document['room_id']
                document['category'] = None

            document['sub_category'] = CONVERT_CATEGORY_IDS.get(stream_info.category_id)
            if document['sub_category'] is None and document['category'] is not None:
                document['sub_category'] = document['category'] * 100 + 99

            col.create(document)

        # recheck RoomShow for StreamTimeNewDBTalkTV
        for i in range(100):
            RoomShow = get_RoomShow_model(i)
            room_show_list = RoomShow.objects.get_stream_info(dt_from, dt_to)
            for room_show in room_show_list:
                document = StreamTimeNewLogSerializer(room_show).data
                if col.is_existed(room_id=document['room_id'],
                                  streamer_uin=document['streamer_uin'],
                                  start_time=document['start_time'],
                                  stop_time=document['stop_time']):
                    continue
                document['_table'] = 'room_show_trace'
                room_extend = RoomExtend.objects.get_room(document['room_id'])
                if room_extend:
                    if room_extend.room_mode == 1:
                        document['category'] = Category.IDOL_ROOM
                        if document['room_id'] in (69, 96):
                            document['sub_category'] = SubCategory.IDOL_DANCE
                        else:
                            document['sub_category'] = SubCategory.IDOL_SING
                    elif room_extend.room_mode == 101 or room_extend.room_mode == 4:
                        if room_extend.game_id in MOBILE_GAME_IDS:
                            document['category'] = Category.MOBILE_GAME
                        else:
                            document['category'] = Category.PC_GAME
                        document['sub_category'] = CONVERT_GAME_CATEGORY_IDS.get(room_extend.game_id)
                    elif room_extend.room_mode == 104:
                        document['category'] = Category.LIVE_MOBILE
                        document['sub_category'] = SubCategory.LIVE_MOBILE_OTHERS
                else:
                    document['category'] = None
                    document['sub_category'] = None
                col.create(document)

        self._info_log('Parse stream log', dt_from, dt_to)
