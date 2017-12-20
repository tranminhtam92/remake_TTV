import json
import traceback

from bson import json_util
from django.db.models import signals

from log_storage.collections import ViewLogCollection
from log_storage.models import ErrorDetail
from log_storage.serializers import LeaveRoomLogSerializer
from log_storage.support_models import get_RoomShow_model, RoomExtend, StreamTimeNewDBTalkTV
from talktv.constants import CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS
from talktv.constants import CONVERT_IDOL_CATEGORY_IDS
from talktv.constants import CONVERT_LM_CATEGORY_IDS
from talktv.constants import DANCE_ROOM_IDS
from talktv.constants import LOG_LEAVEROOM_KEYS, Category
from talktv.constants import LOG_VIEW
from talktv.constants import MAX_STREAM_LENGTH
from talktv.constants import PCGameID
from talktv.constants import SubCategory


def save_documents_to_database(documents):
    documents = list(set(documents))
    documents = [json.loads(i, object_hook=json_util.object_hook) for i in documents]
    collection = ViewLogCollection()
    collection.create_many(documents=documents)


def create_document(valid_data):
    document = LeaveRoomLogSerializer.create_document(valid_data)
    data = json.dumps(document, default=json_util.default)
    return data


def parse_data_room_idol(valid_data, room_info_list):
    """
    Support function for function `parse_data_per_file`
    """
    documents = []
    used_time_list = []
    for room_info in room_info_list:
        if room_info.stream_length < MAX_STREAM_LENGTH:
            temp_data = valid_data.copy()
            temp_data['start_time'] = max(room_info.start_time, temp_data['start_time'])
            temp_data['stop_time'] = min(room_info.stop_time, temp_data['stop_time'])
            if temp_data['stop_time'] <= temp_data['start_time']:
                continue
            temp_data['streamer_uin'] = room_info.streamer_uin
            temp_data['view_length'] = int((temp_data['stop_time'] - temp_data['start_time']).total_seconds())

            if isinstance(room_info, StreamTimeNewDBTalkTV) and valid_data['room_id'] not in DANCE_ROOM_IDS:
                temp_data['sub_category'] = CONVERT_IDOL_CATEGORY_IDS.get(room_info.category_id, SubCategory.IDOL_OTHERS)

            documents.append(create_document(valid_data=temp_data))
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
        documents.append(create_document(valid_data=temp_data))

    return documents


def retry_parse_row_data(row, file_data, result):
    try:
        row_data = file_data[row]
        data = row_data.strip().split('|')
        if len(data) == 14:
            data.append(0)  # NOTE: if room_mode not found, set room_mode = 0
        data = dict(zip(LOG_LEAVEROOM_KEYS, data))

        serializer = LeaveRoomLogSerializer(data=data)
        if serializer.is_valid():
            valid_data = serializer.validated_data
        else:
            ErrorDetail.objects.save_error_detail(result, row, result.file_name, serializer.errors)
            return False

        if valid_data['view_length'] == 0 or valid_data['platform'] == 999:
            return True

        valid_data['_row'] = row
        valid_data['_log_file'] = result.file_name
        room_id = valid_data['room_id']

        # get channel_name
        room_extend = RoomExtend.objects.get_room(room_id=room_id)
        if room_extend:
            valid_data['channel_name'] = room_extend.channel_name
        else:
            valid_data['channel_name'] = str(room_id)

        tmp_start = valid_data['start_time']
        tmp_stop = valid_data['stop_time'].replace(minute=59, second=59, microsecond=999999)

        if valid_data['room_mode'] == 1:
            valid_data['category'] = Category.IDOL_ROOM
            room_info_list = StreamTimeNewDBTalkTV.objects.get_room_info(room_id, tmp_start, tmp_stop)
            if not room_info_list:
                RoomShow = get_RoomShow_model(room_id=room_id)
                room_info_list = RoomShow.objects.get_room_info(room_id, tmp_start, tmp_stop)
            documents = parse_data_room_idol(valid_data=valid_data, room_info_list=room_info_list)
        else:
            if room_extend and valid_data['room_mode'] != 0:
                valid_data['streamer_uin'] = room_extend.streamer_uin
            else:
                valid_data['streamer_uin'] = 0

            # Game
            if valid_data['is_game']:
                category_id = StreamTimeNewDBTalkTV.objects.get_category_info(room_id, tmp_start, tmp_stop)
                if room_extend:
                    valid_data['game_id'] = CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS.get(category_id, room_extend.game_id)
                else:
                    valid_data['game_id'] = CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS.get(category_id, PCGameID.OTHERS)
            # Other
            elif valid_data['room_mode'] == 104:
                valid_data['category'] = Category.LIVE_MOBILE
                category_id = StreamTimeNewDBTalkTV.objects.get_category_info(room_id, tmp_start, tmp_stop)
                valid_data['sub_category'] = CONVERT_LM_CATEGORY_IDS.get(category_id, SubCategory.LIVE_MOBILE_OTHERS)
            documents = [create_document(valid_data=valid_data), ]
        save_documents_to_database(documents)
        return True
    except Exception as e:
        ErrorDetail.objects.save_error_detail(result, row, result.file_name, e, traceback.format_exc())
        return False


def read_file_data(result):
    rf = open(result.file_path, 'r')
    content = rf.readlines()
    rf.close()
    data = [r.strip() for r in content]
    return data


def retry_parse_log(sender, instance, created, **kwargs):
    signals.post_save.disconnect(retry_parse_log, sender=sender)

    if created:
        return
    if instance.log_type != LOG_VIEW:
        return
    if not instance.is_finished:
        return
    if not instance.retry:
        return

    errors = ErrorDetail.objects.get_latest_error_details(result=instance)
    if errors is None:
        instance.retry = False
        instance.save()

    instance.start_retrying()
    file_data = read_file_data(result=instance)
    for error in errors:
        is_success = retry_parse_row_data(error.row, file_data, instance)
        if is_success:
            instance.success_rows += 1
    new_errors = ErrorDetail.objects.get_latest_error_details(result=instance)
    if new_errors is None:
        instance.set_system_log()
    else:
        instance.set_system_log('Error rows:', *[e.row for e in new_errors])
    instance.finish_retrying()

    signals.post_save.connect(retry_parse_log, sender=sender)
