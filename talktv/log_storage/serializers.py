from datetime import timedelta

from dateutil import parser
from rest_framework import serializers

from talktv.constants import Category
from talktv.constants import DANCE_ROOM_IDS
from talktv.constants import GamePlatform
from talktv.constants import MOBILE_GAME_IDS
from talktv.constants import MobileGameID
from talktv.constants import PCGameID
from talktv.constants import SubCategory

ROOM_MODE_GAME = (101, 4,)


def create_document(validated_data):
    valid_data = validated_data.copy()
    # add game_platform
    if valid_data['is_game']:
        if valid_data['game_id'] in MOBILE_GAME_IDS:
            valid_data['game_platform'] = GamePlatform.Mobile
        else:
            valid_data['game_platform'] = GamePlatform.PC
    else:
        valid_data['game_platform'] = None
        valid_data['game_id'] = None

    if not valid_data.get('category', None):
        # add category property
        if valid_data['room_mode'] == 0:  # Normal room
            category = Category.NORMAL_ROOM
        elif valid_data['room_mode'] == 1:  # Idol room
            category = Category.IDOL_ROOM
        elif valid_data['game_platform'] == GamePlatform.PC:  # PC game
            category = Category.PC_GAME
        elif valid_data['game_platform'] == GamePlatform.Mobile:  # Mobile Game
            category = Category.MOBILE_GAME
        else:  # Others
            category = Category.OTHERS
        valid_data['category'] = category

    if not valid_data.get('sub_category', None):
        # add sub_category property
        if valid_data['category'] == Category.IDOL_ROOM:
            if valid_data['room_id'] in DANCE_ROOM_IDS:
                sub_category = SubCategory.IDOL_DANCE
            else:
                sub_category = SubCategory.IDOL_SING
        elif valid_data['category'] == Category.PC_GAME:
            if valid_data['game_id'] == PCGameID.LOL:
                sub_category = SubCategory.PC_LOL
            elif valid_data['game_id'] == PCGameID.FPS:
                sub_category = SubCategory.PC_FPS
            elif valid_data['game_id'] == PCGameID.AOE:
                sub_category = SubCategory.PC_AOE
            else:
                sub_category = SubCategory.PC_OTHERS
        elif valid_data['category'] == Category.MOBILE_GAME:
            if valid_data['game_id'] == MobileGameID.LIEN_QUAN:
                sub_category = SubCategory.MOBILE_LQ
            elif valid_data['game_id'] == MobileGameID.CROSSFIRE_LEGENDS:
                sub_category = SubCategory.MOBILE_CF
            else:
                sub_category = SubCategory.MOBILE_OTHERS
        else:
            sub_category = SubCategory.OTHERS
        valid_data['sub_category'] = sub_category

    return valid_data


class LeaveRoomLogSerializer(serializers.Serializer):
    uin = serializers.IntegerField()
    login_ip = serializers.IPAddressField()
    platform = serializers.IntegerField()
    start_time = serializers.DateTimeField(required=False, default=None)
    stop_time = serializers.DateTimeField()
    view_length = serializers.IntegerField(required=False, default=0)
    room_id = serializers.IntegerField()
    room_mode = serializers.IntegerField()

    def validate(self, data):
        if data['start_time'] is None:
            data['start_time'] = data['stop_time'] - timedelta(seconds=data['view_length'])
        else:
            data['view_length'] = int((data['stop_time'] - data['start_time']).total_seconds())

        data['is_game'] = True if data['room_mode'] in ROOM_MODE_GAME else False
        return data

    @staticmethod
    def create_document(validated_data):
        return create_document(validated_data)


class ChatLogSerializer(serializers.Serializer):
    uin = serializers.IntegerField()
    user_type = serializers.IntegerField()
    login_ip = serializers.IPAddressField()
    room_id = serializers.IntegerField()
    event_time = serializers.CharField()

    def validate_event_time(self, data):
        data = parser.parse(data)
        return data


class StreamTimeNewLogSerializer(serializers.Serializer):
    streamer_uin = serializers.IntegerField()
    room_id = serializers.IntegerField()
    stream_length = serializers.IntegerField()
    start_time = serializers.DateTimeField()
    stop_time = serializers.DateTimeField()


class StreamTimeLogSerializer(serializers.Serializer):
    channel_name = serializers.CharField(source='channel_name.channel_name')
    room_id = serializers.IntegerField(source='channel_name.room_id')
    stream_length = serializers.IntegerField()
    start_time = serializers.DateTimeField()
    stop_time = serializers.DateTimeField()


class MobileEnterLogSerializer(serializers.Serializer):
    uin = serializers.IntegerField()
    login_ip = serializers.IPAddressField()
    platform = serializers.IntegerField()
    start_time = serializers.DateTimeField()
    room_id = serializers.IntegerField()
    room_mode = serializers.IntegerField()


class MobileQuitLogSerializer(serializers.Serializer):
    uin = serializers.IntegerField()
    login_ip = serializers.IPAddressField()
    platform = serializers.IntegerField()
    stop_time = serializers.DateTimeField()
    room_id = serializers.IntegerField()
    room_mode = serializers.IntegerField()

class BillingLogSerializer(serializers.Serializer):
    commit_time = serializers.DateTimeField()
    bill_id = serializers.IntegerField()
    room_id = serializers.IntegerField()
    room_mode = serializers.IntegerField()
    g_user_id = serializers.IntegerField()
    r_user_id = serializers.IntegerField()
    amount = serializers.IntegerField()
    g_txu = serializers.IntegerField()
    r_kkx = serializers.IntegerField()
    status = serializers.IntegerField()
