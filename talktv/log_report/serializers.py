from rest_framework import serializers

from talktv.constants import CATEGORY_IDS
from talktv.constants import PLATFORM_IDS
from talktv.constants import REPORT_TYPE_CHOICES
from talktv.constants import SUB_CATEGORY_IDS


class RequestSerializer(serializers.Serializer):
    dt_from = serializers.DateTimeField()
    dt_to = serializers.DateTimeField()
    report_type = serializers.ChoiceField(choices=REPORT_TYPE_CHOICES)
    platform = serializers.ChoiceField(required=False, choices=PLATFORM_IDS, default=None)
    category = serializers.ChoiceField(required=False, choices=CATEGORY_IDS, default=None)
    sub_category = serializers.ChoiceField(required=False, choices=SUB_CATEGORY_IDS, default=None)
    room_id = serializers.IntegerField(required=False, default=None)
    streamer_uin = serializers.IntegerField(required=False, default=None)
    is_game = serializers.BooleanField(required=False, default=False)
