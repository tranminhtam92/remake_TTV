from datetime import datetime

from rest_framework import serializers


class RequestSerializer(serializers.Serializer):
    dt_from = serializers.DateTimeField(required=False)
    dt_to = serializers.DateTimeField(required=False)
    room_id = serializers.IntegerField(required=False)
    anchor_uin = serializers.IntegerField()

    def validate(self, attrs):
        attrs = super(RequestSerializer, self).validate(attrs)
        if 'dt_from' not in attrs:
            attrs['dt_from'] = datetime.now().replace(minute=0, second=0, microsecond=0)
        if 'dt_to' not in attrs:
            attrs['dt_to'] = datetime.now()
        return attrs


class AnchorGiftLogSerializer(serializers.Serializer):
    user_uin = serializers.IntegerField()
    anchor_uin = serializers.IntegerField()
    room_id = serializers.IntegerField()
    gift_id = serializers.IntegerField()
    gift_num = serializers.IntegerField()
    give_time = serializers.DateTimeField()
    gift_name = serializers.CharField()


class LoginRequestSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField(style={'input_type': 'password'})
