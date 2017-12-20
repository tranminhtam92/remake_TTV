from datetime import datetime

import ldap
from decouple import config
from django.conf import settings
from django.contrib.auth import login, logout
from django.contrib.auth.models import User
from rest_framework import status
from rest_framework.generics import GenericAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from log_event.models import get_AnnchorGiftLog_model
from log_event.serializers import RequestSerializer, AnchorGiftLogSerializer, LoginRequestSerializer

LDAP_URL = config('LDAP_URL')

ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, False)
ldap.set_option(ldap.OPT_REFERRALS, False)
ldap.set_option(ldap.OPT_PROTOCOL_VERSION, 3)


class GiftDataView(GenericAPIView):
    # permission_classes = (IsAuthenticated,)

    def get(self, request):
        request_serializer = RequestSerializer(data=self.request.query_params)
        if not request_serializer.is_valid():
            return Response(status=status.HTTP_400_BAD_REQUEST, data=request_serializer.errors)
        query_params = request_serializer.validated_data

        objects = get_AnnchorGiftLog_model(query_params['anchor_uin']).objects
        objects = objects.filter(give_time__gte=query_params['dt_from'], give_time__lte=query_params['dt_to'])
        objects = objects.filter(anchor_uin=query_params['anchor_uin'])
        if 'room_id' in query_params:
            objects = objects.filter(room_id=query_params['room_id'])
        objects = objects.extra(select={
            'gift_name': 'SELECT `name` FROM giftinfo WHERE giftinfo.`gift_id` = {}.`gift_id`'.format(
                objects.model._meta.db_table)
        })
        serializer = AnchorGiftLogSerializer(objects, many=True)
        data = serializer.data
        summary = {}
        for row in data:
            if row['gift_id'] in summary:
                summary[row['gift_id']]['gift_num'] = summary[row['gift_id']]['gift_num'] + row['gift_num']
            else:
                summary[row['gift_id']] = {
                    'gift_id': row['gift_id'],
                    'gift_name': row['gift_name'],
                    'gift_num': row['gift_num'],
                }

        return Response(data={
            'query_params': query_params,
            'data': data,
            'summary': sorted(summary.values(), key=lambda x: x['gift_id']),
        })


class LoginView(GenericAPIView):
    authentication_classes = []
    serializer_class = LoginRequestSerializer

    def post(self, request):
        serializer = self.get_serializer(data=self.request.data)
        if not serializer.is_valid():
            return Response(status=status.HTTP_400_BAD_REQUEST, data=serializer.errors)

        try:
            l = ldap.initialize(LDAP_URL)
            email = serializer.validated_data['email']
            password = serializer.validated_data['password']
        except:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE, data='Cannot connect to VNG Auth server')

        try:
            l.simple_bind_s(email, password)
        except:
            return Response(status=status.HTTP_401_UNAUTHORIZED, data='Wrong email or wrong password')

        try:
            user = User.objects.get(email=email)
            response = Response(data={
                'email': user.email,
            })
            response.set_cookie('email', user.email, expires=request.session.get_expiry_age())
            logout(request)
            login(request, user)
        except:
            return Response(status=status.HTTP_403_FORBIDDEN, data='User does not have permission to access')

        return response


class LogoutView(APIView):
    authentication_classes = []

    def post(self, request):
        response = Response(data={"detail": "Successfully logged out."})
        response.delete_cookie('email')
        logout(request)
        return response


class DateTimeView(APIView):
    def get(self, request):
        return Response(data={
            'time_zone': settings.TIME_ZONE,
            'now': str(datetime.now())
        })
