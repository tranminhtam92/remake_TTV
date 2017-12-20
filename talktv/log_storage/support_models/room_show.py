import time

from django.core.cache import cache
from django.db import models
from django_unixdatetimefield import UnixDateTimeField


class RoomShowManager(models.Manager):
    def get_stream_info(self, start_time, stop_time):
        unix_start = time.mktime(start_time.timetuple())
        unix_stop = time.mktime(stop_time.timetuple())
        raw_query = "select id, uin, StartTime, min(StopTime) as StopTime, min(TimeLength) as TimeLength from {} where StopTime between %s and %s group by uin, StartTime;".format(
            self.model._meta.db_table)
        data = []
        queryset = self.raw(raw_query, (unix_stop, unix_start))
        cached_data = cache.get(queryset.query)
        if cached_data:
            return cached_data
        for info in queryset:
            data.append(info)
        cache.set(queryset.query, data)
        return data

    def get_room_info(self, room_id, start_time, stop_time):
        unix_start = time.mktime(start_time.timetuple())
        unix_stop = time.mktime(stop_time.timetuple())
        raw_query = "select id, uin, StartTime, min(StopTime) as StopTime, min(TimeLength) as TimeLength from roomshowtrace%s where RoomId1=%s and StartTime<=%s and StopTime>=%s group by uin, StartTime;"
        data = []
        queryset = self.raw(raw_query, (room_id % 100, room_id, unix_stop, unix_start))
        cached_data = cache.get(queryset.query)
        if cached_data:
            return cached_data
        for info in queryset:
            data.append(info)
        cache.set(queryset.query, data)
        return data


dict_RoomShow_model = {}


def get_RoomShow_model(room_id):
    k = str(room_id % 100)
    if k not in dict_RoomShow_model:
        class Meta:
            db_table = 'roomshowtrace{}'.format(k)

        attrs = {
            'Meta': Meta,
            'room_id': models.PositiveIntegerField(db_column='RoomId1'),
            'streamer_uin': models.PositiveIntegerField(db_column='uin'),
            'stream_length': models.PositiveIntegerField(db_column='TimeLength'),
            'start_time': UnixDateTimeField(db_column='StartTime'),
            'stop_time': UnixDateTimeField(db_column='StopTime'),
            'objects': RoomShowManager(),
            '__module__': 'log_storage.models',
        }
        dict_RoomShow_model[k] = type("RoomShow{}".format(k), (models.Model,), attrs)
    return dict_RoomShow_model[k]
