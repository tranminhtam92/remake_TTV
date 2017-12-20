from django.core.cache import cache
from django.db import models

from .room_extend import RoomExtend


class StreamTimeDBTalkTVManager(models.Manager):
    def get_stream_info(self, dt_from, dt_to):
        query_set = self.filter(stop_time__range=(dt_from, dt_to))
        return list(query_set)

    def get_first_room_info(self, channel_name, dt_from, dt_to):
        query_set = self.filter(channel_name=channel_name)
        query_set = query_set.filter(stream_length__gt=0)
        query_set = query_set.filter(start_time__lte=dt_to)
        query_set = query_set.filter(stop_time__gte=dt_from)
        query_set = query_set.order_by('-stream_length')
        query_set = query_set[:1]

        cached_query = 'get_first_room_info' + str(query_set.query)
        cached_data = cache.get(cached_query)
        if cached_data:
            return cached_data

        if len(query_set) > 0:
            cache.set(cached_query, query_set[0])
            return query_set[0]
        return None

    def get_ids(self, dt_from, dt_to):
        query = self.filter(stop_time__range=(dt_from, dt_to))
        return [i.id for i in query]


class StreamTimeDBTalkTV(models.Model):
    class Meta:
        db_table = 'streamtime'

    channel_name = models.ForeignKey(RoomExtend, db_column='channel')
    start_time = models.DateTimeField(db_column='timeOn')
    stop_time = models.DateTimeField(db_column='timeOff')
    stream_length = models.IntegerField(db_column='time')
    list_bitrate = models.CharField(max_length=200, db_column='listBitrate')

    objects = StreamTimeDBTalkTVManager()
