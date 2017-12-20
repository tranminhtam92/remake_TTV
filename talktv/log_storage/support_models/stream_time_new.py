import time
from datetime import datetime

from django.core.cache import cache
from django.db import models


class StreamTimeNewDBTalkTVManager(models.Manager):
    def get_stream_info(self, dt_from, dt_to):
        unix_from = int(time.mktime(dt_from.timetuple()))
        unix_to = int(time.mktime(dt_to.timetuple()))
        query_set = self.filter(unix_stop_time__range=(unix_from, unix_to))
        query_set = query_set.filter(is_finished=True)
        return list(query_set)

    def get_room_info(self, room_id, dt_from, dt_to):
        unix_from = int(time.mktime(dt_from.timetuple()))
        unix_to = int(time.mktime(dt_to.timetuple()))

        query_set = self.filter(room_id=room_id)
        query_set = query_set.filter(stream_length__gt=0)
        query_set = query_set.filter(unix_start_time__lte=unix_to)
        query_set = query_set.filter(unix_stop_time__gte=unix_from)
        query_set = query_set.order_by('-stream_length')

        cached_query = 'get_room_info' + str(query_set.query)
        cached_data = cache.get(cached_query)
        if cached_data:
            return cached_data

        if len(query_set) > 0:
            query_set = list(query_set)
            cache.set(cached_query, query_set)
            return query_set
        cache.set(cached_query, None)
        return None

    def get_category_info(self, room_id, dt_from, dt_to):
        unix_from = int(time.mktime(dt_from.timetuple()))
        unix_to = int(time.mktime(dt_to.timetuple()))

        query_set = self.filter(room_id=room_id)
        query_set = query_set.filter(stream_length__gt=0)
        query_set = query_set.filter(unix_start_time__lte=unix_to)
        query_set = query_set.filter(unix_stop_time__gte=unix_from)
        query_set = query_set.order_by('-stream_length')
        query_set = query_set[:1]

        cached_query = 'get_category_info' + str(query_set.query)
        cached_data = cache.get(cached_query)
        if cached_data:
            return cached_data

        if len(query_set) > 0:
            cache.set(cached_query, query_set[0].category_id)
            return query_set[0].category_id
        return None

    def get_first_room_info(self, room_id, dt_from, dt_to):
        unix_from = int(time.mktime(dt_from.timetuple()))
        unix_to = int(time.mktime(dt_to.timetuple()))

        query_set = self.filter(room_id=room_id)
        query_set = query_set.filter(stream_length__gt=0)
        query_set = query_set.filter(unix_start_time__lte=unix_to)
        query_set = query_set.filter(unix_stop_time__gte=unix_from)
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
        unix_from = int(time.mktime(dt_from.timetuple()))
        unix_to = int(time.mktime(dt_to.timetuple()))
        query = self.filter(is_finished=True)
        query = query.filter(unix_stop_time__range=(unix_from, unix_to))
        return [i.id for i in query]


class StreamTimeNewDBTalkTV(models.Model):
    class Meta:
        db_table = 'streamtime_new'

    streamer_uin = models.IntegerField(db_column='uin')
    room_id = models.IntegerField(db_column='roomId')
    unix_start_time = models.IntegerField(db_column='timeOn')
    unix_stop_time = models.IntegerField(db_column='timeOff')
    stream_length = models.IntegerField(db_column='timeLength')
    category_id = models.IntegerField(db_column='catId')
    platform = models.IntegerField(db_column='fromSource')
    is_finished = models.BooleanField(db_column='finished')

    objects = StreamTimeNewDBTalkTVManager()

    @property
    def start_time(self):
        return datetime.fromtimestamp(self.unix_start_time)

    @property
    def stop_time(self):
        return datetime.fromtimestamp(self.unix_stop_time)
