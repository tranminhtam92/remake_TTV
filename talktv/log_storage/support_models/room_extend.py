from django.core.cache import cache
from django.db import models


class RoomExtendManager(models.Manager):
    def get_room(self, room_id):
        query_set = self.raw(
            "select roomId, iUin, roomMode, streamGameId, max(roomAlias) as roomAlias from room_extend where roomId = %s;",
            (room_id,))

        query = query_set.query
        cached_data = cache.get(query)
        if cached_data:
            return cached_data

        for row in query_set:
            if row.room_id:
                cache.set(query, row)
                return row
        cache.set(query, None)
        return None

    def get_room_via_channel_name(self, channel_name):
        query_set = self.raw(
            "select roomId, iUin, roomMode, streamGameId, roomAlias from room_extend where roomAlias = %s;",
            (channel_name,))

        query = query_set.query
        cached_data = cache.get(query)
        if cached_data:
            return cached_data

        for row in query_set:
            if row.room_id:
                cache.set(query, row)
                return row
        cache.set(query, None)
        return None


class RoomExtend(models.Model):
    class Meta:
        db_table = 'room_extend'

    room_id = models.IntegerField(db_column='roomId')
    channel_name = models.CharField(max_length=255, db_column='roomAlias', primary_key=True)
    streamer_uin = models.IntegerField(db_column='iUin')
    game_id = models.IntegerField(db_column='streamGameId')
    room_mode = models.IntegerField(db_column='roomMode')
    objects = RoomExtendManager()
