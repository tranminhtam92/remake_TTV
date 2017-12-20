from django.db import models


class StreamerInfoManager(models.Manager):
    def get_streamer_uins_in_class(self, streamer_class):
        streamers = self.all().filter(rank=streamer_class)
        streamers = list(streamers)
        ids = [streamer.uin for streamer in streamers]
        return ids


class StreamerInfo(models.Model):
    class Meta:
        db_table = 'tbl_vcaster_info'

    uin = models.PositiveIntegerField(unique=True)
    name = models.CharField(max_length=255)
    rank = models.PositiveSmallIntegerField()
    disable = models.BooleanField()

    objects = StreamerInfoManager()
