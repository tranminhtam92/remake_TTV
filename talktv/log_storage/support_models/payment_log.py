import time
from datetime import datetime

from django.core.cache import cache
from django.db import models

from django_unixdatetimefield import UnixDateTimeField

class ZTalkBillingLogDBManager(models.Manager):

    def get_billing_log(self, dt_from, dt_to):
        query_set = self.filter(commit_time__range=(dt_from, dt_to))

        if len(query_set) > 0:
            query_set = list(query_set)
            return query_set
        return None

class ZTalkBillingLogDB(models.Model):
    class Meta:
        db_table = 'ztalk_billing_log'

    bill_id = models.IntegerField(db_column='bill_id', primary_key=True)
    room_id = models.IntegerField(db_column='room_id')
    commit_time = models.DateTimeField(db_column='commit_time')
    room_mode = models.IntegerField()
    g_user_id = models.IntegerField()
    r_user_id = models.IntegerField()
    amount = models.IntegerField()
    g_txu = models.IntegerField()
    r_kkx = models.IntegerField()
    status = models.IntegerField()
    objects = ZTalkBillingLogDBManager()



class ZTalkTransactionLogDBManager(models.Manager):

    def get_client_type(self, bill_id):

        query_set = self.filter(bill_id=bill_id)
        print 'get_client_type', query_set.query
        if len(query_set) > 0:
            query_set = list(query_set)
            return query_set[0]
        return None

class ZTalkTransactionLogDB(models.Model):
    class Meta:
        db_table = 'ztalk_trans_log'

    bill_id = models.IntegerField(db_column='bill_id', primary_key=True)
    client_type = models.IntegerField(db_column='client_type')
    commit_time = models.DateTimeField(db_column='commit_time')
    

    objects = ZTalkTransactionLogDBManager()


