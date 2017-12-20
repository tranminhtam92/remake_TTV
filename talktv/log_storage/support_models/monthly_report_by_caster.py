import time

from django.db import models


class MonthlyReportByCasterManager(models.Manager):
    def save_report(self, report_time, streamer_uin, a30, a30_pc, a30_web, a30_mobile, v30_pc, v30_web, v30_mobile):
        unix_time = int(time.mktime(report_time.timetuple()))
        obj = self.create(report_time=unix_time,
                          streamer_uin=streamer_uin,
                          active_users=a30,
                          active_users_pc=a30_pc,
                          active_users_web=a30_web,
                          active_users_mobile=a30_mobile,
                          total_view_length_pc=v30_pc,
                          total_view_length_web=v30_web,
                          total_view_length_mobile=v30_mobile)
        obj.save()
        return obj


class MonthlyReportByCaster(models.Model):
    class Meta:
        db_table = 'MonthlyReportByCaster'

    report_time = models.IntegerField(db_column='iMonth')
    streamer_uin = models.IntegerField(db_column='iIdolUin')
    active_users = models.IntegerField(null=True, db_column='iUser_All')
    active_users_pc = models.IntegerField(null=True, db_column='iUser_PC')
    active_users_web = models.IntegerField(null=True, db_column='iUser_Web')
    active_users_mobile = models.IntegerField(null=True, db_column='iUser_Mobile')
    total_view_length_pc = models.IntegerField(null=True, db_column='iViewtime_PC')
    total_view_length_web = models.IntegerField(null=True, db_column='iViewtime_Web')
    total_view_length_mobile = models.IntegerField(null=True, db_column='iViewtime_Mobile')

    objects = MonthlyReportByCasterManager()
