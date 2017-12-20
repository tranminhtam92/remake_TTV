import os
import re
from datetime import datetime

from django.db import models

from talktv.constants import LOG_TYPE_CHOICES, LOG_MOBILE, LOG_CHAT, LOG_VIEW
from talktv.constants import RESULT_CHOICES, RESULT_SUCCESS, RESULT_FAILED, RESULT_WAITING, RESULT_WARNING


class ResultManager(models.Manager):
    def create_result_for_log(self, log_type, file_path, total_rows):
        file_name = os.path.basename(file_path)
        params = {
            'log_type': log_type,
            'file_path': file_path,
            'file_name': file_name,
            'log_time': datetime.strptime(re.findall(r'\d+_\d+', file_name)[0], '%Y%m%d_%H%M'),
            'total_rows': total_rows,
            'start_time': datetime.now()
        }
        obj = super(ResultManager, self).create(**params)
        obj.save()
        return obj

    def create_result_for_log_chat(self, file_path, total_rows):
        file_name = os.path.basename(file_path)
        params = {
            'log_type': Result.LOG_CHAT,
            'file_path': file_path,
            'file_name': file_name,
            'log_time': datetime.strptime(re.findall(r'\d+_\d+', file_name)[0], '%Y%m%d_%H%M'),
            'total_rows': total_rows,
            'start_time': datetime.now()
        }
        obj = super(ResultManager, self).create(**params)
        obj.save()
        return obj


class Result(models.Model):
    LOG_VIEW = LOG_VIEW
    LOG_CHAT = LOG_CHAT
    LOG_MOBILE = LOG_MOBILE
    LOG_TYPE_CHOICES = LOG_TYPE_CHOICES

    RESULT_SUCCESS = RESULT_SUCCESS
    RESULT_WARNING = RESULT_WARNING
    RESULT_FAILED = RESULT_FAILED
    RESULT_WAITING = RESULT_WAITING
    RESULT_CHOICES = RESULT_CHOICES

    class Meta:
        db_table = 'tool_result'
        index_together = [
            ['file_name', 'created_at']
        ]

    log_time = models.DateTimeField(blank=True, null=True, default=None)
    file_name = models.CharField(max_length=255)
    file_path = models.TextField()
    log_type = models.CharField(max_length=8, choices=LOG_TYPE_CHOICES, editable=False)

    is_finished = models.BooleanField(default=False)
    result = models.CharField(max_length=8, choices=RESULT_CHOICES, default=RESULT_WAITING, editable=False)
    total_rows = models.PositiveIntegerField()
    success_rows = models.PositiveIntegerField(default=0)
    task_time = models.IntegerField(default=0)

    retries = models.IntegerField(default=0)
    retry = models.BooleanField(default=False)

    system_log = models.TextField(blank=True)

    start_time = models.DateTimeField(editable=False)
    stop_time = models.DateTimeField(null=True, default=None, editable=False)

    created_at = models.DateTimeField(auto_now_add=True, auto_now=False, editable=False)
    updated_at = models.DateTimeField(auto_now_add=False, auto_now=True, editable=False)

    objects = ResultManager()

    @property
    def percentage(self):
        if self.total_rows == 0:
            return 0
        return round(self.success_rows * 100.0 / self.total_rows, 2)

    def set_system_log(self, *args):
        content = " ".join([str(i) for i in args])
        self.system_log = content

    def finish(self, success_rows=0, task_time=0):
        self.success_rows = success_rows
        self.task_time = int(task_time)
        if self.percentage >= 95:
            self.result = Result.RESULT_SUCCESS
        elif self.percentage >= 90:
            self.result = Result.RESULT_WARNING
        else:
            self.result = Result.RESULT_FAILED
        self.is_finished = True
        self.stop_time = datetime.now()
        self.save()

    def finish_log_chat(self, success_rows=0, task_time=0):
        self.success_rows = success_rows
        self.task_time = int(task_time)
        if self.percentage >= 90:
            self.result = Result.RESULT_SUCCESS
        else:
            self.result = Result.RESULT_FAILED
        self.is_finished = True
        self.stop_time = datetime.now()
        self.save()

    def start_retrying(self):
        self.start_time = datetime.now()
        self.stop_time = None
        self.is_finished = False
        self.retries += 1
        self.save()

    def finish_retrying(self):
        self.stop_time = datetime.now()
        self.is_finished = True
        self.retry = False
        self.save()

    def __unicode__(self):
        return unicode(self.file_name)
