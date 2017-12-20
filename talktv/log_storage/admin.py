import os
from datetime import timedelta, datetime

from dateutil import parser
from decouple import config
from django.contrib import admin
from django.core.urlresolvers import reverse
from django.db.models import F
from django.utils.html import format_html

from log_storage.models import Result, ErrorDetail

TRACKING_PARSE_LOG_DIR = config('TRACKING_PARSE_LOG_DIR')


class LogAtFilter(admin.SimpleListFilter):
    title = 'log at'
    parameter_name = 'log_time'

    def lookups(self, request, model_admin):
        objects = Result.objects.extra(select={'log_at': "date(log_time)"}).values('log_at').distinct()
        dates = set([obj['log_at'].replace(day=1) for obj in list(objects) if obj['log_at'] is not None])
        choices = [(i, i.strftime('%b %Y')) for i in dates]
        return choices

    def queryset(self, request, queryset):
        if self.value():
            dt_time = parser.parse(self.value())
            month = dt_time.month
            year = dt_time.year
            return queryset.filter(log_time__month=month, log_time__year=year)


class ResultAdmin(admin.ModelAdmin):
    model = Result

    def percentage(self, obj):
        if obj.total_rows == 0:
            return 0
        if obj.result == Result.RESULT_WAITING and \
                (obj.log_type == Result.LOG_VIEW or obj.log_type == Result.LOG_MOBILE):
            path = os.path.join(TRACKING_PARSE_LOG_DIR, obj.file_name)
            if not os.path.exists(path):
                return "0 %"
            with open(path, 'r') as f:
                rows = len(f.readlines())
                return "{} %".format(round(rows * 100.0 / obj.total_rows, 2))
        return "{} %".format(round(obj.success_rows * 100.0 / obj.total_rows, 2))

    def total_time(self, obj):
        if obj.result == Result.RESULT_WAITING:
            seconds = int((datetime.now() - obj.start_time).total_seconds())
        else:
            seconds = obj.task_time

        return str(timedelta(seconds=seconds))

    total_time.admin_order_field = 'task_time'

    def start_at(self, obj):
        return obj.start_time.replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

    start_at.admin_order_field = 'start_time'

    def log_at(self, obj):
        if obj.log_time is None:
            return ''
        return obj.log_time.replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

    log_at.admin_order_field = 'log_time'

    list_display = ('id', 'log_type', 'file_name', 'log_at',
                    'total_rows', 'success_rows', 'percentage',
                    'result', 'start_at', 'total_time')
    list_filter = ('log_type', 'result', LogAtFilter, ('log_time', admin.DateFieldListFilter))


class LogTypeFilter(admin.SimpleListFilter):
    title = 'Log Type'
    parameter_name = 'result'

    def lookups(self, request, model_admin):
        return Result.LOG_TYPE_CHOICES

    def queryset(self, request, queryset):
        if self.value() == Result.LOG_CHAT:
            return queryset.filter(result__log_type=Result.LOG_CHAT)
        elif self.value() == Result.LOG_VIEW:
            return queryset.filter(result__log_type=Result.LOG_VIEW)
        elif self.value() == Result.LOG_MOBILE:
            return queryset.filter(result__log_type=Result.LOG_MOBILE)


class IsRetriedFilter(admin.SimpleListFilter):
    title = 'Is Retried'
    parameter_name = 'retries'

    def lookups(self, request, model_admin):
        return (
            ('True', 'True'),
            ('False', 'False'),
        )

    def queryset(self, request, queryset):
        if self.value() == 'True':
            return queryset.exclude(retries=F('result__retries'))
        if self.value() == 'False':
            return queryset.filter(retries=F('result__retries'))


class ErrorDetailAdmin(admin.ModelAdmin):
    model = Result

    def result_link(self, obj):
        url = reverse('admin:log_storage_result_change', args=(obj.result_id,))
        return format_html("<a href='{}'>{}_{}</a>", url, obj.result, obj.result.retries)

    result_link.admin_order_field = 'result'
    result_link.short_description = 'result'

    list_display = ('id', 'result_link', 'retries', 'is_retried', 'row', 'detail')
    ordering = ('-result_id', '-retries', 'row')
    list_filter = (LogTypeFilter, IsRetriedFilter,)


admin.site.register(Result, ResultAdmin)
admin.site.register(ErrorDetail, ErrorDetailAdmin)

admin.site.site_title = 'TalkTV'
admin.site.site_header = 'TalkTV Data Monitor'
admin.site.index_title = 'Dashboard'
