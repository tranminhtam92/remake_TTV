from django.db import models


class ErrorDetailManager(models.Manager):
    def save_error_detail(self, result, row, *args):
        content = " ".join([str(i) for i in args])
        params = {
            'result': result,
            'retries': result.retries,
            'row': row,
            'detail': content,
        }
        obj = self.create(**params)
        obj.save()

    def get_latest_error_details(self, result):
        query_set = self.get_queryset().filter(result=result, retries=result.retries)
        if len(query_set) > 0:
            return list(query_set)
        return None


class ErrorDetail(models.Model):
    class Meta:
        db_table = 'tool_error_detail'
        index_together = [
            ['result', 'retries', 'row'],
        ]

    result = models.ForeignKey('log_storage.Result', on_delete=models.CASCADE)
    retries = models.IntegerField()
    row = models.IntegerField()
    detail = models.TextField()

    objects = ErrorDetailManager()

    def __unicode__(self):
        return "{}_{}_{}".format(self.result.file_name, self.retries, self.row)

    @property
    def is_retried(self):
        return self.result.retries != self.retries
