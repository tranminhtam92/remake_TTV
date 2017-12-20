from django.db.models import signals

from error_detail import ErrorDetail
from log_storage.triggers import retry_parse_log
from result import Result

signals.post_save.connect(retry_parse_log, sender=Result)
