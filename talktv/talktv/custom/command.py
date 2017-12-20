import os
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand


class CustomBaseCommand(BaseCommand):
    command_name = 'base_command'

    def _write_log(self, content):
        f_path = os.path.join(settings.BASE_DIR, '{}.txt'.format(self.command_name))
        with open(f_path, 'a+') as f:
            f.write(content + '\n')

    def _error_log(self, *args):
        content = ' '.join([str(i) for i in args])
        content = 'ERROR   : {}'.format(content)
        self._write_log(content)

    def _warning_log(self, *args):
        content = ' '.join([str(i) for i in args])
        content = 'WARNING : {}'.format(content)
        self._write_log(content)

    def _success_log(self, *args):
        content = ' '.join([str(i) for i in args])
        content = 'SUCCESS : {}'.format(content)
        self._write_log(content)

    def _info_log(self, *args):
        content = '   '.join([str(i) for i in args])
        content = '{} : {}'.format(datetime.now().replace(microsecond=0), content)
        self._write_log(content)
