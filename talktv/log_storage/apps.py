# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import warnings

from django.apps import AppConfig
from django.core.cache import CacheKeyWarning


class LogStorageConfig(AppConfig):
    name = 'log_storage'


warnings.simplefilter("ignore", CacheKeyWarning)
