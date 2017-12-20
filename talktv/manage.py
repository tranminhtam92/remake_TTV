#!/usr/bin/env python
import logging
import os
import sys
import traceback

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "talktv.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            import django
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise
    log = logging.getLogger('command_django')
    try:
        execute_from_command_line(sys.argv)
    except Exception as e:
        err_msg = traceback.format_exc()
        log.error(err_msg)
        raise e
