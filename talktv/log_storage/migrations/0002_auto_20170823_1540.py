# -*- coding: utf-8 -*-
# Generated by Django 1.11.3 on 2017-08-23 15:40
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('log_storage', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='result',
            name='log_time',
            field=models.DateTimeField(blank=True, default=None, null=True),
        ),
        migrations.AlterField(
            model_name='result',
            name='system_log',
            field=models.TextField(blank=True),
        ),
    ]
