# Generated by Django 4.2.9 on 2024-02-13 07:00

from django.contrib.postgres.operations import UnaccentExtension
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0024_update_sql_entity_methods"),
    ]

    operations = [UnaccentExtension()]
