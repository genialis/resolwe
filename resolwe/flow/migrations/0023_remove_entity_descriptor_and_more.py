# Generated by Django 4.2.11 on 2024-03-18 13:12

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0022_fix_group_by"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="entity",
            name="descriptor",
        ),
        migrations.RemoveField(
            model_name="entity",
            name="descriptor_dirty",
        ),
        migrations.RemoveField(
            model_name="entity",
            name="descriptor_schema",
        ),
        migrations.RemoveField(
            model_name="process",
            name="entity_descriptor_schema",
        ),
    ]
