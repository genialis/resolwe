# Generated by Django 4.2.16 on 2024-10-09 08:50

from django.db import migrations, models

from resolwe.flow.models.fields import VersionField


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0028_update_all_sql_entity_methods"),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name="annotationfield",
            name="uniquetogether_name_group",
        ),
        migrations.AddField(
            model_name="annotationfield",
            name="version",
            field=VersionField(default="0.0.0"),
        ),
        migrations.AddConstraint(
            model_name="annotationfield",
            constraint=models.UniqueConstraint(
                fields=("name", "group", "version"),
                name="uniquetogether_name_group_version",
            ),
        ),
    ]
