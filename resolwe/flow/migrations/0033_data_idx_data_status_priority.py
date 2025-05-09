# Generated by Django 4.2.16 on 2024-11-08 09:42

from django.db import migrations, models

import resolwe.flow.models.base


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0032_alter_process_managers"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="data",
            index=models.Index(
                models.Case(
                    models.When(
                        status=resolwe.flow.models.base.DataStatus["ERROR"],
                        then=models.Value(0),
                    ),
                    models.When(
                        status=resolwe.flow.models.base.DataStatus["UPLOADING"],
                        then=models.Value(1),
                    ),
                    models.When(
                        status=resolwe.flow.models.base.DataStatus["PREPARING"],
                        then=models.Value(2),
                    ),
                    models.When(
                        status=resolwe.flow.models.base.DataStatus["WAITING"],
                        then=models.Value(3),
                    ),
                    models.When(
                        status=resolwe.flow.models.base.DataStatus["RESOLVING"],
                        then=models.Value(4),
                    ),
                    models.When(
                        status=resolwe.flow.models.base.DataStatus["DONE"],
                        then=models.Value(5),
                    ),
                ),
                name="idx_data_status_priority",
            ),
        ),
    ]
