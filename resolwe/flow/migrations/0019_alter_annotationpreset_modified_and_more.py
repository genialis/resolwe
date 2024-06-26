# Generated by Django 4.2.7 on 2023-12-06 11:59

from django.db import migrations

import resolwe.flow.models.base


class Migration(migrations.Migration):
    dependencies = [
        ("flow", "0018_add_annotationvalue_default_order"),
    ]

    operations = [
        migrations.AlterField(
            model_name="annotationpreset",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="collection",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="data",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="descriptorschema",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="entity",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="process",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="relation",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="storage",
            name="modified",
            field=resolwe.flow.models.base.ModifiedField(auto_now=True, db_index=True),
        ),
    ]
