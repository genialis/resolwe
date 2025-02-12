# Generated by Django 5.1.5 on 2025-02-12 09:47

from django.db import migrations

import resolwe.flow.models.fields


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0034_remove_data_idx_data_status_priority_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="annotationfield",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="annotationpreset",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="annotationvalue",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="collection",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="data",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="descriptorschema",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="entity",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="process",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="relation",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
        migrations.AlterField(
            model_name="storage",
            name="version",
            field=resolwe.flow.models.fields.VersionField(default="0.0.0"),
        ),
    ]
