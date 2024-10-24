# Generated by Django 4.2.16 on 2024-10-09 09:41

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


def assign_contributor(apps, schema_editor):
    """Assign initial contributor to annotation values."""

    AnnotationValue = apps.get_model("flow", "AnnotationValue")
    for annotation_value in AnnotationValue.objects.iterator():
        annotation_value.contributor = annotation_value.entity.contributor
        annotation_value.save(update_fields=["contributor"])


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("flow", "0029_remove_annotationfield_uniquetogether_name_group_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="annotationvalue",
            name="contributor",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.RunPython(assign_contributor),
        migrations.AlterField(
            model_name="annotationvalue",
            name="contributor",
            field=models.ForeignKey(
                default=None,
                on_delete=django.db.models.deletion.PROTECT,
                to=settings.AUTH_USER_MODEL,
            ),
            preserve_default=False,
        ),
    ]