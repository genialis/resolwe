# Generated by Django 3.2.12 on 2022-10-26 12:41

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("flow", "0006_add_descriptor_to_relation"),
    ]

    operations = [
        migrations.AlterField(
            model_name="data",
            name="process_rc",
            field=models.SmallIntegerField(blank=True, null=True),
        ),
    ]