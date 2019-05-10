# pylint: disable=missing-docstring
from django.db import models


class TestModel(models.Model):

    class Meta:
        default_permissions = ()
        permissions = (
            ('view_testmodel', "Can view model"),
            ('edit_testmodel', "Can edit model"),
        )

    name = models.CharField(max_length=30)

    field_process_type = models.CharField(max_length=100)

    number = models.IntegerField()

    date = models.DateTimeField(null=True)


class TestModelWithDependency(models.Model):

    name = models.CharField(max_length=30)

    dependencies = models.ManyToManyField('TestDependency')

    dependency = models.ForeignKey(
        'TestDependency', related_name='main_dep', null=True, on_delete=models.SET_NULL
    )


class TestDependency(models.Model):

    name = models.CharField(max_length=30)


class TestSelfDependency(models.Model):

    name = models.CharField(max_length=30)

    dependencies = models.ManyToManyField('self', symmetrical=False, related_name='parents')
