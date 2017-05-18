# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import models


class TestModel(models.Model):

    class Meta:
        permissions = (
            ('view_testmodel', "Can view model"),
            ('edit_testmodel', "Can edit model"),
        )

    name = models.CharField(max_length=30)

    field_process_type = models.CharField(max_length=100)

    number = models.IntegerField()


class TestModelWithDependency(models.Model):

    name = models.CharField(max_length=30)

    dependencies = models.ManyToManyField('TestDependency')


class TestDependency(models.Model):

    name = models.CharField(max_length=30)
