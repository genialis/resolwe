# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import models


class TestModel(models.Model):

    class Meta:
        permissions = (
            ("view_model", "Can view model"),
            ("edit_model", "Can edit model"),
        )

    name = models.CharField(max_length=30)

    number = models.IntegerField()
