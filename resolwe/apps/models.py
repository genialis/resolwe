"""
===========
Apps Models
===========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import models
from django.contrib.postgres.fields import JSONField

from resolwe.flow.models import BaseModel


class Package(BaseModel):

    """Postgres model for storing packages."""

    class Meta(BaseModel.Meta):
        """App Meta options."""
        permissions = (
            ("view_package", "Can view packages"),
            ("share_package", "Can share packages"),
        )

    #: list of available modules (PostgreSQL ArrayField coming in Django 1.8)
    modules = JSONField()

    #: index page of the app
    index = models.CharField(max_length=1000)


class App(BaseModel):

    """Postgres model for storing apps."""

    class Meta(BaseModel.Meta):
        """App Meta options."""
        permissions = (
            ("view_app", "Can view apps"),
            ("edit_app", "Can edit apps"),
            ("share_app", "Can share apps"),
            ("add_app", "Can add apps"),
        )

    #: parent package
    package = models.ForeignKey('Package')

    #: list of modules to display (PostgreSQL ArrayField coming in Django 1.8)
    modules = JSONField()

    #: list of collections associated with the app
    collections = models.ManyToManyField('flow.Collection')

    #: default collection on the app
    default_collection = models.ForeignKey('flow.Collection', related_name='default_collection',
                                           blank=True, null=True, on_delete=models.SET_NULL)

    #: detailed description
    description = models.TextField(blank=True)
