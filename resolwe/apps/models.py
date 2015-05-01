"""
===========
Apps Models
===========

"""
from django.db import models
from jsonfield import JSONField

from resolwe.flow.models import BaseModel


class Package(BaseModel):

    """Postgres model for storing packages."""

    #: package version
    version = models.PositiveIntegerField()

    #: list of available modules (PostgreSQL ArrayField coming in Django 1.8)
    modules = JSONField()

    #: index page of the app
    index = models.CharField(max_length=50)


class App(BaseModel):

    """Postgres model for storing apps."""

    #: parent package
    package = models.ForeignKey('Package')

    #: list of modules to display (PostgreSQL ArrayField coming in Django 1.8)
    modules = JSONField()

    #: list of projects associated with the app
    projects = models.ManyToManyField('flow.Project')

    #: default project on the app
    default_project = models.ForeignKey('flow.Project', related_name='default_project',
                                        blank=True, null=True, on_delete=models.SET_NULL)

    #: detailed description
    description = models.TextField(blank=True)
