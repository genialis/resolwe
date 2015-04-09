from django.db import models
from django.core.validators import RegexValidator
from jsonfield import JSONField

from flow.models import BaseModel


class Package(BaseModel):

    #: package version
    version = models.CharField(max_length=50, validators=[
        RegexValidator(
            regex=r'^[0-9]+(\.[0-9]+)*$',
            message='Version must be dot separated integers',
            code='invalid_version'
        )
    ])

    #: list of available modules (PostgreSQL ArrayField coming in Django 1.8)
    modules = JSONField()

    #: index page of the app
    index = models.CharField(max_length=50)


class App(BaseModel):

    #: parent package
    package = models.ForeignKey('Package')

    #: list of modules to display (PostgreSQL ArrayField coming in Django 1.8)
    modules = JSONField()

    #: list of projects associated with the app
    projects = models.ManyToManyField('flow.Project')

    #: default project on the app
    default_project = models.ForeignKey('flow.Project', related_name='default_project', blank=True, null=True, on_delete=models.SET_NULL)

    #: detailed description
    description = models.TextField(blank=True)
