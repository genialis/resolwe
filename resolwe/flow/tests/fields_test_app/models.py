# pylint: disable=missing-docstring
from django.db import models
from versionfield import VersionField

from resolwe.flow.models.fields import ResolweSlugField


class TestModel(models.Model):
    name = models.CharField(max_length=30)

    slug = ResolweSlugField(populate_from="name", unique_with="version")

    version = VersionField(default="0.0.0")
