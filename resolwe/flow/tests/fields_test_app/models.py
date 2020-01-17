# pylint: disable=missing-docstring
from versionfield import VersionField

from django.db import models

from resolwe.flow.models.fields import ResolweSlugField


class TestModel(models.Model):

    name = models.CharField(max_length=30)

    slug = ResolweSlugField(populate_from="name", unique_with="version")

    version = VersionField(default="0.0.0")
