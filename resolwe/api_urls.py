"""
Urls for Django Rest Framework

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.conf.urls import include, url

from rest_framework import routers

from .apps.views import AppViewSet, PackageViewSet
from .flow.views import (ProjectViewSet, ToolViewSet, DataViewSet,
                         DescriptorSchemaViewSet, TriggerViewSet, StorageViewSet)

api_router = routers.DefaultRouter(trailing_slash=False)  # pylint: disable=invalid-name
api_router.register(r'package', PackageViewSet)
api_router.register(r'app', AppViewSet)
api_router.register(r'project', ProjectViewSet)
api_router.register(r'tool', ToolViewSet)
api_router.register(r'data', DataViewSet)
api_router.register(r'descriptorschema', DescriptorSchemaViewSet)
api_router.register(r'trigger', TriggerViewSet)
api_router.register(r'storage', StorageViewSet)


urlpatterns = [  # pylint: disable=invalid-name
    url(r'^', include(api_router.urls, namespace='resolwe-api')),
]
