"""Urls for Django Rest Framework."""
from django.conf.urls import include, url

from rest_framework import routers

from .flow.views import (
    CollectionViewSet, DataViewSet, DescriptorSchemaViewSet, EntityViewSet, ProcessViewSet, RelationViewSet,
    StorageViewSet,
)

api_router = routers.DefaultRouter(trailing_slash=False)  # pylint: disable=invalid-name
api_router.register(r'collection', CollectionViewSet)
api_router.register(r'process', ProcessViewSet)
api_router.register(r'data', DataViewSet)
api_router.register(r'entity', EntityViewSet)
api_router.register(r'relation', RelationViewSet)
api_router.register(r'descriptorschema', DescriptorSchemaViewSet)
api_router.register(r'storage', StorageViewSet)


urlpatterns = [  # pylint: disable=invalid-name
    url(r'^', include(api_router.urls, namespace='resolwe-api')),
]
