"""Urls for Django Rest Framework."""
from django.urls import include, path

from rest_framework import routers

from .flow.views import (
    CollectionViewSet,
    DataViewSet,
    DescriptorSchemaViewSet,
    EntityViewSet,
    ProcessViewSet,
    RelationViewSet,
    StorageViewSet,
)
from .storage.views import UploadConfig

api_router = routers.DefaultRouter(trailing_slash=False)
api_router.register(r"collection", CollectionViewSet)
api_router.register(r"process", ProcessViewSet)
api_router.register(r"data", DataViewSet)
api_router.register(r"entity", EntityViewSet)
api_router.register(r"relation", RelationViewSet)
api_router.register(r"descriptorschema", DescriptorSchemaViewSet)
api_router.register(r"storage", StorageViewSet)
api_router.register(r"upload_config", UploadConfig, basename="upload_config")


urlpatterns = [
    path("api/", include((api_router.urls, "resolwe-api"), namespace="resolwe-api")),
]
