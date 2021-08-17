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

from .billing.views import (
    BillingAccountViewSet,
    EstimatedCostByBillingAccountViewset,
    EstimatedCostByCollectionViewset,
)

api_router = routers.DefaultRouter(trailing_slash=False)
api_router.register(r"collection", CollectionViewSet)
api_router.register(r"process", ProcessViewSet)
api_router.register(r"data", DataViewSet)
api_router.register(r"entity", EntityViewSet)
api_router.register(r"relation", RelationViewSet)
api_router.register(r"descriptorschema", DescriptorSchemaViewSet)
api_router.register(r"storage", StorageViewSet)
api_router.register(r"estimatedcostbycollection", EstimatedCostByCollectionViewset)
api_router.register(
    r"estimatedcostbybillingaccount", EstimatedCostByBillingAccountViewset
)
api_router.register(r"billingaccount", BillingAccountViewSet)


urlpatterns = [
    path("api/", include((api_router.urls, "resolwe-api"), namespace="resolwe-api")),
]
