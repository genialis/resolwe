from django.conf.urls import include, url

from rest_framework import routers

from .apps.views import AppViewSet, PackageViewSet
from .flow.views import ProjectViewSet, ToolViewSet, DataViewSet, TemplateViewSet, TriggerViewSet, StorageViewSet

apiRouter = routers.DefaultRouter(trailing_slash=False)
apiRouter.register(r'package', PackageViewSet)
apiRouter.register(r'app', AppViewSet)
apiRouter.register(r'project', ProjectViewSet)
apiRouter.register(r'tool', ToolViewSet)
apiRouter.register(r'data', DataViewSet)
apiRouter.register(r'template', TemplateViewSet)
apiRouter.register(r'trigger', TriggerViewSet)
apiRouter.register(r'storage', StorageViewSet)


urlpatterns = [
    url(r'^', include(apiRouter.urls, namespace='resolwe-api')),
]
