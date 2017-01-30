"""Search router."""
from rest_framework.routers import DefaultRouter, Route


class SearchRouter(DefaultRouter):
    """Custom router for search endpoints.

    Search endpoints don't follow REST principles and thus don't need
    routes that default router provides.

    """

    routes = [
        Route(
            url=r'^{prefix}{trailing_slash}$',
            mapping={
                'get': 'list',
                'post': 'list_with_post'
            },
            name='{basename}',
            initkwargs={}
        )
    ]
