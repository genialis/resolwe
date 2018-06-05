""".. Ignore pydocstyle D400.

==================
Elastic Paginators
==================

Paginator classes used in Elastic app.


.. autoclass:: resolwe.elastic.pagination.LimitOffsetPostPagination

"""
from rest_framework.pagination import LimitOffsetPagination, _positive_int


def get_query_param(request, key):
    """Get query parameter uniformly for GET and POST requests."""
    value = request.query_params.get(key) or request.data.get(key)
    if value is None:
        raise KeyError()
    return value


class LimitOffsetPostPagination(LimitOffsetPagination):
    """Limit/offset paginator.

    This is standard limit/offset paginator from Django REST framework,
    with difference that it supports passing ``limit`` and ``offset``
    attributes also in the body of the request (not just as query
    parameter).
    """

    def get_limit(self, request):
        """Return limit parameter."""
        if self.limit_query_param:
            try:
                return _positive_int(
                    get_query_param(request, self.limit_query_param),
                    strict=True,
                    cutoff=self.max_limit
                )
            except (KeyError, ValueError):
                pass

        return self.default_limit

    def get_offset(self, request):
        """Return offset parameter."""
        try:
            return _positive_int(
                get_query_param(request, self.offset_query_param),
            )
        except (KeyError, ValueError):
            return 0
