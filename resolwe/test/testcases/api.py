""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.test.TransactionResolweAPITestCase
    :members:

.. autoclass:: resolwe.test.ResolweAPITestCase

"""

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase as DjangoTestCase
from django.urls import reverse
from rest_framework.test import (
    APIRequestFactory,
    APITransactionTestCase,
    force_authenticate,
)

from resolwe.test import TestCaseHelpers


class TransactionResolweAPITestCase(TestCaseHelpers, APITransactionTestCase):
    """Base class for testing Resolwe REST API.

    This class is derived from Django REST Framework's
    :drf:`APITransactionTestCase <testing/#test-cases>` class and has
    implemented some basic features that makes testing Resolwe
    API easier. These features includes following functions:

    .. automethod:: _get_list
    .. automethod:: _get_detail
    .. automethod:: _post
    .. automethod:: _patch
    .. automethod:: _delete
    .. automethod:: _detail_permissions

    It also has included 2 views made from referenced DRF's ``ViewSet``.
    First mimic list view and has following links between request's
    methods and ViewSet's methods:

      *  ``GET`` -> ``list``
      *  ``POST`` -> ``create``

    Second mimic detail view and has following links between request's
    methods and ViewSet's methods:

      *  ``GET`` -> ``retrieve``
      *  ``PUT`` -> ``update``
      *  ``PATCH`` -> ``partial_update``
      *  ``DELETE`` -> ``destroy``
      *  ``POST`` -> ``permissions``

    If any of the listed methods is not defined in the VievSet,
    corresponding link is omitted.

    .. note::
        ``self.viewset`` (instance of DRF's ``Viewset``) and
        ``self.resource_name`` (string) must be defined before calling
        super ``setUp`` method to work properly.

    ``self.factory`` is instance of DRF's ``APIRequestFactory``.

    """

    def setUp(self):
        """Prepare data."""
        super().setUp()
        User = get_user_model()
        # TODO: Remove this when removing fixtures
        if User.objects.filter(pk=2).exists():
            self.user1 = User.objects.get(pk=2)
        if User.objects.filter(pk=3).exists():
            self.user2 = User.objects.get(pk=3)
        if User.objects.filter(pk=4).exists():
            self.user3 = User.objects.get(pk=4)
        if User.objects.filter(pk=5).exists():
            self.admin = User.objects.get(pk=5)

        # TODO: Change username to `admin` when fixtures are removed
        self.admin = User.objects.create_superuser(
            username="admin2", email="admin@test.com", password="admin"
        )
        self.contributor = User.objects.create_user(
            username="contributor", email="contributor@genialis.com"
        )
        self.user = User.objects.create_user(username="user", email="user@genialis.com")
        # Create the anonymous user (if it does not exist).
        # The user is created during migrations, but deleted between tests.
        if not User.objects.filter(username=settings.ANONYMOUS_USER_NAME).exists():
            User.objects.create_user(
                username=settings.ANONYMOUS_USER_NAME,
                email=f"{settings.ANONYMOUS_USER_NAME}@genialis.com",
            )

        if not hasattr(self, "viewset"):
            raise KeyError("`self.viewset` must be defined in child class")

        if not hasattr(self, "resource_name"):
            raise KeyError("`self.resource_name` must be defined in child class")

        self.factory = APIRequestFactory()

        list_url_mapping = {}
        if hasattr(self.viewset, "list"):
            list_url_mapping["get"] = "list"
        if hasattr(self.viewset, "create"):
            list_url_mapping["post"] = "create"

        if list_url_mapping:
            self.list_view = self.viewset.as_view(list_url_mapping)

        detail_url_mapping = {}
        if hasattr(self.viewset, "retrieve"):
            detail_url_mapping["get"] = "retrieve"
        if hasattr(self.viewset, "update"):
            detail_url_mapping["put"] = "update"
        if hasattr(self.viewset, "partial_update"):
            detail_url_mapping["patch"] = "partial_update"
        if hasattr(self.viewset, "destroy"):
            detail_url_mapping["delete"] = "destroy"
        if hasattr(self.viewset, "detail_permissions"):
            detail_url_mapping["post"] = "detail_permissions"

        if detail_url_mapping:
            self.detail_view = self.viewset.as_view(detail_url_mapping)

    def detail_url(self, pk):
        """Get detail url."""
        return reverse(
            "resolwe-api:{}-detail".format(self.resource_name), kwargs={"pk": pk}
        )  # noqa pylint: disable=no-member

    def detail_permissions(self, pk):
        """Get detail permissions url."""
        return reverse(
            "resolwe-api:{}-permissions".format(self.resource_name), kwargs={"pk": pk}
        )  # noqa pylint: disable=no-member

    @property
    def list_url(self):
        """Get list url."""
        return reverse("resolwe-api:{}-list".format(self.resource_name))

    def _render_query_params(self, params):
        """Generate query parameters from given dict."""
        if not params:
            return ""

        return "?" + "&".join(
            "{}={}".format(key, value) for key, value in params.items()
        )

    def _get_list(self, user=None, query_params={}):
        """Make ``GET`` request to ``self.list_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        url = self.list_url + self._render_query_params(query_params)
        request = self.factory.get(url, format="json")
        force_authenticate(request, user)
        return self.list_view(request)

    def _get_detail(self, pk, user=None, query_params={}):
        """Make ``GET`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        url = self.detail_url(pk) + self._render_query_params(query_params)
        request = self.factory.get(url, format="json")
        force_authenticate(request, user)
        return self.detail_view(request, pk=pk)

    def _post(self, data={}, user=None, query_params={}):
        """Make ``POST`` request to ``self.list_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param dict data: data for posting in request's body
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        url = self.list_url + self._render_query_params(query_params)
        request = self.factory.post(url, data=data, format="json")
        force_authenticate(request, user)
        return self.list_view(request)

    def _patch(self, pk, data={}, user=None, query_params={}):
        """Make ``PATCH`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param dict data: data for posting in request's body
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        url = self.detail_url(pk) + self._render_query_params(query_params)
        request = self.factory.patch(url, data=data, format="json")
        force_authenticate(request, user)
        return self.detail_view(request, pk=pk)

    def _delete(self, pk, user=None, query_params={}):
        """Make ``DELETE`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        url = self.detail_url(pk) + self._render_query_params(query_params)
        request = self.factory.delete(url, format="json")
        force_authenticate(request, user)
        return self.detail_view(request, pk=pk)

    def _detail_permissions(self, pk, data={}, user=None):
        """Make ``POST`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param dict data: data for posting in request's body
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.post(
            self.detail_permissions(pk), data=data, format="json"
        )
        force_authenticate(request, user)
        return self.detail_view(request, pk=pk)

    def assertKeys(self, data, wanted):
        """Assert dictionary keys."""
        self.assertEqual(sorted(data.keys()), sorted(wanted))


class ResolweAPITestCase(TransactionResolweAPITestCase, DjangoTestCase):
    """Base class for writing Resolwe API tests.

    It is based on :class:`~.TransactionResolweAPITestCase` and Django's
    :class:`~django.test.TestCase`. The latter encloses the test code in
    a database transaction that is rolled back at the end of the test.
    """
