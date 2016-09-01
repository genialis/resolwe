""".. Ignore pydocstyle D400.

=======
Testing
=======

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth.models import User
from django.core.urlresolvers import reverse
from django.test import override_settings

from rest_framework.test import APITestCase, APIRequestFactory, force_authenticate


@override_settings(CELERY_ALWAYS_EAGER=True)
class ResolweAPITestCase(APITestCase):
    """Base class for testing Resolwe REST API.

    This class is derived from Django REST Framework's
    :drf:`APITestCase <testing/#test-cases>` class and has implemented
    some basic features that makes testing Resolwe API easier. These
    features includes following functions:

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
        super(ResolweAPITestCase, self).setUp()

        # TODO: Remove this when removing fixtures
        if User.objects.filter(pk=2).exists():
            self.user1 = User.objects.get(pk=2)
        if User.objects.filter(pk=3).exists():
            self.user2 = User.objects.get(pk=3)
        if User.objects.filter(pk=4).exists():
            self.user3 = User.objects.get(pk=4)
        if User.objects.filter(pk=5).exists():
            self.admin = User.objects.get(pk=5)

        if not hasattr(self, 'viewset'):
            raise KeyError("`self.viewset` must be defined in child class")

        if not hasattr(self, 'resource_name'):
            raise KeyError("`self.resource_name` must be defined in child class")

        self.factory = APIRequestFactory()

        list_url_mapping = {}
        if hasattr(self.viewset, 'list'):  # pylint: disable=no-member
            list_url_mapping['get'] = 'list'
        if hasattr(self.viewset, 'create'):  # pylint: disable=no-member
            list_url_mapping['post'] = 'create'

        self.list_view = self.viewset.as_view(list_url_mapping)  # pylint: disable=no-member

        detail_url_mapping = {}
        if hasattr(self.viewset, 'retrieve'):  # pylint: disable=no-member
            detail_url_mapping['get'] = 'retrieve'
        if hasattr(self.viewset, 'update'):  # pylint: disable=no-member
            detail_url_mapping['put'] = 'update'
        if hasattr(self.viewset, 'partial_update'):  # pylint: disable=no-member
            detail_url_mapping['patch'] = 'partial_update'
        if hasattr(self.viewset, 'destroy'):  # pylint: disable=no-member
            detail_url_mapping['delete'] = 'destroy'
        if hasattr(self.viewset, 'detail_permissions'):  # pylint: disable=no-member
            detail_url_mapping['post'] = 'detail_permissions'

        self.detail_view = self.viewset.as_view(detail_url_mapping)  # pylint: disable=no-member

    def detail_url(self, pk):
        """Get detail url."""
        return reverse('resolwe-api:{}-detail'.format(self.resource_name), kwargs={'pk': pk})  # noqa pylint: disable=no-member

    def detail_permissions(self, pk):
        """Get detail permissions url."""
        return reverse('resolwe-api:{}-permissions'.format(self.resource_name), kwargs={'pk': pk})  # noqa pylint: disable=no-member

    @property
    def list_url(self):
        """Get list url."""
        return reverse('resolwe-api:{}-list'.format(self.resource_name))  # pylint: disable=no-member

    def _get_list(self, user=None):
        """Make ``GET`` request to ``self.list_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: Rendered API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.get(self.list_url, format='json')
        force_authenticate(request, user)
        resp = self.list_view(request)
        resp.render()
        return resp

    def _get_detail(self, pk, user=None):
        """Make ``GET`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: Rendered API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.get(self.detail_url(pk), format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def _post(self, data={}, user=None):
        """Make ``POST`` request to ``self.list_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param dict data: data for posting in request's body
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: Rendered API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.post(self.list_url, data=data, format='json')
        force_authenticate(request, user)
        resp = self.list_view(request)
        resp.render()
        return resp

    def _patch(self, pk, data={}, user=None):
        """Make ``PATCH`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param dict data: data for posting in request's body
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: Rendered API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.patch(self.detail_url(pk), data=data, format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def _delete(self, pk, user=None):
        """Make ``DELETE`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: Rendered API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.delete(self.detail_url(pk), format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def _detail_permissions(self, pk, data={}, user=None):
        """Make ``POST`` request to ``self.detail_view`` view.

        If ``user`` is not ``None``, the given user is authenticated
        before making the request.

        :param int pk: Primary key of the coresponding object
        :param dict data: data for posting in request's body
        :param user: User to authenticate in request
        :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
        :return: Rendered API response object
        :rtype: :drf:`Response <responses/#response>`

        """
        request = self.factory.post(self.detail_permissions(pk), data=data, format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def assertKeys(self, data, wanted):  # pylint: disable=invalid-name
        """Assert dictionary keys."""
        self.assertEqual(sorted(data.keys()), sorted(wanted))
