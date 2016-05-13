from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth.models import User
from django.core.urlresolvers import reverse
from django.test import override_settings

from rest_framework.test import APITestCase, APIRequestFactory, force_authenticate


@override_settings(CELERY_ALWAYS_EAGER=True)
class ResolweAPITestCase(APITestCase):

    def setUp(self):
        super(ResolweAPITestCase, self).setUp()

        self.factory = APIRequestFactory()

        self.user1 = User.objects.get(pk=1)
        self.user2 = User.objects.get(pk=2)
        self.user3 = User.objects.get(pk=3)
        self.admin = User.objects.get(pk=4)

        list_url_mapping = {}
        if hasattr(self.viewset, 'list'):
            list_url_mapping['get'] = 'list'
        if hasattr(self.viewset, 'create'):
            list_url_mapping['post'] = 'create'

        self.list_view = self.viewset.as_view(list_url_mapping)

        detail_url_mapping = {}
        if hasattr(self.viewset, 'retrieve'):
            detail_url_mapping['get'] = 'retrieve'
        if hasattr(self.viewset, 'update'):
            detail_url_mapping['put'] = 'update'
        if hasattr(self.viewset, 'partial_update'):
            detail_url_mapping['patch'] = 'partial_update'
        if hasattr(self.viewset, 'destroy'):
            detail_url_mapping['delete'] = 'destroy'
        if hasattr(self.viewset, 'detail_permissions'):
            detail_url_mapping['post'] = 'detail_permissions'

        self.detail_view = self.viewset.as_view(detail_url_mapping)

        self.detail_url = lambda pk: reverse('resolwe-api:{}-detail'.format(self.resource_name), kwargs={'pk': pk})
        self.detail_permissions = lambda pk: reverse('resolwe-api:{}-permissions'.format(self.resource_name),
                                                     kwargs={'pk': pk})
        self.list_url = reverse('resolwe-api:collection-list')

    def _get_list(self, user=None):
        request = self.factory.get(self.list_url, format='json')
        force_authenticate(request, user)
        resp = self.list_view(request)
        resp.render()
        return resp

    def _get_detail(self, pk, user=None):
        request = self.factory.get(self.detail_url(pk), format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def _post(self, data={}, user=None):
        request = self.factory.post(self.list_url, data=data, format='json')
        force_authenticate(request, user)
        resp = self.list_view(request)
        resp.render()
        return resp

    def _patch(self, pk, data={}, user=None):
        request = self.factory.patch(self.detail_url(pk), data=data, format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def _delete(self, pk, user=None):
        request = self.factory.delete(self.detail_url(pk), format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def _detail_permissions(self, pk, data={}, user=None):
        request = self.factory.post(self.detail_permissions(pk), data=data, format='json')
        force_authenticate(request, user)
        resp = self.detail_view(request, pk=pk)
        resp.render()
        return resp

    def assertKeys(self, data, wanted):
        self.assertEqual(sorted(data.keys()), sorted(wanted))
