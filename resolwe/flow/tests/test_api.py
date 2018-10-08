# pylint: disable=missing-docstring
import mock

from django.contrib.auth.models import AnonymousUser
from django.contrib.contenttypes.models import ContentType
from django.core.urlresolvers import reverse
from django.db import DEFAULT_DB_ALIAS, connections
from django.test.utils import CaptureQueriesContext

from guardian.conf.settings import ANONYMOUS_USER_NAME
from guardian.models import UserObjectPermission
from guardian.shortcuts import assign_perm, remove_perm
from rest_framework import exceptions, status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.views import CollectionViewSet, DataViewSet, EntityViewSet, ProcessViewSet
from resolwe.test import ResolweAPITestCase, TestCase

factory = APIRequestFactory()  # pylint: disable=invalid-name


MESSAGES = {
    'NOT_FOUND': 'Not found.',
    'NO_PERMS': 'You do not have permission to perform this action.',
}


class TestDataViewSetCase(TestCase):
    def setUp(self):
        super().setUp()

        self.data_viewset = DataViewSet.as_view(actions={
            'get': 'list',
            'post': 'create',
        })
        self.data_detail_viewset = DataViewSet.as_view(actions={
            'get': 'retrieve',
        })

        self.collection = Collection.objects.create(contributor=self.contributor)

        self.proc = Process.objects.create(
            type='data:test:process',
            slug='test-process',
            version='1.0.0',
            contributor=self.contributor,
            entity_type='test-schema',
            entity_descriptor_schema='test-schema',
            input_schema=[{'name': 'input_data', 'type': 'data:test:', 'required': False}],
        )

        self.descriptor_schema = DescriptorSchema.objects.create(
            slug='test-schema',
            version='1.0.0',
            contributor=self.contributor,
        )

        assign_perm('view_collection', self.contributor, self.collection)
        assign_perm('add_collection', self.contributor, self.collection)
        assign_perm('view_process', self.contributor, self.proc)
        assign_perm('view_descriptorschema', self.contributor, self.descriptor_schema)

    def test_prefetch(self):
        request = factory.get('/', '', format='json')
        force_authenticate(request, self.contributor)

        for _ in range(10):
            Data.objects.create(contributor=self.contributor, process=self.proc)

        # Check prefetch. The number of queries without prefetch depends
        # on the number of Data objects. With prefetch 56 queries,
        # without prefetch 73 queries. Python 2 and 3 have slightly
        # different number of queries, so we set a loose constraint in test.
        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            self.data_viewset(request)
            self.assertLess(len(captured_queries), 62)

    def test_descriptor_schema(self):
        # Descriptor schema can be assigned by slug.
        data = {'process': 'test-process', 'descriptor_schema': 'test-schema'}
        request = factory.post('/', data, format='json')
        force_authenticate(request, self.contributor)
        self.data_viewset(request)

        data = Data.objects.latest()
        self.assertEqual(data.descriptor_schema, self.descriptor_schema)

        # Descriptor schema can be assigned by id.
        data = {'process': 'test-process', 'descriptor_schema': self.descriptor_schema.pk}
        request = factory.post('/', data, format='json')
        force_authenticate(request, self.contributor)
        self.data_viewset(request)

        data = Data.objects.latest()
        self.assertEqual(data.descriptor_schema, self.descriptor_schema)

    def test_use_latest_with_perm(self):
        Process.objects.create(
            type='test:process',
            name='Test process',
            slug='test-process',
            version='2.0.0',
            contributor=self.contributor,
        )
        DescriptorSchema.objects.create(
            name='Test schema',
            slug='test-schema',
            version='2.0.0',
            contributor=self.contributor,
        )

        data = {'process': 'test-process', 'descriptor_schema': 'test-schema'}
        request = factory.post('/', data, format='json')
        force_authenticate(request, self.contributor)
        self.data_viewset(request)

        data = Data.objects.latest()
        # Check that older versions are user if user doesn't have permissions on the latest
        self.assertEqual(data.process, self.proc)
        self.assertEqual(data.descriptor_schema, self.descriptor_schema)

    def test_public_create(self):
        assign_perm('view_process', AnonymousUser(), self.proc)

        data = {'process': 'test-process'}
        request = factory.post('/', data, format='json')
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Data.objects.count(), 1)

        data = Data.objects.latest()
        self.assertEqual(data.contributor.username, ANONYMOUS_USER_NAME)
        self.assertEqual(data.process.slug, 'test-process')

    def test_inherit_permissions(self):
        data_ctype = ContentType.objects.get_for_model(Data)
        entity_ctype = ContentType.objects.get_for_model(Entity)

        assign_perm('view_collection', self.user, self.collection)
        assign_perm('add_collection', self.user, self.collection)

        post_data = {'process': 'test-process', 'collections': [self.collection.pk]}
        request = factory.post('/', post_data, format='json')
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        data = Data.objects.last()
        entity = Entity.objects.last()

        self.assertTrue(self.user.has_perm('view_data', data))
        self.assertTrue(self.user.has_perm('view_entity', entity))
        self.assertTrue(self.user.has_perm('add_entity', entity))
        self.assertEqual(UserObjectPermission.objects.filter(content_type=data_ctype, user=self.user).count(), 1)
        self.assertEqual(UserObjectPermission.objects.filter(content_type=entity_ctype, user=self.user).count(), 2)

        # Add some permissions and run another process in same entity.
        assign_perm('edit_collection', self.user, self.collection)
        assign_perm('share_entity', self.user, entity)

        post_data = {
            'process': 'test-process',
            'collections': [self.collection.pk],
            'input': {'input_data': data.pk},
        }
        request = factory.post('/', post_data, format='json')
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        data_2 = Data.objects.last()
        self.assertTrue(self.user.has_perm('view_data', data_2))
        self.assertTrue(self.user.has_perm('edit_data', data_2))
        self.assertTrue(self.user.has_perm('share_data', data_2))
        self.assertFalse(self.user.has_perm('edit_entity', entity))
        self.assertEqual(UserObjectPermission.objects.filter(content_type=data_ctype, user=self.user).count(), 4)
        self.assertEqual(UserObjectPermission.objects.filter(content_type=entity_ctype, user=self.user).count(), 3)

    def test_create_entity(self):
        data = {'process': 'test-process', 'collections': [self.collection.pk]}
        request = factory.post('/', data, format='json')
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        # Test that one Entity was created and that it was added to the same collection as Data object.
        self.assertEqual(Entity.objects.count(), 1)
        self.assertEqual(Entity.objects.first().collections.count(), 1)
        self.assertEqual(Entity.objects.first().collections.first().pk, self.collection.pk)

    def test_collections_fields(self):
        # Create data object.
        data = {'process': 'test-process', 'collections': [self.collection.pk]}
        request = factory.post('/', data, format='json')
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        data = Data.objects.last()
        entity = Entity.objects.last()

        # Ensure collections/entities are not present in lists.
        request = factory.get('/', '', format='json')
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertNotIn('collections', response.data[0].keys())
        self.assertNotIn('entities', response.data[0].keys())

        # Check that query returns the correct collection ids.
        request = factory.get('/', '', format='json')
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['collections'], [self.collection.pk])
        self.assertEqual(response.data['entities'], [entity.pk])

        # Check that hydrate_{collections,entities} works. Also ensure that the serializer
        # doesn't crash if hydrate_data is also set (could cause infinite recursion).
        request = factory.get('/', {
            'hydrate_collections': '1',
            'hydrate_entities': '1',
            'hydrate_data': '1',
        }, format='json')
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['collections'][0]['id'], self.collection.pk)
        self.assertEqual(response.data['entities'][0]['id'], entity.pk)

    def test_process_is_active(self):
        # Do not allow creating data of inactive processes
        Process.objects.filter(slug='test-process').update(is_active=False)
        data = {'process': 'test-process'}
        request = factory.post('/', data, format='json')
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, 400)


class TestCollectionViewSetCase(TestCase):
    def setUp(self):
        super().setUp()

        self.checkslug_viewset = CollectionViewSet.as_view(actions={
            'get': 'slug_exists',
        })
        self.add_data_viewset = CollectionViewSet.as_view(actions={
            'post': 'add_data',
        })
        self.remove_data_viewset = CollectionViewSet.as_view(actions={
            'post': 'remove_data',
        })
        self.collection_detail_viewset = CollectionViewSet.as_view(actions={
            'get': 'retrieve',
            'put': 'update',
            'patch': 'partial_update',
            'delete': 'destroy',
        })
        self.collection_list_viewset = CollectionViewSet.as_view(actions={
            'get': 'list',
            'post': 'create',
        })

        self.detail_url = lambda pk: reverse('resolwe-api:collection-detail', kwargs={'pk': pk})

    def _create_data(self):
        process = Process.objects.create(
            name='Test process',
            contributor=self.contributor,
        )

        return Data.objects.create(
            name='Test data',
            contributor=self.contributor,
            process=process,
        )

    def _create_entity(self):
        return Entity.objects.create(
            name='Test entity',
            contributor=self.contributor,
        )

    def test_set_descriptor_schema(self):
        d_schema = DescriptorSchema.objects.create(slug="new-schema", name="New Schema", contributor=self.contributor)

        data = {
            'name': 'Test collection',
            'descriptor_schema': 'new-schema',
        }

        request = factory.post('/', data=data, format='json')
        force_authenticate(request, self.admin)
        self.collection_list_viewset(request)

        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(Collection.objects.first().descriptor_schema, d_schema)

    def test_change_descriptor_schema(self):
        collection = Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.contributor)
        d_schema = DescriptorSchema.objects.create(slug="new-schema", name="New Schema", contributor=self.contributor)

        request = factory.patch(self.detail_url(collection.pk), {'descriptor_schema': 'new-schema'}, format='json')
        force_authenticate(request, self.admin)
        self.collection_detail_viewset(request, pk=collection.pk)

        collection.refresh_from_db()
        self.assertEqual(collection.descriptor_schema, d_schema)

    def test_change_slug(self):
        collection1 = Collection.objects.create(name="Collection", contributor=self.contributor)
        collection2 = Collection.objects.create(name="Collection", contributor=self.contributor)
        self.assertEqual(collection1.slug, 'collection')
        self.assertEqual(collection2.slug, 'collection-2')

        request = factory.patch(self.detail_url(collection1.pk), {'name': 'Collection', 'slug': None}, format='json')
        force_authenticate(request, self.admin)
        response = self.collection_detail_viewset(request, pk=collection1.pk)
        self.assertEqual(response.data['slug'], 'collection')

        request = factory.patch(self.detail_url(collection2.pk), {'slug': 'collection-3'}, format='json')
        force_authenticate(request, self.admin)
        response = self.collection_detail_viewset(request, pk=collection2.pk)
        self.assertEqual(response.data['slug'], 'collection-3')

        request = factory.patch(self.detail_url(collection2.pk), {'slug': 'collection'}, format='json')
        force_authenticate(request, self.admin)
        response = self.collection_detail_viewset(request, pk=collection2.pk)
        self.assertContains(response, 'already taken', status_code=400)

    def test_check_slug(self):
        Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.admin)

        # unauthorized
        request = factory.get('/', {'name': 'collection1'}, format='json')
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.data, None)

        # existing slug
        request = factory.get('/', {'name': 'collection1'}, format='json')
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # existing slug - iexact
        request = factory.get('/', {'name': 'Collection1'}, format='json')
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # non-existing slug
        request = factory.get('/', {'name': 'new-collection'}, format='json')
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, False)

        # bad query parameter
        request = factory.get('/', {'bad': 'parameter'}, format='json')
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 400)

    def test_add_remove_data(self):
        c = Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.contributor)
        assign_perm('view_collection', self.contributor, c)
        assign_perm('edit_collection', self.contributor, c)
        assign_perm('share_collection', self.contributor, c)

        proc = Process.objects.create(type='test:process', name='Test process', contributor=self.contributor)
        d = Data.objects.create(contributor=self.contributor, slug='test1', process=proc)

        request = factory.post(self.detail_url(c.pk), {'ids': [str(d.pk)]}, format='json')
        force_authenticate(request, self.contributor)

        # user w/o permissions cannot add data
        resp = self.add_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.data['detail'], MESSAGES['NO_PERMS'])
        self.assertEqual(c.data.count(), 0)

        assign_perm('add_collection', self.contributor, c)

        # user w/ permissions can add data
        resp = self.add_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(c.data.count(), 1)

        request = factory.post(self.detail_url(c.pk), {'ids': [str(d.pk)]}, format='json')
        force_authenticate(request, self.contributor)
        remove_perm('add_collection', self.contributor, c)

        # user w/o permissions cannot remove data
        resp = self.remove_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.data['detail'], MESSAGES['NO_PERMS'])
        self.assertEqual(c.data.count(), 1)

        assign_perm('add_collection', self.contributor, c)

        # user w/ permissions can remove data
        resp = self.remove_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(c.data.count(), 0)

        request = factory.post(self.detail_url(c.pk), {'ids': ['42']}, format='json')
        force_authenticate(request, self.contributor)

        resp = self.remove_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(c.data.count(), 0)

    def test_delete(self):
        collection = Collection.objects.create(
            name="Test collection",
            contributor=self.contributor,
        )

        data_1, data_2 = self._create_data(), self._create_data()
        entity_1, entity_2 = self._create_entity(), self._create_entity()

        collection.data.add(data_1, data_2)
        collection.entity_set.add(entity_1, entity_2)

        assign_perm("view_collection", self.user, collection)
        assign_perm("edit_collection", self.user, collection)
        assign_perm("view_data", self.user, data_1)
        assign_perm("view_data", self.user, data_2)
        assign_perm("edit_data", self.user, data_1)
        assign_perm("view_entity", self.user, entity_1)
        assign_perm("view_entity", self.user, entity_2)
        assign_perm("edit_entity", self.user, entity_1)

        request = factory.delete(self.detail_url(collection.pk))
        force_authenticate(request, self.user)
        self.collection_detail_viewset(request, pk=collection.pk)

        self.assertTrue(Data.objects.filter(pk=data_1.pk).exists())
        self.assertTrue(Data.objects.filter(pk=data_2.pk).exists())
        self.assertTrue(Entity.objects.filter(pk=entity_1.pk).exists())
        self.assertTrue(Entity.objects.filter(pk=entity_2.pk).exists())

        # Recreate the initial state and test with `delete_content` flag.
        collection = Collection.objects.create(
            name="Test collection",
            contributor=self.contributor,
        )

        collection.data.add(data_1, data_2)
        collection.entity_set.add(entity_1, entity_2)

        assign_perm("view_collection", self.user, collection)
        assign_perm("edit_collection", self.user, collection)

        request = factory.delete('{}?delete_content=1'.format(self.detail_url(collection.pk)))
        force_authenticate(request, self.user)
        self.collection_detail_viewset(request, pk=collection.pk)

        # Only objects with `edit` permission can be deleted.
        self.assertFalse(Data.objects.filter(pk=data_1.pk).exists())
        self.assertTrue(Data.objects.filter(pk=data_2.pk).exists())
        self.assertFalse(Entity.objects.filter(pk=entity_1.pk).exists())
        self.assertTrue(Entity.objects.filter(pk=entity_2.pk).exists())


class ProcessTestCase(ResolweAPITestCase):
    def setUp(self):
        self.resource_name = 'process'
        self.viewset = ProcessViewSet

        super().setUp()

    def test_create_new(self):
        post_data = {
            'name': 'Test process',
            'slug': 'test-process',
            'type': 'data:test:',
        }

        # Normal user is not allowed to create new processes.
        resp = self._post(post_data, self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

        # Superuser can create process.
        resp = self._post(post_data, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

    def test_is_active(self):
        post_data = {
            'name': 'Test process',
            'slug': 'test-process',
            'type': 'data:test:',
            'is_active': False,
        }

        # is_active can not be set through API and is True by default
        response = self._post(post_data, self.admin)
        self.assertTrue(response.data['is_active'])

        # is_active should not be changed through API
        process_id = response.data['id']
        response = self._patch(process_id, {'is_active': False}, self.admin)
        self.assertEqual(response.status_code, 405)  # PATCH not allowed on process


class EntityViewSetTest(TestCase):
    def setUp(self):
        super().setUp()

        self.collection = Collection.objects.create(name="Test Collection", contributor=self.contributor)
        self.collection2 = Collection.objects.create(name="Test Collection 2", contributor=self.contributor)
        self.entity = Entity.objects.create(name="Test entity", contributor=self.contributor)
        process = Process.objects.create(name="Test process", contributor=self.contributor)
        self.data = Data.objects.create(name="Test data", contributor=self.contributor, process=process)
        self.data_2 = Data.objects.create(name="Test data 2", contributor=self.contributor, process=process)

        # another Data object to make sure that other objects are not processed
        Data.objects.create(name="Dummy data", contributor=self.contributor, process=process)

        self.entity.data.add(self.data)
        self.entity.collections.add(self.collection2)

        assign_perm('add_collection', self.contributor, self.collection)
        assign_perm('add_entity', self.contributor, self.entity)
        assign_perm('view_collection', self.contributor, self.collection)
        assign_perm('view_collection', self.contributor, self.collection2)
        assign_perm('view_entity', self.contributor, self.entity)

        self.entityviewset = EntityViewSet()

        self.entity_detail_viewset = EntityViewSet.as_view(actions={
            'get': 'retrieve',
            'put': 'update',
            'patch': 'partial_update',
            'delete': 'destroy',
        })
        self.entity_list_viewset = EntityViewSet.as_view(actions={
            'get': 'list',
            'post': 'create',
        })

        self.detail_url = lambda pk: reverse('resolwe-api:entity-detail', kwargs={'pk': pk})

    def _create_data(self):
        process = Process.objects.create(
            name='Test process',
            contributor=self.contributor,
        )

        return Data.objects.create(
            name='Test data',
            contributor=self.contributor,
            process=process,
        )

    def test_list_filter_collections(self):
        request = factory.get('/', {}, format='json')
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 1)

        request = factory.get('/', {'collections': 999999}, format='json')
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 0)

        request = factory.get('/', {'collections': self.collection.pk}, format='json')
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 0)

        request = factory.get('/', {'collections': self.collection2.pk}, format='json')
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 1)

    def test_add_to_collection(self):
        request_mock = mock.MagicMock(data={'ids': [self.collection.pk]}, user=self.contributor)
        self.entityviewset.get_object = lambda: self.entity

        self.assertEqual(self.entity.collections.count(), 1)

        self.entityviewset.add_to_collection(request_mock)

        self.assertEqual(self.collection.data.count(), 1)
        self.assertEqual(self.entity.collections.count(), 2)

    def test_remove_from_collection(self):
        # Manually add Entity and it's Data objects to the Collection
        self.entity.collections.add(self.collection.pk)
        self.collection.data.add(self.data)

        request_mock = mock.MagicMock(data={'ids': [self.collection.pk]}, user=self.contributor)
        self.entityviewset.get_object = lambda: self.entity

        self.assertEqual(self.entity.collections.count(), 2)

        self.entityviewset.remove_from_collection(request_mock)

        self.assertEqual(self.collection.data.count(), 0)
        self.assertEqual(self.entity.collections.count(), 1)

    def test_add_remove_permissions(self):
        request_mock = mock.MagicMock(data={'ids': [self.collection.pk]}, user=self.contributor)
        self.entityviewset.get_object = lambda: self.entity

        remove_perm('add_collection', self.contributor, self.collection)

        with self.assertRaises(exceptions.PermissionDenied):
            self.entityviewset.remove_from_collection(request_mock)

        with self.assertRaises(exceptions.PermissionDenied):
            self.entityviewset.add_to_collection(request_mock)

    def test_add_data(self):
        self.entity.collections.add(self.collection)

        request_mock = mock.MagicMock(data={'ids': [self.data.pk]}, user=self.contributor)
        self.entityviewset.get_object = lambda: self.entity

        self.entityviewset.add_data(request_mock)

        self.assertEqual(self.entity.data.count(), 1)
        self.assertEqual(self.collection.data.count(), 1)

    def test_delete(self):
        entity = Entity.objects.create(
            name="Test entity",
            contributor=self.contributor,
        )

        data_1, data_2 = self._create_data(), self._create_data()

        entity.data.add(data_1, data_2)

        assign_perm('view_entity', self.user, entity)
        assign_perm('edit_entity', self.user, entity)
        assign_perm('view_data', self.user, data_1)
        assign_perm('view_data', self.user, data_2)
        assign_perm('edit_data', self.user, data_1)

        request = factory.delete(self.detail_url(entity.pk))
        force_authenticate(request, self.user)
        self.entity_detail_viewset(request, pk=entity.pk)

        self.assertTrue(Data.objects.filter(pk=data_1.pk).exists())
        self.assertTrue(Data.objects.filter(pk=data_2.pk).exists())

        # Recreate the initial state and test with `delete_content` flag.
        entity = Entity.objects.create(
            name="Test entity",
            contributor=self.contributor,
        )

        entity.data.add(data_1, data_2)

        assign_perm('view_entity', self.user, entity)
        assign_perm('edit_entity', self.user, entity)

        request = factory.delete('{}?delete_content=1'.format(self.detail_url(entity.pk)))
        force_authenticate(request, self.user)
        self.entity_detail_viewset(request, pk=entity.pk)

        # Only objects with `edit` permission can be deleted.
        self.assertFalse(Data.objects.filter(pk=data_1.pk).exists())
        self.assertTrue(Data.objects.filter(pk=data_2.pk).exists())

        # Ensure that deletion works correctly when all data objects of an entity
        # are deleted.
        entity = Entity.objects.create(
            name="Test entity",
            contributor=self.contributor,
        )

        assign_perm('view_entity', self.user, entity)
        assign_perm('edit_entity', self.user, entity)
        assign_perm('edit_data', self.user, data_2)

        entity.data.add(data_2)

        request = factory.delete('{}?delete_content=1'.format(self.detail_url(entity.pk)))
        force_authenticate(request, self.user)
        response = self.entity_detail_viewset(request, pk=entity.pk)

        self.assertEqual(response.status_code, 204)
        self.assertFalse(Entity.objects.filter(pk=entity.pk).exists())
        self.assertFalse(Data.objects.filter(pk=data_2.pk).exists())
