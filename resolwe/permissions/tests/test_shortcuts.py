# pylint: disable=missing-docstring,invalid-name
from __future__ import absolute_import, division, print_function, unicode_literals

import unittest
import six

from django.contrib.contenttypes.models import ContentType
from django.db.models.query import QuerySet
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.test import TestCase

from guardian.compat import get_user_permission_full_codename
from guardian.exceptions import MixedContentTypeError, WrongAppError
from guardian.shortcuts import get_objects_for_group, assign_perm, remove_perm
from guardian.models import GroupObjectPermission, UserObjectPermission

from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Collection, Data, Process, Storage
from resolwe.permissions.shortcuts import get_objects_for_user, get_user_group_perms, get_object_perms
from resolwe.flow.views import StorageViewSet


factory = APIRequestFactory()  # pylint: disable=invalid-name


class UserGroupTestCase(unittest.TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create(username="test_user")
        self.group1 = Group.objects.create(name="Test group 1")
        self.group2 = Group.objects.create(name="Test group 2")

        self.collection = Collection.objects.create(
            contributor=self.user,
            name="Test collection",
        )

        # This collection is here to make sure that other permissions
        # don't affect tested queries.
        collection2 = Collection.objects.create(
            contributor=self.user,
            name="Test collection 2",
        )
        assign_perm("view_collection", self.user, collection2)
        assign_perm("view_collection", self.group1, collection2)

    def tearDown(self):
        GroupObjectPermission.objects.all().delete()
        UserObjectPermission.objects.all().delete()
        Collection.objects.all().delete()
        Group.objects.all().delete()
        # `public` user is created by guardian
        get_user_model().objects.exclude(username="public").delete()

    def test_user(self):
        assign_perm("view_collection", self.user, self.collection)
        assign_perm("edit_collection", self.user, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)

        self.assertEqual(len(group_perms), 0)
        six.assertCountEqual(self, user_perms, ["view_collection", "edit_collection"])

    def test_user_in_group(self):
        self.group1.user_set.add(self.user)
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)
        self.assertEqual(len(group_perms), 1)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(len(user_perms), 0)

        assign_perm("view_collection", self.user, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)
        self.assertEqual(len(group_perms), 1)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(len(user_perms), 1)
        six.assertCountEqual(self, user_perms, ["view_collection"])

    def test_user_in_multiple_groups(self):
        self.group1.user_set.add(self.user)
        self.group2.user_set.add(self.user)
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        assign_perm("view_collection", self.group2, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)
        self.assertEqual(len(group_perms), 2)
        self.assertEqual(group_perms[0][0], self.group1.pk)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(group_perms[1][0], self.group2.pk)
        six.assertCountEqual(self, group_perms[1][2], ["view_collection"])
        self.assertEqual(len(user_perms), 0)

    def test_group(self):
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        user_perms, group_perms = get_user_group_perms(self.group1, self.collection)
        self.assertEqual(len(group_perms), 1)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(len(user_perms), 0)


class ObjectPermsTestCase(unittest.TestCase):
    def setUp(self):
        self.user1 = get_user_model().objects.create(username="test_user1")
        self.user2 = get_user_model().objects.create(username="test_user2")
        self.group1 = Group.objects.create(name="Test group 1")
        self.group2 = Group.objects.create(name="Test group 2")
        self.anonymous = AnonymousUser()

        self.collection = Collection.objects.create(
            contributor=self.user1,
            name="Test collection",
        )

    def tearDown(self):
        GroupObjectPermission.objects.all().delete()
        UserObjectPermission.objects.all().delete()
        Collection.objects.all().delete()
        Group.objects.all().delete()
        # `public` user is created by guardian
        get_user_model().objects.exclude(username="public").delete()

    def test_all_permissions(self):
        self.group1.user_set.add(self.user1)

        perms = get_object_perms(self.collection)
        self.assertEqual(len(perms), 0)

        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)
        assign_perm("view_collection", self.user2, self.collection)
        expected_perms = [
            {'permissions': ['edit', 'view'], 'type': 'user', 'id': self.user1.pk, 'name': 'test_user1'},
            {'permissions': ['view'], 'type': 'user', 'id': self.user2.pk, 'name': 'test_user2'},
        ]
        perms = get_object_perms(self.collection)
        six.assertCountEqual(self, expected_perms, perms)

        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        assign_perm("view_collection", self.group2, self.collection)
        expected_perms.extend([
            {'permissions': ['edit', 'view'], 'type': 'group', 'id': self.group1.pk, 'name': 'Test group 1'},
            {'permissions': ['view'], 'type': 'group', 'id': self.group2.pk, 'name': 'Test group 2'},
        ])
        perms = get_object_perms(self.collection)
        six.assertCountEqual(self, expected_perms, perms)

        assign_perm("view_collection", self.anonymous, self.collection)
        expected_perms.append(
            {'permissions': ['view'], 'type': 'public'},
        )
        perms = get_object_perms(self.collection)
        six.assertCountEqual(self, expected_perms, perms)

    def test_user_permissions(self):
        self.group1.user_set.add(self.user1)
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)
        assign_perm("view_collection", self.user2, self.collection)
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        assign_perm("view_collection", self.group2, self.collection)

        expected_perms = [
            {'permissions': ['edit', 'view'], 'type': 'user', 'id': self.user1.pk, 'name': 'test_user1'},
            {'permissions': ['edit', 'view'], 'type': 'group', 'id': self.group1.pk, 'name': 'Test group 1'},
        ]
        perms = get_object_perms(self.collection, self.user1)
        six.assertCountEqual(self, expected_perms, perms)

        self.group2.user_set.add(self.user1)
        expected_perms.append(
            {'permissions': ['view'], 'type': 'group', 'id': self.group2.pk, 'name': 'Test group 2'},
        )
        perms = get_object_perms(self.collection, self.user1)
        six.assertCountEqual(self, expected_perms, perms)

        assign_perm("view_collection", self.anonymous, self.collection)
        expected_perms.append(
            {'permissions': ['view'], 'type': 'public'},
        )
        perms = get_object_perms(self.collection, self.user1)
        six.assertCountEqual(self, expected_perms, perms)


class StoragePermsTestCase(TestCase):
    def setUp(self):
        contributor = get_user_model().objects.create(username="contributor")
        proc = Process.objects.create(name='Test process', contributor=contributor)
        self.data = Data.objects.create(name='Test data', contributor=contributor, process=proc)
        dummy_data = Data.objects.create(name='Dummy data', contributor=contributor, process=proc)

        self.storage1 = Storage.objects.create(
            name='Test storage',
            json={},
            data=self.data,
            contributor=contributor,
        )

        self.storage2 = Storage.objects.create(
            name='Test storage 2',
            json={},
            data=self.data,
            contributor=contributor,
        )

        Storage.objects.create(
            name='Dummy storage',
            json={},
            data=dummy_data,
            contributor=contributor,
        )

        self.user = get_user_model().objects.create(username="test_user")
        self.group = Group.objects.create(name="test_group")

        self.storage_list_viewset = StorageViewSet.as_view(actions={
            'get': 'list',
        })

        self.storage_detail_viewset = StorageViewSet.as_view(actions={
            'get': 'retrieve',
        })

    def test_list_permissons(self):
        request = factory.get('/', content_type='application/json')
        force_authenticate(request, self.user)

        resp = self.storage_list_viewset(request)
        self.assertEqual(len(resp.data), 0)

        assign_perm("view_data", self.user, self.data)
        resp = self.storage_list_viewset(request)
        six.assertCountEqual(self, [storage['id'] for storage in resp.data],
                             [self.storage1.pk, self.storage2.pk])

        remove_perm("view_data", self.user, self.data)
        resp = self.storage_list_viewset(request)
        self.assertEqual(len(resp.data), 0)

    def test_detail_permissons(self):
        request = factory.get('/', content_type='application/json')
        force_authenticate(request, self.user)

        resp = self.storage_detail_viewset(request, pk=self.storage1.pk)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

        assign_perm("view_data", self.user, self.data)
        resp = self.storage_detail_viewset(request, pk=self.storage1.pk)
        self.assertEqual(resp.data['name'], 'Test storage')

        remove_perm("view_data", self.user, self.data)
        resp = self.storage_detail_viewset(request, pk=self.storage1.pk)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

    def test_not_allowed_methods(self):
        self.assertFalse(hasattr(StorageViewSet, 'update'))
        self.assertFalse(hasattr(StorageViewSet, 'partial_update'))
        self.assertFalse(hasattr(StorageViewSet, 'destroy'))
        self.assertFalse(hasattr(StorageViewSet, 'create'))


# tests copied from guardina.testapp.tests.test_shortcuts
class GetObjectsForUser(TestCase):

    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create(username='joe')
        self.group = Group.objects.create(name='group')
        self.ctype = ContentType.objects.create(
            model='bar', app_label='fake-for-guardian-tests')

    def test_superuser(self):
        self.user.is_superuser = True
        ctypes = ContentType.objects.all()
        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'], ctypes)
        self.assertEqual(set(ctypes), set(objects))

    def test_with_superuser_true(self):
        self.user.is_superuser = True
        ctypes = ContentType.objects.all()
        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'], ctypes, with_superuser=True)
        self.assertEqual(set(ctypes), set(objects))

    def test_with_superuser_false(self):
        self.user.is_superuser = True
        ctypes = ContentType.objects.all()
        obj1 = ContentType.objects.create(
            model='foo', app_label='guardian-tests')
        assign_perm('change_contenttype', self.user, obj1)
        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'], ctypes, with_superuser=False)
        self.assertEqual(set([obj1]), set(objects))

    def test_anonymous(self):
        self.user = AnonymousUser()
        ctypes = ContentType.objects.all()
        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'], ctypes)

        obj1 = ContentType.objects.create(
            model='foo', app_label='guardian-tests')
        assign_perm('change_contenttype', self.user, obj1)
        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'], ctypes)
        self.assertEqual(set([obj1]), set(objects))

    def test_mixed_perms(self):
        codenames = [
            get_user_permission_full_codename('change'),
            'auth.change_permission',
        ]
        self.assertRaises(MixedContentTypeError, get_objects_for_user,
                          self.user, codenames)

    def test_perms_with_mixed_apps(self):
        codenames = [
            get_user_permission_full_codename('change'),
            'contenttypes.change_contenttype',
        ]
        self.assertRaises(MixedContentTypeError, get_objects_for_user,
                          self.user, codenames)

    def test_mixed_perms_and_klass(self):
        self.assertRaises(MixedContentTypeError, get_objects_for_user,
                          self.user, ['auth.change_group'], get_user_model())

    def test_no_app_label_nor_klass(self):
        self.assertRaises(WrongAppError, get_objects_for_user, self.user,
                          ['change_group'])

    def test_empty_perms_sequence(self):
        objects = get_objects_for_user(self.user, [], Group.objects.all())
        self.assertEqual(
            set(objects),
            set()
        )

    def test_perms_single(self):
        perm = 'auth.change_group'
        assign_perm(perm, self.user, self.group)
        self.assertEqual(
            set(get_objects_for_user(self.user, perm)),
            set(get_objects_for_user(self.user, [perm])))

    def test_klass_as_model(self):
        assign_perm('contenttypes.change_contenttype', self.user, self.ctype)

        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'], ContentType)
        self.assertEqual([obj.name for obj in objects], [self.ctype.name])

    def test_klass_as_manager(self):
        assign_perm('auth.change_group', self.user, self.group)
        objects = get_objects_for_user(self.user, ['auth.change_group'],
                                       Group.objects)
        self.assertEqual([obj.name for obj in objects], [self.group.name])

    def test_klass_as_queryset(self):
        assign_perm('auth.change_group', self.user, self.group)
        objects = get_objects_for_user(self.user, ['auth.change_group'],
                                       Group.objects.all())
        self.assertEqual([obj.name for obj in objects], [self.group.name])

    def test_ensure_returns_queryset(self):
        objects = get_objects_for_user(self.user, ['auth.change_group'])
        self.assertTrue(isinstance(objects, QuerySet))

    def test_simple(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            assign_perm('change_group', self.user, group)

        objects = get_objects_for_user(self.user, ['auth.change_group'])
        self.assertEqual(len(objects), len(groups))
        self.assertTrue(isinstance(objects, QuerySet))
        self.assertEqual(
            set(objects),
            set(groups))

    def test_multi_perms(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            assign_perm('auth.change_group', self.user, group)
        assign_perm('auth.delete_group', self.user, groups[1])

        objects = get_objects_for_user(self.user, ['auth.change_group',
                                                   'auth.delete_group'])
        self.assertEqual(len(objects), 1)
        self.assertTrue(isinstance(objects, QuerySet))
        self.assertEqual(
            set(objects.values_list('name', flat=True)),
            set([groups[1].name]))

    def test_multi_perms_no_groups(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            assign_perm('auth.change_group', self.user, group)
        assign_perm('auth.delete_group', self.user, groups[1])

        objects = get_objects_for_user(self.user, ['auth.change_group',
                                                   'auth.delete_group'], use_groups=False)
        self.assertEqual(len(objects), 1)
        self.assertTrue(isinstance(objects, QuerySet))
        self.assertEqual(
            set(objects.values_list('name', flat=True)),
            set([groups[1].name]))

    def test_any_of_multi_perms(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        assign_perm('auth.change_group', self.user, groups[0])
        assign_perm('auth.delete_group', self.user, groups[2])

        objects = get_objects_for_user(self.user, ['auth.change_group',
                                                   'auth.delete_group'], any_perm=True)
        self.assertEqual(len(objects), 2)
        self.assertTrue(isinstance(objects, QuerySet))
        self.assertEqual(
            set(objects.values_list('name', flat=True)),
            set([groups[0].name, groups[2].name]))

    def test_groups_perms(self):
        group1 = Group.objects.create(name='group1')
        group2 = Group.objects.create(name='group2')
        group3 = Group.objects.create(name='group3')
        groups = [group1, group2, group3]
        for group in groups:
            self.user.groups.add(group)

        # Objects to operate on
        ctypes = list(ContentType.objects.all().order_by('id'))
        assign_perm('auth.change_group', self.user)
        assign_perm('change_contenttype', self.user, ctypes[0])
        assign_perm('change_contenttype', self.user, ctypes[1])
        assign_perm('delete_contenttype', self.user, ctypes[1])
        assign_perm('delete_contenttype', self.user, ctypes[2])

        assign_perm('change_contenttype', groups[0], ctypes[3])
        assign_perm('change_contenttype', groups[1], ctypes[3])
        assign_perm('change_contenttype', groups[2], ctypes[4])
        assign_perm('delete_contenttype', groups[0], ctypes[0])

        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'])
        self.assertEqual(
            set(objects.values_list('id', flat=True)),
            set(ctypes[i].id for i in [0, 1, 3, 4]))

        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype',
                                        'contenttypes.delete_contenttype'])
        self.assertEqual(
            set(objects.values_list('id', flat=True)),
            set(ctypes[i].id for i in [0, 1]))

        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype'])
        self.assertEqual(
            set(objects.values_list('id', flat=True)),
            set(ctypes[i].id for i in [0, 1, 3, 4]))

    def test_has_global_permission_only(self):
        group_names = ['group1', 'group2', 'group3']
        for name in group_names:
            Group.objects.create(name=name)

        # global permission to change any group
        perm = 'auth.change_group'

        assign_perm(perm, self.user)
        objects = get_objects_for_user(self.user, perm)
        remove_perm(perm, self.user)
        self.assertEqual(set(objects),
                         set(Group.objects.all()))

    def test_has_global_permission_and_object_based_permission(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        # global permission to change any group
        perm_global = 'auth.change_group'
        perm_obj = 'delete_group'
        assign_perm(perm_global, self.user)
        assign_perm(perm_obj, self.user, groups[0])
        objects = get_objects_for_user(self.user, [perm_global, perm_obj])
        remove_perm(perm_global, self.user)
        self.assertEqual(set(objects.values_list('name', flat=True)),
                         set([groups[0].name]))

    def test_has_global_permission_and_object_based_permission_any_perm(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        # global permission to change any group
        perm_global = 'auth.change_group'
        # object based permission to change only a specific group
        perm_obj = 'auth.delete_group'
        assign_perm(perm_global, self.user)
        assign_perm(perm_obj, self.user, groups[0])
        objects = get_objects_for_user(
            self.user, [perm_global, perm_obj], any_perm=True, accept_global_perms=True)
        remove_perm(perm_global, self.user)
        self.assertEqual(set(objects),
                         set(Group.objects.all()))

    def test_object_based_permission_without_global_permission(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        # global permission to delete any group
        perm_global = 'auth.delete_group'
        perm_obj = 'auth.delete_group'
        assign_perm(perm_global, self.user)
        assign_perm(perm_obj, self.user, groups[0])
        objects = get_objects_for_user(
            self.user, [perm_obj], accept_global_perms=False)
        remove_perm(perm_global, self.user)
        self.assertEqual(set(objects.values_list('name', flat=True)),
                         set([groups[0].name]))

    def test_object_based_permission_with_groups_2perms(self):
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            self.user.groups.add(group)
        # Objects to operate on
        ctypes = list(ContentType.objects.all().order_by('id'))
        assign_perm('contenttypes.change_contenttype', self.user)
        assign_perm('change_contenttype', self.user, ctypes[0])
        assign_perm('change_contenttype', self.user, ctypes[1])
        assign_perm('delete_contenttype', self.user, ctypes[1])
        assign_perm('delete_contenttype', self.user, ctypes[2])

        assign_perm('change_contenttype', groups[0], ctypes[3])
        assign_perm('change_contenttype', groups[1], ctypes[3])
        assign_perm('change_contenttype', groups[2], ctypes[4])
        assign_perm('delete_contenttype', groups[0], ctypes[0])

        objects = get_objects_for_user(self.user,
                                       ['contenttypes.change_contenttype',
                                        'contenttypes.delete_contenttype'], accept_global_perms=True)
        self.assertEqual(
            set(objects.values_list('id', flat=True)),
            set([ctypes[0].id, ctypes[1].id, ctypes[2].id]))

    def test_object_based_permission_with_groups_3perms(self):

        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            self.user.groups.add(group)
        # Objects to operate on
        ctypes = list(ContentType.objects.all().order_by('id'))
        assign_perm('contenttypes.change_contenttype', self.user)
        assign_perm('change_contenttype', self.user, ctypes[0])
        assign_perm('change_contenttype', self.user, ctypes[1])
        assign_perm('delete_contenttype', self.user, ctypes[1])
        assign_perm('delete_contenttype', self.user, ctypes[2])
        # add_contenttype does not make sense, here just for testing purposes,
        # to also cover one if branch in function.
        assign_perm('add_contenttype', self.user, ctypes[1])

        assign_perm('change_contenttype', groups[0], ctypes[3])
        assign_perm('change_contenttype', groups[1], ctypes[3])
        assign_perm('change_contenttype', groups[2], ctypes[4])
        assign_perm('delete_contenttype', groups[0], ctypes[0])
        assign_perm('add_contenttype', groups[0], ctypes[0])

        objects = get_objects_for_user(
            self.user, ['contenttypes.change_contenttype', 'contenttypes.delete_contenttype',
                        'contenttypes.add_contenttype'],
            accept_global_perms=True)
        self.assertEqual(
            set(objects.values_list('id', flat=True)),
            set([ctypes[0].id, ctypes[1].id]))

    def test_exception_different_ctypes(self):
        self.assertRaises(MixedContentTypeError, get_objects_for_user,
                          self.user, ['auth.change_permission', 'auth.change_group'])

    def test_has_any_permissions(self):
        # We use groups as objects.
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            assign_perm('change_group', self.user, group)

        objects = get_objects_for_user(self.user, [], Group)
        self.assertEqual(len(objects), len(groups))
        self.assertTrue(isinstance(objects, QuerySet))
        self.assertEqual(
            set(objects),
            set(groups))

    def test_short_codenames_with_klass(self):
        assign_perm('contenttypes.change_contenttype', self.user, self.ctype)

        objects = get_objects_for_user(self.user,
                                       ['change_contenttype'], ContentType)
        self.assertEqual([obj.name for obj in objects], [self.ctype.name])

    def test_has_any_group_permissions(self):
        # We use groups as objects.
        group_names = ['group1', 'group2', 'group3']
        groups = [Group.objects.create(name=name) for name in group_names]
        for group in groups:
            assign_perm('change_group', self.group, group)

        objects = get_objects_for_group(self.group, [], Group)
        self.assertEqual(len(objects), len(groups))
        self.assertTrue(isinstance(objects, QuerySet))
        self.assertEqual(
            set(objects),
            set(groups))
