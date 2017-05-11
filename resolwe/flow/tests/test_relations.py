# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.apps import apps
from django.core.management import call_command

from guardian.shortcuts import assign_perm
from rest_framework import status
from rest_framework.test import force_authenticate

from resolwe.flow.models import Collection, Entity, Relation
from resolwe.flow.models.entity import PositionInRelation, RelationType
from resolwe.flow.views import RelationViewSet
from resolwe.test import ResolweAPITestCase


class TestRelationsAPI(ResolweAPITestCase):

    def setUp(self):
        self.viewset = RelationViewSet
        self.resource_name = 'relation'

        super(TestRelationsAPI, self).setUp()

        # Load fixtures with relation types
        flow_config = apps.get_app_config('flow')
        call_command('loaddata', os.path.join(flow_config.path, 'tests', 'fixtures', 'relationtypes'), verbosity=0)

        self.collection = Collection.objects.create(name='Test collection', contributor=self.contributor)
        collection_2 = Collection.objects.create(name='Second collection', contributor=self.contributor)
        self.entity_1 = Entity.objects.create(name='First entity', contributor=self.contributor)
        self.entity_2 = Entity.objects.create(name='Second entity', contributor=self.contributor)
        self.entity_3 = Entity.objects.create(name='Third entity', contributor=self.contributor)
        self.entity_4 = Entity.objects.create(name='Fourth entity', contributor=self.contributor)

        rel_type_group = RelationType.objects.get(name='group')
        rel_type_series = RelationType.objects.get(name='series')

        self.relation_group = Relation.objects.create(
            contributor=self.contributor,
            collection=self.collection,
            type=rel_type_group,
            label='replicates',
            name='Group relation',
        )
        PositionInRelation.objects.create(relation=self.relation_group, entity=self.entity_1)
        PositionInRelation.objects.create(relation=self.relation_group, entity=self.entity_2)

        self.relation_series = Relation.objects.create(
            contributor=self.contributor,
            collection=collection_2,
            type=rel_type_series,
            label='time-series',
            name='Series relation',
        )
        PositionInRelation.objects.create(relation=self.relation_series, entity=self.entity_1, position='beginning')
        PositionInRelation.objects.create(relation=self.relation_series, entity=self.entity_2, position='beginning')
        PositionInRelation.objects.create(relation=self.relation_series, entity=self.entity_3, position='end')
        PositionInRelation.objects.create(relation=self.relation_series, entity=self.entity_4, position='end')

        assign_perm('view_relation', self.contributor, self.relation_group)
        assign_perm('view_relation', self.contributor, self.relation_series)

        self.add_entity_viewset = RelationViewSet.as_view({
            'post': 'add_entity',
        })

        self.remove_entity_viewset = RelationViewSet.as_view({
            'post': 'remove_entity',
        })

    def test_get(self):
        resp = self._get_detail(self.relation_group.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.assertEqual(resp.data['entities'], [
            {'entity': self.entity_1.pk, 'position': None},
            {'entity': self.entity_2.pk, 'position': None},
        ])

    def test_filtering(self):
        # Filtering by id
        query_params = {'id': self.relation_group.pk}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]['id'], self.relation_group.pk)

        # Filtering by collection
        query_params = {'collection': self.collection.pk}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]['id'], self.relation_group.pk)

        # Filtering by type
        query_params = {'type': 'group'}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]['id'], self.relation_group.pk)

        # Filtering by label
        query_params = {'label': 'replicates'}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]['id'], self.relation_group.pk)

        # Filtering by entity
        query_params = {'entity': self.entity_4.pk}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]['id'], self.relation_series.pk)

        # Filtering by entity and position
        query_params = {'entity': self.entity_4.pk, 'position': 'end'}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]['id'], self.relation_series.pk)

        # Filtering by entity and position - not matching position
        query_params = {'entity': self.entity_4.pk, 'position': 'beginning'}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 0)

        # Filtering by position - missing entity
        query_params = {'position': 'end'}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_create_without_position(self):
        data = {
            'collection': self.collection.pk,
            'type': 'group',
            'entities': [{'entity': self.entity_3.pk}, {'entity': self.entity_4.pk}],
        }

        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(Relation.objects.count(), 3)

        relation = Relation.objects.last()
        self.assertEqual(relation.collection, self.collection)
        self.assertEqual(relation.positioninrelation_set.count(), 2)
        self.assertTrue(relation.positioninrelation_set.filter(entity=self.entity_3.pk).exists())
        self.assertTrue(relation.positioninrelation_set.filter(entity=self.entity_4.pk).exists())

    def test_create_with_position(self):
        data = {
            'collection': self.collection.pk,
            'type': 'series',
            'entities': [
                {'entity': self.entity_3.pk, 'position': 1},
                {'entity': self.entity_4.pk, 'position': 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(Relation.objects.count(), 3)

        relation = Relation.objects.last()
        self.assertEqual(relation.collection, self.collection)
        self.assertEqual(relation.positioninrelation_set.count(), 2)
        self.assertTrue(relation.positioninrelation_set.filter(entity=self.entity_3.pk, position=1).exists())
        self.assertTrue(relation.positioninrelation_set.filter(entity=self.entity_4.pk, position=2).exists())

    def test_create_with_missing_type(self):
        data = {
            'collection': self.collection.pk,
            'entities': [
                {'entity': self.entity_3.pk, 'position': 1},
                {'entity': self.entity_4.pk, 'position': 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_create_missing_collection(self):
        data = {
            'type': 'series',
            'entities': [
                {'entity': self.entity_3.pk, 'position': 1},
                {'entity': self.entity_4.pk, 'position': 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update(self):
        new_collection = Collection.objects.create(name='Test collection 2', contributor=self.contributor)
        data = {'collection': new_collection.pk}
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, new_collection)

        # Relation type cannot be changed
        data = {'type': 'series'}
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)

        # Entities nust be updated through special endpoints
        data = {
            'entities': [
                {'entity': self.entity_3.pk, 'position': 1},
            ],
        }
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)

    def test_delete(self):
        resp = self._delete(self.relation_group.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)

        self.assertEqual(Relation.objects.count(), 1)

    def test_add_entity(self):
        data = [{'entity': self.entity_3.pk}]

        request = self.factory.post('', data=data, format='json')
        force_authenticate(request, self.contributor)
        self.add_entity_viewset(request, pk=self.relation_group.pk)

        self.assertEqual(self.relation_group.positioninrelation_set.count(), 3)
        self.assertEqual(self.relation_group.positioninrelation_set.last().position, None)

    def test_add_entity_with_position(self):
        data = [{'entity': self.entity_3.pk, 'position': 'latest'}]

        request = self.factory.post('', data=data, format='json')
        force_authenticate(request, self.contributor)
        self.add_entity_viewset(request, pk=self.relation_group.pk)

        self.assertEqual(self.relation_group.positioninrelation_set.count(), 3)
        self.assertEqual(self.relation_group.positioninrelation_set.last().position, 'latest')

    def test_add_entity_position_none(self):
        data = [{'entity': self.entity_3.pk, 'position': None}]

        request = self.factory.post('', data=data, format='json')
        force_authenticate(request, self.contributor)
        self.add_entity_viewset(request, pk=self.relation_group.pk)

        self.assertEqual(self.relation_group.positioninrelation_set.count(), 3)
        self.assertEqual(self.relation_group.positioninrelation_set.last().position, None)

    def test_add_entity_missing_key(self):
        data = [{'foo': self.entity_3.pk}]
        request = self.factory.post('', data=data, format='json')
        force_authenticate(request, self.contributor)
        self.add_entity_viewset(request, pk=self.relation_group.pk)

        self.assertEqual(self.relation_group.positioninrelation_set.count(), 2)

    def test_remove_entity(self):
        data = {'ids': [self.entity_2.pk]}
        request = self.factory.post('', data=data, format='json')
        force_authenticate(request, self.contributor)
        self.remove_entity_viewset(request, pk=self.relation_group.pk)

        self.assertEqual(self.relation_group.positioninrelation_set.count(), 1)
