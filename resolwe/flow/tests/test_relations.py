# pylint: disable=missing-docstring
import os

from django.apps import apps
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connections
from django.test.utils import CaptureQueriesContext

from guardian.shortcuts import assign_perm
from rest_framework import status

from resolwe.flow.models import Collection, DescriptorSchema, Entity, Relation
from resolwe.flow.models.entity import RelationPartition, RelationType
from resolwe.flow.views import RelationViewSet
from resolwe.permissions.utils import assign_contributor_permissions
from resolwe.test import TransactionResolweAPITestCase


class TestRelationsAPI(TransactionResolweAPITestCase):
    def setUp(self):
        self.viewset = RelationViewSet
        self.resource_name = "relation"

        super().setUp()

        # Load fixtures with relation types
        flow_config = apps.get_app_config("flow")
        call_command(
            "loaddata",
            os.path.join(flow_config.path, "tests", "fixtures", "relationtypes"),
            verbosity=0,
        )

        self.collection = Collection.objects.create(
            name="Test collection", contributor=self.contributor
        )
        self.collection_2 = Collection.objects.create(
            name="Second collection", contributor=self.contributor
        )
        self.entity_1 = Entity.objects.create(
            name="First entity", contributor=self.contributor
        )
        self.entity_2 = Entity.objects.create(
            name="Second entity", contributor=self.contributor
        )
        self.entity_3 = Entity.objects.create(
            name="Third entity", contributor=self.contributor
        )
        self.entity_4 = Entity.objects.create(
            name="Fourth entity", contributor=self.contributor
        )

        assign_contributor_permissions(self.collection, self.contributor)
        assign_contributor_permissions(self.collection_2, self.contributor)

        self.rel_type_group = RelationType.objects.get(name="group")
        self.rel_type_series = RelationType.objects.get(name="series")

        self.relation_group = Relation.objects.create(
            contributor=self.contributor,
            collection=self.collection,
            type=self.rel_type_group,
            category="replicates",
        )
        self.group_partiton_1 = RelationPartition.objects.create(
            relation=self.relation_group, entity=self.entity_1
        )
        self.group_partiton_2 = RelationPartition.objects.create(
            relation=self.relation_group, entity=self.entity_2
        )

        self.relation_series = Relation.objects.create(
            contributor=self.contributor,
            collection=self.collection_2,
            type=self.rel_type_series,
            category="time-series",
            unit=Relation.UNIT_HOUR,
        )
        self.series_partiton_1 = RelationPartition.objects.create(
            relation=self.relation_series,
            entity=self.entity_1,
            label="beginning",
            position=0,
        )
        self.series_partiton_2 = RelationPartition.objects.create(
            relation=self.relation_series,
            entity=self.entity_2,
            label="beginning",
            position=0,
        )
        self.series_partiton_3 = RelationPartition.objects.create(
            relation=self.relation_series, entity=self.entity_3, label="end", position=1
        )
        self.series_partiton_4 = RelationPartition.objects.create(
            relation=self.relation_series, entity=self.entity_4, label="end", position=1
        )

        assign_perm("view_relation", self.contributor, self.relation_group)
        assign_perm("view_relation", self.contributor, self.relation_series)

    def test_prefetch(self):
        self.relation_group.delete()
        self.relation_series.delete()

        descriptor_schema_1 = DescriptorSchema.objects.create(
            contributor=self.contributor,
        )
        descriptor_schema_2 = DescriptorSchema.objects.create(contributor=self.user,)

        self.collection.descriptor_schema = descriptor_schema_1
        self.collection.save()

        self.collection_2.contributor = self.user
        self.collection_2.descriptor_schema = descriptor_schema_2
        self.collection_2.save()

        for i in range(5):
            relation = Relation.objects.create(
                contributor=self.contributor,
                type=self.rel_type_group,
                category="replicates-{}".format(i),
                collection=self.collection,
            )
            assign_perm("view_relation", self.contributor, relation)

        for i in range(5):
            relation = Relation.objects.create(
                contributor=self.user,
                type=self.rel_type_group,
                category="replicates-{}".format(i),
                collection=self.collection_2,
            )
            assign_perm("view_relation", self.contributor, relation)

        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            response = self._get_list(self.contributor)
            self.assertEqual(len(response.data), 10)
            self.assertEqual(len(captured_queries), 12)

    def test_get(self):
        resp = self._get_detail(self.relation_group.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.assertEqual(resp.data["collection"]["id"], self.collection.pk)
        self.assertEqual(resp.data["type"], "group")
        self.assertEqual(resp.data["category"], "replicates")
        self.assertEqual(resp.data["unit"], None)
        self.assertCountEqual(
            resp.data["partitions"],
            [
                {
                    "id": self.group_partiton_1.pk,
                    "entity": self.entity_1.pk,
                    "position": None,
                    "label": None,
                },
                {
                    "id": self.group_partiton_2.pk,
                    "entity": self.entity_2.pk,
                    "position": None,
                    "label": None,
                },
            ],
        )

        resp = self._get_detail(self.relation_series.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.assertEqual(resp.data["collection"]["id"], self.collection_2.pk)
        self.assertEqual(resp.data["type"], "series")
        self.assertEqual(resp.data["category"], "time-series")
        self.assertEqual(resp.data["unit"], "hr")
        self.assertCountEqual(
            resp.data["partitions"],
            [
                {
                    "id": self.series_partiton_1.pk,
                    "entity": self.entity_1.pk,
                    "position": 0,
                    "label": "beginning",
                },
                {
                    "id": self.series_partiton_2.pk,
                    "entity": self.entity_2.pk,
                    "position": 0,
                    "label": "beginning",
                },
                {
                    "id": self.series_partiton_3.pk,
                    "entity": self.entity_3.pk,
                    "position": 1,
                    "label": "end",
                },
                {
                    "id": self.series_partiton_4.pk,
                    "entity": self.entity_4.pk,
                    "position": 1,
                    "label": "end",
                },
            ],
        )

    def test_filtering(self):
        # Filtering by id
        query_params = {"id": self.relation_group.pk}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]["id"], self.relation_group.pk)

        # Filtering by collection
        query_params = {"collection": self.collection.pk}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]["id"], self.relation_group.pk)

        # Filtering by type
        query_params = {"type": "group"}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]["id"], self.relation_group.pk)

        # Filtering by category
        query_params = {"category": "replicates"}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]["id"], self.relation_group.pk)

        # Filtering by entity
        query_params = {"entity": self.entity_4.pk}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]["id"], self.relation_series.pk)

        # Filtering by entity and label
        query_params = {"entity": self.entity_4.pk, "label": "end"}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 1)
        self.assertEqual(resp.data[0]["id"], self.relation_series.pk)

        # Filtering by entity and label - not matching label
        query_params = {"entity": self.entity_4.pk, "label": "beginning"}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(len(resp.data), 0)

        # Filtering by label - missing entity
        query_params = {"label": "end"}
        resp = self._get_list(user=self.contributor, query_params=query_params)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_create_only_entity(self):
        data = {
            "collection": {"id": self.collection.pk},
            "type": "group",
            "category": "clones",
            "partitions": [{"entity": self.entity_3.pk}, {"entity": self.entity_4.pk},],
        }

        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(Relation.objects.count(), 3)

        relation = Relation.objects.last()
        self.assertEqual(relation.collection, self.collection)
        self.assertEqual(relation.relationpartition_set.count(), 2)
        self.assertTrue(
            relation.relationpartition_set.filter(entity=self.entity_3.pk).exists()
        )
        self.assertTrue(
            relation.relationpartition_set.filter(entity=self.entity_4.pk).exists()
        )

    def test_create_empty_partitions(self):
        data = {
            "collection": {"id": self.collection.pk},
            "type": "group",
            "category": "clones",
            "partitions": [],
        }

        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            resp.data["partitions"], ["List of partitions must not be empty."]
        )

    def test_create_with_position(self):
        data = {
            "collection": {"id": self.collection.pk},
            "type": "series",
            "category": "time series",
            "partitions": [
                {"entity": self.entity_3.pk, "position": 1},
                {"entity": self.entity_4.pk, "position": 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(Relation.objects.count(), 3)

        relation = Relation.objects.last()
        self.assertEqual(relation.collection, self.collection)
        self.assertEqual(relation.relationpartition_set.count(), 2)
        self.assertTrue(
            relation.relationpartition_set.filter(
                entity=self.entity_3.pk, position=1
            ).exists()
        )
        self.assertTrue(
            relation.relationpartition_set.filter(
                entity=self.entity_4.pk, position=2
            ).exists()
        )

    def test_create_with_label(self):
        data = {
            "collection": {"id": self.collection.pk},
            "type": "series",
            "category": "time series",
            "partitions": [
                {"entity": self.entity_3.pk, "label": "Hr01"},
                {"entity": self.entity_4.pk, "label": "Hr02"},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(Relation.objects.count(), 3)

        relation = Relation.objects.last()
        self.assertEqual(relation.collection, self.collection)
        self.assertEqual(relation.relationpartition_set.count(), 2)
        self.assertTrue(
            relation.relationpartition_set.filter(
                entity=self.entity_3.pk, label="Hr01"
            ).exists()
        )
        self.assertTrue(
            relation.relationpartition_set.filter(
                entity=self.entity_4.pk, label="Hr02"
            ).exists()
        )

    def test_create_with_missing_type(self):
        data = {
            "collection": {"id": self.collection.pk},
            "category": "time series",
            "partitions": [
                {"entity": self.entity_3.pk, "position": 1},
                {"entity": self.entity_4.pk, "position": 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_create_missing_category(self):
        data = {
            "collection": {"id": self.collection.pk},
            "type": "series",
            "partitions": [
                {"entity": self.entity_3.pk, "position": 1},
                {"entity": self.entity_4.pk, "position": 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_create_duplicated_category(self):
        data = {
            "collection": {"id": self.collection.pk},
            "type": "group",
            "category": "RePlIcAtEs",
            "partitions": [{"entity": self.entity_3.pk}, {"entity": self.entity_4.pk},],
        }

        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            resp.data["non_field_errors"],
            ["The fields collection, category must make a unique set."],
        )

    def test_create_missing_collection(self):
        data = {
            "type": "series",
            "category": "time series",
            "partitions": [
                {"entity": self.entity_3.pk, "position": 1},
                {"entity": self.entity_4.pk, "position": 2},
            ],
        }
        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update(self):
        data = {"collection": {"id": self.collection_2.pk}}
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection_2)
        # Make sure partitions were not deleted.
        self.assertEqual(self.relation_group.relationpartition_set.count(), 2)

        # Relation type cannot be changed
        data = {"type": "series"}
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)

        data = {
            "partitions": [{"entity": self.entity_3.pk}, {"entity": self.entity_4.pk},],
        }
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(self.relation_group.entities.count(), 2)
        self.assertTrue(
            self.relation_group.relationpartition_set.filter(
                entity=self.entity_3.pk
            ).exists()
        )
        self.assertTrue(
            self.relation_group.relationpartition_set.filter(
                entity=self.entity_4.pk
            ).exists()
        )

    def test_update_superuser(self):
        data = {"collection": {"id": self.collection_2.pk}}
        resp = self._patch(self.relation_group.pk, data, user=self.admin)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection_2)

    def test_update_different_user(self):
        data = {"collection": {"id": self.collection_2.pk}}
        resp = self._patch(self.relation_group.pk, data, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection)

        assign_contributor_permissions(self.collection, self.user)
        assign_contributor_permissions(self.collection_2, self.user)

        resp = self._patch(self.relation_group.pk, data, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection_2)

    def test_delete(self):
        resp = self._delete(self.relation_group.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Relation.objects.filter(pk=self.relation_group.pk).exists())

    def test_delete_superuser(self):
        resp = self._delete(self.relation_group.pk, user=self.admin)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Relation.objects.filter(pk=self.relation_group.pk).exists())

    def test_delete_different_user(self):
        resp = self._delete(self.relation_group.pk, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertTrue(Relation.objects.filter(pk=self.relation_group.pk).exists())

        assign_contributor_permissions(self.collection, self.user)

        resp = self._delete(self.relation_group.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Relation.objects.filter(pk=self.relation_group.pk).exists())

    def test_delete_collection(self):
        self.collection.delete()
        self.assertFalse(Relation.objects.filter(pk=self.relation_group.pk).exists())

    def test_delete_entity(self):
        self.entity_1.delete()
        self.assertEqual(self.relation_group.entities.count(), 1)

    def test_delete_empty(self):
        self.entity_1.delete()
        self.assertTrue(Relation.objects.filter(pk=self.relation_group.pk).exists())
        self.entity_2.delete()
        self.assertFalse(Relation.objects.filter(pk=self.relation_group.pk).exists())
        self.assertTrue(Relation.objects.filter(pk=self.relation_series.pk).exists())
