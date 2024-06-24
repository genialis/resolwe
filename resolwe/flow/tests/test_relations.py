# pylint: disable=missing-docstring
import os

from django.apps import apps
from django.contrib.auth.models import AnonymousUser
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connections
from django.test.utils import CaptureQueriesContext
from rest_framework import status

import resolwe.permissions.models
from resolwe.flow.models import Collection, DescriptorSchema, Entity, Relation
from resolwe.flow.models.entity import RelationPartition, RelationType
from resolwe.flow.views import RelationViewSet
from resolwe.permissions.models import Permission
from resolwe.permissions.utils import assign_contributor_permissions
from resolwe.test import TransactionResolweAPITestCase


class TestRelationsAPI(TransactionResolweAPITestCase):
    def setUp(self):
        # Force reading anonymous user from the database for every test.
        resolwe.permissions.models.ANONYMOUS_USER = None
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

        self.descriptor_schema = DescriptorSchema.objects.create(
            slug="test-schema",
            version="1.0.0",
            contributor=self.contributor,
            schema=[
                {
                    "name": "general",
                    "group": [
                        {
                            "name": "relation_name",
                            "type": "basic:string:",
                            "required": True,
                        },
                    ],
                }
            ],
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
            descriptor_schema=self.descriptor_schema,
            descriptor={"general": {"relation_name": "group"}},
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
            descriptor_schema=self.descriptor_schema,
            descriptor={"general": {"relation_name": "series"}},
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
        self.collection.set_permission(Permission.VIEW, AnonymousUser())

    def test_prefetch(self):
        self.relation_group.delete()
        self.relation_series.delete()

        descriptor_schema_1 = DescriptorSchema.objects.create(
            contributor=self.contributor,
        )
        descriptor_schema_2 = DescriptorSchema.objects.create(
            contributor=self.user,
        )

        self.collection.descriptor_schema = descriptor_schema_1
        self.collection.save()

        self.collection_2.contributor = self.user
        self.collection_2.descriptor_schema = descriptor_schema_2
        self.collection_2.save()

        for i in range(5):
            Relation.objects.create(
                contributor=self.contributor,
                type=self.rel_type_group,
                category="replicates-{}".format(i),
                collection=self.collection,
            )

        for i in range(5):
            Relation.objects.create(
                contributor=self.user,
                type=self.rel_type_group,
                category="replicates-{}".format(i),
                collection=self.collection_2,
            )

        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            response = self._get_list(self.contributor)
            self.assertEqual(len(response.data), 10)
            self.assertEqual(len(captured_queries), 6)

    def test_get(self):
        resp = self._get_detail(self.relation_group.pk, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["slug"], "relation")
        self.assertEqual(resp.data["collection"]["id"], self.collection.pk)
        self.assertEqual(resp.data["type"], "group")
        self.assertEqual(resp.data["category"], "replicates")
        self.assertEqual(resp.data["unit"], None)
        self.assertEqual(
            resp.data["descriptor_schema"]["id"], self.descriptor_schema.id
        )
        self.assertEqual(resp.data["descriptor_dirty"], False)
        self.assertEqual(
            resp.data["descriptor"], {"general": {"relation_name": "group"}}
        )
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
        self.assertEqual(
            resp.data["descriptor_schema"]["id"], self.descriptor_schema.id
        )
        self.assertEqual(resp.data["descriptor_dirty"], False)
        self.assertEqual(
            resp.data["descriptor"], {"general": {"relation_name": "series"}}
        )
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

    def test_dirty_descriptor(self):
        self.assertEqual(self.relation_group.descriptor_dirty, False)
        self.relation_group.descriptor = {}
        self.relation_group.save()

        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.descriptor_dirty, True)
        self.assertEqual(self.relation_group.descriptor, {})

    def test_get_public_user(self):
        resp = self._get_detail(self.relation_series.pk, user=AnonymousUser())
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

        resp = self._get_detail(self.relation_group.pk, user=AnonymousUser())
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
            "partitions": [
                {"entity": self.entity_3.pk},
                {"entity": self.entity_4.pk},
            ],
        }

        # Anonymous user must not be able to create relations.
        resp = self._post(data, user=AnonymousUser())
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

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
            "partitions": [
                {"entity": self.entity_3.pk},
                {"entity": self.entity_4.pk},
            ],
        }

        resp = self._post(data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_409_CONFLICT)
        self.assertIn(
            "duplicate key value violates unique constraint",
            resp.data["error"],
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

        # Change the relation slug.
        new_slug = "my-unique-relation-slug"
        data = {"slug": new_slug}
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.slug, new_slug)

        # Relation type cannot be changed
        data = {"type": "series"}
        resp = self._patch(self.relation_group.pk, data, user=self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)

        data = {
            "partitions": [
                {"entity": self.entity_3.pk},
                {"entity": self.entity_4.pk},
            ],
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

    def test_update_public_user(self):
        # No view permissions.
        data = {"collection": {"id": self.collection_2.pk}}
        resp = self._patch(self.relation_series.pk, data, user=AnonymousUser())
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection)

        # View permissions.
        data = {"collection": {"id": self.collection_2.pk}}
        resp = self._patch(self.relation_group.pk, data, user=AnonymousUser())
        self.assertEqual(resp.status_code, status.HTTP_401_UNAUTHORIZED)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection)

    def test_update_different_user(self):
        # No view permission: anonymous user has view permission: remove it.
        self.collection.set_permission(Permission.NONE, AnonymousUser())
        data = {"collection": {"id": self.collection_2.pk}}
        resp = self._patch(self.relation_group.pk, data, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection)

        # No edit permission.
        self.collection.set_permission(Permission.VIEW, self.user)
        resp = self._patch(self.relation_group.pk, data, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.relation_group.refresh_from_db()
        self.assertEqual(self.relation_group.collection, self.collection)

        # All permissions.
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

    def test_delete_publicuser(self):
        """Anonymous user must not be able to delete relations."""
        # No view permissions.
        resp = self._delete(self.relation_series.pk, user=AnonymousUser())
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertTrue(Relation.objects.filter(pk=self.relation_series.pk).exists())

        # No edit permissions.
        resp = self._delete(self.relation_group.pk, user=AnonymousUser())
        self.assertEqual(resp.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertTrue(Relation.objects.filter(pk=self.relation_group.pk).exists())

    def test_delete_different_user(self):
        # No view permissions: anonymous user has view permission: remove it.
        self.collection.set_permission(Permission.NONE, AnonymousUser())
        resp = self._delete(self.relation_group.pk, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertTrue(Relation.objects.filter(pk=self.relation_group.pk).exists())

        # No edit permissions, authenticated.
        self.collection.set_permission(Permission.VIEW, self.user)
        resp = self._delete(self.relation_group.pk, user=self.user)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertTrue(Relation.objects.filter(pk=self.relation_group.pk).exists())

        # Edit & view permissions, authenticated.
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
