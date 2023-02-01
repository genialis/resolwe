# pylint: disable=missing-docstring
from unittest.mock import ANY, MagicMock

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import DEFAULT_DB_ALIAS, connections
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import (
    Collection,
    Data,
    DataDependency,
    DescriptorSchema,
    Entity,
    Process,
)
from resolwe.flow.signals import post_duplicate
from resolwe.flow.views import (
    CollectionViewSet,
    DataViewSet,
    EntityViewSet,
    ProcessViewSet,
)
from resolwe.permissions.models import Permission, get_anonymous_user
from resolwe.permissions.utils import set_permission
from resolwe.test import ResolweAPITestCase, TestCase
from resolwe.test.utils import create_data_location

factory = APIRequestFactory()


MESSAGES = {
    "NOT_FOUND": "Not found.",
    "NO_PERMS": "You do not have permission to perform this action.",
}


class TestDataViewSetCase(TestCase):
    def setUp(self):
        super().setUp()
        self.data_viewset = DataViewSet.as_view(
            actions={
                "get": "list",
                "post": "create",
            }
        )
        self.duplicate_viewset = DataViewSet.as_view(
            actions={
                "post": "duplicate",
            }
        )
        self.move_to_collection_viewset = DataViewSet.as_view(
            actions={
                "post": "move_to_collection",
            }
        )
        self.data_detail_viewset = DataViewSet.as_view(
            actions={
                "get": "retrieve",
                "patch": "partial_update",
            }
        )
        self.parents_viewset = DataViewSet.as_view(
            actions={
                "get": "parents",
            }
        )
        self.children_viewset = DataViewSet.as_view(
            actions={
                "get": "children",
            }
        )

        self.proc = Process.objects.create(
            type="data:test:process",
            slug="test-process",
            version="1.0.0",
            contributor=self.contributor,
            entity_type="test-schema",
            entity_descriptor_schema="test-schema",
            input_schema=[
                {"name": "input_data", "type": "data:test:", "required": False}
            ],
        )

        self.descriptor_schema = DescriptorSchema.objects.create(
            slug="test-schema",
            version="1.0.0",
            contributor=self.contributor,
        )

        self.collection = Collection.objects.create(
            contributor=self.contributor,
            descriptor_schema=self.descriptor_schema,
        )

        self.entity = Entity.objects.create(
            collection=self.collection,
            contributor=self.contributor,
            descriptor_schema=self.descriptor_schema,
        )
        self.collection.set_permission(Permission.EDIT, self.contributor)
        self.proc.set_permission(Permission.VIEW, self.contributor)
        self.descriptor_schema.set_permission(Permission.VIEW, self.contributor)

    def test_prefetch(self):
        process_2 = Process.objects.create(contributor=self.user)
        descriptor_schema_2 = DescriptorSchema.objects.create(
            contributor=self.user,
        )
        collection_2 = Collection.objects.create(
            contributor=self.user, descriptor_schema=descriptor_schema_2
        )
        entity_2 = Entity.objects.create(
            collection=collection_2,
            contributor=self.user,
            descriptor_schema=descriptor_schema_2,
        )

        for i in range(5):
            create_kwargs = {
                "contributor": self.contributor,
                "descriptor_schema": self.descriptor_schema,
                "process": self.proc,
            }
            if i < 4:
                create_kwargs["collection"] = self.collection
            if i < 3:
                create_kwargs["entity"] = self.entity
            Data.objects.create(**create_kwargs)

        for i in range(5):
            create_kwargs = {
                "contributor": self.user,
                "descriptor_schema": descriptor_schema_2,
                "process": process_2,
            }
            if i < 4:
                create_kwargs["collection"] = collection_2
            if i < 3:
                create_kwargs["entity"] = entity_2
            data = Data.objects.create(**create_kwargs)
            set_permission(
                Permission.VIEW,
                self.contributor,
                data.collection or data.entity or data,
            )

        request = factory.get("/", "", format="json")
        force_authenticate(request, self.contributor)

        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            response = self.data_viewset(request)
            self.assertEqual(len(response.data), 10)
            self.assertEqual(len(captured_queries), 18)

    def test_descriptor_schema(self):
        # Descriptor schema can be assigned by slug.
        data = {
            "process": {"slug": "test-process"},
            "descriptor_schema": {"slug": "test-schema"},
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        self.data_viewset(request)

        data = Data.objects.latest()
        self.assertEqual(data.descriptor_schema, self.descriptor_schema)

        # Descriptor schema can be assigned by id.
        data = {
            "process": {"slug": "test-process"},
            "descriptor_schema": {"slug": self.descriptor_schema.pk},
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        self.data_viewset(request)

        data = Data.objects.latest()
        self.assertEqual(data.descriptor_schema, self.descriptor_schema)

    def test_use_latest_with_perm(self):
        Process.objects.create(
            type="test:process",
            name="Test process",
            slug="test-process",
            version="2.0.0",
            contributor=self.contributor,
        )
        DescriptorSchema.objects.create(
            name="Test schema",
            slug="test-schema",
            version="2.0.0",
            contributor=self.contributor,
        )

        data = {
            "process": {"slug": "test-process"},
            "descriptor_schema": {"slug": "test-schema"},
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        self.data_viewset(request)

        data = Data.objects.latest()
        # Check that older versions are user if user doesn't have permissions on the latest
        self.assertEqual(data.process, self.proc)
        self.assertEqual(data.descriptor_schema, self.descriptor_schema)

    def test_public_create(self):
        self.proc.set_permission(Permission.VIEW, get_anonymous_user())

        data = {"process": {"slug": "test-process"}}
        request = factory.post("/", data, format="json")
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Data.objects.count(), 1)

        data = Data.objects.latest()
        self.assertEqual(data.contributor.username, settings.ANONYMOUS_USER_NAME)
        self.assertEqual(data.process.slug, "test-process")

    def test_resources(self):
        process_resources = {"cores": 4, "storage": 500}
        data = {
            "process": {"slug": "test-process"},
            "process_resources": process_resources,
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Data.objects.count(), 1)

        data = Data.objects.latest()
        self.assertEqual(data.process_resources, process_resources)

    def test_resources_invalid_schema(self):
        process_resources = {"cpu": 4, "storage": 500}
        data = {
            "process": {"slug": "test-process"},
            "process_resources": process_resources,
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)

        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            resp.data["error"],
            "Additional properties are not allowed ('cpu' was unexpected)",
        )
        self.assertEqual(Data.objects.count(), 0)

    def test_inherit_permissions(self):
        self.collection.set_permission(Permission.EDIT, self.user)

        post_data = {
            "process": {"slug": "test-process"},
            "collection": {"id": self.collection.pk},
        }
        request = factory.post("/", post_data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        data = Data.objects.last()
        entity = Entity.objects.last()

        self.assertTrue(self.user.has_perm(Permission.VIEW, data))
        self.assertTrue(self.user.has_perm(Permission.VIEW, entity))

        # Add some permissions and run another process in same entity.
        self.collection.set_permission(Permission.SHARE, self.user)

        post_data = {
            "process": {"slug": "test-process"},
            "collection": {"id": self.collection.pk},
            "input": {"input_data": data.pk},
        }
        request = factory.post("/", post_data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        data_2 = Data.objects.last()
        self.assertTrue(self.user.has_perm(Permission.VIEW, data_2))
        self.assertTrue(self.user.has_perm(Permission.EDIT, data_2))
        self.assertTrue(self.user.has_perm(Permission.SHARE, data_2))

    def test_handle_entity(self):
        self.entity.delete()

        data = {
            "process": {"slug": "test-process"},
            "collection": {"id": self.collection.pk},
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.data_viewset(request)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        # Test that one Entity was created and that it was added to the same collection as Data object.
        self.assertEqual(Entity.objects.count(), 1)
        self.assertEqual(Entity.objects.first().collection.pk, self.collection.pk)

    def test_collections_fields(self):
        # Create data object.
        data = {
            "process": {"slug": "test-process"},
            "collection": {"id": self.collection.pk},
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        data = Data.objects.last()
        entity = Entity.objects.last()

        # Ensure collection/entity are present
        request = factory.get("/", "", format="json")
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertIn("collection", response.data[0].keys())
        self.assertIn("entity", response.data[0].keys())

        # Check that query returns the correct collection ids.
        request = factory.get("/", "", format="json")
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["collection"]["id"], self.collection.pk)
        self.assertEqual(response.data["entity"]["id"], entity.pk)

    def test_collection_unassigned(self):
        # Data can be removed from collection through the api.

        # Create data object.
        data = {
            "process": {"slug": "test-process"},
            "collection": {"id": self.collection.pk},
        }
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        data = Data.objects.last()
        entity = Entity.objects.last()

        self.assertEqual(data.collection.id, self.collection.id)
        self.assertEqual(data.entity.id, entity.id)
        self.assertEqual(entity.collection.id, self.collection.id)

        # Assign entity to None
        data.entity = None
        data.save()
        request = factory.patch("/", {"collection": {"id": None}}, format="json")
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["collection"][0],
            "Data object can not be removed from the container.",
        )

    def test_change_collection(self):
        # Create data object. Note that an entity is created as well.
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=self.proc,
        )

        # Move data to some collection
        request = factory.patch(
            "/", {"collection": {"id": self.collection.pk}}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["collection"][0],
            "If Data is in entity, you can only move it to another collection by moving entire entity.",
        )

        entity = data.entity

        # Data can not be moved from collection.
        with self.assertRaises(ValidationError):
            data.move_to_entity(None)

        entity.move_to_collection(self.collection)
        data.move_to_entity(entity)

        request = factory.patch(
            "/", {"collection": {"id": self.collection.pk}}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data.refresh_from_db()
        self.assertEqual(data.collection, self.collection)

        # Should copy tags and permissions
        collection = Collection.objects.create(
            contributor=self.contributor, tags=["test:tag"]
        )
        set_permission(Permission.EDIT, self.contributor, collection)

        self.proc.entity_type = None
        self.proc.save()

        # Create data outside container and move it to the collection.
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=self.proc,
            collection=None,
            entity=None,
        )
        request = factory.patch(
            "/", {"collection": {"id": collection.pk}}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.data_detail_viewset(request, pk=data.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        data.refresh_from_db()
        self.assertEqual(data.tags, collection.tags)

    def test_move_to_collection(self):
        data_in_entity = Data.objects.create(
            contributor=self.contributor, process=self.proc
        )
        data_orphan = Data.objects.create(
            contributor=self.contributor, process=self.proc
        )
        data_orphan.entity.data.remove(data_orphan)

        collection_1 = Collection.objects.create(contributor=self.contributor)
        collection_1.data.add(data_orphan)
        collection_1.set_permission(Permission.EDIT, self.contributor)

        collection_2 = Collection.objects.create(contributor=self.contributor)
        collection_2.set_permission(Permission.EDIT, self.contributor)

        # Assert preventing moving data in entity.
        request = factory.post(
            reverse("resolwe-api:data-move-to-collection"),
            {
                "ids": [data_in_entity.id],
                "destination_collection": collection_2.id,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.move_to_collection_viewset(request)

        self.assertEqual(
            response.data["error"],
            "If Data is in entity, you can only move it to another collection by moving entire entity.",
        )

        # Assert moving data not in entity.
        request = factory.post(
            reverse("resolwe-api:data-move-to-collection"),
            {
                "ids": [data_orphan.id],
                "destination_collection": collection_2.id,
            },
            format="json",
        )

        self.assertEqual(collection_1.data.count(), 1)
        self.assertEqual(collection_2.data.count(), 0)

        force_authenticate(request, self.contributor)
        response = self.move_to_collection_viewset(request)

        self.assertEqual(collection_1.data.count(), 0)
        self.assertEqual(collection_2.data.count(), 1)

        # Assert preventing moving data if destination collection
        # lacks permissions.
        collection_1.set_permission(Permission.VIEW, self.contributor)
        request = factory.post(
            reverse("resolwe-api:data-move-to-collection"),
            {
                "ids": [data_orphan.id],
                "destination_collection": collection_1.id,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.move_to_collection_viewset(request)

        self.assertEqual(
            response.data["detail"],
            "You do not have permission to perform this action.",
        )

        # It shouldn't be possible to move the data if you don't
        # have edit permission on both collections.
        collection_1.set_permission(Permission.EDIT, self.contributor)
        collection_2.set_permission(Permission.VIEW, self.contributor)
        request = factory.post(
            reverse("resolwe-api:data-move-to-collection"),
            {
                "ids": [data_orphan.id],
                "destination_collection": collection_1.id,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.move_to_collection_viewset(request)

        self.assertEqual(
            response.data["detail"],
            "You do not have permission to perform this action.",
        )
        self.assertEqual(collection_1.data.count(), 0)
        self.assertEqual(collection_2.data.count(), 1)

    def test_process_is_active(self):
        # Do not allow creating data of inactive processes
        Process.objects.filter(slug="test-process").update(is_active=False)
        data = {"process": {"slug": "test-process"}}
        request = factory.post("/", data, format="json")
        force_authenticate(request, self.contributor)
        response = self.data_viewset(request)
        self.assertEqual(response.status_code, 400)

    def test_duplicate(self):
        def create_data():
            data = Data.objects.create(contributor=self.contributor, process=self.proc)
            set_permission(
                Permission.VIEW,
                self.contributor,
                data.collection or data.entity or data,
            )

            data_location = create_data_location()
            data_location.data.add(data)

            data.status = Data.STATUS_DONE
            data.save()
            return data

        # Handler is called when post_duplicate signal is triggered.
        handler = MagicMock()
        post_duplicate.connect(handler, sender=Data)

        # Simplest form:
        data = create_data()
        request = factory.post(
            reverse("resolwe-api:data-duplicate"), {"ids": [data.id]}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        duplicate = Data.objects.get(id=response.data[0]["id"])
        self.assertTrue(duplicate.is_duplicate())
        handler.assert_called_once_with(
            signal=post_duplicate,
            instances=[duplicate],
            old_instances=ANY,
            sender=Data,
        )
        handler.reset_mock()

        # Inherit collection
        data = create_data()
        self.collection.set_permission(Permission.EDIT, self.contributor)
        self.collection.data.add(data)
        request = factory.post(
            reverse("resolwe-api:data-duplicate"),
            {"ids": [data.id], "inherit_collection": True},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        duplicate = Data.objects.get(id=response.data[0]["id"])
        self.assertTrue(duplicate.is_duplicate())
        self.assertTrue(duplicate.collection.id, self.collection.id)
        handler.assert_called_once_with(
            signal=post_duplicate,
            instances=[duplicate],
            old_instances=ANY,
            sender=Data,
        )

    def test_duplicate_not_auth(self):
        request = factory.post(reverse("resolwe-api:data-duplicate"), format="json")
        response = self.duplicate_viewset(request)

        self.assertEqual(response.data["detail"], MESSAGES["NOT_FOUND"])

    def test_duplicate_wrong_parameters(self):
        request = factory.post(reverse("resolwe-api:data-duplicate"), format="json")
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        self.assertEqual(response.data["detail"], "`ids` parameter is required")

        request = factory.post(
            reverse("resolwe-api:data-duplicate"), {"ids": 1}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        self.assertEqual(response.data["detail"], "`ids` parameter not a list")

        request = factory.post(
            reverse("resolwe-api:data-duplicate"), {"ids": []}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        self.assertEqual(response.data["detail"], "`ids` parameter is empty")

        request = factory.post(
            reverse("resolwe-api:data-duplicate"), {"ids": ["a"]}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        self.assertEqual(
            response.data["detail"], "`ids` parameter contains non-integers"
        )

        request = factory.post(
            reverse("resolwe-api:data-duplicate"), {"ids": [0]}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)
        self.assertEqual(
            response.data["detail"], "Data objects with the following ids not found: 0"
        )

    def test_parents_children(self):
        parent = Data.objects.create(contributor=self.contributor, process=self.proc)
        child_1 = Data.objects.create(contributor=self.contributor, process=self.proc)
        child_2 = Data.objects.create(contributor=self.contributor, process=self.proc)

        DataDependency.objects.create(
            parent=parent, child=child_1, kind=DataDependency.KIND_IO
        )
        DataDependency.objects.create(
            parent=parent, child=child_2, kind=DataDependency.KIND_IO
        )

        set_permission(
            Permission.VIEW, self.user, parent.collection or parent.entity or parent
        )
        set_permission(
            Permission.VIEW, self.user, child_1.collection or child_1.entity or child_1
        )

        request = factory.get("/", format="json")
        force_authenticate(request, self.user)
        response = self.children_viewset(request, pk=parent.pk)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], child_1.pk)

        request = factory.get("/", format="json")
        force_authenticate(request, self.user)
        response = self.parents_viewset(request, pk=child_1.pk)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], parent.pk)


class TestCollectionViewSetCase(TestCase):
    def setUp(self):
        super().setUp()

        self.process = Process.objects.create(
            name="Test process", contributor=self.contributor
        )

        self.checkslug_viewset = CollectionViewSet.as_view(
            actions={
                "get": "slug_exists",
            }
        )
        self.add_data_viewset = CollectionViewSet.as_view(
            actions={
                "post": "add_data",
            }
        )
        self.remove_data_viewset = CollectionViewSet.as_view(
            actions={
                "post": "remove_data",
            }
        )
        self.duplicate_viewset = CollectionViewSet.as_view(
            actions={
                "post": "duplicate",
            }
        )
        self.collection_detail_viewset = CollectionViewSet.as_view(
            actions={
                "get": "retrieve",
                "put": "update",
                "patch": "partial_update",
                "delete": "destroy",
            }
        )
        self.collection_list_viewset = CollectionViewSet.as_view(
            actions={
                "get": "list",
                "post": "create",
            }
        )

        self.list_url = reverse("resolwe-api:collection-list")

        self.detail_url = lambda pk: reverse(
            "resolwe-api:collection-detail", kwargs={"pk": pk}
        )

    def _create_data(self, data_status=None):
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=self.process,
        )

        if data_status:
            data.status = data_status
            data.save()

        return data

    def _create_entity(self, data_objects=[]):
        entity = Entity.objects.create(name="Test entity", contributor=self.contributor)

        if data_objects:
            entity.data.add(*data_objects)

        return entity

    def test_prefetch(self):
        descriptor_schema_1 = DescriptorSchema.objects.create(
            contributor=self.contributor,
        )
        descriptor_schema_2 = DescriptorSchema.objects.create(
            contributor=self.user,
        )

        for _ in range(5):
            collection = Collection.objects.create(
                contributor=self.contributor, descriptor_schema=descriptor_schema_1
            )
            collection.set_permission(Permission.VIEW, self.contributor)

        for _ in range(5):
            collection = Collection.objects.create(
                contributor=self.user, descriptor_schema=descriptor_schema_2
            )
            collection.set_permission(Permission.VIEW, self.contributor)

        request = factory.get("/", "", format="json")
        force_authenticate(request, self.contributor)

        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            response = self.collection_list_viewset(request)
            self.assertEqual(len(response.data), 10)
            self.assertEqual(len(captured_queries), 8)

    def test_set_descriptor_schema(self):
        d_schema = DescriptorSchema.objects.create(
            slug="new-schema", name="New Schema", contributor=self.contributor
        )

        data = {
            "name": "Test collection",
            "descriptor_schema": {"slug": "new-schema"},
        }

        request = factory.post("/", data=data, format="json")
        force_authenticate(request, self.admin)
        self.collection_list_viewset(request)

        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(Collection.objects.first().descriptor_schema, d_schema)

    def test_change_descriptor_schema(self):
        collection = Collection.objects.create(
            slug="collection1", name="Collection 1", contributor=self.contributor
        )
        d_schema = DescriptorSchema.objects.create(
            slug="new-schema", name="New Schema", contributor=self.contributor
        )

        # For updates, id must be used.
        data = {"descriptor_schema": {"id": d_schema.pk}}
        request = factory.patch(
            self.detail_url(collection.pk), data=data, format="json"
        )
        force_authenticate(request, self.admin)
        self.collection_detail_viewset(request, pk=collection.pk)

        collection.refresh_from_db()
        self.assertEqual(collection.descriptor_schema, d_schema)

    def test_change_slug(self):
        collection1 = Collection.objects.create(
            name="Collection", contributor=self.contributor
        )
        collection2 = Collection.objects.create(
            name="Collection", contributor=self.contributor
        )
        self.assertEqual(collection1.slug, "collection")
        self.assertEqual(collection2.slug, "collection-2")

        request = factory.patch(
            self.detail_url(collection1.pk),
            {"name": "Collection", "slug": None},
            format="json",
        )
        force_authenticate(request, self.admin)
        response = self.collection_detail_viewset(request, pk=collection1.pk)
        self.assertEqual(response.data["slug"], "collection")

        request = factory.patch(
            self.detail_url(collection2.pk), {"slug": "collection-3"}, format="json"
        )
        force_authenticate(request, self.admin)
        response = self.collection_detail_viewset(request, pk=collection2.pk)
        self.assertEqual(response.data["slug"], "collection-3")

        request = factory.patch(
            self.detail_url(collection2.pk), {"slug": "collection"}, format="json"
        )
        force_authenticate(request, self.admin)
        response = self.collection_detail_viewset(request, pk=collection2.pk)
        self.assertContains(response, "already taken", status_code=400)

    def test_check_slug(self):
        Collection.objects.create(
            slug="collection1", name="Collection 1", contributor=self.admin
        )

        # unauthorized
        request = factory.get("/", {"name": "collection1"}, format="json")
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.data, None)

        # existing slug
        request = factory.get("/", {"name": "collection1"}, format="json")
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # existing slug - iexact
        request = factory.get("/", {"name": "Collection1"}, format="json")
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # non-existing slug
        request = factory.get("/", {"name": "new-collection"}, format="json")
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, False)

        # bad query parameter
        request = factory.get("/", {"bad": "parameter"}, format="json")
        force_authenticate(request, self.admin)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 400)

    def test_delete(self):
        collection = Collection.objects.create(
            name="Test collection",
            contributor=self.contributor,
        )
        data_1, data_2 = self._create_data(), self._create_data()
        entity_1, entity_2 = self._create_entity(), self._create_entity()
        data_1.move_to_collection(collection)
        data_2.move_to_collection(collection)
        entity_1.move_to_collection(collection)
        entity_2.move_to_collection(collection)

        collection.set_permission(Permission.EDIT, self.user)

        request = factory.delete(self.detail_url(collection.pk))
        force_authenticate(request, self.user)
        self.collection_detail_viewset(request, pk=collection.pk)

        # All containing objects are deleted, regardless of their permission.
        self.assertFalse(Data.objects.filter(pk=data_1.pk).exists())
        self.assertFalse(Data.objects.filter(pk=data_2.pk).exists())
        self.assertFalse(Entity.objects.filter(pk=entity_1.pk).exists())
        self.assertFalse(Entity.objects.filter(pk=entity_2.pk).exists())

    def test_duplicate(self):
        # Handler is called when post_duplicate signal is triggered.
        handler = MagicMock()
        post_duplicate.connect(handler, sender=Collection)

        request = factory.post("/", {}, format="json")
        force_authenticate(request, self.contributor)
        self.collection_list_viewset(request)

        collection = Collection.objects.first()

        request = factory.post(
            reverse("resolwe-api:collection-duplicate"),
            {"ids": [collection.id]},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)

        duplicate = Collection.objects.get(id=response.data[0]["id"])
        self.assertTrue(duplicate.is_duplicate())
        handler.assert_called_once_with(
            signal=post_duplicate,
            instances=[duplicate],
            old_instances=ANY,
            sender=Collection,
        )

    def test_duplicate_not_auth(self):
        request = factory.post(
            reverse("resolwe-api:collection-duplicate"), format="json"
        )
        response = self.duplicate_viewset(request)

        self.assertEqual(response.data["detail"], MESSAGES["NOT_FOUND"])

    def test_collection_data_count(self):
        data1 = self._create_data()
        data2 = self._create_data()

        entity = self._create_entity([data1, data2])

        collection1 = Collection.objects.create(contributor=self.contributor)
        collection1.set_permission(Permission.VIEW, get_anonymous_user())

        collection2 = Collection.objects.create(contributor=self.contributor)
        collection2.set_permission(Permission.VIEW, get_anonymous_user())
        collection2.entity_set.add(entity)
        collection2.data.add(data1, data2)

        response = self.client.get(self.detail_url(collection1.id))
        self.assertEqual(response.data["data_count"], 0)

        response = self.client.get(self.detail_url(collection2.id))
        self.assertEqual(response.data["data_count"], 2)

    def test_collection_entity_count(self):
        # Collection 1
        collection1 = Collection.objects.create(contributor=self.contributor)
        collection1.set_permission(Permission.VIEW, get_anonymous_user())
        collection1.data.add(self._create_data(), self._create_data())

        # Collection 2
        collection2 = Collection.objects.create(contributor=self.contributor)
        collection2.set_permission(Permission.VIEW, get_anonymous_user())

        col2_data1 = self._create_data()
        col2_data2 = self._create_data()
        col2_entity = self._create_entity([col2_data1, col2_data2])

        collection2.entity_set.add(col2_entity)
        collection2.data.add(col2_data1, col2_data2)

        # Asserts
        response = self.client.get(self.detail_url(collection1.id))
        self.assertEqual(response.data["entity_count"], 0)

        response = self.client.get(self.detail_url(collection2.id))
        self.assertEqual(response.data["entity_count"], 1)
        self.assertEqual(response.data["data_count"], 2)

    def test_collection_status(self):
        data_error = self._create_data(Data.STATUS_ERROR)
        data_uploading = self._create_data(Data.STATUS_UPLOADING)
        data_processing = self._create_data(Data.STATUS_PROCESSING)
        data_preparing = self._create_data(Data.STATUS_PREPARING)
        data_waiting = self._create_data(Data.STATUS_WAITING)
        data_resolving = self._create_data(Data.STATUS_RESOLVING)
        data_done = self._create_data(Data.STATUS_DONE)

        collection = Collection.objects.create(
            contributor=self.contributor, name="error"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(
            data_error,
            data_uploading,
            data_processing,
            data_preparing,
            data_waiting,
            data_resolving,
            data_done,
        )

        collection = Collection.objects.create(
            contributor=self.contributor, name="uploading"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(
            data_uploading,
            data_processing,
            data_preparing,
            data_waiting,
            data_resolving,
            data_done,
        )

        collection = Collection.objects.create(
            contributor=self.contributor, name="processing"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(
            data_processing, data_preparing, data_waiting, data_resolving, data_done
        )

        collection = Collection.objects.create(
            contributor=self.contributor, name="preparing"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(data_preparing, data_waiting, data_resolving, data_done)

        collection = Collection.objects.create(
            contributor=self.contributor, name="waiting"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(data_waiting, data_resolving, data_done)

        collection = Collection.objects.create(
            contributor=self.contributor, name="resolving"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(data_resolving, data_done)

        collection = Collection.objects.create(
            contributor=self.contributor, name="done"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add(data_done)

        collection = Collection.objects.create(
            contributor=self.contributor, name="empty"
        )
        collection.set_permission(Permission.VIEW, get_anonymous_user())
        collection.data.add()

        collections = self.client.get(self.list_url).data

        get_collection = lambda collections, name: next(
            x for x in collections if x["name"] == name
        )
        self.assertEqual(
            get_collection(collections, "error")["status"], Data.STATUS_ERROR
        )
        self.assertEqual(
            get_collection(collections, "uploading")["status"], Data.STATUS_UPLOADING
        )
        self.assertEqual(
            get_collection(collections, "processing")["status"], Data.STATUS_PROCESSING
        )
        self.assertEqual(
            get_collection(collections, "preparing")["status"], Data.STATUS_PREPARING
        )
        self.assertEqual(
            get_collection(collections, "waiting")["status"], Data.STATUS_WAITING
        )
        self.assertEqual(
            get_collection(collections, "resolving")["status"], Data.STATUS_RESOLVING
        )
        self.assertEqual(
            get_collection(collections, "done")["status"], Data.STATUS_DONE
        )
        self.assertEqual(get_collection(collections, "empty")["status"], None)


class ProcessTestCase(ResolweAPITestCase):
    def setUp(self):
        self.resource_name = "process"
        self.viewset = ProcessViewSet

        super().setUp()

    def test_create_new(self):
        post_data = {
            "name": "Test process",
            "slug": "test-process",
            "type": "data:test:",
        }

        # Normal user is not allowed to create new processes.
        resp = self._post(post_data, self.contributor)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

        # Superuser can create process.
        resp = self._post(post_data, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

    def test_is_active(self):
        post_data = {
            "name": "Test process",
            "slug": "test-process",
            "type": "data:test:",
            "is_active": False,
        }

        # is_active can not be set through API and is True by default
        response = self._post(post_data, self.admin)
        self.assertTrue(response.data["is_active"])

        # is_active should not be changed through API
        process_id = response.data["id"]
        response = self._patch(process_id, {"is_active": False}, self.admin)
        self.assertEqual(response.status_code, 405)  # PATCH not allowed on process


class EntityViewSetTest(TestCase):
    def setUp(self):
        super().setUp()

        self.descriptor_schema = DescriptorSchema.objects.create(
            contributor=self.contributor,
        )
        self.collection = Collection.objects.create(
            name="Test Collection",
            contributor=self.contributor,
            descriptor_schema=self.descriptor_schema,
        )
        self.collection2 = Collection.objects.create(
            name="Test Collection 2", contributor=self.contributor
        )
        self.entity = Entity.objects.create(
            name="Test entity",
            contributor=self.contributor,
            collection=self.collection2,
        )
        process = Process.objects.create(
            name="Test process", contributor=self.contributor
        )
        self.data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
            status=Data.STATUS_DONE,
            entity=self.entity,
            collection=self.collection2,
        )

        data_location = create_data_location()
        data_location.data.add(self.data)
        self.data_2 = Data.objects.create(
            name="Test data 2",
            contributor=self.contributor,
            process=process,
            status=Data.STATUS_DONE,
        )
        data_location = create_data_location()
        data_location.data.add(self.data_2)

        # another Data object to make sure that other objects are not processed
        data = Data.objects.create(
            name="Dummy data", contributor=self.contributor, process=process
        )
        data_location = create_data_location()
        data_location.data.add(data)

        self.collection.set_permission(Permission.EDIT, self.contributor)
        self.collection2.set_permission(Permission.VIEW, self.contributor)

        self.entityviewset = EntityViewSet()

        self.duplicate_viewset = EntityViewSet.as_view(
            actions={
                "post": "duplicate",
            }
        )
        self.move_to_collection_viewset = EntityViewSet.as_view(
            actions={
                "post": "move_to_collection",
            }
        )
        self.entity_detail_viewset = EntityViewSet.as_view(
            actions={
                "get": "retrieve",
                "put": "update",
                "patch": "partial_update",
                "delete": "destroy",
            }
        )
        self.entity_list_viewset = EntityViewSet.as_view(
            actions={
                "get": "list",
                "post": "create",
            }
        )

        self.detail_url = lambda pk: reverse(
            "resolwe-api:entity-detail", kwargs={"pk": pk}
        )

    def _create_data(self):
        process = Process.objects.create(
            name="Test process",
            contributor=self.contributor,
        )

        return Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )

    def test_prefetch(self):
        self.entity.delete()

        descriptor_schema_2 = DescriptorSchema.objects.create(contributor=self.user)
        collection_2 = Collection.objects.create(
            contributor=self.user, descriptor_schema=descriptor_schema_2
        )

        for i in range(5):
            create_kwargs = {
                "contributor": self.contributor,
                "descriptor_schema": self.descriptor_schema,
            }
            if i < 4:
                create_kwargs["collection"] = self.collection
            entity = Entity.objects.create(**create_kwargs)
            set_permission(
                Permission.VIEW, self.contributor, entity.collection or entity
            )

        for i in range(5):
            create_kwargs = {
                "contributor": self.user,
                "descriptor_schema": descriptor_schema_2,
            }
            if i < 4:
                create_kwargs["collection"] = collection_2
            entity = Entity.objects.create(**create_kwargs)
            set_permission(
                Permission.VIEW, self.contributor, entity.collection or entity
            )

        set_permission(Permission.EDIT, self.contributor, entity.collection or entity)

        request = factory.get("/", "", format="json")
        force_authenticate(request, self.contributor)

        import logging

        l = logging.getLogger("django.db.backends")
        l.setLevel(logging.DEBUG)
        l.addHandler(logging.StreamHandler())
        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            response = self.entity_list_viewset(request)
            self.assertEqual(len(response.data), 10)
            self.assertEqual(len(captured_queries), 11)

    def test_list_filter_collection(self):
        request = factory.get("/", {}, format="json")
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 1)

        request = factory.get("/", {"collection": self.collection.pk}, format="json")
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 0)

        request = factory.get("/", {"collection": self.collection2.pk}, format="json")
        force_authenticate(request, self.contributor)
        resp = self.entity_list_viewset(request)
        self.assertEqual(len(resp.data), 1)

    def test_change_collection(self):
        self.collection.tags = ["test:tag"]
        self.collection.save()
        self.data.entity.move_to_collection(self.collection2)
        self.collection2.set_permission(Permission.EDIT, self.contributor)
        request_data = {"collection": {"id": self.collection.pk}}
        request = factory.patch("/", request_data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.entity_detail_viewset(request, pk=self.entity.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.entity.refresh_from_db()
        self.data.refresh_from_db()
        self.assertEqual(self.entity.collection.id, self.collection.id)
        self.assertEqual(self.data.entity.id, self.entity.id)
        self.assertEqual(self.data.collection.id, self.collection.id)
        self.assertEqual(self.entity.tags, self.collection.tags)
        self.assertEqual(self.data.tags, self.collection.tags)

    def test_change_collection_to_none(self):
        self.entity.collection.set_permission(Permission.EDIT, self.contributor)
        self.data.collection = self.collection2
        self.data.save()

        request_data = {"collection": {"id": None}}
        request = factory.patch("/", request_data, format="json")
        force_authenticate(request, self.contributor)
        resp = self.entity_detail_viewset(request, pk=self.entity.pk)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)

        self.assertIn("can only be moved to another container.", resp.data["error"])

    def test_move_to_collection(self):
        source_collection = Collection.objects.create(contributor=self.contributor)
        source_collection.set_permission(Permission.EDIT, self.contributor)
        entity = Entity.objects.create(
            contributor=self.contributor, collection=source_collection
        )
        data = self._create_data()
        data.entity = entity
        data.collection = source_collection
        data.save()

        destination_collection = Collection.objects.create(contributor=self.contributor)
        destination_collection.set_permission(Permission.EDIT, self.contributor)

        request = factory.post(
            reverse("resolwe-api:entity-move-to-collection"),
            {
                "ids": [entity.id],
                "source_collection": source_collection.id,
                "destination_collection": destination_collection.id,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)

        self.assertEqual(source_collection.entity_set.count(), 1)
        self.assertEqual(source_collection.data.count(), 1)
        self.assertEqual(destination_collection.entity_set.count(), 0)
        self.assertEqual(destination_collection.data.count(), 0)

        self.move_to_collection_viewset(request)

        self.assertEqual(source_collection.entity_set.count(), 0)
        self.assertEqual(source_collection.data.count(), 0)
        self.assertEqual(destination_collection.entity_set.count(), 1)
        self.assertEqual(destination_collection.entity_set.first().id, entity.id)
        self.assertEqual(destination_collection.data.first().id, data.id)

    def test_delete(self):
        entity = Entity.objects.create(
            name="Test entity",
            contributor=self.contributor,
        )

        data_1, data_2 = self._create_data(), self._create_data()
        data_1.move_to_entity(entity)
        data_2.move_to_entity(entity)
        entity.set_permission(Permission.EDIT, self.user)

        request = factory.delete(self.detail_url(entity.pk))
        force_authenticate(request, self.user)
        self.entity_detail_viewset(request, pk=entity.pk)

        # If user has edit permison on entity, all containing objects
        # are deleted, regardless of their permission.
        self.assertFalse(Data.objects.filter(pk=data_1.pk).exists())
        self.assertFalse(Data.objects.filter(pk=data_2.pk).exists())

    def test_duplicate(self):
        handler = MagicMock()
        post_duplicate.connect(handler, sender=Entity)

        collection = Collection.objects.create(contributor=self.contributor)
        collection.set_permission(Permission.EDIT, self.contributor)
        entity = Entity.objects.first()
        entity.move_to_collection(collection)

        request = factory.post(
            reverse("resolwe-api:entity-duplicate"), {"ids": [entity.id]}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)

        duplicate = Entity.objects.get(id=response.data[0]["id"])
        self.assertTrue(duplicate.is_duplicate())
        self.assertEqual(collection.entity_set.count(), 1)
        self.assertEqual(collection.data.count(), 1)
        handler.assert_called_once_with(
            signal=post_duplicate,
            instances=[duplicate],
            old_instances=ANY,
            sender=Entity,
        )
        handler.reset_mock()

        request = factory.post(
            reverse("resolwe-api:entity-duplicate"),
            {"ids": [entity.id], "inherit_collection": True},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)

        self.assertEqual(collection.entity_set.count(), 2)
        self.assertEqual(collection.data.count(), 2)
        duplicate = Entity.objects.get(id=response.data[0]["id"])
        handler.assert_called_once_with(
            signal=post_duplicate,
            instances=[duplicate],
            old_instances=ANY,
            sender=Entity,
        )
        handler.reset_mock()

        # Assert collection membership.
        collection_without_perm = Collection.objects.create(
            contributor=self.contributor
        )
        entity.move_to_collection(collection_without_perm)

        request = factory.post(
            reverse("resolwe-api:entity-duplicate"),
            {"ids": [entity.id], "inherit_collection": True},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.duplicate_viewset(request)

        self.assertEqual(collection_without_perm.entity_set.count(), 1)
        self.assertEqual(collection_without_perm.data.count(), 1)

    def test_duplicate_not_auth(self):
        request = factory.post(reverse("resolwe-api:entity-duplicate"), format="json")
        response = self.duplicate_viewset(request)

        self.assertEqual(response.data["detail"], MESSAGES["NOT_FOUND"])
