# pylint: disable=missing-docstring
from datetime import datetime
from typing import Any, Sequence

from django.core.exceptions import ValidationError
from django.db.models import ProtectedError
from django.urls import reverse
from django.utils.timezone import now
from rest_framework import status
from rest_framework.response import Response
from rest_framework.test import APIClient, APIRequestFactory, force_authenticate

from resolwe.flow.models import AnnotationField, Collection, Entity
from resolwe.flow.models.annotations import (
    AnnotationGroup,
    AnnotationPreset,
    AnnotationType,
    AnnotationValue,
)
from resolwe.flow.serializers.annotations import (
    AnnotationFieldSerializer,
    AnnotationValueSerializer,
)
from resolwe.flow.views import (
    AnnotationFieldViewSet,
    AnnotationPresetViewSet,
    AnnotationValueViewSet,
    CollectionViewSet,
    EntityViewSet,
)
from resolwe.permissions.models import Permission, get_anonymous_user
from resolwe.test import TestCase

factory = APIRequestFactory()


class AnnotationValueTest(TestCase):
    """Test annotation value model."""

    def setUp(self):
        """Prepare the test entity and annotation values."""
        super().setUp()
        entity: Entity = Entity.objects.create(
            name="Entity", contributor=self.contributor
        )
        annotation_group: AnnotationGroup = AnnotationGroup.objects.create(
            name="group", label="Annotation group", sort_order=1
        )
        self.field = AnnotationField.objects.create(
            name="Field 1",
            label="Field 1 label",
            type=AnnotationType.STRING.value,
            sort_order=1,
            group=annotation_group,
        )
        self.value = AnnotationValue.objects.create(
            entity=entity, field=self.field, value="Test", contributor=self.contributor
        )

    def test_protected(self):
        """Assert field can not be removed while values exist."""
        with self.assertRaises(ProtectedError):
            self.field.delete()
        self.value.delete()
        self.field.delete()


class FilterAnnotations(TestCase):
    """Test filtering Entities by annotation values."""

    def setUp(self):
        """Prepare the test entity and annotation values."""
        super().setUp()

        self.viewset = EntityViewSet.as_view(actions={"get": "list"})
        self.entity1: Entity = Entity.objects.create(
            name="E1", contributor=self.contributor
        )
        entity2: Entity = Entity.objects.create(name="E2", contributor=self.contributor)
        self.entity1.set_permission(Permission.VIEW, self.contributor)
        entity2.set_permission(Permission.VIEW, self.contributor)
        annotation_group: AnnotationGroup = AnnotationGroup.objects.create(
            name="group", label="Annotation group", sort_order=1
        )

        self.fields_vocabulary = {
            AnnotationType.STRING: {
                "entIty_1": "label1 entIty_1",
                "entity_2": "label2 entity_2",
            }
        }
        self.fields = {
            annotation_type: AnnotationField.objects.create(
                name=annotation_type.value,
                label=annotation_type.value,
                type=annotation_type.value,
                sort_order=1,
                group=annotation_group,
                vocabulary=self.fields_vocabulary.get(annotation_type, None),
            )
            for annotation_type in AnnotationType
        }
        field_values: dict[AnnotationType, list[tuple[Entity, Any]]] = {
            AnnotationType.STRING: [(self.entity1, "entIty_1"), (entity2, "entity_2")],
            AnnotationType.INTEGER: [(self.entity1, 1), (entity2, 2)],
            AnnotationType.DECIMAL: [(self.entity1, 1.1), (entity2, 2.2)],
            AnnotationType.DATE: [
                (self.entity1, "1111-01-01"),
                (entity2, "2222-02-02"),
            ],
        }
        for annotation_type, values in field_values.items():
            for entity, value in values:
                field = self.fields[annotation_type]
                AnnotationValue.objects.create(
                    entity=entity,
                    field=field,
                    value=value,
                    contributor=self.contributor,
                )
        self.first_id = [self.entity1.id]
        self.second_id = [entity2.id]
        self.both_ids = [self.entity1.id, entity2.id]

    def _verify_response(self, query: str, expected_entity_ids: Sequence[int]):
        """Validate the response."""
        request = factory.get("/", {"annotations": query}, format="json")
        force_authenticate(request, self.contributor)
        response = self.viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        received_entity_ids = [entry["id"] for entry in response.data]
        self.assertCountEqual(received_entity_ids, expected_entity_ids)

    def test_label_created(self):
        """Test label in annotation value was populated during setup."""
        for value_field in self.fields[AnnotationType.STRING].values.all():
            self.assertEqual(
                value_field.label,
                self.fields_vocabulary[AnnotationType.STRING][value_field.value],
            )

    def test_label_recomputed(self):
        """Test label is recomputed when vocabulary changes."""
        new_vocabulary = {
            "entIty_1": "entity_1 new",
            "entity_2": "label entity_2 new",
        }
        self.fields[AnnotationType.STRING].vocabulary = new_vocabulary
        self.fields[AnnotationType.STRING].save()
        for value_field in self.fields[AnnotationType.STRING].values.all():
            self.assertEqual(value_field.label, new_vocabulary[value_field.value])

    def test_revalidate_on_type_change(self):
        self.fields[AnnotationType.STRING].type = AnnotationType.INTEGER.value
        with self.assertRaises(ValidationError):
            self.fields[AnnotationType.STRING].save()

    def test_permissions(self):
        """Test filtering by string annotation values."""
        field_id = self.fields[AnnotationType.STRING].id
        self._verify_response(f"{field_id}__icontains:ent", self.both_ids)
        self.entity1.set_permission(Permission.NONE, self.contributor)
        self._verify_response(f"{field_id}__icontains:ent", self.second_id)

    def test_string(self):
        """Test filtering by string annotation values."""
        field_id = self.fields[AnnotationType.STRING].id
        self._verify_response(f"{field_id}:entity_1", [])
        self._verify_response(f"{field_id}:entIty_1", self.first_id)
        self._verify_response(f"{field_id}__icontains:entity_1", self.first_id)
        self._verify_response(f"{field_id}__icontains:ent", self.both_ids)
        # Test filtering by labels.
        self._verify_response(f"{field_id}__icontains:label", self.both_ids)
        self._verify_response(f"{field_id}__icontains:label2", self.second_id)
        self._verify_response(f"{field_id}__icontains:label1", self.first_id)

    def test_integer(self):
        """Test filtering by integer annotation values."""
        field_id = self.fields[AnnotationType.INTEGER].id
        self._verify_response(f"{field_id}:1", self.first_id)
        self._verify_response(f"{field_id}__gt:1", self.second_id)
        self._verify_response(f"{field_id}__in:1,2", self.both_ids)

    def test_decimal(self):
        """Test filtering by decimal annotation values."""
        field_id = self.fields[AnnotationType.DECIMAL].id
        self._verify_response(f"{field_id}:1.1", self.first_id)
        self._verify_response(f"{field_id}__gt:1.1", self.second_id)
        self._verify_response(f"{field_id}__in:1.1,2.2", self.both_ids)

    def test_date(self):
        """Test filtering by date annotation values."""
        field_id = self.fields[AnnotationType.DATE].id
        self._verify_response(f"{field_id}:1111-01-01", self.first_id)
        self._verify_response(f"{field_id}__gt:1111-01-01", self.second_id)
        self._verify_response(f"{field_id}__lt:3333-03-03", self.both_ids)

    def test_all_fields_included(self):
        """Check if all field types are tested.

        Assumption: test for the field type TYPE is named test_TYPE.
        """
        for annotation_type in AnnotationType:
            method_name = f"test_{annotation_type.value.lower()}"
            self.assertIn(method_name, dir(self))


class TestOrderEntityByAnnotations(TestCase):
    """Test ordering Entities by annotation values."""

    def setUp(self):
        """Prepare the test entity and annotation values."""
        super().setUp()

        self.viewset = EntityViewSet.as_view(actions={"get": "list"})
        entity1: Entity = Entity.objects.create(name="E1", contributor=self.contributor)
        entity2: Entity = Entity.objects.create(name="E2", contributor=self.contributor)
        entity1.set_permission(Permission.VIEW, self.contributor)
        entity2.set_permission(Permission.VIEW, self.contributor)
        annotation_group: AnnotationGroup = AnnotationGroup.objects.create(
            name="group", label="Annotation group", sort_order=1
        )
        self.fields = {
            annotation_type: AnnotationField.objects.create(
                name=annotation_type.value,
                label=annotation_type.value,
                type=annotation_type.value,
                sort_order=1,
                group=annotation_group,
            )
            for annotation_type in AnnotationType
        }
        field_values: dict[AnnotationType, list[tuple[Entity, Any]]] = {
            AnnotationType.STRING: [(entity1, "abc"), (entity2, "bc")],
            AnnotationType.INTEGER: [(entity1, 2), (entity2, 10)],
            AnnotationType.DECIMAL: [(entity1, 2.2), (entity2, 10.1)],
            AnnotationType.DATE: [(entity1, "1111-01-01"), (entity2, "2222-02-02")],
        }
        for annotation_type, values in field_values.items():
            for entity, value in values:
                field = self.fields[annotation_type]
                AnnotationValue.objects.create(
                    entity=entity,
                    field=field,
                    value=value,
                    contributor=self.contributor,
                )
        self.first_id = [entity1.id]
        self.second_id = [entity2.id]
        self.both_ids = [entity1.id, entity2.id]

    def _verify_response(self, query: str, expected_entity_ids: Sequence[int]):
        """Validate the response."""
        request = factory.get("/", {"ordering": query}, format="json")
        force_authenticate(request, self.contributor)
        response = self.viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        received_entity_ids = [entry["id"] for entry in response.data]
        self.assertEqual(received_entity_ids, expected_entity_ids)

    def test_string(self):
        """Test ordering by string annotation values."""
        field_id = self.fields[AnnotationType.STRING].id
        self._verify_response(f"annotations__{field_id}", self.both_ids)
        self._verify_response(
            f"-annotations__{field_id}", list(reversed(self.both_ids))
        )
        # Test filtering respects labels.
        field = self.fields[AnnotationType.STRING]
        field.vocabulary = {"abc": "bc", "bc": "abc"}
        field.save()
        self._verify_response(f"-annotations__{field_id}", self.both_ids)
        self._verify_response(f"annotations__{field_id}", list(reversed(self.both_ids)))

    def test_integer(self):
        """Test filtering by integer annotation values."""
        field_id = self.fields[AnnotationType.INTEGER].id
        self._verify_response(f"annotations__{field_id}", self.both_ids)
        self._verify_response(
            f"-annotations__{field_id}", list(reversed(self.both_ids))
        )

    def test_decimal(self):
        """Test filtering by decimal annotation values."""
        field_id = self.fields[AnnotationType.DECIMAL].id
        self._verify_response(f"annotations__{field_id}", self.both_ids)
        self._verify_response(
            f"-annotations__{field_id}", list(reversed(self.both_ids))
        )

    def test_date(self):
        """Test filtering by date annotation values."""
        field_id = self.fields[AnnotationType.DATE].id
        self._verify_response(f"annotations__{field_id}", self.both_ids)
        self._verify_response(
            f"-annotations__{field_id}", list(reversed(self.both_ids))
        )

    def test_multi(self):
        """Test filtering by multiple annotation field values."""
        field1 = self.fields[AnnotationType.STRING]
        field2 = self.fields[AnnotationType.INTEGER]
        self._verify_response(
            f"annotations__{field1.id},annotations__{field2.id}", self.both_ids
        )
        self._verify_response(
            f"annotations__{field1.id},-annotations__{field2.id}", self.both_ids
        )
        self._verify_response(
            f"-annotations__{field1.id},-annotations__{field2.id}",
            list(reversed(self.both_ids)),
        )
        # Test ordering when first values are same.
        value = AnnotationValue.objects.get(
            field=field1, _value={"value": "abc", "label": "abc"}
        )
        value.value = "bc"
        value.save()
        self._verify_response(
            f"annotations__{field1.id},annotations__{field2.id}", self.both_ids
        )
        self._verify_response(
            f"annotations__{field1.id},-annotations__{field2.id}",
            list(reversed(self.both_ids)),
        )

    def test_all_fields_included(self):
        """Check if all field types are tested.

        Assumption: test for the field type TYPE is named test_TYPE.
        """
        for annotation_type in AnnotationType:
            method_name = f"test_{annotation_type.value.lower()}"
            self.assertIn(method_name, dir(self))


class AnnotationViewSetsTest(TestCase):

    NOT_FOUND = "No AnnotationValue matches the given query."

    def setUp(self):
        super().setUp()

        self.anonymous = get_anonymous_user()
        self.collection1: Collection = Collection.objects.create(
            name="Test Collection 1",
            contributor=self.contributor,
        )
        self.collection2: Collection = Collection.objects.create(
            name="Test Collection 2", contributor=self.contributor
        )
        self.entity1: Entity = Entity.objects.create(
            name="Test entity 1",
            contributor=self.contributor,
            collection=self.collection1,
        )
        self.entity2: Entity = Entity.objects.create(
            name="Test entity 2",
            contributor=self.contributor,
            collection=self.collection2,
        )

        self.collection1.set_permission(Permission.EDIT, self.contributor)
        self.collection2.set_permission(Permission.VIEW, self.contributor)

        self.annotation_group1: AnnotationGroup = AnnotationGroup.objects.create(
            name="group1", label="Annotation group 1", sort_order=2
        )
        self.annotation_group2: AnnotationGroup = AnnotationGroup.objects.create(
            name="group2", label="Annotation group 2", sort_order=1
        )

        AnnotationField.objects.create(
            name="field1",
            label="Annotation field 1",
            sort_order=2,
            group=self.annotation_group1,
            type="STRING",
            vocabulary={"string": "label string", "another": "Another one"},
        )

        self.annotation_field1: AnnotationField = AnnotationField.objects.create(
            name="field1",
            label="Annotation field 1",
            sort_order=2,
            group=self.annotation_group1,
            type="STRING",
            vocabulary={"string": "label string", "another": "Another one"},
            version="1.0.0",
        )

        self.annotation_field2: AnnotationField = AnnotationField.objects.create(
            name="field2",
            label="Annotation field 2",
            sort_order=1,
            group=self.annotation_group2,
            type="INTEGER",
        )
        self.annotation_field1.collection.add(self.collection1)
        self.annotation_field2.collection.add(self.collection2)

        self.preset1: AnnotationPreset = AnnotationPreset.objects.create(
            name="Preset 1", contributor=self.contributor
        )
        self.preset2: AnnotationPreset = AnnotationPreset.objects.create(
            name="Preset 2", contributor=self.contributor
        )

        self.preset1.fields.add(self.annotation_field1, self.annotation_field2)
        self.preset2.fields.add(self.annotation_field2)

        self.preset_viewset = AnnotationPresetViewSet.as_view(actions={"get": "list"})
        self.annotationfield_viewset = AnnotationFieldViewSet.as_view(
            actions={"get": "list"}
        )

        self.annotationvalue_viewset = AnnotationValueViewSet.as_view(
            actions={
                "get": "list",
                "post": "create",
                "patch": "partial_update",
                "delete": "destroy",
            }
        )
        self.annotation_value1: AnnotationValue = AnnotationValue.objects.create(
            entity=self.entity1,
            field=self.annotation_field1,
            value="string",
            contributor=self.contributor,
        )
        self.annotation_value2: AnnotationValue = AnnotationValue.objects.create(
            entity=self.entity2,
            field=self.annotation_field2,
            value=2,
            contributor=self.contributor,
        )

    def test_create_annotation_value(self):
        """Test creating new annotation value objects."""
        field = AnnotationField.objects.create(
            name="field3",
            label="Annotation field 3",
            sort_order=1,
            group=self.annotation_group1,
            type=AnnotationType.INTEGER.value,
        )

        self.client = APIClient()
        path = reverse("resolwe-api:annotationvalue-list")
        values = {
            "entity": self.entity1.pk,
            "field": field.pk,
            "value": -1,
        }
        values_count = AnnotationValue.objects.count()

        # Unauthenticated request.
        response = self.client.post(path, values, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertAlmostEqual(values_count, AnnotationValue.objects.count())

        # Authenticated request.
        self.client.force_authenticate(self.contributor)
        response = self.client.post(path, values, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        created_value = AnnotationValue.objects.get(pk=response.data["id"])
        self.assertEqual(created_value.entity, self.entity1)
        self.assertEqual(created_value.field, field)
        self.assertEqual(created_value.value, -1)
        self.assertAlmostEqual(values_count + 1, AnnotationValue.objects.count())

        # Bulk create, no permission on entity 2.
        AnnotationValue.all_objects.all().delete()
        values = [
            {
                "entity": self.entity1.pk,
                "field": field.pk,
                "value": -1,
                "contributor": self.contributor.pk,
            },
            {
                "entity": self.entity2.pk,
                "field": field.pk,
                "value": -2,
                "contributor": self.contributor.pk,
            },
        ]
        response = self.client.post(path, values, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertAlmostEqual(0, AnnotationValue.objects.count())

        # Bulk create, edit permission on both entities.
        self.collection2.set_permission(Permission.EDIT, self.contributor)
        self.client.force_authenticate(self.contributor)
        response = self.client.post(path, values, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        expected = AnnotationValueSerializer(
            AnnotationValue.objects.all(), many=True
        ).data
        self.assertCountEqual(response.data, expected)

        # Bulk create, request for same entity and field.
        values = [
            {
                "entity": self.entity1.pk,
                "field": field.pk,
                "value": -10,
                "contributor": self.contributor.pk,
            },
            {
                "entity": self.entity1.pk,
                "field": field.pk,
                "value": -20,
                "contributor": self.contributor.pk,
            },
        ]
        response = self.client.post(path, values, format="json")
        self.assertContains(
            response,
            "Duplicate annotation values for the same entity and field.",
            status_code=status.HTTP_400_BAD_REQUEST,
        )
        self.assertEqual(
            AnnotationValue.objects.get(entity=self.entity1, field=field).value, -1
        )
        self.assertEqual(
            AnnotationValue.objects.get(entity=self.entity2, field=field).value, -2
        )
        self.assertEqual(AnnotationValue.objects.count(), 2)

        # Authenticated request, no permission.
        values = [
            {
                "entity": self.entity1.pk,
                "field": field.pk,
                "value": -10,
                "contributor": self.contributor.pk,
            },
            {
                "entity": self.entity2.pk,
                "field": field.pk,
                "value": -20,
                "contributor": self.contributor.pk,
            },
        ]
        AnnotationValue.objects.all().delete()
        self.entity1.collection.set_permission(Permission.NONE, self.contributor)
        response = self.client.post(path, values, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertAlmostEqual(0, AnnotationValue.objects.count())

    def test_update_annotation_value(self):
        """Test updating new annotation value objects."""
        client = APIClient()
        path = reverse(
            "resolwe-api:annotationvalue-detail", args=[self.annotation_value1.pk]
        )
        values = {"id": self.annotation_value1.pk, "value": "another"}

        # Unauthenticated request.
        response = client.patch(path, values, format="json")
        self.assertContains(
            response,
            "Partial updates are not supported.",
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )

        # Authenticated request.
        client.force_authenticate(self.contributor)
        response = client.patch(path, values, format="json")
        self.assertContains(
            response,
            "Partial updates are not supported.",
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        )

        # Single / bulk update with put.
        self.entity1.collection.set_permission(Permission.EDIT, self.contributor)
        path = reverse("resolwe-api:annotationvalue-list")
        client = APIClient()
        values = [
            {
                "entity": self.entity1.pk,
                "field": self.annotation_field1.pk,
                "value": -1,
            }
        ]

        # Unauthenticated request.
        response = client.put(path, values, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            response.data,
            {"detail": "You do not have permission to perform this action."},
        )

        # Authenticated request with validation error.
        client.force_authenticate(self.contributor)
        response = client.put(path, values, format="json")
        self.annotation_value1.refresh_from_db()

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["error"],
            [
                "The value '-1' is not of the expected type 'str'.",
                f"The value '-1' is not valid for the field {self.annotation_field1}.",
            ],
        )
        self.assertEqual(self.annotation_value1.value, "string")
        self.assertEqual(AnnotationValue.objects.count(), 2)

        # Authenticated request without validation error.
        values[0]["value"] = "another"
        response = client.put(path, values, format="json")
        new_value = AnnotationValue.objects.get(
            entity=self.annotation_value1.entity, field=self.annotation_value1.field
        )
        self.annotation_value1.refresh_from_db()
        expected = AnnotationValueSerializer([new_value], many=True).data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, expected)
        self.assertEqual(AnnotationValue.objects.count(), 2)
        self.assertEqual(self.annotation_value1.value, "string")

        # Multi with validation error.
        values = [
            {
                "field": self.annotation_field2.pk,
                "value": 1,
                "entity": self.entity1.pk,
            },
            {
                "field": self.annotation_field2.pk,
                "value": "string",
                "entity": self.entity1.pk,
            },
        ]
        response = client.put(path, values, format="json")
        self.assertContains(
            response,
            "Duplicate annotation values for the same entity and field.",
            status_code=status.HTTP_400_BAD_REQUEST,
        )

        # Multi.
        values = [
            {
                "field": self.annotation_field2.pk,
                "value": 1,
                "entity": self.entity1.pk,
            },
            {
                "field": self.annotation_field1.pk,
                "value": "string",
                "entity": self.entity1.pk,
            },
        ]

        response = client.put(path, values, format="json")
        self.annotation_value1.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        created_value = AnnotationValue.objects.get(
            entity=self.entity1, field=self.annotation_field2
        )
        updated_value = AnnotationValue.objects.get(
            entity=self.annotation_value1.entity, field=self.annotation_value1.field
        )
        expected = AnnotationValueSerializer(
            [updated_value, created_value], many=True
        ).data
        self.assertCountEqual(response.data, expected)
        self.assertEqual(updated_value.value, "string")
        self.assertEqual(updated_value.label, "label string")
        self.assertEqual(AnnotationValue.objects.count(), 3)

        # Multi + delete.
        values = [
            {
                "field": self.annotation_field2.pk,
                "value": 2,
                "entity": self.entity1.pk,
            },
            {
                "field": self.annotation_field1.pk,
                "value": None,
                "entity": self.entity1.pk,
            },
        ]
        response = client.put(path, values, format="json")
        updated_value = AnnotationValue.objects.get(
            entity=created_value.entity, field=created_value.field
        )
        expected = AnnotationValueSerializer([updated_value], many=True).data
        print("Got response", response.data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertCountEqual(response.data, expected)
        with self.assertRaises(AnnotationValue.DoesNotExist):
            AnnotationValue.objects.get(
                entity=self.annotation_value1.entity, field=self.annotation_value1.field
            )
        created_value = AnnotationValue.objects.get(
            entity=created_value.entity, field=created_value.field
        )
        self.assertEqual(created_value.value, 2)
        self.assertEqual(created_value.label, 2)

    def test_delete_annotation_value(self):
        """Test deleting annotation value objects."""
        client = APIClient()
        path = reverse(
            "resolwe-api:annotationvalue-detail", args=[self.annotation_value1.pk]
        )
        # Unauthenticated request, no view permission.
        response = client.delete(path, format="json")
        self.assertContains(
            response, self.NOT_FOUND, status_code=status.HTTP_404_NOT_FOUND
        )

        # Unauthenticated request, view permission.
        self.annotation_value1.entity.collection.set_permission(
            Permission.VIEW, get_anonymous_user()
        )
        response = client.delete(path, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            response.data,
            {"detail": "Authentication credentials were not provided."},
        )

        # Unauthenticated request, edit permission.
        self.annotation_value1.entity.collection.set_permission(
            Permission.EDIT, get_anonymous_user()
        )
        response = client.delete(path, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        with self.assertRaises(AnnotationValue.DoesNotExist):
            self.annotation_value1.refresh_from_db()
        self.annotation_value1: AnnotationValue = AnnotationValue.objects.create(
            entity=self.entity1,
            field=self.annotation_field1,
            value="string",
            contributor=self.contributor,
        )
        path = reverse(
            "resolwe-api:annotationvalue-detail", args=[self.annotation_value1.pk]
        )

        # Authenticated request.
        client.force_authenticate(self.contributor)
        response = client.delete(path, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        with self.assertRaises(AnnotationValue.DoesNotExist):
            self.annotation_value1.refresh_from_db()

        # Authenticated request, view permission.
        path = reverse(
            "resolwe-api:annotationvalue-detail", args=[self.annotation_value2.pk]
        )
        self.annotation_value2.entity.collection.set_permission(
            Permission.VIEW, self.contributor
        )
        response = client.delete(path, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            response.data,
            {"detail": "You do not have permission to perform this action."},
        )

        # Authenticated request, no permission.
        self.annotation_value2.entity.collection.set_permission(
            Permission.NONE, self.contributor
        )
        response = client.delete(path, format="json")
        self.assertContains(
            response, self.NOT_FOUND, status_code=status.HTTP_404_NOT_FOUND
        )

    def test_annotate_path(self):
        """Test annotate entity queryset."""
        entities = Entity.objects.all().annotate_path("group1.field1")
        first = entities.get(pk=self.entity1.pk)
        second = entities.get(pk=self.entity2.pk)
        self.assertEqual(first.group1_field1, "string")
        self.assertIsNone(second.group1_field1)

    def test_filter_value_by_group_name(self):
        # Unauthenticated request, no permissions.
        request = factory.get(
            "/", {"field__group__name": self.annotation_group1.name}, format="json"
        )
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        self.annotation_value1.entity.collection.set_permission(
            Permission.VIEW, get_anonymous_user()
        )
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        # Authenticated request without entity filter.
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        # Requests with entity filter.
        self.annotation_value1.entity.collection.set_permission(
            Permission.NONE, get_anonymous_user()
        )
        request = factory.get(
            "/",
            {
                "entity": self.entity1.pk,
                "field__group__name": self.annotation_group1.name,
            },
            format="json",
        )
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

        # Proper authenticated request.
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], self.annotation_value1.id)

    def test_filter_field_by_entity(self):
        """Filter fields by entity"""
        # Unauthenticated request, no permissions to entity.
        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        response: Response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        # Authenticated request, permissions to entity.
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], self.annotation_field1.name)

    def test_created_field(self):
        """Test created field is present in the response."""
        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 1)
        self.assertEqual(
            datetime.strptime(response.data[0]["created"], "%Y-%m-%dT%H:%M:%S.%f%z"),
            self.annotation_value1.created,
        )

    def test_filter_created_field(self):
        """Test filtering by created time."""
        self.annotation_value1.created = now()
        self.annotation_value1.save()
        request = factory.get(
            "/", {"created__gte": self.annotation_value1.created}, format="json"
        )
        force_authenticate(request, self.contributor)
        result1 = self.annotationvalue_viewset(request).data
        request = factory.get(
            "/", {"created__lt": self.annotation_value1.created}, format="json"
        )
        force_authenticate(request, self.contributor)
        result2 = self.annotationvalue_viewset(request).data
        self.assertEqual(len(result1), 1)
        self.assertEqual(len(result2), 1)
        self.assertEqual(result1[0]["id"], self.annotation_value1.pk)
        self.assertEqual(result2[0]["id"], self.annotation_value2.pk)

    def test_sort_values_created(self):
        """Test annotation values can be ordered by created field."""
        request = factory.get("/", {"ordering": "created"}, format="json")
        force_authenticate(request, self.contributor)
        order1 = self.annotationvalue_viewset(request).data
        request = factory.get("/", {"ordering": "-created"}, format="json")
        force_authenticate(request, self.contributor)
        order2 = self.annotationvalue_viewset(request).data
        self.assertEqual(len(order1), 2)
        self.assertListEqual(order1, list(reversed(order2)))

    def test_list_filter_preset(self):
        request = factory.get("/", {}, format="json")
        response = self.preset_viewset(request)

        # Unauthenticated request, no permissions.
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        # Set permissions to the presets.
        self.preset1.set_permission(Permission.VIEW, self.anonymous)
        self.preset1.set_permission(Permission.OWNER, self.contributor)
        self.preset2.set_permission(Permission.OWNER, self.contributor)

        response = self.preset_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "Preset 1")

        # Permission for anonymous user to see both presets. Presets should be ordered
        # by their ids.
        force_authenticate(request, self.contributor)
        response = self.preset_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["name"], "Preset 1")
        self.assertEqual(response.data[1]["name"], "Preset 2")

        # Filter by name
        request = factory.get("/", {"name": "Preset 1"}, format="json")
        force_authenticate(request, self.contributor)
        response = self.preset_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "Preset 1")

        request = factory.get("/", {"name__icontains": "ReSEt 2"}, format="json")
        force_authenticate(request, self.contributor)
        response = self.preset_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "Preset 2")

    def test_preset_fields(self):
        self.preset1.set_permission(Permission.OWNER, self.contributor)
        self.preset2.set_permission(Permission.OWNER, self.contributor)

        request = factory.get("/", {}, format="json")
        force_authenticate(request, self.contributor)
        response = self.preset_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["name"], "Preset 1")
        self.assertEqual(response.data[1]["name"], "Preset 2")
        self.assertCountEqual(
            [self.annotation_field1.id, self.annotation_field2.id],
            response.data[0]["fields"],
        )
        self.assertCountEqual([self.annotation_field2.id], response.data[1]["fields"])

    def test_annotation_field(self):
        """Test the annotation field endpoint."""

        # No authentication is necessary to access the annotation field endpoint.
        request = factory.get("/", {}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 2)
        expected = AnnotationFieldSerializer(
            [self.annotation_field2, self.annotation_field1], many=True
        ).data
        self.assertEqual(response.data, expected)

        # Filter by id.
        request = factory.get("/", {"id": self.annotation_field1.pk}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], self.annotation_field1.name)

        # Filter by required.
        request = factory.get("/", {"required": False}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        self.annotation_field1.required = True
        self.annotation_field1.save()

        request = factory.get("/", {"required": True}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        # Filter by collection.
        request = factory.get("/", {"collection": self.collection1.id}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get(
            "/",
            {"collection__name": f"{self.collection1.name}"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get(
            "/",
            {
                "collection__name__in": f"{self.collection1.name},{self.collection2.name}"
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["name"], "field2")
        self.assertEqual(response.data[1]["name"], "field1")

        # Only collection 1 has permissions.
        self.collection2.set_permission(Permission.NONE, self.contributor)
        request = factory.get(
            "/",
            {
                "collection__name__in": f"{self.collection1.name},{self.collection2.name}"
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        # No permission on collection 2.
        request = factory.get(
            "/",
            {"collection": f"{self.collection2.pk}"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        self.collection2.set_permission(Permission.VIEW, self.contributor)
        request = factory.get(
            "/",
            {"collection__slug": self.collection1.slug},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        # When collections filter with no match is given expect empty results.
        request = factory.get(
            "/",
            {"collection__slug": "nonexisting-slug"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        self.collection1.set_permission(Permission.NONE, self.contributor)
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)
        self.collection1.set_permission(Permission.VIEW, self.contributor)

        # Filter by name and label.
        request = factory.get("/", {"name": "field1"}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get("/", {"name__icontains": "Ld1"}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get("/", {"label": "Annotation field 1"}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get("/", {"label__icontains": "fiELd 1"}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        # Check sort ordering.
        # Add another field to annotation group 1.
        # Check that order changes with respect to group and the field sort order.
        self.annotation_group1.sort_order = self.annotation_group2.sort_order - 1
        self.annotation_group1.save()

        field: AnnotationField = AnnotationField.objects.create(
            name="field1_2",
            label="Annotation field 1_2",
            sort_order=self.annotation_field1.sort_order + 1,
            group=self.annotation_group1,
            type="INTEGER",
        )
        request = factory.get("/", {}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[0]["name"], "field1")
        self.assertEqual(response.data[0]["label"], "Annotation field 1")
        self.assertEqual(response.data[1]["name"], "field1_2")
        self.assertEqual(response.data[1]["label"], "Annotation field 1_2")
        self.assertEqual(response.data[2]["name"], "field2")
        self.assertEqual(response.data[2]["label"], "Annotation field 2")

        # Change the field sort order within the group.
        field.sort_order = self.annotation_field1.sort_order - 1
        field.save()
        response = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[1]["name"], "field1")
        self.assertEqual(response.data[1]["label"], "Annotation field 1")
        self.assertEqual(response.data[0]["name"], "field1_2")
        self.assertEqual(response.data[0]["label"], "Annotation field 1_2")
        self.assertEqual(response.data[2]["name"], "field2")
        self.assertEqual(response.data[2]["label"], "Annotation field 2")

        # Change the groups sort order.
        self.annotation_group1.sort_order = self.annotation_group2.sort_order + 1
        self.annotation_group1.save()
        field.save()
        response = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[2]["name"], "field1")
        self.assertEqual(response.data[2]["label"], "Annotation field 1")
        self.assertEqual(response.data[1]["name"], "field1_2")
        self.assertEqual(response.data[1]["label"], "Annotation field 1_2")
        self.assertEqual(response.data[0]["name"], "field2")
        self.assertEqual(response.data[0]["label"], "Annotation field 2")

    def test_set_to_collection(self):
        """Set annotation fields to collection."""
        self.collection1.annotation_fields.clear()
        self.collection2.annotation_fields.clear()

        viewset = CollectionViewSet.as_view(actions={"post": "set_annotation_fields"})
        request = factory.post(
            "/",
            {
                "annotation_fields": [
                    {"id": self.annotation_field1.pk},
                    {"id": self.annotation_field2.pk},
                ],
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.collection1.pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk, self.annotation_field2.pk],
        )
        self.assertCountEqual(
            self.collection2.annotation_fields.values_list("pk", flat=True), []
        )

        # Set only one.
        viewset = CollectionViewSet.as_view(actions={"post": "set_annotation_fields"})
        request = factory.post(
            "/",
            {
                "annotation_fields": [{"id": self.annotation_field1.pk}],
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.collection1.pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk],
        )
        self.assertCountEqual(
            self.collection2.annotation_fields.values_list("pk", flat=True), []
        )

        # No permission to edit.
        request = factory.post(
            "/",
            {
                "annotation_fields": [
                    {"id": self.annotation_field1.id},
                    {"id": self.annotation_field2.id},
                ],
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.collection2.pk)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_required_fields(self):
        """Test required fields are added to the collection."""
        collection = Collection.objects.create(
            name="Test collection", contributor=self.contributor
        )
        self.assertCountEqual(collection.annotation_fields.all(), [])
        self.annotation_field1.required = True
        self.annotation_field1.save()
        collection = Collection.objects.create(
            name="Test collection", contributor=self.contributor
        )
        self.assertCountEqual(
            collection.annotation_fields.all(), [self.annotation_field1]
        )

    def test_list_filter_values(self):
        request = factory.get("/", {}, format="json")

        # Unauthenticated request without permissions.
        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        # Unauthenticated request with permissions.
        self.collection1.set_permission(Permission.VIEW, self.anonymous)

        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)
        self.assertTrue(response.data[0]["field"], self.annotation_field1.pk)
        self.assertTrue(response.data[0]["value"], "string")
        self.assertTrue(response.data[0]["label"], "label string")
        self.collection1.set_permission(Permission.NONE, self.anonymous)

        # Authenticated request.
        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)
        self.assertTrue(response.data[0]["field"], self.annotation_field1.pk)
        self.assertTrue(response.data[0]["value"], "string")
        self.assertTrue(response.data[0]["label"], "label string")

        # Another authenticated request.
        self.annotation_value2.entity = self.entity1
        self.annotation_value2.save()
        request = factory.get("/", {"entity__in": [self.entity1.pk]}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 2)
        # Ordering should be first by group, then field.
        self.assertEqual(response.data[0]["id"], self.annotation_value2.pk)
        self.assertEqual(response.data[1]["id"], self.annotation_value1.pk)

        # Change group ordering and test responses.
        self.annotation_group2.sort_order = self.annotation_group1.sort_order + 1
        self.annotation_group2.save()
        request = factory.get("/", {"entity__in": [self.entity1.pk]}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 2)
        # Ordering should be first by group, then field.
        self.assertEqual(response.data[0]["id"], self.annotation_value1.pk)
        self.assertEqual(response.data[1]["id"], self.annotation_value2.pk)

        # On equal group ordering field ordering must be used.
        self.annotation_group2.sort_order = self.annotation_group1.sort_order
        self.annotation_group2.save()
        request = factory.get("/", {"entity__in": [self.entity1.pk]}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 2)
        # Ordering should be first by group, then field.
        self.assertEqual(response.data[0]["id"], self.annotation_value2.pk)
        self.assertEqual(response.data[1]["id"], self.annotation_value1.pk)

        # Reverse field ordering.
        self.annotation_field2.sort_order = self.annotation_field1.sort_order + 1
        self.annotation_field2.save()

        request = factory.get("/", {"entity__in": [self.entity1.pk]}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 2)
        # Ordering should be first by group, then field.
        self.assertEqual(response.data[0]["id"], self.annotation_value1.pk)
        self.assertEqual(response.data[1]["id"], self.annotation_value2.pk)

        # Filter by field_name
        request = factory.get(
            "/",
            {"entity": self.entity1.pk, "field__name": "field1"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)

        # Filter by value/label.
        request = factory.get(
            "/",
            {"entity": self.entity1.pk, "label": "label"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)

        request = factory.get(
            "/",
            {"entity": self.entity1.pk, "label": "string"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)

        # Filter by field_label
        request = factory.get(
            "/",
            {"entity__in": [self.entity1.pk], "field__label__icontains": "FiEld 1"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)

    def test_empty_vocabulary(self):
        """Test empty vocabulary.

        This is not strictly a viewset test but it fits best here.
        """
        # Setting vocabulary to empty on field with existing values must raise exception.
        self.annotation_field1.vocabulary = {}
        with self.assertRaisesMessage(
            ValidationError,
            str(["The value 'string' is not valid for the field group1.field1."]),
        ):
            self.annotation_field1.save()
        self.annotation_field1.refresh_from_db()
        self.assertIsNotNone(self.annotation_field1.vocabulary)

        # Disable vocabulary by setting vocabulary to None.
        # First check that incorrect entries are not allowed and disable the vocabulary.
        # The labels must be recomputed and any entry must be allowed.
        with self.assertRaisesMessage(
            ValidationError,
            str(["The value 'non_existing' is not valid for the field group1.field1."]),
        ):
            AnnotationValue.objects.create(
                entity=self.entity1, field=self.annotation_field1, value="non_existing"
            )
        self.annotation_value1.refresh_from_db()
        self.assertEqual(self.annotation_value1._value["label"], "label string")
        self.annotation_field1.vocabulary = None
        self.annotation_field1.save()
        self.annotation_value1.refresh_from_db()
        # Labels must be recomputed to the original value.
        self.assertEqual(self.annotation_value1._value["label"], "string")
        self.entity1.annotations.all().delete()
        AnnotationValue.objects.create(
            entity=self.entity1,
            field=self.annotation_field1,
            value="non_existing",
            contributor=self.contributor,
        )

    def test_set_values_by_path(self):
        def has_value(entity, field_id, value):
            self.assertEqual(
                value, entity.annotations.filter(field_id=field_id).get().value
            )

        # Remove vocabulary to simplify testing.
        self.annotation_field1.vocabulary = None
        self.annotation_field1.save()
        viewset = EntityViewSet.as_view(actions={"post": "set_annotations"})
        request = factory.post("/", {}, format="json")

        # Unauthenticated request, no permissions.
        response: Response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # Request without required parameter.
        request = factory.post("/", [{}], format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertDictEqual(
            response.data[0],
            {
                "field_path": ["This field is required."],
                "value": ["This field is required."],
            },
        )

        annotations = [
            {
                "field_path": str(self.annotation_field1),
                "value": "new value",
                "contributor": self.contributor.pk,
            },
            {
                "field_path": str(self.annotation_field2),
                "value": -1,
                "contributor": self.contributor.pk,
            },
        ]

        # Valid request without regex validation.
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.entity1.annotations.count(), 2)
        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, -1)

        annotations = [
            {"field_path": str(self.annotation_field1), "value": None},
            {"field_path": str(self.annotation_field2), "value": 2},
        ]

        # Valid request without regex validation, delete annotation.
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.entity1.annotations.count(), 1)
        has_value(self.entity1, self.annotation_field2.pk, 2)

        # Re-create deleted annotation value.
        self.annotation_value1 = AnnotationValue.objects.create(
            entity=self.entity1,
            field=self.annotation_field1,
            value="new value",
            contributor=self.contributor,
        )

        # Wrong type.
        annotations = [{"field_path": str(self.annotation_field1), "value": 10}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            response.data,
            {"error": ["The value '10' is not of the expected type 'str'."]},
        )

        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, 2)

        # Wrong regex.
        self.annotation_field1.validator_regex = "b+"
        self.annotation_field1.save()
        annotations = [{"field_path": str(self.annotation_field1), "value": "aaa"}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            response.data,
            {
                "error": [
                    f"The value 'aaa' for the field '{self.annotation_field1.pk}' does not match the regex 'b+'."
                ],
            },
        )
        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, 2)

        # Wrong regex and type.
        annotations = [{"field_path": str(self.annotation_field1), "value": 10}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertCountEqual(
            response.data["error"],
            [
                f"The value '10' for the field '{self.annotation_field1.pk}' does not match the regex 'b+'.",
                "The value '10' is not of the expected type 'str'.",
            ],
        )
        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, 2)

        # Multiple fields validation error.
        annotations = [
            {"field_path": str(self.annotation_field1), "value": 10},
            {"field_path": str(self.annotation_field2), "value": "string"},
        ]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertCountEqual(
            response.data["error"],
            [
                "The value '10' is not of the expected type 'str'.",
                f"The value '10' for the field '{self.annotation_field1.pk}' does not match the regex 'b+'.",
                "The value 'string' is not of the expected type 'int'.",
            ],
        )
        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, 2)

        # Regular request with regex validation.
        annotations = [{"field_path": str(self.annotation_field1), "value": "bbb"}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.annotation_value1.refresh_from_db()
        self.assertEqual(self.annotation_value1.value, "new value")
        has_value(self.entity1, self.annotation_field1.pk, "bbb")
        has_value(self.entity1, self.annotation_field2.pk, 2)
        has_value(self.entity1, self.annotation_field2.pk, 2)

        # Non-existing field.
        annotations = [{"field_path": "non.existing", "value": "bbb"}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertContains(
            response, "Field 'non.existing' does not exist.", status_code=400
        )

        # Invalid field name.
        annotations = [{"field_path": "wei.rd.field", "value": "bbb"}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response = viewset(request, pk=self.entity1.pk)
        self.assertContains(response, "Invalid path 'wei.rd.field'.", status_code=400)

        # Check the entire history of the annotation value1.
        # "string" -> new value -> deleted -> new value -> bbb
        expected = [
            {"value": "string", "label": "string"},
            {"value": "new value", "label": "new value"},
            None,
            {"value": "new value", "label": "new value"},
            {"value": "bbb", "label": "bbb"},
        ]
        self.assertEqual(
            expected,
            list(
                AnnotationValue.all_objects.filter(
                    entity=self.annotation_value1.entity,
                    field=self.annotation_value1.field,
                ).values_list("_value", flat=True)
            ),
        )
