# pylint: disable=missing-docstring
from typing import Sequence

from django.core.exceptions import ValidationError
from django.http import HttpResponse

from rest_framework import status
from rest_framework.response import Response
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import AnnotationField, Collection, Entity
from resolwe.flow.models.annotations import (
    AnnotationGroup,
    AnnotationPreset,
    AnnotationType,
    AnnotationValue,
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
        field_values = {
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
                AnnotationValue.objects.create(entity=entity, field=field, value=value)
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
        field_values = {
            AnnotationType.STRING: [(entity1, "abc"), (entity2, "bc")],
            AnnotationType.INTEGER: [(entity1, 2), (entity2, 10)],
            AnnotationType.DECIMAL: [(entity1, 2.2), (entity2, 10.1)],
            AnnotationType.DATE: [(entity1, "1111-01-01"), (entity2, "2222-02-02")],
        }
        for annotation_type, values in field_values.items():
            for entity, value in values:
                field = self.fields[annotation_type]
                AnnotationValue.objects.create(entity=entity, field=field, value=value)
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

        self.annotation_field1: AnnotationField = AnnotationField.objects.create(
            name="field1",
            label="Annotation field 1",
            sort_order=2,
            group=self.annotation_group1,
            type="STRING",
            vocabulary={"string": "label string"},
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
            actions={"get": "list"}
        )
        self.annotation_value1: AnnotationValue = AnnotationValue.objects.create(
            entity=self.entity1, field=self.annotation_field1, value="string"
        )
        self.annotation_value2: AnnotationValue = AnnotationValue.objects.create(
            entity=self.entity2, field=self.annotation_field2, value=2
        )

    def test_annotate_path(self):
        """Test annotate entity queryset."""
        entities = Entity.objects.all().annotate_path("group1.field1")
        first = entities.get(pk=self.entity1.pk)
        second = entities.get(pk=self.entity2.pk)
        self.assertEqual(first.group1_field1, "string")
        self.assertIsNone(second.group1_field1)

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

    def test_list_filter_preset(self):
        request = factory.get("/", {}, format="json")
        response: HttpResponse = self.preset_viewset(request)

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
        response: HttpResponse = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["name"], "field2")
        self.assertEqual(response.data[0]["label"], "Annotation field 2")
        received = dict(response.data[0])
        received["group"] = dict(received["group"])
        self.assertEqual(
            received,
            {
                "id": self.annotation_field2.id,
                "name": "field2",
                "label": "Annotation field 2",
                "sort_order": 1,
                "type": "INTEGER",
                "validator_regex": None,
                "vocabulary": None,
                "required": False,
                "collection": [self.collection2.pk],
                "description": "",
                "group": {
                    "id": self.annotation_group2.id,
                    "name": "group2",
                    "label": "Annotation group 2",
                    "sort_order": 1,
                },
            },
        )
        self.assertCountEqual(response.data[0]["collection"], [self.collection2.id])
        self.assertEqual(response.data[1]["name"], "field1")
        self.assertEqual(response.data[1]["label"], "Annotation field 1")
        self.assertCountEqual(response.data[1]["collection"], [self.collection1.id])
        self.assertEqual(
            dict(response.data[1]["group"]),
            {
                "id": self.annotation_group1.id,
                "name": "group1",
                "label": "Annotation group 1",
                "sort_order": 2,
            },
        )

        # Filter by id.
        request = factory.get("/", {"id": self.annotation_field1.pk}, format="json")
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], self.annotation_field1.name)

        # Filter by annotation values path.
        field = self.annotation_value1.field
        entity = self.annotation_value1.entity
        request = factory.get(
            "/", {"entity": entity.pk, "full_path": "non.existing"}, format="json"
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        request = factory.get(
            "/",
            {
                "entity": entity.pk,
                "full_path": f"{field.group.name}.{field.name}",
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["value"], self.annotation_value1.value)

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

        # When no permissions are given the 403 must be returned.
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
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
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
        response: HttpResponse = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[0]["name"], "field1")
        self.assertEqual(response.data[0]["label"], "Annotation field 1")
        self.assertEqual(response.data[1]["name"], "field1_2")
        self.assertEqual(response.data[1]["label"], "Annotation field 1_2")
        self.assertEqual(response.data[2]["name"], "field2")
        self.assertEqual(response.data[2]["label"], "Annotation field 2")
        self.assertCountEqual(response.data[0]["collection"], [self.collection1.id])
        self.assertCountEqual(response.data[1]["collection"], [])
        self.assertCountEqual(response.data[2]["collection"], [self.collection2.id])

        # Change the field sort order within the group.
        field.sort_order = self.annotation_field1.sort_order - 1
        field.save()
        response: HttpResponse = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[1]["name"], "field1")
        self.assertEqual(response.data[1]["label"], "Annotation field 1")
        self.assertEqual(response.data[0]["name"], "field1_2")
        self.assertEqual(response.data[0]["label"], "Annotation field 1_2")
        self.assertEqual(response.data[2]["name"], "field2")
        self.assertEqual(response.data[2]["label"], "Annotation field 2")
        self.assertCountEqual(response.data[1]["collection"], [self.collection1.id])
        self.assertCountEqual(response.data[0]["collection"], [])
        self.assertCountEqual(response.data[2]["collection"], [self.collection2.id])

        # Change the groups sort order.
        self.annotation_group1.sort_order = self.annotation_group2.sort_order + 1
        self.annotation_group1.save()
        field.save()
        response: HttpResponse = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[2]["name"], "field1")
        self.assertEqual(response.data[2]["label"], "Annotation field 1")
        self.assertEqual(response.data[1]["name"], "field1_2")
        self.assertEqual(response.data[1]["label"], "Annotation field 1_2")
        self.assertEqual(response.data[0]["name"], "field2")
        self.assertEqual(response.data[0]["label"], "Annotation field 2")
        self.assertCountEqual(response.data[2]["collection"], [self.collection1.id])
        self.assertCountEqual(response.data[1]["collection"], [])
        self.assertCountEqual(response.data[0]["collection"], [self.collection2.id])

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
        response: Response = viewset(request, pk=self.collection1.pk)
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
        response: Response = viewset(request, pk=self.collection2.pk)
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

        # Unauthenticated request without entity filter.
        response: HttpResponse = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["__all__"][0],
            "At least one of the entity filters must be set.",
        )

        # Authenticated request without entity filter.
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["__all__"][0],
            "At least one of the entity filters must be set.",
        )

        # Unauthenticated request without permissions.
        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        response: Response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        # Unauthenticated request with permissions.
        self.collection1.set_permission(Permission.VIEW, self.anonymous)
        request = factory.get("/", {"entity": self.entity1.pk}, format="json")
        response: Response = self.annotationvalue_viewset(request)
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
        response: Response = self.annotationvalue_viewset(request)
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
        response: Response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 2)

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
            entity=self.entity1, field=self.annotation_field1, value="non_existing"
        )

    def test_set_values(self):
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
        response: HttpResponse = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # Request without required parameter.
        request = factory.post("/", [{}], format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertDictEqual(
            response.data[0],
            {
                "field": ["This field is required."],
                "value": ["This field is required."],
            },
        )

        annotations = [
            {"field": {"id": self.annotation_field1.pk}, "value": "new value"},
            {"field": {"id": self.annotation_field2.pk}, "value": -1},
        ]

        # Valid request without regex validation.
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.entity1.annotations.count(), 2)
        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, -1)

        # Wrong type.
        annotations = [{"field": {"id": self.annotation_field1.pk}, "value": 10}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            response.data,
            {"error": ["The value '10' is not of the expected type 'str'."]},
        )

        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, -1)

        # Wrong regex.
        self.annotation_field1.validator_regex = "b+"
        self.annotation_field1.save()
        annotations = [{"field": {"id": self.annotation_field1.pk}, "value": "aaa"}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)
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
        has_value(self.entity1, self.annotation_field2.pk, -1)

        # Wrong regex and type.
        annotations = [{"field": {"id": self.annotation_field1.pk}, "value": 10}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertCountEqual(
            response.data["error"],
            [
                f"The value '10' for the field '{self.annotation_field1.pk}' does not match the regex 'b+'.",
                "The value '10' is not of the expected type 'str'.",
            ],
        )
        has_value(self.entity1, self.annotation_field1.pk, "new value")
        has_value(self.entity1, self.annotation_field2.pk, -1)

        # Multiple fields validation error.
        annotations = [
            {"field": {"id": self.annotation_field1.pk}, "value": 10},
            {"field": {"id": self.annotation_field2.pk}, "value": "string"},
        ]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)
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
        has_value(self.entity1, self.annotation_field2.pk, -1)

        # Regular request with regex validation.
        annotations = [{"field": {"id": self.annotation_field1.pk}, "value": "bbb"}]
        request = factory.post("/", annotations, format="json")
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.entity1.pk)
        self.annotation_value1.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.annotation_value1.value, "bbb")
        has_value(self.entity1, self.annotation_field1.pk, "bbb")
        has_value(self.entity1, self.annotation_field2.pk, -1)
