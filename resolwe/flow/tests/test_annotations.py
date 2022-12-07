# pylint: disable=missing-docstring

from django.http import HttpResponse

from rest_framework import status
from rest_framework.response import Response
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import AnnotationField, Collection, Entity
from resolwe.flow.models.annotations import (
    AnnotationGroup,
    AnnotationPreset,
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
            name="Preset 1"
        )
        self.preset2: AnnotationPreset = AnnotationPreset.objects.create(
            name="Preset 2"
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
        # Unauthenticated user should get 403 response.
        request = factory.get("/", {}, format="json")
        response: HttpResponse = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        # Authenticated users should see all the fields.
        force_authenticate(request, self.contributor)
        response: HttpResponse = self.annotationfield_viewset(request)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["name"], "field2")
        self.assertEqual(response.data[0]["label"], "Annotation field 2")
        self.assertCountEqual(response.data[0]["collection"], [self.collection2.id])

        self.assertEqual(response.data[1]["name"], "field1")
        self.assertEqual(response.data[1]["label"], "Annotation field 1")
        self.assertCountEqual(response.data[1]["collection"], [self.collection1.id])

        # Filter by collection.
        request = factory.get(
            "/",
            {"collection": self.collection1.pk},
            format="json",
        )
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
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)
        self.collection1.set_permission(Permission.VIEW, self.contributor)

        # Filter by name and label.
        request = factory.get("/", {"name": "field1"}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get("/", {"name__icontains": "Ld1"}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get("/", {"label": "Annotation field 1"}, format="json")
        force_authenticate(request, self.contributor)
        response = self.annotationfield_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["name"], "field1")

        request = factory.get("/", {"label__icontains": "fiELd 1"}, format="json")
        force_authenticate(request, self.contributor)
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
        force_authenticate(request, self.contributor)
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
        force_authenticate(request, self.contributor)
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
        force_authenticate(request, self.contributor)
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

    def test_add_to_collection(self):
        """Add annotation fields to collection."""
        self.collection1.annotation_fields.clear()
        self.collection2.annotation_fields.clear()

        viewset = CollectionViewSet.as_view(
            actions={"post": "add_fields_to_collection"}
        )
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
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk, self.annotation_field2.pk],
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

    def test_remove_from_collection(self):
        """Add annotation fields to collection."""
        self.collection1.annotation_fields.add(self.annotation_field2)
        viewset = CollectionViewSet.as_view(
            actions={"post": "remove_fields_from_collection"}
        )

        # Request without confirmation.
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
        response: Response = viewset(request, pk=self.collection1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data,
            [
                "Annotations for the given fields will be removed from the samples in "
                "the collection. Set 'confirm_action' argument to 'True' to confirm."
            ],
        )
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk, self.annotation_field2.pk],
        )

        request = factory.post(
            "/",
            {
                "annotation_fields": [
                    {"id": self.annotation_field1.id},
                    {"id": self.annotation_field2.id},
                ],
                "confirm_action": False,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.collection1.pk)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data,
            [
                "Annotations for the given fields will be removed from the samples in "
                "the collection. Set 'confirm_action' argument to 'True' to confirm."
            ],
        )
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk, self.annotation_field2.pk],
        )

        # Request with confirmation.
        request = factory.post(
            "/",
            {
                "annotation_fields": [
                    {"id": self.annotation_field1.id},
                    {"id": self.annotation_field2.id},
                ],
                "confirm_action": True,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk, self.annotation_field2.pk],
        )
        response: Response = viewset(request, pk=self.collection1.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True), []
        )

        # Request with confirmation without permissions.
        self.collection1.annotation_fields.add(self.annotation_field1)
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk],
        )
        request = factory.post(
            "/",
            {
                "annotation_fields": [
                    {"id": self.annotation_field1.id},
                    {"id": self.annotation_field2.id},
                ],
                "confirm_action": True,
            },
            format="json",
        )
        force_authenticate(request, self.contributor)
        response: Response = viewset(request, pk=self.collection2.pk)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        self.assertDictEqual(
            response.data,
            {"detail": "You do not have permission to perform this action."},
        )
        self.assertCountEqual(
            self.collection1.annotation_fields.values_list("pk", flat=True),
            [self.annotation_field1.pk],
        )

    def test_list_filter_values(self):
        request = factory.get("/", {}, format="json")

        # Unauthenticated request, no permissions.
        response: HttpResponse = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        # Authenticated request without permissions on entities.
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["entity_ids"][0], "This field is required.")

        # Authenticated request.
        request = factory.get("/", {"entity_ids": [self.entity1.pk]}, format="json")
        force_authenticate(request, self.contributor)
        response: Response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)
        self.assertTrue(response.data[0]["field"], self.annotation_field1.pk)
        self.assertTrue(response.data[0]["value"], "string")

        # Another authenticated request.
        self.annotation_value2.entity = self.entity1
        self.annotation_value2.save()
        request = factory.get("/", {"entity_ids": [self.entity1.pk]}, format="json")
        force_authenticate(request, self.contributor)
        response: Response = self.annotationvalue_viewset(request)
        self.assertTrue(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data), 2)

        # Filter by field_name
        request = factory.get(
            "/",
            {"entity_ids": [self.entity1.pk], "field__name": "field1"},
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
            {"entity_ids": [self.entity1.pk], "field__label__icontains": "FiEld 1"},
            format="json",
        )
        force_authenticate(request, self.contributor)
        response = self.annotationvalue_viewset(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertTrue(response.data[0]["id"], self.annotation_value1.pk)

    def test_set_values(self):
        def has_value(entity, field_id, value):
            self.assertEqual(
                value, entity.annotations.filter(field_id=field_id).get().value
            )

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

        print("All onnatitons")
        for av in AnnotationValue.objects.all():
            print(av.__dict__)

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
