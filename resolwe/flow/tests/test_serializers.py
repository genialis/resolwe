# pylint: disable=missing-docstring
from collections import OrderedDict

from rest_framework.test import APIRequestFactory

from resolwe.flow.models import Data, DescriptorSchema, Process
from resolwe.flow.serializers import DataSerializer
from resolwe.permissions.models import Permission
from resolwe.test import TestCase


class ResolweDictRelatedFieldTest(TestCase):
    def setUp(self):
        super().setUp()

        self.process = Process.objects.create(
            slug="test-process",
            contributor=self.contributor,
        )
        self.process.set_permission(Permission.VIEW, self.user)

        self.descriptor_schema1 = DescriptorSchema.objects.create(
            slug="test-schema",
            contributor=self.contributor,
            version="1.0.0",
        )
        self.descriptor_schema1.set_permission(Permission.VIEW, self.user)

        self.descriptor_schema2 = DescriptorSchema.objects.create(
            slug="test-schema",
            contributor=self.contributor,
            version="2.0.0",
        )
        self.descriptor_schema2.set_permission(Permission.VIEW, self.user)

        self.descriptor_schema3 = DescriptorSchema.objects.create(
            slug="test-schema",
            contributor=self.contributor,
            version="3.0.0",
        )

        self.factory = APIRequestFactory()

    def test_to_internal_value(self):
        request = self.factory.get("/")
        request.user = self.user
        request.query_params = {}
        data = {
            "contributor": self.user.pk,
            "process": {"slug": "test-process"},
            "descriptor_schema": {"slug": "test-schema"},
        }

        serializer = DataSerializer(data=data, context={"request": request})
        # is_valid() needs to be called before accessing ``validated_data``
        serializer.is_valid()
        # Check that descriptor schmena with highest version & view permission is used:
        self.assertEqual(
            serializer.validated_data["descriptor_schema"], self.descriptor_schema2
        )

    def test_to_representation(self):
        request = self.factory.get("/")
        request.user = self.user
        request.query_params = {}

        data = Data.objects.create(
            contributor=self.user,
            process=self.process,
            descriptor_schema=self.descriptor_schema1,
        )

        serializer = DataSerializer(data, context={"request": request})
        self.assertEqual(serializer.data["process"]["id"], self.process.pk)

        # Check that descriptor_schema is properly hydrated (but remove
        # values that are not deterministic from the checking procedure)
        descriptor_schema_hydrated = serializer.data["descriptor_schema"]
        for key in ["created", "modified", "id"]:
            self.assertTrue(key in descriptor_schema_hydrated)
            descriptor_schema_hydrated.pop(key)
        descriptor_schema_hydrated.get("contributor", {}).pop("id")
        self.assertDictEqual(
            descriptor_schema_hydrated,
            {
                "slug": "test-schema",
                "version": "1.0.0",
                "name": "",
                "description": "",
                "schema": [],
                "contributor": OrderedDict(
                    [
                        ("first_name", "Joe"),
                        ("last_name", "Miller"),
                        ("username", "contributor"),
                    ]
                ),
            },
        )
