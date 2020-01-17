# pylint: disable=missing-docstring
from resolwe.flow.models import Data, DescriptorSchema, Process
from resolwe.test import TestCase


class DescriptorTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.process = Process.objects.create(
            name="Dummy process", contributor=self.contributor
        )

        self.descriptor_schema = DescriptorSchema.objects.create(
            name="Descriptor schema",
            contributor=self.contributor,
            schema=[
                {
                    "name": "test_field",
                    "type": "basic:string:",
                    "default": "default value",
                }
            ],
        )

    def test_default_values(self):
        data = Data.objects.create(
            name="Data object",
            contributor=self.contributor,
            process=self.process,
            descriptor_schema=self.descriptor_schema,
        )
        self.assertEqual(data.descriptor["test_field"], "default value")

        data = Data.objects.create(
            name="Data object 2",
            contributor=self.contributor,
            process=self.process,
            descriptor_schema=self.descriptor_schema,
            descriptor={"test_field": "changed value"},
        )
        self.assertEqual(data.descriptor["test_field"], "changed value")
