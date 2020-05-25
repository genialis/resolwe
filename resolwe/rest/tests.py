# pylint: disable=missing-docstring
import itertools

from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Data, Entity, Process
from resolwe.flow.views import DataViewSet
from resolwe.test import TestCase

factory = APIRequestFactory()


class ProjectionTest(TestCase):
    def setUp(self):
        super().setUp()

        self.entity = Entity.objects.create(
            name="Test entity", contributor=self.contributor
        )
        process = Process.objects.create(
            name="Test process",
            contributor=self.contributor,
            output_schema=[
                {
                    "name": "foo",
                    "label": "Foo",
                    "group": [
                        {"name": "bar", "label": "Bar", "type": "basic:integer:"},
                        {"name": "hello", "label": "Hello", "type": "basic:string:"},
                    ],
                },
                {"name": "another", "label": "Another", "type": "basic:integer:"},
            ],
        )
        data_output = {
            "foo": {"bar": 42, "hello": "world",},
            "another": 3,
        }
        self.data_output = data_output
        self.data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
            output=data_output,
        )
        self.data_2 = Data.objects.create(
            name="Test data 2",
            contributor=self.contributor,
            process=process,
            output=data_output,
        )

        self.entity.data.add(self.data)
        self.entity.data.add(self.data_2)

        self.data_viewset = DataViewSet.as_view(actions={"get": "list"})

    def get_projection(self, fields):
        request = factory.get("/", {"fields": ",".join(fields)}, format="json")
        force_authenticate(request, self.admin)
        return self.data_viewset(request).data

    def test_projection(self):
        # Test top-level projection.
        all_fields = self.get_projection([])[0].keys()
        for field_count in range(1, 3):
            for fields in itertools.combinations(all_fields, field_count):
                data = self.get_projection(fields)[0]
                self.assertCountEqual(data.keys(), set(fields))

        # Test nested projection.
        data = self.get_projection(["entity__name,process__contributor__username"])[0]
        self.assertEqual(
            data,
            {
                "entity": {"name": "Test entity"},
                "process": {"contributor": {"username": "contributor",},},
            },
        )
        # Test deep projection: an empty projection means that all fields
        # should be returned without any projection.
        data = self.get_projection(["entity"])[0]
        self.assertCountEqual(
            data["entity"].keys(),
            [
                "created",
                "descriptor_dirty",
                "duplicated",
                "id",
                "modified",
                "contributor",
                "data_count",
                "description",
                "descriptor",
                "descriptor_schema",
                "name",
                "settings",
                "slug",
                "tags",
                "collection",
                "type",
                "status",
            ],
        )

        # Test top-level JSON projection.
        data = self.get_projection(["output"])[0]
        self.assertEqual(data["output"], self.data_output)

        # Test nested projection into JSON.
        data = self.get_projection(["output__foo__bar"])[0]
        self.assertEqual(data, {"output": {"foo": {"bar": 42,},},})
