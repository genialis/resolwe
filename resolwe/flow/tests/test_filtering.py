# pylint: disable=missing-docstring
import datetime

from django.contrib.auth import get_user_model
from django.utils.timezone import get_current_timezone

from guardian.shortcuts import assign_perm
from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.views import (
    CollectionViewSet,
    DataViewSet,
    DescriptorSchemaViewSet,
    EntityViewSet,
    ProcessViewSet,
)
from resolwe.test import TestCase

factory = APIRequestFactory()


class BaseViewSetFiltersTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        user_model = get_user_model()
        cls.admin = user_model.objects.create_superuser(
            username="admin",
            email="admin@test.com",
            password="admin",
            first_name="James",
            last_name="Smith",
        )
        cls.contributor = user_model.objects.create_user(
            username="contributor",
            email="contributor@test.com",
            first_name="Joe",
            last_name="Miller",
        )
        cls.user = user_model.objects.create_user(
            username="normal_user",
            email="user@test.com",
            first_name="John",
            last_name="Williams",
        )

    def setUp(self):
        self.viewset = self.viewset_class.as_view(actions={"get": "list",})

    def _check_filter(
        self, query_args, expected, expected_status_code=status.HTTP_200_OK
    ):
        """Check that query_args filter to expected queryset."""
        request = factory.get("/", query_args, format="json")
        force_authenticate(request, self.admin)
        response = self.viewset(request)

        if status.is_success(response.status_code):
            self.assertEqual(len(response.data), len(expected))
            self.assertCountEqual(
                [item.pk for item in expected], [item["id"] for item in response.data],
            )
        else:
            self.assertEqual(response.status_code, expected_status_code)
            response.render()

        return response


class CollectionViewSetFiltersTest(BaseViewSetFiltersTest):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        cls.descriptor_schema = DescriptorSchema.objects.create(
            slug="test-schema",
            version="1.0.0",
            contributor=cls.contributor,
            schema=[
                {
                    "name": "company",
                    "group": [
                        {"name": "name", "type": "basic:string:", "required": False,},
                        {
                            "name": "departments",
                            "type": "list:basic:string:",
                            "required": False,
                        },
                    ],
                }
            ],
        )

        cls.collections = [
            Collection.objects.create(
                name="Test collection 0",
                slug="test-collection-0",
                contributor=cls.contributor,
                description="My description!",
                descriptor_schema=cls.descriptor_schema,
                descriptor={
                    "company": {
                        "name": "Genialis",
                        "departments": ["engineering", "operations"],
                    }
                },
                tags=["first-tag"],
            ),
            Collection.objects.create(
                name="Test collection 1",
                slug="test-collection-1",
                contributor=cls.contributor,
                description="My favourite test collection",
                descriptor_schema=cls.descriptor_schema,
                tags=["first-tag", "second-tag"],
            ),
            Collection.objects.create(
                name="User test collection",
                slug="user-collection",
                contributor=cls.user,
                description="User's description",
                tags=["second-tag"],
            ),
        ]

        tzone = get_current_timezone()
        cls.collections[0].created = datetime.datetime(2016, 7, 30, 0, 59, tzinfo=tzone)
        cls.collections[0].save()
        cls.collections[1].created = datetime.datetime(2016, 7, 30, 1, 59, tzinfo=tzone)
        cls.collections[1].save()
        cls.collections[2].created = datetime.datetime(2016, 7, 30, 2, 59, tzinfo=tzone)
        cls.collections[2].save()

        assign_perm("owner_collection", cls.admin, cls.collections[0])
        assign_perm("owner_collection", cls.admin, cls.collections[1])
        assign_perm("owner_collection", cls.user, cls.collections[2])

        cls.viewset_class = CollectionViewSet

    def test_filter_id(self):
        self._check_filter({"id": self.collections[0].pk}, [self.collections[0]])
        self._check_filter({"id": "420"}, [])
        self._check_filter(
            {"id__in": "{},{}".format(self.collections[0].pk, self.collections[1].pk)},
            self.collections[:2],
        )
        self._check_filter({"id__gt": self.collections[0].pk}, self.collections[1:])

    def test_filter_slug(self):
        self._check_filter({"slug": "test-collection-0"}, [self.collections[0]])
        self._check_filter(
            {"slug__in": "test-collection-1,user-collection"}, self.collections[1:]
        )

    def test_filter_name(self):
        self._check_filter({"name": "Test collection 1"}, [self.collections[1]])
        self._check_filter({"name": "Test collection"}, [])

        self._check_filter({"name__iexact": "test collection 1"}, [self.collections[1]])

        self._check_filter(
            {"name__startswith": "Test collection"}, self.collections[:2]
        )
        self._check_filter(
            {"name__istartswith": "test collection"}, self.collections[:2]
        )

        self._check_filter({"name__contains": "test"}, [self.collections[2]])
        self._check_filter({"name__icontains": "test"}, self.collections)

    def test_filter_description(self):
        self._check_filter(
            {"description__icontains": "Favourite"}, [self.collections[1]]
        )
        self._check_filter({"description__contains": "Favourite"}, [])
        self._check_filter({"description__contains": 420}, [])

    def test_filter_descriptor_schema(self):
        self._check_filter(
            {"descriptor_schema": str(self.descriptor_schema.pk)}, self.collections[:2]
        )
        self._check_filter({"descriptor_schema": "999999"}, [])

    def test_filter_tags(self):
        self._check_filter({"tags": "first-tag"}, self.collections[:2])
        self._check_filter({"tags": "first-tag,second-tag"}, [self.collections[1]])

    def test_filter_contributor(self):
        self._check_filter(
            {"contributor": str(self.contributor.pk)}, self.collections[:2]
        )
        self._check_filter(
            {"contributor__in": "{},{}".format(self.contributor.pk, self.user.pk)},
            self.collections,
        )

    def test_filter_owners(self):
        self._check_filter({"owners": str(self.admin.pk)}, self.collections[:2])
        self._check_filter({"owners": "999999"}, [])

    def test_filter_contributor_name(self):
        # Filter by first name.
        self._check_filter({"contributor_name": "Joe"}, self.collections[:2])
        # Filter by last name.
        self._check_filter({"contributor_name": "Miller"}, self.collections[:2])
        # Filter by username.
        self._check_filter({"contributor_name": "contributor"}, self.collections[:2])
        # Filter by combination of first and last name.
        self._check_filter({"contributor_name": "John Williams"}, [self.collections[2]])

    def test_filter_owners_name(self):
        # Filter by first name.
        self._check_filter({"owners_name": "James"}, self.collections[:2])
        # Filter by last name.
        self._check_filter({"owners_name": "Smith"}, self.collections[:2])
        # Filter by username.
        self._check_filter({"owners_name": "admin"}, self.collections[:2])
        # Filter by combination of first and last name.
        self._check_filter({"owners_name": "John Williams"}, [self.collections[2]])

    def test_filter_created(self):
        self._check_filter({"created": "2016-07-30T00:59:00"}, self.collections[:1])
        self._check_filter({"created__gt": "2016-07-30T01:59:00"}, self.collections[2:])
        self._check_filter(
            {"created__gte": "2016-07-30T01:59:00"}, self.collections[1:]
        )
        self._check_filter({"created__lt": "2016-07-30T01:59:00"}, self.collections[:1])
        self._check_filter(
            {"created__lte": "2016-07-30T01:59:00"}, self.collections[:2]
        )

    def test_filter_modified(self):
        self._check_filter(
            {"modified": self.collections[0].modified.isoformat()}, self.collections[:1]
        )
        self._check_filter(
            {"modified__gt": self.collections[1].modified.isoformat()},
            self.collections[2:],
        )
        self._check_filter(
            {"modified__gte": self.collections[1].modified.isoformat()},
            self.collections[1:],
        )
        self._check_filter(
            {"modified__lt": self.collections[1].modified.isoformat()},
            self.collections[:1],
        )
        self._check_filter(
            {"modified__lte": self.collections[1].modified.isoformat()},
            self.collections[:2],
        )

    def test_filter_text(self):
        # By name.
        self._check_filter({"text": "Test collection 1"}, [self.collections[1]])
        self._check_filter({"text": "Test"}, self.collections)
        self._check_filter({"text": "test"}, self.collections)
        self._check_filter({"text": "user"}, [self.collections[2]])

        # By contributor.
        self._check_filter({"text": "joe"}, self.collections[:2])
        self._check_filter({"text": "Miller"}, self.collections[:2])

        # By owner.
        self._check_filter({"text": "james"}, self.collections[:2])
        self._check_filter({"text": "Smith"}, self.collections[:2])

        # By description.
        self._check_filter(
            {"text": "description"}, [self.collections[0], self.collections[2]]
        )
        self._check_filter({"text": "my"}, self.collections[:2])
        self._check_filter({"text": "my description"}, [self.collections[0]])
        self._check_filter({"text": "user"}, [self.collections[2]])

        # By descriptor.
        self._check_filter({"text": "genialis"}, [self.collections[0]])
        self._check_filter({"text": "engineering"}, [self.collections[0]])

        # By mixed fields.
        self._check_filter({"text": "test joe"}, self.collections[:2])
        self._check_filter({"text": "joe my description"}, [self.collections[0]])

        # Check that changes are applied immediately.
        self.collections[0].name = "Awesome new name"
        self.collections[0].save()

        self._check_filter({"text": "awesome"}, [self.collections[0]])

    def test_filter_mixed(self):
        self._check_filter(
            {"name": "Test collection 0", "owners_name": "James Smith"},
            [self.collections[0]],
        )
        self._check_filter(
            {"text": "collection 0", "owners_name": "James Smith"},
            [self.collections[0]],
        )

    def test_order_by_relevance(self):
        result = self._check_filter({"text": "test collection"}, self.collections)
        self.assertEqual(result.data[0]["id"], self.collections[1].pk)

        # Make another collection more important.
        collection = self.collections[2]
        collection.description = "This is test collection. My test collection rocks."
        collection.save()

        result = self._check_filter({"text": "test collection"}, self.collections)
        self.assertEqual(result.data[0]["id"], self.collections[2].pk)

        # Check that ordering can be overriden.
        result = self._check_filter(
            {"text": "test collection", "ordering": "id"}, self.collections
        )
        self.assertEqual(result.data[0]["id"], self.collections[0].pk)


class EntityViewSetFiltersTest(BaseViewSetFiltersTest):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        cls.descriptor_schema = DescriptorSchema.objects.create(
            slug="test-schema",
            version="1.0.0",
            contributor=cls.contributor,
            schema=[
                {
                    "name": "company",
                    "group": [
                        {"name": "name", "type": "basic:string:", "required": False,},
                        {
                            "name": "departments",
                            "type": "list:basic:string:",
                            "required": False,
                        },
                    ],
                }
            ],
        )

        cls.collection1 = Collection.objects.create(
            name="My collection", slug="my-collection", contributor=cls.contributor,
        )

        cls.collection2 = Collection.objects.create(
            name="Other collection",
            slug="other-collection",
            contributor=cls.contributor,
        )

        cls.entities = [
            Entity.objects.create(
                name="Test entity 0",
                slug="test-entity-0",
                contributor=cls.contributor,
                collection=cls.collection1,
                description="My description!",
                descriptor_schema=cls.descriptor_schema,
                descriptor={
                    "company": {
                        "name": "Genialis",
                        "departments": ["engineering", "operations"],
                    }
                },
                tags=["first-tag"],
            ),
            Entity.objects.create(
                name="Test entity 1",
                slug="test-entity-1",
                contributor=cls.contributor,
                collection=cls.collection2,
                description="My favourite test entity",
                descriptor_schema=cls.descriptor_schema,
                tags=["first-tag", "second-tag"],
            ),
            Entity.objects.create(
                name="User test entity",
                slug="user-entity",
                contributor=cls.user,
                description="User's description",
                tags=["second-tag"],
            ),
        ]

        tzone = get_current_timezone()
        cls.entities[0].created = datetime.datetime(2016, 7, 30, 0, 59, tzinfo=tzone)
        cls.entities[0].save()
        cls.entities[1].created = datetime.datetime(2016, 7, 30, 1, 59, tzinfo=tzone)
        cls.entities[1].save()
        cls.entities[2].created = datetime.datetime(2016, 7, 30, 2, 59, tzinfo=tzone)
        cls.entities[2].save()

        assign_perm("owner_entity", cls.admin, cls.entities[0])
        assign_perm("owner_entity", cls.admin, cls.entities[1])
        assign_perm("owner_entity", cls.user, cls.entities[2])

        cls.viewset_class = EntityViewSet

    def test_filter_id(self):
        self._check_filter({"id": self.entities[0].pk}, [self.entities[0]])
        self._check_filter({"id": "420"}, [])
        self._check_filter(
            {"id__in": "{},{}".format(self.entities[0].pk, self.entities[1].pk)},
            self.entities[:2],
        )
        self._check_filter({"id__gt": str(self.entities[0].pk)}, self.entities[1:])

    def test_filter_slug(self):
        self._check_filter({"slug": "test-entity-0"}, [self.entities[0]])
        self._check_filter({"slug__in": "test-entity-1,user-entity"}, self.entities[1:])

    def test_filter_name(self):
        self._check_filter({"name": "Test entity 1"}, [self.entities[1]])
        self._check_filter({"name": "Test entity"}, [])

        self._check_filter({"name__iexact": "test entity 1"}, [self.entities[1]])

        self._check_filter({"name__startswith": "Test entity"}, self.entities[:2])
        self._check_filter({"name__istartswith": "test entity"}, self.entities[:2])

        self._check_filter({"name__contains": "test"}, [self.entities[2]])
        self._check_filter({"name__icontains": "test"}, self.entities)

    def test_filter_description(self):
        self._check_filter({"description__icontains": "Favourite"}, [self.entities[1]])
        self._check_filter({"description__contains": "Favourite"}, [])
        self._check_filter({"description__contains": "420"}, [])

    def test_filter_descriptor_schema(self):
        self._check_filter(
            {"descriptor_schema": str(self.descriptor_schema.pk)}, self.entities[:2]
        )
        self._check_filter({"descriptor_schema": "999999"}, [])

    def test_filter_tags(self):
        self._check_filter({"tags": "first-tag"}, self.entities[:2])
        self._check_filter({"tags": "first-tag,second-tag"}, [self.entities[1]])

    def test_filter_contributor(self):
        self._check_filter({"contributor": str(self.contributor.pk)}, self.entities[:2])
        self._check_filter(
            {"contributor__in": "{},{}".format(self.contributor.pk, self.user.pk)},
            self.entities,
        )

    def test_filter_owners(self):
        self._check_filter({"owners": str(self.admin.pk)}, self.entities[:2])
        self._check_filter({"owners": "999999"}, [])

    def test_filter_contributor_name(self):
        # Filter by first name.
        self._check_filter({"contributor_name": "Joe"}, self.entities[:2])
        # Filter by last name.
        self._check_filter({"contributor_name": "Miller"}, self.entities[:2])
        # Filter by username.
        self._check_filter({"contributor_name": "contributor"}, self.entities[:2])
        # Filter by combination of first and last name.
        self._check_filter({"contributor_name": "John Williams"}, [self.entities[2]])

    def test_filter_owners_name(self):
        # Filter by first name.
        self._check_filter({"owners_name": "James"}, self.entities[:2])
        # Filter by last name.
        self._check_filter({"owners_name": "Smith"}, self.entities[:2])
        # Filter by username.
        self._check_filter({"owners_name": "admin"}, self.entities[:2])
        # Filter by combination of first and last name.
        self._check_filter({"owners_name": "John Williams"}, [self.entities[2]])

    def test_filter_created(self):
        self._check_filter({"created": "2016-07-30T00:59:00"}, self.entities[:1])
        self._check_filter({"created__gt": "2016-07-30T01:59:00"}, self.entities[2:])
        self._check_filter({"created__gte": "2016-07-30T01:59:00"}, self.entities[1:])
        self._check_filter({"created__lt": "2016-07-30T01:59:00"}, self.entities[:1])
        self._check_filter({"created__lte": "2016-07-30T01:59:00"}, self.entities[:2])

    def test_filter_modified(self):
        self._check_filter(
            {"modified": self.entities[0].modified.isoformat()}, self.entities[:1]
        )
        self._check_filter(
            {"modified__gt": self.entities[1].modified.isoformat()}, self.entities[2:]
        )
        self._check_filter(
            {"modified__gte": self.entities[1].modified.isoformat()}, self.entities[1:]
        )
        self._check_filter(
            {"modified__lt": self.entities[1].modified.isoformat()}, self.entities[:1]
        )
        self._check_filter(
            {"modified__lte": self.entities[1].modified.isoformat()}, self.entities[:2]
        )

    def test_filter_collection(self):
        self._check_filter({"collection": str(self.collection1.pk)}, [self.entities[0]])
        self._check_filter(
            {
                "collection__in": "{},{}".format(
                    self.collection1.pk, self.collection2.pk
                )
            },
            self.entities[:2],
        )
        self._check_filter({"collection": "999999"}, [])
        self._check_filter(
            {"collection__in": "{},{}".format(self.collection1.pk, "999999")},
            self.entities[:1],
        )
        self._check_filter(
            {"collection__isnull": True}, self.entities[2:],
        )

    def test_filter_collection_name(self):
        self._check_filter({"collection__name": "My collection"}, [self.entities[0]])
        self._check_filter(
            {"collection__name__icontains": "Collection"}, self.entities[:2]
        )

    def test_filter_collection_slug(self):
        self._check_filter({"collection__slug": "my-collection"}, [self.entities[0]])
        self._check_filter(
            {"collection__slug__in": "my-collection,other-collection"},
            self.entities[:2],
        )

    def test_filter_text(self):
        # By name.
        self._check_filter({"text": "Test entity 1"}, [self.entities[1]])
        self._check_filter({"text": "Test"}, self.entities)
        self._check_filter({"text": "test"}, self.entities)
        self._check_filter({"text": "user"}, [self.entities[2]])

        # By contributor.
        self._check_filter({"text": "joe"}, self.entities[:2])
        self._check_filter({"text": "Miller"}, self.entities[:2])

        # By owner.
        self._check_filter({"text": "james"}, self.entities[:2])
        self._check_filter({"text": "Smith"}, self.entities[:2])

        # By description.
        self._check_filter(
            {"text": "description"}, [self.entities[0], self.entities[2]]
        )
        self._check_filter({"text": "my"}, self.entities[:2])
        self._check_filter({"text": "my description"}, [self.entities[0]])
        self._check_filter({"text": "user"}, [self.entities[2]])

        # By descriptor.
        self._check_filter({"text": "genialis"}, [self.entities[0]])
        self._check_filter({"text": "engineering"}, [self.entities[0]])

        # By mixed fields.
        self._check_filter({"text": "test joe"}, self.entities[:2])
        self._check_filter({"text": "joe my description"}, [self.entities[0]])

        # Check that changes are applied immediately.
        self.entities[0].name = "Awesome new name"
        self.entities[0].save()

        self._check_filter({"text": "awesome"}, [self.entities[0]])

    def test_filter_mixed(self):
        self._check_filter(
            {"name": "Test entity 0", "owners_name": "James Smith"}, [self.entities[0]]
        )
        self._check_filter(
            {"text": "entity 0", "owners_name": "James Smith"}, [self.entities[0]]
        )

    def test_order_by_relevance(self):
        result = self._check_filter({"text": "test entity"}, self.entities)
        self.assertEqual(result.data[0]["id"], self.entities[1].pk)

        # Make another entity more important.
        entity = self.entities[2]
        entity.description = "This is test entity. My test entity rocks."
        entity.save()

        result = self._check_filter({"text": "test entity"}, self.entities)
        self.assertEqual(result.data[0]["id"], self.entities[2].pk)

        # Check that ordering can be overriden.
        result = self._check_filter(
            {"text": "test entity", "ordering": "id"}, self.entities
        )
        self.assertEqual(result.data[0]["id"], self.entities[0].pk)


class DataViewSetFiltersTest(BaseViewSetFiltersTest):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        tzone = get_current_timezone()

        descriptor_schema = DescriptorSchema.objects.create(
            slug="test-schema",
            version="1.0.0",
            contributor=cls.contributor,
            schema=[
                {
                    "name": "company",
                    "group": [
                        {"name": "name", "type": "basic:string:", "required": False,},
                        {
                            "name": "departments",
                            "type": "list:basic:string:",
                            "required": False,
                        },
                    ],
                }
            ],
        )

        cls.collection1 = Collection.objects.create(
            name="My collection", slug="my-collection", contributor=cls.contributor,
        )

        cls.collection2 = Collection.objects.create(
            name="Other collection",
            slug="other-collection",
            contributor=cls.contributor,
        )

        cls.entity1 = Entity.objects.create(
            name="My entity",
            slug="my-entity",
            collection=cls.collection1,
            contributor=cls.contributor,
        )

        cls.entity2 = Entity.objects.create(
            name="Other entity",
            slug="other-entity",
            collection=cls.collection2,
            contributor=cls.contributor,
        )

        cls.proc1 = Process.objects.create(
            type="data:test:process1:",
            name="First process",
            slug="test-process-1",
            version="1.0.0",
            contributor=cls.contributor,
            input_schema=[
                {"name": "input_data", "type": "data:test:", "required": False}
            ],
        )

        cls.proc2 = Process.objects.create(
            type="data:test:process2:",
            name="Second process",
            slug="test-process-2",
            version="1.0.0",
            contributor=cls.contributor,
            input_schema=[
                {"name": "input_data", "type": "data:test:", "required": False}
            ],
        )

        cls.data = [
            Data.objects.create(
                name="Data 0",
                slug="dataslug-0",
                contributor=cls.contributor,
                collection=cls.collection1,
                entity=cls.entity1,
                process=cls.proc1,
                status=Data.STATUS_RESOLVING,
                started=datetime.datetime(2016, 7, 31, 0, 0, tzinfo=tzone),
                finished=datetime.datetime(2016, 7, 31, 0, 30, tzinfo=tzone),
                tags=["first-tag"],
                descriptor_schema=descriptor_schema,
                descriptor={
                    "company": {
                        "name": "Genialis",
                        "departments": ["engineering", "operations"],
                    }
                },
            ),
            Data.objects.create(
                name="Data 1",
                slug="dataslug-1",
                contributor=cls.contributor,
                collection=cls.collection2,
                entity=cls.entity2,
                process=cls.proc1,
                status=Data.STATUS_DONE,
                started=datetime.datetime(2016, 7, 31, 1, 0, tzinfo=tzone),
                finished=datetime.datetime(2016, 7, 31, 1, 30, tzinfo=tzone),
                tags=["first-tag", "second-tag", "data"],
            ),
            Data.objects.create(
                name="User data",
                slug="dataslug-2",
                contributor=cls.user,
                process=cls.proc2,
                status=Data.STATUS_DONE,
                started=datetime.datetime(2016, 7, 31, 2, 0, tzinfo=tzone),
                finished=datetime.datetime(2016, 7, 31, 2, 30, tzinfo=tzone),
                tags=["second-tag"],
            ),
        ]

        cls.data[0].created = datetime.datetime(2016, 7, 30, 0, 59, tzinfo=tzone)
        cls.data[0].save()
        cls.data[1].created = datetime.datetime(2016, 7, 30, 1, 59, tzinfo=tzone)
        cls.data[1].save()
        cls.data[2].created = datetime.datetime(2016, 7, 30, 2, 59, tzinfo=tzone)
        cls.data[2].save()

        assign_perm("owner_data", cls.admin, cls.data[0])
        assign_perm("owner_data", cls.admin, cls.data[1])
        assign_perm("owner_data", cls.user, cls.data[2])

        cls.viewset_class = DataViewSet

    def test_filter_id(self):
        self._check_filter({"id": str(self.data[0].pk)}, [self.data[0]])
        self._check_filter({"id": 420}, [])
        self._check_filter(
            {"id__in": "{},{}".format(self.data[0].pk, self.data[2].pk)},
            [self.data[0], self.data[2]],
        )

    def test_filter_slug(self):
        self._check_filter({"slug": "dataslug-1"}, [self.data[1]])
        self._check_filter(
            {"slug__in": "dataslug-1,dataslug-2"}, [self.data[1], self.data[2]]
        )

    def test_filter_name(self):
        self._check_filter({"name": "Data 1"}, [self.data[1]])
        self._check_filter({"name": "Data"}, [])

        self._check_filter({"name__iexact": "data 1"}, [self.data[1]])

        self._check_filter({"name__startswith": "Data"}, self.data[:2])
        self._check_filter({"name__istartswith": "data"}, self.data[:2])

        self._check_filter({"name__contains": "Data"}, self.data[:2])
        self._check_filter({"name__icontains": "Data"}, self.data)

    def test_filter_tags(self):
        self._check_filter({"tags": "first-tag"}, self.data[:2])
        self._check_filter({"tags": "first-tag,second-tag"}, [self.data[1]])

    def test_filter_contributor(self):
        self._check_filter({"contributor": str(self.contributor.pk)}, self.data[:2])
        self._check_filter(
            {"contributor__in": "{},{}".format(self.contributor.pk, self.user.pk)},
            self.data,
        )

    def test_filter_owners(self):
        self._check_filter({"owners": str(self.admin.pk)}, self.data[:2])
        self._check_filter({"owners": "999999"}, [])

    def test_filter_contributor_name(self):
        # Filter by first name.
        self._check_filter({"contributor_name": "Joe"}, self.data[:2])
        # Filter by last name.
        self._check_filter({"contributor_name": "Miller"}, self.data[:2])
        # Filter by username.
        self._check_filter({"contributor_name": "contributor"}, self.data[:2])
        # Filter by combination of first and last name.
        self._check_filter({"contributor_name": "John Williams"}, [self.data[2]])

    def test_filter_owners_name(self):
        # Filter by first name.
        self._check_filter({"owners_name": "James"}, self.data[:2])
        # Filter by last name.
        self._check_filter({"owners_name": "Smith"}, self.data[:2])
        # Filter by username.
        self._check_filter({"owners_name": "admin"}, self.data[:2])
        # Filter by combination of first and last name.
        self._check_filter({"owners_name": "John Williams"}, [self.data[2]])

    def test_filter_created(self):
        self._check_filter({"created": "2016-07-30T00:59:00"}, [self.data[0]])
        self._check_filter({"created__gt": "2016-07-30T01:59:00"}, self.data[2:])
        self._check_filter({"created__gte": "2016-07-30T01:59:00"}, self.data[1:])
        self._check_filter({"created__lt": "2016-07-30T01:59:00"}, self.data[:1])
        self._check_filter({"created__lte": "2016-07-30T01:59:00"}, self.data[:2])

    def test_filter_modified(self):
        self._check_filter(
            {"modified": self.data[0].modified.isoformat()}, [self.data[0]]
        )
        self._check_filter(
            {"modified__gt": self.data[1].modified.isoformat()}, self.data[2:]
        )
        self._check_filter(
            {"modified__gte": self.data[1].modified.isoformat()}, self.data[1:]
        )
        self._check_filter(
            {"modified__lt": self.data[1].modified.isoformat()}, self.data[:1]
        )
        self._check_filter(
            {"modified__lte": self.data[1].modified.isoformat()}, self.data[:2]
        )

    def test_filter_started(self):
        self._check_filter({"started": "2016-07-31T00:00:00"}, [self.data[0]])
        self._check_filter({"started__gt": "2016-07-31T01:00:00"}, self.data[2:])
        self._check_filter({"started__gte": "2016-07-31T01:00:00"}, self.data[1:])
        self._check_filter({"started__lt": "2016-07-31T01:00:00"}, self.data[:1])
        self._check_filter({"started__lte": "2016-07-31T01:00:00"}, self.data[:2])

    def test_filter_finished(self):
        self._check_filter({"finished": "2016-07-31T00:30:00"}, [self.data[0]])
        self._check_filter({"finished__gt": "2016-07-31T01:30:00"}, self.data[2:])
        self._check_filter({"finished__gte": "2016-07-31T01:30:00"}, self.data[1:])
        self._check_filter({"finished__lt": "2016-07-31T01:30:00"}, self.data[:1])
        self._check_filter({"finished__lte": "2016-07-31T01:30:00"}, self.data[:2])

    def test_filter_collection(self):
        self._check_filter({"collection": str(self.collection1.pk)}, [self.data[0]])
        self._check_filter(
            {
                "collection__in": "{},{}".format(
                    self.collection1.pk, self.collection2.pk
                )
            },
            self.data[:2],
        )
        self._check_filter({"collection": "999999"}, [])
        self._check_filter(
            {"collection__in": "{},{}".format(self.collection1.pk, "999999")},
            self.data[:1],
        )
        self._check_filter(
            {"collection__isnull": True}, self.data[2:],
        )

    def test_filter_collection_name(self):
        self._check_filter({"collection__name": "My collection"}, [self.data[0]])
        self._check_filter({"collection__name__icontains": "Collection"}, self.data[:2])

    def test_filter_collection_slug(self):
        self._check_filter({"collection__slug": "my-collection"}, [self.data[0]])
        self._check_filter(
            {"collection__slug__in": "my-collection,other-collection"}, self.data[:2]
        )

    def test_filter_entity(self):
        self._check_filter({"entity": str(self.entity1.pk)}, [self.data[0]])
        self._check_filter(
            {"entity__in": "{},{}".format(self.entity1.pk, self.entity2.pk)},
            self.data[:2],
        )
        self._check_filter({"entity": "999999"}, [])
        self._check_filter(
            {"entity__in": "{},{}".format(self.entity1.pk, "999999")}, [self.data[0]],
        )
        self._check_filter(
            {"entity__in": "{},{}".format(self.entity1.pk, self.entity2.pk, "999999")},
            self.data[:2],
        )
        self._check_filter(
            {"entity__isnull": True}, self.data[2:],
        )

    def test_filter_entity_name(self):
        self._check_filter({"entity__name": "My entity"}, [self.data[0]])
        self._check_filter({"entity__name__icontains": "Entity"}, self.data[:2])

    def test_filter_entity_slug(self):
        self._check_filter({"entity__slug": "my-entity"}, [self.data[0]])
        self._check_filter(
            {"entity__slug__in": "my-entity,other-entity"}, self.data[:2]
        )

    def test_filter_type(self):
        self._check_filter({"type": "data:"}, self.data)
        self._check_filter({"type": "data:test:"}, self.data)
        self._check_filter({"type": "data:test:process1"}, self.data[:2])
        self._check_filter({"type": "data:test:process1:"}, self.data[:2])
        self._check_filter({"type__exact": "data:test"}, [])
        self._check_filter({"type__exact": "data:test:process1:"}, self.data[:2])

    def test_filter_status(self):
        self._check_filter({"status": "OK"}, self.data[1:])
        self._check_filter({"status": "RE"}, [self.data[0]])
        self._check_filter({"status__in": "OK,RE"}, self.data)

    def test_filter_process(self):
        self._check_filter({"process": str(self.proc1.pk)}, self.data[:2])
        self._check_filter({"process": str(self.proc2.pk)}, self.data[2:])
        self._check_filter({"process": "999999"}, [])

    def test_filter_process_name(self):
        self._check_filter({"process__name": "First process"}, self.data[:2])
        self._check_filter({"process__name__iexact": "first process"}, self.data[:2])
        self._check_filter({"process__name__startswith": "First"}, self.data[:2])
        self._check_filter({"process__name": "first process"}, [])

    def test_filter_process_slug(self):
        self._check_filter({"process__slug": "test-process-1"}, self.data[:2])
        self._check_filter({"process__slug": "test-process-2"}, self.data[2:])
        self._check_filter({"process__slug": "test-process"}, [])

    def test_filter_text(self):
        # By name.
        self._check_filter({"text": "Data 1"}, [self.data[1]])
        self._check_filter({"text": "Data"}, self.data)
        self._check_filter({"text": "data"}, self.data)

        # By contributor.
        self._check_filter({"text": "joe"}, self.data[:2])
        self._check_filter({"text": "Miller"}, self.data[:2])

        # By owner.
        self._check_filter({"text": "james"}, self.data[:2])
        self._check_filter({"text": "Smith"}, self.data[:2])

        # By process name.
        self._check_filter({"text": "first"}, self.data[:2])
        self._check_filter({"text": "process"}, self.data)

        # By descriptor.
        self._check_filter({"text": "genialis"}, [self.data[0]])
        self._check_filter({"text": "engineering"}, [self.data[0]])

        # By mixed fields.
        self._check_filter({"text": "data joe"}, self.data[:2])
        self._check_filter({"text": "joe first"}, self.data[:2])

        # Check that changes are applied immediately.
        self.data[0].name = "Awesome new name"
        self.data[0].save()

        self._check_filter({"text": "awesome"}, [self.data[0]])

    def test_filter_mixed(self):
        self._check_filter(
            {"name": "Data 0", "owners_name": "James Smith"}, [self.data[0]]
        )
        self._check_filter(
            {"text": "Data 0", "owners_name": "James Smith"}, [self.data[0]]
        )

    def test_order_by_relevance(self):
        result = self._check_filter({"text": "data"}, self.data)
        self.assertEqual(result.data[0]["id"], self.data[1].pk)

        # Make another entity more important.
        data = self.data[2]
        data.name = "Data data data"
        data.save()

        result = self._check_filter({"text": "data"}, self.data)
        self.assertEqual(result.data[0]["id"], self.data[2].pk)

        # Check that ordering can be overriden.
        result = self._check_filter({"text": "data", "ordering": "id"}, self.data)
        self.assertEqual(result.data[0]["id"], self.data[0].pk)

    def test_nonexisting_parameter(self):
        response = self._check_filter({"foo": "bar"}, [], expected_status_code=400)

        self.assertRegex(
            str(response.data["__all__"]),
            r"Unsupported parameter\(s\): foo. Please use a combination of: .*",
        )


class DescriptorSchemaViewSetFiltersTest(BaseViewSetFiltersTest):
    def setUp(self):
        self.viewset_class = DescriptorSchemaViewSet
        super().setUp()

        tzone = get_current_timezone()

        self.ds1 = DescriptorSchema.objects.create(
            contributor=self.user, slug="slug1", name="ds1"
        )
        self.ds1.created = datetime.datetime(2015, 7, 28, 11, 57, tzinfo=tzone)
        self.ds1.save()

        self.ds2 = DescriptorSchema.objects.create(
            contributor=self.user, slug="slug2", name="ds2"
        )
        self.ds2.created = datetime.datetime(2016, 8, 29, 12, 58, 0, tzinfo=tzone)
        self.ds2.save()

        self.ds3 = DescriptorSchema.objects.create(
            contributor=self.admin, slug="slug3", name="ds3"
        )
        self.ds3.created = datetime.datetime(2017, 9, 30, 13, 59, 0, tzinfo=tzone)
        self.ds3.save()

    def test_id(self):
        self._check_filter({"id": self.ds1.pk}, [self.ds1])
        self._check_filter({"id": self.ds2.pk}, [self.ds2])
        self._check_filter({"id__in": self.ds2.pk}, [self.ds2])
        self._check_filter(
            {"id__in": "{},{}".format(self.ds1.pk, self.ds2.pk)}, [self.ds1, self.ds2]
        )
        self._check_filter({"id__gt": self.ds1.pk}, [self.ds2, self.ds3])

    def test_slug(self):
        self._check_filter({"slug": "slug1"}, [self.ds1])
        self._check_filter({"slug": "slug2"}, [self.ds2])
        self._check_filter({"slug__in": "slug"}, [])
        self._check_filter({"slug__in": "slug2,slug"}, [self.ds2])

    def test_name(self):
        self._check_filter({"name": "ds1"}, [self.ds1])
        self._check_filter({"name": "ds2"}, [self.ds2])
        self._check_filter({"name__startswith": "ds"}, [self.ds1, self.ds2, self.ds3])
        self._check_filter({"name__startswith": "ds2"}, [self.ds2])
        self._check_filter({"name__startswith": "dsx"}, [])
        self._check_filter({"name__icontains": "2"}, [self.ds2])
        self._check_filter({"name__icontains": "DS"}, [self.ds1, self.ds2, self.ds3])

    def test_contributor(self):
        self._check_filter({"contributor": self.user.pk}, [self.ds1, self.ds2])
        self._check_filter({"contributor": self.admin.pk}, [self.ds3])
        self._check_filter({"contributor": "999999"}, [])
        self._check_filter({"contributor__in": self.admin.pk}, [self.ds3])
        self._check_filter(
            {"contributor__in": "999999,{}".format(self.admin.pk)}, [self.ds3]
        )
        self._check_filter(
            {"contributor__in": "{},{}".format(self.user.pk, self.admin.pk)},
            [self.ds1, self.ds2, self.ds3],
        )

    def test_created(self):
        self._check_filter({"created": "2015-07-28T11:57:00"}, [self.ds1])
        self._check_filter({"created": "2016-08-29T12:58:00.000000"}, [self.ds2])
        self._check_filter({"created": "2017-09-30T13:59:00.000000Z"}, [self.ds3])
        self._check_filter({"created": "2017-09-30T13:59:00.000000+0000"}, [self.ds3])
        self._check_filter({"created": "2017-09-30T13:59:00.000000+0000"}, [self.ds3])

        self._check_filter({"created__date": "2016-08-29"}, [self.ds2])
        self._check_filter({"created__time": "12:58:00"}, [self.ds2])

    def test_modified(self):
        now = self.ds1.modified
        self._check_filter(
            {"modified": now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")}, [self.ds1]
        )

    def test_nonexisting_parameter(self):
        response = self._check_filter(
            {"foo": "bar"}, [self.ds1], expected_status_code=400
        )

        self.assertRegex(
            str(response.data["__all__"][0]),
            r"Unsupported parameter\(s\): foo. Please use a combination of: .*",
        )


class ProcessViewSetFiltersTest(BaseViewSetFiltersTest):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        cls.proc_1 = Process.objects.create(
            contributor=cls.contributor,
            type="data:alignment:bam:",
            category="analyses:alignment:",
            scheduling_class="BA",
        )

        cls.proc_2 = Process.objects.create(
            contributor=cls.contributor,
            type="data:expression:",
            category="analyses:",
            scheduling_class="IN",
        )

        cls.viewset_class = ProcessViewSet

    def test_category(self):
        self._check_filter({"category": "analyses:"}, [self.proc_1, self.proc_2])
        self._check_filter({"category": "analyses:alignment"}, [self.proc_1])

    def test_type(self):
        self._check_filter({"type": "data:"}, [self.proc_1, self.proc_2])
        self._check_filter({"type": "data:alignment:bam"}, [self.proc_1])
        self._check_filter({"type": "data:expression"}, [self.proc_2])

    def test_scheduling_class(self):
        self._check_filter({"scheduling_class": "BA"}, [self.proc_1])
        self._check_filter({"scheduling_class": "IN"}, [self.proc_2])

    def test_is_active(self):
        self._check_filter({"is_active": "true"}, [self.proc_1, self.proc_2])
        self._check_filter({"is_active": "false"}, [])

        self.proc_1.is_active = False
        self.proc_1.save()

        self._check_filter({"is_active": "true"}, [self.proc_2])
        self._check_filter({"is_active": "false"}, [self.proc_1])
