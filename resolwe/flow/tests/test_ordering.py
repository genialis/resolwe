# pylint: disable=missing-docstring
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.urls import reverse

from guardian.shortcuts import assign_perm
from rest_framework.test import APITestCase

from resolwe.flow.models import Collection, Data, Entity, Process


class ProcessOrderingTest(APITestCase):
    def setUp(self):
        super().setUp()

        user_model = get_user_model()
        user = user_model.objects.create(username="user")

        self.proc_1 = Process.objects.create(
            name="My process", contributor=user, version=1
        )
        self.proc_2 = Process.objects.create(
            name="My process", contributor=user, version=2
        )

        assign_perm("view_process", AnonymousUser(), self.proc_1)
        assign_perm("view_process", AnonymousUser(), self.proc_2)

        self.url = reverse("resolwe-api:process-list")

    def test_ordering_version(self):
        response = self.client.get(self.url, {"ordering": "version"}, format="json")
        self.assertEqual(response.data[0]["id"], self.proc_1.id)
        self.assertEqual(response.data[1]["id"], self.proc_2.id)

        response = self.client.get(self.url, {"ordering": "-version"}, format="json")
        self.assertEqual(response.data[0]["id"], self.proc_2.id)
        self.assertEqual(response.data[1]["id"], self.proc_1.id)


class CollectionOrderingTest(APITestCase):
    def setUp(self):
        super().setUp()

        user_model = get_user_model()
        self.user_1 = user_model.objects.create(
            username="user_1", first_name="Zion", last_name="Zucchini"
        )
        self.user_2 = user_model.objects.create(
            username="user_2", first_name="Zack", last_name="Zucchini"
        )
        self.user_3 = user_model.objects.create(
            username="user_3", first_name="Adam", last_name="Angus"
        )

        self.collection_1 = Collection.objects.create(
            name="Collection 1", contributor=self.user_1
        )
        self.collection_2 = Collection.objects.create(
            name="Collection 2", contributor=self.user_2
        )
        self.collection_3 = Collection.objects.create(
            name="Collection 3", contributor=self.user_3
        )

        assign_perm("view_collection", AnonymousUser(), self.collection_1)
        assign_perm("view_collection", AnonymousUser(), self.collection_2)
        assign_perm("view_collection", AnonymousUser(), self.collection_3)

        self.url = reverse("resolwe-api:collection-list")

    def test_ordering(self):
        response = self.client.get(
            self.url, {"ordering": "contributor__last_name,contributor__first_name"}
        )
        self.assertEqual(
            response.data[0]["contributor"]["first_name"], self.user_3.first_name
        )
        self.assertEqual(
            response.data[1]["contributor"]["first_name"], self.user_2.first_name
        )
        self.assertEqual(
            response.data[2]["contributor"]["first_name"], self.user_1.first_name
        )

        response = self.client.get(
            self.url, {"ordering": "-contributor__last_name,-contributor__first_name"}
        )
        self.assertEqual(
            response.data[0]["contributor"]["first_name"], self.user_1.first_name
        )
        self.assertEqual(
            response.data[1]["contributor"]["first_name"], self.user_2.first_name
        )
        self.assertEqual(
            response.data[2]["contributor"]["first_name"], self.user_3.first_name
        )


class EntityOrderingTest(APITestCase):
    def setUp(self):
        super().setUp()

        user_model = get_user_model()
        self.user_1 = user_model.objects.create(
            username="user_1", first_name="Zion", last_name="Zucchini"
        )
        self.user_2 = user_model.objects.create(
            username="user_2", first_name="Zack", last_name="Zucchini"
        )
        self.user_3 = user_model.objects.create(
            username="user_3", first_name="Adam", last_name="Angus"
        )

        self.entity_1 = Entity.objects.create(name="Entity 1", contributor=self.user_1)
        self.entity_2 = Entity.objects.create(name="Entity 2", contributor=self.user_2)
        self.entity_3 = Entity.objects.create(name="Entity 3", contributor=self.user_3)

        assign_perm("view_entity", AnonymousUser(), self.entity_1)
        assign_perm("view_entity", AnonymousUser(), self.entity_2)
        assign_perm("view_entity", AnonymousUser(), self.entity_3)

        self.url = reverse("resolwe-api:entity-list")

    def test_ordering(self):
        response = self.client.get(
            self.url, {"ordering": "contributor__last_name,contributor__first_name"}
        )
        self.assertEqual(
            response.data[0]["contributor"]["first_name"], self.user_3.first_name
        )
        self.assertEqual(
            response.data[1]["contributor"]["first_name"], self.user_2.first_name
        )
        self.assertEqual(
            response.data[2]["contributor"]["first_name"], self.user_1.first_name
        )

        response = self.client.get(
            self.url, {"ordering": "-contributor__last_name,-contributor__first_name"}
        )
        self.assertEqual(
            response.data[0]["contributor"]["first_name"], self.user_1.first_name
        )
        self.assertEqual(
            response.data[1]["contributor"]["first_name"], self.user_2.first_name
        )
        self.assertEqual(
            response.data[2]["contributor"]["first_name"], self.user_3.first_name
        )


class DataOrderingTest(APITestCase):
    def setUp(self):
        super().setUp()

        user_model = get_user_model()
        self.user_1 = user_model.objects.create(
            username="user_1", first_name="Zion", last_name="Zucchini"
        )
        self.user_2 = user_model.objects.create(
            username="user_2", first_name="Zack", last_name="Zucchini"
        )
        self.user_3 = user_model.objects.create(
            username="user_3", first_name="Adam", last_name="Angus"
        )

        process = Process.objects.create(name="My process", contributor=self.user_1)
        self.data_1 = Data.objects.create(
            name="Data 1", contributor=self.user_1, process=process
        )
        self.data_2 = Data.objects.create(
            name="Data 2", contributor=self.user_2, process=process
        )
        self.data_3 = Data.objects.create(
            name="Data 3", contributor=self.user_3, process=process
        )

        assign_perm("view_data", AnonymousUser(), self.data_1)
        assign_perm("view_data", AnonymousUser(), self.data_2)
        assign_perm("view_data", AnonymousUser(), self.data_3)

        self.url = reverse("resolwe-api:data-list")

    def test_ordering(self):
        response = self.client.get(
            self.url, {"ordering": "contributor__last_name,contributor__first_name"}
        )
        self.assertEqual(
            response.data[0]["contributor"]["first_name"], self.user_3.first_name
        )
        self.assertEqual(
            response.data[1]["contributor"]["first_name"], self.user_2.first_name
        )
        self.assertEqual(
            response.data[2]["contributor"]["first_name"], self.user_1.first_name
        )

        response = self.client.get(
            self.url, {"ordering": "-contributor__last_name,-contributor__first_name"}
        )
        self.assertEqual(
            response.data[0]["contributor"]["first_name"], self.user_1.first_name
        )
        self.assertEqual(
            response.data[1]["contributor"]["first_name"], self.user_2.first_name
        )
        self.assertEqual(
            response.data[2]["contributor"]["first_name"], self.user_3.first_name
        )
