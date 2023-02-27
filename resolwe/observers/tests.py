# pylint: disable=missing-docstring

import asyncio
import json
import uuid

import async_timeout
from channels.db import database_sync_to_async
from channels.routing import URLRouter
from channels.testing import WebsocketCommunicator

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TransactionTestCase, override_settings
from django.urls import path

from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Collection, Data, Entity, Process
from resolwe.flow.views import DataViewSet, EntityViewSet
from resolwe.permissions.models import Permission
from resolwe.permissions.utils import get_anonymous_user
from resolwe.test import TransactionResolweAPITestCase

from .consumers import ClientConsumer
from .models import Observer, Subscription
from .protocol import ChangeType


# If FLOW_MANAGER_DISABLE_AUTO_CALLS is False, Data objects will receive
# an UPDATE signal before the CREATE signal.
@override_settings(FLOW_MANAGER_DISABLE_AUTO_CALLS=True)
class ObserverTestCase(TransactionTestCase):
    def setUp(self):
        super().setUp()

        self.user_alice = get_user_model().objects.create(
            username="alice",
            email="alice@test.com",
            first_name="Ana",
            last_name="Banana",
        )
        self.user_bob = get_user_model().objects.create(
            username="capital-bob",
            email="bob@bob.bob",
            first_name="Capital",
            last_name="Bobnik",
        )
        self.process = Process.objects.create(
            name="Dummy process", contributor=self.user_alice
        )

        self.client_consumer = URLRouter(
            [path("ws/<slug:session_id>", ClientConsumer().as_asgi())]
        )
        self.subscription_id = uuid.UUID(int=0)
        self.subscription_id2 = uuid.UUID(int=1)

    async def assert_no_more_messages(self, client):
        """Assert there are no messages queued by a websocket client."""
        with self.assertRaises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.01):
                raise ValueError("Unexpected message:", await client.receive_from())

    async def await_subscription_observer_count(self, count):
        """Wait until the number of all subscriptions is equal to count."""

        @database_sync_to_async
        def get_subscription_count():
            subs = Subscription.objects.filter(session_id="test_session")
            total = 0
            for sub in subs:
                total += sub.observers.count()
            return total

        try:
            async with async_timeout.timeout(1):
                while await get_subscription_count() != count:
                    await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            raise ValueError(
                "Expecting subscription-observer count to be {count}, but it's {actual}".format(
                    count=count, actual=await get_subscription_count()
                )
            )

    async def await_object_count(self, object, count):
        """Wait until the number of all observers is equal to count."""

        @database_sync_to_async
        def get_count():
            return object.objects.count()

        try:
            async with async_timeout.timeout(1):
                while await get_count() != count:
                    await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            raise ValueError(
                "Expecting {obj} count to be {count}, but it's {actual}".format(
                    obj=object.__name__, count=count, actual=await get_count()
                )
            )

    async def websocket_subscribe_unsubscribe(self, Object, create_obj):
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        connected, _ = await client.connect()
        self.assertTrue(connected)
        await self.await_subscription_observer_count(0)

        # Subscribe to C/D and U separately.
        @database_sync_to_async
        def subscribe():
            content_type = ContentType.objects.get_for_model(Object)
            Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id,
            ).subscribe(
                content_type=content_type,
                object_ids=[43],
                change_types=[ChangeType.CREATE, ChangeType.DELETE],
            )
            Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id2,
            ).subscribe(
                content_type=content_type,
                object_ids=[43],
                change_types=[ChangeType.UPDATE],
            )
            return content_type

        content_type = await subscribe()

        await self.await_object_count(Subscription, 2)
        await self.await_object_count(Observer, 3)
        await self.await_subscription_observer_count(3)

        # Create an object.
        obj = await create_obj(pk=43)
        await database_sync_to_async(obj.set_permission)(
            Permission.OWNER, self.user_alice
        )

        # Assert we detect creations.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "object_id": 43,
                "change_type": ChangeType.CREATE.name,
                "subscription_id": self.subscription_id.hex,
                "source": [content_type.name, 43],
            },
        )

        # Assert we detect modifications.
        obj.name = "name2"
        await database_sync_to_async(obj.save)()
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "object_id": 43,
                "change_type": ChangeType.UPDATE.name,
                "subscription_id": self.subscription_id2.hex,
                "source": [content_type.name, 43],
            },
        )

        # Unsubscribe from updates.
        @database_sync_to_async
        def unsubscribe():
            Subscription.objects.get(
                observers__object_id=43, observers__change_type=ChangeType.UPDATE.value
            ).delete()

        await unsubscribe()

        await self.await_object_count(Subscription, 1)
        # Update observer should be deleted because no one is subscribed to it.
        await self.await_object_count(Observer, 2)
        await self.await_subscription_observer_count(2)

        # Assert we don't detect updates anymore.
        obj.name = "name2"
        await database_sync_to_async(obj.save)()
        await self.assert_no_more_messages(client)

        # Assert we detect deletions.
        await database_sync_to_async(obj.delete)()
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "object_id": 43,
                "change_type": ChangeType.DELETE.name,
                "subscription_id": self.subscription_id.hex,
                "source": [content_type.name, 43],
            },
        )

        await client.disconnect()

    async def test_websocket_subscribe_unsubscribe_collection(self):
        @database_sync_to_async
        def create_collection(pk):
            return Collection.objects.create(
                pk=pk,
                name="Test collection",
                slug="test-collection",
                contributor=self.user_alice,
            )

        await self.websocket_subscribe_unsubscribe(Collection, create_collection)

    async def test_websocket_subscribe_unsubscribe_data(self):
        @database_sync_to_async
        def create_data(pk):
            data = Data.objects.create(
                pk=pk,
                name="Test data",
                slug="test-data",
                contributor=self.user_alice,
                process=self.process,
                size=0,
            )
            return data

        await self.websocket_subscribe_unsubscribe(Data, create_data)

    async def test_websocket_subscribe_unsubscribe_entity(self):
        @database_sync_to_async
        def create_entity(pk):
            return Entity.objects.create(
                pk=pk,
                name="Test entity",
                slug="test-entity",
                contributor=self.user_alice,
            )

        await self.websocket_subscribe_unsubscribe(Entity, create_entity)

    async def test_remove_observers_after_socket_close(self):
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        client.scope["user"] = self.user_alice
        connected, _ = await client.connect()
        self.assertTrue(connected)

        @database_sync_to_async
        def subscribe():
            Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id,
            ).subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[42],
                change_types=[ChangeType.UPDATE],
            )

        await subscribe()
        await self.await_subscription_observer_count(1)
        await client.disconnect()
        await self.await_subscription_observer_count(0)

    async def test_observe_containers_update(self):
        """Test containers are also observed when object in them changes."""
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        connected, _ = await client.connect()
        self.assertTrue(connected)

        @database_sync_to_async
        def create_entity_collection():
            collection = Collection.objects.create(
                pk=40, contributor=self.user_alice, name="test collection"
            )
            entity = Entity.objects.create(
                pk=41,
                contributor=self.user_alice,
                name="test entity",
                collection=collection,
            )
            data = Data.objects.create(
                pk=42,
                name="Test data",
                slug="test-data",
                contributor=self.user_alice,
                process=self.process,
                size=0,
                collection=collection,
                entity=entity,
            )
            collection.set_permission(Permission.OWNER, self.user_alice)
            return collection, entity, data

        # Create collection, entity and data before subscribing.
        collection, entity, data = await create_entity_collection()

        # Create a subscription to the Data, Entity and Collection content_type.
        @database_sync_to_async
        def subscribe():
            subscription = Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id,
            )

            subscription.subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[data.id],
                change_types=[ChangeType.UPDATE],
            )
            subscription.subscribe(
                content_type=ContentType.objects.get_for_model(Entity),
                object_ids=[entity.id],
                change_types=[ChangeType.UPDATE],
            )
            subscription.subscribe(
                content_type=ContentType.objects.get_for_model(Collection),
                object_ids=[collection.id],
                change_types=[ChangeType.UPDATE],
            )

        await subscribe()
        await self.await_subscription_observer_count(3)

        # Create a new Data object in the collection and entity.
        @database_sync_to_async
        def update_data():
            data.status = Data.STATUS_DONE
            data.save()

        await update_data()

        updates = [json.loads(await client.receive_from()) for _ in range(3)]
        self.assertCountEqual(
            updates,
            [
                {
                    "change_type": ChangeType.UPDATE.name,
                    "object_id": 40,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["data", 42],
                },
                {
                    "change_type": ChangeType.UPDATE.name,
                    "object_id": 41,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["data", 42],
                },
                {
                    "change_type": ChangeType.UPDATE.name,
                    "object_id": 42,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["data", 42],
                },
            ],
        )
        await self.assert_no_more_messages(client)

        @database_sync_to_async
        def update_entity():
            entity.name = "New entity name"
            entity.save()

        await update_entity()
        updates = [json.loads(await client.receive_from()) for _ in range(2)]
        self.assertCountEqual(
            updates,
            [
                {
                    "change_type": ChangeType.UPDATE.name,
                    "object_id": 40,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["entity", 41],
                },
                {
                    "change_type": ChangeType.UPDATE.name,
                    "object_id": 41,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["entity", 41],
                },
            ],
        )
        await self.assert_no_more_messages(client)

    async def test_observe_containers_create(self):
        """Test containers are also observed when object in them changes."""
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        connected, _ = await client.connect()
        self.assertTrue(connected)

        @database_sync_to_async
        def create_collection():
            collection = Collection.objects.create(
                pk=40, contributor=self.user_alice, name="test collection"
            )
            collection.set_permission(Permission.OWNER, self.user_alice)
            return collection

        # Create collection before subscribing.
        collection = await create_collection()

        # Create a subscription to Collection object.
        @database_sync_to_async
        def subscribe():
            subscription = Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id,
            )
            subscription.subscribe(
                content_type=ContentType.objects.get_for_model(Collection),
                object_ids=[collection.id],
                change_types=[ChangeType.CREATE],
            )

        await subscribe()
        await self.await_subscription_observer_count(1)

        # Create a new Entity object in the collection.
        @database_sync_to_async
        def create_entity():
            factory = APIRequestFactory()
            entity = {
                "name": "Entity name",
                "collection": {"id": collection.id},
            }
            request = factory.post("/", entity, format="json")
            force_authenticate(request, self.user_alice)
            viewset = EntityViewSet.as_view(actions={"post": "create"})
            response = viewset(request)
            return Entity.objects.get(pk=response.data["id"])

        entity = await create_entity()
        update = json.loads(await client.receive_from())
        self.assertDictEqual(
            update,
            {
                "change_type": ChangeType.CREATE.name,
                "object_id": collection.id,
                "subscription_id": self.subscription_id.hex,
                "source": ["entity", entity.id],
            },
        )
        await self.assert_no_more_messages(client)

        # Create a subscription to entity.
        @database_sync_to_async
        def subscribe_entity():
            entity = Entity.objects.get()

            subscription = Subscription.objects.get(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id,
            )
            subscription.subscribe(
                content_type=ContentType.objects.get_for_model(Entity),
                object_ids=[entity.id],
                change_types=[ChangeType.CREATE],
            )

        await subscribe_entity()

        @database_sync_to_async
        def create_data():
            self.process.set_permission(Permission.OWNER, self.user_alice)
            factory = APIRequestFactory()
            data = {
                "process": {"slug": self.process.slug},
                "collection": {"id": collection.id},
                "entity": {"id": entity.id},
            }
            request = factory.post("/", data, format="json")
            force_authenticate(request, self.user_alice)
            viewset = DataViewSet.as_view(actions={"post": "create"})
            response = viewset(request)
            return response.data["id"]

        data_id = await create_data()
        updates = [json.loads(await client.receive_from()) for _ in range(2)]
        self.assertCountEqual(
            updates,
            [
                {
                    "change_type": ChangeType.CREATE.name,
                    "object_id": collection.id,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["data", data_id],
                },
                {
                    "change_type": ChangeType.CREATE.name,
                    "object_id": entity.id,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["data", data_id],
                },
            ],
        )
        await self.assert_no_more_messages(client)

    async def test_observe_table(self):
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        connected, _ = await client.connect()
        self.assertTrue(connected)

        # Create a subscription to the Data content_type.
        @database_sync_to_async
        def subscribe():
            Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session",
                subscription_id=self.subscription_id,
            ).subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[None],
                change_types=[ChangeType.CREATE, ChangeType.DELETE],
            )

        await subscribe()
        await self.await_subscription_observer_count(2)

        # Create a new Data object.
        @database_sync_to_async
        def create_data():
            return Data.objects.create(
                pk=42,
                name="Test data",
                slug="test-data",
                contributor=self.user_alice,
                process=self.process,
                size=0,
            )

        # Create a new Data object in the collection using the API call.
        @database_sync_to_async
        def create_data_in_collection():
            collection = Collection.objects.create(
                contributor=self.user_alice, name="test collection"
            )
            collection.set_permission(Permission.OWNER, self.user_alice)
            self.process.set_permission(Permission.OWNER, self.user_alice)

            factory = APIRequestFactory()
            data = {
                "process": {"slug": self.process.slug},
                "collection": {"id": collection.id},
            }
            request = factory.post("/", data, format="json")
            force_authenticate(request, self.user_alice)
            viewset = DataViewSet.as_view(actions={"post": "create"})
            response = viewset(request)
            return response.data["id"]

        @database_sync_to_async
        def create_data_in_collection_superuser():
            """Test observing works for superusers without explicit permissions."""
            self.user_alice.is_superuser = True
            self.user_alice.save()
            collection = Collection.objects.create(
                contributor=self.user_alice, name="test collection"
            )
            factory = APIRequestFactory()
            data = {
                "process": {"slug": self.process.slug},
                "collection": {"id": collection.id},
            }
            request = factory.post("/", data, format="json")
            force_authenticate(request, self.user_alice)
            viewset = DataViewSet.as_view(actions={"post": "create"})
            response = viewset(request)
            self.user_alice.is_superuser = False
            self.user_alice.save()
            return response.data["id"]

        data = await create_data()

        # Assert we detect creations.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "change_type": ChangeType.CREATE.name,
                "object_id": 42,
                "subscription_id": self.subscription_id.hex,
                "source": ["data", 42],
            },
        )
        await self.assert_no_more_messages(client)

        # Repeat the test with data object in collection. This asserts that the signal
        # post_permission_changed can be triggered without the previous call to the
        # pre_permission_changed  signal and user is notified when object is created in
        # container using API.
        data_id = await create_data_in_collection()

        # Assert we detect creations.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "change_type": ChangeType.CREATE.name,
                "object_id": data_id,
                "subscription_id": self.subscription_id.hex,
                "source": ["data", data_id],
            },
        )
        await self.assert_no_more_messages(client)

        # Repeat the test above for superuser without explicit permissions.
        data_id = await create_data_in_collection_superuser()

        # Assert we detect creations.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "change_type": ChangeType.CREATE.name,
                "object_id": data_id,
                "subscription_id": self.subscription_id.hex,
                "source": ["data", data_id],
            },
        )
        await self.assert_no_more_messages(client)

        # Delete the Data object.
        @database_sync_to_async
        def delete_data(data):
            data.delete()

        await delete_data(data)

        # Assert we detect deletions.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "change_type": ChangeType.DELETE.name,
                "object_id": 42,
                "subscription_id": self.subscription_id.hex,
                "source": ["data", 42],
            },
        )
        await self.assert_no_more_messages(client)

        # Assert subscription didn't delete because Data got deleted.
        await self.await_subscription_observer_count(2)

    async def test_change_permission_group(self):
        client_bob = WebsocketCommunicator(self.client_consumer, "/ws/test_session_bob")
        connected_bob, _ = await client_bob.connect()
        self.assertTrue(connected_bob)

        client_alice = WebsocketCommunicator(
            self.client_consumer, "/ws/test_session_alice"
        )
        connected_alice, _ = await client_alice.connect()
        self.assertTrue(connected_alice)

        # Create a Data object visible to Bob.
        @database_sync_to_async
        def create_data():
            self.collection = Collection.objects.create(
                contributor=self.user_alice,
                name="Test collection",
            )
            self.collection.set_permission(Permission.VIEW, self.user_bob)
            self.collection.set_permission(Permission.VIEW, self.user_alice)

            self.collection2 = Collection.objects.create(
                contributor=self.user_alice,
                name="Test collection 2",
            )
            self.collection2.set_permission(Permission.NONE, self.user_bob)
            self.collection2.set_permission(Permission.VIEW, self.user_alice)

            data = Data.objects.create(
                pk=42,
                name="Test data",
                slug="test-data",
                contributor=self.user_alice,
                process=self.process,
                collection=self.collection,
                size=0,
            )
            return data

        data = await create_data()

        # Create a subscription to the Data object by Bob.
        @database_sync_to_async
        def subscribe(data):
            subscription_bob = Subscription.objects.create(
                user=self.user_bob,
                session_id="test_session_bob",
                subscription_id=self.subscription_id,
            )
            subscription_bob.subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[42],
                change_types=[ChangeType.UPDATE, ChangeType.DELETE],
            )
            subscription_bob.subscribe(
                content_type=ContentType.objects.get_for_model(Collection),
                object_ids=[self.collection.pk, self.collection2.pk],
                change_types=[ChangeType.UPDATE, ChangeType.DELETE, ChangeType.CREATE],
            )

            subscription_alice = Subscription.objects.create(
                user=self.user_alice,
                session_id="test_session_alice",
                subscription_id=self.subscription_id2,
            )
            subscription_alice.subscribe(
                content_type=ContentType.objects.get_for_model(Collection),
                object_ids=[self.collection.pk, self.collection2.pk],
                change_types=[ChangeType.DELETE, ChangeType.CREATE, ChangeType.UPDATE],
            )
            subscription_alice.subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[data.pk],
                change_types=[ChangeType.DELETE, ChangeType.CREATE, ChangeType.UPDATE],
            )

        await subscribe(data)

        # Reset the PermissionGroup of the Data object (removes permissions to Bob)
        @database_sync_to_async
        def change_permission_group(data):
            data.move_to_collection(self.collection2)

        await change_permission_group(data)
        notifications_bob = [
            json.loads(await client_bob.receive_from()) for _ in range(2)
        ]
        notifications_alice = [
            json.loads(await client_alice.receive_from()) for _ in range(3)
        ]

        # Assert that Bob sees this as a deletion.
        self.assertCountEqual(
            notifications_bob,
            [
                {
                    "change_type": ChangeType.DELETE.name,
                    "object_id": 42,
                    "subscription_id": self.subscription_id.hex,
                    "source": ["data", 42],
                },
                {
                    "object_id": self.collection.pk,
                    "change_type": ChangeType.DELETE.name,
                    "source": ["data", 42],
                    "subscription_id": self.subscription_id.hex,
                },
            ],
        )
        self.assertCountEqual(
            notifications_alice,
            [
                {
                    "object_id": data.pk,
                    "change_type": ChangeType.UPDATE.name,
                    "subscription_id": self.subscription_id2.hex,
                    "source": ["data", 42],
                },
                {
                    "object_id": self.collection.pk,
                    "change_type": ChangeType.DELETE.name,
                    "source": ["data", 42],
                    "subscription_id": self.subscription_id2.hex,
                },
                {
                    "object_id": self.collection2.pk,
                    "change_type": ChangeType.CREATE.name,
                    "source": ["data", 42],
                    "subscription_id": self.subscription_id2.hex,
                },
            ],
        )
        await self.assert_no_more_messages(client_bob)
        await self.assert_no_more_messages(client_alice)

    async def test_modify_permissions(self):
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        connected, details = await client.connect()
        self.assertTrue(connected)

        # Create a new Data object.
        @database_sync_to_async
        def create_data():
            return Data.objects.create(
                pk=42,
                name="Test data",
                slug="test-data",
                contributor=self.user_alice,
                process=self.process,
                size=0,
            )

        data = await create_data()

        # Create a subscription to the Data content_type by Bob.
        @database_sync_to_async
        def subscribe():
            Subscription.objects.create(
                user=self.user_bob,
                session_id="test_session",
                subscription_id=self.subscription_id,
            ).subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[None],
                change_types=[ChangeType.CREATE, ChangeType.UPDATE, ChangeType.DELETE],
            )

        await subscribe()
        await self.await_subscription_observer_count(3)

        # Grant Bob view permissions to the Data object.
        @database_sync_to_async
        def grant_permissions(data):
            data.set_permission(Permission.VIEW, self.user_bob)

        await grant_permissions(data)

        # Assert we detect gaining permissions as creations.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "change_type": ChangeType.CREATE.name,
                "object_id": 42,
                "subscription_id": self.subscription_id.hex,
                "source": ["data", 42],
            },
        )
        await self.assert_no_more_messages(client)

        # Revoke permissions for Bob.
        @database_sync_to_async
        def revoke_permissions(data):
            data.set_permission(Permission.NONE, self.user_bob)

        await revoke_permissions(data)

        # Assert we detect losing permissions as deletions.
        self.assertDictEqual(
            json.loads(await client.receive_from()),
            {
                "change_type": ChangeType.DELETE.name,
                "object_id": 42,
                "subscription_id": self.subscription_id.hex,
                "source": ["data", 42],
            },
        )
        await self.assert_no_more_messages(client)

    async def test_observe_table_no_permissions(self):
        client = WebsocketCommunicator(self.client_consumer, "/ws/test_session")
        connected, details = await client.connect()
        self.assertTrue(connected)

        # Create a subscription to the Data content_type by Bob.
        @database_sync_to_async
        def subscribe():
            Subscription.objects.create(
                user=self.user_bob,
                session_id="test_session",
                subscription_id=self.subscription_id,
            ).subscribe(
                content_type=ContentType.objects.get_for_model(Data),
                object_ids=[None],
                change_types=[ChangeType.CREATE, ChangeType.UPDATE, ChangeType.DELETE],
            )

        await subscribe()
        await self.await_subscription_observer_count(3)

        # Create a new Data object.
        @database_sync_to_async
        def create_data():
            data = Data.objects.create(
                pk=42,
                name="Test data",
                slug="test-data",
                contributor=self.user_alice,
                process=self.process,
                size=0,
            )
            return data

        data = await create_data()

        # Assert we don't detect creations (Bob doesn't have permissions).
        await self.assert_no_more_messages(client)

        # Delete the Data object.
        @database_sync_to_async
        def delete_data(data):
            data.delete()

        await delete_data(data)

        # Assert we don't detect deletions.
        await self.assert_no_more_messages(client)

        # Assert subscription didn't delete.
        await self.await_subscription_observer_count(3)


class ObserverAPITestCase(TransactionResolweAPITestCase):
    def setUp(self):
        self.viewset = DataViewSet
        self.resource_name = "data"
        super().setUp()
        self.list_view = self.viewset.as_view({"post": "subscribe"})

        user_model = get_user_model()
        self.user_alice = user_model.objects.create(
            username="alice",
            email="alice@test.com",
            first_name="Ana",
            last_name="Banana",
        )
        self.user_bob = user_model.objects.create(
            username="capital-bob",
            email="bob@bob.bob",
            first_name="Capital",
            last_name="Bobnik",
        )
        self.process = Process.objects.create(
            name="Dummy process", contributor=self.user_alice
        )
        Data.objects.create(
            pk=42,
            name="Test data",
            slug="test-data",
            contributor=self.user_alice,
            process=self.process,
            size=0,
        )
        self.client_consumer = URLRouter(
            [path("ws/<slug:subscriber_id>", ClientConsumer().as_asgi())]
        )

    def test_subscribe(self):
        # Subscribe to model updates.
        resp = self._post(user=self.user_alice, data={"session_id": "test"})
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertDictEqual(
            resp.data,
            {"subscription_id": Subscription.objects.all()[0].subscription_id.hex},
        )
        # There should be 2 subscriptions: for CREATE and DLETE.
        self.assertEqual(Observer.objects.count(), 2)
        self.assertEqual(
            Observer.objects.filter(
                change_type=ChangeType.CREATE.value,
                object_id=None,
                content_type=ContentType.objects.get_for_model(Data),
            ).count(),
            1,
        )

        # Subscribe to instance updates.
        resp = self._post(
            user=self.user_alice, data={"session_id": "test", "ids": [42]}
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        sub_qs = Subscription.objects.filter(
            session_id="test", observers__object_id=42
        ).distinct()
        self.assertEqual(sub_qs.count(), 1)
        self.assertDictEqual(
            resp.data,
            {"subscription_id": sub_qs.first().subscription_id.hex},
        )
        self.assertEqual(Observer.objects.count(), 4)
        self.assertEqual(
            Observer.objects.filter(
                change_type=ChangeType.UPDATE.value,
                object_id=42,
                content_type=ContentType.objects.get_for_model(Data),
            ).count(),
            1,
        )
        self.assertEqual(
            Observer.objects.filter(
                change_type=ChangeType.DELETE.value,
                object_id=42,
                content_type=ContentType.objects.get_for_model(Data),
            ).count(),
            1,
        )

        # Re-subscribe to the same endpoint.
        resp = self._post(
            user=self.user_alice, data={"session_id": "test", "ids": [42]}
        )
        # Assert we don't have duplicate observers.
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(Observer.objects.count(), 4)

    def test_subscribe_to_forbidden_object(self):
        resp = self._post(user=self.user_bob, data={"session_id": "test", "ids": [42]})
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(Observer.objects.count(), 0)

    def test_subscribe_to_nonexistent_object(self):
        resp = self._post(
            user=self.user_alice, data={"session_id": "test", "ids": [111]}
        )
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(Observer.objects.count(), 0)

    def test_subscribe_anonymously(self):
        # A different anonymous user was created with each test, so the
        # ANONYMOUS_USER global variable is cached incorrectly and must be reset.
        anon = get_anonymous_user(cache=False)

        # Make data public.
        data = Data.objects.get(pk=42)
        data.set_permission(Permission.VIEW, anon)

        resp = self._post(data={"session_id": "test", "ids": [42]})
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertDictEqual(
            resp.data,
            {"subscription_id": Subscription.objects.all()[0].subscription_id.hex},
        )
        self.assertEqual(
            Observer.objects.filter(
                change_type=ChangeType.UPDATE.value,
                object_id=42,
                content_type=ContentType.objects.get_for_model(Data),
            ).count(),
            1,
        )
        self.assertEqual(
            Observer.objects.filter(
                change_type=ChangeType.DELETE.value,
                object_id=42,
                content_type=ContentType.objects.get_for_model(Data),
            ).count(),
            1,
        )
