# pylint: disable=missing-docstring

import threading
import time
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.db import IntegrityError, connection
from django.db.models.signals import pre_delete
from django.test import TransactionTestCase
from django.utils import timezone

from resolwe.flow.models import Data

from .models import Observer, Subscription
from .protocol import ChangeType


class ObserverConcurrencyTestCase(TransactionTestCase):
    """Subscribe to observers while other sessions delete them.

    Each concurrent operation runs in its own thread with its own database
    connection, so the operations commit and lock as they do in production.
    """

    def setUp(self):
        super().setUp()
        self.user = get_user_model().objects.create(
            username="alice", email="alice@test.com"
        )
        self.content_type = ContentType.objects.get_for_model(Data)

    def start_thread(self, function):
        """Run the function in a new thread and return the thread."""

        def target():
            try:
                function()
            except BaseException as error:
                thread.error = error
            finally:
                connection.close()

        thread = threading.Thread(target=target)
        thread.error = None
        thread.start()
        return thread

    def join_thread(self, thread):
        """Wait for the thread to finish and re-raise its error."""
        thread.join(timeout=10)
        self.assertFalse(thread.is_alive(), "The thread did not finish.")
        if thread.error is not None:
            raise thread.error

    def wait_until_finished_or_waiting_for_lock(self, thread, connections=1):
        """Wait until the thread finishes or the connections wait for row locks."""
        deadline = time.monotonic() + 10
        while thread.is_alive():
            with connection.cursor() as cursor:
                # The activity statistics are cached for the whole transaction.
                cursor.execute("SELECT pg_stat_clear_snapshot()")
                cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
                if cursor.fetchone()[0] >= connections:
                    return
            if time.monotonic() > deadline:
                self.fail("The thread neither finished nor waited for a lock.")
            time.sleep(0.01)

    def subscribe(self, subscription):
        """Subscribe to updates of the object with id 42."""
        subscription.subscribe(self.content_type, [42], [ChangeType.UPDATE])

    def run_after_get_or_create(self, function):
        """Patch the observer lookup to run the function after its first call."""
        get_or_create = Observer.objects.get_or_create
        calls = []

        def get_or_create_then_run(*args, **kwargs):
            result = get_or_create(*args, **kwargs)
            if not calls:
                calls.append(result)
                function()
            return result

        return patch.object(
            Observer.objects, "get_or_create", side_effect=get_or_create_then_run
        )

    def assert_subscribed(self, subscription):
        """Assert the subscription observes updates of the object with id 42."""
        self.assertTrue(
            subscription.observers.filter(
                content_type=self.content_type,
                object_id=42,
                change_type=ChangeType.UPDATE.value,
            ).exists()
        )

    def test_subscribe_while_only_other_subscriber_deletes_observer(self):
        subscription_b = Subscription.objects.create(user=self.user, session_id="b")
        self.subscribe(subscription_b)
        subscription_a = Subscription.objects.create(user=self.user, session_id="a")

        # Session B disconnects after session A has read the observer and before it
        # adds the observer to its subscription.
        with self.run_after_get_or_create(
            lambda: self.join_thread(self.start_thread(subscription_b.delete))
        ):
            self.subscribe(subscription_a)

        self.assertFalse(Subscription.objects.filter(session_id="b").exists())
        self.assert_subscribed(subscription_a)

    def test_subscribe_during_delete_of_only_other_subscriber(self):
        subscription_b = Subscription.objects.create(user=self.user, session_id="b")
        self.subscribe(subscription_b)
        subscription_a = Subscription.objects.create(user=self.user, session_id="a")
        threads = []

        # Session A subscribes after session B has selected the observers to delete
        # and before it deletes them.
        def subscribe_a(**kwargs):
            pre_delete.disconnect(subscribe_a, sender=Observer)
            threads.append(self.start_thread(lambda: self.subscribe(subscription_a)))
            self.wait_until_finished_or_waiting_for_lock(threads[0])

        pre_delete.connect(subscribe_a, sender=Observer, weak=False)
        self.addCleanup(pre_delete.disconnect, subscribe_a, sender=Observer)
        subscription_b.delete()
        self.join_thread(threads[0])

        self.assertFalse(Subscription.objects.filter(session_id="b").exists())
        self.assert_subscribed(subscription_a)

    def test_subscribe_while_clean_observers_deletes_observer(self):
        Observer.objects.create(
            content_type=self.content_type,
            object_id=42,
            change_type=ChangeType.UPDATE.value,
        )
        subscription_a = Subscription.objects.create(user=self.user, session_id="a")

        # The observer has no subscriptions, so the cleanup deletes it after session
        # A has read it.
        with self.run_after_get_or_create(
            lambda: self.join_thread(
                self.start_thread(lambda: call_command("clean_observers"))
            )
        ):
            self.subscribe(subscription_a)

        self.assert_subscribed(subscription_a)

    def test_subscribe_during_delete_of_own_subscription(self):
        subscription = Subscription.objects.create(user=self.user, session_id="a")
        # The request and the disconnect load the subscription separately.
        request_subscription = Subscription.objects.get(pk=subscription.pk)
        threads = []

        # The session disconnects while its own subscribe request adds an observer.
        # The request tries to add the observer after the disconnect has deleted
        # the links to the observers and before it deletes the subscription. It
        # waits for the disconnect and fails, as the subscription is gone.
        def subscribe_before_subscription_delete(execute, sql, params, many, context):
            if not threads and sql.startswith('DELETE FROM "observers_subscription"'):
                threads.append(
                    self.start_thread(lambda: self.subscribe(request_subscription))
                )
                self.wait_until_finished_or_waiting_for_lock(threads[0])
            return execute(sql, params, many, context)

        with connection.execute_wrapper(subscribe_before_subscription_delete):
            subscription.delete()

        self.assertEqual(len(threads), 1)
        with self.assertRaises(IntegrityError):
            self.join_thread(threads[0])
        self.assertFalse(Subscription.objects.exists())
        self.assertFalse(Subscription.observers.through.objects.exists())

    def test_subscribe_while_new_observer_is_created_and_deleted(self):
        subscription_a = Subscription.objects.create(user=self.user, session_id="a")
        observer_filter = {
            "content_type": self.content_type,
            "object_id": 42,
            "change_type": ChangeType.UPDATE.value,
        }
        failed_inserts = []

        # Another session creates the observer after session A did not find it, and
        # the observer is deleted again before session A reads it after its insert
        # failed.
        def create_and_delete_observer(execute, sql, params, many, context):
            if failed_inserts or not sql.startswith('INSERT INTO "observers_observer"'):
                return execute(sql, params, many, context)
            self.join_thread(
                self.start_thread(lambda: Observer.objects.create(**observer_filter))
            )
            try:
                return execute(sql, params, many, context)
            except IntegrityError:
                failed_inserts.append(sql)
                self.join_thread(
                    self.start_thread(
                        lambda: Observer.objects.filter(**observer_filter).delete()
                    )
                )
                raise

        with connection.execute_wrapper(create_and_delete_observer):
            self.subscribe(subscription_a)

        self.assertEqual(len(failed_inserts), 1)
        self.assert_subscribed(subscription_a)

    def test_subscribe_while_both_subscribers_of_observer_are_deleted(self):
        content_type = self.content_type
        observer_1, observer_2 = (
            Observer.objects.create(
                content_type=content_type,
                object_id=42,
                change_type=change_type.value,
            )
            for change_type in (ChangeType.CREATE, ChangeType.UPDATE)
        )
        subscription_a = Subscription.objects.create(user=self.user, session_id="a")
        subscription_a.observers.add(observer_1)
        subscription_b = Subscription.objects.create(user=self.user, session_id="b")
        subscription_b.observers.add(observer_1, observer_2)
        threads = []

        # Session A subscribes to the second observer while both sessions
        # disconnect. The disconnect of B waits for the second observer and the
        # disconnect of A waits for B, before A's subscribe commits.
        def delete_subscriptions(execute, sql, params, many, context):
            if not threads and sql.startswith(
                'INSERT INTO "observers_subscription_observers"'
            ):
                threads.append(self.start_thread(subscription_b.delete))
                self.wait_until_finished_or_waiting_for_lock(threads[0])
                threads.append(self.start_thread(subscription_a.delete))
                self.wait_until_finished_or_waiting_for_lock(threads[1], 2)
            return execute(sql, params, many, context)

        with connection.execute_wrapper(delete_subscriptions):
            self.subscribe(subscription_a)

        self.assertEqual(len(threads), 2)
        for thread in threads:
            self.join_thread(thread)
        self.assertFalse(Subscription.objects.exists())
        self.assertFalse(Observer.objects.exists())

    def test_disconnect_while_clean_observers_deletes_old_subscription(self):
        subscription = Subscription.objects.create(user=self.user, session_id="a")
        self.subscribe(subscription)
        Subscription.objects.update(
            created=timezone.now() - timezone.timedelta(days=30)
        )
        threads = []

        # The session disconnects after the cleanup has deleted the links of its
        # old subscription to the observers and before it deletes the subscription.
        def disconnect_before_subscription_delete(execute, sql, params, many, context):
            if not threads and sql.startswith('DELETE FROM "observers_subscription" '):
                threads.append(self.start_thread(subscription.delete))
                self.wait_until_finished_or_waiting_for_lock(threads[0])
            return execute(sql, params, many, context)

        with connection.execute_wrapper(disconnect_before_subscription_delete):
            call_command("clean_observers")

        self.assertEqual(len(threads), 1)
        self.join_thread(threads[0])
        self.assertFalse(Subscription.objects.exists())
