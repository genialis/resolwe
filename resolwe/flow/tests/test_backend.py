# pylint: disable=missing-docstring
from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.test import TestCase

from resolwe.flow.engine import manager
from resolwe.flow.models import Data, Tool


class ManagerTest(TestCase):
    def setUp(self):
        u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')
        t = Tool(slug='test-processor',
                 name='Test Processor',
                 contributor=u,
                 type='data:test',
                 version=1)
        t.save()

        d = Data(slug='test-data',
                 name='Test Data',
                 contributor=u,
                 tool=t)
        d.save()

    def test_manager(self):
        manager.communicate()
