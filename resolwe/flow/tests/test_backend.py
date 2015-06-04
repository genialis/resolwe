# pylint: disable=missing-docstring
import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
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

        data_path = settings.FLOW['EXECUTOR']['DATA_PATH']

        if os.path.exists(data_path):
            shutil.rmtree(data_path)

        os.makedirs(data_path)

    def test_manager(self):
        manager.communicate()
