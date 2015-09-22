# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase

from resolwe.flow.engine import manager
from resolwe.flow.models import Data, Process


class ManagerTest(TestCase):
    def setUp(self):
        u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')
        p = Process(slug='test-processor',
                    name='Test Processor',
                    contributor=u,
                    type='data:test',
                    version=1,
                    run={'script': '{% if reads.type.startswith("data:reads:") %}'})
        p.save()

        d = Data(slug='test-data',
                 name='Test Data',
                 contributor=u,
                 process=p)
        d.save()

        data_path = settings.FLOW['EXECUTOR']['DATA_PATH']

        if os.path.exists(data_path):
            shutil.rmtree(data_path)

        os.makedirs(data_path)

    def test_invalid_template(self):
        manager.communicate()
        self.assertEquals(Data.objects.get(slug='test-data').status, Data.STATUS_ERROR)
        self.assertTrue('Error in process script' in Data.objects.get(slug='test-data').process_error[0])
