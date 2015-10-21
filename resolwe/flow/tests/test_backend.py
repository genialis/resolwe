# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase

from resolwe.flow.engine import manager
from resolwe.flow.models import Data, Process


class BackendTest(TestCase):
    def setUp(self):
        u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')
        self.p = Process(slug='test-processor',
                         name='Test Process',
                         contributor=u,
                         type='data:test',
                         version=1)
        self.p.save()

        self.d = Data(slug='test-data',
                      name='Test Data',
                      contributor=u,
                      process=self.p)
        self.d.save()
        data_path = settings.FLOW['EXECUTOR']['DATA_PATH']

        if os.path.exists(data_path):
            shutil.rmtree(data_path)

        os.makedirs(data_path)

    def test_manager(self):
        manager.communicate()

    def test_dtlbash(self):
        self.p.slug = 'test-processor-dtlbash'
        self.p.run = {'script': """
gen-info \"Test processor info\"
gen-warning \"Test processor warning\"

echo '{"proc.info": "foo"}'
"""}
        self.p.save()

        self.d.slug = 'test-data-dtlbash'
        self.d.process = self.p
        self.d.save()
        self.d = Data(id=self.d.id)
        print(self.d.output)
