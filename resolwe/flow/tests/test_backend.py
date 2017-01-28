# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Process
from resolwe.test import TestCase


class BackendTest(TestCase):
    def setUp(self):
        super(BackendTest, self).setUp()

        self.p = Process(slug='test-processor',
                         name='Test Process',
                         contributor=self.contributor,
                         type='data:test',
                         version=1)
        self.p.save()

        self.d = Data(slug='test-data',
                      name='Test Data',
                      contributor=self.contributor,
                      process=self.p)
        self.d.save()

    def test_manager(self):
        manager.communicate(verbosity=0)

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
