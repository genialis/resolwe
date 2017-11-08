# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime

import six

from django.contrib.auth import get_user_model

from resolwe.flow.filters import CollectionFilter, DataFilter
from resolwe.flow.models import Collection, Data, DataDependency, DescriptorSchema, Process
from resolwe.test import TestCase


class DataFilterTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user_1 = get_user_model().objects.create(username="first_user")
        cls.user_2 = get_user_model().objects.create(username="second_user")

        cls.proc_1 = Process.objects.create(
            name='Test process 1',
            slug='test-process-1',
            contributor=cls.user_1,
            type='data:test:'
        )

        cls.proc_2 = Process.objects.create(
            name='Test process 2',
            slug='test-process-2',
            contributor=cls.user_1,
            type='data:test:second:'
        )

        cls.proc_3 = Process.objects.create(
            name='Test process 3',
            slug='test-process-3',
            contributor=cls.user_1,
            type='data:third:'
        )

        cls.data_1 = Data.objects.create(
            name='Test data 1',
            slug='test-data-1',
            contributor=cls.user_1,
            process=cls.proc_1,
            status=Data.STATUS_DONE,
            started=datetime.datetime(2016, 7, 30, 14, 0),
            finished=datetime.datetime(2016, 7, 30, 14, 30),
        )
        cls.data_1.created = datetime.datetime(2016, 7, 30, 13, 59)
        cls.data_1.save()

        cls.data_2 = Data.objects.create(
            name='Test data 2',
            slug='test-data-2',
            contributor=cls.user_1,
            process=cls.proc_2,
            status=Data.STATUS_ERROR,
            started=datetime.datetime(2016, 8, 30, 15, 0),
            finished=datetime.datetime(2016, 8, 30, 15, 30),
            tags=['foo', 'bar', 'moo']
        )
        cls.data_2.created = datetime.datetime(2016, 8, 30, 14, 59)
        cls.data_2.save()

        cls.data_3 = Data.objects.create(
            name='Another data object',
            slug='another-data-object',
            contributor=cls.user_2,
            process=cls.proc_3,
            status=Data.STATUS_DONE,
            started=datetime.datetime(2013, 1, 15, 8, 0),
            finished=datetime.datetime(2013, 1, 15, 10, 0),
            tags=['bar']
        )
        cls.data_3.created = datetime.datetime(2013, 1, 15, 7, 59)
        cls.data_3.save()

        cls.collection = Collection.objects.create(
            name='Test collection',
            slug='test-collection',
            contributor=cls.user_1
        )
        cls.collection.data.add(cls.data_1)

    def _apply_filter(self, filters, expected):
        filtered = DataFilter(filters, queryset=Data.objects.all())
        six.assertCountEqual(self, filtered.qs, expected)

    def test_filter_id(self):
        self._apply_filter({'id': self.data_1.pk}, [self.data_1])
        self._apply_filter({'id__in': '{},{}'.format(self.data_1.pk, self.data_2.pk)},
                           [self.data_1, self.data_2])

    def test_filter_slug(self):
        self._apply_filter({'slug': 'test-data-2'}, [self.data_2])
        self._apply_filter({'slug__startswith': 'test'}, [self.data_1, self.data_2])

    def test_filter_name(self):
        self._apply_filter({'name': 'Test data 2'}, [self.data_2])
        self._apply_filter({'name__startswith': 'Test'}, [self.data_1, self.data_2])

    def test_filter_contributor(self):
        self._apply_filter({'contributor': self.user_1.pk}, [self.data_1, self.data_2])

    def test_filter_created(self):
        self._apply_filter({'created__year': '2016'}, [self.data_1, self.data_2])
        self._apply_filter({'created__year__gt': '2015'}, [self.data_1, self.data_2])
        self._apply_filter({'created__year__gt': '2015', 'created__month__gte': '8'}, [self.data_2])

    def test_filter_modified(self):
        year = datetime.date.today().strftime('%Y')
        self._apply_filter({'modified__year': year}, [self.data_1, self.data_2, self.data_3])
        self._apply_filter({'modified__year__gt': year}, [])

    def test_filter_collection(self):
        self._apply_filter({'collection': self.collection.pk}, [self.data_1])
        self._apply_filter({'collection__slug': 'test-collection'}, [self.data_1])

    def test_filter_type(self):
        self._apply_filter({'type': 'data:test:'}, [self.data_1, self.data_2])

    def test_filter_status(self):
        self._apply_filter({'status': 'ok'}, [self.data_1, self.data_3])

    def test_filter_finished(self):
        self._apply_filter({'finished__year': '2016'}, [self.data_1, self.data_2])
        self._apply_filter({'finished__year__gt': '2015'}, [self.data_1, self.data_2])
        self._apply_filter({'finished__year__gt': '2015', 'finished__month__gte': '8'}, [self.data_2])

    def test_filter_started(self):
        self._apply_filter({'started__year': '2016'}, [self.data_1, self.data_2])
        self._apply_filter({'started__year__gt': '2015'}, [self.data_1, self.data_2])
        self._apply_filter({'started__year__gt': '2015', 'started__month__gte': '8'}, [self.data_2])

    def test_filter_process(self):
        self._apply_filter({'process': self.proc_1.pk}, [self.data_1])
        self._apply_filter({'process__slug': 'test-process-1'}, [self.data_1])

    def test_filter_tags(self):
        self._apply_filter({'tags': 'foo'}, [self.data_2])
        self._apply_filter({'tags': 'bar'}, [self.data_2, self.data_3])
        self._apply_filter({'tags': 'bar,moo'}, [self.data_2])

    def test_filter_parents_children(self):
        self._apply_filter({'parents': self.data_1.pk}, [])
        self._apply_filter({'parents': self.data_2.pk}, [])
        self._apply_filter({'children': self.data_1.pk}, [])
        self._apply_filter({'children': self.data_2.pk}, [])

        DataDependency.objects.create(
            parent=self.data_1,
            child=self.data_2,
            kind=DataDependency.KIND_IO,
        )

        self._apply_filter({'parents': self.data_1.pk}, Data.objects.filter(parents=self.data_1))
        self._apply_filter({'parents': self.data_2.pk}, Data.objects.filter(parents=self.data_2))
        self._apply_filter({'children': self.data_1.pk}, Data.objects.filter(children=self.data_1))
        self._apply_filter({'children': self.data_2.pk}, Data.objects.filter(children=self.data_2))


class CollectionFilterTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user_1 = get_user_model().objects.create(username="first_user")
        cls.user_2 = get_user_model().objects.create(username="second_user")

        cls.descriptor_schema_1 = DescriptorSchema.objects.create(
            contributor=cls.user_1,
            name='Descriptor schema 1'
        )

        cls.descriptor_schema_2 = DescriptorSchema.objects.create(
            contributor=cls.user_2,
            name='Descriptor schema 2'
        )

        cls.collection_1 = Collection.objects.create(
            name='Test collection 1',
            slug='test-collection-1',
            descriptor_schema=cls.descriptor_schema_1,
            contributor=cls.user_1
        )
        cls.collection_1.created = datetime.datetime(2016, 7, 30, 13, 59)
        cls.collection_1.save()

        cls.collection_2 = Collection.objects.create(
            name='Test collection 2',
            slug='test-collection-2',
            descriptor_schema=cls.descriptor_schema_1,
            contributor=cls.user_1
        )
        cls.collection_2.created = datetime.datetime(2016, 8, 30, 14, 59)
        cls.collection_2.save()

        cls.collection_3 = Collection.objects.create(
            name='Another collection',
            slug='another-collection',
            descriptor_schema=cls.descriptor_schema_2,
            contributor=cls.user_2
        )
        cls.collection_3.created = datetime.datetime(2013, 1, 15, 7, 59)
        cls.collection_3.save()

        cls.proc = Process.objects.create(
            name='Test process',
            slug='test-process',
            contributor=cls.user_1,
            type='data:test:'
        )

        cls.data = Data.objects.create(
            name='Test data',
            slug='test-data',
            contributor=cls.user_1,
            process=cls.proc,
            status=Data.STATUS_DONE
        )
        cls.collection_1.data.add(cls.data)
        cls.collection_2.data.add(cls.data)

    def _apply_filter(self, filters, expected):
        filtered = CollectionFilter(filters, queryset=Collection.objects.all())
        six.assertCountEqual(self, filtered.qs, expected)

    def test_filter_id(self):
        self._apply_filter({'id': self.collection_1.pk}, [self.collection_1])
        self._apply_filter({'id__in': '{},{}'.format(self.collection_1.pk, self.collection_2.pk)},
                           [self.collection_1, self.collection_2])

    def test_filter_slug(self):
        self._apply_filter({'slug': 'test-collection-2'}, [self.collection_2])
        self._apply_filter({'slug__startswith': 'test'}, [self.collection_1, self.collection_2])

    def test_filter_name(self):
        self._apply_filter({'name': 'Test collection 2'}, [self.collection_2])
        self._apply_filter({'name__startswith': 'Test'}, [self.collection_1, self.collection_2])

    def test_filter_contributor(self):
        self._apply_filter({'contributor': self.user_1.pk},
                           [self.collection_1, self.collection_2])

    def test_filter_created(self):
        self._apply_filter({'created__year': '2016'}, [self.collection_1, self.collection_2])
        self._apply_filter({'created__year__gt': '2015'}, [self.collection_1, self.collection_2])
        self._apply_filter({'created__year__gt': '2015', 'created__month__gte': '8'}, [self.collection_2])

    def test_filter_modified(self):
        year = datetime.date.today().strftime('%Y')
        self._apply_filter({'modified__year': year}, [self.collection_1, self.collection_2, self.collection_3])
        self._apply_filter({'modified__year__gt': year}, [])

    def test_filter_data(self):
        self._apply_filter({'data': self.data.pk}, [self.collection_1, self.collection_2])

    def test_filter_descriptor_schema(self):
        self._apply_filter({'descriptor_schema': self.descriptor_schema_1.pk}, [self.collection_1, self.collection_2])
