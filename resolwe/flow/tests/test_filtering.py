# pylint: disable=missing-docstring
import datetime

from django.contrib.auth import get_user_model
from django.utils.timezone import get_current_timezone

from guardian.shortcuts import assign_perm
from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.filters import CollectionFilter, DataFilter, EntityFilter
from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.views import CollectionViewSet, DataViewSet, DescriptorSchemaViewSet, ProcessViewSet
from resolwe.test import TestCase

factory = APIRequestFactory()  # pylint: disable=invalid-name


class BaseViewSetFiltersTest(TestCase):

    def setUp(self):
        super().setUp()

        self.viewset = self.viewset_class.as_view(actions={  # pylint: disable=no-member
            'get': 'list',
        })

    def _check_filter(self, query_args, expected, expected_status_code=status.HTTP_200_OK):
        """Check that query_args filter to expected queryset."""
        request = factory.get('/', query_args, format='json')
        force_authenticate(request, self.admin)
        response = self.viewset(request)

        if status.is_success(response.status_code):
            self.assertEqual(len(response.data), len(expected))
            self.assertCountEqual(
                [item.pk for item in expected],
                [item['id'] for item in response.data],
            )
        else:
            self.assertEqual(response.status_code, expected_status_code)
            response.render()
            return response


class CollectionViewSetFiltersTest(BaseViewSetFiltersTest):

    def setUp(self):
        self.viewset_class = CollectionViewSet
        super().setUp()

        self.collection = Collection.objects.create(
            name='Test collection 1',
            contributor=self.contributor,
        )

    def test_filter_name(self):
        self._check_filter({'name': 'Test collection 1'}, [self.collection])
        self._check_filter({'name': 'Test collection'}, [])


class DataViewSetFiltersTest(BaseViewSetFiltersTest):

    def setUp(self):
        self.viewset_class = DataViewSet
        super().setUp()

        tzone = get_current_timezone()

        self.collection = Collection.objects.create(contributor=self.contributor)

        self.proc1 = Process.objects.create(
            type='data:test:process1:',
            name='First process',
            slug='test-process-1',
            version='1.0.0',
            contributor=self.contributor,
            entity_type='test-schema',
            entity_descriptor_schema='test-schema',
            input_schema=[{'name': 'input_data', 'type': 'data:test:', 'required': False}],
        )

        self.proc2 = Process.objects.create(
            type='data:test:process2:',
            name='Second process',
            slug='test-process-2',
            version='1.0.0',
            contributor=self.contributor,
            entity_type='test-schema',
            entity_descriptor_schema='test-schema',
            input_schema=[{'name': 'input_data', 'type': 'data:test:', 'required': False}],
        )

        self.descriptor_schema = DescriptorSchema.objects.create(
            slug='test-schema',
            version='1.0.0',
            contributor=self.contributor,
        )

        self.data = []
        for index in range(10):
            data = Data.objects.create(
                name='Data {}'.format(index),
                slug='dataslug-{}'.format(index),
                contributor=self.contributor,
                process=self.proc1 if index < 5 else self.proc2,
                status=Data.STATUS_DONE if index > 0 else Data.STATUS_RESOLVING,
                started=datetime.datetime(2016, 7, 31, index, 0, tzinfo=tzone),
                finished=datetime.datetime(2016, 7, 31, index, 30, tzinfo=tzone),
                tags=['foo', 'index{}'.format(index)],
            )

            data.created = datetime.datetime(2016, 7, 30, index, 59, tzinfo=tzone)
            data.save()

            assign_perm('owner_data', self.admin, data)

            if index == 0:
                self.collection.data.add(data)

            self.data.append(data)

    def test_filter_id(self):
        self._check_filter({'id': str(self.data[0].pk)}, [self.data[0]])
        self._check_filter({'id': str(self.data[2].pk)}, [self.data[2]])
        self._check_filter({'id__in': '{},{}'.format(self.data[0].pk, self.data[2].pk)}, [self.data[0], self.data[2]])

    def test_filter_slug(self):
        self._check_filter({'slug': 'dataslug-1'}, [self.data[1]])
        self._check_filter({'slug': 'dataslug-5'}, [self.data[5]])
        self._check_filter({'slug__in': 'dataslug-1'}, [self.data[1]])
        self._check_filter({'slug__in': 'dataslug-1,dataslug-5'}, [self.data[1], self.data[5]])

    def test_filter_name(self):
        self._check_filter({'name': 'Data 1'}, [self.data[1]])
        self._check_filter({'name': 'data 1'}, [])
        self._check_filter({'name': 'Data 2'}, [self.data[2]])
        self._check_filter({'name': 'Data'}, [])
        self._check_filter({'name': '1'}, [])

    def test_filter_contributor(self):
        self._check_filter({'contributor': str(self.contributor.pk)}, self.data)
        self._check_filter({'contributor': '0'}, [])
        self._check_filter({'contributor__in': '0,{}'.format(self.contributor.pk)}, self.data)

    def test_filter_owners(self):
        self._check_filter({'owners': str(self.admin.pk)}, self.data)
        self._check_filter({'owners': '0'}, [])
        self._check_filter({'owners__in': '0,{}'.format(self.admin.pk)}, self.data)

    def test_filter_created(self):
        self._check_filter({'created': self.data[0].created.isoformat()}, [self.data[0]])
        self._check_filter({'created__gt': '2016'}, self.data)
        self._check_filter({'created__gt': '2016-07-30T05:59:00'}, self.data[6:])
        self._check_filter({'created__gte': '2016-07-30T05:59:00'}, self.data[5:])
        self._check_filter({'created__lt': '2016'}, [])
        self._check_filter({'created__lt': '2016-07-30T05:59:00'}, self.data[:5])
        self._check_filter({'created__lte': '2016-07-30T05:59:00'}, self.data[:6])

    def test_filter_modified(self):
        now = self.data[0].modified
        five_offset = self.data[5].modified
        self._check_filter({'modified': now.isoformat()}, [self.data[0]])
        self._check_filter({'modified__gt': str(now.year)}, self.data)
        self._check_filter({'modified__gt': five_offset.isoformat()}, self.data[6:])
        self._check_filter({'modified__gte': five_offset.isoformat()}, self.data[5:])
        self._check_filter({'modified__lt': str(now.year)}, [])
        self._check_filter({'modified__lt': five_offset.isoformat()}, self.data[:5])
        self._check_filter({'modified__lte': five_offset.isoformat()}, self.data[:6])

    def test_filter_started(self):
        self._check_filter({'started': self.data[0].started.isoformat()}, [self.data[0]])
        self._check_filter({'started__gt': '2016'}, self.data)
        self._check_filter({'started__gt': '2016-07-31T05:00:00'}, self.data[6:])
        self._check_filter({'started__gte': '2016-07-31T05:00:00'}, self.data[5:])
        self._check_filter({'started__lt': '2016'}, [])
        self._check_filter({'started__lt': '2016-07-31T05:00:00'}, self.data[:5])
        self._check_filter({'started__lte': '2016-07-31T05:00:00'}, self.data[:6])

    def test_filter_finished(self):
        self._check_filter({'finished': self.data[0].finished.isoformat()}, [self.data[0]])
        self._check_filter({'finished__gt': '2016'}, self.data)
        self._check_filter({'finished__gt': '2016-07-31T05:30:00'}, self.data[6:])
        self._check_filter({'finished__gte': '2016-07-31T05:30:00'}, self.data[5:])
        self._check_filter({'finished__lt': '2016'}, [])
        self._check_filter({'finished__lt': '2016-07-31T05:30:00'}, self.data[:5])
        self._check_filter({'finished__lte': '2016-07-31T05:30:00'}, self.data[:6])

    def test_filter_collection(self):
        self._check_filter({'collection': str(self.collection.pk)}, [self.data[0]])

    def test_filter_type(self):
        self._check_filter({'type': 'data'}, self.data)
        self._check_filter({'type': 'data:test'}, self.data)
        self._check_filter({'type': 'data:test:process1'}, self.data[:5])
        self._check_filter({'type': 'data:test:process2'}, self.data[5:])
        self._check_filter({'type': 'data:'}, [])
        self._check_filter({'type': 'data:test:'}, [])
        self._check_filter({'type': 'data:test:process1:'}, self.data[:5])
        self._check_filter({'type': 'data:test:process2:'}, self.data[5:])
        self._check_filter({'type__exact': 'data:test'}, [])
        self._check_filter({'type__exact': 'data:test:process1:'}, self.data[:5])
        self._check_filter({'type__exact': 'data:test:process2:'}, self.data[5:])

    def test_filter_status(self):
        self._check_filter({'status': 'OK'}, self.data[1:])
        self._check_filter({'status': 'RE'}, [self.data[0]])
        self._check_filter({'status__in': 'OK,RE'}, self.data)

    def test_filter_process(self):
        self._check_filter({'process': str(self.proc1.pk)}, self.data[:5])
        self._check_filter({'process': str(self.proc2.pk)}, self.data[5:])

    def test_filter_process_name(self):
        self._check_filter({'process_name': 'first'}, self.data[:5])
        self._check_filter({'process_name': 'fir'}, self.data[:5])
        self._check_filter({'process_name': 'rst'}, [])
        self._check_filter({'process_name': 'second'}, self.data[5:])
        self._check_filter({'process_name': 'sec'}, self.data[5:])

    def test_filter_tags(self):
        self._check_filter({'tags': 'foo'}, self.data)
        self._check_filter({'tags': 'foo,index1'}, [self.data[1]])
        self._check_filter({'tags': 'foo,index5'}, [self.data[5]])
        self._check_filter({'tags': 'foo,index10'}, [])
        self._check_filter({'tags': 'bar'}, [])

    def test_filter_text(self):
        # By slug.
        self._check_filter({'text': 'dataslug-1'}, [self.data[1]])
        self._check_filter({'text': 'datasl'}, self.data)

        # By name.
        self._check_filter({'text': 'Data 1'}, [self.data[1]])
        self._check_filter({'text': 'data 2'}, [self.data[2]])
        self._check_filter({'text': 'Data'}, self.data)
        self._check_filter({'text': 'data'}, self.data)
        self._check_filter({'text': 'dat'}, self.data)
        self._check_filter({'text': 'ata'}, [])

        # By contributor.
        self._check_filter({'text': 'joe'}, self.data)
        self._check_filter({'text': 'oe'}, [])
        self._check_filter({'text': 'Miller'}, self.data)
        self._check_filter({'text': 'mill'}, self.data)

        # By owner.
        self._check_filter({'text': 'james'}, self.data)
        self._check_filter({'text': 'mes'}, [])
        self._check_filter({'text': 'Smith'}, self.data)
        self._check_filter({'text': 'smi'}, self.data)

        # By process name.
        self._check_filter({'text': 'first'}, self.data[:5])
        self._check_filter({'text': 'fir'}, self.data[:5])
        self._check_filter({'text': 'rst'}, [])
        self._check_filter({'text': 'process'}, self.data)

    def test_nonexisting_parameter(self):
        response = self._check_filter({'foo': 'bar'}, [self.data[0]], expected_status_code=400)

        self.assertRegex(
            str(response.data['detail']),
            r'Unsupported parameter\(s\): foo. Please use a combination of: .*'
        )


class DescriptorSchemaViewSetFiltersTest(BaseViewSetFiltersTest):

    def setUp(self):
        self.viewset_class = DescriptorSchemaViewSet
        super().setUp()

        tzone = get_current_timezone()

        self.ds1 = DescriptorSchema.objects.create(contributor=self.user, slug='slug1', name='ds1')
        self.ds1.created = datetime.datetime(2015, 7, 28, 11, 57, tzinfo=tzone)
        self.ds1.save()

        self.ds2 = DescriptorSchema.objects.create(contributor=self.user, slug='slug2', name='ds2')
        self.ds2.created = datetime.datetime(2016, 8, 29, 12, 58, 0, tzinfo=tzone)
        self.ds2.save()

        self.ds3 = DescriptorSchema.objects.create(contributor=self.admin, slug='slug3', name='ds3')
        self.ds3.created = datetime.datetime(2017, 9, 30, 13, 59, 0, tzinfo=tzone)
        self.ds3.save()

    def test_id(self):
        self._check_filter({'id': self.ds1.pk}, [self.ds1])
        self._check_filter({'id': self.ds2.pk}, [self.ds2])
        self._check_filter({'id__in': self.ds2.pk}, [self.ds2])
        self._check_filter({'id__in': '{},{}'.format(self.ds1.pk, self.ds2.pk)}, [self.ds1, self.ds2])
        self._check_filter({'id__gt': self.ds1.pk}, [self.ds2, self.ds3])

    def test_slug(self):
        self._check_filter({'slug': 'slug1'}, [self.ds1])
        self._check_filter({'slug': 'slug2'}, [self.ds2])
        self._check_filter({'slug__icontains': 'LUG'}, [self.ds1, self.ds2, self.ds3])
        self._check_filter({'slug__in': 'slug'}, [])
        self._check_filter({'slug__in': 'slug2,slug'}, [self.ds2])
        self._check_filter({'slug__startswith': 'slug'}, [self.ds1, self.ds2, self.ds3])
        self._check_filter({'slug__startswith': 'slug2'}, [self.ds2])
        self._check_filter({'slug__endswith': 'g1'}, [self.ds1])

    def test_name(self):
        self._check_filter({'name': 'ds1'}, [self.ds1])
        self._check_filter({'name': 'ds2'}, [self.ds2])
        self._check_filter({'name__startswith': 'ds'}, [self.ds1, self.ds2, self.ds3])
        self._check_filter({'name__startswith': 'ds2'}, [self.ds2])
        self._check_filter({'name__startswith': 'dsx'}, [])
        self._check_filter({'name__icontains': '2'}, [self.ds2])
        self._check_filter({'name__icontains': 'DS'}, [self.ds1, self.ds2, self.ds3])

    def test_contributor(self):
        self._check_filter({'contributor': self.user.pk}, [self.ds1, self.ds2])
        self._check_filter({'contributor': self.admin.pk}, [self.ds3])
        self._check_filter({'contributor__in': self.admin.pk}, [self.ds3])
        self._check_filter(
            {'contributor__in': '{},{}'.format(self.user.pk, self.admin.pk)},
            [self.ds1, self.ds2, self.ds3],
        )

    def test_created(self):
        self._check_filter({'created': '2015-07-28T11:57:00'}, [self.ds1])
        self._check_filter({'created': '2016-08-29T12:58:00.000000'}, [self.ds2])
        self._check_filter({'created': '2017-09-30T13:59:00.000000Z'}, [self.ds3])
        self._check_filter({'created': '2017-09-30T13:59:00.000000+0000'}, [self.ds3])
        self._check_filter({'created': '2017-09-30T13:59:00.000000+0000'}, [self.ds3])

        self._check_filter({'created__date': '2016-08-29'}, [self.ds2])
        self._check_filter({'created__time': '12:58:00'}, [self.ds2])

        self._check_filter({'created__year': '2016'}, [self.ds2])
        self._check_filter({'created__year__gt': '2015'}, [self.ds2, self.ds3])
        self._check_filter({'created__month__gte': '8'}, [self.ds2, self.ds3])
        self._check_filter({'created__day__lt': '29'}, [self.ds1])
        self._check_filter({'created__hour__lte': '12'}, [self.ds1, self.ds2])
        self._check_filter({'created__minute__gt': '57'}, [self.ds2, self.ds3])

    def test_modified(self):
        now = self.ds1.modified
        self._check_filter({'modified': now.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}, [self.ds1])
        self._check_filter({'modified__year': now.year}, [self.ds1, self.ds2, self.ds3])

    def test_nonexisting_parameter(self):
        response = self._check_filter({'foo': 'bar'}, [self.ds1], expected_status_code=400)

        self.assertRegex(
            str(response.data['__all__'][0]),
            r'Unsupported parameter\(s\): foo. Please use a combination of: .*'
        )


class ProcessViewSetFiltersTest(BaseViewSetFiltersTest):

    def setUp(self):
        self.viewset_class = ProcessViewSet
        super().setUp()

        self.proc_1 = Process.objects.create(
            contributor=self.contributor,
            type='data:alignment:bam:',
            category='analyses:alignment:',
            scheduling_class='BA',
        )

        self.proc_2 = Process.objects.create(
            contributor=self.contributor,
            type='data:expression:',
            category='analyses:',
            scheduling_class='IN',
        )

    def test_category(self):
        self._check_filter({'category': 'analyses:'}, [self.proc_1, self.proc_2])
        self._check_filter({'category': 'analyses:alignment'}, [self.proc_1])

    def test_type(self):
        self._check_filter({'type': 'data:'}, [self.proc_1, self.proc_2])
        self._check_filter({'type': 'data:alignment:bam'}, [self.proc_1])
        self._check_filter({'type': 'data:expression'}, [self.proc_2])

    def test_scheduling_class(self):
        self._check_filter({'scheduling_class': 'BA'}, [self.proc_1])
        self._check_filter({'scheduling_class': 'IN'}, [self.proc_2])

    def test_is_active(self):
        self._check_filter({'is_active': 'true'}, [self.proc_1, self.proc_2])
        self._check_filter({'is_active': 'false'}, [])

        self.proc_1.is_active = False
        self.proc_1.save()

        self._check_filter({'is_active': 'true'}, [self.proc_2])
        self._check_filter({'is_active': 'false'}, [self.proc_1])


class DataFilterTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user_1 = get_user_model().objects.create(username="first_user")
        cls.user_2 = get_user_model().objects.create(username="second_user")

        tzone = get_current_timezone()

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
            started=datetime.datetime(2016, 7, 30, 14, 0, tzinfo=tzone),
            finished=datetime.datetime(2016, 7, 30, 14, 30, tzinfo=tzone),
            size=42,
        )
        cls.data_1.created = datetime.datetime(2016, 7, 30, 13, 59, tzinfo=tzone)
        cls.data_1.save()

        cls.data_2 = Data.objects.create(
            name='Test data 2',
            slug='test-data-2',
            contributor=cls.user_1,
            process=cls.proc_2,
            status=Data.STATUS_ERROR,
            started=datetime.datetime(2016, 8, 30, 15, 0, tzinfo=tzone),
            finished=datetime.datetime(2016, 8, 30, 15, 30, tzinfo=tzone),
            tags=['foo', 'bar', 'moo'],
            size=42,
        )
        cls.data_2.created = datetime.datetime(2016, 8, 30, 14, 59, tzinfo=tzone)
        cls.data_2.save()

        cls.data_3 = Data.objects.create(
            name='Another data object',
            slug='another-data-object',
            contributor=cls.user_2,
            process=cls.proc_3,
            status=Data.STATUS_DONE,
            started=datetime.datetime(2013, 1, 15, 8, 0, tzinfo=tzone),
            finished=datetime.datetime(2013, 1, 15, 10, 0, tzinfo=tzone),
            tags=['bar']
        )
        cls.data_3.created = datetime.datetime(2013, 1, 15, 7, 59, tzinfo=tzone)
        cls.data_3.save()

        cls.collection = Collection.objects.create(
            name='Test collection',
            slug='test-collection',
            contributor=cls.user_1
        )
        cls.collection.data.add(cls.data_1)

    def _apply_filter(self, filters, expected):
        filtered = DataFilter(filters, queryset=Data.objects.all())
        self.assertCountEqual(filtered.qs, expected)

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
        self._apply_filter({'status__in': 'ER'}, [self.data_2])
        self._apply_filter({'status__in': 'ER,OK'}, [self.data_1, self.data_2, self.data_3])

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


class CollectionFilterTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user_1 = get_user_model().objects.create(username="first_user")
        cls.user_2 = get_user_model().objects.create(username="second_user")

        tzone = get_current_timezone()

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
        cls.collection_1.created = datetime.datetime(2016, 7, 30, 13, 59, tzinfo=tzone)
        cls.collection_1.save()

        cls.collection_2 = Collection.objects.create(
            name='Test collection 2',
            slug='test-collection-2',
            descriptor_schema=cls.descriptor_schema_1,
            contributor=cls.user_1
        )
        cls.collection_2.created = datetime.datetime(2016, 8, 30, 14, 59, tzinfo=tzone)
        cls.collection_2.save()

        cls.collection_3 = Collection.objects.create(
            name='Another collection',
            slug='another-collection',
            descriptor_schema=cls.descriptor_schema_2,
            contributor=cls.user_2
        )
        cls.collection_3.created = datetime.datetime(2013, 1, 15, 7, 59, tzinfo=tzone)
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
        self.assertCountEqual(filtered.qs, expected)

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


class EntityFilterTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user_1 = get_user_model().objects.create(username="first_user")

        cls.collection_1 = Collection.objects.create(contributor=cls.user_1)
        cls.collection_2 = Collection.objects.create(contributor=cls.user_1)

        cls.entity_1 = Entity.objects.create(
            contributor=cls.user_1,
            descriptor_completed=True,
        )
        cls.entity_1.collections.add(cls.collection_1)

        cls.entity_2 = Entity.objects.create(
            contributor=cls.user_1,
            descriptor_completed=False,
        )
        cls.entity_2.collections.add(cls.collection_2)

    def _apply_filter(self, filters, expected):
        filtered = EntityFilter(filters, queryset=Entity.objects.all())
        self.assertCountEqual(filtered.qs, expected)

    def test_descriptor_completed(self):
        self._apply_filter({'descriptor_completed': 'true'}, [self.entity_1])
        self._apply_filter({'descriptor_completed': '1'}, [self.entity_1])
        self._apply_filter({'descriptor_completed': 'false'}, [self.entity_2])
        self._apply_filter({'descriptor_completed': '0'}, [self.entity_2])

    def test_collection(self):
        self._apply_filter({'collection': self.collection_1.id}, [self.entity_1])
        self._apply_filter({'collection': self.collection_2.id}, [self.entity_2])
