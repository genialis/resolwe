# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import elasticsearch_dsl as dsl

from resolwe.elastic.builder import ManyToManyDependency
from resolwe.elastic.fields import Name, ProcessType
from resolwe.elastic.indices import BaseDocument, BaseIndex

from .models import TestModel, TestModelWithDependency


class TestSearchDocument(BaseDocument):
    # pylint: disable=no-member
    id = dsl.Integer()  # pylint: disable=invalid-name
    name = dsl.String()
    num = dsl.Integer()
    json = dsl.Object()

    field_name = Name()
    field_process_type = ProcessType()
    none_test = dsl.Integer()

    class Meta:
        index = 'test_search'


class TestSearchIndex(BaseIndex):
    mapping = {
        'num': 'number',
        'field_name': 'name',
        'none_test': 'this.does.not.exist',
    }

    queryset = TestModel.objects.all()
    object_type = TestModel
    document_class = TestSearchDocument

    def get_json_value(self, obj):
        return {'key': 'value'}


class TestAnalyzerSearchDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.String(analyzer=dsl.analyzer(
        'test_analyzer',
        tokenizer='keyword',
        filter=[
            'lowercase',
        ],
    ))

    class Meta:
        index = 'test_analyzer_search'


class TestAnalyzerSearchIndex(BaseIndex):
    queryset = TestModel.objects.all()
    object_type = TestModel
    document_class = TestAnalyzerSearchDocument


class TestModelWithDependencyDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.String()

    class Meta:
        index = 'test_model_with_dependency_search'


class TestModelWithDependencySearchIndex(BaseIndex):
    queryset = TestModelWithDependency.objects.all().prefetch_related('dependencies')
    object_type = TestModelWithDependency
    document_class = TestModelWithDependencyDocument

    def get_dependencies(self):
        return [TestModelWithDependency.dependencies]

    def get_name_value(self, obj):
        names = [dep.name for dep in obj.dependencies.all()]
        return '{}: {}'.format(obj.name, ', '.join(names))


class TestModelWithFilterDependencyDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.String()

    class Meta:
        index = 'test_model_with_filter_dependency_search'


class FilterHelloDependency(ManyToManyDependency):
    def filter(self, obj, update_fields=None):
        return obj.name == 'hello'


class TestModelWithFilterDependencySearchIndex(BaseIndex):
    queryset = TestModelWithDependency.objects.all().prefetch_related('dependencies')
    object_type = TestModelWithDependency
    document_class = TestModelWithFilterDependencyDocument

    def get_dependencies(self):
        return [FilterHelloDependency(TestModelWithDependency.dependencies)]

    def get_name_value(self, obj):
        names = [dep.name for dep in obj.dependencies.all()]
        return '{}: {}'.format(obj.name, ', '.join(names))
