# pylint: disable=missing-docstring
import elasticsearch_dsl as dsl

from resolwe.elastic.builder import ManyToManyDependency
from resolwe.elastic.fields import Name, ProcessType
from resolwe.elastic.indices import BaseDocument, BaseIndex

from .models import TestDependency, TestModel, TestModelWithDependency, TestSelfDependency


class TestSearchDocument(BaseDocument):
    # pylint: disable=no-member
    id = dsl.Integer()  # pylint: disable=invalid-name
    name = dsl.Text(fielddata=True)
    num = dsl.Integer()
    date = dsl.Date()
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
    name = dsl.Text(analyzer=dsl.analyzer(
        'test_analyzer',
        tokenizer='keyword',
        filter=[
            'lowercase',
        ],
        fielddata=True,
    ))

    class Meta:
        index = 'test_analyzer_search'


class TestAnalyzerSearchIndex(BaseIndex):
    queryset = TestModel.objects.all()
    object_type = TestModel
    document_class = TestAnalyzerSearchDocument


class TestModelWithDependencyDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.Text(fielddata=True)

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
    name = dsl.Text(fielddata=True)

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


class TestModelWithReverseDependencyDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.String()

    class Meta:
        index = 'test_model_with_reverse_dependency_search'


class TestModelWithReverseDependencySearchIndex(BaseIndex):
    queryset = TestDependency.objects.all()
    object_type = TestDependency
    document_class = TestModelWithReverseDependencyDocument

    def get_dependencies(self):
        return [TestDependency.testmodelwithdependency_set]  # pylint: disable=no-member

    def get_name_value(self, obj):
        names = [dep.name for dep in obj.testmodelwithdependency_set.all()]
        return '{}: {}'.format(obj.name, ', '.join(names))


class TestModelWithSelfDependencyDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.String()

    class Meta:
        index = 'test_model_with_self_dependency_search'


class TestModelWithSelfDependencySearchIndex(BaseIndex):
    queryset = TestSelfDependency.objects.all()
    object_type = TestSelfDependency
    document_class = TestModelWithSelfDependencyDocument

    def get_dependencies(self):
        return [TestSelfDependency.dependencies]

    def get_name_value(self, obj):
        names = [dep.name for dep in obj.dependencies.all().order_by('pk')]
        return '{}: {}'.format(obj.name, ', '.join(names))
