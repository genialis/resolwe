# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import elasticsearch_dsl as dsl

from resolwe.elastic.indices import BaseDocument, BaseIndex

from .models import TestModel


class TestSearchDocument(BaseDocument):
    # pylint: disable=no-member
    name = dsl.String()
    num = dsl.Integer()
    json = dsl.Object()

    class Meta:
        index = 'test_search'


class TestSearchIndex(BaseIndex):
    mapping = {
        'num': 'number',
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
