# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import serializers

from resolwe.elastic.viewsets import ElasticSearchBaseViewSet

from .elastic_indexes import TestSearchDocument


class TestSerializer(serializers.Serializer):  # pylint: disable=abstract-method
    name = serializers.CharField()
    num = serializers.IntegerField()
    json = serializers.JSONField(source='json.to_dict')


class TestViewSet(ElasticSearchBaseViewSet):

    document_class = TestSearchDocument

    serializer_class = TestSerializer

    filtering_fields = ('name',)
    ordering_fields = ('name')
    ordering = '-name'


class TestEmptyOrderingViewSet(ElasticSearchBaseViewSet):
    document_class = TestSearchDocument
    serializer_class = TestSerializer


class TestCustomFieldFilterViewSet(ElasticSearchBaseViewSet):
    document_class = TestSearchDocument
    serializer_class = TestSerializer
    filtering_fields = ('name',)

    def custom_filter_name(self, value, search):
        """Test for custom filter name, which queries the 'num' field instead of 'name'."""
        return search.query('match', num=value)
