"""Elastic search fields for Resolwe."""
from __future__ import absolute_import, division, print_function, unicode_literals

import elasticsearch_dsl as dsl

# Process type analyzer. During indexing we tokenize by type paths, during search,
# we do not tokenize at all.
# pylint: disable=invalid-name
process_type_tokenizer = dsl.tokenizer('process_type_tokenizer', type='path_hierarchy', delimiter=':')
process_type_analyzer = dsl.analyzer('process_type_analyzer', tokenizer=process_type_tokenizer, filter=['lowercase'])
process_type_search_analyzer = dsl.analyzer('process_type_search_analyzer', tokenizer='keyword', filter=['lowercase'])
# pylint: enable=invalid-name


class RawStringSubfieldMixin(object):
    """String field with a 'raw' subfield (e.g. for sorting)."""

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('fields', {})['raw'] = {'type': 'string', 'index': 'not_analyzed'}
        super(RawStringSubfieldMixin, self).__init__(*args, **kwargs)


class Name(RawStringSubfieldMixin, dsl.String):
    """Field for names supporting term matches.

    Includes a 'raw' subfield for sorting.
    """

    pass


class ProcessType(RawStringSubfieldMixin, dsl.String):
    """Field for process type supporting hierarchical type matches.

    Includes a 'raw' subfield for sorting.
    """

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('analyzer', process_type_analyzer)
        kwargs.setdefault('search_analyzer', process_type_search_analyzer)
        super(ProcessType, self).__init__(*args, **kwargs)
