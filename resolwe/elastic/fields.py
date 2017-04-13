"""Elastic search fields for Resolwe."""
from __future__ import absolute_import, division, print_function, unicode_literals

import elasticsearch_dsl as dsl

# pylint: disable=invalid-name
# Process type analyzer. During indexing we tokenize by type paths, during search,
# we do not tokenize at all.
process_type_tokenizer = dsl.tokenizer('process_type_tokenizer', type='path_hierarchy', delimiter=':')
process_type_analyzer = dsl.analyzer('process_type_analyzer', tokenizer=process_type_tokenizer, filter=['lowercase'])
process_type_search_analyzer = dsl.analyzer('process_type_search_analyzer', tokenizer='keyword', filter=['lowercase'])

# Name analyzer.
name_analyzer = dsl.analyzer(
    'name_analyzer',
    type='pattern',
    # The pattern matches token separators.
    pattern=r'''
          ([^\p{L}\d]+)                 # swallow non letters and numbers,
        | (?<=\D)(?=\d)                 # or non-number followed by number,
        | (?<=\d)(?=\D)                 # or number followed by non-number,
    ''',
    flags='CASE_INSENSITIVE|COMMENTS',
    lowercase=True,
)
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

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('analyzer', name_analyzer)
        super(Name, self).__init__(*args, **kwargs)


class ProcessType(RawStringSubfieldMixin, dsl.String):
    """Field for process type supporting hierarchical type matches.

    Includes a 'raw' subfield for sorting.
    """

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('analyzer', process_type_analyzer)
        kwargs.setdefault('search_analyzer', process_type_search_analyzer)
        super(ProcessType, self).__init__(*args, **kwargs)
