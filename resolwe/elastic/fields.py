"""Elastic search fields for Resolwe."""
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

# During indexing, we lowercase terms and tokenize using edge_ngram.
ngrams_analyzer = dsl.analyzer(
    'ngrams_index',
    tokenizer='standard',
    filter=[
        'lowercase',
        dsl.token_filter(
            'ngrams_filter',
            type='edgeNGram',
            min_gram=1,
            max_gram=15,
        ),
    ],
)
# During search, we only lowercase terms.
ngrams_search_analyzer = dsl.analyzer(
    'ngrams_search',
    tokenizer='standard',
    filter=['lowercase'],
)
# pylint: enable=invalid-name


class RawKeywordSubfieldMixin:
    """String field with a 'raw' subfield (e.g. for sorting)."""

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('fields', {})['raw'] = {'type': 'keyword'}
        super().__init__(*args, **kwargs)


class NgramsSubfieldMixin:
    """String field with a 'ngrams' subfield (e.g. for autocomplete)."""

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('fields', {})['ngrams'] = {
            'type': 'text',
            'analyzer': ngrams_analyzer,
            'search_analyzer': ngrams_search_analyzer,
        }
        super().__init__(*args, **kwargs)


class Name(RawKeywordSubfieldMixin, NgramsSubfieldMixin, dsl.Text):
    """Field for names supporting term matches.

    Includes a 'raw' and 'ngrams' subfields for sorting and
    autocomplete.
    """

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('analyzer', name_analyzer)
        super().__init__(*args, **kwargs)


class ProcessType(RawKeywordSubfieldMixin, dsl.Text):
    """Field for process type supporting hierarchical type matches.

    Includes a 'raw' subfield for sorting.
    """

    def __init__(self, *args, **kwargs):
        """Construct field."""
        kwargs.setdefault('analyzer', process_type_analyzer)
        kwargs.setdefault('search_analyzer', process_type_search_analyzer)
        super().__init__(*args, **kwargs)


class User(NgramsSubfieldMixin, dsl.Text):
    """Field for users supporting partial matches."""


class Slug(NgramsSubfieldMixin, dsl.Keyword):
    """Field for slugs supporting partial matches."""
