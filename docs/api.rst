===
API
===

The Resolwe framework provides a RESTful API through which most of its
functionality is exposed.

TODO

Elasticsearch endpoints
=======================

Advanced lookups
----------------

All fields that can be filtered upon (as defined for each viewset) support
specific lookup operators that can be used for some more advanced lookups.

Currently the supported lookup operators are:

* ``lt`` creates an ES range query with ``lt`` bound. Supported for number
  and date fields.
* ``lte`` creates an ES range query with ``lte`` bound. Supported for number
  and date fields.
* ``gt`` creates an ES range query with ``gt`` bound. Supported for number
  and date fields.
* ``gte`` creates an ES range query with ``gte`` bound. Supported for number
  and date fields.
* ``in`` creates an ES boolean query with all values passed as a should
  match. For GET requests, multiple values should be comma-separated.
* ``exact`` creates an ES query on the ``raw`` subfield of the given field,
  requiring the value to match exactly with the raw value that was supplied
  during indexing.

Limiting fields in responses
============================

As responses from the Resolwe API can contain a lot of data, especially with
nested JSON outputs and schemas, the API provides a way of limiting what is
returned with each response.

This is achieved through the use of a special ``fields`` GET parameter, which
can specify one or multiple field projections. Each projection defines what
should be returned. As a working example, let's assume we have the following
API response when no field projections are applied:

.. code:: json

    [
        {
            "foo": {
                "name": "Foo",
                "bar": {
                    "level3": 42,
                    "another": "hello"
                }
            },
            "name": "Boo"
        },
        {
            "foo": {
                "name": "Different",
            },
            "name": "Another"
        }
    ]

A field projection may reference any of the top-level fields. For example, by
using the ``fields=name`` projection, we get the following result:

.. code:: json

    [
        {
            "name": "Boo"
        },
        {
            "name": "Another"
        }
    ]

Basically all fields not matching the projection are gone. We can go further
and also project deeply nested fields, e.g., ``fields=foo__name``:

.. code:: json

    [
        {
            "foo": {
                "name": "Foo"
            }
        },
        {
            "foo": {
                "name": "Different"
            }
        }
    ]

And at last, we can combine multiple projections by separating them with commas,
e.g., ``fields=name,foo__name``, giving us:

.. code:: json

    [
        {
            "foo": {
                "name": "Foo"
            },
            "name": "Boo"
        },
        {
            "foo": {
                "name": "Different"
            },
            "name": "Another"
        }
    ]
