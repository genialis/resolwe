==========================
Type extension composition
==========================

Many types that are part of the core Resolwe framework contain logic that
users of the framework may need to extend. To facilitate this in a controlled
manner, the Resolwe framework provides a generic type extension composition
system.

Making a type extendable
========================

The composition system is very generic and as such can be used on any type.
It provides a single method which allows you to retrieve a list of all
registered extensions for a type or an instance of that type.

.. code:: python

    >>> composer.get_extensions(my_type_or_instance)
    [<Extension1>, <Extension2>]

The type can then use this API to incorporate the registered extensions into
its current instance however it chooses. Note that what these extensions are
is entirely dependent upon the type that uses them.

For example, in the core Resolwe framework we make all index definitions
extendable by using something like:

.. code:: python

    for extension in composer.get_extensions(attr):
        mapping = getattr(extension, 'mapping', {})
        index.mapping.update(mapping)

Writng an extension
===================

On the other side, you can also define extensions for types that are using
the above mentioned API. All extensions are automatically discovered during
Django application registration if they are placed in a module called
``extensions`` in the given application.

Extensions can be registered using a simple API:

.. code:: python

    class MyExtension:
        pass

    composer.add_extension('fully.qualified.type.Path', MyExtension)

Again, what the extension is depends on the type that is being extended. Now
we describe some common extension types for types that are part of the Resolwe
core.

Data viewset
------------

It is possible to extend the filters of the ``Data`` viewset by defining an
extension as follows:

.. code:: python

    class ExtendedDataViewSet:
        """Data viewset extensions."""
        filtering_fields = ('source', 'species', 'build', 'feature_type')

        def text_filter(self, value):
            return [
                Q('match', species={'query': value, 'operator': 'and', 'boost': 2.0}),
                Q('match', source={'query': value, 'operator': 'and', 'boost': 2.0}),
                Q('match', build={'query': value, 'operator': 'and', 'boost': 2.0}),
                Q('match', feature_type={'query': value, 'operator': 'and', 'boost': 1.0}),
            ]
    composer.add_extension('resolwe.flow.views.data.DataViewSet', ExtendedDataViewSet)
