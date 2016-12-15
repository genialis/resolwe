""".. Ignore pydocstyle D400.

===============
Elastic Indices
===============

Main two classes


.. autoclass:: resolwe.elastic.indices.BaseDocument
    :members:

.. autoclass:: resolwe.elastic.indices.BaseIndex
    :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from six import add_metaclass

import elasticsearch_dsl as dsl
from elasticsearch_dsl.document import DocTypeMeta

from django.conf import settings

from guardian.models import GroupObjectPermission, UserObjectPermission

from resolwe.flow.models import dict_dot


__all__ = ('BaseDocument', 'BaseIndex')


class BaseDocumentMeta(DocTypeMeta):
    """Meta class for ``BaseDocument``."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        """Wrapp index name into ``IndexPrefix`` and create new object."""
        if 'Meta' in namespace:
            index_prefix = getattr(settings, 'ELASTICSEARCH_INDEX_PREFIX', '')
            namespace['Meta'].index = index_prefix + namespace['Meta'].index
        return super(BaseDocumentMeta, mcs).__new__(mcs, name, bases, namespace, **kwargs)


@add_metaclass(BaseDocumentMeta)
class BaseDocument(dsl.DocType):
    """Base document class to build ElasticSearch documents.

    This is standard ``elasticsearch-dsl`` ``DocType`` class with
    already added fields for handling permissions.

    """

    #: list of user ids with view permission on the object
    users_with_permissions = dsl.String(multi=True)

    #: list of group ids with view permission on the object
    groups_with_permissions = dsl.String(multi=True)


class BaseIndex(object):
    """Base index class.

    Builds ElasticSearch index for specific type of objects. Index is
    based on document type defined in ``document_type``. Fields are
    determined from document and are populated with one of the following
    methods (in the exact order):

      * ``get_<field_name>_value`` method is used
      * ``mapping[<feild_name>]`` is used - if value is callable, it is
        called with current object as only argument
      * value is extracted from the object's field with the same name

    To make the index, caall ``run`` function. Index is build for all
    objects in queryset. To build index for just one object, specify it
    in ``obj`` parameter of ``run`` function.

    To work properly, subclass of this class must override following
    attributes:

      * object_type - class to which object must belong to be processed
      * document_class - subclass of :class:`BaseDocument` that is used
        to build actual index

    Additional (optional) methods and attributes that can be overriden
    are:

      * mapping - mapping for transforming object into index
      * :func:`~BaseIndex.preprocess_object`
      * :func:`~BaseIndex.filter`

    """

    #: queryset of objects to index
    queryset = None

    #: type of object that are indexed, i.e. Django model
    object_type = None

    #: document class used to create index
    document_class = None

    #: mapping used for building document
    mapping = {}

    def __init__(self):
        """Perform initial checks and save given object."""
        class_name = type(self).__name__
        if not self.object_type:
            raise RuntimeError('`object_type` must be defined in {}'.format(class_name))

        if not self.document_class:
            raise RuntimeError('`document_class` must be defined in {}'.format(class_name))

        if self.queryset is None:
            raise RuntimeError('`queryset` must be defined in {}'.format(class_name))

        self._index_name = self.document_class()._get_index()  # pylint: disable=not-callable,protected-access

    def filter(self, obj):
        """Determine if object should be processed.

        If ``False`` is returned, processingg of the current object will
        be aborted.
        """
        pass

    def preprocess_object(self, obj):
        """Preprocess object before indexing.

        This function is called before `func:process_object`. It can be
        used for advanced pre-processing of the object, i.e. adding
        annotations that will be used in multiple fields.
        """
        return obj

    def get_object_id(self, obj):
        """Return unique identifier of the object.

        Object's id is returned by default. This method can be overriden
        if object doesn't have ``id`` attribute.
        """
        return obj.id

    def generate_id(self, obj):
        """Generate unique document id for ElasticSearch."""
        object_type = type(obj).__name__.lower()
        return '{}_{}'.format(object_type, self.get_object_id(obj))

    def process_object(self, obj):
        """Process current object and push it to the ElasticSearch."""
        document = self.document_class(meta={'id': self.generate_id(obj)})  # pylint: disable=not-callable

        for field in document._doc_type.mapping:  # pylint: disable=protected-access
            if field in ['users_with_permissions', 'groups_with_permissions']:
                continue  # These fields are handled separately

            # use get_X_value function
            get_value_function = getattr(self, 'get_{}_value'.format(field), None)
            if get_value_function:
                setattr(document, field, get_value_function(obj))
                continue

            # use `mapping` dict
            if field in self.mapping:
                if callable(self.mapping[field]):
                    setattr(document, field, self.mapping[field](obj))
                    continue

                object_attr = dict_dot(obj, self.mapping[field])
                if callable(object_attr):
                    # use method on object
                    setattr(document, field, object_attr(obj))
                else:
                    # use attribute on object
                    setattr(document, field, object_attr)
                continue

            # get value from the object
            try:
                object_value = dict_dot(obj, field)
                setattr(document, field, object_value)
                continue
            except KeyError:
                pass

            raise AttributeError('Cannot determine mapping for field {}'.format(field))

        permissions = self.get_permissions(obj)
        document.users_with_permissions = permissions['users']
        document.groups_with_permissions = permissions['groups']

        document.save()

    def create_mapping(self):
        """Create the mappings in elasticsearch."""
        self.document_class.init()

    def build(self, obj=None):
        """Main function for building indexes."""
        # `.all()` forces new DB query
        if obj and obj not in self.queryset.all():
            return

        queryset = [obj] if obj else self.queryset.all()

        for obj in queryset:
            if self.filter(obj) is False:
                continue
            self.process_object(self.preprocess_object(obj))

    def get_permissions(self, obj):
        """Return users and groups with ``view`` permission on the current object.

        Return a dict with two keys - ``users`` and ``groups`` - which
        contain list of ids of users/groups with ``view`` permission.
        """
        # TODO: Optimize this for bulk running
        return {
            'users': list(UserObjectPermission.objects.filter(
                object_pk=obj.id, permission__codename__startswith='view'
            ).distinct('user').values_list('user_id', flat=True)),
            'groups': list(GroupObjectPermission.objects.filter(
                object_pk=obj.id, permission__codename__startswith='view'
            ).distinct('group').values_list('group', flat=True)),
        }

    def remove_object(self, obj):
        """Remove current object from the ElasticSearch."""
        obj_id = self.generate_id(obj)
        index = self.document_class.get(obj_id)
        index.delete()

    def search(self):
        """Return search query of document object."""
        return self.document_class.search()