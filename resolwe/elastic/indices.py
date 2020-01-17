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
import copy
import logging
import threading

import elasticsearch_dsl as dsl
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.exceptions import IllegalOperation

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from django.db.models import Subquery

from django_priority_batch import PrioritizedBatcher
from guardian.conf.settings import ANONYMOUS_USER_NAME
from guardian.models import GroupObjectPermission, UserObjectPermission

from resolwe.flow.utils import dict_dot

from .composer import composer
from .utils import prepare_connection

__all__ = ("BaseDocument", "BaseIndex")

logger = logging.getLogger(__name__)


class BaseDocument(dsl.Document):
    """Base document class to build ElasticSearch documents.

    This is standard ``elasticsearch-dsl`` ``DocType`` class with
    already added fields for handling permissions.

    """

    #: list of user ids with view permission on the object
    users_with_permissions = dsl.Keyword(multi=True)

    #: list of group ids with view permission on the object
    groups_with_permissions = dsl.Keyword(multi=True)

    #: identifies if object has public view permission assigned
    public_permission = dsl.Boolean()


class BaseIndex:
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

    #: auto generated ES index postfix used in tests
    testing_postfix = ""

    #: mapping used for building document
    mapping = {}

    def __init__(self):
        """Perform initial checks and save given object."""
        class_name = type(self).__name__
        if not self.object_type:
            raise RuntimeError("`object_type` must be defined in {}".format(class_name))

        if not self.document_class:
            raise RuntimeError(
                "`document_class` must be defined in {}".format(class_name)
            )

        if self.queryset is None:
            raise RuntimeError("`queryset` must be defined in {}".format(class_name))

        # Apply any extensions defined for the given document class. Document class extensions
        # come in the form of field definitions.
        self.document_class = copy.deepcopy(self.document_class)
        for extension in composer.get_extensions(self.document_class):
            for name in dir(extension):
                field = getattr(extension, name)
                if isinstance(field, dsl.Field):
                    self.document_class._doc_type.mapping.field(name, field)

        #: list of built documents waiting to be pushed
        self.push_queue = []

        self._index_name = self.document_class()._get_index()
        self._mapping_created = False

        #: id of thread id where connection was established
        self.connection_thread_id = None

    def _refresh_connection(self):
        """Refresh connection to Elasticsearch when worker is started.

        File descriptors (sockets) can be shared between multiple
        threads. If same connection is used by multiple threads at the
        same time, this can cause timeouts in some of the pushes. So
        connection needs to be reestablished in each thread to make sure
        that it is unique per thread.
        """
        # Thread with same id can be created when one terminates, but it
        # is ok, as we are only concerned about concurent pushes.
        current_thread_id = threading.current_thread().ident

        if current_thread_id != self.connection_thread_id:
            prepare_connection()

            self.connection_thread_id = current_thread_id

    def filter(self, obj):
        """Determine if object should be processed.

        If ``False`` is returned, processingg of the current object will
        be aborted.
        """

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
        return "{}_{}".format(object_type, self.get_object_id(obj))

    def process_object(self, obj):
        """Process current object and push it to the ElasticSearch."""
        document = self.document_class(meta={"id": self.generate_id(obj)})

        for field in document._doc_type.mapping:
            if field in [
                "users_with_permissions",
                "groups_with_permissions",
                "public_permission",
            ]:
                continue  # These fields are handled separately

            try:
                # use get_X_value function
                get_value_function = getattr(self, "get_{}_value".format(field), None)
                if get_value_function:
                    setattr(document, field, get_value_function(obj))
                    continue

                # use `mapping` dict
                if field in self.mapping:
                    if callable(self.mapping[field]):
                        setattr(document, field, self.mapping[field](obj))
                        continue

                    try:
                        object_attr = dict_dot(obj, self.mapping[field])
                    except (KeyError, AttributeError):
                        object_attr = None

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

                raise AttributeError(
                    "Cannot determine mapping for field {}".format(field)
                )

            except Exception:
                logger.exception(
                    "Error occurred while setting value of field '%s' in '%s' Elasticsearch index.",
                    field,
                    self.__class__.__name__,
                    extra={"object_type": self.object_type, "obj_id": obj.pk},
                )

        permissions = self.get_permissions(obj)
        document.users_with_permissions = permissions["users"]
        document.groups_with_permissions = permissions["groups"]
        document.public_permission = permissions["public"]

        self.push_queue.append(document)

    def create_mapping(self):
        """Create the mappings in elasticsearch."""
        if self._mapping_created:
            return

        logger.debug(
            "Pushing mapping for Elasticsearch index '%s'.", self.__class__.__name__
        )
        try:
            self.document_class.init()
            self._mapping_created = True
        except IllegalOperation as error:
            if error.args[0].startswith("You cannot update analysis configuration"):
                # Ignore mapping update errors, which are thrown even when the analysis
                # configuration stays the same.
                # TODO: Remove this when https://github.com/elastic/elasticsearch-dsl-py/pull/272 is merged.
                return

            raise

    def build(self, obj=None, queryset=None, push=True):
        """Build indexes."""
        if obj is not None and queryset is not None:
            raise ValueError(
                "Only one of 'obj' and 'queryset' parameters can be passed to the build method."
            )

        if obj is not None:
            if self.queryset.model != obj._meta.model:
                logger.debug(
                    "Object type mismatch, skipping build of '%s' Elasticsearch index.",
                    self.__class__.__name__,
                )
                return

            if not self.queryset.filter(pk=self.get_object_id(obj)).exists():
                logger.debug(
                    "Object not in predefined queryset, skipping build of '%s' Elasticsearch index.",
                    self.__class__.__name__,
                )
                return

        elif queryset is not None:
            if self.queryset.model != queryset.model:
                logger.debug(
                    "Queryset type mismatch, skipping build of '%s' Elasticsearch index.",
                    self.__class__.__name__,
                )
                return

        FULL_REBUILD = "full"

        def handler(agg=None):
            """Index build handler."""
            if agg == FULL_REBUILD:
                queryset = self.queryset.all()
            else:
                queryset = self.queryset.none().union(*agg)

            self._build(queryset=queryset, push=push)

        def aggregator(agg=None):
            """Index build aggregator."""
            if agg == FULL_REBUILD:
                # A full rebuild is required, ignore any other builds.
                pass
            else:
                if agg is None:
                    agg = []

                if obj is not None:
                    # Build of a single object.
                    agg.append(self.queryset.filter(pk=obj.pk))
                elif queryset is not None:
                    # Build of multiple objects.
                    agg.append(queryset)
                else:
                    # Full rebuild, ignore any other builds.
                    agg = FULL_REBUILD

            return agg

        batcher = PrioritizedBatcher.global_instance()
        if batcher.is_started:
            batcher.add(
                "resolwe.elastic",
                handler,
                group_by=(self._index_name, push),
                aggregator=aggregator,
            )
        else:
            self._build(obj=obj, queryset=queryset, push=push)

    def _build(self, obj=None, queryset=None, push=True):
        """Build indexes."""
        logger.debug("Building '%s' Elasticsearch index...", self.__class__.__name__)

        if obj is not None:
            build_list = [obj]

        elif queryset is not None:
            build_list = self.queryset.intersection(queryset)

            logger.debug("Found %s elements to build.", build_list.count())

        else:
            build_list = self.queryset.all()

            logger.debug("Found %s elements to build.", build_list.count())

        for obj in build_list:
            if self.filter(obj) is False:
                continue

            try:
                obj = self.preprocess_object(obj)
            except Exception:
                logger.exception(
                    "Error occurred while preprocessing '%s' Elasticsearch index.",
                    self.__class__.__name__,
                    extra={"object_type": self.object_type, "obj_id": obj.pk},
                )

            try:
                self.process_object(obj)
            except Exception:
                logger.exception(
                    "Error occurred while processing '%s' Elasticsearch index.",
                    self.__class__.__name__,
                    extra={"object_type": self.object_type, "obj_id": obj.pk},
                )

        logger.debug(
            "Finished building '%s' Elasticsearch index.", self.__class__.__name__
        )

        if push:
            self.push()

    def push(self):
        """Push built documents to ElasticSearch."""
        self._refresh_connection()
        self.create_mapping()

        if not self.push_queue:
            logger.debug("No documents to push, skipping push.")
            return

        logger.debug(
            "Found %s documents to push to Elasticsearch.", len(self.push_queue)
        )

        bulk(
            connections.get_connection(),
            (doc.to_dict(True) for doc in self.push_queue),
            refresh=True,
        )
        self.push_queue = []

        logger.debug("Finished pushing builded documents to Elasticsearch server.")

    def destroy(self):
        """Destroy an index."""
        self._refresh_connection()

        self.push_queue = []
        index_name = self.document_class()._get_index()
        connections.get_connection().indices.delete(index_name, ignore=404)

        self._mapping_created = False

    def get_permissions(self, obj):
        """Return users and groups with ``view`` permission on the current object.

        Return a dict with two keys - ``users`` and ``groups`` - which
        contain list of ids of users/groups with ``view`` permission.
        """
        # TODO: Optimize this for bulk running
        content_type = ContentType.objects.get_for_model(obj)
        permissions_subquery = Subquery(
            # Override the default ordering to simplify the query.
            Permission.objects.filter(
                content_type=content_type, codename__startswith="view",
            )
            .order_by()
            .values("pk")
        )
        public_user_subquery = Subquery(
            get_user_model().objects.filter(username=ANONYMOUS_USER_NAME).values("pk")
        )

        # NOTE: Django-guardian has a combined database index on
        # (object_pk, content_type), so we have to filter by both to
        # take the advantage of it.
        filters = {
            "object_pk": obj.id,
            "permission": permissions_subquery,
            "content_type": content_type,
        }
        return {
            "users": list(
                UserObjectPermission.objects.filter(**filters).values_list(
                    "user_id", flat=True
                )
            ),
            "groups": list(
                GroupObjectPermission.objects.filter(**filters).values_list(
                    "group", flat=True
                )
            ),
            "public": UserObjectPermission.objects.filter(
                user=public_user_subquery, **filters
            ).exists(),
        }

    def get_dependencies(self):
        """Return dependencies, which should trigger updates of this index."""
        return []

    def remove_object(self, obj):
        """Remove current object from the ElasticSearch."""
        obj_id = self.generate_id(obj)
        es_obj = self.document_class.get(obj_id, ignore=[404])
        # Object may not exist in this index.
        if es_obj:
            es_obj.delete(refresh=True)

    def search(self):
        """Return search query of document object."""
        self.create_mapping()
        return self.document_class.search()
