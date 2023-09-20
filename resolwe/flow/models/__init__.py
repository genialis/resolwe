""".. Ignore pydocstyle D400.

===========
Flow Models
===========

Base Model
==========

Base model for all other models.

.. autoclass:: resolwe.flow.models.base.BaseModel
    :members:

Collection Model
================

Postgres ORM model for the organization of collections.

.. autoclass:: resolwe.flow.models.collection.BaseCollection
    :members:

.. autoclass:: resolwe.flow.models.Collection
    :members:

Data model
==========

Postgres ORM model for keeping the data structured.

.. autoclass:: resolwe.flow.models.Data
    :members:

.. autoclass:: resolwe.flow.models.DataDependency
    :members:

Entity–relationship model
=========================

Postgres ORM to define the entity–relationship model that describes how
data objects are related in a specific domain.

.. autoclass:: resolwe.flow.models.Entity
    :members:

.. autoclass:: resolwe.flow.models.Relation
    :members:

.. autoclass:: resolwe.flow.models.RelationType
    :members:

DescriptorSchema model
======================

Postgres ORM model for storing descriptors.

.. autoclass:: resolwe.flow.models.DescriptorSchema
    :members:

Process model
=============

Postgres ORM model for storing processes.

.. autoclass:: resolwe.flow.models.Process
    :members:

Storage model
=============

Postgres ORM model for storing JSON.

.. autoclass:: resolwe.flow.models.Storage
    :members:

Secret model
============

Postgres ORM model for storing secrets.

.. autoclass:: resolwe.flow.models.Secret

ProcessMigrationHistory model
=============================

Postgres ORM model for storing proces migration history.

.. autoclass:: resolwe.flow.models.ProcessMigrationHistory
    :members:

DataMigrationHistory model
==========================

Postgres ORM model for storing data migration history.

.. autoclass:: resolwe.flow.models.DataMigrationHistory
    :members:

AnnotationGroup model
==========================

Postgres ORM model for storing annotation group data.

.. autoclass:: resolwe.flow.models.annotations.AnnotationGroup
    :members:

AnnotationField model
==========================

Postgres ORM model for storing annotation field data.

.. autoclass:: resolwe.flow.models.annotations.AnnotationField
    :members:

AnnotationPreset model
==========================

Postgres ORM model for storing annotation presets data.

.. autoclass:: resolwe.flow.models.annotations.AnnotationPreset
    :members:

AnnotationValue model
==========================

Postgres ORM model for storing annotation values.

.. autoclass:: resolwe.flow.models.annotations.AnnotationValue
    :members:

History model
=============

Basic model for history tracking.

.. autoclass:: resolwe.flow.models.history.History
    :members:

DataHistory model
=================

Model for tracking changes in Data objects.

.. autoclass:: resolwe.flow.models.history.DataHistory
    :members:

    
CollectionHistory model
=======================

Model for tracking changes in Collection objects.

.. autoclass:: resolwe.flow.models.history.CollectionHistory
    :members:

TrackChange model
=================

Basic model to track changes to particular model fields.

.. autoclass:: resolwe.flow.models.history.TrackChange
    :members:

DataSlugChange model
====================

Track data slug changes.

.. autoclass:: resolwe.flow.models.history.DataSlugChange
    :members:

CollectionSlugChange model
==========================

Track collection slug changes.

.. autoclass:: resolwe.flow.models.history.CollectionSlugChange
    :members:

DataNameChange model
====================

Track data name changes.

.. autoclass:: resolwe.flow.models.history.DataNameChange
    :members:

CollectionNameChange model
==========================

Track collection name changes.

.. autoclass:: resolwe.flow.models.history.CollectionNameChange
    :members:

    
CollectionChange model
======================

Track collection changes.

.. autoclass:: resolwe.flow.models.history.CollectionChange
    :members:

SizeChange model
================

Track size changes.

.. autoclass:: resolwe.flow.models.history.SizeChange
    :members:

ProcessingHistory model
=======================

Track processing data objects.

.. autoclass:: resolwe.flow.models.history.ProcessingHistory
    :members:


Utility functions
=================

.. autofunction:: resolwe.flow.models.utils.duplicate.bulk_duplicate



"""

from .annotations import (
    AnnotationField,
    AnnotationGroup,
    AnnotationPreset,
    AnnotationValue,
)
from .collection import Collection
from .data import Data, DataDependency
from .descriptor import DescriptorSchema
from .entity import Entity, Relation, RelationPartition, RelationType
from .history import (
    CollectionHistory,
    CollectionNameChange,
    CollectionSlugChange,
    DataHistory,
    DataNameChange,
    DataSlugChange,
)
from .migrations import DataMigrationHistory, ProcessMigrationHistory
from .process import Process
from .secret import Secret
from .storage import Storage
from .worker import Worker

__all__ = (
    "AnnotationGroup",
    "AnnotationPreset",
    "AnnotationField",
    "AnnotationValue",
    "Collection",
    "CollectionHistory",
    "CollectionNameChange",
    "CollectionSlugChange",
    "Data",
    "DataHistory",
    "DataDependency",
    "DataMigrationHistory",
    "DataNameChange",
    "DataSlugChange",
    "DescriptorSchema",
    "Entity",
    "Process",
    "ProcessMigrationHistory",
    "Relation",
    "RelationPartition",
    "RelationType",
    "Secret",
    "Storage",
    "Worker",
)
