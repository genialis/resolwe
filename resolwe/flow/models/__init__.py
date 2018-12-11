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

.. autoclass:: resolwe.flow.models.DataLocation
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

"""

from .collection import Collection
from .data import Data, DataDependency, DataLocation
from .descriptor import DescriptorSchema
from .entity import Entity, Relation, RelationType
from .migrations import DataMigrationHistory, ProcessMigrationHistory
from .process import Process
from .secret import Secret
from .storage import Storage
