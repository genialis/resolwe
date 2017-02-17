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

DescriptorSchema model
======================

Postgres ORM model for storing descriptors.

.. autoclass:: resolwe.flow.models.DescriptorSchema
    :members:

Process model
=============

Postgress ORM model for storing processes.

.. autoclass:: resolwe.flow.models.Process
    :members:

Storage model
=============

Postgres ORM model for storing JSON.

.. autoclass:: resolwe.flow.models.Storage
    :members:

"""

from .collection import Collection
from .data import Data
from .descriptor import DescriptorSchema
from .entity import Entity, Relation, RelationType
from .process import Process
from .storage import Storage
