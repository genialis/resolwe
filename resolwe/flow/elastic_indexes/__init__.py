"""Elastic search indexes for Resolwe models."""

from .collection import CollectionDocument, CollectionIndex
from .data import DataDocument, DataIndex
from .entity import EntityDocument, EntityIndex

__all__ = (
    "CollectionDocument",
    "CollectionIndex",
    "DataDocument",
    "DataIndex",
    "EntityDocument",
    "EntityIndex",
)
