"""Permission storage model."""
import logging
from enum import IntEnum
from typing import List, Optional, Union

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.db import models

logger = logging.getLogger(__name__)


class Permission(IntEnum):
    """Permission values.

    There is no need for permission to have its own bit since permissions with
    higher numbers always include all lower permissions.
    """

    VIEW = 1
    EDIT = 2
    SHARE = 4
    OWNER = 8

    def has_permission(self, permission: "Permission") -> bool:
        """Return True if the given permission in included in current one."""
        return self >= permission

    @staticmethod
    def from_name(permission_name: str) -> "Permission":
        """Get the permission from name."""
        mapping = {
            "view": Permission.VIEW,
            "edit": Permission.EDIT,
            "share": Permission.SHARE,
            "owner": Permission.OWNER,
        }
        return mapping[permission_name]

    @staticmethod
    def highest_permission() -> "Permission":
        """Return the highest permission."""
        return Permission.OWNER

    def all_permissions(self) -> List[str]:
        """Get a list of all permissions (as strings) included in this one."""
        return [
            str(permission)
            for permission in Permission
            if permission.value <= self.value
        ]

    def all_permission_objects(self) -> List["Permission"]:
        """Get a list of all permissions (as objects) included in this one.

        The permissions are sorted from low to high.
        """
        return sorted(
            [permission for permission in Permission if permission.value <= self.value]
        )

    def __str__(self) -> str:
        """Get the string representation."""
        mapping = {
            Permission.VIEW: "view",
            Permission.EDIT: "edit",
            Permission.SHARE: "share",
            Permission.OWNER: "owner",
        }
        return mapping[self]


class PermissionModel(models.Model):
    """Permission on the permission group."""

    permission = models.PositiveSmallIntegerField()
    users = models.ManyToManyField(get_user_model(), related_name="permission_model")
    groups = models.ManyToManyField(Group(), related_name="permission_model")
    permission_group = models.ForeignKey(
        "PermissionGroup", on_delete=models.CASCADE, related_name="permissions"
    )

    def __str__(self) -> str:
        """Get the string representation."""
        return f"PermissionModel({self.id}, {Permission(self.permission)}, {self.users.all()}, {self.groups.all()})"


class PermissionGroup(models.Model):
    """Group of objecs sharing the same permission."""

    def __str__(self) -> str:
        """Get the string representation."""
        return f"PermissionGroup({self.id}, {self.permissions.all()})"


class PermissionObject(models.Model):
    """Base permission object.

    Every object that has permissions must inherit from this one.
    """

    class Meta:
        """PermissionObject meta options."""

        abstract = True

    #: permission group for the object
    permission_group = models.ForeignKey(
        PermissionGroup, on_delete=models.CASCADE, related_name="%(class)s", null=True
    )

    def __init__(self, *args, **kwargs):
        """Initialize."""
        # The properties used to determine when object is in the container. The
        # order of properties is important: properties listed sooner have
        # precedence over properties listed later when inheriting permission
        # group.
        self._container_properties = ("collection", "entity")
        self._container_attributes = dict()
        super().__init__(*args, **kwargs)
        self._refresh_container_attributes()

    def _refresh_container_attributes(self):
        """Refresh values of container properties.

        Store them into private attributes to avoid hitting the database each
        time we need them.
        """
        self._container_attributes = {
            container_property: getattr(self, f"{container_property}_id", None)
            for container_property in self._container_properties
        }

    def set_permission(
        self, permission: Optional[str], user_or_group: Union[User, Group]
    ):
        """Enable use of syntax obj.set_permission instead."""
        # Resolve circular import.
        from resolwe.permissions.utils import set_permission

        set_permission(permission, user_or_group, self)

    @property
    def topmost_container(self) -> Optional[models.Model]:
        """Get the top-most container.

        :returns: the top-most container or None if it does not exist.
        """
        for property in self._container_properties:
            value = getattr(self, property)
            if value is not None:
                return value
        return None

    def in_container(self, cached: bool = True) -> bool:
        """Return if object is contained in a container."""
        if cached:
            return any(self._container_attributes.values())
        else:
            return any(
                getattr(self, f"{container_property}_id", False)
                for container_property in self._container_properties
            )

    def save(self, *args, **kwargs):
        """Override the save method.

        When 'permission_group' attribute is not set create one. This could
        lead to a lot unassigned permission groups lying around.
        """
        # When object is contaied in the container it inherits the permission
        # group from the container. The outermost container has precedence.
        if self.in_container(cached=False):
            for container_property in self._container_properties:
                value = getattr(self, f"{container_property}_id", None)
                # Object was moved to a different container.
                if value != self._container_attributes[container_property]:
                    container = getattr(self, container_property)
                    self.permission_group_id = container.permission_group_id
                    # Add collection tags to data objects if they are moved to
                    # a new collection.
                    if (
                        self._meta.label in ["flow.Data", "flow.Entity"]
                        and container_property == "collection"
                    ):
                        self.tags = container.tags

                    # When moving entity all data objects in the entity must
                    # have the permission group set to the new one and tags
                    # updated.
                    if self._meta.label == "flow.Entity":
                        # Update permission group and tags.
                        self.data.update(
                            permission_group_id=container.permission_group_id
                        )
                        self.data.update(tags=container.tags)
                        self.data.update(collection_id=value)
                    break
                # Is this if necessary?
                # Object was moved to a different container.
                elif value is not None:
                    container = getattr(self, container_property)
                    self.permission_group_id = container.permission_group_id
                    break

            assert (
                self.permission_group_id is not None
            ), f"Container object of {self}({self._meta.label}) must have permission group set."
        elif self.in_container(cached=True):
            raise ValidationError("Object can only be moved to another container.")
        elif self.permission_group is None:
            self.permission_group = PermissionGroup.objects.create()

        super().save(*args, **kwargs)
        self._refresh_container_attributes()
