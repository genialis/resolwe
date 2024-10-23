"""Permission storage model."""

import logging
from enum import IntEnum
from functools import reduce
from typing import Generic, Iterable, List, Optional, TypeVar, Union

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group, User
from django.db import models, transaction

from resolwe.observers.protocol import post_permission_changed, pre_permission_changed

UserOrGroup = Union[User, Group]
M = TypeVar("M", bound=models.Model)
MP = TypeVar("MP", bound="PermissionInterface")
Q = TypeVar("Q", bound=models.QuerySet)


logger = logging.getLogger(__name__)
PermissionList = List["Permission"]


# Get and store the anonymous user for later use to avoid hits to the database.
ANONYMOUS_USER = None


def get_anonymous_user(cache=True) -> User:
    """Get the anonymous user.

    Note that is the actual user object with username specified in setting
    ANONYMOUS_USER_NAME or id specified in setting ANONYMOUS_USER_ID. The later
    setting has precedence.

    Store the computed value into global variable get_anonymous_user() to avoid
    querying the database every time, unless the cache parameter is False.

    :raises RuntimeError: when ANONYMOUS_USER_NAME and ANONYMOUS_USER_ID are
        not set.
    """
    global ANONYMOUS_USER
    if ANONYMOUS_USER is None or not cache:
        ANONYMOUS_USER = get_user_model().objects.get(
            username=settings.ANONYMOUS_USER_NAME
        )
    return ANONYMOUS_USER


class Permission(IntEnum):
    """Enum that describes all possible permissions on Resolwe objects.

    The permissions on Resolwe objects are ordered linearly so they can be
    mapped to natural numbers. Permissions that are mapped to lower numbers are
    implicitely included in the permissions mapped to a higher number.

    Whenever dealing with permissions (reading, setting...) it is necessary to
    use this enum.

    The instances of Permission class are iterable and iterating over an
    instance P returs all permissions that are lower or equal to P, ordered
    from bottom to top.
    """

    # Possible permission levels. They must be orded from bottom to top since
    # methods bellow rely on the ordering.
    NONE = 0
    VIEW = 1
    EDIT = 2
    SHARE = 4
    OWNER = 8

    @staticmethod
    def from_name(permission_name: str) -> "Permission":
        """Get the permission from permission name.

        :returns: Permission object when permission_name is the name of the
            permission and highest permission if permission_name is 'ALL'.

        :raises KeyError: when permission name is not known.
        """
        if permission_name == "ALL":
            return Permission.highest()
        return Permission[permission_name.upper()]

    @staticmethod
    def highest() -> "Permission":
        """Return the highest permission."""
        return list(Permission)[-1]

    def __iter__(self):
        """Iterate through permission in increasing order.

        When iterating over permission instance the permissions included in the
        current one (including permission itself) are returned (in increasing
        order).

        The permission Permission.NONE is excluded from the listing.
        """
        for permission in Permission:
            if 0 < permission.value <= self.value:
                yield permission

    def __reversed__(self):
        """Iterate through permissions in decreasing order.

        When iterating over permission instance the permissions included in the
        current one (including permission itself) are returned (in decreasing
        order).

        The permission Permission.NONE is excluded from the listing.
        """
        for permission in reversed(Permission):
            if 0 < permission.value <= self.value:
                yield permission

    def __str__(self) -> str:
        """Get the string representation of the permission.

        This is used in serialization so it must be equal to 'view', 'edit',
        'share' and 'owner'.
        """
        return self.name.lower()


class PermissionQuerySet(models.QuerySet[MP]):
    """Queryset with methods that simlify filtering by permissions."""

    def _check_permissions(self):
        """Raise exception if the model does not support permissions."""
        if not issubclass(self.model, PermissionInterface):
            raise TypeError(
                f"Permissions are not supported on model `{self.model._meta.label}`."
            )

    def _filter_by_permission(
        self,
        user: Optional[User],
        groups: models.QuerySet,
        permission: "Permission",
        public: bool = True,
        with_superuser: bool = True,
    ) -> "PermissionQuerySet":
        """Filter queryset by permissions.

        This is a generic method that is called in public methods.

        :attr user: the user which permissions should be considered.

        :attr groups: the groups which permissions should be considered.

        :attr permission: the lowest permission entity must have.

        :attr public: when True consider also public permission.

        :attr with_superuser: when false treat superuser as reguar user.
        """

        # Skip filtering for superuser when with_superuser is set.
        if user is not None and user.is_superuser and with_superuser:
            return self

        filters = dict()
        permission_group_path = self.model.permission_group_path()

        if user:
            filters["user"] = models.Q(
                **{
                    f"{permission_group_path}__permissions__user": user,
                    f"{permission_group_path}__permissions__value__gte": permission,
                }
            )

        if public:
            filters["public"] = models.Q(
                **{
                    f"{permission_group_path}__permissions__user": get_anonymous_user(),
                    f"{permission_group_path}__permissions__value__gte": permission,
                }
            )
        if groups:
            filters["groups"] = models.Q(
                **{
                    f"{permission_group_path}__permissions__group__in": groups.values_list(
                        "pk", flat=True
                    ),
                    f"{permission_group_path}__permissions__value__gte": permission,
                }
            )

        # List here is needed otherwise more joins are performed on the query
        # bellow. Some Django queries (for example ExpressionLateralJoin) do
        # not like that and will fail without evaluating the ids query first.
        ids = list(
            self.filter(
                reduce(lambda filters, filter: filters | filter, filters.values())
            )
            .distinct()
            .values_list("pk", flat=True)
        )
        return self.filter(id__in=ids)

    def filter_for_user(
        self,
        user: User | AnonymousUser,
        permission: Permission = Permission.VIEW,
        use_groups: bool = True,
        public: bool = True,
        with_superuser: bool = True,
    ) -> "PermissionQuerySet":
        """Filter objects for user.

        :attr user: the user which permissions should be considered.

        :attr permission: the lowest permission entity must have.

        :attr use_groups: when True consider also permissions of the user groups.

        :attr public: when True consider also public permission.

        :attr with_superuser: when false treat superuser as reguar user.
        """

        self._check_permissions()

        from resolwe.permissions.utils import get_user  # Circular import

        user = get_user(user)
        groups = user.groups.all() if use_groups else user.groups.none()

        return self._filter_by_permission(
            user, groups, permission, public, with_superuser
        )

    def set_permission(self, permission: Permission, user_or_group: UserOrGroup):
        """Set the permission on the objects in the queryset."""
        from resolwe.permissions.utils import get_identity  # Circular import

        self._check_permissions()

        if any(datum for datum in self if not datum.can_set_permission()):
            ids = self.values_list("pk", flat=True)
            raise RuntimeError(
                f"The permissions can not be set on objects with ids {ids} of type {self.model}."
            )
        entity, entity_type = get_identity(user_or_group)
        for permission_group_id in self.values_list(
            self.model.permission_group_path(), flat=True
        ).distinct():
            PermissionModel.all_objects.update_or_create(
                **{"permission_group_id": permission_group_id, entity_type: entity},
                defaults={"value": permission.value},
            )


class PermissionManager(models.Manager[M], Generic[M, Q]):
    """The manager used for permission objects."""

    # The queryset to use.
    QuerySet: type[Q] = PermissionQuerySet  # type: ignore

    def get_queryset(self) -> Q:
        """Return the queryset."""
        return self.QuerySet(model=self.model, using=self._db)

    def filter_for_user(
        self,
        user: User | AnonymousUser,
        permission: Permission = Permission.VIEW,
        use_groups: bool = True,
        public: bool = True,
        with_superuser: bool = True,
    ) -> Q:
        """Filter the objects for user."""
        return self.get_queryset().filter_for_user(
            user, permission, use_groups, public, with_superuser
        )


class PositivePermissionsManager(models.Manager):
    """Return only PermissionModels with values greater than 0."""

    def get_queryset(self) -> models.QuerySet:
        """Override default queryset."""
        return super().get_queryset().filter(value__gt=0)


class PermissionModel(models.Model):
    """Store a permission for a singe user/group on permission group.

    Exactly one of fields user/group must be non-null.
    """

    #: permission value
    value = models.PositiveSmallIntegerField()

    #: user this permission belongs to
    user = models.ForeignKey(
        get_user_model(),
        related_name="model_permissions",
        on_delete=models.CASCADE,
        null=True,
    )

    #: group this permission belongs to
    group = models.ForeignKey(
        Group, related_name="model_permissions", on_delete=models.CASCADE, null=True
    )

    #: permission group this permission belongs to
    permission_group = models.ForeignKey(
        "PermissionGroup", on_delete=models.CASCADE, related_name="permissions"
    )

    # Access all permissions, including the ones with 0 permission.
    all_objects = models.Manager()

    # By default return only positive permissions.
    objects = PositivePermissionsManager()

    class Meta:
        """Define constraints enforced on the model.

        The tripple (value, permission_group, user/group) must be unique.
        """

        constraints = [
            models.UniqueConstraint(
                fields=["permission_group", "value", "user"],
                name="one_permission_per_user",
                condition=models.Q(user__isnull=False),
            ),
            models.UniqueConstraint(
                fields=["permission_group", "value", "group"],
                name="one_permission_per_group",
                condition=models.Q(group__isnull=False),
            ),
            models.CheckConstraint(
                check=models.Q(user__isnull=False, group__isnull=True)
                | models.Q(user__isnull=True, group__isnull=False),
                name="exactly_one_of_user_group_must_be_set",
            ),
        ]

    @property
    def permission(self) -> Permission:
        """Return the permission object associated with this instance."""
        return Permission(self.value)

    @property
    def permissions(self) -> PermissionList:
        """Return the permission objects associated with this instance."""
        return list(self.permission)

    def __str__(self) -> str:
        """Get the string representation used for debugging."""
        return (
            f"PermissionModel({self.id}, {Permission(self.permission)}, "
            f"user: {self.user}, group: {self.group})"
        )


class PermissionGroup(models.Model):
    """Group of objecs that have the same permissions.

    Example: a container and all its contents have same permission group.
    """

    def __str__(self) -> str:
        """Return the string representation used for debugging purposes."""
        return f"PermissionGroup({self.id}, {self.permissions.all()})"

    @transaction.atomic
    def set_permission(self, permission: Permission, user_or_group: UserOrGroup):
        """Set the given permission on this permission group.

        All previous permissions are removed.

        :raises AssertionError: when no user or group is given.
        """
        from resolwe.permissions.utils import get_identity  # Circular import

        entity, entity_name = get_identity(user_or_group)
        self.permissions.update_or_create(
            **{"permission_group": self, entity_name: entity},
            defaults={"value": permission.value},
        )

    def get_permission(self, user_or_group: UserOrGroup) -> Permission:
        """Get the permission for the given user or group."""
        from resolwe.permissions.utils import get_identity  # Circular import

        entity, entity_name = get_identity(user_or_group)

        # Superuser has all the permissions.
        if entity_name == "user" and entity.is_superuser:  # type: ignore
            return Permission.highest()

        permission_model = self.permissions.filter(**{entity_name: entity}).first()
        if permission_model is not None:
            return permission_model.permission
        else:
            return Permission.NONE

    def users_with_permission(
        self, permission: Permission, with_superusers=False
    ) -> List[User]:
        """Get a list of users with at least this permission level.

        Calling this with Permission.NONE will return users for whom an explicit
        PermissionModel with Permission.NONE exists.

        :attr permission: the permission level user must have.
        :attr with_superusers: should superusers be included in the returned list.
        """
        filtered_permissions = self.permissions.filter(value__gte=permission)
        filter = models.Q(
            groups__in=filtered_permissions.values_list("group", flat=True)
        ) | models.Q(pk__in=filtered_permissions.values_list("user", flat=True))
        if with_superusers:
            filter |= models.Q(is_superuser=True)
        return list(get_user_model().objects.filter(filter).distinct())

    def groups_with_permission(self, permission: Permission) -> List[Group]:
        """Get a list of groups with at least this permission level.

        Calling this with Permission.NONE will return groups for which an explicit
        PermissionModel with Permission.NONE exists.

        :attr permission: the permission level group must have.
        """
        filtered_permissions = self.permissions.filter(value__gte=permission)
        filter = models.Q(pk__in=filtered_permissions.values_list("group", flat=True))
        return list(Group.objects.filter(filter).distinct())


class PermissionInterface(models.Model):
    """The abstract model that defines permission interface.

    When a model level permissions are needed, the model mus inherit from this class.
    """

    #: custom manager with permission filtering methods.
    objects: PermissionManager = PermissionManager()

    class Meta:
        """Make a class abstract."""

        abstract = True

    @property
    def permission_group(self) -> PermissionGroup:
        """Return a permission group object."""
        raise NotImplementedError("The method permission_group must be implemented.")

    @classmethod
    def permission_proxy(cls) -> Optional[str]:
        """Return the path to the permission permission proxy.

        When None, the model has no permission proxy.
        """
        raise NotImplementedError(
            "The method permission_group_path must be implemented."
        )

    @classmethod
    def permission_group_path(cls) -> str:
        """Return the path to the permission group."""
        base_path = "permission_group"
        proxy = cls.permission_proxy()
        return base_path if proxy is None else f"{proxy}__{base_path}"

    def can_set_permission(self) -> bool:
        """Can permission be set on this object."""
        return self.permission_proxy is None

    def is_owner(self, user: User) -> bool:
        """Return if user is the owner of this instance."""
        return self.has_permission(Permission.OWNER, user)

    def has_permission(self, permission: Permission, user: User):
        """Check if user has the given permission on the current object."""
        return (
            self._meta.model.objects.filter(pk=self.pk)
            .filter_for_user(user, permission)
            .exists()
        )

    def set_permission(self, permission: Permission, user_or_group: UserOrGroup):
        """Set permission on this instance.

        It performs additional check if permissions can be set on this object.

        :raises RuntimeError: when given object has no permission_group or is
            contained in a container.
        """
        if not self.can_set_permission():
            raise RuntimeError(
                f"The permissions can not be set on object {self} with id {self.pk} ({self._meta.label})."
            )

        pre_permission_changed.send(sender=type(self), instance=self)
        self.permission_group.set_permission(permission, user_or_group)
        post_permission_changed.send(sender=type(self), instance=self)

    def get_permission(self, user_or_group: UserOrGroup) -> Permission:
        """Get permission for given user or group on this instance."""
        return self.permission_group.get_permission(user_or_group)

    def get_permissions(self, user_or_group: UserOrGroup) -> PermissionList:
        """Get a list of all permissions on the object.

        The Permission.NONE is excluded from list.
        """
        permission = self.get_permission(user_or_group)
        if permission:
            return list(permission)
        else:
            return []

    def users_with_permission(
        self, permission: Permission, with_superusers=False
    ) -> List[User]:
        """Get a list of users with at least this permission level.

        Calling this with Permission.NONE will return users for whom an explicit
        PermissionModel with Permission.NONE exists.

        :attr permission: the permission level user must have.
        :attr with_superusers: should superusers be included in the returned list.
        """
        return self.permission_group.users_with_permission(permission, with_superusers)

    def groups_with_permission(self, permission: Permission) -> List[Group]:
        """Get a list of groups with at least this permission level.

        Calling this with Permission.NONE will return groups for which an explicit
        PermissionModel with Permission.NONE exists.

        :attr permission: the permission level group must have.
        """
        return self.permission_group.groups_with_permission(permission)


class PermissionObject(PermissionInterface):
    """Base permission object.

    Every object that has permissions must inherit from this one.
    """

    #: permission group for the object
    permission_group = models.ForeignKey(
        PermissionGroup, on_delete=models.CASCADE, related_name="%(class)s", null=True
    )

    class Meta:
        """Make this class abstract so no new table is created for it."""

        abstract = True

    def __init__(self, *args, **kwargs):
        """Initialize."""
        # The properties used to determine if object is in container.
        self._container_properties = ("collection", "entity")
        super().__init__(*args, **kwargs)

    @classmethod
    def permission_proxy(cls) -> Optional[str]:
        """Return the path where permission_group model is accessible."""
        return None

    @property
    def topmost_container(self) -> Optional[models.Model]:
        """Get the top-most container of the object.

        :returns: the top-most container or None if it does not exist.
        """
        for property in self._container_properties:
            value = getattr(self, property, None)
            if value is not None:
                return value
        return None

    @property
    def containers(self) -> Iterable[models.Model]:
        """Get the sequence of containers this object is in.

        :returns: a sequence of containers (may be empty) this object is in.
        """
        containers = (getattr(self, prop, None) for prop in self._container_properties)
        return [container for container in containers if container is not None]

    def in_container(self) -> bool:
        """Return if object lies in a container."""
        return self.topmost_container is not None

    def can_set_permission(self) -> bool:
        """Can permissions be set on this object."""
        return not self.in_container()

    def save(self, *args, **kwargs):
        """Set the permission_group property of the object.

        If object belongs to a container set its permission group to the one
        used by the container.

        If object does not belong to a container and has no permission group
        create a new one and assign it to the object.

        Mental note: this could lead to orphaned PermissionGroup objects lying
        around.
        """
        container = self.topmost_container
        if container is not None:
            self.permission_group_id = container.permission_group_id
        elif self.permission_group is None:
            self.permission_group = PermissionGroup.objects.create()
        # The instance was not yet commited to the database and permission
        # group attribute has been set.
        # Be careful: this can happen on rollback due to slug colision on base
        # object. In this case the property is set to a permission group, which
        # is not commited to the database due to slug colision.
        # We have to create a new one.
        elif self.pk is None:
            try:
                self.permission_group.refresh_from_db()
            except PermissionGroup.DoesNotExist:
                # Instance was not commited to the database.
                self.permission_group = PermissionGroup.objects.create()

        super().save(*args, **kwargs)
