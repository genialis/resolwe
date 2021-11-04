"""Base class for filtering permissions by user."""
from django.db.models import Prefetch, Q

from resolwe.permissions.utils import get_anonymous_user, get_user


class FilterPermissionsForUser:
    """Filter the permissions based on the user of the requests.

    Assumptions: the base class has queryset and qs_permission_model defined.
    """

    def prefetch_current_user_permissions(self, queryset):
        """Prefetch permissions for the current user."""
        user = get_user(self.request.user)
        filters = Q(user=user) | Q(group__in=user.groups.all())
        anonymous_user = get_anonymous_user()
        if user != anonymous_user:
            filters |= Q(user=anonymous_user)

        qs_permission_model = self.qs_permission_model.filter(filters)
        return queryset.prefetch_related(
            Prefetch("permission_group__permissions", queryset=qs_permission_model)
        )
