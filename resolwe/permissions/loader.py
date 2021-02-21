""".. Ignore pydocstyle D400.

==================
Permissions loader
==================

.. autofunction:: load_permissions

"""
import os
import pkgutil
from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

permissions_classes = {}


def get_permissions_class(permissions_name=None):
    """Load and cache permissions class.

    If ``permissions_name`` is not given, it defaults to permissions
    class set in Django ``FLOW_API['PERMISSIONS']`` setting.
    """

    def load_permissions(permissions_name):
        """Look for a fully qualified flow permissions class."""
        try:
            return import_module("{}".format(permissions_name)).ResolwePermissions
        except AttributeError:
            raise AttributeError(
                "'ResolwePermissions' class not found in {} module.".format(
                    permissions_name
                )
            )
        except ImportError as ex:
            # The permissions module wasn't found. Display a helpful error
            # message listing all possible (built-in) permissions classes.
            permissions_dir = os.path.join(os.path.dirname(__file__), "..", "perms")
            permissions_dir = os.path.normpath(permissions_dir)

            try:
                builtin_permissions = [
                    name
                    for _, name, _ in pkgutil.iter_modules([permissions_dir])
                    if name not in ["tests"]
                ]
            except EnvironmentError:
                builtin_permissions = []
            if permissions_name not in [
                "resolwe.auth.{}".format(p) for p in builtin_permissions
            ]:
                permissions_reprs = map(repr, sorted(builtin_permissions))
                err_msg = (
                    "{} isn't an available flow permissions class.\n"
                    "Try using 'resolwe.auth.XXX', where XXX is one of:\n"
                    "    {}\n"
                    "Error was: {}".format(
                        permissions_name, ", ".join(permissions_reprs), ex
                    )
                )
                raise ImproperlyConfigured(err_msg)
            else:
                # If there's some other error, this must be an error in Django
                raise

    if permissions_name is None:
        permissions_name = settings.FLOW_API["PERMISSIONS"]

    if permissions_name not in permissions_classes:
        permissions_classes[permissions_name] = load_permissions(permissions_name)

    return permissions_classes[permissions_name]
