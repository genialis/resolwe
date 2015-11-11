"""
==========
Data Purge
==========

"""
import os
import shutil

from django.conf import settings

from flow.models import Data, iterate_fields, Storage


def data_purge(data_ids=None, delete=False, verbosity=1):
    """Print files not referenced from meta data.

    If data_ids not given, run on all data objects.
    If delete is True, delete unreferenced files.

    """
    data_path = settings.FLOW_EXECUTOR['data_path']
    path_offset = len(data_path) + 24 + 2

    def remove_file(fn, paths):
        """From paths remove fn and dirs before fn in dir tree."""
        while fn:
            for i in range(len(paths) - 1, -1, -1):
                if fn == paths[i]:
                    paths.pop(i)
            fn, _ = os.path.split(fn)

    def remove_tree(fn, paths):
        """From paths remove fn and dirs before or after fn in dir tree."""
        for i in range(len(paths) - 1, -1, -1):
            head = paths[i]
            while head:
                if fn == head:
                    paths.pop(i)
                    break
                head, _ = os.path.split(head)

        remove_file(fn, paths)

    def subfiles(root):
        """Extend unreferenced list with all subdirs and files in top dir."""
        subs = []
        for path, dirs, files in os.walk(root, topdown=False):
            path = path[path_offset:]
            subs.extend(os.path.join(path, f) for f in files)
            subs.extend(os.path.join(path, d) for d in dirs)
        return subs

    check = Data.objects.all()
    purge = []
    ref_storage = set()
    finished = [Data.STATUS_DONE, Data.STATUS_ERROR]
    if data_ids:
        check = check.filter(id__in=data_ids)

        purge = set(data_ids)
    else:
        purge = set(os.listdir(data_path))

    # Remove not referenced
    purge = purge.difference(str(d.id) for d in check.only('pk'))
    purge = [os.path.join(data_path, p) for p in purge]

    check = check.filter(status__in=finished)

    for d in check:
        root = os.path.join(data_path, str(d.id))
        subs = subfiles(root)

        meta_fields = [
            ['output', 'output_schema'],
            ['static', 'static_schema'],
            ['var', 'var_template'],
        ]

        for meta_field, meta_field_schema in meta_fields:
            for field_schema, fields in iterate_fields(d[meta_field], d[meta_field_schema]):
                if 'type' in field_schema:
                    field_type = field_schema['type']
                    field_name = field_schema['name']

                    # Remove basic:file: entries
                    if field_type.startswith('basic:file:'):
                        remove_file(fields[field_name]['file'], subs)

                    # Remove list:basic:file: entries
                    elif field_type.startswith('list:basic:file:'):
                        for field in fields[field_name]:
                            remove_file(field['file'], subs)

                    # Remove basic:url: entries
                    elif field_type.startswith('basic:url:'):
                        remove_file(fields[field_name]['url'], subs)

                    # Remove list:basic:url: entries
                    elif field_type.startswith('list:basic:url:'):
                        for field in fields[field_name]:
                            remove_file(field['url'], subs)

                    # Remove refs entries
                    if field_type.startswith('basic:file:') or field_type.startswith('basic:url:'):
                        for ref in fields[field_name].get('refs', []):
                            remove_tree(ref, subs)

                    elif field_type.startswith('list:basic:file:') or field_type.startswith('list:basic:url:'):
                        for field in fields[field_name]:
                            for ref in field.get(['refs'], []):
                                remove_tree(ref, subs)

                    if field_schema['type'].startswith('basic:json:'):
                        ref_storage.add(fields[field_name])

        purge.extend(os.path.join(root, f) for f in subs)

    # IMPORTANT: Unreferenced `Storage` objects can be determined only
    #            if all `Data` objects are checked.
    unref_storage = Storage.objects.none()
    if (verbosity or delete) and data_ids is None:
        unref_storage = Storage.objects.filter(pk__nin=ref_storage).only('pk')

    if verbosity >= 1:
        # Print unreferenced files
        if purge:
            print "Unreferenced files ({}):".format(len(purge))
            for name in purge:
                print "  {}".format(name)
        else:
            print "No unreferenced files"

        if unref_storage:
            print "Unreferenced Storage objects ({}):".format(unref_storage.count())
            for s in unref_storage:
                print "  {}".format(s.pk)
        elif data_ids is None:
            print "No unreferenced Storage objects"

    # Go through unreferenced files and delete them
    if delete:
        for name in purge:
            if os.path.isfile(name) or os.path.islink(name):
                os.remove(name)
            elif os.path.isdir(name):
                shutil.rmtree(name)

        # Safety check to prevent deleting all `Storage` objects
        if data_ids is None:
            unref_storage.delete()
