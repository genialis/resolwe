"""
==========
Data Purge
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django.conf import settings

from resolwe.flow.models import Data, iterate_fields


def get_purge_files(root, output, output_schema, descriptor, descriptor_schema):
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
            path = path[len(root) + 1:]
            subs.extend(os.path.join(path, f) for f in files)
            subs.extend(os.path.join(path, d) for d in dirs)
        return subs

    unreferenced_files = subfiles(root)

    remove_file('jsonout.txt', unreferenced_files)
    remove_file('stderr.txt', unreferenced_files)
    remove_file('stdout.txt', unreferenced_files)

    meta_fields = [
        [output, output_schema],
        [descriptor, descriptor_schema]
    ]

    for meta_field, meta_field_schema in meta_fields:
        for field_schema, fields in iterate_fields(meta_field, meta_field_schema):
            if 'type' in field_schema:
                field_type = field_schema['type']
                field_name = field_schema['name']

                # Remove basic:file: entries
                if field_type.startswith('basic:file:'):
                    remove_file(fields[field_name]['file'], unreferenced_files)

                # Remove list:basic:file: entries
                elif field_type.startswith('list:basic:file:'):
                    for field in fields[field_name]:
                        remove_file(field['file'], unreferenced_files)

                # Remove basic:dir: entries
                elif field_type.startswith('basic:dir:'):
                    remove_tree(fields[field_name]['dir'], unreferenced_files)

                # Remove list:basic:dir: entries
                elif field_type.startswith('list:basic:dir:'):
                    for field in fields[field_name]:
                        remove_tree(field['dir'], unreferenced_files)

                # Remove refs entries
                if field_type.startswith('basic:file:') or field_type.startswith('basic:dir:'):
                    for ref in fields[field_name].get('refs', []):
                        remove_tree(ref, unreferenced_files)

                elif field_type.startswith('list:basic:file:') or field_type.startswith('list:basic:dir:'):
                    for field in fields[field_name]:
                        for ref in field.get('refs', []):
                            remove_tree(ref, unreferenced_files)

    return set([os.path.join(root, filename) for filename in unreferenced_files])


def data_purge(data_ids=None, delete=False, verbosity=0):
    """Print files not referenced from meta data.

    If data_ids not given, run on all data objects.
    If delete is True, delete unreferenced files.

    """

    data_path = settings.FLOW_EXECUTOR['DATA_DIR']
    unreferenced_files = set()

    data_qs = Data.objects.filter(status__in=[Data.STATUS_DONE, Data.STATUS_ERROR])
    if data_ids is not None:
        data_qs = data_qs.filter(pk__in=data_ids)

    for data in data_qs:
        root = os.path.join(data_path, str(data.id))

        unreferenced_files.update(get_purge_files(
            root,
            data.output,
            data.process.output_schema,
            data.descriptor,
            getattr(data.descriptor_schema, 'schema', [])
        ))

    if verbosity >= 1:
        # Print unreferenced files
        if unreferenced_files:
            print("Unreferenced files ({}):".format(len(unreferenced_files)))
            for name in unreferenced_files:
                print("  {}".format(name))
        else:
            print("No unreferenced files")

    # Go through unreferenced files and delete them
    if delete:
        for name in unreferenced_files:
            if os.path.isfile(name) or os.path.islink(name):
                os.remove(name)
            elif os.path.isdir(name):
                shutil.rmtree(name)
