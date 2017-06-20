""".. Ignore pydocstyle D400.

==============
Flow Executors
==============

.. autoclass:: resolwe.flow.executors.BaseFlowExecutor
    :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import os
import shutil
import traceback
import uuid
from collections import defaultdict

import six

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction

from resolwe.flow.engine import BaseEngine
from resolwe.flow.models import Data, Process
from resolwe.flow.utils import dict_dot, iterate_fields
from resolwe.flow.utils.purge import data_purge
from resolwe.permissions.utils import copy_permissions
from resolwe.utils import BraceMessage as __

if settings.USE_TZ:
    from django.utils.timezone import now  # pylint: disable=ungrouped-imports
else:
    import datetime
    now = datetime.datetime.now  # pylint: disable=invalid-name


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
CWD = os.getcwd()


def iterjson(text):
    """Decode JSON stream."""
    decoder = json.JSONDecoder()
    while len(text) > 0:
        obj, ndx = decoder.raw_decode(text)

        if not isinstance(obj, dict):
            raise ValueError()

        text = text[ndx:].lstrip('\r\n')
        yield obj


class BaseFlowExecutor(BaseEngine):
    """Represents a workflow executor."""

    exported_files_mapper = defaultdict(dict)

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super(BaseFlowExecutor, self).__init__(*args, **kwargs)
        self.data_id = None
        self.process = None
        self.requirements = {}
        self.resources = {}

    def hydrate_spawned_files(self, filename, data_id):
        """Hydrate spawned files' paths."""
        if filename not in self.exported_files_mapper[self.data_id]:
            raise KeyError('Use `re-export` to prepare the file for spawned process: {}'.format(filename))

        export_fn = self.exported_files_mapper[self.data_id].pop(filename)

        if self.exported_files_mapper[self.data_id] == {}:
            self.exported_files_mapper.pop(self.data_id)

        return {'file_temp': export_fn, 'file': filename}

    def get_tools(self):
        """Get tools paths."""
        tools_paths = []
        for app_config in apps.get_app_configs():
            proc_path = os.path.join(app_config.path, 'tools')
            if os.path.isdir(proc_path):
                tools_paths.append(proc_path)

        custom_tools_paths = getattr(settings, 'RESOLWE_CUSTOM_TOOLS_PATHS', [])
        if not isinstance(custom_tools_paths, list):
            raise KeyError("`RESOLWE_CUSTOM_TOOLS_PATHS` setting must be a list.")
        tools_paths.extend(custom_tools_paths)

        return tools_paths

    def start(self):
        """Start process execution."""
        pass

    def run_script(self, script):
        """Run process script."""
        raise NotImplementedError('`run_script` function must be defined')

    def end(self):
        """End process execution."""
        pass

    def get_stdout(self):
        """Get process' standard output."""
        return self.stdout  # pylint: disable=no-member

    def update_data_status(self, **kwargs):
        """Update (PATCH) data object."""
        data = Data.objects.get(pk=self.data_id)
        for key, value in kwargs.items():
            setattr(data, key, value)

        update_fields = list(kwargs.keys())
        try:
            # Ensure that we only update the fields that were changed.
            data.save(update_fields=update_fields)
        except ValidationError as ex:
            data.process_error.append(str(ex))
            data.status = Data.STATUS_ERROR

            if 'process_error' not in update_fields:
                update_fields.append('process_error')
            if 'status' not in update_fields:
                update_fields.append('status')

            try:
                data.save(update_fields=update_fields)
            except:  # pylint: disable=bare-except
                pass

            raise ex

    def run(self, data_id, script, verbosity=1):
        """Execute the script and save results."""
        if verbosity >= 1:
            print('RUN: {} {}'.format(data_id, script))

        self.data_id = data_id

        # Fetch data instance to get any executor requirements.
        self.process = Data.objects.get(pk=data_id).process
        requirements = self.process.requirements
        self.requirements = requirements.get('executor', {}).get(self.name, {})
        self.resources = requirements.get('resources', {})

        data_dir = settings.FLOW_EXECUTOR['DATA_DIR']
        dir_mode = getattr(settings, 'FLOW_EXECUTOR', {}).get('DATA_DIR_MODE', 0o755)

        output_path = os.path.join(data_dir, str(data_id))

        os.mkdir(output_path)
        # os.mkdir is not guaranteed to set the given mode
        os.chmod(output_path, dir_mode)
        os.chdir(output_path)

        log_file = open('stdout.txt', 'w+')
        json_file = open('jsonout.txt', 'w+')

        proc_pid = self.start()

        self.update_data_status(
            status=Data.STATUS_PROCESSING,
            started=now(),
            process_pid=proc_pid
        )

        # Run processor and handle intermediate results
        self.run_script(script)
        spawn_processors = []
        output = {}
        process_error, process_warning, process_info = [], [], []
        process_progress, process_rc = 0, 0

        # read processor output
        try:
            stdout = self.get_stdout()
            while True:
                line = stdout.readline()
                if not line:
                    break

                try:
                    if line.strip().startswith('run'):
                        # Save processor and spawn if no errors
                        log_file.write(line)
                        log_file.flush()

                        for obj in iterjson(line[3:].strip()):
                            spawn_processors.append(obj)
                    elif line.strip().startswith('export'):
                        file_name = line[6:].strip()

                        export_folder = settings.FLOW_EXECUTOR['UPLOAD_DIR']
                        unique_name = 'export_{}'.format(uuid.uuid4().hex)
                        export_path = os.path.join(export_folder, unique_name)

                        self.exported_files_mapper[self.data_id][file_name] = unique_name

                        shutil.move(file_name, export_path)
                    else:
                        # If JSON, save to MongoDB
                        updates = {}
                        for obj in iterjson(line):
                            for key, val in six.iteritems(obj):
                                if key.startswith('proc.'):
                                    if key == 'proc.error':
                                        process_error.append(val)
                                        if not process_rc:
                                            process_rc = 1
                                            updates['process_rc'] = process_rc
                                        updates['process_error'] = process_error
                                        updates['status'] = Data.STATUS_ERROR
                                    elif key == 'proc.warning':
                                        process_warning.append(val)
                                        updates['process_warning'] = process_warning
                                    elif key == 'proc.info':
                                        process_info.append(val)
                                        updates['process_info'] = process_info
                                    elif key == 'proc.rc':
                                        process_rc = int(val)
                                        updates['process_rc'] = process_rc
                                        if process_rc != 0:
                                            updates['status'] = Data.STATUS_ERROR
                                    elif key == 'proc.progress':
                                        process_progress = int(float(val) * 100)
                                        updates['process_progress'] = process_progress
                                else:
                                    dict_dot(output, key, val)
                                    updates['output'] = output

                        if updates:
                            updates['modified'] = now()
                            self.update_data_status(**updates)

                        if process_rc > 0:
                            log_file.close()
                            json_file.close()
                            os.chdir(CWD)
                            return

                        # Debug output
                        # Not referenced in Data object
                        json_file.write(line)
                        json_file.flush()

                except ValueError as ex:
                    # Ignore if not JSON
                    log_file.write(line)
                    log_file.flush()

        except MemoryError as ex:
            logger.error(__("Out of memory: {}", ex))

        except IOError as ex:
            # TODO: if ex.errno == 28: no more free space
            raise ex
        finally:
            # Store results
            log_file.close()
            json_file.close()
            os.chdir(CWD)

        return_code = self.end()

        if process_rc < return_code:
            process_rc = return_code

        # This transaction is needed to make sure that processing of
        # current data object is finished before manager for spawned
        # processes is triggered.
        with transaction.atomic():
            if spawn_processors and process_rc == 0:
                parent_data = Data.objects.get(pk=self.data_id)

                # Spawn processors
                for d in spawn_processors:
                    d['contributor'] = parent_data.contributor
                    d['process'] = Process.objects.filter(slug=d['process']).latest()

                    for field_schema, fields in iterate_fields(d.get('input', {}), d['process'].input_schema):
                        type_ = field_schema['type']
                        name = field_schema['name']
                        value = fields[name]

                        if type_ == 'basic:file:':
                            fields[name] = self.hydrate_spawned_files(value, data_id)
                        elif type_ == 'list:basic:file:':
                            fields[name] = [self.hydrate_spawned_files(fn, data_id) for fn in value]

                    with transaction.atomic():
                        d = Data.objects.create(**d)
                        d.parents.add(parent_data)

                        # Copy permissions.
                        copy_permissions(parent_data, d)

                        # Copy collections.
                        for collection in parent_data.collection_set.all():
                            collection.data.add(d)

            if process_rc == 0:
                self.update_data_status(
                    status=Data.STATUS_DONE,
                    process_progress=100,
                    finished=now()
                )
            else:
                self.update_data_status(
                    status=Data.STATUS_ERROR,
                    process_progress=100,
                    process_rc=process_rc,
                    finished=now()
                )

            try:
                # Cleanup after processor
                data_purge(data_ids=[data_id], delete=True, verbosity=verbosity)
            except:  # pylint: disable=bare-except
                logger.error(__("Purge error:\n\n{}", traceback.format_exc()))

        # if not update_data(data):  # Data was deleted
        #     # Restore original directory
        #     os.chdir(settings.PROJECT_ROOT)
        #     return

        # Restore original directory
        # os.chdir(settings.PROJECT_ROOT)

        # return data_id

    def terminate(self, data_id):
        """Terminate a running script."""
        raise NotImplementedError('subclasses of BaseFlowExecutor may require a terminate() method')
