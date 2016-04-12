# Command to run on local machine to migrate data from gendev:
# ./tests/manage.py genesis_migrate -u=genesis -p=resres --port=10117 --db-name=gendev_data --output=gendev.txt

from __future__ import absolute_import, division, print_function, unicode_literals

from datetime import datetime
import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.utils.text import slugify

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm, get_groups_with_perms, get_users_with_perms

from resolwe.flow.models import Data, DescriptorSchema, Process, Collection, Storage, Trigger, dict_dot, iterate_fields


class Command(BaseCommand):

    """Migrate data from `Genesis`"""

    help = 'Migrate data from `Genesis`'

    def add_arguments(self, parser):
        parser.add_argument('-u', '--username', type=str, default='postgres', help="MongoDB username")
        parser.add_argument('-p', '--password', required=True, type=str, help="MongoDB password")
        parser.add_argument('--db-name', required=True, type=str, help="MongoDB database name")
        parser.add_argument('--host', type=str, default='localhost', help="MongoDB host")
        parser.add_argument('--port', type=int, default=27017, help="MongoDB port")
        parser.add_argument('--output', type=str, default=None, help="Output file for id mappings")
        parser.add_argument('--purge', action='store_true', help='Purge local database before migration')

    persistence_dict = {
        'raw': 'RAW',
        'cached': 'CAC',
        'temp': 'TMP',
    }

    status_dict = {
        'uploading': 'UP',
        'resolving': 'RE',
        'waiting': 'WT',
        'processing': 'PR',
        'done': 'OK',
        'error': 'ER',
        'dirty': 'DR',
    }

    storage_index = {}
    descriptor_schema_index = {}
    id_mapping = {_type: {} for _type in ['collection', 'process', 'data', 'storage', 'trigger']}
    collection_tags = {}

    missing_users = set()
    missing_collections = set()
    missing_default_collections = []
    missing_data = set()
    long_names = []
    unreferenced_storages = []
    orphan_triggers = []

    def process_slug(self, name):
        return slugify(name.replace(':', '-'))

    def get_contributor(self, contributor_id):
        user_model = get_user_model()
        try:
            return user_model.objects.get(pk=contributor_id)
        except user_model.DoesNotExist:
            return user_model.objects.filter(is_superuser=True).first()

    def migrate_permissions(self, new, old):
        content_type = ContentType.objects.get_for_model(new)

        def set_permissions(entity_type):
            model = get_user_model() if entity_type == 'users' else Group

            for entity_id in old['permissions'].get(entity_type, {}):
                try:
                    entity = model.objects.get(pk=int(entity_id))
                except ValueError:
                    # old format of user ids
                    continue
                except model.DoesNotExist:
                    self.missing_users.add(entity_id)
                    continue

                for perm in old['permissions'][entity_type][entity_id]:
                    if '{}_{}'.format(perm.lower(), content_type) == 'download_app':
                        # depricated permission
                        continue
                    assign_perm('{}_{}'.format(perm.lower(), content_type), entity, new)

        set_permissions('users')
        set_permissions('groups')

        def set_public_permissions():
            user = AnonymousUser()
            perms = old['permissions'].get('public', {})
            for perm in perms:
                if '{}_{}'.format(perm.lower(), content_type) == 'download_app':
                    # depricated permission
                    continue
                assign_perm('{}_{}'.format(perm.lower(), content_type), user, new)

        set_public_permissions()

    def migrate_collection(self, collection):
        new = Collection()
        new.name = collection[u'name']
        new.slug = collection[u'url_slug'] if u'url_slug' in collection else slugify(collection[u'name'])
        new.description = collection.get(u'description', '')
        new.contributor = self.get_contributor(collection['author_id'])
        new.settings = collection.get(u'settings', {})
        # Remove data table settings as they are incompatible with the new frontend.
        for applicationConfig in new.settings.values():
            applicationConfig['tables'] = {}
        # XXX: Django will change this on create
        new.created = collection[u'date_created']
        # XXX: Django will change this on save
        new.modified = collection[u'date_modified']
        new.save()

        self.migrate_permissions(new, collection)

        for tag in collection.get(u'tags', []):
            if tag not in self.collection_tags:
                self.collection_tags[tag] = []
            self.collection_tags[tag].append(new)

        self.id_mapping['collection'][str(collection[u'_id'])] = new.pk

    def migrate_process(self, process):
        new = Process()
        new.name = process[u'label']
        new.slug = self.process_slug(process[u'name'])
        new.version = process[u'version']
        new.type = process[u'type']
        new.description = process.get(u'description', '')
        new.contributor = self.get_contributor(process['author_id'])
        new.category = process.get(u'category', '')
        # XXX: Django will change this on create
        new.created = process[u'date_created']
        # XXX: Django will change this on save
        new.modified = process[u'date_modified']
        new.output_schema = process[u'output_schema']
        new.input_schema = process.get(u'input_schema', {})
        new.persistence = self.persistence_dict[process[u'persistence']]
        new.run['script'] = process[u'run'][u'bash']
        new.save()

        self.migrate_permissions(new, process)

        self.id_mapping['process'][str(process[u'_id'])] = new.pk

    def migrate_data(self, data):
        contributor = self.get_contributor(data[u'author_id'])

        # DESCRIPTOR SCHEMA ############################################
        ds_fields = []
        ds_fields.extend(data.get(u'static_schema', []))
        ds_fields.extend(data.get(u'var_template', []))
        ds_fields.sort(key=lambda d: d[u'name'])
        ds_fields_dumped = json.dumps(ds_fields)

        if ds_fields_dumped in self.descriptor_schema_index:
            descriptor_schema = self.descriptor_schema_index[ds_fields_dumped]
        else:
            descriptor_schema = DescriptorSchema(schema=ds_fields)
            descriptor_schema.name = 'data_{}_descriptor'.format(data[u'_id'])
            descriptor_schema.contributor = contributor
            descriptor_schema.save()

            self.descriptor_schema_index[ds_fields_dumped] = descriptor_schema

        descriptor = {}
        descriptor.update(data.get(u'static', {}))
        descriptor.update(data.get(u'var', {}))

        # PROCESS ######################################################
        if u'processor_version' not in data:
            data[u'processor_version'] = '0.0.0'

        process_slug = self.process_slug(data[u'processor_name'])
        process_version = data[u'processor_version']
        try:
            process = Process.objects.get(slug=process_slug, version=process_version)
        except Process.DoesNotExist:
            latest = Process.objects.filter(slug=process_slug).order_by('-version').first()

            if latest:
                process = Process()
                process.name = latest.name
                process.slug = latest.slug
                process.category = latest.category
                process.description = latest.description
                process.contributor = latest.contributor

                process.version = process_version
                process.type = data[u'type']
                process.output_schema = data[u'output_schema']
                process.input_schema = data.get(u'input_schema', {})
                process.persistence = self.persistence_dict[data[u'persistence']]

                process.run['script'] = 'gen-require common\ngen-error "Depricated process, use the latest version."'

                # XXX
                # process.created =
                # process.modified =

                process.save()

                # copy permissions from latest process
                for user, perms in get_users_with_perms(latest, attach_perms=True).iteritems():
                    for perm in perms:
                        assign_perm(perm, user, process)
                for group, perms in get_groups_with_perms(latest, attach_perms=True).iteritems():
                    for perm in perms:
                        assign_perm(perm, group, process)
            else:
                # Create dummy processor if there is no other version
                dummy_name = 'Dummy processor of type {}'.format(data[u'type'])
                try:
                    process = Process.objects.get(name=dummy_name)
                except Process.DoesNotExist:
                    process = Process.objects.create(
                        name=dummy_name,
                        slug='non-existent',
                        contributor=get_user_model().objects.filter(is_superuser=True).first(),
                        type=data[u'type'],
                        category='data:non-existent',
                        run={'script': {'gen-require common\ngen-error "This processor is not intendent to be run."'}},
                    )

        # DATA #########################################################
        new = Data()
        new.name = data.get(u'static', {}).get(u'name', '')
        if len(new.name) > 100:
            self.long_names.append(new.name)
            new.name = new.name[:97] + '...'
        new.status = self.status_dict[data[u'status']]
        new.process = process
        new.contributor = contributor
        new.input = data[u'input'] if u'input' in data else {}
        new.output = data[u'output']
        new.descriptor_schema = descriptor_schema
        new.descriptor = descriptor
        new.checksum = data.get(u'checksum', '')
        # XXX: Django will change this on create
        new.created = data[u'date_created']
        # XXX: Django will change this on save
        new.modified = data[u'date_modified']
        if u'date_start' in data and u'date_finish' in data:
            new.started = data[u'date_start']
            new.finished = data[u'date_finish']
        elif u'date_finish' in data:
            new.started = data[u'date_finish']
            new.finished = data[u'date_finish']
        elif u'date_start' in data:
            new.started = data[u'date_start']
            new.finished = data[u'date_start']
        else:
            new.started = datetime.fromtimestamp(0)
            new.finished = datetime.fromtimestamp(0)
        new.save()

        for case_id in data[u'case_ids']:
            try:
                collection = Collection.objects.get(pk=self.id_mapping[u'collection'][str(case_id)])
            except KeyError:
                self.missing_collections.add(str(case_id))
                continue
            collection.data.add(new)

        for field_schema, fields, path in iterate_fields(data[u'output'], data[u'output_schema'], ''):
            if 'type' in field_schema and field_schema['type'].startswith('basic:json:'):
                self.storage_index[fields[field_schema['name']]] = {
                    'id': new.pk,
                    'path': path,
                }

        self.migrate_permissions(new, data)

        self.id_mapping['data'][str(data[u'_id'])] = new.pk

        # DESCRIPTOR SCHEMA PERMISSIONS ################################
        for user in get_users_with_perms(new):
            assign_perm('view_descriptorschema', user, obj=descriptor_schema)

        for group in get_groups_with_perms(new):
            assign_perm('view_descriptorschema', group, obj=descriptor_schema)

    def migrate_data_references(self):
        def map_reference(reference):
            try:
                return self.id_mapping['data'][reference]
            except KeyError, error:
                self.missing_data.add(error.message)
                return None

        # Fix references in JSON documents in the second pass.
        for old_id, new_id in self.id_mapping['data'].items():
            data = Data.objects.get(pk=new_id)
            for field_schema, fields in iterate_fields(data.input, data.process.input_schema):
                if 'type' not in field_schema:
                    continue

                name = field_schema['name']
                value = fields[name]
                if field_schema['type'].startswith('data:'):
                    fields[name] = map_reference(value)
                elif field_schema['type'].startswith('list:data:'):
                    fields[name] = map(map_reference, value)

            data.save()

    def migrate_storage(self, storage):
        if str(storage[u'_id']) not in self.storage_index:
            self.unreferenced_storages.append(storage[u'_id'])
            return 1

        data_id = self.storage_index[str(storage[u'_id'])]['id']
        data_path = self.storage_index[str(storage[u'_id'])]['path']
        data = Data.objects.get(pk=data_id)

        new = Storage()
        new.name = 'data_{}_storage'.format(data_id)
        new.data = data
        new.json = storage[u'json']
        new.contributor = self.get_contributor(storage[u'author_id'])
        # XXX: Django will change this on create
        new.created = storage[u'date_created']
        # XXX: Django will change this on save
        new.modified = storage[u'date_modified']
        new.save()

        dict_dot(data.output, data_path, new.pk)
        data.save()

        self.id_mapping['storage'][str(storage[u'_id'])] = new.pk

    def migrate_trigger(self, trigger):
        if str(trigger[u'case_id']) not in self.id_mapping[u'collection']:
            self.orphan_triggers.append(str(trigger[u'case_id']))
            return 1

        new = Trigger()
        new.name = trigger[u'name']
        new.contributor = self.get_contributor(trigger[u'author_id'])
        new.type = trigger[u'type']
        new.trigger = trigger[u'trigger']
        new.trigger_input = trigger[u'trigger_input']
        new.process = Process.objects.filter(
            slug=self.process_slug(trigger[u'processor_name'])).order_by('-version').first()
        new.input = trigger[u'input']
        new.collection = Collection.objects.get(pk=self.id_mapping[u'collection'][str(trigger[u'case_id'])])
        new.autorun = trigger[u'autorun']
        # XXX: Django will change this on create
        new.created = trigger[u'date_created']
        # XXX: Django will change this on save
        new.modified = trigger[u'date_modified']
        new.save()

        self.id_mapping['trigger'][str(trigger[u'_id'])] = new.pk

    def clear_database(self):
        for model in [Collection, Data, Process, Storage, DescriptorSchema,
                      GroupObjectPermission, UserObjectPermission]:
            model.objects.all().delete()

    def handle(self, *args, **options):
        try:
            from pymongo import MongoClient
        except ImportError:
            self.stdout.write('PyMongo is required')
            exit(1)

        if options['purge']:
            self.clear_database()

        mongo_uri = 'mongodb://{username}:{password}@{host}:{port}/{db_name}'.format(**options)
        client = MongoClient(mongo_uri)

        self.stdout.write('Migrating collections...')
        for collection in client[options['db_name']]['case'].find():
            self.migrate_collection(collection)
        self.stdout.write('DONE')

        self.stdout.write('Migrating processes...')
        for process in client[options['db_name']].processor.find():
            self.migrate_process(process)
        self.stdout.write('DONE')

        self.stdout.write('Migrating data...')
        for data in client[options['db_name']].data.find():
            self.migrate_data(data)
        self.migrate_data_references()
        self.stdout.write('DONE')

        self.stdout.write('Migrating storage...')
        for storage in client[options['db_name']].storage.find():
            self.migrate_storage(storage)
        self.stdout.write('DONE')

        self.stdout.write('Migrating trigger...')
        for trigger in client[options['db_name']].trigger.find():
            self.migrate_trigger(trigger)
        self.stdout.write('DONE')

        if options['output']:
            with open(options['output'], 'w') as fn:
                json.dump(self.id_mapping, fn)
        else:
            self.stdout.write('\nID mappings:', json.dumps(self.id_mapping))

        self.stdout.write('\nMissing users: {}'.format(sorted(list(self.missing_users))))
        self.stdout.write('Missing collections (referenced in Data objects): {}'.format(list(self.missing_collections)))
        self.stdout.write('Missing collections (referenced in triggers): {}'.format(self.orphan_triggers))
        self.stdout.write('Missing data (referenced in Data objects): {}'.format(list(self.missing_data)))
        self.stdout.write('Number of shortened names: {}'.format(len(self.long_names)))
        self.stdout.write('Number of unreferenced Storage objects: {}'.format(len(self.unreferenced_storages)))
