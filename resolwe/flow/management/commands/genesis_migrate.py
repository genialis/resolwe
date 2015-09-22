from __future__ import absolute_import, division, print_function, unicode_literals

import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.utils.text import slugify

from guardian.shortcuts import assign_perm, get_groups_with_perms, get_users_with_perms

from resolwe.flow.models import Data, DescriptorSchema, Process, Project, Storage, Trigger, dict_dot, iterate_fields
from resolwe.apps.models import App, Package


class Command(BaseCommand):

    """Migrate data from `Genesis`"""

    help = 'Migrate data from `Genesis`'

    def add_arguments(self, parser):
        parser.add_argument('-u', '--username', type=str, default='postgres', help="MongoDB username")
        # TODO: add required=True for password after testing
        parser.add_argument('-p', '--password', type=str, help="MongoDB password")
        # TODO: add required=True for db-name after testing
        parser.add_argument('--db-name', type=str, help="MongoDB database name")
        parser.add_argument('--host', type=str, default='localhost', help="MongoDB host")
        parser.add_argument('--port', type=int, default=27017, help="MongoDB port")
        parser.add_argument('--output', type=str, default=None, help="Output file for id mappings")

        # REMOVE AFTER TESTING
        parser.add_argument('-g', '--gendev', action='store_true', help='Use settings for gendev')
        # ##

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

    def process_slug(self, name):
        return slugify(name.replace(':', '-'))

    def convert_version(self, version):
        return reduce(lambda a, b: 1000 * a + b, list(map(int, version.split('.'))))

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
                    print("User with id {} doesn't exists.".format(entity_id))
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

    def migrate_project(self, project):
        new = Project()
        new.name = project[u'name']
        new.slug = project[u'url_slug'] if u'url_slug' in project else slugify(project[u'name'])
        new.description = project[u'description']
        new.contributor = get_user_model().objects.get(id=project['author_id'])
        new.settings = project[u'settings']
        # XXX: Django will change this on create
        new.created = project[u'date_created']
        # XXX: Django will change this on save
        new.modified = project[u'date_modified']
        new.save()

        self.migrate_permissions(new, project)

        for tag in project[u'tags']:
            if tag in self.project_tags:
                self.project_tags[tag].append(new)
            else:
                self.project_tags[tag] = [new]

        self.id_mapping['project'][str(project[u'_id'])] = new.pk

    def migrate_process(self, process):
        new = Process()
        new.name = process[u'label']
        new.slug = self.process_slug(process[u'name'])
        new.version = self.convert_version(process[u'version'])
        new.type = process[u'type']
        new.description = process[u'description']
        new.contributor = get_user_model().objects.get(id=process['author_id'])
        if u'category' in process:
            new.category = process[u'category']
        # XXX: Django will change this on create
        new.created = process[u'date_created']
        # XXX: Django will change this on save
        new.modified = process[u'date_modified']
        new.output_schema = process[u'output_schema']
        new.input_schema = process[u'input_schema']
        new.persistence = self.persistence_dict[process[u'persistence']]
        new.run['script'] = process[u'run'][u'bash']
        new.save()

        self.migrate_permissions(new, process)

        self.id_mapping['process'][str(process[u'_id'])] = new.pk

    def migrate_data(self, data):
        contributor = get_user_model().objects.get(id=data[u'author_id'])

        # DESCRIPTOR SCHEMA ############################################
        ds_fields = []
        if u'static_schema' in data:
            ds_fields.extend(data[u'static_schema'])
        if u'var_template' in data:
            ds_fields.extend(data[u'var_template'])
        ds_fields_dumped = json.dumps(ds_fields)

        ds_fields.sort(key=lambda d: d[u'name'])

        if ds_fields_dumped in self.descriptor_schema_index:
            descriptor_schema = self.descriptor_schema_index[ds_fields_dumped]
        else:
            descriptor_schema = DescriptorSchema(schema=ds_fields)
            descriptor_schema.name = 'data_{}_descriptor'.format(data[u'_id'])
            descriptor_schema.slug = descriptor_schema.unique_slug(descriptor_schema.name)
            descriptor_schema.contributor = contributor
            descriptor_schema.save()

            self.descriptor_schema_index[ds_fields_dumped] = descriptor_schema

        descriptor = []
        if u'static' in data:
            descriptor.extend(data[u'static'])
        if u'var' in data:
            descriptor.extend(data[u'var'])

        # PROCESS ######################################################
        process_slug = self.process_slug(data[u'processor_name'])
        process_version = self.convert_version(data[u'processor_version'])
        try:
            process = Process.objects.get(slug=process_slug, version=process_version)
        except Process.DoesNotExist:
            latest = Process.objects.filter(slug=process_slug).order_by('-version').first()

            process = Process()
            process.name = latest.name
            process.slug = latest.slug
            process.category = latest.category
            process.description = latest.description
            process.contributor = latest.contributor

            process.version = process_version
            process.type = data[u'type']
            process.output_schema = data[u'output_schema']
            if u'input_schema' in data:
                process.input_schema = data[u'input_schema']
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

        # DATA #########################################################
        new = Data()
        new.name = data[u'static'][u'name'] if u'name' in data[u'static'] else data[u'_id']
        new.slug = new.unique_slug(new.name)
        new.status = self.status_dict[data[u'status']]
        new.process = process
        new.contributor = contributor
        new.input = data[u'input'] if u'input' in data else {}
        new.output = data[u'output']
        new.descriptor_schema = descriptor_schema
        new.descriptor = descriptor
        new.checksum = data[u'checksum']
        # XXX: Django will change this on create
        new.created = data[u'date_created']
        # XXX: Django will change this on save
        new.modified = data[u'date_modified']
        new.started = data[u'date_start']
        new.finished = data[u'date_finish']
        new.save()

        for case_id in data[u'case_ids']:
            try:
                project = Project.objects.get(pk=self.id_mapping[u'project'][str(case_id)])
            except KeyError:
                print("Mapping for (old) project id {} does not exist.".format(str(case_id)))
                continue
            project.data.add(new)

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

    def migrate_storage(self, storage):
        data_id = self.storage_index[str(storage[u'_id'])]['id']
        data_path = self.storage_index[str(storage[u'_id'])]['path']
        data = Data.objects.get(pk=data_id)

        new = Storage()
        new.name = 'data_{}_storage'.format(data_id)
        new.slug = new.unique_slug(new.name)
        new.data = data
        new.json = storage[u'json']
        new.contributor = get_user_model().objects.get(id=storage[u'author_id'])
        # XXX: Django will change this on create
        new.created = storage[u'date_created']
        # XXX: Django will change this on save
        new.modified = storage[u'date_modified']
        new.save()

        dict_dot(data.output, data_path, new.pk)
        data.save()

        self.id_mapping['storage'][str(storage[u'_id'])] = new.pk

    def migrate_trigger(self, trigger):

        new = Trigger()

        new.name = trigger[u'name']
        new.slug = new.unique_slug(new.name)
        new.contributor = get_user_model().objects.get(id=trigger[u'author_id'])
        new.type = trigger[u'type']
        new.trigger = trigger[u'trigger']
        new.trigger_input = trigger[u'trigger_input']
        new.process = Process.objects.filter(
            slug=self.process_slug(trigger[u'processor_name'])).order_by('-version').first()
        new.input = trigger[u'input']
        new.project = Project.objects.get(pk=self.id_mapping[u'project'][str(trigger[u'case_id'])])
        new.autorun = trigger[u'autorun']
        # XXX: Django will change this on create
        new.created = trigger[u'date_created']
        # XXX: Django will change this on save
        new.modified = trigger[u'date_modified']
        new.save()

        self.id_mapping['trigger'][str(trigger[u'_id'])] = new.pk

    def migrate_package(self, package):
        new = Package()
        new.name = package[u'title']
        new.slug = package[u'name']
        new.version = self.convert_version(package[u'version'])
        new.modules = package[u'modules']
        new.index = package[u'index']
        new.contributor = get_user_model().objects.get(id=package[u'author_id'])
        # XXX: Django will change this on create
        new.created = package[u'date_created']
        # XXX: Django will change this on save
        new.modified = package[u'date_modified']
        new.save()

        self.migrate_permissions(new, package)

        self.id_mapping['package'][str(package[u'_id'])] = new.pk

    def migrate_app(self, app):
        new = App()
        new.name = app[u'name']
        new.slug = app[u'url_slug']
        new.description = app[u'description']
        new.package = Package.objects.get(slug=app[u'package'])
        new.modules = app[u'modules']
        new.contributor = get_user_model().objects.get(id=app[u'author_id'])
        if u'default_project' in app:
            new.default_project = Project.objects.get(slug=app[u'default_project'])
        # XXX: Django will change this on create
        new.created = app[u'date_created']
        # XXX: Django will change this on save
        new.modified = app[u'date_modified']
        new.save()

        if new.slug in self.project_tags:
            new.projects.add(*self.project_tags[new.slug])

        self.migrate_permissions(new, app)

        self.id_mapping['app'][str(app[u'_id'])] = new.pk

    # REMOVE AFTER TESTING ####################
    def clear_database(self):
        Project.objects.all().delete()
        Data.objects.all().delete()
        Process.objects.all().delete()
        Storage.objects.all().delete()
        DescriptorSchema.objects.all().delete()
        Package.objects.all().delete()
        App.objects.all().delete()

        from guardian.models import GroupObjectPermission, UserObjectPermission

        GroupObjectPermission.objects.all().delete()
        UserObjectPermission.objects.all().delete()
    # #########################################

    def handle(self, *args, **options):
        try:
            from pymongo import MongoClient
        except ImportError:
            self.stdout.write('PyMongo is required')
            exit(1)

        self.storage_index = {}
        self.descriptor_schema_index = {}
        self.id_mapping = {_type: {} for _type in ['project', 'process', 'data', 'storage',
                                                   'trigger', 'package', 'app']}
        self.project_tags = {}

        # REMOVE AFTER TESTING ####################
        if options['gendev']:
            options['username'] = 'genesis'
            options['password'] = 'resres'
            options['host'] = 'localhost'
            options['port'] = 10117
            options['db_name'] = 'gendev_data'
            options['output'] = 'genesis.txt'

            self.clear_database()
        # #########################################

        mongo_uri = 'mongodb://{username}:{password}@{host}:{port}/{db_name}'.format(**options)
        client = MongoClient(mongo_uri)

        print('Migrating projects...', end='')
        for project in client[options['db_name']].case.find():
            self.migrate_project(project)
        print('DONE')

        print('Migrating processes...', end='')
        for process in client[options['db_name']].processor.find():
            self.migrate_process(process)
        print('DONE')

        print('Migrating data...', end='')
        for data in client[options['db_name']].data.find():
            self.migrate_data(data)
        print('DONE')

        print('Migrating storage...', end='')
        for storage in client[options['db_name']].storage.find():
            self.migrate_storage(storage)
        print('DONE')

        print('Migrating trigger...', end='')
        for trigger in client[options['db_name']].trigger.find():
            self.migrate_trigger(trigger)
        print('DONE')

        print('Migrating package...', end='')
        for package in client[options['db_name']].gen_package.find():
            self.migrate_package(package)
        print('DONE')

        print('Migrating app...', end='')
        for app in client[options['db_name']].gen_app.find():
            self.migrate_app(app)
        print('DONE')

        if options['output']:
            with open(options['output'], 'w') as fn:
                json.dump(self.id_mapping, fn)
        else:
            print(json.dumps(self.id_mapping))
