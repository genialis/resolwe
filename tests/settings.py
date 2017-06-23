"""
Django settings for running tests for Resolwe package.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

SECRET_KEY = 'secret'

DEBUG = True

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.staticfiles',

    # 'kombu.transport.django',  # required for Celery to work with Django DB.

    'rest_framework',
    'guardian',
    'mathfilters',
    'versionfield',

    'resolwe',
    'resolwe.permissions',
    'resolwe.flow',
    'resolwe.elastic',
    'resolwe.toolkit',
)

ROOT_URLCONF = 'tests.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
            ],
        },
    },
]

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
    'guardian.backends.ObjectPermissionBackend',
)

ANONYMOUS_USER_NAME = 'public'

# Get the current Tox testing environment
# NOTE: This is useful for concurrently running tests with separate environments
toxenv = os.environ.get('TOXENV', '')

# Check if PostgreSQL settings are set via environment variables
pgname = os.environ.get('RESOLWE_POSTGRESQL_NAME', 'resolwe')
pguser = os.environ.get('RESOLWE_POSTGRESQL_USER', 'resolwe')
pghost = os.environ.get('RESOLWE_POSTGRESQL_HOST', 'localhost')
pgport = int(os.environ.get('RESOLWE_POSTGRESQL_PORT', 55432))

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': pgname,
        'USER': pguser,
        'HOST': pghost,
        'PORT': pgport,
        'TEST': {
            'NAME': 'resolwe_test' + toxenv
        }
    }
}

BROKER_URL = 'django://'
CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = [CELERY_TASK_SERIALIZER]

STATIC_URL = '/static/'

FLOW_EXECUTOR = {
    'NAME': 'resolwe.flow.executors.local',
    'DATA_DIR': os.path.join(PROJECT_ROOT, '.test_data'),
    'UPLOAD_DIR': os.path.join(PROJECT_ROOT, '.test_upload'),
}

# Set custom Docker command if set via environment variable.
if 'RESOLWE_DOCKER_COMMAND' in os.environ:
    FLOW_DOCKER_COMMAND = os.environ['RESOLWE_DOCKER_COMMAND']

# Disable SECCOMP if set via environment variable.
FLOW_DOCKER_DISABLE_SECCOMP = strtobool(os.environ.get('RESOLWE_DOCKER_DISABLE_SECCOMP', '0'))

FLOW_API = {
    'PERMISSIONS': 'resolwe.permissions.permissions',
}
FLOW_EXPRESSION_ENGINES = [
    {
        'ENGINE': 'resolwe.flow.expression_engines.jinja',
        'CUSTOM_FILTERS': [
            'resolwe.flow.tests.expression_filters',
        ]
    },
]
FLOW_EXECUTION_ENGINES = [
    'resolwe.flow.execution_engines.bash',
    'resolwe.flow.execution_engines.workflow',
]

FLOW_PROCESSES_FINDERS = (
    'resolwe.flow.finders.FileSystemProcessesFinder',
    'resolwe.flow.finders.AppDirectoriesFinder',
)

FLOW_DOCKER_MAPPINGS = [
    {'src': os.path.join(FLOW_EXECUTOR['DATA_DIR'], '{data_id}'),
     'dest': '/data',
     'mode': 'rw,Z'},
    {'src': FLOW_EXECUTOR['DATA_DIR'],
     'dest': '/data_all',
     'mode': 'ro,z'},
    {'src': FLOW_EXECUTOR['UPLOAD_DIR'],
     'dest': '/upload',
     'mode': 'rw,z'},
]

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework.authentication.SessionAuthentication',
    ),
    'DEFAULT_FILTER_BACKENDS': (
        'resolwe.permissions.filters.ResolwePermissionsFilter',
        'rest_framework_filters.backends.DjangoFilterBackend',
        'rest_framework.filters.OrderingFilter',
    ),
    'EXCEPTION_HANDLER': 'resolwe.flow.utils.exceptions.resolwe_exception_handler',
}

ELASTICSEARCH_HOST = os.environ.get('RESOLWE_ES_HOST', 'localhost')
ELASTICSEARCH_PORT = int(os.environ.get('RESOLWE_ES_PORT', '59200'))
