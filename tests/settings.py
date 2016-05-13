"""
Django settings for running tests for Resolwe package.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

SECRET_KEY = 'secret'

DEBUG = True

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

# List of apps to test with django-jenkins
PROJECT_APPS = (
    'resolwe',
    'resolwe.permissions',
    'resolwe.flow',
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
    'django_jenkins',
) + PROJECT_APPS

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

ANONYMOUS_USER_ID = -1

# This is needed for runing concurrent tests on Jenkins
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

JENKINS_TASKS = (
    'django_jenkins.tasks.run_pylint',
    'django_jenkins.tasks.run_pep8',
)

PYLINT_RCFILE = '.pylintrc'
PEP8_RCFILE = '.pep8rc'

FLOW_EXECUTOR = {
    'NAME': 'resolwe.flow.executors.local',
    'DATA_PATH': os.path.join(PROJECT_ROOT, '.data'),
    'UPLOAD_PATH': os.path.join(PROJECT_ROOT, '.upload'),
}
# Set custom executor command if set via environment variable
if 'RESOLWE_EXECUTOR_COMMAND' in os.environ:
    FLOW_EXECUTOR['COMMAND'] = os.environ['RESOLWE_EXECUTOR_COMMAND']
FLOW_API = {
    'PERMISSIONS': 'resolwe.permissions.genesis',
}
FLOW_EXPRESSION_ENGINES = [
    'resolwe.flow.exprengines.dtlbash'
]

FLOW_PROCESSES_FINDERS = (
    'resolwe.flow.finders.FileSystemProcessesFinder',
    'resolwe.flow.finders.AppDirectoriesFinder',
)

FLOW_PROCESSES_DIRS = ('resolwe/flow/tests/processes',)

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework.authentication.SessionAuthentication',
    ),
    'DEFAULT_FILTER_BACKENDS': (
        'rest_framework.filters.DjangoObjectPermissionsFilter',
        'rest_framework_filters.backends.DjangoFilterBackend',
    ),
}
