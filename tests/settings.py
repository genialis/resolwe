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
    'resolwe.apps',
    'resolwe.perms',
    'resolwe.flow',
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.staticfiles',

    'rest_framework',
    'guardian',
    'mathfilters',
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

# Check if PostgreSQL port is set via environment variable
pgport = int(os.environ.get('RESOLWE_POSTGRESQL_PORT', 5432))

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'resolwe',
        'USER': 'postgres',
        'HOST': 'localhost',
        'PORT': pgport,
        'TEST': {
            'NAME': 'resolwe_test' + toxenv
        }
    }
}

STATIC_URL = '/static/'

JENKINS_TASKS = (
    'django_jenkins.tasks.run_pylint',
    'django_jenkins.tasks.run_pep8',
)

PYLINT_RCFILE = '.pylintrc'
PEP8_RCFILE = '.pep8rc'

FLOW = {
    'EXECUTOR': {
        'NAME': 'resolwe.flow.executors.local',
        'DATA_PATH': os.path.join(PROJECT_ROOT, 'data'),
    },
    'API': {
        'PERMISSIONS': 'resolwe.perms.genesis',
    },
}

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework.authentication.SessionAuthentication',
    ),
    'DEFAULT_FILTER_BACKENDS': (
        'rest_framework.filters.DjangoObjectPermissionsFilter',
    ),
}
