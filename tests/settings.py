"""
Django settings for running tests for Resolwe package.

"""

import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

SECRET_KEY = 'secret'

DEBUG = True
TEMPLATE_DEBUG = DEBUG

MIDDLEWARE_CLASSES = ()

# List of apps to test with django-jenkins
PROJECT_APPS = (
    'resolwe',
    'resolwe.apps',
    'resolwe.flow',
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',

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

# This is needed for runing concurrent tests on Jenkins
toxenv = os.environ.get('TOXENV', '')

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'resolwe',
        'USER': 'resolwe',
        'PASSWORD': 'resres',
        'HOST': 'localhost',
        'PORT': 5432,
        'TEST': {
            'NAME': 'resolwe_test' + toxenv
        }
    }
}

JENKINS_TASKS = (
    'django_jenkins.tasks.run_pylint',
    'django_jenkins.tasks.run_pep8',
)

PYLINT_RCFILE = '.pylintrc'
PEP8_RCFILE = '.pep8rc'
