"""
Django settings for running tests for Resolwe package.

"""
import os
from distutils.util import strtobool

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
    'channels',

    # 'kombu.transport.django',  # required for Celery to work with Django DB.

    'rest_framework',
    'guardian',
    'versionfield',

    'resolwe',
    'resolwe.permissions',
    'resolwe.flow',
    'resolwe.elastic',
    'resolwe.storage',
    'resolwe.toolkit',
    'resolwe.test_helpers',
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
pgpass = os.environ.get('RESOLWE_POSTGRESQL_PASS', 'resolwe')
pghost = os.environ.get('RESOLWE_POSTGRESQL_HOST', 'localhost')
pgport = int(os.environ.get('RESOLWE_POSTGRESQL_PORT', 55432))

DATABASES = {
    'default': {
        'ENGINE': 'resolwe.db.postgresql',
        'NAME': pgname,
        'USER': pguser,
        'PASSWORD': pgpass,
        'HOST': pghost,
        'PORT': pgport,
        'TEST': {
            'NAME': pgname + '_test'
        }
    }
}

REDIS_CONNECTION = {
    'host': 'localhost',
    'port': int(os.environ.get('RESOLWE_REDIS_PORT', 56379)),
    'db': int(os.environ.get('RESOLWE_REDIS_DATABASE', 1)),
}

ASGI_APPLICATION = 'resolwe.flow.routing.channel_routing'

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            'hosts': [(REDIS_CONNECTION['host'], REDIS_CONNECTION['port'])],
            'expiry': 3600,
        },
    },
}

BROKER_URL = 'django://'
CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = [CELERY_TASK_SERIALIZER]

STATIC_URL = '/static/'

FLOW_EXECUTOR = {
    'NAME': 'resolwe.flow.executors.local',
    'DATA_DIR': os.path.join(PROJECT_ROOT, '.test_data'),
    'UPLOAD_DIR': os.path.join(PROJECT_ROOT, '.test_upload'),
    'RUNTIME_DIR': os.path.join(PROJECT_ROOT, '.test_runtime'),
    'REDIS_CONNECTION': REDIS_CONNECTION,
}

# Check if any Manager settings are set via environment variables
manager_prefix = os.environ.get('RESOLWE_MANAGER_REDIS_PREFIX', 'resolwe.flow.manager')
FLOW_MANAGER = {
    'REDIS_PREFIX': manager_prefix,
    'REDIS_CONNECTION': REDIS_CONNECTION,
    'TEST': {
        'REDIS_PREFIX': manager_prefix + '-test',
    },
}

# Set custom Docker command if set via environment variable.
if 'RESOLWE_DOCKER_COMMAND' in os.environ:
    FLOW_DOCKER_COMMAND = os.environ['RESOLWE_DOCKER_COMMAND']

# Ignore errors when pulling Docker images from 'list_docker_images --pull' command.
FLOW_DOCKER_IGNORE_PULL_ERRORS = strtobool(os.environ.get('RESOLWE_DOCKER_IGNORE_PULL_ERRORS', '1'))

# Don't pull Docker images if set via the environment variable.
FLOW_DOCKER_DONT_PULL = strtobool(os.environ.get('RESOLWE_DOCKER_DONT_PULL', '0'))

# Disable SECCOMP if set via environment variable.
FLOW_DOCKER_DISABLE_SECCOMP = strtobool(os.environ.get('RESOLWE_DOCKER_DISABLE_SECCOMP', '0'))

# Ensure all container images follow a specific format.
FLOW_CONTAINER_VALIDATE_IMAGE = r'.+:(?!latest)'

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
    'resolwe.flow.execution_engines.python',
    'resolwe.flow.execution_engines.workflow',
]

FLOW_PROCESSES_FINDERS = (
    'resolwe.flow.finders.FileSystemProcessesFinder',
    'resolwe.flow.finders.AppDirectoriesFinder',
)

FLOW_DOCKER_VOLUME_EXTRA_OPTIONS = {
    'data': 'Z',
    'data_all': 'z',
    'upload': 'z',
    'secrets': 'Z',
    'users': 'Z',
    'tools': 'z',
    'runtime': 'Z',
}

FLOW_DOCKER_EXTRA_VOLUMES = []

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework.authentication.SessionAuthentication',
    ),
    'DEFAULT_FILTER_BACKENDS': (
        'resolwe.permissions.filters.ResolwePermissionsFilter',
        'django_filters.rest_framework.backends.DjangoFilterBackend',
        'resolwe.flow.filters.OrderingFilter',
    ),
    'EXCEPTION_HANDLER': 'resolwe.flow.utils.exceptions.resolwe_exception_handler',

    # Python<3.7 cannot parse iso-8601 formatted datetimes with tz-info form
    # "+01:00" (DRF default). It can only parse "+0100" form, so we need to
    # modify this setting. This will be fixed in Python3.7, where "+01:00" can
    # be parsed by ``datetime.datetime.strptime`` syntax.
    # For more, check "%z" syntax description in:
    # https://docs.python.org/3.7/library/datetime.html#strftime-and-strptime-behavior
    'DATETIME_FORMAT': '%Y-%m-%dT%H:%M:%S.%f%z'
}

# Time
USE_TZ = True
TIME_ZONE = 'UTC'

# Django does not support parsing of 'iso-8601' formated datetimes by default.
# Since Django-filters uses Django forms for parsing, we need to modify Django
# setting ``DATETIME_INPUT_FORMATS`` to support 'iso-8601' format.
# https://docs.djangoproject.com/en/1.11/ref/settings/#datetime-input-formats
DATETIME_INPUT_FORMATS = (
    # These are already given Django defaults:
    '%Y-%m-%d %H:%M:%S',       # '2006-10-25 14:30:59'
    '%Y-%m-%d %H:%M:%S.%f',    # '2006-10-25 14:30:59.000200'
    '%Y-%m-%d %H:%M',          # '2006-10-25 14:30'
    '%Y-%m-%d',                # '2006-10-25'
    # These are iso-8601 formatted:
    '%Y-%m-%dT%H:%M:%S.%f%z',  # '2006-10-25T14:30:59.000200+0200' or '2006-10-25T14:30:59.000200+02:00' (Python>=3.7)
    '%Y-%m-%dT%H:%M:%S.%fZ',   # '2006-10-25T14:30:59.000200Z'
    '%Y-%m-%dT%H:%M:%S.%f',    # '2006-10-25T14:30:59.000200'
    '%Y-%m-%dT%H:%M:%SZ',      # '2006-10-25T14:30:59Z'
    '%Y-%m-%dT%H:%M:%S',       # '2006-10-25T14:30:59'
    '%Y-%m-%dT%H:%M',          # '2006-10-25T14:30'
)


ELASTICSEARCH_HOST = os.environ.get('RESOLWE_ES_HOST', 'localhost')
ELASTICSEARCH_PORT = int(os.environ.get('RESOLWE_ES_PORT', '59200'))

# Testing.

TEST_RUNNER = 'resolwe.test_helpers.test_runner.ResolweRunner'
TEST_PROCESS_REQUIRE_TAGS = True
TEST_PROCESS_PROFILE = False


# Logging.

# Set RESOLWE_LOG_FILE environment variable to a file path to enable
# logging debugging messages to to a file.
debug_file_path = os.environ.get('RESOLWE_LOG_FILE', os.devnull)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - %(name)s[%(process)s]: %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'WARNING',
            'formatter': 'standard',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': debug_file_path,
            'formatter': 'standard',
            'maxBytes': 1024 * 1024 * 10,  # 10 MB
        },
    },
    'loggers': {
        '': {
            'handlers': ['file'],
            'level': 'DEBUG',
        },
        'elasticsearch': {
            'handlers': ['file'],
            'level': 'WARNING',
            'propagate': False,
        },
        'urllib3': {
            'handlers': ['file'],
            'level': 'WARNING',
            'propagate': False,
        },
    }
}
