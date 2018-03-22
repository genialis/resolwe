#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Open source dataflow package for Django framework.

See:
https://github.com/genialis/resolwe
"""

from setuptools import find_packages, setup
# Use codecs' open for a consistent encoding
from codecs import open
from os import path

base_dir = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(base_dir, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Get package metadata from 'resolwe.__about__.py' file
about = {}
with open(path.join(base_dir, 'resolwe', '__about__.py'), encoding='utf-8') as f:
    exec(f.read(), about)

setup(
    name=about['__title__'],

    version=about['__version__'],

    description=about['__summary__'],
    long_description=long_description,

    url=about['__url__'],

    author=about['__author__'],
    author_email=about['__email__'],

    license=about['__license__'],

    # exclude tests from built/installed package
    packages=find_packages(exclude=['tests', 'tests.*', '*.tests', '*.tests.*']),
    package_data={
        'resolwe': [
            'flow/executors/requirements.txt',
            'flow/static/flow/*.json',
            'toolkit/processes/**.yml',
            'toolkit/tools/**.py',
        ]
    },
    install_requires=[
        'Django~=1.11.0',
        'djangorestframework~=3.7.0',
        'djangorestframework-filters~=0.10.0',
        # XXX: Remove django-autoslug after all migrations that import
        # it are deleted
        'django-autoslug==1.9.3',
        'django-guardian>=1.4.2',
        'django-mathfilters>=0.3.0',
        'django-versionfield2>=0.5.0',
        'django-fernet-fields>=0.5',
        'elasticsearch-dsl~=5.4.0',
        'psycopg2>=2.5.0',
        'mock>=1.3.0',
        'PyYAML>=3.11',
        'jsonschema>=2.4.0',
        'six>=1.10.0',
        'Sphinx>=1.5.1, <1.7.0',
        'Jinja2>=2.9.6',
        'wrapt>=1.10.8',
        'shellescape>=3.4.1',
        # XXX: Temporarily pin urllib3 since the latest version of the requests
        # package (2.18.1) explicitly requires urllib3<1.22,>=1.21.1
        'urllib3==1.21.1',
        # XXX: djangorestframework-filters has too open requirement for
        # django-filter and doesn't work with the latest version, so we
        # have to pin it
        'django-filter~=1.0.0',
        'redis~=2.10.6',
        'asgi-redis~=1.4.3',
        'channels~=1.1.6',
    ],
    python_requires='>=3.4, <3.6',
    extras_require={
        'docs':  [
            'sphinx_rtd_theme',
        ],
        'package': [
            'twine',
            'wheel',
        ],
        'test': [
            'check-manifest',
            'coverage>=4.2',
            # pycodestyle 2.3.0 raises false-positive for variables
            # starting with 'def'
            # https://github.com/PyCQA/pycodestyle/issues/617
            'pycodestyle~=2.2.0',
            'pydocstyle>=2.0.0',
            # Pylint 1.7.0 introduces new warning/errors and is temporarily
            'pylint~=1.6.4',
            'readme_renderer',
            'resolwe-runtime-utils>=1.1.0',
            'testfixtures>=4.10.0',
            'tblib>=1.3.0',
            'isort',
        ],
    },

    classifiers=[
        'Development Status :: 4 - Beta',

        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP :: WSGI',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: OS Independent',

        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='resolwe dataflow django',
)
