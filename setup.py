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
            'flow/static/flow/*.json',
        ]
    },
    install_requires=[
        'django>=1.9,<1.10a1',
        # XXX: djangorestframework 3.3.3 fails to install on Read the Docs
        # Bug report: https://github.com/rtfd/readthedocs.org/issues/2101
        'djangorestframework==3.3.2',
        'djangorestframework-filters==0.8.0',
        'django-autoslug>=1.9.0',
        'django-guardian>=1.4.2',
        'django-mathfilters>=0.3.0',
        # XXX: django-versionfield2 0.5.0 does not work yet:
        # https://ci.genialis.com/job/resolwe-master/141/testReport/
        'django-versionfield2==0.4.0',
        'psycopg2>=2.5.0',
        'jsonfield>=1.0.3',
        'mock==1.3.0',
        'pyyaml>=3.11',
        'jsonschema>=2.4.0',
        'six>=1.10.0',
    ],
    extras_require = {
        'docs':  [
            # XXX: Intersphinx URLs are broken when using non-default inventory
            # (like in Django's case) with Sphinx 1.4.3+
            # Bug report: https://github.com/sphinx-doc/sphinx/issues/2789
            'Sphinx==1.4.2',
            'sphinx_rtd_theme',
        ],
        'package': [
            'twine',
            'wheel',
        ],
        'test': [
            'coverage>=3.7.1',
            'pep8>=1.6.2',
            'pylint>=1.4.3',
            'testfixtures>=4.10.0',
            'check-manifest',
            'readme_renderer',
            'resolwe-runtime-utils>=1.1.0',
            'Sphinx==1.4.2',
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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='resolwe dataflow django',
)
