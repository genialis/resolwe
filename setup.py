#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages, setup


setup(
    name='Resolwe',
    version=__import__('resolwe').VERSION,
    url='https://github.com/genialis/resolwe',
    author='Genialis d.o.o.',
    author_email='info@genialis.com',
    description='Open source enterprise dataflow engine in Django.',
    license='Apache License (2.0)',
    long_description=open('README.rst', 'r').read(),
    packages=find_packages(),
    install_requires=[
        'django>=1.8,<1.9a1',
        'djangorestframework>=3.1',
        'django-guardian==1.3',
        'django-filter>=0.9.2',
        'django-pgjsonb>=0.0.16',
        'django-mathfilters>=0.3.0',
        'django-versionfield2>=0.4.0',
        'jsonfield>=1.0.3',
        'pyyaml>=3.11',
        'jsonschema>=2.4.0',
    ],
    extras_require = {
        'docs':  ['sphinx>=1.3.2'],
        'test': [
            'django-jenkins>=0.17.0',
            'coverage>=3.7.1',
            'pep8>=1.6.2',
            'pylint>=1.4.3',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP :: WSGI',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
