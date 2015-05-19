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
    license='BSD',
    long_description=open('README.rst', 'r').read(),
    packages=find_packages(),
    dependency_links=[
        'https://github.com/genialis/django-pgjsonb/tarball/master#egg=django-pgjsonb-0.0.12'
    ],
    install_requires=[
        'django>=1.8',
        'djangorestframework>=3.1',
        'django-filter>=0.9.2',
        'django-pgjsonb==0.0.12',
        'django-jenkins==0.17.0',
        'django-mathfilters>=0.3.0'
        'jsonfield>=1.0.3',
        'pyyaml>=3.11',
        'jsonschema>=2.4.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
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
