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
    dependency_links = ['https://github.com/yjmade/django-pgjsonb/tarball/master/#egg=django-pgjsonb'],
    install_requires=[
        'django>=1.8',
        'jsonfield>=1.0.3',
        'django-pgjsonb',
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
