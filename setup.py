import os.path

import setuptools

# Get the long description from README.
with open("README.rst", "r") as fh:
    long_description = fh.read()

# Get package metadata from '__about__.py' file.
about = {}
base_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(base_dir, "resolwe", "__about__.py"), "r") as fh:
    exec(fh.read(), about)

setuptools.setup(
    name=about["__title__"],
    use_scm_version=True,
    description=about["__summary__"],
    long_description=long_description,
    long_description_content_type="text/x-rst",
    author=about["__author__"],
    author_email=about["__email__"],
    url=about["__url__"],
    license=about["__license__"],
    # Exclude tests from built/installed package.
    packages=setuptools.find_packages(
        exclude=["tests", "tests.*", "*.tests", "*.tests.*"]
    ),
    package_data={
        "resolwe": [
            "flow/executors/requirements.txt",
            "flow/migrations/*.sql",
            "flow/static/flow/*.json",
            "toolkit/processes/**.yml",
            "toolkit/processes/**.py",
            "toolkit/tools/**.py",
        ]
    },
    python_requires=">=3.11, <3.13",
    install_requires=[
        "asgiref~=3.6.0",
        "asteval==0.9.29",
        "async-timeout~=4.0.2",
        "channels~=4.0.0",
        "channels_redis~=4.1.0",
        # Storage requirement for computing hashes.
        "crcmod",
        "kubernetes~=26.1.0",
        "docker~=6.1.1",
        "Django~=4.2",
        "djangorestframework~=3.14.0",
        "django-filter~=23.1",
        "django-versionfield~=1.0.3",
        "django-fernet-fields-v2~=0.8",
        "drf-spectacular~=0.26.4",
        "Jinja2~=3.1.3",
        "jsonschema~=4.17.3",
        "plumbum~=1.8.1",
        "psycopg[binary]~=3.1.9",
        "python-decouple~=3.8",
        "PyYAML~=6.0",
        "redis~=4.5.4",
        "shellescape~=3.8.1",
        "beautifulsoup4~=4.12.2",
        "Sphinx~=6.1.3",
        "wrapt~=1.15.0",
        "pyzmq~=26.0.3",
        "uvloop~=0.19.0",
        # The requests library version 2.32 is incompatible with the docker-py.
        # See https://github.com/psf/requests/issues/6707 for details.
        # Remove the version pin when the issue is resolved.
        "requests==2.31.0",
    ],
    extras_require={
        "storage_s3": [
            "boto3~=1.26.109",
            "crcmod",
        ],
        "storage_gcs": [
            "crcmod",
            "google-cloud-storage~=2.8.0",
        ],
        "docs": [
            "sphinx_rtd_theme",
            "pyasn1>=0.4.8",
            "daphne>=4.0.0",
        ],
        "package": [
            "twine",
            "wheel",
        ],
        "test": [
            "black==24.1.0",
            "check-manifest>=0.49",
            "coverage>=7.2.3",
            "flake8>=6.0.0",
            "pydocstyle>=6.3.0",
            "readme_renderer",
            "setuptools_scm",
            "testfixtures>=7.1.0",
            "tblib>=1.7.0",
            "isort>=5.12.0",
            "daphne>=4.0.0",
            "django-stubs>=4.2.4",
            "djangorestframework-stubs[compatible-mypy]>=3.14.0",
            "types-setuptools",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords="resolwe dataflow django",
)
