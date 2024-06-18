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
        "asgiref~=3.8.1",
        "asteval==0.9.33",
        "async-timeout~=4.0.3",
        "channels~=4.1.0",
        "channels_redis~=4.2.0",
        # Storage requirement for computing hashes.
        "crcmod",
        "kubernetes~=29.0.0",
        "docker~=7.1.0",
        "Django~=4.2",
        "djangorestframework~=3.15.1",
        "django-filter~=24.2",
        "django-versionfield~=1.0.3",
        "django-fernet-fields-v2~=0.9",
        "drf-spectacular~=0.27.2",
        "Jinja2~=3.1.4",
        "jsonschema~=4.22.0",
        "plumbum~=1.8.3",
        "psycopg[binary]~=3.1.19",
        "python-decouple~=3.8",
        "PyYAML~=6.0.1",
        "redis~=5.0.4",
        "shellescape~=3.8.1",
        "beautifulsoup4~=4.12.3",
        "Sphinx~=7.3.7",
        "wrapt~=1.16.0",
        "pyzmq~=26.0.3",
        "uvloop~=0.19.0",
        "pytz~=2024.1", 
    ],
    extras_require={
        "storage_s3": [
            "boto3~=1.34.118",
            "crcmod",
        ],
        "storage_gcs": [
            "crcmod",
            "google-cloud-storage~=2.16.0",
        ],
        "docs": [
            "sphinx_rtd_theme",
            "pyasn1>=0.6.0",
            "daphne>=4.1.2",
        ],
        "package": [
            "twine",
            "wheel",
        ],
        "test": [
            "black==24.4.2",
            "check-manifest>=0.49",
            "coverage>=7.5.3",
            "flake8>=7.0.0",
            "pydocstyle>=6.3.0",
            "readme_renderer",
            "setuptools_scm",
            "testfixtures>=8.2.0",
            "tblib>=3.0.0",
            "isort>=5.13.2",
            "daphne>=4.1.2",
            "django-stubs>=4.2.4",
            "djangorestframework-stubs[compatible-mypy]>=3.15.0",
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
