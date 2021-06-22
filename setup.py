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
    python_requires=">=3.6, <3.10",
    install_requires=[
        # XXX: Temporarily pin asgiref to 3.2.x since testing framework freezes
        # with version 3.3.x
        "asgiref~=3.3.1",
        # Temporarily pin asteval, version >= 0.9.24 is no longer compatible with
        # Python 3.6.
        "asteval==0.9.23",
        "async-timeout~=3.0.0",
        "channels~=3.0.3",
        "channels_redis~=3.2.0",
        # Storage requirement for computing hashes.
        "crcmod",
        "kubernetes~=12.0.1",
        "docker~=4.4.4",
        "Django~=3.1.7",
        "djangorestframework~=3.12.2",
        "django-filter~=2.4.0",
        "django-guardian~=2.3.0",
        "django-versionfield~=1.0.2",
        "django-fernet-fields~=0.6",
        "django-priority-batch >=3.0a1, ==3.*",
        "Jinja2~=2.11.3",
        "jsonschema~=3.2.0",
        "plumbum~=1.7.0",
        "psycopg2-binary~=2.8.6",
        "PyYAML~=5.4.1",
        "redis~=3.5.3",
        "resolwe-runtime-utils~=3.0.0",
        "shellescape~=3.8.1",
        "beautifulsoup4~=4.9.3",
        "Sphinx~=3.5.3",
        "wrapt~=1.12.1",
        "pyzmq~=22.0.3",
    ],
    extras_require={
        "storage_s3": [
            "boto3~=1.17.29",
            "crcmod",
        ],
        "storage_gcs": [
            "crcmod",
            "google-cloud-storage~=1.35.0",
        ],
        "docs": [
            "sphinx_rtd_theme",
            "pyasn1>=0.4.8",
        ],
        "package": [
            "twine",
            "wheel",
        ],
        "test": [
            "black>=20.8b1",
            "check-manifest>=0.46",
            "coverage>=5.3.1",
            "flake8>=3.8.4",
            "pydocstyle>=5.1.1",
            "readme_renderer",
            "setuptools_scm",
            "testfixtures>=6.17.1",
            "tblib>=1.7.0",
            "isort>=5.7.0",
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="resolwe dataflow django",
)
