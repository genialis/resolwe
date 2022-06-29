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
    python_requires=">=3.6, <3.11",
    install_requires=[
        "asgiref~=3.4.1;python_version < '3.7'",
        "asgiref~=3.5.2;python_version >= '3.7'",
        "asteval==0.9.26",
        "async-timeout~=4.0.2",
        "channels~=3.0.4",
        "channels_redis~=3.3.1",
        # Storage requirement for computing hashes.
        "crcmod",
        "kubernetes~=21.7.0",
        "docker~=5.0.3",
        "Django~=3.2.12",
        "djangorestframework~=3.13.1",
        "django-filter~=21.1",
        "django-versionfield~=1.0.2",
        "django-fernet-fields~=0.6",
        "django-priority-batch >=4.0a1, ==4.*",
        "Jinja2~=3.0.3",
        "jsonschema~=4.0.0",
        "plumbum~=1.7.2",
        "psycopg2-binary~=2.9.3",
        "PyYAML~=6.0",
        "redis~=4.1.3",
        "shellescape~=3.8.1",
        "beautifulsoup4~=4.10.0",
        "Sphinx~=4.3.2",
        "wrapt~=1.13.3",
        "pyzmq~=22.3.0",
    ],
    extras_require={
        "storage_s3": [
            "boto3~=1.21.0",
            "crcmod",
        ],
        "storage_gcs": [
            "crcmod",
            "google-cloud-storage~=2.0.0",
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
            "black>=22.1.0",
            "check-manifest>=0.47",
            "coverage>=6.2.0",
            "flake8>=4.0.1",
            "pydocstyle>=6.1.1",
            "readme_renderer",
            "setuptools_scm",
            "testfixtures>=6.18.3",
            "tblib>=1.7.0",
            "isort>=5.10.1",
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
        "Programming Language :: Python :: 3.10",
    ],
    keywords="resolwe dataflow django",
)
