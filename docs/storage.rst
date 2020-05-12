=========================
Resolwe Storage Framework
=========================

Resolwe Storage Framework is storage management system for Resolwe Flow Design.
Currenlty it supports storing data to local filesystem, Google Cloud Storage
and Amazon Simple Storage Service.

Testing connectors
==================

When testing connectors make sure there are credentials available (see sample
settings bellow for details). These tests start with 
``storage_credentials_test_`` and are skipped by default when running tox. Since
tox does not allow decrypting files on pull requests these tests would fail on
all pull requests blocking merge.

Example settings
================

.. code::

    # Storage connector sample settings.
    STORAGE_LOCAL_CONNECTOR = 'local'
    STORAGE_CONNECTORS = {
        # Code assumes that connector named 'local' exists and points
        # to the location FLOW_EXECUTOR["DATA_DIR"].
        # If this is not true BAD THING WILL HAPPEN.
        "local": {
            "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
            "config": {
                "priority": 0,  # If ommited, default 100 is used
                "path": FLOW_EXECUTOR["DATA_DIR"],
                # Public URL from where data is served
                "public_url": "/local_data",
                # Delete from here after delay days from last access to this storage
                # location and when min_other_copies of data exist on other
                # locations.
                "delete": {
                    "delay": 2,  # in days
                    "min_other_copies": 2,
                },
                "copy": {
                    # Override default settings for this data_slug.
                    "data_slug": {
                        "00hrfastqgz-66": {
                            "delay": 0
                        }
                    },
                    "delay": 10,
                }
            }
        },
        "S3": {
            "connector": "resolwe.storage.connectors.s3connector.AwsS3Connector",
            "config": {
                "priority": 10,
                "bucket": "genialis-test-storage",
                "copy": {  # copy here from delay days from creation of filestorage object
                    "delay": 5,  # in days
                },
                # Region name is needed to generate valid pre-signed urls.
                "region_name": "eu-central-1",
                "credentials": os.path.join(
                    PROJECT_ROOT, "testing_credentials_s3.json"
                ),
            },
        },
        "GCS": {
            "connector": "resolwe.storage.connectors.googleconnector.GoogleConnector",
            "config": {
                "bucket": "genialis-test-storage",
                "credentials": os.path.join(
                    PROJECT_ROOT, "testing_credentials_gcs.json"
                ),
                "copy": {
                    "delay": 2,  # in days
                },
            },
        },
    }
