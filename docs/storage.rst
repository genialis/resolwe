=========================
Resolwe Storage Framework
=========================

Resolwe Storage Framework is storage management system for Resolwe Flow Design.
Currenlty it supports storing data to local filesystem, Google Cloud Storage
and Amazon Simple Storage Service.

Example settings
================

.. code::

    # Storage connector sample settings.
    LOCAL_CONNECTOR = 'local'
    STORAGE_CONNECTORS = {
        # Code assumes that connector named 'local' exists and points
        # to the location FLOW_EXECUTOR["DATA_DIR"].
        # If this is not true BAD THING WILL HAPPEN.
        "local": {
            "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
            "config": {
                "priority": 0,  # If ommited, default 100 is used
                "path": FLOW_EXECUTOR["DATA_DIR"],
                # Delete from here after delay days from last access to this storage
                # location and when min_other_copies of data exist on other
                # locations.
                "delete": {
                    "delay": 2,  # in days
                    "min_other_copies": 2,
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
                # Two values bellow affect e_tag computation on Amazon S3 connector.
                # Default value for both settings is 8MB.
                "multipart_threshold": 8*1024*1024,
                "multipart_chunksize": 8*1024*1024,
                "credentials": os.path.join(
                    PROJECT_ROOT, "testing_credentials_s3.json"
                ),
            },
        },
        "GCS": {
            "connector": "resolwe.storage.connectors.googleconnector.GoogleConnector",
            "config": {
                "priority": 10,
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
