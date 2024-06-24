# pylint: disable=missing-docstring
import hashlib
import os
import shutil
import tempfile

import crcmod
from django.conf import settings

from resolwe.storage.connectors import (
    AwsS3Connector,
    GoogleConnector,
    LocalFilesystemConnector,
)
from resolwe.storage.connectors.transfer import Transfer
from resolwe.storage.connectors.utils import get_transfer_object
from resolwe.test import TestCase

CONNECTORS = {
    "local": {
        "path": "CHANGE",
    },
    "S3": {
        "bucket": "genialis-test-storage",
        "credentials": os.path.join(
            settings.PROJECT_ROOT, "testing_credentials_s3.json"
        ),
    },
    "GCS": {
        "bucket": "genialis_storage_test",
        "credentials": os.path.join(
            settings.PROJECT_ROOT, "testing_credentials_gcs.json"
        ),
    },
}


def hashes(filename):
    """Compute MD5 and CRC32C hexdigest."""
    hashers = (hashlib.md5(), crcmod.predefined.PredefinedCrc("crc32c"))
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            for hasher in hashers:
                hasher.update(chunk)
    return {"md5": hashers[0].hexdigest(), "crc32c": hashers[1].hexdigest()}


def create_file(name, size):
    with open(name, "wb") as f:
        f.seek(size - 1)
        f.write(b"\0")


class BaseTestCase(TestCase):
    _mb = 1024 * 1024
    _files = (
        ("1", 1 * _mb),
        ("dir/2", 10 * _mb),
    )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tmp_dir = tempfile.TemporaryDirectory()
        local_settings = CONNECTORS["local"].copy()
        local_settings["path"] = cls.tmp_dir.name
        cls.created_files = [f[0] for f in cls._files]
        cls.local = LocalFilesystemConnector(local_settings, "local")
        cls.gcs = GoogleConnector(CONNECTORS["GCS"], "GCS")
        cls.s3 = AwsS3Connector(CONNECTORS["S3"], "S3")

    @classmethod
    def _purge_tmp_dir(cls):
        for root, dirs, files in os.walk(cls.tmp_dir.name):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

    @classmethod
    def _create_files(cls):
        os.makedirs(cls.path(cls, ""))
        os.makedirs(cls.path(cls, "dir"))
        cls.transfer_objects = []
        for file_, size in cls._files:
            create_file(cls.path(cls, file_), size)
            with open(cls.path(cls, file_), "r+b") as f:
                f.write((file_.encode("utf-8")))
            cls.transfer_objects.append(
                get_transfer_object(
                    cls.path(cls, file_), os.path.join(cls.tmp_dir.name, cls._prefix)
                )
            )

    @classmethod
    def _copy_files_to(cls, connector):
        for file_ in cls.created_files:
            connector_path = os.path.join(cls._prefix, file_)
            if not connector.exists(connector_path):
                with open(cls.path(cls, file_), "rb") as f:
                    connector.push(f, connector_path)
                    connector.set_hashes(connector_path, hashes(cls.path(cls, file_)))

    def path(self, filename):
        return os.path.join(self.tmp_dir.name, self._prefix, filename)

    def _test_from_to(self, from_connector, to_connector):
        t = Transfer(from_connector, to_connector)
        t.transfer_objects(self._prefix, self.transfer_objects)
        objects = to_connector.get_object_list(self._prefix)
        self.assertEqual(len(objects), len(self.created_files))
        for file_ in self.created_files:
            real_path = os.path.join(self._prefix, file_)
            self.assertIn(file_, objects)
            for type_ in ("md5", "crc32c"):
                self.assertEqual(
                    from_connector.get_hash(real_path, type_).lower(),
                    to_connector.get_hash(real_path, type_).lower(),
                )


class TransferFromLocalTest(BaseTestCase):
    _prefix = "local"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._create_files()
        cls.gcs.delete(cls._prefix, cls.created_files)
        cls.s3.delete(cls._prefix, cls.created_files)

    def test_to_google(self):
        self._test_from_to(self.local, self.gcs)

    def test_to_s3(self):
        self._test_from_to(self.local, self.s3)


class TransferFromGoogleTest(BaseTestCase):
    _prefix = "google"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._create_files()
        cls.gcs.delete(cls._prefix, cls.created_files)
        cls._copy_files_to(cls.gcs)
        cls._purge_tmp_dir()

    def setUp(self):
        super().setUp()
        self.local.delete(self._prefix, self.created_files)
        self.s3.delete(self._prefix, self.created_files)

    def test_to_local(self):
        self._test_from_to(self.gcs, self.local)

    def test_to_s3(self):
        self._test_from_to(self.gcs, self.s3)


class TransferFromS3Test(BaseTestCase):
    _prefix = "s3"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._create_files()
        cls.s3.delete(cls._prefix, cls.created_files)
        cls._copy_files_to(cls.s3)
        cls._purge_tmp_dir()

    def setUp(self):
        super().setUp()
        self.gcs.delete(self._prefix, self.created_files)
        self.local.delete(self._prefix, self.created_files)

    def test_to_local(self):
        self._test_from_to(self.s3, self.local)

    def test_to_gcs(self):
        self._test_from_to(self.s3, self.gcs)
