# pylint: disable=missing-docstring
import os

from resolwe.test import ProcessTestCase, tag_process, with_docker_executor


class ArchiverProcessTestCase(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self.files_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "files")
        )

    @with_docker_executor
    @tag_process("archiver")
    def test_archiver(self):
        with self.preparation_stage():
            binary = self.run_process("upload-file", {"src": "file binary"})
            image = self.run_process("upload-image-file", {"src": "file image.png"})
            tab = self.run_process("upload-tab-file", {"src": "file tab.txt"})

        archive = self.run_process(
            "archiver", {"data": [binary.id, image.id, tab.id], "fields": ["file"]}
        )

        self.assertFileExists(archive, "archive")
