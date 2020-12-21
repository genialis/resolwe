# pylint: disable=missing-docstring
import os

from resolwe.test import ProcessTestCase, tag_process, with_docker_executor


class FilesProcessTestCase(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self.files_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "files")
        )

    @with_docker_executor
    @tag_process("upload-file")
    def test_upload_file(self):
        upload_file = self.run_process("upload-file", {"src": "file binary"})
        self.assertFile(upload_file, "file", "file binary")

    @with_docker_executor
    @tag_process("upload-file")
    def test_upload_file_nested_path(self):
        """Test that nested files from files directory are processed correctly."""
        upload_file = self.run_process(
            "upload-file", {"src": "upload_file/input_file.txt"}
        )
        self.assertFile(upload_file, "file", "upload_file/input_file.txt")
