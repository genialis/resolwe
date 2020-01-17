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

    @with_docker_executor
    @tag_process("upload-image-file")
    def test_upload_image(self):
        upload_image = self.run_process("upload-image-file", {"src": "file image.png"})
        self.assertFile(upload_image, "file", "file image.png")

    @with_docker_executor
    @tag_process("upload-tab-file")
    def test_upload_tabular_tab(self):
        upload_tab = self.run_process("upload-tab-file", {"src": "file tab.tab.gz"})
        self.assertFile(
            upload_tab, "file", "tab_file_tabular.tab.gz", compression="gzip"
        )
        self.assertFile(upload_tab, "src_file", "file tab.tab.gz", compression="gzip")

    @with_docker_executor
    @tag_process("upload-tab-file")
    def test_upload_tabular_csv(self):
        upload_csv = self.run_process("upload-tab-file", {"src": "file tab.csv.gz"})
        self.assertFile(
            upload_csv, "file", "csv_file_tabular.tab.gz", compression="gzip"
        )
        self.assertFile(upload_csv, "src_file", "file tab.csv.gz", compression="gzip")

    @with_docker_executor
    @tag_process("upload-tab-file")
    def test_upload_tabular_xls(self):
        upload_xls = self.run_process("upload-tab-file", {"src": "file tab.xls"})
        self.assertFile(
            upload_xls, "file", "xls_file_tabular.tab.gz", compression="gzip"
        )
        self.assertFile(upload_xls, "src_file", "file tab.xls")

    @with_docker_executor
    @tag_process("upload-tab-file")
    def test_upload_tabular_xlsx(self):
        upload_xlsx = self.run_process("upload-tab-file", {"src": "file tab.1.xlsx"})
        self.assertFile(
            upload_xlsx, "file", "xlsx_file_tabular.tab.gz", compression="gzip"
        )
        self.assertFile(upload_xlsx, "src_file", "file tab.1.xlsx")
