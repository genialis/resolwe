# pylint: disable=missing-docstring
import os

from resolwe.flow.models import Data
from resolwe.test import ProcessTestCase, tag_process, with_docker_executor


class DirsProcessTestCase(ProcessTestCase):
    @with_docker_executor
    def setUp(self):
        super().setUp()
        self.files_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "files")
        )

        self.dir = self.run_process("upload-dir", {"src": "compressed dir.tar.gz"})
        self.dir_sym_link = self.run_process(
            "upload-dir", {"src": "dir sym link.tar.gz"}
        )

    @tag_process("upload-dir")
    def test_assert_dir_exists(self):
        self.assertDirExists(self.dir, "dir")
        self.assertDirExists(self.dir_sym_link, "dir")

    @tag_process("upload-dir")
    def test_assert_dir(self):
        self.assertDir(self.dir, "dir", "compressed dir.tar.gz")
        self.assertDir(self.dir_sym_link, "dir", "dir sym link.tar.gz")

    @tag_process("upload-dir")
    def test_assert_dir_structure(self):
        # Complete tree structure of 'compressed dir.tar.gz'
        structure = {
            "file binary": None,
            "text file.txt": None,
            "root dir": {
                "file binary": None,
                "text file.txt": None,
                "nested dir.ext": {"empty dir": {},},
            },
        }

        partial_structure = {
            "root dir": {"file binary": None,},
        }

        sym_link = {
            "text file.txt": None,
            "sym link": None,
        }

        self.assertDirStructure(self.dir, "dir", structure)
        self.assertRaisesRegex(
            AssertionError,
            r"Directory structure mismatch \(exact check\).",
            ProcessTestCase.assertDirStructure,
            self,
            self.dir,
            "dir",
            partial_structure,
        )
        self.assertDirStructure(self.dir, "dir", partial_structure, exact=False)

        self.assertDirStructure(self.dir_sym_link, "dir", sym_link)

    @with_docker_executor
    @tag_process("upload-dir")
    def test_bad_dir(self):
        self.run_process(
            "upload-dir", {"src": "dir bad format.tar.gz"}, Data.STATUS_ERROR
        )
