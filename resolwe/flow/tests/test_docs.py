"""
Tests for autoprocess Sphinx extension.

Current implementation of tests only checks that:

    * build process is executed with zero exit status
    * desired html file is produced
    * no errors or warnings were raised during build process

"""
# pylint: disable=missing-docstring

import os
import shutil

from sphinx import build_main

from resolwe.test import TestCase


class TestAutoporocess(TestCase):
    def setUp(self):
        super().setUp()

        self.rst_source_files_dir = os.path.dirname(__file__) + "/files/"
        self.build_output_dir = os.path.dirname(__file__) + "/build"
        self.stderr_file = os.path.abspath(
            os.path.dirname(__file__) + "/files/errors.txt"
        )

    def tearDown(self):
        # Remove the build folder and its contents
        shutil.rmtree(self.build_output_dir)
        super().tearDown()

    def test_build_ok(self):
        args = [
            "build_sphinx",
            "-E",  # Dont use a saved environment but rebuild it completely.
            "-q",  # Do not output anything on standard output, only write warnings and errors to standard error.
            "-w{}".format(
                self.stderr_file
            ),  # Write warnings (and errors) to the given file
            self.rst_source_files_dir,  # souce dir of rst files
            self.build_output_dir,  # output dir for html files
        ]

        exit_status = build_main(args)

        self.assertEqual(exit_status, 0)
        # Confirm that html file was generated:
        self.assertTrue(
            os.path.isfile(os.path.join(self.build_output_dir, "contents.html"))
        )
        #  Confirm there is no content (no errors/warnings) in self.stderr_file:
        self.assertEqual(os.stat(self.stderr_file).st_size, 0)
