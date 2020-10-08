# pylint: disable=missing-docstring
import os

from django.test import override_settings
from django.utils.crypto import get_random_string

from resolwe.flow.models import Process
from resolwe.test import ProcessTestCase


class PurgeTestFieldsMixin:
    """
    This class contains tests, which validate each field individually. It is used
    to perform basic unit tests (on simulated data, without running any processors)
    as well as more complex end-to-end tests (by running processors). Having them
    in this mixin makes sure that both, unit and e2e tests, are run for each field.
    """

    def test_basic_file(self):
        self.assertFieldWorks(
            "basic:file:",
            field_value={"file": "but_not_this"},
            script_setup="touch remove_this but_not_this",
            script_save="re-save-file sample but_not_this",
            removed=["remove_this"],
            not_removed=["but_not_this"],
        )

    def test_basic_file_specialization(self):
        self.assertFieldWorks(
            "basic:file:html:",
            field_value={"file": "but_not_this"},
            script_setup="touch remove_this but_not_this",
            script_save="re-save-file sample but_not_this",
            removed=["remove_this"],
            not_removed=["but_not_this"],
        )

    def test_basic_file_list(self):
        self.assertFieldWorks(
            "list:basic:file:",
            field_value=[{"file": "but_not_this"}, {"file": "and_not_this"}],
            script_setup="touch remove_this but_not_this and_not_this",
            script_save="re-save-file-list sample but_not_this and_not_this",
            removed=["remove_this"],
            not_removed=["but_not_this", "and_not_this"],
        )

    def test_basic_dir(self):
        self.assertFieldWorks(
            "basic:dir:",
            field_value={"dir": "but_not_this"},
            script_setup="""
mkdir remove_this but_not_this
touch remove_this/a remove_this/b remove_this/c
touch but_not_this/a but_not_this/b but_not_this/c
""",
            script_save="re-save-dir sample but_not_this",
            removed=["remove_this/", "remove_this/a", "remove_this/b", "remove_this/c"],
            not_removed=["but_not_this/a", "but_not_this/b", "but_not_this/c"],
        )

    def test_basic_dir_list(self):
        self.assertFieldWorks(
            "list:basic:dir:",
            field_value=[{"dir": "but_not_this"}, {"dir": "and_not_this"}],
            script_setup="""
mkdir remove_this but_not_this and_not_this
touch remove_this/a remove_this/b remove_this/c
touch but_not_this/a but_not_this/b but_not_this/c
touch and_not_this/a and_not_this/b and_not_this/c
""",
            script_save="re-save-dir-list sample but_not_this and_not_this",
            removed=["remove_this/", "remove_this/a", "remove_this/b", "remove_this/c"],
            not_removed=[
                "but_not_this/a",
                "but_not_this/b",
                "but_not_this/c",
                "and_not_this/a",
                "and_not_this/b",
                "and_not_this/c",
            ],
        )


@override_settings(TEST_PROCESS_REQUIRE_TAGS=False)  # Test uses dynamic processes.
class PurgeE2ETest(PurgeTestFieldsMixin, ProcessTestCase):
    def create_and_run_processor(self, processor, **kwargs):
        processor_slug = get_random_string(6)
        Process.objects.create(
            slug=processor_slug,
            name="Test Purge Process",
            contributor=self.admin,
            type="data:test",
            version=1,
            **processor,
        )
        data = self.run_process(processor_slug, **kwargs)
        return data

    def assertFilesRemoved(self, data, *files):
        for name in files:
            path = data.location.get_path(filename=name)
            self.assertFalse(os.path.isfile(path), msg=path)

    def assertFilesNotRemoved(self, data, *files):
        for name in files:
            path = data.location.get_path(filename=name)
            self.assertTrue(os.path.isfile(path), msg=path)

    def test_complex_processor(self):
        data = self.create_and_run_processor(
            processor=dict(
                input_schema=[],
                output_schema=[
                    {
                        "name": "sample_file",
                        "label": "Sample output file",
                        "type": "basic:file:",
                    },
                    {
                        "name": "another_sample_file",
                        "label": "Another sample output file",
                        "type": "basic:file:",
                    },
                    {
                        "name": "sample_dir",
                        "label": "Sample output directory",
                        "type": "basic:dir:",
                    },
                    {
                        "name": "sample_file_list",
                        "label": "Sample list of output files",
                        "type": "list:basic:file:",
                    },
                    {
                        "name": "sample_dir_list",
                        "label": "Sample list of output directories",
                        "type": "list:basic:dir:",
                    },
                ],
                run={
                    "language": "bash",
                    "program": """
touch these files should be removed
touch this_file_should_stay
mkdir -p should_stay/
touch should_stay/file
mkdir -p directory/should/be
touch directory/should/be/removed
touch directory/should/be/removed2
mkdir -p stay/directory
touch stay/directory/file1
touch stay/directory/file2
touch entry1 entry2 entry3
mkdir dir1 dir2 dir3
touch dir1/a dir2/b dir3/c dir3/d
touch ref1 ref2 ref3 ref4
mkdir refs
touch refs/a refs/b

re-save-file another_sample_file should_stay/file
re-save-file sample_file this_file_should_stay ref3
re-save-dir sample_dir stay ref4
re-save-file-list sample_file_list entry1 entry2:ref1 entry3:refs
re-save-dir-list sample_dir_list dir1:ref2 dir2 dir3
""",
                },
            )
        )

        self.assertFilesRemoved(
            data, "these", "files", "should", "be", "removed", "directory"
        )
        self.assertFilesNotRemoved(
            data,
            "this_file_should_stay",
            "stay/directory/file1",
            "stay/directory/file2",
            "entry1",
            "entry2",
            "entry3",
            "dir1/a",
            "dir2/b",
            "dir3/c",
            "dir3/d",
            "ref1",
            "ref2",
            "ref3",
            "ref4",
            "refs/a",
            "refs/b",
            "should_stay/file",
        )

    def assertFieldWorks(
        self, field_type, field_value, script_setup, script_save, removed, not_removed
    ):
        """
        Checks that a field is handled correctly when running a processor, which
        uses the field.
        """

        field_schema = {"name": "sample", "label": "Sample output", "type": field_type}

        # Test output.
        data = self.create_and_run_processor(
            processor=dict(
                input_schema=[],
                output_schema=[field_schema],
                run={"language": "bash", "program": script_setup + "\n" + script_save},
            )
        )
        self.assertFilesRemoved(data, *removed)
        self.assertFilesNotRemoved(data, *not_removed)

        # Test descriptor.
        # descriptor_schema = DescriptorSchema.objects.create(
        #     slug=get_random_string(6), contributor=self.admin, schema=[field_schema]
        # )
        # data = self.create_and_run_processor(
        #     processor=dict(
        #         input_schema=[],
        #         output_schema=[],
        #         run={"language": "bash", "program": script_setup},
        #     ),
        #     descriptor_schema=descriptor_schema,
        #     descriptor={"sample": field_value},
        # )

        # self.assertFilesRemoved(data, *removed)
        # self.assertFilesNotRemoved(data, *not_removed)
