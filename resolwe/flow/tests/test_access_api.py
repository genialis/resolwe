# pylint: disable=missing-docstring
import sys
import unittest

from django.contrib.auth.models import AnonymousUser
from django.test import LiveServerTestCase

from guardian.shortcuts import assign_perm

from resolwe.flow.models import Collection, Process, Storage
from resolwe.test import (
    ProcessTestCase,
    check_installed,
    tag_process,
    with_docker_executor,
    with_resolwe_host,
)


class AccessAPIFromExecutorProcessTestCase(ProcessTestCase, LiveServerTestCase):
    def _create_collection(self):
        """Create a test collection that will be accessed via a test process.

        Assign :class:`~django.contrib.auth.models.AnonymousUser`
        ``view_collection`` permission to the created collection.

        :return: created test collection
        :rtype: Collection

        """
        collection = Collection.objects.create(
            name="collection-foo",
            contributor=self.contributor,
        )
        assign_perm("view_collection", AnonymousUser(), collection)
        return collection

    def create_process(self):
        """Create a test process that will query the API for collections.

        Get the list of all collections using ``curl`` to query the Live
        Resolwe host.

        :return: created test process
        :rtype: Process

        """
        process = Process.objects.create(
            name="Test accessing API from process",
            requirements={"expression-engine": "jinja", "resources": {"network": True}},
            contributor=self.contributor,
            type="data:test:api-access",
            input_schema=[],
            output_schema=[
                {
                    "name": "collection-list",
                    "type": "basic:json:",
                    "required": False,
                },
            ],
            run={
                "language": "bash",
                # NOTE: Resolwe Runtime Utilities are not available in the
                # resolwe/test:base Docker image so we need to use plain echo
                # NOTE: curl must be called with --silent argument to not ouput
                # its progress since it can interfere with parsing of JSON on
                # process' standard output
                "program": r"""
re-save collection-list "$(curl --silent --show-error $RESOLWE_HOST_URL/api/collection)"
""",
            },
        )
        return process

    def setUp(self):
        """Custom initilization of :class:`~ProcessTestCase`."""
        super().setUp()

        self.process = self.create_process()

    def check_results(self, data):
        """Check if information obtained for collection via API is correct."""
        if "collection-list" not in data.output:
            self.fail(
                "The test process didn't obtain collection list from the live Resolwe host."
                + self._debug_info(data)
            )
        collection_list = Storage.objects.get(data=data).json
        self.assertEqual(len(collection_list), 1)
        self.assertEqual(collection_list[0]["slug"], self.collection.slug)

    @unittest.skipUnless(*check_installed("curl"))
    @tag_process("test-accessing-api-from-process")
    def test_access_api_from_local_executor_process_without_decorator(self):
        """Test if a process running via local executor cannot access API."""
        data = self.run_process(self.process.slug)
        with self.assertRaises(AssertionError):
            self.check_results(data)

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "Accessing live Resolwe host from a Docker container on non-Linux systems is not possible yet.",
    )
    @with_resolwe_host
    @with_docker_executor
    @tag_process("test-accessing-api-from-process")
    def test_access_api_from_docker_executor_process(self):
        """Test if a process running via Docker executor can access API."""

        # NOTE: pylint gets confused if the above string is inline with the method name
        data = self.run_process(self.process.slug)
        self.check_results(data)

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "Accessing live Resolwe host from a Docker container on non-Linux systems is not possible yet.",
    )
    @with_docker_executor
    @with_resolwe_host
    @tag_process("test-accessing-api-from-process")
    def test_access_api_from_docker_executor_process_reverse_decorators(self):
        """Test if a process running via Docker executor can access API."""

        # NOTE: pylint gets confused if the above string is inline with the method name
        data = self.run_process(self.process.slug)
        self.check_results(data)
