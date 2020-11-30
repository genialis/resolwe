# pylint: disable=missing-docstring
import copy
from pathlib import Path

from django.conf import settings

from resolwe.flow.executors.constants import DATA_LOCAL_VOLUME, TMPDIR
from resolwe.flow.models import Data, Process
from resolwe.test import TransactionTestCase, with_docker_executor


class EnvVarsTest(TransactionTestCase):
    @with_docker_executor
    def test_envvars(self):
        flow_executor = copy.copy(getattr(settings, "FLOW_EXECUTOR", {}))
        flow_executor["SET_ENV"] = {
            "SET_ENV_TEST": "test_var",
        }

        with self.settings(
            FLOW_EXECUTOR=flow_executor, RESOLWE_HOST_URL="some.special.host"
        ):
            process = Process.objects.create(
                name="Test environment variables",
                requirements={"expression-engine": "jinja"},
                contributor=self.contributor,
                type="test:data:envvars:",
                input_schema=[],
                output_schema=[
                    {"name": "resolweapihost", "type": "basic:string:"},
                    {"name": "setenvtest", "type": "basic:string:"},
                    {"name": "tmpdir", "type": "basic:string:"},
                ],
                run={
                    "language": "bash",
                    "program": """
re-save resolweapihost $RESOLWE_HOST_URL
re-save setenvtest $SET_ENV_TEST
re-save tmpdir $TMPDIR
""",
                },
            )

            data = Data.objects.create(
                name="Data object",
                contributor=self.contributor,
                process=process,
                input={},
            )

            # update output
            data = Data.objects.get(pk=data.pk)
            tmp_path = DATA_LOCAL_VOLUME / TMPDIR
            self.assertEqual(data.output["resolweapihost"], "some.special.host")
            self.assertEqual(data.output["setenvtest"], "test_var")
            self.assertEqual(Path(data.output["tmpdir"]), tmp_path)
