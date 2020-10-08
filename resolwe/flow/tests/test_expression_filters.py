# pylint: disable=missing-docstring
from resolwe.flow.expression_engines import EvaluationError
from resolwe.flow.managers import manager
from resolwe.flow.models import Data, DescriptorSchema, Process, Storage
from resolwe.test import TestCase, TransactionTestCase


class ProcessFieldsTagsTest(TransactionTestCase):
    def test_templatetags(self):
        inputs_input_process = Process.objects.create(
            name="Inputs input process",
            contributor=self.contributor,
            type="data:test:inputsinputobject:",
            run={"language": "bash", "program": "echo foobar"},
        )

        inputs_input_data = Data.objects.create(
            name="Inputs input Data object",
            contributor=self.contributor,
            process=inputs_input_process,
        )

        input_process = Process.objects.create(
            name="Input process",
            contributor=self.contributor,
            type="data:test:inputobject:",
            input_schema=[
                {"name": "test_input", "type": "data:test:inputsinputobject:"},
            ],
            output_schema=[
                {"name": "test_file", "type": "basic:file:"},
            ],
            run={
                "language": "bash",
                "program": """
mkdir -p path/to
touch path/to/file.txt
re-save-file test_file path/to/file.txt
""",
            },
        )

        descriptor_schema = DescriptorSchema.objects.create(
            name="Test schema",
            slug="test-schema",
            contributor=self.contributor,
            schema=[
                {
                    "name": "descriptions",
                    "required": False,
                    "group": [
                        {"name": "text", "type": "basic:string:"},
                    ],
                }
            ],
        )

        input_data = Data.objects.create(
            name="Input Data object",
            contributor=self.contributor,
            process=input_process,
            input={"test_input": inputs_input_data.pk},
            descriptor_schema=descriptor_schema,
            descriptor={"descriptions": {"text": "This is test Data object."}},
        )

        process = Process.objects.create(
            name="Test template tags",
            requirements={"expression-engine": "jinja"},
            contributor=self.contributor,
            type="test:data:templatetags:",
            input_schema=[
                {"name": "input_data", "type": "data:test:inputobject:"},
                {"name": "input_data_list", "type": "list:data:test:inputobject:"},
                {"name": "spacy", "type": "basic:string:"},
            ],
            output_schema=[
                {"name": "name", "type": "basic:string:"},
                {"name": "list_name", "type": "list:basic:string:"},
                {"name": "slug", "type": "basic:string:"},
                {"name": "list_slug", "type": "list:basic:string:"},
                {"name": "id", "type": "basic:integer:"},
                {"name": "list_id", "type": "list:basic:string:"},
                {"name": "type", "type": "basic:string:"},
                {"name": "list_type", "type": "list:basic:string:"},
                {"name": "basename", "type": "basic:string:"},
                {"name": "dirname", "type": "basic:string:"},
                {"name": "subtype", "type": "basic:string:"},
                {"name": "list_subtype", "type": "list:basic:string:"},
                {"name": "yesno", "type": "basic:string:"},
                {"name": "datalookup", "type": "basic:integer:"},
                {"name": "file_url", "type": "basic:string:"},
                {"name": "relative_path", "type": "basic:string:"},
                {"name": "relative_path2", "type": "basic:string:"},
                {"name": "unsafe", "type": "basic:string:"},
                {"name": "safe", "type": "basic:string:"},
                {"name": "description_text", "type": "basic:string:"},
                {"name": "description_full", "type": "basic:json:"},
                {"name": "list_description_text", "type": "basic:string:"},
                {"name": "list_description_full", "type": "basic:json:"},
                {"name": "all", "type": "basic:string:"},
                {"name": "any", "type": "basic:string:"},
                {"name": "input_id", "type": "basic:integer:"},
            ],
            run={
                "language": "bash",
                "program": """
re-save name {{ input_data | name }}
re-save list_name {{ input_data_list | name }}
re-save slug {{ input_data | slug }}
re-save list_slug {{ input_data_list | slug }}
re-save id {{ input_data | id }}
re-save list_id {{ input_data_list | id }}
re-save type {{ input_data | type }}
re-save list_type {{ input_data_list | type }}
re-save basename {{ '/foo/bar/moo' | basename }}
re-save dirname {{ '/foo/bar/moo' | dirname }}
re-save subtype {{ 'data:test:inputobject:' | subtype('data:') }}
re-save list_subtype {{ ['data:test:inputobject:'] | subtype('data:') }}
re-save yesno {{ true | yesno('yes', 'no') }}
re-save datalookup {{ 'input-data-object' | data_by_slug }}
re-save file_url {{ input_data.test_file | get_url }}
re-save relative_path {{ input_data.test_file | relative_path }}
re-save relative_path2 {{ input_data.test_file.file | relative_path }}
re-save unsafe {{ spacy }}
re-save description_text {{ input_data | descriptor('descriptions.text') }}
re-save description_full {{ input_data | descriptor }}
re-save list_description_text {{ input_data_list[0] | descriptor('descriptions.text') }}
re-save list_description_full {{ input_data_list[0] | descriptor }}
re-save all {{ [true, false] | all }}
re-save any {{ [true, false] | any }}
re-save input_id {{ input_data | input('test_input') | id }}

function save-safe() {
    re-save safe $1
}
save-safe {{ spacy | safe }}
""",
            },
        )
        data = Data.objects.create(
            name="Data object",
            contributor=self.contributor,
            process=process,
            input={
                "input_data": input_data.pk,
                "input_data_list": [input_data.pk],
                "spacy": "this has 'some' spaces",
            },
        )

        data.refresh_from_db()
        self.assertEqual(data.output["name"], input_data.name)
        self.assertEqual(data.output["slug"], input_data.slug)
        self.assertEqual(data.output["id"], input_data.pk)
        self.assertEqual(data.output["list_id"], [input_data.id])
        self.assertEqual(data.output["type"], input_process.type)
        self.assertEqual(data.output["basename"], "moo")
        self.assertEqual(data.output["dirname"], "/foo/bar")
        self.assertEqual(data.output["subtype"], "True")
        self.assertEqual(data.output["yesno"], "yes")
        self.assertEqual(data.output["datalookup"], input_data.pk)
        self.assertEqual(
            data.output["file_url"],
            "localhost/data/{}/path/to/file.txt".format(input_data.pk),
        )
        self.assertEqual(data.output["relative_path"], "path/to/file.txt")
        self.assertEqual(data.output["relative_path2"], "path/to/file.txt")
        self.assertEqual(data.output["unsafe"], "this has 'some' spaces")
        self.assertEqual(data.output["safe"], "this")
        self.assertEqual(data.output["description_text"], "This is test Data object.")
        self.assertEqual(
            data.output["list_description_text"], "This is test Data object."
        )
        self.assertEqual(data.output["input_id"], inputs_input_data.pk)

        storage = Storage.objects.get(pk=data.output["description_full"])
        self.assertEqual(
            storage.json, {"descriptions": {"text": "This is test Data object."}}
        )

        storage = Storage.objects.get(pk=data.output["list_description_full"])
        self.assertEqual(
            storage.json, {"descriptions": {"text": "This is test Data object."}}
        )

        # Return values of jinja filters are casted to strings when inserted to bash
        self.assertEqual(data.output["list_name"], str([input_data.name]))
        self.assertEqual(data.output["list_slug"], str([input_data.slug]))
        self.assertEqual(data.output["list_type"], str([input_process.type]))
        self.assertEqual(data.output["list_subtype"], "[True]")

        self.assertEqual(data.output["all"], "False")
        self.assertEqual(data.output["any"], "True")


class ExpressionEngineTest(TestCase):
    def test_jinja_engine(self):
        engine = manager.get_expression_engine("jinja")
        block = engine.evaluate_block("Hello {{ world }}", {"world": "cruel world"})
        self.assertEqual(block, "Hello cruel world")
        block = engine.evaluate_block(
            "Hello {% if world %}world{% endif %}", {"world": True}
        )
        self.assertEqual(block, "Hello world")

        with self.assertRaises(EvaluationError):
            engine.evaluate_block("Hello {% bar")

        expression = engine.evaluate_inline("world", {"world": "cruel world"})
        self.assertEqual(expression, "cruel world")
        expression = engine.evaluate_inline("world", {"world": True})
        self.assertEqual(expression, True)
        expression = engine.evaluate_inline(
            'world | yesno("yes", "no")', {"world": False}
        )
        self.assertEqual(expression, "no")
        expression = engine.evaluate_inline("[1, 2, 3, world]", {"world": 4})
        self.assertEqual(expression, [1, 2, 3, 4])
        expression = engine.evaluate_inline("a.b.c.d", {})
        self.assertEqual(expression, None)
        expression = engine.evaluate_inline("a.b.c().d", {})
        self.assertEqual(expression, None)
        expression = engine.evaluate_inline(
            "a.b.0.d", {"a": {"b": [{"d": "Hello world"}]}}
        )
        self.assertEqual(expression, "Hello world")
        expression = engine.evaluate_inline("a.b.0.d", {})
        self.assertEqual(expression, None)
        expression = engine.evaluate_inline("[world, false] | all", {"world": True})
        self.assertFalse(expression)
        expression = engine.evaluate_inline("[world, false] | any", {"world": True})
        self.assertTrue(expression)

        # Test that propagation of undefined values works.
        expression = engine.evaluate_inline('foo.bar | name | default("bar")', {})
        self.assertEqual(expression, "bar")
        expression = engine.evaluate_inline('foo.bar | id | default("bar")', {})
        self.assertEqual(expression, "bar")
        expression = engine.evaluate_inline('foo.bar | type | default("bar")', {})
        self.assertEqual(expression, "bar")
        expression = engine.evaluate_inline('foo.bar | basename | default("bar")', {})
        self.assertEqual(expression, "bar")

        # Ensure that filter decorations are correctly copied when decorating filters to
        # automatically propagate undefined values on exceptions.
        expression = engine.evaluate_inline('foo | join(" ")', {"foo": ["a", "b", "c"]})
        self.assertEqual(expression, "a b c")
