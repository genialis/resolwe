import json
from pathlib import Path

from resolwe import process
from resolwe.process import (
    Cmd,
    DataField,
    DirField,
    FileField,
    FloatField,
    GroupField,
    IntegerField,
    JsonField,
    ListField,
    Persistence,
    Process,
    SchedulingClass,
    StringField,
    UrlField,
)
from resolwe.process.models import Collection, Data
from resolwe.process.models import Process as ProcessM


class EntityProcess(Process):
    slug = "entity-process"
    name = "Entity process"
    data_name = "Data with entity"
    version = "1.0.0"
    process_type = "data:entity"
    entity = {
        "type": "sample",
    }

    class Output:
        list_string = ListField(StringField(), label="My list")
        optional = StringField("Optional output", required=False)

    def run(self, inputs, outputs):
        outputs.list_string = ["foo", "bar"]


class PythonProcess(Process):
    """This is a process description."""

    slug = "test-python-process"
    name = "Test Python Process"
    version = "0.1.2"
    process_type = "data:python"
    category = "analyses"
    scheduling_class = SchedulingClass.BATCH
    persistence = Persistence.CACHED
    data_name = "Foo: {{input_data | name}}"
    entity = {
        "type": "sample",
        "descriptor_schema": "sample",
        "input": "input_data",
    }
    requirements = {
        "expression-engine": "jinja",
        "executor": {
            "docker": {
                "image": "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-20.04",
            }
        },
    }

    class Input:
        """Input fields."""

        my_field = StringField(label="My field")
        my_list = ListField(StringField(), label="My list")
        input_data = DataField("test:save", label="My input data")
        input_entity_data = DataField("entity", label="My entity data")
        bar = DataField(data_type="test:save", label="My bar", relation_type="group")
        baz = DataField(
            data_type="test:save",
            required=False,
            label="My bar",
            relation_type="group",
            relation_npartitions=1,
        )
        baz_list = ListField(
            DataField(data_type="test:save"),
            required=False,
            label="My baz list",
            relation_type="group",
            relation_npartitions=1,
        )
        url = UrlField(UrlField.DOWNLOAD, label="My URL")
        integer = IntegerField(label="My integer")
        my_float = FloatField(label="My float")
        my_json = JsonField(label="Blah blah")
        my_optional = StringField(
            label="Optional", required=False, default="default value"
        )
        my_optional_no_default = StringField(
            label="Optional no default", required=False
        )

        class MyGroup:
            foo = IntegerField(label="Foo")
            bar = StringField(label="Bar")
            group_optional_no_default = StringField(
                label="Group optional no default", required=False
            )

        my_group = GroupField(MyGroup, label="My group")

    class Output:
        string_output = StringField(label="My string output")
        list_string_output = ListField(StringField(), label="My list string output")
        file_output = FileField(label="My output")
        list_file_output = ListField(FileField(), label="My list output")
        dir_output = DirField(label="My output")
        input_data_name = StringField(label="Input data name")
        input_entity_name = StringField(label="Input entity name")
        docker_image = StringField(label="Docker image")

    def run(self, inputs, outputs):
        print("All inputs are:", inputs)
        print("Input data:", inputs.input_data)
        print("Input data ID:", inputs.input_data.id)
        print("Input data file output:", inputs.input_data.output.saved_file.path)
        print("Input data type:", inputs.input_data.type)
        print("Input data descriptor:", inputs.input_data.descriptor)
        print("Group bar:", inputs.my_group.bar)
        print("Group foo:", inputs.my_group.foo)
        print("Entity name of the input:", inputs.input_entity_data.entity.name)
        print("Docker image:", self.requirements.executor.docker.image)

        if inputs.my_optional:
            print("My optional:", inputs.my_optional)

        if inputs.my_optional_no_default:
            raise AttributeError("inputs.my_optional_no_default should not exist.")

        if inputs.my_group.group_optional_no_default:
            raise AttributeError(
                "inputs.my_group.group_optional_no_default should not exist."
            )

        if inputs.input_entity_data.output.optional:
            raise AttributeError("inputs.list_string_output.optional should not exist.")

        try:
            inputs.invalid_input
        except AttributeError as err:
            if "Inputs have no field invalid_input" in str(err):
                pass

        try:
            inputs.input_entity_data.output.invalid_field
        except AttributeError as err:
            if "DataField has no member invalid_field" in str(err):
                pass

        bar = Cmd["ls"]["-l", "-a", "/"] | Cmd["grep"]["python"]
        print("hello world:\n", bar())

        Cmd["mkdir"]["test"]()
        (Cmd["echo"]['"Some content"'] > "test/testfile.txt")()
        (Cmd["echo"]['"Some more content"'] > "testfile2.txt")()

        outputs.file_output = "test/testfile.txt"
        outputs.list_file_output = ["test/testfile.txt", "testfile2.txt"]
        outputs.dir_output = "test/"
        outputs.input_data_name = inputs.input_data.name
        outputs.input_entity_name = inputs.input_entity_data.entity.name
        outputs.docker_image = self.requirements.executor.docker.image
        outputs.string_output = "OK"
        outputs.list_string_output = ["foo", "bar"]


class PythonProcessGroup(Process):
    """Process with Group fields."""

    slug = "test-python-process-group-field"
    name = "Test Python Process for GroupField"
    version = "0.1.2"
    process_type = "data:python:group"

    class Input:
        """Input fields."""

        class MyGroup:
            foo = IntegerField(label="Foo", required=False, default=42)
            bar = StringField(label="Bar", required=False)

        class MyGroup2:
            foo = IntegerField(label="Foo", required=False)

        my_group = GroupField(MyGroup, label="My group")
        my_group2 = GroupField(
            MyGroup2, label="My group2 that has all elements without defaults."
        )

    class Output:
        """Output fields."""

        out_foo = IntegerField(label="Foo.", required=False)
        out_bar = StringField(label="Bar.", required=False)
        out_foo2 = IntegerField(label="Foo2.", required=False)

    def run(self, inputs, outputs):
        if inputs.my_group.foo:
            outputs.out_foo = inputs.my_group.foo
        if inputs.my_group.bar:
            outputs.out_bar = inputs.my_group.bar
        if inputs.my_group2.foo:
            outputs.out_foo2 = inputs.my_group2.foo


class PythonProcess2(process.Process):
    """Inherit from 'module.Class'."""

    slug = "test-python-process-2"
    name = "Test Python Process 2"
    version = "0.0.1"
    process_type = "data:python"

    def run(self, inputs, outputs):
        pass


class ErrorProcess(Process):
    slug = "test-python-process-error"
    name = "Test Python Process Error"
    version = "0.0.1"
    process_type = "data:python:error"

    def run(self, inputs, outputs):
        raise ValueError("Value error in ErrorProcess")


class AnnotateProcess(Process):
    slug = "test-python-process-annotate-entity"
    name = "Test Python Process Annotate Entity"
    version = "0.0.1"
    process_type = "data:python:annotate"
    entity = {
        "type": "sample",
    }

    def run(self, inputs, outputs):
        annotations = {"general.description": "desc", "general.species": "Valid"}
        self.update_entity_descriptor(annotations)


class FileProcess(Process):
    slug = "test-python-process-file"
    name = "Test Python Process File"
    version = "0.0.1"
    process_type = "data:python:file"

    class Input:
        """Input fields."""

        src = FileField(label="Input file")

    class Output:
        """Input fields."""

        dst = FileField(label="Output file")

    def run(self, inputs, outputs):
        file_name = inputs.src.import_file()
        outputs.dst = file_name


class RequirementsProcess(Process):
    slug = "test-python-process-requirements"
    name = "Test Python Process Requirements"
    version = "0.0.1"
    process_type = "data:python:requirements"
    requirements = {
        "resources": {
            "cores": 2,
            "memory": 4096,
        },
    }

    class Output:
        """Input fields."""

        cores = IntegerField(label="Cores")
        memory = IntegerField(label="Memory")

    def run(self, inputs, outputs):
        outputs.cores = self.requirements["resources"]["cores"]
        outputs.memory = self.requirements["resources"]["memory"]

        print("Cores:", outputs.cores)
        print("Memory:", outputs.memory)


class ProcessWithWorkflowInput(Process):
    slug = "process-with-workflow-input"
    name = "Process with workflow input"
    version = "1.0.0"
    process_type = "data:test"

    class Input:
        """Input fields."""

        data = DataField(data_type="", label="Data.")

    def run(self, inputs, outputs):
        pass


class ProcessWithChoicesInput(Process):
    slug = "process-with-choices-input"
    name = "Process with choices input"
    version = "1.0.0"
    process_type = "data:test"

    class Input:
        """Input fields."""

        string_input = StringField(
            label="Input field",
            allow_custom_choice=True,
            choices=[
                ("foo", "foo"),
                ("bar", "bar"),
            ],
        )

    class Output:
        """Output fields."""

        string_output = StringField(label="Output field")

    def run(self, inputs, outputs):
        outputs.string_output = inputs.string_input


class RelationsProcess(Process):
    slug = "test-process-relations"
    name = "Test Python Process relations"
    version = "0.0.1"
    process_type = "data:python:relations"
    requirements = {
        "relations": [{"type": "series"}],
    }

    class Input:
        """Input fields."""

        data = ListField(DataField(data_type=""), label="Data.")

    class Output:
        """Output fields."""

        relation_id = IntegerField(label="Relation id")
        relation_type = StringField(label="Relation type")
        relation_ordered = StringField(label="Relation ordering")
        relation_category = StringField(label="Relation category")
        relation_unit = StringField(label="Relation unit")
        relation_partition_label = StringField(label="Relation partition label")
        relation_partition_position = IntegerField(label="Relation partition label")

    def run(self, inputs, outputs):
        # Access relation attributes
        outputs.relation_id = inputs.data[0].relations[0].id
        outputs.relation_type = inputs.data[0].relations[0].type
        outputs.relation_ordered = str(inputs.data[0].relations[0].ordered)
        outputs.relation_category = inputs.data[0].relations[0].category
        outputs.relation_unit = inputs.data[0].relations[0].unit
        # Access relation partition attributes
        outputs.relation_partition_label = (
            inputs.data[0].relations[0].partitions[0].label
        )
        outputs.relation_partition_position = (
            inputs.data[0].relations[0].partitions[0].position
        )


class DataNameProcess(Process):
    slug = "data-name-process"
    name = "Data name process"
    data_name = "{{ data_input | name | default('?') }}"
    version = "1.0.0"
    process_type = "data:name"
    requirements = {"expression-engine": "jinja"}

    class Input:
        data_input = DataField(data_type="", label="Data input")

    class Output:
        name = StringField(label="Name")

    def run(self, inputs, outputs):
        outputs.name = self.name


class CreateCollection(Process):
    slug = "create-collection"
    name = "Create collection"
    data_name = "{{ data_input | name | default('?') }}"
    version = "1.0.0"
    process_type = "data:name"
    requirements = {"expression-engine": "jinja"}

    class Input:
        collection_name = StringField(label="Collection name")

    def run(self, inputs, outputs):
        Collection.create(name=inputs.collection_name)


class GetCollection(Process):
    slug = "get-collection"
    name = "Get collection"
    data_name = "{{ data_input | name | default('?') }}"
    version = "1.0.0"
    process_type = "data:name"
    requirements = {"expression-engine": "jinja"}

    class Input:
        collection_name = StringField(label="Collection name")

    class Output:
        collection_slug = StringField(label="Collection slug")

    def run(self, inputs, outputs):
        collection = Collection.get(name=inputs.collection_name)
        outputs.collection_slug = collection.slug


class FilterCollection(Process):
    slug = "filter-collection"
    name = "Filter collection"
    data_name = "{{ data_input | name | default('?') }}"
    version = "1.0.0"
    process_type = "data:name"
    requirements = {"expression-engine": "jinja"}

    class Input:
        collection_name = StringField(label="Collection name")

    class Output:
        number_of_collections = IntegerField(label="Number of collections")

    def run(self, inputs, outputs):
        collections = Collection.filter(name=inputs.collection_name)
        outputs.number_of_collections = len(collections)


class CreateData(Process):
    slug = "create-data"
    name = "Create data object"
    data_name = "{{ data_input | name | default('?') }}"
    version = "1.0.0"
    process_type = "data:name"
    requirements = {"expression-engine": "jinja"}

    class Input:
        data_name = StringField(label="Data name")
        collection_name = StringField(label="Collection name")

    def run(self, inputs, outputs):
        process = ProcessM.get(slug="create-collection")
        Data.create(
            process=process,
            name=inputs.data_name,
            input={"collection_name": inputs.collection_name},
        )


class TestStorage(Process):
    slug = "storage-objects-test"
    name = "Test working with storage objects"
    data_name = "{{ data_input | name | default('?') }}"
    version = "1.0.0"
    process_type = "data:name"
    requirements = {"expression-engine": "jinja"}

    class Output:
        output_string = JsonField(label="Output string")
        output_file = JsonField(label="Output file")

    def run(self, inputs, outputs):
        # Test update of storage objects.
        outputs.output_string = ["first", "valid", "json"]
        outputs.output_string = ["valid", "json"]
        # Test storage objects are read from file (it it exists).
        json_filename = "json_file.out"
        Path(json_filename).write_text(json.dumps(["valid", "json", "file"]))
        outputs.output_file = json_filename
