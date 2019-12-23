"""Describe what tasks this Process accomplishes."""
# Order of imported packages should be:
# Standard libraries
from pathlib import Path
from shutil import copy2

# Third party libraries
from plumbum import TEE

# Resolwe
from resolwe.process import (
    Process,
    SchedulingClass,
    Persistence,
    Cmd,
    StringField,
    TextField,
    BooleanField,
    IntegerField,
    FloatField,
    DateField,
    DateTimeField,
    UrlField,
    SecretField,
    FileField,
    FileHtmlField,
    DirField,
    JsonField,
    ListField,
    DataField,
    GroupField,
)

# In-house libraries (i.e. resolwe_bio)
# Local libraries


class YourProcessName(Process):
    """Process description in one line ends with a period.

    Continue with a more elaborate description of what the process
    does, short layout of the algorithm and what output(s) to expect.

    Note that this is not a working process, but rather just shows
    bits and pieces that can be used in your real process.
    """

    slug = "process-slug-name"
    name = "Human readable name of the process"
    process_type = "data:text_processing:"
    version = "1.0.0"
    # A category of the process, can be used in i.e. frontend app.
    category = "Read Processing"
    # Dynamically generated name of the resulting datum object. Lookup
    # happens from Input class fields.
    data_name = 'Data object ({{ string_field.file|default("?") }})'
    scheduling_class = SchedulingClass.BATCH  # or .INTERACTIVE for short jobs
    # or RAW or TMP, see documentation at
    # https://resolwe.readthedocs.io/en/latest/proc.html#persistence
    persistence = Persistence.CACHED
    # Use below entity to make the created object also an entity of
    # type sample. See
    # https://resolwe.readthedocs.io/en/latest/proc.html#entity for
    # more details.
    # Fields descriptor_schema, input and always_create are optional.
    entity = {
        "type": "sample",  # name of your entity
        # Use a descriptor schema slug if it differs from "type".
        "descriptor_schema": "descriptor-schema-of-entity",
        "input": "name of input that has the entity",
        # If you want to create entity in all cases
        "always_create": True,
    }
    requirements = {
        "expression-engine": "jinja",
        "executor": {
            "docker": {
                # Specify Docker image, may need to include a version.
                "image": "resolwe/base:fedora-31"
            }
        },
        "resources": {
            "cores": 8,
            "memory": 16384,
            # Connects container to a network.
            "network": True,
        },
    }

    class Input:
        """Input fields to process MergeData."""

        string_field = StringField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
            choices=[
                ("computer_readable1", "Human readable 1"),
                ("computer_readable2", "Human readable 2"),
            ],
            default="computer_readable1",
            required=False,
            hidden=False,
            allow_custom_choice=True,
        )
        text_field = TextField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
            default="default text",
            required=False,
            hidden=True,
        )
        boolean_field1 = BooleanField(
            label="Labels are short and do not end in a period",
            description="Note that description fields always end in a period.",
            default=False,
            required=True,
            hidden=False,
        )
        integer_field = IntegerField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
            default=1,
        )
        float_field = FloatField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
            default=3.14,
        )
        date_field = DateField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
            default="2020-04-20",
        )
        datetime_field = DateTimeField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
            default="2020-04-20 12:16:00",
        )
        url_field = UrlField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        secret_field = SecretField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        file_field = FileField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        filehtml_field = FileHtmlField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        dir_field = DirField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        json_field = JsonField(
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        list_field = ListField(
            DataField(data_type="your:data:type"),
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )
        data_field = DataField(
            # data_type should not start with data:
            data_type="your:data:type",
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )

        class Advanced:
            """Add advanced list of options."""

            boolean_field2 = BooleanField(
                label="Labels are short and do not end in a period",
                description="Description ends in a period.",
                default=False,
            )

        group_field = GroupField(
            Advanced,
            label="Labels are short and do not end in a period",
            disabled=False,
            # Will show when boolean_field1 is flipped.
            hidden="!boolean_field1",
            collapsed=True,
        )

    class Output:
        """Output fields to process MergeData"""

        file_out = FileField(label="Labels are short and do not end in a period")
        dir_optional = DirField(
            label="Labels are short and do not end in a period", required=False
        )
        list_out = ListField(
            FileField(),
            label="Labels are short and do not end in a period",
            description="Description ends in a period.",
        )

    def run(self, inputs, outputs):
        """Enter your not so pseudo code below

        Inputs are available through `inputs` variable. I.e., using
        `inputs.list_fields` should yield a list.

        """

        # Below code extracts file name and omits .txt extension.
        # Assuming Input has a variable called file_field, which in
        # turn has field raw_file which holds a path to the file.
        basename = Path(inputs.file_field.raw_file.path).name
        assert basename.endswith(".txt")
        name = basename[:-4]

        # Possible values for various fields.
        inputs.string_field  # string ('computer_readable1')
        inputs.text_field  # string ('default text')
        inputs.boolean_field  # boolean True or False
        inputs.integer_field  # integer scalar (1)
        inputs.float_field  # float scalar (3.14)
        inputs.date_field  # `yyy-mm-dd` (2020-04-20)
        inputs.datetime_field  # `yyy-mm-dd hh:mm:ss` (2020-04-20 12:16:00)
        inputs.secret_field  # TODO: secret model
        inputs.file_field  # string (relative) path to file
        inputs.filehtml_field  # string (relative) path to file
        inputs.dir_field  # string (relative) path to directory
        inputs.json_field  # string (relative) path to file
        inputs.list_field  # list [of, subordinate, fields]
        # Possible self-explanatory properties of DataField.
        inputs.data_field.id
        inputs.data_field.type
        inputs.data_field.descriptor
        inputs.data_field.entity_name
        inputs.url_field  # string or dict {'file': '', refs: ''}
        # You can access fields in GroupField entries from Input.
        inputs.group_field.boolean_field2

        # Run a simple command line tool.
        (Cmd["ls"]["-lha"] > "output_file.txt")()

        # You can use piping as in a terminal.
        (Cmd["ls"] | Cmd["wc"]["-l"])()

        # To run a command line tool and capture its output, use TEE.
        args = ["-l", "-a", "-h"]
        return_code, std_out, std_err = Cmd["ls"][args] & TEE(
            retcode=None,
            buffered=True,
            timeout=None
            # returns None if process exists sucessfully
        )

        # Report errors, warning or info using:
        self.error(f"Oops, an error occurred in {inputs.data_field.id}")
        self.warning(f"Warning in {inputs.data_field.id}")
        self.info(f"Pass info about {self.version}")

        # You can move the progress bar using a float on scale 0.0 to 1.0.
        self.progress(0.5)

        # Specify all fields in Output class to `outputs`.
        outputs.file_out = "./path/to/filename.txt"
        outputs.list_out = ["./path/to/file1.txt", "./path/to/file2.txt"]

        # To import file from the input, you can use import_file() function.
        imported_file = inputs.file_field.import_file()

        # You need to export all fields except those that are explicitly
        # set to required=False.
        if Path("./result_folder").exists:
            outputs.dir_optional = "./result_folder"
