"""Upload file process."""
from resolwe.process import FileField, Process, SchedulingClass


class UploadFile(Process):
    """Upload file process."""

    slug = "upload-file"
    name = "File"
    process_type = "data:file"
    data_name = '{{ src.file|default("?") }}'
    version = "1.1.0"
    category = "Import"
    description = "Import any other file format such as a pdf."
    requirements = {
        "expression-engine": "jinja",
        "executor": {
            "docker": {
                "image": "resolwebio/base:ubuntu-20.04",
            },
        },
        "resources": {
            "cores": 1,
            "memory": 1024,
            "network": True,
        },
    }
    scheduling_class = SchedulingClass.BATCH

    class Input:
        """Input parameters."""

        src = FileField(label="Input file")

    class Output:
        """Output parameters."""

        file = FileField(label="File")

    def run(self, inputs, outputs):
        """Upload file."""
        input_file = inputs.src.import_file(imported_format="extracted")
        outputs.file = input_file
