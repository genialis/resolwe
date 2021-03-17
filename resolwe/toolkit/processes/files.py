"""Upload file process."""
from pathlib import Path

from resolwe.process import FileField, Process, SchedulingClass


class UploadFile(Process):
    """Upload file process."""

    slug = "upload-file"
    name = "File"
    process_type = "data:file"
    data_name = '{{ src.file|default("?") }}'
    version = "1.1.2"
    category = "Import"
    description = "Import any other file format such as a pdf."
    requirements = {
        "expression-engine": "jinja",
        "executor": {
            "docker": {
                "image": "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-20.04-03022021",
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
        supported_suffixes = [".gz", ".bz2", ".zip", ".rar", ".7z"]
        input_file = Path(inputs.src.import_file())

        if Path(f"{input_file}.tar.gz").is_file():
            outputs.file = f"{input_file}.tar.gz"
        elif Path(inputs.src.path).suffix in supported_suffixes:
            outputs.file = f"{input_file}.gz"
        else:
            outputs.file = f"{input_file}"
