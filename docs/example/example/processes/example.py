from resolwe.process import Cmd, DataField, IntegerField, Process, FileField


class UploadDocument(Process):
    """A Document Upload process."""

    slug = "upload-document"
    name = "Upload Document"
    process_type = "data:doc"
    version = "1.0.0"
    requirements = {
        "expression-engine": "jinja",
    }

    class Input:
        """Input fields to process UploadDocument."""
        in_doc = FileField(label="Input document")

    class Output:
        """Output field of the process UploadDocument."""
        out_doc = FileField(label="Output document")

    def run(self, inputs, outputs):
        """Run the analysis."""
        input_doc = inputs.in_doc.import_file()
        outputs.out_doc = input_doc


class WordCount(Process):
    """A Word Count process."""

    slug = "wc"
    name = "Word Count"
    process_type = "data:wc"
    version = "1.0.0"
    requirements = {
        "expression-engine": "jinja",
    }

    class Input:
        """Input fields to process WordCount."""
        document = DataField(data_type="doc", label="Document")

    class Output:
        """Output field of the process WordCount."""
        words = IntegerField(label="Number of words")

    def run(self, inputs, outputs):
        """Run the analysis."""
        document = inputs.document.output.out_doc.path
        num_words = int((Cmd["wc"]["-w"] < document)())
        outputs.words = num_words


class NumberOfLines(Process):
    """A Line Count process."""

    slug = "ln"
    name = "Number of Lines"
    process_type = "data:ln"
    version = "1.0.0"
    requirements = {
        "expression-engine": "jinja",
    }

    class Input:
        """Input fields to process NumberOfLines."""
        document = DataField(data_type="doc", label="Document")

    class Output:
        """Output field of the process NumberOfLines."""
        lines = IntegerField(label="Number of lines")

    def run(self, inputs, outputs):
        """Run the analysis."""
        document = inputs.document.output.out_doc.path
        num_lines = int((Cmd["wc"]["-l"] < document)())
        outputs.lines = num_lines
