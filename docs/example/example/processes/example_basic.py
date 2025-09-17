from resolwe.process import Cmd, IntegerField, Process, FileField

class WordCount(Process):
    """A Word Count process."""

    slug = "wc-basic"
    name = "Word Count"
    process_type = "data:wc"
    version = "1.0.0"
    requirements = {
        "expression-engine": "jinja",
    }

    class Input:
        """Input fields to process WordCount."""
        document = FileField(label="Document")

    class Output:
        """Output field of the process WordCount."""
        words = IntegerField(label="Number of words")

    def run(self, inputs, outputs):
        """Run the analysis."""
        input_doc = inputs.document.import_file()
        num_words = int((Cmd["wc"]["-w"] < input_doc)())
        outputs.words = num_words
