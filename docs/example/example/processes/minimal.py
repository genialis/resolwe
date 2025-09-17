from resolwe.process import Process

class Minimal(Process):
    """A minimal example process."""

    slug = "mini"
    name = "Minimalistic Process"
    process_type = "data:mini"
    version = "1.0.0"
    requirements = {
        "expression-engine": "jinja",
    }

    class Input:
        """Input fields to process Minimal."""
        pass

    class Output:
        """Output field of the process Minimal."""
        pass

    def run(self, inputs, outputs):
        """Run the analysis."""
        print("Hello bioinformatician!")
