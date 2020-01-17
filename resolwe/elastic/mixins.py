""".. Ignore pydocstyle D400.

===============
Elastic Indices
===============
"""
from resolwe.elastic.builder import index_builder


class ElasticIndexFilterMixin:
    """Mixin for management commands with index filtering."""

    def add_arguments(self, parser):
        """Command arguments."""
        super().add_arguments(parser)

        parser.add_argument(
            "-i",
            "--index",
            action="append",
            default=[],
            help="Only process specific indices (default: not set)",
        )
        parser.add_argument(
            "-e",
            "--exclude",
            action="append",
            default=[],
            help="Do not process specific indices (default: not set)",
        )

    def invalid_index(self, name):
        """Show an invalid index error message."""
        self.stderr.write("Unknown index: {}".format(name))
        self.stderr.write("Supported indices are:")
        for index in index_builder.indexes:
            self.stderr.write("  * {}".format(index.__class__.__name__))

    def has_filter(self, options):
        """Return true if any filters have been specified."""
        return options["index"] or options["exclude"]

    def handle_index(self, index):
        """Process index."""

    def filter_indices(self, options, verbosity, *args, **kwargs):
        """Filter indices and execute an action for each index."""
        index_name_map = {
            index.__class__.__name__: index for index in index_builder.indexes
        }

        # Process includes.
        if options["index"]:
            indices = set(options["index"])
        else:
            indices = set(index_name_map.keys())

        # Process excludes.
        for index_name in options["exclude"]:
            if index_name not in index_name_map:
                self.invalid_index(index_name)
                return

            indices.discard(index_name)

        # Execute action for each remaining index.
        for index_name in indices:
            try:
                index = index_name_map[index_name]
            except KeyError:
                self.invalid_index(index_name)
                return

            if verbosity > 0:
                self.stdout.write("Processing index '{}'...".format(index_name))
            self.handle_index(index, *args, **kwargs)
