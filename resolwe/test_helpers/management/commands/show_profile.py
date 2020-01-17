""".. Ignore pydocstyle D400.

=======================
Interpret Test Profiles
=======================

"""
import json

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Utility for interpreting test profiles."""

    help = "Utility for interpreting test profiles."

    def add_arguments(self, parser):
        """Add command arguments."""
        parser.add_argument("log", nargs="+", type=str, help="profile log filename")
        parser.add_argument(
            "--sort",
            type=str,
            choices=("name", "time"),
            default="name",
            help="sort order (default: name)",
        )

    def handle(self, log, **options):
        """Handle command."""
        results = {}
        for filename in log:
            with open(filename) as log_file:
                for line in log_file:
                    data = json.loads(line)
                    test = results.setdefault(data["test"], {})
                    del data["test"]
                    test.update(data)

        # Generate sort function.
        if options["sort"] == "name":

            def sort_key(item):
                """Sort by block name."""
                key, _ = item
                return key

            sort_reverse = False
        elif options["sort"] == "time":

            def sort_key(item):
                """Sort by block total time."""
                _, blocks = item
                return blocks["total"]

            sort_reverse = True

        totals = {}
        for test, blocks in sorted(results.items(), key=sort_key, reverse=sort_reverse):
            print("{}".format(test))

            total_time = blocks["total"]
            for block, time in blocks.items():
                totals[block] = totals.get(block, 0.0) + time
                print(
                    "  {}: {:.2f} ({:.2f}%)".format(
                        block, time, time * 100.0 / total_time
                    )
                )

            print("")

        print("Overall:")
        total_time = totals["total"]
        for block, time in totals.items():
            print(
                "  {}: {:.2f} ({:.2f}%)".format(block, time, time * 100.0 / total_time)
            )
