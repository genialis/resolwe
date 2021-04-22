""".. Ignore pydocstyle D400.

=========================
Prepare runtime directory
=========================

"""
import inspect
import shutil
from importlib import import_module
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from resolwe.storage import settings as storage_settings


class Command(BaseCommand):
    """Prepare runtime directory."""

    help = "Prepare runtime directory."

    def __init__(self, *args, **kwargs):
        """Initialize command."""
        super().__init__(*args, **kwargs)

        if not "runtime" in storage_settings.FLOW_VOLUMES:
            raise CommandError("Volume 'runtime' must be defined in storage settings.")

        self.runtime_dir = Path(
            storage_settings.FLOW_VOLUMES["runtime"]["config"]["path"]
        )

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument(
            "-c",
            "--clear",
            action="store_true",
            dest="clear",
            help="Clear existing files before copying new files.",
        )

        parser.add_argument(
            "--noinput",
            "--no-input",
            action="store_false",
            dest="interactive",
            help="Do NOT prompt the user for input of any kind.",
        )

    def get_confirmation(self):
        """Get user confirmation to proceed."""
        if self.clear:
            action = "This will DELETE ALL FILES in this location!"
        else:
            action = "This will overwrite existing files!"

        message = (
            "\n"
            "You have requested to prepare runtime directory at the destination\n"
            "location as specified in your settings\n"
            "\n"
            "    {destination}\n"
            "\n"
            "{action}\n"
            "Are you sure you want to do this?\n"
            "\n"
            "Type 'yes' to continue, or 'no' to cancel: ".format(
                destination=self.runtime_dir,
                action=action,
            )
        )

        if input("".join(message)) != "yes":
            raise CommandError("Preparing runtime directory cancelled.")

    def clear_runtime(self):
        """Delete contents of the runtime directory."""
        self.stdout.write("Deleting contents of '{}'.".format(self.runtime_dir))
        for entry in self.runtime_dir.glob("*"):
            if entry.is_file() or entry.is_symlink():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry)

    def copy_dir(self, source: Path, destination: Path):
        """Copy dir from source to destination.

        The shutil copytree method could be used but it will not overwrite
        directories (option added in Python 3.8, while we support Python 3.6).

        This method iterates through all entries in source directory and copies
        them over to the destination directory (overwriting them if they
        already exist).
        """
        for source_entry in source.rglob("*"):
            relative_source_path = source_entry.relative_to(source)
            destination_entry = destination / relative_source_path
            if source_entry.is_dir():
                destination_entry.mkdir(exist_ok=True, parents=True)
            elif source_entry.is_file() or source_entry.is_symlink():
                destination_entry.parent.mkdir(exist_ok=True, parents=True)
                shutil.copy(source_entry, destination_entry)

    def prepare_runtime(self):
        """Prepare runtime directory."""
        import resolwe.flow.executors as executor_package
        import resolwe.storage.connectors as connectors_package

        source_file = inspect.getsourcefile(executor_package)
        assert (
            source_file is not None
        ), "Unable to determine the 'resolwe.flow.executors' source directory."
        exec_dir = Path(source_file).parent
        dest_dir = self.runtime_dir / "executors"
        self.copy_dir(exec_dir, dest_dir)

        source_file = inspect.getsourcefile(connectors_package)
        assert (
            source_file is not None
        ), "Unable to determine the 'resolwe.storage.connectors' source directory."
        exec_dir = Path(source_file).parent
        dest_dir = dest_dir / "connectors"
        self.copy_dir(exec_dir, dest_dir)

        for runtime_class_name in settings.FLOW_PROCESSES_RUNTIMES:
            module_name, class_name = runtime_class_name.rsplit(".", 1)
            runtime_module = import_module(module_name)
            source_file = Path(inspect.getsourcefile(runtime_module))
            source_dir = source_file.parent

            dest_dir = Path(storage_settings.FLOW_VOLUMES["runtime"]["config"]["path"])
            dest_dir = dest_dir.joinpath("python_runtime", *module_name.split(".")[:-1])
            self.copy_dir(source_dir, dest_dir)

    def set_options(self, **options):
        """Set instance variables based on an options dict."""
        self.clear = options["clear"]
        self.interactive = options["interactive"]

    def handle(self, **options):
        """Collect tools."""
        self.set_options(**options)

        previous_data = any(self.runtime_dir.glob("*"))
        if self.interactive and previous_data:
            self.get_confirmation()

        if self.clear and previous_data:
            self.clear_runtime()

        self.prepare_runtime()
