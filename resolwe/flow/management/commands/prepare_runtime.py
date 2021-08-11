""".. Ignore pydocstyle D400.

=========================
Prepare runtime directory
=========================

"""
import shutil
from importlib.util import find_spec
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from resolwe.storage import settings as storage_settings


def copy_dir(source: Path, destination: Path):
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


def get_source_path(module_name: str) -> Path:
    """Get the source code for the given module.

    :raises RuntimeError: when no source code can be found.
    """
    spec = find_spec(module_name)
    if spec is None or spec.origin is None:
        raise RuntimeError(f"Source code for module {module_name} could not be found.")
    return Path(spec.origin)


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

    def prepare_runtime(self, runtime_dir):
        """Prepare runtime directory."""
        modules_destination = {
            "resolwe.flow.executors": Path("executors"),
            "resolwe.storage.connectors": Path("executors/connectors"),
        }

        for module_name, relative_destination in modules_destination.items():
            copy_dir(
                get_source_path(module_name).parent,
                runtime_dir / relative_destination,
            )

        # Copy files necessary to bootstrap python process to the executor.
        # These files must be mounted inside container, the rest of the runtime
        # is transfered over the protocol.
        for file in [
            "resolwe.process.communicator",
            "resolwe.process.bootstrap_python_runtime",
        ]:
            shutil.copy(get_source_path(file), runtime_dir)

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

        self.prepare_runtime(self.runtime_dir)
