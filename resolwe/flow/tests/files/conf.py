import os

# -- General configuration ------------------------------------------------

# The extension modules to enable.
extensions = ["resolwe.flow.utils.docs.autoprocess"]

# Parent directory of all process definitions:
autoprocess_process_dir = os.path.normpath(
    os.path.abspath(os.path.dirname(__file__)) + "/processes"
)

# Base of the url to process source code:
autoprocess_source_base_url = (
    "https://github.com/genialis/resolwe-bio/blob/master/resolwe_bio/processes/"
)
