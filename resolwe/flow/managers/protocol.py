"""Protocol constants used by the manager and executors."""


class WorkerProtocol:
    """Constants used by the manager workers."""

    COMMAND = "command"
    DATA_ID = "data_id"

    COMMUNICATE = "communicate"
    COMMUNICATE_EXTRA = "kwargs"

    FINISH = "finish_data"
    FINISH_COMMUNICATE_EXTRA = "communicate_kwargs"

    ABORT = "abort_data"


class ExecutorProtocol:
    """Constants used by the executor<->listener protocol."""

    DOWNLOAD_STARTED = "download_started"
    DOWNLOAD_FINISHED = "download_finished"
    DOWNLOAD_ABORTED = "download_aborted"
    DOWNLOAD_IN_PROGRESS = "download_in_progress"
    DOWNLOAD_STARTED_LOCK = "download_started_lock"
    STORAGE_LOCATION_ID = "storage_location_id"
    STORAGE_ACCESS_LOG_ID = "storage_access_log_id"

    MISSING_DATA_LOCATIONS = "missing_data_locations"
    STORAGE_DATA_LOCATIONS = "storage_data_locations"

    GET_FILES_TO_DOWNLOAD = "get_files_to_download"
    GET_REFERENCED_FILES = "get_referenced_files"
    REFERENCED_FILES = "referenced_files"

    RESULT = "result"
    RESULT_ERROR = "ER"


class ExecutorFiles:
    """Various files used by the executor."""

    # Base runtime subdirectory where settings will be stored.
    SETTINGS_SUBDIR = "settings"
    # Base runtime subdirectory where socket files will be.
    SOCKETS_SUBDIR = "sockets"

    FILE_LIST_KEY = "serialized_files"

    EXECUTOR_SETTINGS = "settings.json"
    DJANGO_SETTINGS = "django_settings.json"
    DATA = "data.json"
    LOCATION_SUBPATH = "storage_location.json"
    DATA_META = "data_meta.json"
    PROCESS = "process.json"
    PROCESS_META = "process_meta.json"

    PROCESS_SCRIPT = "process_script.sh"

    SECRETS_DIR = "secrets"

    STARTUP_PROCESSING_SCRIPT = "startup_processing_container.py"
    STARTUP_COMMUNICATION_SCRIPT = "startup_communication_container.py"
    SOCKET_UTILS = "socket_utils.py"
    CONSTANTS = "constants.py"
