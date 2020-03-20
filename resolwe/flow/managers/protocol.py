"""Protocol constants used by the manager and executors."""


class WorkerProtocol:
    """Constants used by the manager workers."""

    COMMAND = "command"
    DATA_ID = "data_id"

    COMMUNICATE = "communicate"
    COMMUNICATE_SETTINGS = "settings_override"
    COMMUNICATE_EXTRA = "kwargs"

    FINISH = "finish_data"
    FINISH_SPAWNED = "spawned"
    FINISH_COMMUNICATE_EXTRA = "communicate_kwargs"

    ABORT = "abort_data"


class ExecutorProtocol:
    """Constants used by the executor<->listener protocol."""

    COMMAND = "command"
    DATA_ID = "data_id"

    UPDATE = "update"
    UPDATE_CHANGESET = "changeset"

    ANNOTATE = "annotate"
    ANNOTATIONS = "annotations"

    DOWNLOAD_RESULT = "download_result"
    DOWNLOAD_STARTED = "download_started"
    DOWNLOAD_FINISHED = "download_finished"
    DOWNLOAD_ABORTED = "download_aborted"
    DOWNLOAD_IN_PROGRESS = "download_in_progress"
    DOWNLOAD_STARTED_LOCK = "download_started_lock"
    STORAGE_LOCATION_ID = "storage_location_id"
    STORAGE_ACCESS_LOG_ID = "storage_access_log_id"
    STORAGE_LOCATION_LOCK = "storage_location_lock"
    STORAGE_LOCATION_UNLOCK = "storage_location_unlock"
    STORAGE_LOCATION_LOCK_REASON = "storage_location_lock_reason"

    MISSING_DATA_LOCATIONS = "missing_data_locations"
    STORAGE_DATA_LOCATIONS = "storage_data_locations"

    GET_FILES_TO_DOWNLOAD = "get_files_to_download"
    GET_REFERENCED_FILES = "get_referenced_files"
    REFERENCED_FILES = "referenced_files"

    FINISH = "finish"
    FINISH_PROCESS_RC = "process_rc"
    FINISH_SPAWN_PROCESSES = "spawn_processes"
    FINISH_EXPORTED_FILES = "exported_files_mapper"

    ABORT = "abort"

    RESULT = "result"
    RESULT_OK = "OK"
    RESULT_ERROR = "ER"

    LOG = "log"
    LOG_MESSAGE = "message"


class ExecutorFiles:
    """Various files used by the executor."""

    FILE_LIST_KEY = "serialized_files"

    EXECUTOR_SETTINGS = "settings.json"
    DJANGO_SETTINGS = "django_settings.json"
    DATA = "data.json"
    STORAGE_LOCATION = "storage_location.json"
    DATA_META = "data_meta.json"
    PROCESS = "process.json"
    PROCESS_META = "process_meta.json"

    PROCESS_SCRIPT = "process_script.sh"

    SECRETS_DIR = "secrets"
