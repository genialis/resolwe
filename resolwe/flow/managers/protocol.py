"""Protocol constants used by the manager and executors."""


class WorkerProtocol:
    """Constants used by the manager workers."""

    COMMAND = 'command'
    DATA_ID = 'data_id'

    COMMUNICATE = 'communicate'
    COMMUNICATE_SETTINGS = 'settings_override'
    COMMUNICATE_EXTRA = 'kwargs'

    FINISH = 'finish_data'
    FINISH_SPAWNED = 'spawned'
    FINISH_COMMUNICATE_EXTRA = 'communicate_kwargs'

    ABORT = 'abort_data'


class ExecutorProtocol:
    """Constants used by the executor<->listener protocol."""

    COMMAND = 'command'
    DATA_ID = 'data_id'

    UPDATE = 'update'
    UPDATE_CHANGESET = 'changeset'

    FINISH = 'finish'
    FINISH_PROCESS_RC = 'process_rc'
    FINISH_SPAWN_PROCESSES = 'spawn_processes'
    FINISH_EXPORTED_FILES = 'exported_files_mapper'

    ABORT = 'abort'

    RESULT = 'result'
    RESULT_OK = 'OK'
    RESULT_ERROR = 'ER'

    LOG = 'log'
    LOG_MESSAGE = 'message'


class ExecutorFiles:
    """Various files used by the executor."""

    FILE_LIST_KEY = 'serialized_files'

    EXECUTOR_SETTINGS = 'settings.json'
    DJANGO_SETTINGS = 'django_settings.json'
    DATA = 'data.json'
    DATA_META = 'data_meta.json'
    PROCESS = 'process.json'
    PROCESS_META = 'process_meta.json'

    PROCESS_SCRIPT = 'process_script.sh'

    SECRETS_DIR = 'secrets'
