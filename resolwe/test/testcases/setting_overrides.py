"""Overrides for Django settings in tests."""

import copy

from django.conf import settings

from resolwe.storage import settings as storage_settings


def prepare_executor_settings():
    """Override FLOW_EXECUTOR settings with settings specified in FLOW_EXECUTOR['TEST']."""
    global FLOW_EXECUTOR_SETTINGS
    FLOW_EXECUTOR_SETTINGS = copy.deepcopy(getattr(settings, "FLOW_EXECUTOR", {}))
    TEST_SETTINGS_OVERRIDES = getattr(settings, "FLOW_EXECUTOR", {}).get("TEST", {})
    FLOW_EXECUTOR_SETTINGS.update(TEST_SETTINGS_OVERRIDES)


def prepare_manager_settings():
    """Override FLOW_MANAGER settings with settings specified in FLOW_MANAGER['TEST']."""
    global FLOW_MANAGER_SETTINGS
    FLOW_MANAGER_SETTINGS = copy.deepcopy(getattr(settings, "FLOW_MANAGER", {}))
    TEST_SETTINGS_OVERRIDES = getattr(settings, "FLOW_MANAGER", {}).get("TEST", {})
    FLOW_MANAGER_SETTINGS.update(TEST_SETTINGS_OVERRIDES)


def prepare_storage_settings():
    """Prepare the storage connectors and volumes settings."""
    global STORAGE_CONNECTORS, FLOW_VOLUMES
    STORAGE_CONNECTORS = copy.deepcopy(storage_settings.STORAGE_CONNECTORS)
    for connector_settings in STORAGE_CONNECTORS.values():
        connector_settings["config"].update(connector_settings.get("test", {}))

    FLOW_VOLUMES = copy.deepcopy(storage_settings.FLOW_VOLUMES)
    for volume in FLOW_VOLUMES.values():
        volume["config"].update(volume.get("test", {}))


# Prepare the settings overrides.
prepare_executor_settings()
prepare_manager_settings()
prepare_storage_settings()
