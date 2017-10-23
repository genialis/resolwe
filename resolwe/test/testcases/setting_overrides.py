"""Overrides for Django settings in tests."""

from __future__ import absolute_import, division, print_function, unicode_literals

import copy

from django.conf import settings


def _get_updated_docker_mappings(flow_executor_settings):
    """Return updated FLOW_DOCKER_MAPPINGS setting.

    Update FLOW_DOCKER_MAPPINGS setting to match the given Flow Executor
    settings.

    :param dict flow_executor_settings: Flow Executor settings

    :return: updated FLOW_DOCKER_MAPPINGS setting
    :rtype: dict

    """
    flow_docker_mappings = copy.deepcopy(getattr(settings, 'FLOW_DOCKER_MAPPINGS', {}))
    for map_ in flow_docker_mappings:
        for map_entry in ['src', 'dest']:
            for setting in ['DATA_DIR', 'UPLOAD_DIR']:
                if settings.FLOW_EXECUTOR[setting] in map_[map_entry]:
                    map_[map_entry] = map_[map_entry].replace(
                        settings.FLOW_EXECUTOR[setting], flow_executor_settings[setting], 1)
    return flow_docker_mappings


# override all FLOW_EXECUTOR settings that are specified in FLOW_EXECUTOR['TEST']
FLOW_EXECUTOR_SETTINGS = copy.deepcopy(getattr(settings, 'FLOW_EXECUTOR', {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, 'FLOW_EXECUTOR', {}).get('TEST', {})
FLOW_EXECUTOR_SETTINGS.update(TEST_SETTINGS_OVERRIDES)

# update FLOW_DOCKER_MAPPINGS setting if necessary
FLOW_DOCKER_MAPPINGS = _get_updated_docker_mappings(FLOW_EXECUTOR_SETTINGS)
