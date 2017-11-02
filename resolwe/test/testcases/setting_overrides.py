"""Overrides for Django settings in tests."""

from __future__ import absolute_import, division, print_function, unicode_literals

import copy

from django.conf import settings

# override all FLOW_EXECUTOR settings that are specified in FLOW_EXECUTOR['TEST']
FLOW_EXECUTOR_SETTINGS = copy.deepcopy(getattr(settings, 'FLOW_EXECUTOR', {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, 'FLOW_EXECUTOR', {}).get('TEST', {})
FLOW_EXECUTOR_SETTINGS.update(TEST_SETTINGS_OVERRIDES)


# update FLOW_DOCKER_MAPPINGS setting if necessary
def _get_updated_docker_mappings(previous_settings=getattr(settings, 'FLOW_EXECUTOR', {})):
    """Reconstruct the docker mappings setting.

    Any pre-existing patched mappings are needed here because the new
    mappings are constructed using a simple search and replace:
    occurences of the old directory settings are replaced with the
    updated ones.

    :param previous_settings: The original flow executor settings before
        update, needed to construct new mappings.
    :return: The updated docker mappings dictionary.
    :rtype: dict
    """
    if 'FLOW_DOCKER_MAPPINGS' in globals():
        previous_mappings = FLOW_DOCKER_MAPPINGS
    else:
        # Read FLOW_DOCKER_MAPPINGS setting from Django settings upon importing
        # the module
        previous_mappings = getattr(settings, 'FLOW_DOCKER_MAPPINGS', {})
    mappings_copy = copy.deepcopy(previous_mappings)
    for map_ in mappings_copy:
        for map_entry in ['src', 'dest']:
            for setting in ['DATA_DIR', 'UPLOAD_DIR']:
                if previous_settings[setting] in map_[map_entry]:
                    map_[map_entry] = map_[map_entry].replace(
                        previous_settings[setting], FLOW_EXECUTOR_SETTINGS[setting], 1)
    return mappings_copy


FLOW_DOCKER_MAPPINGS = _get_updated_docker_mappings()

# override all FLOW_MANAGER settings that are specified in FLOW_MANAGER['TEST']
FLOW_MANAGER_SETTINGS = copy.deepcopy(getattr(settings, 'FLOW_MANAGER', {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, 'FLOW_MANAGER', {}).get('TEST', {})
FLOW_MANAGER_SETTINGS.update(TEST_SETTINGS_OVERRIDES)
