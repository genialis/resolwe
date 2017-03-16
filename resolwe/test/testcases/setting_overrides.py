"""Overrides for Django settings in tests."""

from __future__ import absolute_import, division, print_function, unicode_literals

import copy

from django.conf import settings

# override all FLOW_EXECUTOR settings that are specified in FLOW_EXECUTOR['TEST']
FLOW_EXECUTOR_SETTINGS = copy.copy(getattr(settings, 'FLOW_EXECUTOR', {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, 'FLOW_EXECUTOR', {}).get('TEST', {})
FLOW_EXECUTOR_SETTINGS.update(TEST_SETTINGS_OVERRIDES)

# update FLOW_DOCKER_MAPPINGS setting if necessary
FLOW_DOCKER_MAPPINGS = copy.copy(getattr(settings, 'FLOW_DOCKER_MAPPINGS', {}))
for map_ in FLOW_DOCKER_MAPPINGS:
    for map_entry in ['src', 'dest']:
        for setting in ['DATA_DIR', 'UPLOAD_DIR']:
            if settings.FLOW_EXECUTOR[setting] in map_[map_entry]:
                map_[map_entry] = map_[map_entry].replace(
                    settings.FLOW_EXECUTOR[setting], FLOW_EXECUTOR_SETTINGS[setting], 1)
