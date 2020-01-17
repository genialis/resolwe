"""Overrides for Django settings in tests."""

import copy

from django.conf import settings

# override all FLOW_EXECUTOR settings that are specified in FLOW_EXECUTOR['TEST']
FLOW_EXECUTOR_SETTINGS = copy.deepcopy(getattr(settings, "FLOW_EXECUTOR", {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, "FLOW_EXECUTOR", {}).get("TEST", {})
FLOW_EXECUTOR_SETTINGS.update(TEST_SETTINGS_OVERRIDES)

# override all FLOW_MANAGER settings that are specified in FLOW_MANAGER['TEST']
FLOW_MANAGER_SETTINGS = copy.deepcopy(getattr(settings, "FLOW_MANAGER", {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, "FLOW_MANAGER", {}).get("TEST", {})
FLOW_MANAGER_SETTINGS.update(TEST_SETTINGS_OVERRIDES)
