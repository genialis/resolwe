""".. Ignore pydocstyle D400.

=====
State
=====

Synchronized singleton state container for the manager.

.. autofunction:: resolwe.flow.managers.state.update_constants
.. autoclass:: resolwe.flow.managers.state.ManagerState

"""

# This module should not import anything local, or there will be circular
# dependencies, since the constants are needed in various sub-modules inside
# resolwe.flow.managers.

import json
from collections import namedtuple

import redis

from django.conf import settings

ManagerChannelPair = namedtuple("ManagerChannelPair", ["queue", "queue_response"])

MANAGER_CONTROL_CHANNEL = "DUMMY.control"
MANAGER_EXECUTOR_CHANNELS = ManagerChannelPair("DUMMY.queue", "DUMMY.queue_response")
MANAGER_STATE_PREFIX = "DUMMY.state_prefix"
MANAGER_LISTENER_STATS = "DUMMY.listener_stats"


def update_constants():
    """Recreate channel name constants with changed settings.

    This kludge is mostly needed due to the way Django settings are
    patched for testing and how modules need to be imported throughout
    the project. On import time, settings are not patched yet, but some
    of the code needs static values immediately. Updating functions such
    as this one are then needed to fix dummy values.
    """
    global MANAGER_CONTROL_CHANNEL, MANAGER_EXECUTOR_CHANNELS
    global MANAGER_LISTENER_STATS, MANAGER_STATE_PREFIX
    redis_prefix = getattr(settings, "FLOW_MANAGER", {}).get("REDIS_PREFIX", "")

    MANAGER_CONTROL_CHANNEL = "{}.control".format(redis_prefix)
    MANAGER_EXECUTOR_CHANNELS = ManagerChannelPair(
        "{}.result_queue".format(redis_prefix),
        "{}.result_queue_response".format(redis_prefix),
    )
    MANAGER_STATE_PREFIX = "{}.state".format(redis_prefix)
    MANAGER_LISTENER_STATS = "{}.listener_stats".format(redis_prefix)


update_constants()


class ManagerState:
    """State interface implementation.

    This holds variables required to be shared between all manager
    workers and takes care of operation atomiticy and synchronization.
    Redis facilitates storage shared between workers, whereas
    atomicity needs to be dealt with explicitly; this interface hides
    the Redis and Python details required to achieve
    syntax-transparent atomicity (such as being able to do
    ``executor_count += 1``, a load-modify-store operation sequence).
    """

    class LuaFunction:
        """Wrapper class for Lua function strings."""

        def __init__(self, owner, script, short=False):
            """Construct a Lua function instance and register it.

            :param owner: The owning :class:`RedisAtomicBase` instance.
            :param script: The Lua script text to register.
            :param short: If ``True``, the script as given is just an
                expression that still needs to be wrapped into a full
                script.
            """
            if short:
                self.script = (
                    """
                    local oldval = tonumber(redis.call('EXISTS', KEYS[1]) and redis.call('GET', KEYS[1]) or ARGV[2])
                    local arg = tonumber(ARGV[1])
                    local newval = """
                    + script
                    + """
                    redis.call('SET', KEYS[1], newval)
                    return newval
                """
                )
            else:
                self.script = script

            self.owner = owner
            self.function = owner.redis.register_script(self.script)

        def __call__(self, *args):
            """Call the underlying Lua function transparently."""
            return self.function(keys=[self.owner.item_name], args=args)

    class RedisAtomicBase:
        """Base class for atomic data handling."""

        def __init__(self, conn, key_prefix, item_name, *args, **kwargs):
            """Initialize the base instance, register Lua scripts.

            :param conn: The Redis interface object.
            :param key_prefix: The key prefix used for variable names.
            :param item_name: The name of this variable.
            """
            self.redis = conn
            self.item_name = key_prefix + "." + item_name

    class IntegerDatum(RedisAtomicBase):
        """Class for integer data supporting standard arithmetic."""

        def __init__(self, *args, **kwargs):
            """Initialize atomic integer instance.

            :param initial_value: Optional. The initial value of this
                variable in case it doesn't exist in Redis yet.
            """
            super().__init__(*args, **kwargs)
            self._lua_add = ManagerState.LuaFunction(self, "oldval + arg", short=True)
            self._lua_mul = ManagerState.LuaFunction(self, "oldval * arg", short=True)
            self._lua_floordiv = ManagerState.LuaFunction(
                self, "floor(oldval / arg)", short=True
            )
            self._lua_cas = ManagerState.LuaFunction(
                self,
                """
                local oldval = tonumber(redis.call('EXISTS', KEYS[1]) and redis.call('GET', KEYS[1]) or ARGV[3])
                if oldval == tonumber(ARGV[1]) then
                    redis.call('SET', KEYS[1], tonumber(ARGV[2]))
                    return 1
                else
                    return 0
                end
            """,
            )
            self.initial_value = kwargs.get("initial_value", 0)

        def __int__(self):
            """Convert the proxy into an integer."""
            # Use incrby instead of get, as incrby always returns an integer.
            return self.redis.incrby(self.item_name, 0)

        def set(self, val=None):
            """Set to a new value."""
            self.redis.set(self.item_name, val if val else self.initial_value)

        def add(self, val):
            """Add a value to the integer atomically."""
            return self._lua_add(val, self.initial_value)

        def mul(self, val):
            """Multiply atomically by the given value."""
            return self._lua_mul(val, self.initial_value)

        def floordiv(self, val):
            """Perform integer division atomically."""
            return self._lua_floordiv(val, self.initial_value)

        def cas(self, oldval, newval):
            """Compare and swap operation."""
            return self._lua_cas(oldval, newval, self.initial_value)

    class ObjectDatum(RedisAtomicBase):
        """Redis atomic class for JSONifiable Python data."""

        def set(self, val):
            """Set to a new value.

            :param val: The value to be serialized into JSON.
            """
            val = json.dumps(val)
            self.redis.set(self.item_name, val)

        def get(self):
            """Parse the value from the internal JSON representation.

            :return: The deserialized python object represented by the
                JSON in this variable, or ``None``.
            """
            val = self.redis.get(self.item_name)
            if not val:
                return None
            try:
                val = json.loads(val.decode("utf-8"))
            except json.JSONDecodeError:
                val = None
            return val

    def __init__(self, key_prefix):
        """Initialize the Redis connection.

        :param key_prefix: The key prefix used for variable names.
        """
        self.redis = redis.StrictRedis(
            **getattr(settings, "FLOW_EXECUTOR", {}).get("REDIS_CONNECTION", {})
        )
        self.key_prefix = key_prefix
        self._settings_override = self.ObjectDatum(
            self.redis, key_prefix, "settings_override"
        )

    def reset(self):
        """Reset all properties to their initial values."""
        self.settings_override = None

    def destroy_channels(self):
        """Destroy Redis channels managed by this state instance."""
        for item_name in dir(self):
            item = getattr(self, item_name)
            if isinstance(item, self.RedisAtomicBase):
                self.redis.delete(item.item_name)

    @property
    def settings_override(self):
        """Get the settings overrides for the manager workers."""
        return self._settings_override.get()

    @settings_override.setter
    def settings_override(self, newval):
        """Set a new settings override object."""
        self._settings_override.set(newval)
