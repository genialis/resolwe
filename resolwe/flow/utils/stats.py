""".. Ignore pydocstyle D400.

==========
Statistics
==========

Various statistical utilities, used mostly for manager load tracking.
"""
import math
import time
from collections import deque, namedtuple


class NumberSeriesShape:
    """Helper class for computing characteristics for numerical data.

    Given a series of numerical data, the class will keep a record of
    the extremes seen, arithmetic mean and standard deviation.
    """

    def __init__(self):
        """Construct an instance of the class."""
        self.high = -math.inf
        self.low = math.inf
        self.mean = 0
        self.deviation = 0
        self.count = 0
        self._rolling_variance = 0

    def update(self, num):
        """Update metrics with the new number."""
        num = float(num)
        self.count += 1
        self.low = min(self.low, num)
        self.high = max(self.high, num)

        # Welford's online mean and variance algorithm.
        delta = num - self.mean
        self.mean = self.mean + delta / self.count
        delta2 = num - self.mean
        self._rolling_variance = self._rolling_variance + delta * delta2

        if self.count > 1:
            self.deviation = math.sqrt(self._rolling_variance / (self.count - 1))
        else:
            self.deviation = 0.0

    def to_dict(self):
        """Pack the stats computed into a dictionary."""
        return {
            "high": self.high,
            "low": self.low,
            "mean": self.mean,
            "count": self.count,
            "deviation": self.deviation,
        }


def _display_interval(i):
    """Convert a time interval into a human-readable string.

    :param i: The interval to convert, in seconds.
    """
    sigils = ["d", "h", "m", "s"]
    factors = [24 * 60 * 60, 60 * 60, 60, 1]
    remain = int(i)
    result = ""
    for fac, sig in zip(factors, sigils):
        if remain < fac:
            continue
        result += "{}{}".format(remain // fac, sig)
        remain = remain % fac
    return result


class SimpleLoadAvg:
    """Helper class for a sort of load average based on event times.

    Given a series of queue depth events, it will compute the average
    number of events for three different window lengths, emulating a
    form of 'load average'. The calculation itself is modelled after the
    Linux scheduler, with a 5-second sampling rate. Because we don't get
    consistent (time-wise) samples, the sample taken is the average of a
    simple moving window for the last 5 seconds; this is to avoid
    numerical errors if actual time deltas were used to compute the
    scaled decay.
    """

    class _Interval:
        """Convenience class containing bookkeeping for an interval."""

        Point = namedtuple("Point", ["time", "count"])

        def __init__(self, interval):
            """Construct an instance of the class.

            :param interval: The interval to represent.
            """
            self.series = deque()
            self.sampling_window = 5
            self.interval = interval
            self.display = _display_interval(interval)
            self.last_bound = None
            self.decay = 1 / math.exp(self.sampling_window / self.interval)
            self.value = 0

        def push(self, count, timestamp):
            """Add a new data point to the window."""
            sample = self.Point(timestamp, count)

            # Check if we're still filling up the current window.
            if self.last_bound is None:
                self.last_bound = sample.time
                self.series.appendleft(sample)
                return
            if not self.series or sample.time - self.last_bound < self.sampling_window:
                self.series.appendleft(sample)
                return

            # If the window is full, get the average for the window size we want.
            # This will lead to some jitter if the actual time difference
            # between the latest sample and the earliest one isn't spot on,
            # but that's why this class isn't ComplexLoadAvg.
            avg = 0
            for sample in self.series:
                avg += sample.count
            avg /= len(self.series)

            # Clear storage for the next window.
            self.last_bound = timestamp
            self.series.clear()

            # The actual EMA computation.
            self.value = self.decay * self.value + (1 - self.decay) * avg

    def __init__(self, intervals):
        """Construct an instance of the class.

        :param interval: A list of interval lengths, in seconds.
        """
        self.last_data = -math.inf
        self.intervals = {i: SimpleLoadAvg._Interval(i) for i in intervals}
        for meta in list(self.intervals.values()):
            self.intervals[meta.display] = meta

    def add(self, count, timestamp=None):
        """Add a value at the specified time to the series.

        :param count: The number of work items ready at the specified
            time.
        :param timestamp: The timestamp to add. Defaults to None,
            meaning current time. It should be strictly greater (newer)
            than the last added timestamp.
        """
        if timestamp is None:
            timestamp = time.time()
        if self.last_data >= timestamp:
            raise ValueError(
                "Time {} >= {} in load average calculation".format(
                    self.last_data, timestamp
                )
            )
        self.last_data = timestamp

        for meta in self.intervals.values():
            meta.push(count, timestamp)

    def to_dict(self):
        """Pack the load averages into a nicely-keyed dictionary."""
        result = {}
        for meta in self.intervals.values():
            result[meta.display] = meta.value
        return result
