# pylint: disable=missing-docstring
import math

from resolwe.flow.utils import stats
from resolwe.test import TestCase


class TestStatUtils(TestCase):

    def test_shape_basic(self):
        series = stats.NumberSeriesShape()
        self.assertEqual(series.to_dict(), {
            'high': -math.inf,
            'low': math.inf,
            'mean': 0,
            'count': 0,
            'deviation': 0,
        })

        series.update(1)
        self.assertEqual(series.count, 1)
        self.assertAlmostEqual(series.high, 1.0)
        self.assertAlmostEqual(series.low, 1.0)
        self.assertAlmostEqual(series.mean, 1.0)
        self.assertAlmostEqual(series.deviation, 0.0)

        for _ in range(5):
            series.update(1)
        self.assertEqual(series.count, 6)
        self.assertAlmostEqual(series.high, 1.0)
        self.assertAlmostEqual(series.low, 1.0)
        self.assertAlmostEqual(series.mean, 1.0)
        self.assertAlmostEqual(series.deviation, 0.0)

        large = 1000000.0
        series.update(large)
        series.update(-large)
        self.assertAlmostEqual(series.high, large)
        self.assertAlmostEqual(series.low, -large)
        self.assertAlmostEqual(series.mean, 0.75)
        self.assertAlmostEqual(series.deviation, 534522.483825049)

    def test_shape_tricky(self):
        series = stats.NumberSeriesShape()
        for _ in range(10):
            series.update(1)
        for _ in range(10):
            series.update(-1)
        self.assertAlmostEqual(series.mean, 0.0)
        self.assertAlmostEqual(series.deviation, 1.02597835208515)

    def test_load_avg_basic(self):
        avg = stats.SimpleLoadAvg([60])

        self.assertAlmostEqual(avg.intervals[60].value, 0.0)
        self.assertAlmostEqual(avg.intervals['1m'].value, 0.0)
        avg.add(1, timestamp=0.0)
        self.assertAlmostEqual(avg.intervals[60].value, 0.0)

        avg.add(1, timestamp=0.2)
        with self.assertRaises(ValueError):
            avg.add(1, timestamp=0.1)

        avg.add(1, timestamp=0.4)
        avg.add(1, timestamp=0.6)
        avg.add(1, timestamp=0.8)
        avg.add(1, timestamp=1.0)
        avg.add(1, timestamp=5)
        self.assertAlmostEqual(avg.intervals[60].value, 0.1, places=1)

    def test_load_avg_multi(self):
        avg = stats.SimpleLoadAvg([60, 5 * 60, 15 * 60])
        for i in range(1, 15 * 60):
            avg.add(1, timestamp=i)
        self.assertAlmostEqual(avg.intervals['1m'].value, 1, places=1)
        self.assertAlmostEqual(avg.intervals['5m'].value, 0.9, places=1)
        self.assertAlmostEqual(avg.intervals['15m'].value, 0.6, places=1)

    def test_load_avg_decay(self):
        avg = stats.SimpleLoadAvg([60])
        for i in range(0, 5 * 60):
            avg.add(2, timestamp=i)
        self.assertAlmostEqual(avg.intervals['1m'].value, 2, places=1)

        for i in range(5 * 60, 10 * 60):
            avg.add(5, timestamp=i)
        self.assertAlmostEqual(avg.intervals['1m'].value, 5, places=1)

        for i in range(10 * 60, 15 * 60):
            avg.add(0, timestamp=i)
        self.assertAlmostEqual(avg.intervals['1m'].value, 0, places=1)

    def test_load_avg_display(self):
        # pylint: disable=protected-access
        self.assertEqual(stats._display_interval(59), '59s')
        self.assertEqual(stats._display_interval(60), '1m')
        self.assertEqual(stats._display_interval(65), '1m5s')
        self.assertEqual(stats._display_interval(300), '5m')
        self.assertEqual(stats._display_interval(3666), '1h1m6s')
        self.assertEqual(stats._display_interval(260200), '3d16m40s')
