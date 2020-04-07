# pylint: disable=missing-docstring
import random
import time
from concurrent.futures import ThreadPoolExecutor

from resolwe.storage.connectors.transfer import CircularBuffer
from resolwe.test import TestCase


class CircularBufferTest(TestCase):
    def test_basic_read_write(self):
        stream = CircularBuffer()
        data = b"testing"
        stream.write(data)
        self.assertEqual(stream.read(len(data)), data)
        self.assertEqual(stream.tell(), len(data))

    def test_read_write_circular(self):
        stream = CircularBuffer(buffer_size=11)
        data = b"testing"
        stream.write(data)
        self.assertEqual(stream.read(len(data)), data)
        # Here we have to rotate around
        stream.write(data)
        self.assertEqual(stream.read(len(data)), data)
        self.assertEqual(stream.tell(), 2 * len(data))

    def test_read_large_chunk(self):
        reading_bytes = 10000
        write_data = b"small"
        stream = CircularBuffer(buffer_size=11)

        def task_a():
            return stream.read(reading_bytes)

        def task_b():
            for i in range(int(reading_bytes / len(write_data)) + 2):
                stream.write(write_data)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(task_a)
            executor.submit(task_b)
        read_data = future_a.result()
        self.assertEqual(
            read_data,
            (write_data * (int(reading_bytes / len(write_data)) + 1))[:reading_bytes],
        )

    def test_write_large_chunk(self):
        reading_bytes = 1000
        write_data = b"writinginlargechunksmuchlargerthanmybuffer"
        stream = CircularBuffer(buffer_size=11)

        def task_a():
            data = stream.read(reading_bytes)
            stream.close()
            return data

        def task_b():
            for i in range(int(reading_bytes / len(write_data)) + 2):
                stream.write(write_data)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(task_a)
            executor.submit(task_b)
        read_data = future_a.result()
        self.assertEqual(
            read_data,
            (write_data * (int(reading_bytes / len(write_data)) + 1))[:reading_bytes],
        )

    def test_read_write_largesize_delay(self):
        mb = 1024 * 1024
        reading_bytes = 1024 * mb  # 1GB, needs at least 2G RAM
        read_chunk_max_size = 50 * mb  # 50 MB
        write_data = b"testing"
        stream = CircularBuffer(buffer_size=1024 * 1024 * 100)  # 100 MB

        def task_a():
            read_bytes = 0
            data = bytearray(reading_bytes)
            while read_bytes < reading_bytes:
                time.sleep(random.random() / 10)  # max 0.1s
                to_read = random.randint(1, read_chunk_max_size)
                to_read = min(to_read, reading_bytes - read_bytes)
                chunk = stream.read(to_read)
                data[read_bytes : read_bytes + to_read] = chunk
                read_bytes += to_read
            stream.close()
            return data

        def task_b():
            wrote_bytes = 0
            while wrote_bytes < reading_bytes:
                time.sleep(random.random() / 10)  # max 0.1s
                chunk = write_data * random.randint(1, 1000000)
                stream.write(chunk)
                wrote_bytes += len(chunk)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(task_a)
            executor.submit(task_b)
        read_data = future_a.result()
        self.assertEqual(
            read_data,
            (write_data * (int(reading_bytes / len(write_data)) + 1))[:reading_bytes],
        )
