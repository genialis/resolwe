"""Implementation of circular buffer."""
from threading import Condition
from typing import Optional


class CircularBuffer:
    """In-memory circular buffer."""

    DEFAULT_BUFFER_LENGTH = 1024 * 1024 * 200  # 200 MB
    DEFAULT_READ_WAIT_TIMEOUT = 10  # in seconds

    def __init__(
        self, buffer_size: int = DEFAULT_BUFFER_LENGTH, name: Optional[str] = None
    ):
        """Initialize circular buffer."""
        # + 1 since at least one element in the buffer must be empty
        self.__bs = buffer_size + 1
        # Construct a buffer
        self.__buffer = memoryview(bytearray(self.__bs))
        # Where to start reading from.
        self.__tail = 0
        # Where to start writing into.
        self.__head = 0
        self.__buffer_modify = Condition()
        # How many bytes user wants to read.
        self.__reading_bytes = 0
        # Is the stream closed.
        self.__closed = False
        self._bytes_read = 0
        self.name = name

    @property
    def closed(self) -> bool:
        """Is the stream closed.

        :returns: True if stream is closed, False otherwise.
        :rtype: bool
        """
        return self.__closed

    def seekable(self) -> bool:
        """Is stream seekable."""
        return False

    @property
    def buffer_size(self) -> int:
        """Get the size of the backing memory buffer.

        :return: number of bytes that can be stored in the memory buffer
            (in bytes).
        :rtype: int
        """
        return self.__bs - 1

    def __bytes_available(self) -> int:
        """Get the number of bytes availabe to consume."""
        free = self.__head - self.__tail
        return free if free >= 0 else free + self.__bs

    def __bytes_free(self) -> int:
        """Get the number of free bytes in the buffer."""
        return self.__bs - self.__bytes_available() - 1

    @property
    def __can_write(self) -> bool:
        """Is there any space available in the memory buffer."""
        return self.__bytes_free() > 0

    def read(self, size: int = None) -> bytes:
        """Read size bytes from the buffer.

        Blocks until there are not enought bytes available. If stream is
        closed then all available bytes are immediately returned.

        :param size: the number of bytes to read.
        :type size: int

        :return: data that was read from the stream.
        :rtype: bytes
        """
        if size is None:
            # Amazon reads without size parameter if it determines that the
            # stream is "small".
            # Just make it our entire buffer size, it should be closed anyway.
            size = CircularBuffer.DEFAULT_BUFFER_LENGTH
        ret = memoryview(bytearray(size))
        ret_pos = 0

        with self.__buffer_modify:
            while ret_pos < size:
                self.__reading_bytes = min(size, self.buffer_size)
                while (
                    self.__bytes_available() < self.__reading_bytes and not self.closed
                ):
                    self.__buffer_modify.wait(CircularBuffer.DEFAULT_READ_WAIT_TIMEOUT)
                bytes_to_read = min(self.__bytes_available(), size - ret_pos)
                slice_start = self.__tail
                slice_end = slice_start + bytes_to_read
                if slice_end <= self.__bs:
                    ret[ret_pos : ret_pos + bytes_to_read] = self.__buffer[
                        slice_start:slice_end
                    ]
                else:  # rotation
                    slice_end -= self.__bs
                    sep = self.__bs - slice_start
                    ret[ret_pos : ret_pos + sep] = self.__buffer[
                        slice_start : self.__bs
                    ]
                    ret[ret_pos + sep : ret_pos + sep + slice_end] = self.__buffer[
                        0:slice_end
                    ]
                self.__tail = slice_end
                ret_pos += bytes_to_read
                self._bytes_read += bytes_to_read
                # Notify the writing thread.
                self.__buffer_modify.notify()
                if self.closed:
                    break

            self.__reading_bytes = 0
            # Notify the writing thread.
            self.__buffer_modify.notify()
            return ret[0:ret_pos].tobytes()

    def write(self, data: bytes) -> int:
        """Write data into the stream.

        :param data: data to be writen to the stream.
        :type data: bytes

        :return: the number of bytes actually writen.
        :rtype: int
        """
        datamv = memoryview(data)
        data_pos = 0
        with self.__buffer_modify:
            # Wait for enough bytes to become available.
            # Abort if stream was closed.
            while data_pos < len(datamv) and not self.closed:
                while not self.__can_write and not self.closed:
                    self.__buffer_modify.wait()
                bytes_to_write = min(self.__bytes_free(), len(datamv) - data_pos)
                slice_start = self.__head
                slice_end = slice_start + bytes_to_write
                if slice_end <= self.__bs:
                    self.__buffer[slice_start : slice_start + bytes_to_write] = datamv[
                        data_pos : data_pos + bytes_to_write
                    ]
                else:  # rotation
                    slice_end %= self.__bs
                    sep = self.__bs - slice_start
                    self.__buffer[slice_start : self.__bs] = datamv[
                        data_pos : data_pos + sep
                    ]
                    remaining_bytes = bytes_to_write - sep
                    self.__buffer[0:remaining_bytes] = datamv[
                        data_pos + sep : data_pos + bytes_to_write
                    ]
                self.__head += bytes_to_write
                self.__head %= self.__bs
                data_pos += bytes_to_write
                if self.__reading_bytes > 0:
                    if self.__bytes_available() >= self.__reading_bytes:
                        self.__buffer_modify.notify()
        return data_pos

    def close(self):
        """Close stream."""
        self.__closed = True
        with self.__buffer_modify:
            self.__buffer_modify.notify()

    def tell(self) -> int:
        """Get the number of bytes read from the stream.

        :return: number of bytes read from the stream.
        :rtype: int
        """
        return self._bytes_read
