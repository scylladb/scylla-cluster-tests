# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

from typing import Optional, TextIO, List, Union, AnyStr, Iterable
from re import Pattern


class ReiterableGenerator:
    def __init__(self, generator):
        self._generator = generator

    def __iter__(self):
        return self._generator()


class File:
    """
    File object that support chaining and context managing

    Example:
        >>> assert File('/tmp/123', 'r+').writelines(['someline\\n', 'someline #1\\n', 'someline #2\\n']
        >>> ).move_to_beginning().readlines() == ['someline\\n', 'someline #1\\n', 'someline #2\\n']
    """

    def __init__(self, path: str, mode: str = 'r', buffering: Optional[int] = None,
                 encoding: Optional[str] = None, errors: Optional[str] = None, newline: Optional[str] = None,
                 closefd: bool = True):
        self.path = path
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd
        self._io = self._open()

    def get_file_length(self) -> int:
        return self._open().seek(0, 2)

    def flush(self):
        self._io.flush()
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._io.close()

    def _open(self) -> TextIO:
        kwargs = {attr_name: getattr(self, attr_name) for attr_name in
                  ['mode', 'buffering', 'encoding', 'errors', 'closefd'] if getattr(self, attr_name, None) is not None}
        return open(self.path, **kwargs)

    def move_to(self, pos) -> 'File':
        self._io.seek(pos)
        return self

    def move_to_end(self) -> 'File':
        self._io.seek(0, 2)
        return self

    def move_to_beginning(self) -> 'File':
        self._io.seek(0)
        return self

    def move_to_relative(self, pos: int) -> 'File':
        self._io.seek(pos, 1)
        return self

    def move_to_relative_from_end(self, pos: int = 0) -> 'File':
        self._io.seek(pos, 2)
        return self

    def write(self, any_str: AnyStr) -> 'File':
        self._io.write(any_str)
        return self

    def writelines(self, list_of_any_str: List[AnyStr]) -> 'File':
        self._io.writelines(list_of_any_str)
        return self

    def seek(self, offset: int, whence: int = 0) -> 'File':
        self._io.seek(offset, whence)
        return self

    def read(self, num_bytes: int = -1) -> AnyStr:
        return self._io.read(num_bytes)

    def readline(self, limit: int = -1) -> AnyStr:
        return self._io.readline(limit)

    def readlines(self, hint: int = -1) -> List[AnyStr]:
        return self._io.readlines(hint)

    def read_lines_filtered(self, *patterns: Union[Pattern]) -> Iterable[str]:
        """
        Read lines from the file, filter them and yield
        :param patterns: List of patterns
        :param return_pattern: If True, it will return tuple of string and pattern
        :return:
        """
        def generator():
            for line in self._io:
                for pattern in patterns:
                    if pattern.search(line):
                        yield line
                        break
        return ReiterableGenerator(generator=generator)

    def iterate_lines(self) -> Iterable[str]:
        def generator():
            for line in self._io:
                yield line
        return ReiterableGenerator(generator=generator)

    def __getattr__(self, item):
        return getattr(self._io, item)
