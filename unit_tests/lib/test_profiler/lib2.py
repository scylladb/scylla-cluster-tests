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
#

import time
from sdcm.utils.profiler import ProfileableProcess as LibProfileableProcess, ProfileableThread as LibProfileableThread


__all__ = [
    "LibProfileableProcessCustomClass",
    "LibProfileableProcessCustomClassWithRun",
    "LibProfileableThreadCustomClass",
    "LibProfileableThreadCustomClassWithRun",
]


def lib2_function_sleep():
    time.sleep(3)


def lib2_function_while():
    end_time = time.time() + 3
    while time.time() <= end_time:
        pass


def thread_body():
    lib2_function_sleep()
    lib2_function_while()


def lib_process_body():
    lib2_function_sleep()
    lib2_function_while()


class LibProfileableThreadCustomClass(LibProfileableThread):
    pass


class LibProfileableThreadCustomClassWithRun(LibProfileableThread):
    def run(self) -> None:
        thread_body()


class LibProfileableProcessCustomClass(LibProfileableProcess):
    pass


class LibProfileableProcessCustomClassWithRun(LibProfileableProcess):
    def run(self) -> None:
        lib_process_body()
