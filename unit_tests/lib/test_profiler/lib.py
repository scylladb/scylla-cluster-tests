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
import threading
from threading import Thread as LibThread
from threading import Thread
import multiprocessing
from multiprocessing import Process as LibProcess
from multiprocessing import Process


__all__ = [
    'LibMultiprocessingProcessCustomClass', 'LibProcessCustomClass', 'LibMultiprocessingProcessCustomClassWithRun',
    'LibProcessCustomClassWithRun', 'LibThreadCustomClass',
    'LibThreadCustomClassWithRun', 'LibThreadingThreadCustomClass', 'LibThreadingThreadCustomClassWithRun', 'LibThread',
    'LibProcess'
]


def lib_function_sleep():
    time.sleep(3)


def lib_function_while():
    end_time = time.time() + 3
    while time.time() <= end_time:
        pass


def lib_thread_body():
    lib_function_sleep()
    lib_function_while()


def lib_process_body():
    lib_function_sleep()
    lib_function_while()


class LibThreadCustomClass(Thread):
    pass


class LibThreadCustomClassWithRun(Thread):
    def run(self) -> None:
        lib_thread_body()


class LibThreadingThreadCustomClass(threading.Thread):
    pass


class LibThreadingThreadCustomClassWithRun(threading.Thread):
    def run(self) -> None:
        lib_thread_body()


class LibProcessCustomClass(Process):
    pass


class LibProcessCustomClassWithRun(Process):
    def run(self) -> None:
        lib_process_body()


class LibMultiprocessingProcessCustomClass(multiprocessing.Process):
    pass


class LibMultiprocessingProcessCustomClassWithRun(multiprocessing.Process):
    def run(self) -> None:
        lib_process_body()
