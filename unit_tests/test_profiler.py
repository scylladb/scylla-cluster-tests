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
from threading import Thread
import multiprocessing
from multiprocessing import Process
import unittest
import os.path
import shutil
from sdcm.utils.common import generate_random_string
from sdcm.prometheus import PrometheusAlertManagerListener
from unit_tests.lib.test_profiler.lib import LibMultiprocessingProcessCustomClass, LibProcessCustomClass, \
    LibMultiprocessingProcessCustomClassWithRun, LibProcessCustomClassWithRun, LibThreadCustomClass, \
    LibThreadCustomClassWithRun, LibThreadingThreadCustomClass, LibThreadingThreadCustomClassWithRun, LibThread, LibProcess

from unit_tests.lib.test_profiler.lib2 import LibProfileableProcessCustomClass, \
    LibProfileableProcessCustomClassWithRun, LibProfileableThreadCustomClass, LibProfileableThreadCustomClassWithRun, \
    LibProfileableThread, LibProfileableProcess

from sdcm.utils.profiler import ProfilerFactory, ProfileableProcess as pp, ProfileableThread as pt


def function_sleep():
    time.sleep(3)


def function_while():
    end_time = time.time() + 3
    while time.time() <= end_time:
        pass


def thread_body():
    function_sleep()
    function_while()


def process_body():
    function_sleep()
    function_while()


class ThreadCustomClass(Thread):
    pass


class ThreadCustomClassWithRun(Thread):
    def run(self) -> None:
        thread_body()


class ThreadingThreadCustomClass(threading.Thread):
    pass


class ThreadingThreadCustomClassWithRun(threading.Thread):
    def run(self) -> None:
        thread_body()


class ProfileableThreadCustomClass(pt):
    pass


class ProfileableThreadCustomClassWithRun(pt):
    def run(self) -> None:
        thread_body()


class ProcessCustomClass(Process):
    pass


class ProcessCustomClassWithRun(Process):
    def run(self) -> None:
        process_body()


class MultiprocessingProcessCustomClass(multiprocessing.Process):
    pass


class MultiprocessingProcessCustomClassWithRun(multiprocessing.Process):
    def run(self) -> None:
        process_body()


class ProfileableProcessCustomClass(pp):
    pass


class ProfileableProcessCustomClassWithRun(pp):
    def run(self) -> None:
        process_body()


class TestProfileFactory(unittest.TestCase):
    """ Test to illustrate profile factory usage """
    tmpdir = os.path.join('/tmp', generate_random_string(10))

    def __init__(self, *args, **kwargs):
        self._subroutines = []
        self.pr_factory = None
        super().__init__(*args, **kwargs)

    def tearDown(self) -> None:
        if not self.pr_factory:
            return
        self.pr_factory.stop_and_dump()
        dirs_to_expect = [
            'PrometheusAlertManagerListener',
            'main',
        ]
        for sub in self._subroutines:
            dirs_to_expect.append(sub._name)

        not_found = []
        for directory in dirs_to_expect:
            bin_path = os.path.join(self.tmpdir, directory, 'stats.bin')
            txt_path = os.path.join(self.tmpdir, directory, 'stats.txt')
            for path in (bin_path, txt_path):
                if not os.path.exists(path):
                    not_found.append(path)
        bin_path = os.path.join(self.tmpdir, 'stats.bin')
        txt_path = os.path.join(self.tmpdir, 'stats.txt')
        for path in (bin_path, txt_path):
            if not os.path.exists(path):
                not_found.append(path)
        self._subroutines = []
        if self.pr_factory:
            self.pr_factory.deactivate()
        assert not not_found, "Following files were not found:\n" + '\n'.join(not_found)
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def test_profile_enabled(self):
        self.pr_factory = ProfilerFactory(self.tmpdir)
        self.pr_factory.activate(autodump=False)
        self._add_tests()
        for sub in self._subroutines:
            sub.start()
        tmp = PrometheusAlertManagerListener('127.0.0.1')
        tmp.start()
        function_sleep()
        function_while()
        done = False
        while not done:
            done = True
            for sub in self._subroutines:
                if sub.is_alive():
                    done = False
                    break
        tmp.stop()

    def test_profile_disabled(self):
        self._add_tests()
        for sub in self._subroutines:
            sub.start()
        tmp = PrometheusAlertManagerListener('127.0.0.1')
        tmp.start()
        function_sleep()
        function_while()
        done = False
        while not done:
            done = True
            for sub in self._subroutines:
                if sub.is_alive():
                    done = False
                    break
        tmp.stop()

    def _add_tests(self):
        self._subroutines.append(Thread(target=thread_body, name="Thread.daemon", daemon=True))
        self._subroutines.append(LibThread(target=thread_body, name="lib.Thread.daemon", daemon=True))
        self._subroutines.append(ThreadCustomClass(target=thread_body, name="Thread.CustomClass.daemon", daemon=True))
        self._subroutines.append(LibThreadCustomClass(
            target=thread_body, name="lib.Thread.CustomClass.daemon", daemon=True))
        self._subroutines.append(ThreadCustomClassWithRun(
            target=thread_body, name="Thread.CustomClassWithRun.daemon", daemon=True))
        self._subroutines.append(LibThreadCustomClassWithRun(
            target=thread_body, name="lib.Thread.CustomClassWithRun.daemon", daemon=True))

        self._subroutines.append(threading.Thread(target=thread_body, name='threading.Thread.daemon', daemon=True))
        self._subroutines.append(ThreadingThreadCustomClass(
            target=thread_body, name="threading.Thread.CustomClass.daemon", daemon=True))
        self._subroutines.append(LibThreadingThreadCustomClass(
            target=thread_body, name="lib.threading.Thread.CustomClass.daemon", daemon=True))
        self._subroutines.append(ThreadingThreadCustomClassWithRun(
            name="threading.Thread.CustomClassWithRun.daemon", daemon=True))
        self._subroutines.append(LibThreadingThreadCustomClassWithRun(
            name="lib.threading.Thread.CustomClassWithRun.daemon", daemon=True))

        self._subroutines.append(pt(target=thread_body, name='ProfilerableThread.daemon', daemon=True))
        self._subroutines.append(LibProfileableThread(
            target=thread_body, name='lib.ProfilerableThread.daemon', daemon=True))
        self._subroutines.append(ProfileableThreadCustomClass(
            target=thread_body, name="ProfilerableThread.CustomClass.daemon", daemon=True))
        self._subroutines.append(LibProfileableThreadCustomClass(
            target=thread_body, name="lib.ProfilerableThread.CustomClass.daemon", daemon=True))
        self._subroutines.append(ProfileableThreadCustomClassWithRun(
            name="ProfilerableThread.CustomClassWithRun.daemon", daemon=True))
        self._subroutines.append(LibProfileableThreadCustomClassWithRun(
            name="lib.ProfilerableThread.CustomClassWithRun.daemon", daemon=True))

        self._subroutines.append(Thread(target=thread_body, name="Thread", daemon=False))
        self._subroutines.append(LibThread(target=thread_body, name="lib.Thread", daemon=False))
        self._subroutines.append(ThreadCustomClass(target=thread_body, name="Thread.CustomClass", daemon=False))
        self._subroutines.append(LibThreadCustomClass(target=thread_body, name="lib.Thread.CustomClass", daemon=False))
        self._subroutines.append(ThreadCustomClassWithRun(
            target=thread_body, name="Thread.CustomClassWithRun", daemon=False))
        self._subroutines.append(LibThreadCustomClassWithRun(
            target=thread_body, name="lib.Thread.CustomClassWithRun", daemon=False))

        self._subroutines.append(threading.Thread(target=thread_body, name='threading.Thread', daemon=False))
        self._subroutines.append(LibThreadingThreadCustomClass(
            target=thread_body, name="lib.ThreadingThread.CustomClass", daemon=False))
        self._subroutines.append(LibThreadingThreadCustomClassWithRun(
            name="lib.ThreadingThread.CustomClassWithRun", daemon=False))

        self._subroutines.append(pt(target=thread_body, name='ProfilerableThread.CustomClass', daemon=False))
        self._subroutines.append(LibProfileableThread(
            target=thread_body, name='lib.ProfilerableThread.CustomClass', daemon=False))
        self._subroutines.append(ProfileableThreadCustomClass(
            target=thread_body, name="ProfilerableThread.CustomClass", daemon=False))
        self._subroutines.append(LibProfileableThreadCustomClass(
            target=thread_body, name="lib.ProfilerableThread.CustomClass", daemon=False))
        self._subroutines.append(ProfileableThreadCustomClassWithRun(
            name="ProfilerableThread.CustomClassWithRun", daemon=False))
        self._subroutines.append(LibProfileableThreadCustomClassWithRun(
            name="lib.ProfilerableThread.CustomClassWithRun", daemon=False))

        self._subroutines.append(Process(target=process_body, name="Process.daemon", daemon=True))
        self._subroutines.append(LibProcess(target=process_body, name="lib.Process.daemon", daemon=True))
        self._subroutines.append(ProcessCustomClass(
            target=process_body, name="Process.CustomClass.daemon", daemon=True))
        self._subroutines.append(LibProcessCustomClass(
            target=process_body, name="lib.Process.CustomClass.daemon", daemon=True))
        self._subroutines.append(ProcessCustomClassWithRun(
            target=process_body, name="Process.CustomClassWithRun.daemon", daemon=True))
        self._subroutines.append(LibProcessCustomClassWithRun(
            target=process_body, name="lib.Process.CustomClassWithRun.daemon", daemon=True))

        self._subroutines.append(multiprocessing.Process(
            target=process_body, name='multiprocessing.Process.daemon', daemon=True))
        self._subroutines.append(MultiprocessingProcessCustomClass(
            target=process_body, name="multiprocessing.Process.CustomClass.daemon", daemon=True))
        self._subroutines.append(LibMultiprocessingProcessCustomClass(
            target=process_body, name="lib.multiprocessing.Process.CustomClass.daemon", daemon=True))
        self._subroutines.append(MultiprocessingProcessCustomClassWithRun(
            name="multiprocessing.Process.CustomClassWithRun.daemon", daemon=True))
        self._subroutines.append(LibMultiprocessingProcessCustomClassWithRun(
            name="lib.multiprocessing.Process.CustomClassWithRun.daemon", daemon=True))

        self._subroutines.append(pp(target=thread_body, name='ProfilerableProcess.CustomClass.daemon', daemon=True))
        self._subroutines.append(LibProfileableProcess(
            target=process_body, name='lib.ProfilerableProcess.CustomClass.daemon', daemon=True))
        self._subroutines.append(ProfileableProcessCustomClass(
            target=process_body, name="ProfilerableProcess.CustomClass.daemon", daemon=True))
        self._subroutines.append(LibProfileableProcessCustomClass(
            target=process_body, name="lib.ProfilerableProcess.CustomClass.daemon", daemon=True))
        self._subroutines.append(ProfileableProcessCustomClassWithRun(
            name="ProfilerableProcess.CustomClassWithRun.daemon", daemon=True))
        self._subroutines.append(LibProfileableProcessCustomClassWithRun(
            name="lib.ProfilerableProcess.CustomClassWithRun.daemon", daemon=True))

        self._subroutines.append(Process(target=thread_body, name="Process", daemon=False))
        self._subroutines.append(LibProcess(target=thread_body, name="lib.Process", daemon=False))
        self._subroutines.append(ProcessCustomClass(
            target=process_body, name="Process.CustomClass", daemon=False))
        self._subroutines.append(LibProcessCustomClass(
            target=process_body, name="lib.Process.CustomClass", daemon=False))
        self._subroutines.append(ProcessCustomClassWithRun(
            target=process_body, name="Process.CustomClassWithRun", daemon=False))
        self._subroutines.append(LibProcessCustomClassWithRun(
            target=process_body, name="lib.Process.CustomClassWithRun", daemon=False))

        self._subroutines.append(multiprocessing.Process(
            target=process_body, name='multiprocessing.Process', daemon=False))
        self._subroutines.append(MultiprocessingProcessCustomClass(
            target=process_body, name="MultiprocessingProcess.CustomClass", daemon=False))
        self._subroutines.append(LibMultiprocessingProcessCustomClass(
            target=process_body, name="lib.MultiprocessingProcess.CustomClass", daemon=False))
        self._subroutines.append(MultiprocessingProcessCustomClassWithRun(
            name="MultiprocessingProcess.CustomClassWithRun", daemon=False))
        self._subroutines.append(LibMultiprocessingProcessCustomClassWithRun(
            name="lib.MultiprocessingProcess.CustomClassWithRun", daemon=False))

        self._subroutines.append(pp(target=process_body, name='ProfilerableProcess.CustomClass', daemon=False))
        self._subroutines.append(LibProfileableProcess(
            target=process_body, name='lib.ProfilerableProcess.CustomClass', daemon=False))
        self._subroutines.append(ProfileableProcessCustomClass(
            target=process_body, name="ProfilerableProcess.CustomClass", daemon=False))
        self._subroutines.append(LibProfileableProcessCustomClass(
            target=process_body, name="lib.ProfilerableProcess.CustomClass", daemon=False))
        self._subroutines.append(ProfileableProcessCustomClassWithRun(
            name="ProfilerableProcess.CustomClassWithRun", daemon=False))
        self._subroutines.append(LibProfileableProcessCustomClassWithRun(
            name="lib.ProfilerableProcess.CustomClassWithRun", daemon=False))
